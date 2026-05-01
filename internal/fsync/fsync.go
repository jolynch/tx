package fsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"golang.org/x/sys/unix"
)

const (
	backgroundSyncMemoryCapPercent       = 80
	backgroundSyncMaxBatchFiles          = 4096
	backgroundSyncFallbackCapBytes int64 = 8 << 30
)

// SyncWorker manages per-file durability work after writes complete.
type SyncWorker struct {
	background *backgroundSyncer
	inlineSync bool
}

// StartSyncWorker starts a background sync worker. The returned stop function
// must be called when the operation completes; it drains pending syncs and
// logs a summary to stderr.
//
// fsyncInterval controls behavior:
//   - 0: inline fdatasync (the sync callback blocks until fdatasync completes)
//   - >0: background batch fdatasync (enqueue to channel, batch every N bytes)
//   - <0: no per-file fdatasync
//
// If noSync is true, all per-file syncing is disabled and stop is a no-op.
func StartSyncWorker(fsyncInterval int64, noSync bool, stderr io.Writer) (*SyncWorker, func()) {
	worker := &SyncWorker{}
	if noSync {
		return worker, func() {}
	}
	switch {
	case fsyncInterval == 0:
		worker.inlineSync = true
		return worker, func() {}
	case fsyncInterval > 0:
		worker.background = newBackgroundSyncer(fsyncInterval, stderr)
		return worker, func() { worker.background.stop(stderr) }
	default:
		return worker, func() {}
	}
}

// SyncOutput returns a sync callback for fd. The returned function is intended
// to run after all writes complete but before fd is closed.
//
// The state of fd at callback time is important: the file descriptor must still
// be valid and positioned at the final write offset for this output. offset
// must be the starting write offset for the same output segment, because the
// background sync path uses currentPosition-offset to account for written bytes.
func (w *SyncWorker) SyncOutput(fd *os.File, offset int64) func() error {
	if w == nil || fd == nil {
		return func() error { return nil }
	}
	return func() error {
		if w.background != nil {
			dupFD, err := syscall.Dup(int(fd.Fd()))
			if err != nil {
				return syscall.Fdatasync(int(fd.Fd()))
			}
			pos, _ := fd.Seek(0, io.SeekCurrent)
			written := pos - offset
			if written < 0 {
				written = 0
			}
			w.background.enqueue(dupFD, written)
			return nil
		}
		if w.inlineSync {
			return syscall.Fdatasync(int(fd.Fd()))
		}
		return nil
	}
}

// SyncfsDir issues a best-effort syncfs against dir and returns when either
// syncfs completes or ctx is done.
func SyncfsDir(ctx context.Context, dir string, stderr io.Writer) {
	if ctx == nil {
		ctx = context.Background()
	}
	displayPath := dir
	if absPath, err := filepath.Abs(dir); err == nil {
		displayPath = absPath
	}
	fd, err := unix.Open(dir, unix.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(stderr, "final-syncfs: [%s] open failed: %v\n", displayPath, err)
		return
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- unix.Syncfs(fd)
	}()
	select {
	case err := <-done:
		unix.Close(fd)
		elapsed := time.Since(start)
		if err != nil {
			fmt.Fprintf(stderr, "final-syncfs: [%s] filesystem sync failed after %s: %v\n", displayPath, elapsed.Round(time.Millisecond), err)
		} else {
			fmt.Fprintf(stderr, "final-syncfs: [%s] filesystem sync completed in %s\n", displayPath, elapsed.Round(time.Millisecond))
		}
	case <-ctx.Done():
		unix.Close(fd)
		elapsed := time.Since(start)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			fmt.Fprintf(stderr, "WARNING: final-syncfs: [%s] filesystem sync timed out after %s. Data may not yet be durable on disk\n", displayPath, elapsed.Round(time.Millisecond))
			return
		}
		fmt.Fprintf(stderr, "WARNING: final-syncfs: [%s] filesystem sync canceled after %s: %v\n", displayPath, elapsed.Round(time.Millisecond), ctx.Err())
	}
}

// backgroundSyncer moves fdatasync off the download→ack critical path. A single
// reader goroutine drains dup'd file descriptors, forms batches, and dispatches
// those batches to worker goroutines. The byte threshold expands with backlog
// pressure up to a fraction of system memory (or an 8 GiB fallback cap when
// memory size is unavailable), and a file-count cap bounds the number of
// outstanding dup'd descriptors.
type backgroundSyncer struct {
	ch            chan syncRequest
	done          chan struct{}
	wg            sync.WaitGroup
	baseThreshold int64
	maxThreshold  int64
	maxBatchFiles int
	synced        atomic.Int64
	pendingBytes  atomic.Int64
	pendingFiles  atomic.Int64
	peakBytes     atomic.Int64
	peakFiles     atomic.Int64
	currentBatch  atomic.Int64
	peakBatch     atomic.Int64
}

type syncRequest struct {
	fd   int
	size int64
}

func newBackgroundSyncer(threshold int64, stderr io.Writer) *backgroundSyncer {
	bs := &backgroundSyncer{
		ch:            make(chan syncRequest, 1024),
		done:          make(chan struct{}),
		baseThreshold: threshold,
		maxThreshold:  maxAdaptiveThreshold(threshold),
		maxBatchFiles: backgroundSyncMaxBatchFiles,
	}
	bs.currentBatch.Store(threshold)
	bs.peakBatch.Store(threshold)
	go bs.run(stderr)
	return bs
}

func (bs *backgroundSyncer) run(stderr io.Writer) {
	defer close(bs.done)
	var batch []syncRequest
	var batchBytes int64
	for req := range bs.ch {
		batch = append(batch, req)
		batchBytes += req.size
		threshold := adaptiveThreshold(bs.baseThreshold, bs.pendingBytes.Load(), bs.maxThreshold)
		bs.updateBatchThreshold(threshold, stderr)
		if batchBytes >= threshold || len(batch) >= bs.maxBatchFiles {
			// Alias to prevent closure
			batchToSync := batch
			bs.wg.Go(func() {
				bs.syncBatch(batchToSync, stderr)
			})
			bs.updateBatchThreshold(adaptiveThreshold(bs.baseThreshold, bs.pendingBytes.Load(), bs.maxThreshold), nil)
			batch = nil
			batchBytes = 0
		}
	}
	if len(batch) > 0 {
		// Alias to prevent closure
		batchToSync := batch
		bs.wg.Go(func() {
			bs.syncBatch(batchToSync, stderr)
		})
	}
	bs.wg.Wait()
	// Reset currentBatch after all async workers drain so stop() reports the
	// steady-state threshold rather than the last backlog-inflated value.
	bs.updateBatchThreshold(adaptiveThreshold(bs.baseThreshold, bs.pendingBytes.Load(), bs.maxThreshold), nil)
}

func (bs *backgroundSyncer) syncBatch(batch []syncRequest, stderr io.Writer) {
	type fileKey struct{ dev, ino uint64 }
	seen := make(map[fileKey]bool, len(batch))
	for _, req := range batch {
		var stat syscall.Stat_t
		if err := syscall.Fstat(req.fd, &stat); err == nil {
			key := fileKey{stat.Dev, stat.Ino}
			if !seen[key] {
				if err := syscall.Fdatasync(req.fd); err != nil {
					fmt.Fprintf(stderr, "background fdatasync: %v\n", err)
				}
				seen[key] = true
			}
		}
		syscall.Close(req.fd)
		bs.pendingBytes.Add(-req.size)
		bs.pendingFiles.Add(-1)
		bs.synced.Add(req.size)
	}
}

func (bs *backgroundSyncer) enqueue(fd int, size int64) {
	pendingBytes := bs.pendingBytes.Add(size)
	pendingFiles := bs.pendingFiles.Add(1)
	updateMaxAtomic(&bs.peakBytes, pendingBytes)
	updateMaxAtomic(&bs.peakFiles, pendingFiles)
	bs.ch <- syncRequest{fd: fd, size: size}
}

func (bs *backgroundSyncer) stop(stderr io.Writer) {
	close(bs.ch)
	start := time.Now()
	<-bs.done
	elapsed := time.Since(start)
	fmt.Fprintf(stderr, "background-fsync: drained in %s, synced=%s pending=%s/%d files peak=%s/%d files batch=%s peak-batch=%s cap=%s\n",
		elapsed.Round(time.Millisecond),
		encoding.HumanBytes(bs.synced.Load()),
		encoding.HumanBytes(bs.pendingBytes.Load()),
		bs.pendingFiles.Load(),
		encoding.HumanBytes(bs.peakBytes.Load()),
		bs.peakFiles.Load(),
		encoding.HumanBytes(bs.currentBatch.Load()),
		encoding.HumanBytes(bs.peakBatch.Load()),
		encoding.HumanBytes(bs.maxThreshold))
}

func (bs *backgroundSyncer) updateBatchThreshold(threshold int64, stderr io.Writer) {
	current := bs.currentBatch.Load()
	if threshold > current && stderr != nil {
		fmt.Fprintf(stderr, "background-fsync: disk falling behind, growing batch threshold %s -> %s pending=%s cap=%s\n",
			encoding.HumanBytes(current),
			encoding.HumanBytes(threshold),
			encoding.HumanBytes(bs.pendingBytes.Load()),
			encoding.HumanBytes(bs.maxThreshold))
	}
	bs.currentBatch.Store(threshold)
	updateMaxAtomic(&bs.peakBatch, threshold)
}

// adaptiveThreshold returns the smallest power-of-two multiple of
// baseThreshold that is at least as large as pendingBytes, capped at
// maxThreshold and never below baseThreshold.
func adaptiveThreshold(baseThreshold int64, pendingBytes int64, maxThreshold int64) int64 {
	if baseThreshold <= 0 {
		return 0
	}
	if maxThreshold < baseThreshold {
		maxThreshold = baseThreshold
	}
	threshold := baseThreshold
	for threshold < pendingBytes && threshold < maxThreshold {
		if threshold > maxThreshold/2 {
			return maxThreshold
		}
		threshold *= 2
	}
	if threshold > maxThreshold {
		return maxThreshold
	}
	return threshold
}

func maxAdaptiveThreshold(baseThreshold int64) int64 {
	return maxAdaptiveThresholdForMemory(baseThreshold, systemMemoryBytes())
}

func maxAdaptiveThresholdForMemory(baseThreshold int64, mem int64) int64 {
	capBytes := backgroundSyncFallbackCapBytes
	if mem > 0 {
		capBytes = mem * backgroundSyncMemoryCapPercent / 100
	}
	if capBytes < baseThreshold {
		return baseThreshold
	}
	return capBytes
}

func systemMemoryBytes() int64 {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0
	}
	total := uint64(info.Totalram) * uint64(info.Unit)
	if total == 0 || total > uint64(^uint(0)>>1) {
		return 0
	}
	return int64(total)
}

func updateMaxAtomic(dst *atomic.Int64, candidate int64) {
	for {
		current := dst.Load()
		if candidate <= current {
			return
		}
		if dst.CompareAndSwap(current, candidate) {
			return
		}
	}
}
