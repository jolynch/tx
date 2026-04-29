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
		fmt.Fprintf(stderr, "syncfs: [%s] open failed: %v\n", displayPath, err)
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
			fmt.Fprintf(stderr, "syncfs: [%s] filesystem sync failed after %s: %v\n", displayPath, elapsed.Round(time.Millisecond), err)
		} else {
			fmt.Fprintf(stderr, "syncfs: [%s] filesystem sync completed in %s\n", displayPath, elapsed.Round(time.Millisecond))
		}
	case <-ctx.Done():
		unix.Close(fd)
		elapsed := time.Since(start)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			fmt.Fprintf(stderr, "WARNING: syncfs: [%s] filesystem sync timed out after %s. Data may not yet be durable on disk\n", displayPath, elapsed.Round(time.Millisecond))
			return
		}
		fmt.Fprintf(stderr, "WARNING: syncfs: [%s] filesystem sync canceled after %s: %v\n", displayPath, elapsed.Round(time.Millisecond), ctx.Err())
	}
}

// backgroundSyncer moves fdatasync off the download→ack critical path. A single
// reader goroutine drains a channel of dup'd file descriptors and accumulated
// write sizes. When the accumulated size reaches the configured threshold, a
// worker goroutine is spawned to fdatasync the batch (deduplicating by inode).
type backgroundSyncer struct {
	ch        chan syncRequest
	done      chan struct{}
	wg        sync.WaitGroup
	threshold int64
	synced    atomic.Int64
	pending   atomic.Int64
}

type syncRequest struct {
	fd   int
	size int64
}

func newBackgroundSyncer(threshold int64, stderr io.Writer) *backgroundSyncer {
	bs := &backgroundSyncer{
		ch:        make(chan syncRequest, 1024),
		done:      make(chan struct{}),
		threshold: threshold,
	}
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
		if batchBytes >= bs.threshold {
			// Alias needed to ensure wg.Go gets the expected batch
			batchToSync := batch
			bs.wg.Go(func() {
				bs.syncBatch(batchToSync, stderr)
			})
			batch = nil
			batchBytes = 0
		}
	}
	if len(batch) > 0 {
		batchToSync := batch
		bs.wg.Go(func() {
			bs.syncBatch(batchToSync, stderr)
		})
	}
	bs.wg.Wait()
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
		bs.pending.Add(-req.size)
		bs.synced.Add(req.size)
	}
}

func (bs *backgroundSyncer) enqueue(fd int, size int64) {
	bs.pending.Add(size)
	bs.ch <- syncRequest{fd: fd, size: size}
}

func (bs *backgroundSyncer) stop(stderr io.Writer) {
	close(bs.ch)
	start := time.Now()
	<-bs.done
	elapsed := time.Since(start)
	fmt.Fprintf(stderr, "background-sync: drained in %s, synced=%s pending=%s\n",
		elapsed.Round(time.Millisecond),
		encoding.HumanBytes(bs.synced.Load()),
		encoding.HumanBytes(bs.pending.Load()))
}
