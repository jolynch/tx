package fsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/sysio"
)

const (
	backgroundSyncMemoryCapPercent       = 80
	backgroundSyncMaxBatchFiles          = 4096
	backgroundSyncFallbackCapBytes int64 = 8 << 30
	// maxReportedSyncFailures caps the paths stop's error names; the
	// per-file stderr line names every one.
	maxReportedSyncFailures    = 20
	fsyncProgressCountWidth    = 6
	fsyncProgressBytesWidth    = 10
	fsyncProgressDurationWidth = 5
)

// ErrSyncFailed marks stop's error when a background fdatasync failed: those
// files may not be durable. ErrDrainIncomplete marks it when ctx ended the
// drain first, so the durability of the files still queued is unknown.
var (
	ErrSyncFailed      = errors.New("background fdatasync failed")
	ErrDrainIncomplete = errors.New("background fdatasync drain incomplete")
)

// markedError reads as msg and matches mark under errors.Is, so a sentinel
// classifies an error without adding its own text to the message.
type markedError struct {
	msg  string
	mark error
}

func (e *markedError) Error() string        { return e.msg }
func (e *markedError) Is(target error) bool { return target == e.mark }

// SyncWorker manages per-file durability work after writes complete.
type SyncWorker struct {
	background *backgroundSyncer
	inlineSync bool
	faults     *faults.Injector
}

// Option configures StartSyncWorker and SyncfsDir.
type Option func(*options)

type options struct {
	faults *faults.Injector
}

// WithFaults gates the worker's syscalls behind inj; nil, the default,
// disables fault injection.
func WithFaults(inj *faults.Injector) Option {
	return func(o *options) { o.faults = inj }
}

func applyOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

type SyncSnapshot struct {
	PendingBytes int64
	PendingFiles int64
	SyncedBytes  int64
	SyncedFiles  int64
}

// StartSyncWorker starts a background sync worker. The returned stop function
// must be called when the operation completes; it closes the enqueue path,
// drains pending syncs until either the worker finishes or ctx is canceled, and
// logs a summary to stderr. Stop returns an error wrapping ErrSyncFailed that
// names the files a background batch failed to sync. A file's ACK precedes its
// background sync, so this error is the only report of the failure. A drain
// that ctx cuts short warns, and returns ErrDrainIncomplete joined with the
// failures recorded before then.
//
// fsyncInterval controls behavior:
//   - 0: inline fdatasync (the sync callback blocks until fdatasync completes)
//   - >0: background batch fdatasync (enqueue to channel, batch every N bytes)
//   - <0: no per-file fdatasync
//
// If noSync is true, all per-file syncing is disabled and stop is a no-op.
func StartSyncWorker(fsyncInterval int64, noSync bool, progressInterval time.Duration, stderr io.Writer, opts ...Option) (*SyncWorker, func(context.Context) error) {
	o := applyOptions(opts)
	worker := &SyncWorker{faults: o.faults}
	noop := func(context.Context) error { return nil }
	if noSync {
		return worker, noop
	}
	switch {
	case fsyncInterval == 0:
		worker.inlineSync = true
		return worker, noop
	case fsyncInterval > 0:
		worker.background = newBackgroundSyncer(fsyncInterval, stderr, o.faults)
		return worker, func(ctx context.Context) error { return worker.background.stop(ctx, progressInterval, stderr) }
	default:
		return worker, noop
	}
}

// Unsynced returns how many distinct paths background batches failed to
// sync. Stop reports them too; this count is for callers that check after it.
func (w *SyncWorker) Unsynced() int64 {
	if w == nil || w.background == nil {
		return 0
	}
	return w.background.unsynced.Load()
}

// SyncOutput returns a sync callback for fd. The returned function is intended
// to run after all writes complete but before fd is closed.
//
// The state of fd at callback time is important: the file descriptor must still
// be valid and positioned at the final write offset for this output. offset
// must be the starting write offset for the same output segment, because the
// background sync path uses currentPosition-offset to account for written bytes.
// path names the output in stop's error if its background sync fails.
func (w *SyncWorker) SyncOutput(fd *os.File, offset int64, path string) func() error {
	if w == nil || fd == nil {
		return func() error { return nil }
	}
	return func() error {
		if w.background != nil {
			dupFD, err := sysio.Dup(w.faults, int(fd.Fd()))
			if err != nil {
				return fdatasync(w.faults, int(fd.Fd()))
			}
			pos, _ := fd.Seek(0, io.SeekCurrent)
			written := pos - offset
			if written < 0 {
				written = 0
			}
			w.background.enqueue(dupFD, written, path)
			return nil
		}
		if w.inlineSync {
			return fdatasync(w.faults, int(fd.Fd()))
		}
		return nil
	}
}

// fdatasync syncs fd behind inj's gate.
func fdatasync(inj *faults.Injector, fd int) error {
	return sysio.Fdatasync(inj, fd)
}

// SyncfsDir issues a best-effort syncfs against dir and returns when either
// syncfs completes or ctx is done. It prints the outcome to stderr and
// returns an error carrying the same text when the sync failed, timed out,
// or was canceled, so a caller can record that durability is unconfirmed.
func SyncfsDir(ctx context.Context, dir string, progressInterval time.Duration, stderr io.Writer, opts ...Option) error {
	inj := applyOptions(opts).faults
	if ctx == nil {
		ctx = context.Background()
	}
	displayPath := dir
	if absPath, err := filepath.Abs(dir); err == nil {
		displayPath = absPath
	}
	fd, err := sysio.OpenDir(dir) //nolint:forbidigo // Durability setup; the syncfs gate runs below.
	if err != nil {
		msg := fmt.Sprintf("final-syncfs: [%s] open failed: %v", displayPath, err)
		fmt.Fprintln(stderr, msg)
		return errors.New(msg)
	}
	start := time.Now()
	done := make(chan error, 1)
	// The goroutine owns fd: closing it here on a timeout could let a late
	// syncfs run on a reused descriptor.
	go func() {
		defer sysio.Close(fd)
		if err := inj.Sys(faults.SysSyncfs); err != nil {
			done <- err
			return
		}
		done <- loadSyncfsFunc()(fd)
	}()
	finished := func(err error) error {
		elapsed := time.Since(start).Round(time.Millisecond)
		if err == nil {
			fmt.Fprintf(stderr, "final-syncfs: [%s] filesystem sync completed in %s\n", displayPath, elapsed)
			return nil
		}
		msg := fmt.Sprintf("final-syncfs: [%s] filesystem sync failed after %s: %v", displayPath, elapsed, err)
		fmt.Fprintln(stderr, msg)
		return errors.New(msg)
	}
	ended := func() error {
		elapsed := time.Since(start).Round(time.Millisecond)
		msg := fmt.Sprintf("final-syncfs: [%s] filesystem sync canceled after %s: %v", displayPath, elapsed, ctx.Err())
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			msg = fmt.Sprintf("final-syncfs: [%s] filesystem sync timed out after %s. Data may not yet be durable on disk", displayPath, elapsed)
		}
		fmt.Fprintln(stderr, "WARNING: "+msg)
		return errors.New(msg)
	}
	var tick <-chan time.Time
	if progressInterval > 0 {
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case err := <-done:
			return finished(err)
		case <-ctx.Done():
			return ended()
		case <-tick:
			fmt.Fprintf(stderr, "final-syncfs: [%s] waiting %s outstanding=0 files\n", displayPath, time.Since(start).Round(time.Millisecond))
		}
	}
}

type syncfsFuncType func(int) error

var syncfsFunc atomic.Value

func init() {
	syncfsFunc.Store(syncfsFuncType(func(fd int) error {
		// SyncfsDir gates the call before reaching this test hook.
		return sysio.Syncfs(nil, fd)
	}))
}

func loadSyncfsFunc() syncfsFuncType {
	return syncfsFunc.Load().(syncfsFuncType)
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
	syncedFiles   atomic.Int64
	pendingBytes  atomic.Int64
	pendingFiles  atomic.Int64
	peakBytes     atomic.Int64
	peakFiles     atomic.Int64
	currentBatch  atomic.Int64
	peakBatch     atomic.Int64
	// unsynced counts the distinct paths whose fdatasync failed; a split
	// file syncs one descriptor per window, so it can fail more than once.
	// failedPaths keeps the first maxReportedSyncFailures for stop's error.
	unsynced    atomic.Int64
	failedMu    sync.Mutex
	failedSeen  map[string]struct{}
	failedPaths []string
	faults      *faults.Injector
}

type syncRequest struct {
	fd   int
	size int64
	path string
}

func newBackgroundSyncer(threshold int64, stderr io.Writer, inj *faults.Injector) *backgroundSyncer {
	bs := &backgroundSyncer{
		faults:        inj,
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
		// Dedupe by inode when fstat works; without it, sync the descriptor
		// anyway rather than skip a file that may share no inode.
		dev, ino, statErr := sysio.FileID(bs.faults, req.fd)
		key := fileKey{dev, ino}
		if statErr != nil || !seen[key] {
			// No retry: after a failed fdatasync the kernel may have dropped
			// the dirty pages, so a second call can succeed without the data.
			if err := fdatasync(bs.faults, req.fd); err != nil {
				bs.recordFailure(req.path)
				fmt.Fprintf(stderr, "background fdatasync: %s: %v\n", req.path, err)
			}
			if statErr == nil {
				seen[key] = true
			}
		}
		sysio.Close(req.fd)
		bs.pendingBytes.Add(-req.size)
		bs.pendingFiles.Add(-1)
		bs.synced.Add(req.size)
		bs.syncedFiles.Add(1)
	}
}

func (bs *backgroundSyncer) recordFailure(path string) {
	bs.failedMu.Lock()
	defer bs.failedMu.Unlock()
	if _, dup := bs.failedSeen[path]; dup {
		return
	}
	if bs.failedSeen == nil {
		bs.failedSeen = make(map[string]struct{})
	}
	bs.failedSeen[path] = struct{}{}
	bs.unsynced.Add(1)
	if len(bs.failedPaths) < maxReportedSyncFailures {
		bs.failedPaths = append(bs.failedPaths, path)
	}
}

// failedErr returns an error naming the files whose sync failed, or nil.
func (bs *backgroundSyncer) failedErr() error {
	bs.failedMu.Lock()
	defer bs.failedMu.Unlock()
	n := len(bs.failedSeen)
	if n == 0 {
		return nil
	}
	noun := "files"
	if n == 1 {
		noun = "file"
	}
	list := strings.Join(bs.failedPaths, ", ")
	if more := n - len(bs.failedPaths); more > 0 {
		list += fmt.Sprintf(", and %d more (see the background fdatasync lines above)", more)
	}
	return &markedError{msg: fmt.Sprintf("%d %s failed to sync and may not be durable: %s", n, noun, list), mark: ErrSyncFailed}
}

func (bs *backgroundSyncer) enqueue(fd int, size int64, path string) {
	pendingBytes := bs.pendingBytes.Add(size)
	pendingFiles := bs.pendingFiles.Add(1)
	updateMaxAtomic(&bs.peakBytes, pendingBytes)
	updateMaxAtomic(&bs.peakFiles, pendingFiles)
	bs.ch <- syncRequest{fd: fd, size: size, path: path}
}

func (bs *backgroundSyncer) snapshot() SyncSnapshot {
	return SyncSnapshot{
		PendingBytes: bs.pendingBytes.Load(),
		PendingFiles: bs.pendingFiles.Load(),
		SyncedBytes:  bs.synced.Load(),
		SyncedFiles:  bs.syncedFiles.Load(),
	}
}

func (bs *backgroundSyncer) stop(ctx context.Context, progressInterval time.Duration, stderr io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	close(bs.ch)
	start := time.Now()
	elapsed, err := waitForBackgroundSyncDrain(ctx, bs.done, bs.snapshot, progressInterval, stderr, start)
	if err != nil {
		snap := bs.snapshot()
		msg := fmt.Sprintf("background-fsync: drain canceled after %s: %v pending=%s/%d files synced=%s",
			elapsed.Round(time.Millisecond),
			err,
			encoding.HumanBytes(snap.PendingBytes),
			snap.PendingFiles,
			encoding.HumanBytes(snap.SyncedBytes))
		fmt.Fprintln(stderr, "WARNING: "+msg)
		return errors.Join(bs.failedErr(), &markedError{msg: msg, mark: ErrDrainIncomplete})
	}
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
	return bs.failedErr()
}

func waitForBackgroundSyncDrain(ctx context.Context, done <-chan struct{}, snapshot func() SyncSnapshot, progressInterval time.Duration, stderr io.Writer, start time.Time) (time.Duration, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if progressInterval <= 0 {
		select {
		case <-done:
			return time.Since(start), nil
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		}
	}
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	prevSnap := snapshot()
	prevTime := start
	for {
		select {
		case <-done:
			return time.Since(start), nil
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-ticker.C:
			snap := snapshot()
			now := time.Now()
			fmt.Fprintln(stderr, formatFsyncProgressLine(ctx, prevSnap, snap, prevTime, now))
			prevSnap = snap
			prevTime = now
		}
	}
}

func formatFsyncProgressLine(ctx context.Context, prev SyncSnapshot, snap SyncSnapshot, prevTime time.Time, now time.Time) string {
	doneFiles := clampInt64(snap.SyncedFiles, 0, snap.SyncedFiles+snap.PendingFiles)
	totalFiles := clampMinInt64(snap.SyncedFiles+snap.PendingFiles, 0)
	doneBytes := clampInt64(snap.SyncedBytes, 0, snap.SyncedBytes+snap.PendingBytes)
	totalBytes := clampMinInt64(snap.SyncedBytes+snap.PendingBytes, 0)

	var pctFiles float64
	if totalFiles > 0 {
		pctFiles = clampPercent(float64(doneFiles) * 100 / float64(totalFiles))
	}
	var pctBytes float64
	if totalBytes > 0 {
		pctBytes = clampPercent(float64(doneBytes) * 100 / float64(totalBytes))
	}

	etaDisplay := fixedWidthFsyncDurationNA()
	if remainingBytes := totalBytes - doneBytes; remainingBytes > 0 {
		if dt := now.Sub(prevTime).Seconds(); dt > 0 {
			rateBps := float64(snap.SyncedBytes-prev.SyncedBytes) / dt
			if rateBps > 0 {
				etaDisplay = fixedWidthFsyncDuration(time.Duration(float64(remainingBytes) / rateBps * float64(time.Second)))
			}
		}
	}
	budgetDisplay := fixedWidthFsyncDurationNA()
	if deadline, ok := ctx.Deadline(); ok {
		budgetDisplay = fixedWidthFsyncDuration(deadline.Sub(now))
	}

	return fmt.Sprintf(
		"fsync-progress:[%6s/%6s](%5.1f%%) [%s/%s](%5.1f%%) [eta:%s]@[budget:%s]",
		encoding.HumanCount(uint64(doneFiles), fsyncProgressCountWidth),
		encoding.HumanCount(uint64(totalFiles), fsyncProgressCountWidth),
		pctFiles,
		encoding.HumanBytesFixedWidth(doneBytes, fsyncProgressBytesWidth),
		encoding.HumanBytesFixedWidth(totalBytes, fsyncProgressBytesWidth),
		pctBytes,
		etaDisplay,
		budgetDisplay,
	)
}

func fixedWidthFsyncDuration(d time.Duration) string {
	return fmt.Sprintf("%*s", fsyncProgressDurationWidth, compactFsyncDuration(d))
}

func fixedWidthFsyncDurationNA() string {
	return fmt.Sprintf("%*s", fsyncProgressDurationWidth, "n/a")
}

func compactFsyncDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	seconds := d.Round(time.Second).Seconds()
	switch {
	case seconds < 60:
		return fmt.Sprintf("%.0fs", seconds)
	case seconds < 60*60:
		return fmt.Sprintf("%.1fm", seconds/60)
	case seconds < 24*60*60:
		return fmt.Sprintf("%.1fh", seconds/3600)
	case seconds < 7*24*60*60:
		return fmt.Sprintf("%.1fd", seconds/(24*3600))
	default:
		return fmt.Sprintf("%.1fw", seconds/(7*24*3600))
	}
}

func clampPercent(pct float64) float64 {
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

func clampMinInt64(v int64, min int64) int64 {
	if v < min {
		return min
	}
	return v
}

func clampInt64(v int64, min int64, max int64) int64 {
	if max < min {
		max = min
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func (bs *backgroundSyncer) updateBatchThreshold(threshold int64, stderr io.Writer) {
	for {
		current := bs.currentBatch.Load()
		if current == threshold {
			updateMaxAtomic(&bs.peakBatch, threshold)
			return
		}
		if !bs.currentBatch.CompareAndSwap(current, threshold) {
			continue
		}
		peakUpdated := updateMaxAtomic(&bs.peakBatch, threshold)
		if threshold > current && peakUpdated && stderr != nil {
			fmt.Fprintf(stderr, "background-fsync: disk falling behind, growing batch threshold %s -> %s pending=%s cap=%s\n",
				encoding.HumanBytes(current),
				encoding.HumanBytes(threshold),
				encoding.HumanBytes(bs.pendingBytes.Load()),
				encoding.HumanBytes(bs.maxThreshold))
		}
		return
	}
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
	total, err := sysio.TotalMemoryBytes()
	if err != nil || total == 0 || uint64(total) > uint64(^uint(0)>>1) {
		return 0
	}
	return total
}

func updateMaxAtomic(dst *atomic.Int64, candidate int64) bool {
	for {
		current := dst.Load()
		if candidate <= current {
			return false
		}
		if dst.CompareAndSwap(current, candidate) {
			return true
		}
	}
}
