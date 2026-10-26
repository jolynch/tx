package fsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/faults"
)

func TestStartSyncWorkerDisabled(t *testing.T) {
	worker, stop := StartSyncWorker(1024, true, time.Second, ioDiscard{})
	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop() error = %v, want nil", err)
	}
	if worker == nil {
		t.Fatal("StartSyncWorker returned nil worker")
	}
	f, err := os.CreateTemp(t.TempDir(), "sync-disabled-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := worker.SyncOutput(f, 0, f.Name())(); err != nil {
		t.Fatalf("SyncOutput() error = %v, want nil", err)
	}
}

func TestSyncOutputInlineReturnsFdatasyncError(t *testing.T) {
	worker, stop := StartSyncWorker(0, false, time.Second, ioDiscard{})
	defer stop(context.Background())

	f, err := os.CreateTemp(t.TempDir(), "sync-inline-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	syncOutput := worker.SyncOutput(f, 0, f.Name())
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := syncOutput(); err == nil {
		t.Fatal("SyncOutput() error = nil, want non-nil")
	}
}

func TestSyncOutputBackgroundStopDrains(t *testing.T) {
	var stderr bytes.Buffer
	worker, stop := StartSyncWorker(1, false, time.Second, &stderr)

	f, err := os.CreateTemp(t.TempDir(), "sync-background-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	syncOutput := worker.SyncOutput(f, 0, f.Name())
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := syncOutput(); err != nil {
		t.Fatalf("SyncOutput() error = %v, want nil", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop() error = %v, want nil after every sync succeeded", err)
	}

	if !strings.Contains(stderr.String(), "background-fsync: drained") {
		t.Fatalf("stop output = %q, want background drain summary", stderr.String())
	}
	if !strings.Contains(stderr.String(), "peak=") {
		t.Fatalf("stop output = %q, want backlog telemetry", stderr.String())
	}
}

func TestSyncOutputBackgroundDupFallbackReturnsError(t *testing.T) {
	worker, stop := StartSyncWorker(1, false, time.Second, ioDiscard{})
	defer stop(context.Background())

	f, err := os.CreateTemp(t.TempDir(), "sync-background-fallback-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	syncOutput := worker.SyncOutput(f, 0, f.Name())
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := syncOutput(); err == nil {
		t.Fatal("SyncOutput() error = nil, want non-nil")
	}
}

func TestSyncfsDirCanceledContextReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	var stderr bytes.Buffer
	SyncfsDir(ctx, t.TempDir(), time.Second, &stderr)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SyncfsDir took %s, want prompt return", elapsed)
	}
	if got := stderr.String(); !strings.Contains(got, "syncfs:") && !strings.Contains(got, "WARNING: syncfs") {
		t.Fatalf("SyncfsDir log = %q, want syncfs log output", got)
	}
}

func TestBackgroundSyncDrainHeartbeatsWhileWaiting(t *testing.T) {
	done := make(chan struct{})
	var stderr bytes.Buffer
	go func() {
		time.Sleep(25 * time.Millisecond)
		close(done)
	}()

	elapsed, err := waitForBackgroundSyncDrain(context.Background(), done, func() SyncSnapshot {
		return SyncSnapshot{
			PendingBytes: 1024,
			PendingFiles: 3,
			SyncedBytes:  4096,
			SyncedFiles:  4,
		}
	}, 10*time.Millisecond, &stderr, time.Now())
	if err != nil {
		t.Fatalf("waitForBackgroundSyncDrain error = %v, want nil", err)
	}

	if elapsed < 20*time.Millisecond {
		t.Fatalf("wait elapsed = %s, want at least one heartbeat interval", elapsed)
	}
	got := stderr.String()
	if !strings.Contains(got, "fsync-progress:") {
		t.Fatalf("heartbeat output = %q, want fsync progress heartbeat", got)
	}
	if !strings.Contains(got, "[     4/     7]( 57.1%)") {
		t.Fatalf("heartbeat output = %q, want fixed file progress bracket", got)
	}
	if !strings.Contains(got, "[  4.00 KiB/  5.00 KiB]( 80.0%)") {
		t.Fatalf("heartbeat output = %q, want fixed byte progress bracket", got)
	}
	if !strings.Contains(got, "[eta:") || !strings.Contains(got, "@[budget:  n/a]") {
		t.Fatalf("heartbeat output = %q, want eta and n/a budget fields", got)
	}
}

func TestFormatFsyncProgressLineComputesETAAndBudget(t *testing.T) {
	now := time.Unix(100, 0)
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(55*time.Second))
	defer cancel()

	got := formatFsyncProgressLine(
		ctx,
		SyncSnapshot{SyncedBytes: 1024, SyncedFiles: 1, PendingBytes: 4096, PendingFiles: 5},
		SyncSnapshot{SyncedBytes: 2048, SyncedFiles: 2, PendingBytes: 3072, PendingFiles: 4},
		now.Add(-time.Second),
		now,
	)
	want := "fsync-progress:[     2/     6]( 33.3%) [  2.00 KiB/  5.00 KiB]( 40.0%) [eta:   3s]@[budget:  55s]"
	if got != want {
		t.Fatalf("formatFsyncProgressLine() = %q, want %q", got, want)
	}
}

func TestBackgroundSyncDrainStopsOnContextCancel(t *testing.T) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var stderr bytes.Buffer
	start := time.Now()
	cancel()

	elapsed, err := waitForBackgroundSyncDrain(ctx, done, func() SyncSnapshot {
		return SyncSnapshot{
			PendingBytes: 2048,
			PendingFiles: 7,
			SyncedBytes:  8192,
			SyncedFiles:  9,
		}
	}, time.Hour, &stderr, start)

	if err == nil {
		t.Fatal("waitForBackgroundSyncDrain error = nil, want context cancellation")
	}
	if elapsed > time.Second {
		t.Fatalf("wait elapsed = %s, want prompt cancellation", elapsed)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("heartbeat output = %q, want no heartbeat before cancellation", got)
	}
}

func TestSyncfsDirHeartbeatsWhileWaiting(t *testing.T) {
	prevSyncfs := loadSyncfsFunc()
	started := make(chan struct{})
	release := make(chan struct{})
	syncfsFunc.Store(syncfsFuncType(func(fd int) error {
		close(started)
		<-release
		return nil
	}))
	defer func() {
		syncfsFunc.Store(prevSyncfs)
	}()

	var stderr bytes.Buffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		SyncfsDir(context.Background(), t.TempDir(), 10*time.Millisecond, &stderr)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for syncfs to start")
	}
	time.Sleep(25 * time.Millisecond)
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SyncfsDir to return")
	}

	got := stderr.String()
	if !strings.Contains(got, "final-syncfs:") || !strings.Contains(got, "waiting") {
		t.Fatalf("SyncfsDir output = %q, want waiting heartbeat", got)
	}
	if !strings.Contains(got, "outstanding=0 files") {
		t.Fatalf("SyncfsDir output = %q, want outstanding file count", got)
	}
}

func TestAdaptiveThresholdDoublesWithBacklog(t *testing.T) {
	const base = 512
	const capBytes = 4096

	tests := []struct {
		name    string
		pending int64
		want    int64
	}{
		{name: "below base", pending: 511, want: 512},
		{name: "just above base", pending: 513, want: 1024},
		{name: "midway", pending: 2500, want: 4096},
		{name: "above cap", pending: 20000, want: 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := adaptiveThreshold(base, tt.pending, capBytes); got != tt.want {
				t.Fatalf("adaptiveThreshold(%d, %d, %d) = %d, want %d", base, tt.pending, capBytes, got, tt.want)
			}
		})
	}
}

func TestAdaptiveThresholdNeverDropsBelowBase(t *testing.T) {
	if got := adaptiveThreshold(1024, 10<<20, 512); got != 1024 {
		t.Fatalf("adaptiveThreshold should clamp to base threshold, got %d", got)
	}
}

func TestMaxAdaptiveThresholdForMemoryFallbackCap(t *testing.T) {
	const fallbackCap = 8 << 30

	if got := maxAdaptiveThresholdForMemory(512, 0); got != fallbackCap {
		t.Fatalf("maxAdaptiveThresholdForMemory fallback = %d, want %d", got, fallbackCap)
	}
	if got := maxAdaptiveThresholdForMemory(9<<30, 0); got != 9<<30 {
		t.Fatalf("maxAdaptiveThresholdForMemory should never shrink below base threshold, got %d", got)
	}
}

func TestUpdateBatchThresholdLogsGrowthOnly(t *testing.T) {
	bs := &backgroundSyncer{
		baseThreshold: 1024,
		maxThreshold:  8192,
	}
	bs.currentBatch.Store(1024)
	bs.pendingBytes.Store(4096)

	var stderr bytes.Buffer
	bs.updateBatchThreshold(2048, &stderr)
	if got := stderr.String(); !strings.Contains(got, "growing batch threshold 1.00 KiB -> 2.00 KiB") {
		t.Fatalf("growth log = %q, want threshold growth message", got)
	}

	stderr.Reset()
	bs.updateBatchThreshold(1024, &stderr)
	if got := stderr.String(); got != "" {
		t.Fatalf("shrink log = %q, want no output", got)
	}

	bs.updateBatchThreshold(2048, &stderr)
	if got := stderr.String(); got != "" {
		t.Fatalf("repeated growth log = %q, want no output for previous peak", got)
	}

	bs.updateBatchThreshold(4096, &stderr)
	if got := stderr.String(); !strings.Contains(got, "growing batch threshold 2.00 KiB -> 4.00 KiB") {
		t.Fatalf("new peak growth log = %q, want threshold growth message", got)
	}
}

func TestUpdateBatchThresholdConcurrentGrowthLogsOnce(t *testing.T) {
	bs := &backgroundSyncer{
		baseThreshold: 1024,
		maxThreshold:  8192,
	}
	bs.currentBatch.Store(1024)
	bs.pendingBytes.Store(4096)

	writer := &blockingWriter{
		firstStarted: make(chan struct{}),
		release:      make(chan struct{}),
	}
	const goroutines = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			bs.updateBatchThreshold(2048, writer)
		}()
	}

	close(start)
	select {
	case <-writer.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first growth log")
	}
	time.Sleep(10 * time.Millisecond)
	close(writer.release)
	wg.Wait()

	if got := writer.writes.Load(); got != 1 {
		t.Fatalf("growth log writes = %d, want 1", got)
	}
	if got := bs.currentBatch.Load(); got != 2048 {
		t.Fatalf("currentBatch = %d, want 2048", got)
	}
}

type blockingWriter struct {
	writes       atomic.Int64
	firstStarted chan struct{}
	release      chan struct{}
	once         sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	block := false
	w.writes.Add(1)
	w.once.Do(func() {
		block = true
		close(w.firstStarted)
	})
	if block {
		<-w.release
	}
	return len(p), nil
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

// TestSyncSyscallsGoThroughFaults checks the inline and background sync
// paths, and syncfs, ask the injector's gates.
func TestSyncSyscallsGoThroughFaults(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		interval int64
		want     []faults.Syscall
	}{
		{"inline", 0, []faults.Syscall{faults.SysFdatasync}},
		{"background", 1, []faults.Syscall{faults.SysDup, faults.SysFstat, faults.SysFdatasync}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj := faults.New(faults.Config{Seed: 1})
			worker, stop := StartSyncWorker(tc.interval, false, 0, io.Discard, WithFaults(inj))
			if err := worker.SyncOutput(f, 0, f.Name())(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := stop(ctx); err != nil {
				t.Fatalf("stop() error = %v, want nil", err)
			}
			for _, sc := range tc.want {
				if inj.Counts().Sys[sc] == 0 {
					t.Fatalf("syscall %d skipped its gate: %+v", sc, inj.Counts().Sys)
				}
			}
		})
	}
	inj := faults.New(faults.Config{Seed: 1})
	SyncfsDir(context.Background(), t.TempDir(), 0, io.Discard, WithFaults(inj))
	if inj.Counts().Sys[faults.SysSyncfs] == 0 {
		t.Fatal("syncfs skipped its gate")
	}
}

// TestBackgroundSyncBatchSyncsEveryFile checks a passing fstat gate still
// lets the batch tell files apart: two files in one batch are both synced.
func TestBackgroundSyncBatchSyncsEveryFile(t *testing.T) {
	inj := faults.New(faults.Config{Seed: 1})
	worker, stop := StartSyncWorker(1<<30, false, 0, io.Discard, WithFaults(inj))
	dir := t.TempDir()
	for _, name := range []string{"a", "b"} {
		f, err := os.Create(dir + "/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(name); err != nil {
			t.Fatal(err)
		}
		if err := worker.SyncOutput(f, 0, f.Name())(); err != nil {
			t.Fatal(err)
		}
		defer f.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		t.Fatalf("stop() error = %v, want nil", err)
	}
	if got := inj.Counts().Sys[faults.SysFdatasync]; got != 2 {
		t.Fatalf("fdatasync gate asked %d times for two files in one batch, want 2", got)
	}
}

// TestBackgroundSyncReportsFailedFiles checks the background syncer's
// failure contract, for three files in one batch and in a batch each:
//
//   - A failed fdatasync is not retried. It counts in Unsynced, and stop
//     returns an error that wraps ErrSyncFailed, not ErrDrainIncomplete,
//     and names each failed file's path and the count.
//   - A failed fstat costs only deduplication: the file is still synced, so
//     with fdatasync healthy nothing is unsynced and stop returns nil.
//   - The sync callback returns nil either way, since the file's writer
//     has moved on by the time its batch runs.
func TestBackgroundSyncReportsFailedFiles(t *testing.T) {
	const files = 3
	for _, tc := range []struct {
		spec string
		fail bool
	}{
		{"", false},
		{"fdatasync.fail=1", true},
		{"fstat.fail=1", false},
		{"fstat.fail=1,fdatasync.fail=1", true},
	} {
		for _, interval := range []int64{1, 1 << 30} {
			t.Run(fmt.Sprintf("%q/interval=%d", tc.spec, interval), func(t *testing.T) {
				inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: tc.spec}})
				var stderr bytes.Buffer
				worker, stop := StartSyncWorker(interval, false, 0, &stderr, WithFaults(inj))
				dir := t.TempDir()
				var paths []string
				for i := range files {
					path := fmt.Sprintf("%s/out-%c.bin", dir, 'a'+i)
					f, err := os.Create(path)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.WriteString("hello"); err != nil {
						t.Fatal(err)
					}
					if err := worker.SyncOutput(f, 0, path)(); err != nil {
						t.Fatalf("background SyncOutput(%s) = %v, want nil", path, err)
					}
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
					paths = append(paths, path)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := stop(ctx)

				if got := inj.Counts().Sys[faults.SysFdatasync]; got != files {
					t.Fatalf("fdatasync gate asked %d times for %d files, want one each: no skip, no retry", got, files)
				}
				var wantUnsynced int64
				if tc.fail {
					wantUnsynced = files
				}
				if got := worker.Unsynced(); got != wantUnsynced {
					t.Fatalf("Unsynced() = %d, want %d", got, wantUnsynced)
				}
				if !tc.fail {
					if err != nil {
						t.Fatalf("stop() = %v, want nil when every file synced", err)
					}
					return
				}
				if err == nil {
					t.Fatalf("stop() = nil, want an error naming %d unsynced files; stderr %q", files, stderr.String())
				}
				if !errors.Is(err, ErrSyncFailed) {
					t.Fatalf("stop() = %v, want it to wrap ErrSyncFailed", err)
				}
				if errors.Is(err, ErrDrainIncomplete) {
					t.Fatalf("stop() = %v wraps ErrDrainIncomplete, but the drain finished", err)
				}
				msg := err.Error()
				rest := msg
				for _, path := range paths {
					if !strings.Contains(msg, path) {
						t.Fatalf("stop() error %q does not name unsynced file %s", msg, path)
					}
					rest = strings.ReplaceAll(rest, path, "")
				}
				if !regexp.MustCompile(fmt.Sprintf(`\b%d\b`, files)).MatchString(rest) {
					t.Fatalf("stop() error %q does not give the count %d", msg, files)
				}
			})
		}
	}
}

// slowFdatasync is the latency of a slow fdatasync: long enough that a stop
// whose context is already done cannot see its batch finish, short enough
// that the abandoned batch ends soon after the test.
const slowFdatasync = "β(1s,1100ms,1400ms,1500ms)"

// fastThenSlowSeed returns a seed under which, with fdatasync latency firing
// at rate, the first fdatasync does not sleep and the second does. A
// fault's decision depends on the seed, kind, rate, and operation number,
// not its distribution, so a probe with a negligible latency predicts the
// real injector.
func fastThenSlowSeed(t *testing.T, rate string) uint64 {
	t.Helper()
	for seed := uint64(1); seed <= 256; seed++ {
		probe := faults.New(faults.Config{Seed: seed, Specs: map[faults.Family]string{
			faults.FamilySys: "fdatasync.latency=" + rate + ":β(1us,2us,3us,4us)",
		}})
		var slow []bool
		for range 2 {
			before := probe.Counts().InjectedByKind()["sys.fdatasync.latency"]
			_ = probe.Sys(faults.SysFdatasync)
			slow = append(slow, probe.Counts().InjectedByKind()["sys.fdatasync.latency"] > before)
		}
		if !slow[0] && slow[1] {
			return seed
		}
	}
	t.Fatal("no seed in 1..256 makes the first fdatasync fast and the second slow")
	return 0
}

// TestStopDrainCutShort checks stop's error when its context is done before
// the background drain finishes: it wraps ErrDrainIncomplete, and it wraps
// ErrSyncFailed only when a file failed to sync before the cut, in which
// case both hold. Stop returns without waiting for the slow sync. Its
// batch cannot finish before stop looks, since every slow fdatasync sleeps
// at least a second.
func TestStopDrainCutShort(t *testing.T) {
	// enqueue writes a 4 KiB file, a power of two, so a 1-byte interval
	// sends it as a batch of its own unless other bytes are pending.
	enqueue := func(t *testing.T, worker *SyncWorker, path string) {
		t.Helper()
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		if _, err := f.Write(make([]byte, 4096)); err != nil {
			t.Fatal(err)
		}
		if err := worker.SyncOutput(f, 0, path)(); err != nil {
			t.Fatalf("background SyncOutput(%s) = %v, want nil", path, err)
		}
	}
	stopNow := func(stop func(context.Context) error) error {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return stop(ctx)
	}

	t.Run("pending only", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{
			faults.FamilySys: "fdatasync.latency=1:" + slowFdatasync,
		}})
		worker, stop := StartSyncWorker(1, false, 0, io.Discard, WithFaults(inj))
		enqueue(t, worker, t.TempDir()+"/pending.bin")
		start := time.Now()
		err := stopNow(stop)
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("stop took %s with a done context, want it to return without the drain", elapsed)
		}
		if !errors.Is(err, ErrDrainIncomplete) {
			t.Fatalf("stop() = %v, want it to wrap ErrDrainIncomplete", err)
		}
		if errors.Is(err, ErrSyncFailed) {
			t.Fatalf("stop() = %v wraps ErrSyncFailed, but no fdatasync failed", err)
		}
	})
	t.Run("pending and failed", func(t *testing.T) {
		const rate = "0.5"
		inj := faults.New(faults.Config{Seed: fastThenSlowSeed(t, rate), Specs: map[faults.Family]string{
			faults.FamilySys: "fdatasync.fail=1,fdatasync.latency=" + rate + ":" + slowFdatasync,
		}})
		worker, stop := StartSyncWorker(1, false, 0, io.Discard, WithFaults(inj))
		dir := t.TempDir()
		failed := dir + "/failed.bin"
		enqueue(t, worker, failed)
		deadline := time.Now().Add(time.Second)
		for worker.Unsynced() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("the first fdatasync, which takes no latency, did not fail within 1s")
			}
			time.Sleep(time.Millisecond)
		}
		enqueue(t, worker, dir+"/pending.bin")
		start := time.Now()
		err := stopNow(stop)
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("stop took %s with a done context, want it to return without the drain", elapsed)
		}
		if !errors.Is(err, ErrDrainIncomplete) {
			t.Fatalf("stop() = %v, want it to wrap ErrDrainIncomplete", err)
		}
		if !errors.Is(err, ErrSyncFailed) {
			t.Fatalf("stop() = %v, want it to wrap ErrSyncFailed too", err)
		}
		if !strings.Contains(err.Error(), failed) {
			t.Fatalf("stop() = %v, want it to name %s, which failed to sync", err, failed)
		}
	})
}

// TestSyncfsDirReturnsError checks SyncfsDir returns nil only when syncfs
// completes: a failed syncfs, a timeout, and a cancellation each return an
// error, whether or not it reports progress while waiting. A blocked syncfs
// stands in for a slow one, so the context always ends first.
func TestSyncfsDirReturnsError(t *testing.T) {
	prev := loadSyncfsFunc()
	defer syncfsFunc.Store(prev)
	for _, progress := range []time.Duration{0, 5 * time.Millisecond} {
		t.Run(fmt.Sprintf("progress=%s", progress), func(t *testing.T) {
			t.Run("healthy", func(t *testing.T) {
				syncfsFunc.Store(prev)
				inj := faults.New(faults.Config{Seed: 1})
				if err := SyncfsDir(context.Background(), t.TempDir(), progress, io.Discard, WithFaults(inj)); err != nil {
					t.Fatalf("SyncfsDir = %v, want nil", err)
				}
			})
			t.Run("injected failure", func(t *testing.T) {
				syncfsFunc.Store(prev)
				inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "syncfs.fail=1"}})
				var stderr bytes.Buffer
				if err := SyncfsDir(context.Background(), t.TempDir(), progress, &stderr, WithFaults(inj)); err == nil {
					t.Fatalf("SyncfsDir = nil after syncfs failed; stderr %q", stderr.String())
				}
				if !strings.Contains(stderr.String(), "filesystem sync failed") {
					t.Fatalf("SyncfsDir output = %q, want the failure on stderr", stderr.String())
				}
			})
			t.Run("slow failure", func(t *testing.T) {
				// The error arrives after a progress tick, when progress is on.
				syncfsFunc.Store(syncfsFuncType(func(int) error {
					time.Sleep(20 * time.Millisecond)
					return syscall.EIO
				}))
				if err := SyncfsDir(context.Background(), t.TempDir(), progress, io.Discard); err == nil {
					t.Fatal("SyncfsDir = nil after syncfs returned EIO")
				}
			})
			for _, tc := range []struct {
				name string
				ctx  func() (context.Context, context.CancelFunc)
				want string
			}{
				{"expired", func() (context.Context, context.CancelFunc) {
					return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				}, "timed out"},
				{"times out waiting", func() (context.Context, context.CancelFunc) {
					return context.WithTimeout(context.Background(), 30*time.Millisecond)
				}, "timed out"},
				{"canceled", func() (context.Context, context.CancelFunc) {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx, cancel
				}, "canceled"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					release := make(chan struct{})
					defer close(release)
					syncfsFunc.Store(syncfsFuncType(func(int) error {
						<-release
						return nil
					}))
					ctx, cancel := tc.ctx()
					defer cancel()
					var stderr bytes.Buffer
					if err := SyncfsDir(ctx, t.TempDir(), progress, &stderr); err == nil {
						t.Fatalf("SyncfsDir = nil though its context ended first; stderr %q", stderr.String())
					}
					if got := stderr.String(); !strings.Contains(got, "WARNING: final-syncfs") || !strings.Contains(got, tc.want) {
						t.Fatalf("SyncfsDir output = %q, want a WARNING saying %q", got, tc.want)
					}
				})
			}
		})
	}
}

// TestStopReturnsNilWithoutBackgroundSyncer checks the modes with no
// background syncer return a stop that reports nothing, even when every
// fdatasync fails: inline sync returns its error from the callback instead,
// and the other modes never call fdatasync per file.
func TestStopReturnsNilWithoutBackgroundSyncer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		interval   int64
		noSync     bool
		wantSyncFn bool // the callback returns the injected error
	}{
		{"inline", 0, false, true},
		{"disabled", -1, false, false},
		{"no-sync", 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "fdatasync.fail=1"}})
			worker, stop := StartSyncWorker(tc.interval, tc.noSync, 0, io.Discard, WithFaults(inj))
			f, err := os.CreateTemp(t.TempDir(), "out")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.WriteString("hello"); err != nil {
				t.Fatal(err)
			}
			if err := worker.SyncOutput(f, 0, f.Name())(); (err != nil) != tc.wantSyncFn {
				t.Fatalf("SyncOutput() = %v, want error %t", err, tc.wantSyncFn)
			}
			if err := stop(context.Background()); err != nil {
				t.Fatalf("stop() = %v, want nil", err)
			}
			if got := worker.Unsynced(); got != 0 {
				t.Fatalf("Unsynced() = %d, want 0", got)
			}
		})
	}
}

func TestSyncOutputInjectedFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval int64
		spec     string
		wantDup  uint64
	}{
		{"inline", 0, "fdatasync.fail=1", 0},
		{"dup fallback", 1, "dup.fail=1,fdatasync.fail=1", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: tc.spec}})
			worker, stop := StartSyncWorker(tc.interval, false, 0, io.Discard, WithFaults(inj))
			defer stop(context.Background())
			f, err := os.CreateTemp(t.TempDir(), "out")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			err = worker.SyncOutput(f, 0, f.Name())()
			if !errors.Is(err, syscall.EIO) && !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("SyncOutput = %v, want injected fdatasync error", err)
			}
			counts := inj.Counts().Sys
			if counts[faults.SysDup] != tc.wantDup || counts[faults.SysFdatasync] != 1 || counts[faults.SysFstat] != 0 {
				t.Fatalf("unexpected syscall counts: %v", counts)
			}
		})
	}
}

func TestSyncfsDirInjectedFailureSkipsSync(t *testing.T) {
	prev := loadSyncfsFunc()
	defer syncfsFunc.Store(prev)
	var calls atomic.Int64
	syncfsFunc.Store(syncfsFuncType(func(int) error {
		calls.Add(1)
		return nil
	}))
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "syncfs.fail=1"}})
	var stderr bytes.Buffer
	SyncfsDir(context.Background(), t.TempDir(), 0, &stderr, WithFaults(inj))
	if calls.Load() != 0 {
		t.Fatal("syncfs ran after its gate failed")
	}
	if !strings.Contains(stderr.String(), "filesystem sync failed") {
		t.Fatalf("SyncfsDir output = %q, want failure", stderr.String())
	}
	if got := inj.Counts().Sys[faults.SysSyncfs]; got != 1 {
		t.Fatalf("syncfs gate asked %d times, want 1", got)
	}
}

func TestDefaultSyncfsRejectsInvalidDescriptor(t *testing.T) {
	if err := loadSyncfsFunc()(-1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("default syncfs(-1) = %v, want EBADF", err)
	}
}

func TestBackgroundSyncBatchDeduplicatesAndCloses(t *testing.T) {
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprint(injected), func(t *testing.T) {
			var inj *faults.Injector
			if injected {
				inj = faults.New(faults.Config{Seed: 1})
			}
			f, err := os.CreateTemp(t.TempDir(), "out")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			bs := &backgroundSyncer{faults: inj}
			var batch []syncRequest
			for range 2 {
				fd, err := syscall.Dup(int(f.Fd()))
				if err != nil {
					t.Fatal(err)
				}
				batch = append(batch, syncRequest{fd: fd, size: 3})
			}
			bs.pendingBytes.Store(6)
			bs.pendingFiles.Store(2)
			bs.syncBatch(batch, io.Discard)
			for _, req := range batch {
				var stat syscall.Stat_t
				if err := syscall.Fstat(req.fd, &stat); !errors.Is(err, syscall.EBADF) {
					syscall.Close(req.fd)
					t.Errorf("batch descriptor %d remains open: %v", req.fd, err)
				}
			}
			if got := bs.snapshot(); got != (SyncSnapshot{SyncedBytes: 6, SyncedFiles: 2}) || bs.unsynced.Load() != 0 {
				t.Fatalf("batch did not drain: snapshot=%+v unsynced=%d", got, bs.unsynced.Load())
			}
			if inj != nil {
				counts := inj.Counts().Sys
				if counts[faults.SysFstat] != 2 || counts[faults.SysFdatasync] != 1 {
					t.Fatalf("two aliases should sync once: %v", counts)
				}
			}
		})
	}
}
