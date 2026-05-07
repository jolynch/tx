package fsync

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartSyncWorkerDisabled(t *testing.T) {
	worker, stop := StartSyncWorker(1024, true, time.Second, ioDiscard{})
	stop(context.Background())
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
	if err := worker.SyncOutput(f, 0)(); err != nil {
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
	syncOutput := worker.SyncOutput(f, 0)
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
	syncOutput := worker.SyncOutput(f, 0)
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := syncOutput(); err != nil {
		t.Fatalf("SyncOutput() error = %v, want nil", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stop(context.Background())

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
	syncOutput := worker.SyncOutput(f, 0)
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
