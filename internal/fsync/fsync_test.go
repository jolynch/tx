package fsync

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestStartSyncWorkerDisabled(t *testing.T) {
	worker, stop := StartSyncWorker(1024, true, ioDiscard{})
	stop()
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
	worker, stop := StartSyncWorker(0, false, ioDiscard{})
	defer stop()

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
	worker, stop := StartSyncWorker(1, false, &stderr)

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

	stop()

	if !strings.Contains(stderr.String(), "background-fsync: drained") {
		t.Fatalf("stop output = %q, want background drain summary", stderr.String())
	}
	if !strings.Contains(stderr.String(), "peak=") {
		t.Fatalf("stop output = %q, want backlog telemetry", stderr.String())
	}
}

func TestSyncOutputBackgroundDupFallbackReturnsError(t *testing.T) {
	worker, stop := StartSyncWorker(1, false, ioDiscard{})
	defer stop()

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
	SyncfsDir(ctx, t.TempDir(), &stderr)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SyncfsDir took %s, want prompt return", elapsed)
	}
	if got := stderr.String(); !strings.Contains(got, "syncfs:") && !strings.Contains(got, "WARNING: syncfs") {
		t.Fatalf("SyncfsDir log = %q, want syncfs log output", got)
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
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
