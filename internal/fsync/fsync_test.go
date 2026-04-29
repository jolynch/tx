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

	if !strings.Contains(stderr.String(), "background-sync: drained") {
		t.Fatalf("stop output = %q, want background drain summary", stderr.String())
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

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
