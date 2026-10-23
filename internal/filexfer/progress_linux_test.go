//go:build linux

package filexfer

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A child bounds a regression that opens a FIFO without O_NONBLOCK. Its
// writer must return even when no process has the FIFO open for reading.
func TestProgressFIFOWithoutReader(t *testing.T) {
	const env = "TX_PROGRESS_FIFO_CHILD"
	if os.Getenv(env) != "" {
		path := filepath.Join(t.TempDir(), "progress")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatInt}}, time.Hour, func() ProgressStatus { return ProgressStatus{DoneBytes: 42, TotalBytes: 100} })
		stop(false)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProgressFIFOWithoutReader$")
	cmd.Env = append(os.Environ(), env+"=1", "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("progress FIFO open blocked without a reader: %s", out)
	}
	if err != nil {
		t.Fatalf("progress FIFO child: %v\n%s", err, out)
	}
}

func TestProgressFIFOWithReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	for _, pct := range []int64{42, 64} {
		stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatInt}}, time.Hour, func() ProgressStatus { return ProgressStatus{DoneBytes: pct, TotalBytes: 100} })
		stop(false)
	}
	var buf [32]byte
	n, err := unix.Read(fd, buf[:])
	if err != nil || string(buf[:n]) != "42\n64\n" {
		t.Fatalf("FIFO progress = %q, %v", buf[:max(n, 0)], err)
	}
}
