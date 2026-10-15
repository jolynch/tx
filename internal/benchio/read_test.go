package benchio

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestReadLatency(t *testing.T) {
	reader := strings.NewReader("data")
	if got := WithReadLatency(context.Background(), "client", reader); got != reader {
		t.Fatal("wrapped the production reader")
	}
	var calls int
	restore := SetReadLatency(func(ctx context.Context, side string, n int) error {
		calls++
		if side != "server" || n != 4 {
			t.Fatalf("side=%s bytes=%d", side, n)
		}
		return ctx.Err()
	})
	t.Cleanup(restore)
	var data [4]byte
	n, err := WithReadLatency(context.Background(), "server", reader).ReadAt(data[:], 0)
	if n != 4 || err != nil || string(data[:]) != "data" || calls != 1 {
		t.Fatalf("n=%d err=%v calls=%d", n, err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err = WithReadLatency(ctx, "server", reader).ReadAt(data[:], 0)
	if n != 0 || err != context.Canceled {
		t.Fatalf("n=%d err=%v", n, err)
	}
	restore()
	if got := WithReadLatency(context.Background(), "client", reader); got != reader {
		t.Fatal("hook not restored")
	}
	if _, err := WithReadLatency(context.Background(), "client", reader).ReadAt(data[:], 4); err != io.EOF {
		t.Fatalf("err=%v", err)
	}
}
