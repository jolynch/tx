//go:build linux

// Linux only: the benchmark reads from a memfd (unix.MemfdCreate), and the
// zero-copy path it compares is tee/splice. The _linux file name already
// implies this constraint; the line above states it where a reader sees it.

package ftcp

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

// BenchmarkSendFramePayload compares the zero-copy and buffered frame paths
// for one whole-file frame per op, the shape of a small-file SEND. The source
// is a memfd and the sink a loopback socket drained to io.Discard, so it
// measures per-frame CPU and syscalls without disk I/O.
func BenchmarkSendFramePayload(b *testing.B) {
	paths := []struct {
		name   string
		stream func(*os.File, *int64, frameStreamArgs) (frameStreamStats, error)
	}{
		{"zerocopy", streamFramePayloadZeroCopy},
		{"buffered", streamFramePayloadBuffered},
	}
	sizes := []struct {
		name string
		size int
	}{
		{"4KiB", 4 << 10},
		{"16KiB", 16 << 10},
		{"64KiB", 64 << 10},
		{"256KiB", 256 << 10},
		{"1MiB", 1 << 20},
		{"4MiB", 4 << 20},
	}
	for _, size := range sizes {
		src := memfdWithPayload(b, zeroCopyPayload(size.size))
		for _, path := range paths {
			b.Run(path.name+"/"+size.name, func(b *testing.B) {
				server, client := newTCPPair(b)
				defer server.Close()
				drained := make(chan struct{})
				go func() {
					defer close(drained)
					_, _ = io.Copy(io.Discard, client)
				}()
				frameSize := int64(size.size)
				md := encoding.FileFrameMetadata{Size: frameSize, Mode: "0644", UID: "0", GID: "0", User: "root", Group: "root"}
				b.SetBytes(frameSize)
				b.ReportAllocs()
				for b.Loop() {
					offset := int64(0)
					if _, err := path.stream(src, &offset, frameStreamArgs{
						Ctx:           context.Background(),
						FileID:        1,
						FrameSize:     frameSize,
						Comp:          "none",
						Mode:          loadStrategyFast,
						HeaderTS:      1,
						IsTerminal:    true,
						TerminalMD:    &md,
						WindowHasher:  xxh3.New128(),
						Output:        server,
						OutputTCPConn: server,
						PipeSizeBytes: desiredPipeSizeBytes(frameSize, frameSize),
					}); err != nil {
						b.Fatalf("stream frame: %v", err)
					}
				}
				b.StopTimer()
				_ = server.Close()
				<-drained
				_ = client.Close()
			})
		}
	}
}

func memfdWithPayload(b *testing.B, payload []byte) *os.File {
	b.Helper()
	fd, err := unix.MemfdCreate("tx-send-bench", 0)
	if err != nil {
		b.Skipf("memfd_create: %v", err)
	}
	f := os.NewFile(uintptr(fd), "tx-send-bench")
	b.Cleanup(func() { _ = f.Close() })
	if _, err := f.Write(payload); err != nil {
		b.Fatalf("write memfd: %v", err)
	}
	return f
}
