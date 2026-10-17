//go:build linux

// Linux only: the benchmark reads from a memfd (unix.MemfdCreate), and the
// zero-copy path it compares is tee/splice. The _linux file name already
// implies this constraint; the line above states it where a reader sees it.

package ftcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/faults"
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

func memfdWithPayload(b testing.TB, payload []byte) *os.File {
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

// TestSendSyscallGates checks the SEND syscall sites that a short test
// transfer may not reach (poll waits only on a full socket, readahead runs in
// the background) each ask the injector's gate.
func TestSendSyscallGates(t *testing.T) {
	inj := faults.New(faults.Config{Seed: 1})
	src := memfdWithPayload(t, zeroCopyPayload(2<<20))
	defer src.Close()
	reached := func(sc faults.Syscall, name string) {
		t.Helper()
		if inj.Counts().Sys[sc] == 0 {
			t.Fatalf("%s skipped its gate", name)
		}
	}

	sc := faultZeroCopySyscalls(inj, zeroCopySyscalls{
		splice: func(int, *int64, int, *int64, int, int) (int64, error) { return 1, nil },
		tee:    func(int, int, int, int) (int64, error) { return 1, nil },
		poll:   func([]unix.PollFd, int) (int, error) { return 1, nil },
	})
	_, _ = sc.splice(0, nil, 0, nil, 1, 0)
	_, _ = sc.tee(0, 0, 1, 0)
	_, _ = sc.poll(nil, 0)
	reached(faults.SysSplice, "splice")
	reached(faults.SysTee, "tee")
	reached(faults.SysPoll, "poll")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	// The default pipe is 64 KiB; a passing gate must still resize it.
	growPipeBestEffort(inj, r, 256<<10)
	reached(faults.SysFcntl, "F_SETPIPE_SZ")
	if size, err := unix.FcntlInt(r.Fd(), unix.F_GETPIPE_SZ, 0); err != nil || size != 256<<10 {
		t.Fatalf("pipe size %d (%v) after a passing gate, want %d", size, err, 256<<10)
	}

	tryAdviseSequential(inj, src, 0, 1<<20)
	reached(faults.SysFadvise, "fadvise")

	// A hint already queued is applied before the stop function returns.
	done := make(chan struct{})
	hints, stop := startFrameReadahead(inj, src, done)
	sendReadaheadHint(hints, readaheadHint{offset: 0, length: 1 << 20})
	stop()
	close(done)
	reached(faults.SysReadahead, "readahead")
}

// TestSendSiteGateCounts checks the exact number of gate calls each SEND and
// CXSUM site makes, so a site that drops its gate cannot hide behind another
// site asking the same gate.
func TestSendSiteGateCounts(t *testing.T) {
	path := writeTempSendFile(t, zeroCopyPayload(64<<10))

	t.Run("zero-copy frame", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1})
		src, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		server, client := newTCPPair(t)
		defer client.Close()
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			_, _ = io.Copy(io.Discard, client)
		}()
		md := encoding.FileFrameMetadata{Size: 64 << 10, Mode: "0644"}
		offset := int64(0)
		_, err = streamFramePayloadZeroCopy(src, &offset, frameStreamArgs{
			Ctx: context.Background(), FileID: 1, FrameSize: 64 << 10, Comp: "none", Mode: loadStrategyFast,
			HeaderTS: 1, IsTerminal: true, TerminalMD: &md, WindowHasher: xxh3.New128(),
			Output: server, OutputTCPConn: server, PipeSizeBytes: 64 << 10, Faults: inj,
		})
		_ = server.Close()
		<-drained
		if err != nil {
			t.Fatal(err)
		}
		// Two pipes, each end resized, and one duplicated socket.
		sys := inj.Counts().Sys
		if sys[faults.SysPipe] != 2 || sys[faults.SysFcntl] != 4 || sys[faults.SysDup] != 1 ||
			sys[faults.SysSplice] < 2 || sys[faults.SysTee] < 1 {
			t.Fatalf("zero-copy frame gate counts %v", sys)
		}
	})

	t.Run("SEND item", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1})
		deps := &mockDeps{filePath: path}
		var out bytes.Buffer
		o := unbufferedSendOutput(&out)
		o.faults = inj
		if _, err := streamSendItem(context.Background(), o, deps, "tx1", sendItem{FileID: 1, Comp: "none", Path: path, Mode: loadStrategyFast},
			mustFileRef(t, deps, 1, path), defaultZeroCopyMinFrameBytes); err != nil {
			t.Fatal(err)
		}
		c := inj.Counts()
		if c.Opens[faults.SiteSendOpen] != 1 || c.Sys[faults.SysFstat] != 1 || c.Reads[faults.SiteSendRead] != 1 {
			t.Fatalf("SEND item counts %+v", c)
		}
	})

	t.Run("CXSUM item", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1})
		deps := &mockDeps{filePath: path}
		req, err := ParseRequest([]byte(`CXSUM tx1`))
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		ctx := faults.NewContext(context.Background(), inj)
		if err := handleCXSUMWithInput(ctx, req, framedItemBody(t, `fd=1 "/a" offset=0 size=5 algo=xxh128`), &out, deps); err != nil {
			t.Fatal(err)
		}
		c := inj.Counts()
		if c.Opens[faults.SiteCXSUMOpen] != 1 || c.Sys[faults.SysFstat] != 1 || c.Reads[faults.SiteCXSUMRead] != 1 {
			t.Fatalf("CXSUM item counts %+v", c)
		}
	})
}

// gateWaitWriter blocks its first write until inj has answered the gate sc,
// failing after a few seconds. SEND queues the readahead hint for the next
// frame before streaming the current one, so holding the first frame's write
// gives the readahead goroutine a hint it must apply.
type gateWaitWriter struct {
	inj     *faults.Injector
	sc      faults.Syscall
	waited  bool
	reached bool
}

func (w *gateWaitWriter) Write(p []byte) (int, error) {
	if !w.waited {
		w.waited = true
		deadline := time.Now().Add(5 * time.Second)
		for !w.reached && time.Now().Before(deadline) {
			w.reached = w.inj.Counts().Sys[w.sc] > 0
			time.Sleep(time.Millisecond)
		}
	}
	return len(p), nil
}

// TestSendItemReachesReadaheadGate checks SEND's frame readahead goes
// through the injector, by way of streamSendItem rather than the helper.
func TestSendItemReachesReadaheadGate(t *testing.T) {
	path := writeTempSendFile(t, zeroCopyPayload(int(3*defaultFileFrameLogicalSize)))
	inj := faults.New(faults.Config{Seed: 1})
	w := &gateWaitWriter{inj: inj, sc: faults.SysReadahead}
	out := unbufferedSendOutput(w)
	out.faults = inj
	deps := &mockDeps{filePath: path}
	if _, err := streamSendItem(context.Background(), out, deps, "tx1", sendItem{FileID: 1, Comp: "none", Path: path, Mode: loadStrategyFast},
		mustFileRef(t, deps, 1, path), defaultZeroCopyMinFrameBytes); err != nil {
		t.Fatal(err)
	}
	if !w.reached {
		t.Fatal("SEND readahead never reached its gate")
	}
}

// TestRefusalsHalfCloseThroughFaultWrapper checks a busy or diagnostic
// refusal on a wrapped socket still half-closes it, so the client reads EOF
// while the server is still draining the client's opening, as it does
// without the wrapper.
func TestRefusalsHalfCloseThroughFaultWrapper(t *testing.T) {
	inj := faults.New(faults.Config{Seed: 1})
	for name, refuse := range map[string]func(net.Conn){
		"busy":       func(c net.Conn) { refuseBusy(c, nil) },
		"diagnostic": func(c net.Conn) { writeDiagnostic(c, "ERR TEST refused", time.Now().Add(5*time.Second)) },
	} {
		t.Run(name, func(t *testing.T) {
			server, client := newTCPPair(t)
			defer server.Close() // writeDiagnostic leaves closing to its caller
			defer client.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				refuse(inj.WrapConn(server, faults.Server))
			}()
			_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.ReadAll(client); err != nil {
				t.Fatal(err)
			}
			// A half-closed socket reaches EOF while the server drains for
			// up to busyDrainTimeout; a fully closed one only after.
			time.Sleep(busyDrainTimeout / 10)
			select {
			case <-done:
				t.Fatal("EOF arrived only when the server closed the connection")
			default:
			}
			<-done
		})
	}
}
