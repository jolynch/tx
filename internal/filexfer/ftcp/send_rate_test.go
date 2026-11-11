package ftcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/sysio"
	"golang.org/x/sys/unix"
)

// rateRead is one delivery-rate read's result.
type rateRead struct {
	rate    uint64
	limited bool
}

// rateReads records the delivery-rate reads of one test.
type rateReads struct {
	mu  sync.Mutex
	got []rateRead
}

// recordRateReads replaces readLimits for one test: read i returns the rate
// script(i, reader) gives, and is recorded.
func recordRateReads(t *testing.T, script func(i int, r *sysio.TCPRateReader) (uint64, bool)) *rateReads {
	t.Helper()
	rr := &rateReads{}
	prev := readLimits
	readLimits = func(r *sysio.TCPRateReader) sysio.TCPLimits {
		rr.mu.Lock()
		i := len(rr.got)
		rr.mu.Unlock()
		rate, limited := script(i, r)
		rr.mu.Lock()
		rr.got = append(rr.got, rateRead{rate, limited})
		rr.mu.Unlock()
		return sysio.TCPLimits{DeliveryRate: rate, AppLimited: limited, OK: true}
	}
	t.Cleanup(func() { readLimits = prev })
	return rr
}

func (rr *rateReads) snapshot() []rateRead {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return append([]rateRead(nil), rr.got...)
}

// wireTimeAt is the write time wire bytes take at rate, computed as sendRate
// computes it.
func wireTimeAt(wire int64, rate uint64) time.Duration {
	return time.Duration(float64(wire) * float64(time.Second) / float64(rate))
}

// drainedLoopback returns a loopback socket whose peer a goroutine reads to
// the end. wait closes the socket's write side and returns the bytes the
// peer read.
func drainedLoopback(t *testing.T) (*net.TCPConn, func() int64) {
	t.Helper()
	server, client := newTCPPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	type result struct {
		n   int64
		err error
	}
	drained := make(chan result, 1)
	go func() {
		n, err := io.Copy(io.Discard, client)
		drained <- result{n, err}
	}()
	return server, func() int64 {
		_ = server.CloseWrite()
		r := <-drained
		if r.err != nil {
			t.Errorf("loopback reader: %v", r.err)
		}
		return r.n
	}
}

// sendOverLoopback runs one SEND of items to a drained loopback socket, after
// setup tunes the socket, and returns how long the SEND took.
func sendOverLoopback(t *testing.T, deps Deps, setup func(*net.TCPConn), items ...string) time.Duration {
	t.Helper()
	server, wait := drainedLoopback(t)
	if setup != nil {
		setup(server)
	}
	req, err := ParseRequest([]byte("SEND tx1"))
	if err != nil {
		t.Fatal(err)
	}
	body := framedItemBody(t, items...)
	start := time.Now()
	err = handleSENDWithOptions(context.Background(), req, body, server, deps, nil, defaultZeroCopyMinFrameBytes, 25)
	took := time.Since(start)
	wait()
	if err != nil {
		t.Fatalf("handleSENDWithOptions: %v", err)
	}
	return took
}

// TestSendWriteEstimateCorrectsFastWrites sends windows over loopback to a
// reader that takes 16 MiB/s, a slow path. The SEND reads TCP_INFO through
// its socket, and the test scripts the result to the reader's pace: on
// loopback, every sample after the path stalls measures one burst, so the
// kernel's own rate is not the reader's. The first window lands in empty
// socket buffers, so its write returns at once; the policy must see its wire
// bytes over the rate instead. No frame may be charged less than that.
func TestSendWriteEstimateCorrectsFastWrites(t *testing.T) {
	const window, windows = 256 << 10, 6
	// Another window follows each, so each window's one frame is decided.
	data := make([]byte, (windows+1)*window)
	rand.New(rand.NewSource(9)).Read(data)
	path := writeTempSendFile(t, data)
	const pace = 16 << 20
	reads := recordRateReads(t, func(_ int, r *sysio.TCPRateReader) (uint64, bool) {
		_, _ = r.DeliveryRate() // the real read, on the SEND's socket
		return pace, false
	})
	rec := recordFrameMetrics(t)

	server, client := newTCPPair(t)
	defer server.Close()
	defer client.Close()
	if err := server.SetWriteBuffer(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	readerDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 16<<10)
		for {
			if _, err := client.Read(buf); err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				readerDone <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	items := make([]string, windows)
	for i := range items {
		items[i] = fmt.Sprintf(`fd=%d %s offset=%d size=%d comp=adapt`, i+1, strconv.Quote(path), i*window, window)
	}
	req, err := ParseRequest([]byte("SEND tx1"))
	if err != nil {
		t.Fatal(err)
	}
	err = handleSENDWithOptions(context.Background(), req, framedItemBody(t, items...), server, &mockDeps{filePath: path}, nil, defaultZeroCopyMinFrameBytes, 25)
	_ = server.CloseWrite()
	if rerr := <-readerDone; rerr != nil {
		t.Fatalf("reader: %v", rerr)
	}
	if err != nil {
		t.Fatalf("handleSENDWithOptions: %v", err)
	}

	if got := len(reads.snapshot()); got != 1 {
		t.Fatalf("TCP_INFO reads = %d, want 1", got)
	}
	ms := rec.snapshot()
	if len(ms) != windows {
		t.Fatalf("policy saw %d frames, want %d", len(ms), windows)
	}
	for i, m := range ms {
		want := wireTimeAt(m.WireSize, pace)
		if m.WriteLatency < want {
			t.Errorf("frame %d: write %v is under its wire bytes over the delivery rate, %v", i, m.WriteLatency, want)
		}
	}
	if want := wireTimeAt(ms[0].WireSize, pace); ms[0].WriteLatency != want {
		t.Fatalf("first frame, written into empty socket buffers: write %v, want the estimate %v", ms[0].WriteLatency, want)
	}
}

// TestSendDeliveryRateReads checks when a SEND reads TCP_INFO, with scripted
// results. A SEND with no adaptive item reads nothing. An adaptive SEND reads
// after its first decided frame and again after another frame's worth of
// bytes; an application-limited rate is not usable. Frames from the read
// that found a rate on are charged its estimate; the others keep their
// measured writes.
func TestSendDeliveryRateReads(t *testing.T) {
	// Each item is one decided frame and a short terminal one.
	data := make([]byte, defaultFileFrameLogicalSize+1<<10)
	rand.New(rand.NewSource(4)).Read(data)
	path := writeTempSendFile(t, data)
	items := func(comp string) []string {
		out := make([]string, 3)
		for i := range out {
			out[i] = fmt.Sprintf(`fd=%d %s comp=%s`, i+1, strconv.Quote(path), comp)
		}
		return out
	}
	// Six 1 MiB windows of a larger file, each one decided frame: the retry
	// waits for a frame's worth of bytes, four of these frames.
	small := writeTempSendFile(t, make([]byte, 8<<20))
	smallItems := make([]string, 6)
	for i := range smallItems {
		smallItems[i] = fmt.Sprintf(`fd=%d %s offset=%d size=%d comp=adapt`, i+1, strconv.Quote(small), i<<20, 1<<20)
	}
	const slow = 8 << 20 // 4 MiB takes 500 ms at this rate, far longer than a loopback write
	usable := rateRead{rate: slow}
	tests := []struct {
		name      string
		comp      string
		script    []rateRead // read i returns script[min(i, len-1)]
		wantReads int
		from      int // first frame charged the estimate; -1 for none
		path      string
		items     []string // when set, sent instead of three whole files of comp
	}{
		{"none", "none", []rateRead{usable}, 0, -1, "", nil},
		{"lz4", encoding.EncodingLz4, []rateRead{usable}, 0, -1, "", nil},
		{"zstd", encoding.EncodingZstd, []rateRead{usable}, 0, -1, "", nil},
		{"rate on the first read", "adapt", []rateRead{usable}, 2, 0, "", nil},
		{"rate on the retry", "adapt", []rateRead{{}, usable}, 2, 1, "", nil},
		{"no rate", "adapt", []rateRead{{}}, 2, -1, "", nil},
		{"app-limited rate", "adapt", []rateRead{{rate: slow, limited: true}}, 2, -1, "", nil},
		{"app-limited, then usable", "adapt", []rateRead{{rate: slow, limited: true}, usable}, 2, 1, "", nil},
		{"second read after a frame's worth of bytes", "adapt", []rateRead{{}, usable}, 2, 4, small, smallItems},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reads := recordRateReads(t, func(i int, _ *sysio.TCPRateReader) (uint64, bool) {
				r := tc.script[min(i, len(tc.script)-1)]
				return r.rate, r.limited
			})
			rec := recordFrameMetrics(t)
			sendPath, sendItems, wantFrames := path, items(tc.comp), 3
			if tc.items != nil {
				sendPath, sendItems, wantFrames = tc.path, tc.items, len(tc.items)
			}
			if tc.comp != "adapt" {
				wantFrames = 0
			}
			sendOverLoopback(t, &mockDeps{filePath: sendPath}, nil, sendItems...)
			if got := len(reads.snapshot()); got != tc.wantReads {
				t.Fatalf("TCP_INFO reads = %d, want %d", got, tc.wantReads)
			}
			ms := rec.snapshot()
			if len(ms) != wantFrames {
				t.Fatalf("policy saw %d frames, want %d", len(ms), wantFrames)
			}
			for i, m := range ms {
				est := wireTimeAt(m.WireSize, slow)
				if charged := tc.from >= 0 && i >= tc.from; charged != (m.WriteLatency == est) {
					t.Errorf("frame %d: write %v, estimate %v; want charged the estimate = %v", i, m.WriteLatency, est, charged)
				}
			}
		})
	}
}

// TestSendReportsWindowRates checks a SEND leaves a rate record for each
// window whose frame reached the policy: a file counts on its window at
// offset 0, each record carries the kernel's reading and the rate the
// writes showed, and every decided frame counts under its bottleneck class.
func TestSendReportsWindowRates(t *testing.T) {
	data := make([]byte, defaultFileFrameLogicalSize+1<<10)
	rand.New(rand.NewSource(5)).Read(data)
	path := writeTempSendFile(t, data)
	recordRateReads(t, func(int, *sysio.TCPRateReader) (uint64, bool) { return 64 << 20, false })
	items := []string{
		fmt.Sprintf(`fd=1 %s comp=adapt`, strconv.Quote(path)),
		fmt.Sprintf(`fd=2 %s comp=adapt`, strconv.Quote(path)),
		fmt.Sprintf(`fd=3 %s comp=none`, strconv.Quote(path)),
	}
	deps := &mockDeps{filePath: path}
	frames := recordFrameMetrics(t)
	sendOverLoopback(t, deps, nil, items...)
	if r := deps.rates; r.Windows != 2 || r.Files != 2 || r.Kernel.N != 2 || r.User.N != 2 {
		t.Fatalf("rates %+v; want 2 windows and files, each with both readings", r)
	}
	if got, want := deps.rates.Bottlenecks.Total(), int64(len(frames.snapshot())); got != want || got == 0 {
		t.Fatalf("bottleneck classes count %d frames (%+v), want the %d the policy decided", got, deps.rates.Bottlenecks, want)
	}
	// A client retry resends file 1's first window: one more window, but
	// still two files.
	sendOverLoopback(t, deps, nil, items[0])
	if r := deps.rates; r.Windows != 3 || r.Files != 2 {
		t.Fatalf("rates %+v after a resent first window; want 3 windows and 2 files", r)
	}
}

// TestSendRateKeepsHighestReading checks the read schedule of a long SEND of
// full frames, at frames 1, 2, 4, and 8 and then every eighth, and that the
// estimate keeps the highest usable rate: a connection in slow start reads
// low first, and an application-limited or lower later reading must not
// replace a higher one.
func TestSendRateKeepsHighestReading(t *testing.T) {
	const frame = defaultFileFrameLogicalSize
	script := []rateRead{{rate: 1 << 20}, {rate: 64 << 20}, {rate: 1 << 30, limited: true}, {rate: 16 << 20}, {}}
	reads := recordRateReads(t, func(i int, _ *sysio.TCPRateReader) (uint64, bool) {
		r := script[min(i, len(script)-1)]
		return r.rate, r.limited
	})
	r := sendRate{src: &sendRateSource{reader: &sysio.TCPRateReader{}}}
	var readAt []int
	for f := 1; f <= 32; f++ {
		before := len(reads.snapshot())
		got := r.writeTime(frame, 0)
		if len(reads.snapshot()) > before {
			readAt = append(readAt, f)
		}
		want := uint64(64 << 20)
		if len(readAt) == 1 {
			want = 1 << 20
		}
		if got != wireTimeAt(frame, want) {
			t.Fatalf("frame %d: write %v, want %v at %d B/s", f, got, wireTimeAt(frame, want), want)
		}
	}
	if want := []int{1, 2, 4, 8, 16, 24, 32}; !slices.Equal(readAt, want) {
		t.Fatalf("reads at frames %v, want %v", readAt, want)
	}
}

// TestSendRateUsesWritesWithoutKernel checks the rate a connection's writes
// show: it follows writes that waited on the path, a buffered write is
// charged that rate, it carries to the connection's next SEND, and a kernel
// rate takes precedence when the SEND has one.
func TestSendRateUsesWritesWithoutKernel(t *testing.T) {
	const frame = defaultFileFrameLogicalSize
	src := &sendRateSource{}
	r := sendRate{src: src}
	if got := r.writeTime(frame, 40*time.Millisecond); got != 40*time.Millisecond {
		t.Fatalf("first frame: write %v, want its measurement", got)
	}
	// 4 MiB in 40 ms is 100 MiB/s; a buffered write is charged that.
	if got := r.writeTime(frame, time.Millisecond); got != 40*time.Millisecond {
		t.Fatalf("buffered frame: write %v, want 40ms at the rate earlier writes showed", got)
	}
	rate := src.userRate()
	next := sendRate{src: src}
	if got, want := next.writeTime(frame, 0), wireTimeAt(frame, rate); got != want {
		t.Fatalf("next SEND's first frame: write %v, want %v at %d B/s", got, want, rate)
	}
	reads := recordRateReads(t, func(int, *sysio.TCPRateReader) (uint64, bool) { return 16 << 20, false })
	src.reader = &sysio.TCPRateReader{}
	kernel := sendRate{src: src}
	if got, want := kernel.writeTime(frame, 0), wireTimeAt(frame, 16<<20); got != want || len(reads.snapshot()) != 1 {
		t.Fatalf("with a kernel rate: write %v, want %v at 16 MiB/s", got, want)
	}
	if src.userRate() == 0 {
		t.Fatal("the writes' rate stopped updating under a kernel rate")
	}
}

// TestSendLinkSeedsTheConnectionsNextFile checks the link estimate a file
// leaves on its connection: once a file's uncompressed baseline is seeded,
// the connection's next fresh file starts comparing codecs at once, while a
// file with state of its own, or a connection with no estimate, is left
// alone.
func TestSendLinkSeedsTheConnectionsNextFile(t *testing.T) {
	r := sendRate{src: &sendRateSource{}}
	fresh := policy.NewCompressionPolicy()
	r.seedLink(fresh)
	if *fresh != (policy.CompressionPolicy{}) {
		t.Fatal("a connection with no estimate seeded a file")
	}
	first := policy.NewCompressionPolicy()
	for range 4 {
		first.DecideSampled(policy.CompressionModeNone, sampleFrameMetrics(), nil)
		r.saveLink(first)
	}
	if !r.src.hasLink {
		t.Fatal("a seeded uncompressed file left no estimate")
	}
	next := policy.NewCompressionPolicy()
	r.seedLink(next)
	if !next.BackgroundPending() {
		t.Fatal("the connection's next file did not start comparing")
	}
	// Its first frame samples, timed by the frames the connection already
	// sent: a 20 MiB file has just the 5 frames a seeded switch needs, so a
	// file that waited for its own first frame's time would never sample.
	const frames = 5
	pinSampleCost(t, time.Millisecond)
	r.observeFrame(defaultFileFrameLogicalSize, 9*time.Millisecond)
	sampler := sendSampler{slots: make(chan struct{}, 1)}
	if sampler.request(context.Background(), sampleWork{frames, frames * defaultFileFrameLogicalSize, frames, frames * defaultFileFrameLogicalSize}, next, r.wallFrame()) == nil {
		t.Fatal("a seeded file's first frame was not admitted for a sample")
	}
	sampler.close()
	before := *first
	r.seedLink(first)
	if *first != before {
		t.Fatal("seeding changed a file with state of its own")
	}
	var none *sendRate
	none.seedLink(next)
	none.saveLink(next)
}

// TestSendRateSourceSeesThroughWrappers checks a SEND finds its socket
// through the connection's context, which carries it past an authenticated
// session's message writer and a fault wrapper, or from a raw response
// socket; that a connection with no socket still gets a source, for the
// rate its writes show; and that a read through a source allocates nothing
// once its reader is built.
func TestSendRateSourceSeesThroughWrappers(t *testing.T) {
	server, client := newTCPPair(t)
	defer server.Close()
	defer client.Close()
	wrapped := faults.New(faults.Config{Seed: 1}).WrapConn(server, faults.Server)
	var src sendRateSource
	ctx := withSendRateSource(context.Background(), &src, wrapped)
	// A buffer stands in for a session's message writer: no net.Conn.
	var resp bytes.Buffer
	if got := sendRateSourceFor(ctx, &resp); got != &src || got.conn != server {
		t.Fatalf("context source = %p, want the connection's %p, on its socket", got, &src)
	}
	if got := sendRateSourceFor(context.Background(), wrapped); got == nil || got.conn != server {
		t.Fatal("a wrapped response socket offered no source")
	}
	if got := sendRateSourceFor(context.Background(), &resp); got == nil || got.kernel() {
		t.Fatal("a buffer got no source, or one with a kernel rate")
	}
	pipeA, pipeB := net.Pipe()
	defer pipeA.Close()
	defer pipeB.Close()
	var pipeSrc sendRateSource
	if got := sendRateSourceFor(withSendRateSource(context.Background(), &pipeSrc, pipeA), &resp); got != &pipeSrc || got.kernel() {
		t.Fatal("a pipe connection's context carries no source, or one with a kernel rate")
	}
	r := sendRate{src: &src}
	_ = r.writeTime(1, 0)
	if allocs := testing.AllocsPerRun(100, func() {
		r = sendRate{src: &src}
		_ = r.writeTime(4<<20, time.Millisecond)
	}); allocs != 0 {
		t.Fatalf("a delivery-rate read allocated %.1f times", allocs)
	}
}

// TestSendRateClassifiesBottleneck checks the bottleneck a connection's
// TCP_INFO names on loopback: a receiver that drains slowly, a sender paced
// below the path (as a congested network paces it), and a sender that
// cannot keep the socket busy.
func TestSendRateClassifiesBottleneck(t *testing.T) {
	prev := sendBottleneck.Load()
	t.Cleanup(func() { sendBottleneck.Store(prev) })
	// Each case sends for ten classifyWindow spans, so one span a loaded
	// host disturbs cannot keep two in a row from agreeing.
	const sendFor = 10 * classifyWindow
	tests := []struct {
		name       string
		pace       int           // SO_MAX_PACING_RATE on the sender, 0 for none
		readSleep  time.Duration // after each 64 KiB read
		writeSleep time.Duration // after each 64 KiB write
		want       policy.Bottleneck
	}{
		{"slow receiver", 0, 500 * time.Microsecond, 0, policy.BottleneckReceiver},
		{"paced network", 128 << 20, 0, 0, policy.BottleneckNetwork},
		{"slow sender", 0, 0, time.Millisecond, policy.BottleneckSender},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, client := newTCPPair(t)
			defer server.Close()
			defer client.Close()
			if tc.pace > 0 {
				raw, err := server.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var serr error
				if err := raw.Control(func(fd uintptr) {
					serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_MAX_PACING_RATE, tc.pace)
				}); err != nil || serr != nil {
					t.Fatalf("SO_MAX_PACING_RATE: %v %v", err, serr)
				}
			}
			done := make(chan error, 1)
			go func() {
				buf := make([]byte, 64<<10)
				for {
					_, err := io.ReadFull(client, buf)
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						done <- nil
						return
					}
					if err != nil {
						done <- err
						return
					}
					if tc.readSleep > 0 {
						time.Sleep(tc.readSleep)
					}
				}
			}()
			src := &sendRateSource{conn: server}
			chunk := make([]byte, 64<<10)
			for sent, start := 0, time.Now(); time.Since(start) < sendFor; sent += len(chunk) {
				if _, err := server.Write(chunk); err != nil {
					t.Fatal(err)
				}
				if tc.writeSleep > 0 {
					time.Sleep(tc.writeSleep)
				}
				if sent%(1<<20) == 0 {
					_, _ = src.deliveryRate()
				}
			}
			if err := server.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if got := (&sendRate{src: src}).bottleneck(); got != tc.want {
				t.Fatalf("bottleneck = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSendRateClassifySpans checks classify against scripted readings: a
// span shorter than classifyWindow keeps the previous class, a failed read
// or a counter that went backward starts no span, each limit names its
// class at classifyShare, the receiver's window wins over an idle socket
// once the span was busy classifyMinBusy, and a class takes effect on its
// second span in a row.
func TestSendRateClassifySpans(t *testing.T) {
	prev := sendBottleneck.Load()
	t.Cleanup(func() { sendBottleneck.Store(prev) })
	// u scales the script with the thresholds: classifyMinBusy is 10u and
	// classifyWindow at most 20u.
	u := classifyMinBusy / 10
	at := time.Unix(0, 0)
	reading := func(busy, rwnd time.Duration) sysio.TCPLimits {
		return sysio.TCPLimits{Busy: busy, RwndLimited: rwnd, OK: true}
	}
	steps := []struct {
		after time.Duration
		l     sysio.TCPLimits
		want  policy.Bottleneck
	}{
		{0, reading(0, 0), policy.BottleneckUnknown},              // first reading: no span yet
		{10 * u, reading(10*u, 10*u), policy.BottleneckUnknown},   // too short to classify
		{20 * u, reading(30*u, 10*u), policy.BottleneckUnknown},   // network once: 30 of 30u busy, a third rwnd-limited
		{40 * u, reading(70*u, 20*u), policy.BottleneckNetwork},   // network twice
		{40 * u, reading(110*u, 60*u), policy.BottleneckNetwork},  // receiver once: all rwnd-limited
		{40 * u, reading(150*u, 80*u), policy.BottleneckReceiver}, // half of busy is enough
		{40 * u, reading(160*u, 80*u), policy.BottleneckReceiver}, // sender once: 10 of 40u busy
		{40 * u, sysio.TCPLimits{}, policy.BottleneckReceiver},    // a failed read keeps the class
		{40 * u, reading(170*u, 80*u), policy.BottleneckSender},
		{40 * u, reading(5*u, 0), policy.BottleneckSender},  // a counter went backward: a new span
		{40 * u, reading(45*u, 0), policy.BottleneckSender}, // network once
		{40 * u, reading(50*u, 0), policy.BottleneckSender}, // sender once: a lone span changes nothing
		{40 * u, reading(90*u, 0), policy.BottleneckSender}, // network once
		{40 * u, reading(130*u, 0), policy.BottleneckNetwork},
		{40 * u, reading(145*u, 10*u), policy.BottleneckNetwork},  // receiver once: 15 of 40u busy, mostly rwnd-limited
		{40 * u, reading(160*u, 20*u), policy.BottleneckReceiver}, // an idle socket whose receiver paces the rest
	}
	src := &sendRateSource{}
	for i, s := range steps {
		at = at.Add(s.after)
		src.classify(s.l, at)
		if src.bottleneck != s.want {
			t.Fatalf("step %d: bottleneck = %v, want %v", i, src.bottleneck, s.want)
		}
	}
}

// TestSendRateLogsKernelWithoutLimits checks that readings from a kernel
// without busy and limited times name no bottleneck, and that the process
// says so once.
func TestSendRateLogsKernelWithoutLimits(t *testing.T) {
	var buf bytes.Buffer
	flags, out := log.Flags(), log.Writer()
	log.SetFlags(0)
	log.SetOutput(&buf)
	noLimitsOnce = sync.Once{}
	t.Cleanup(func() { log.SetFlags(flags); log.SetOutput(out); noLimitsOnce = sync.Once{} })
	u := classifyMinBusy / 10
	at := time.Now()
	src := &sendRateSource{}
	for i := range 4 {
		src.classify(sysio.TCPLimits{DeliveryRate: 1 << 20, NoLimits: true, OK: true}, at.Add(time.Duration(i)*40*u))
	}
	if src.bottleneck != policy.BottleneckUnknown {
		t.Fatalf("bottleneck = %v without limited times, want unknown", src.bottleneck)
	}
	if got := strings.Count(buf.String(), "needs Linux 4.10"); got != 1 {
		t.Fatalf("logged %d notices, want 1: %q", got, buf.String())
	}
}

// TestSendRateNewConnectionStartsFromProcessClass checks that a TCP
// connection with no class of its own reports the last class any connection
// reached, until it expires, and its own once it has one; a source without
// a socket reports none.
func TestSendRateNewConnectionStartsFromProcessClass(t *testing.T) {
	prev := sendBottleneck.Load()
	t.Cleanup(func() { sendBottleneck.Store(prev) })
	// u scales the script with the thresholds: classifyMinBusy is 10u and
	// classifyWindow at most 20u.
	u := classifyMinBusy / 10
	// The connection reads the process's class at the wall clock.
	at := time.Now().Add(-120 * u)
	first := &sendRateSource{}
	for i, l := range []sysio.TCPLimits{
		{OK: true}, {Busy: 40 * u, RwndLimited: 40 * u, OK: true}, {Busy: 80 * u, RwndLimited: 80 * u, OK: true},
	} {
		first.classify(l, at.Add(time.Duration(i)*40*u))
	}
	if got := processBottleneck(at.Add(80 * u)); got != policy.BottleneckReceiver {
		t.Fatalf("process class = %v after a receiver-bound connection", got)
	}
	if got := processBottleneck(at.Add(80*u + classifyStale + time.Millisecond)); got != policy.BottleneckUnknown {
		t.Fatalf("process class = %v once no connection confirmed it for classifyStale, want unknown", got)
	}
	if got := (&sendRate{src: &sendRateSource{}}).bottleneck(); got != policy.BottleneckUnknown {
		t.Fatalf("source without a socket reports %v, want unknown", got)
	}
	next := &sendRateSource{reader: &sysio.TCPRateReader{}}
	if got := (&sendRate{src: next}).bottleneck(); got != policy.BottleneckReceiver {
		t.Fatalf("new connection reports %v, want the process's receiver", got)
	}
	for i, l := range []sysio.TCPLimits{
		{OK: true}, {Busy: 40 * u, OK: true}, {Busy: 80 * u, OK: true},
	} {
		next.classify(l, at.Add(time.Duration(i)*40*u))
	}
	if got := (&sendRate{src: next}).bottleneck(); got != policy.BottleneckNetwork {
		t.Fatalf("connection with its own class reports %v, want network", got)
	}
}
