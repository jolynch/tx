package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/sysio"
	"github.com/zeebo/xxh3"
)

// rateWriter models a bottlenecked link: each write takes nsPerByte per byte,
// so a compressed frame, which writes fewer bytes, is faster than a raw one.
type rateWriter struct {
	w         io.Writer
	nsPerByte int64
}

func (r *rateWriter) Write(p []byte) (int, error) {
	if d := time.Duration(int64(len(p)) * r.nsPerByte); d > 0 {
		time.Sleep(d)
	}
	return r.w.Write(p)
}

const windowTestFrames = 8

// multiWindowFile registers one regular file (file ID 1) in a real store and
// returns the deps, the transfer ID, and the file's path.
func multiWindowFile(t *testing.T, data []byte) (Deps, string, string) {
	t.Helper()
	path := writeTempSendFile(t, data)
	st := store.NewStore()
	t.Cleanup(st.Close)
	deps := NewRuntimeDeps(st, WithRoot("/"))
	transfer, err := st.NewTransfer(filepath.Dir(path), 0, int64(len(data)))
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	st.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 1, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte(filepath.Clean(path))), FileSize: int64(len(data))},
	}, TransferStateStarted)
	return deps, transfer.ID, path
}

// sendWindow sends frames*defaultFileFrameLogicalSize bytes of file 1 from
// frame index firstFrame as one adaptive SEND item, through a link of
// nsPerByte, and returns the raw response.
func sendWindow(t *testing.T, deps Deps, txferID, path string, firstFrame, frames int, nsPerByte int64) []byte {
	t.Helper()
	refs, errs := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Errorf("GetFileRefs: %v", errs[0])
		return nil
	}
	var raw bytes.Buffer
	link := &rateWriter{w: &raw, nsPerByte: nsPerByte}
	item := sendItem{FileID: 1, Offset: int64(firstFrame) * defaultFileFrameLogicalSize, Size: int64(frames) * defaultFileFrameLogicalSize, Comp: "adapt", Path: path}
	sent, err := streamSendItem(context.Background(), unbufferedSendOutput(link), deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes)
	if err != nil {
		t.Errorf("streamSendItem window at frame %d: %v", firstFrame, err)
	}
	applyWindowState(deps, txferID, sent)
	return raw.Bytes()
}

// applyWindowState stores what a window left for its file's next one, as a
// SEND does once it has sent all its items.
func applyWindowState(deps Deps, txferID string, sent sentItem) {
	if sent.hasComp {
		deps.RecordTransferWindows(txferID, WindowRecords{Compression: []FileCompressionUpdate{sent.comp}})
	}
}

func compressibleBytes(n int64) []byte {
	const pat = "compress-me-"
	return bytes.Repeat([]byte(pat), int(n/int64(len(pat)))+1)[:n]
}

// The adaptive policy must carry over from one window of a file to the next:
// incompressible data must not send losing candidate codecs in any window.
func TestSendWindowsIncompressibleFileStaysMostlyUncompressed(t *testing.T) {
	const windows = 8
	data := make([]byte, windows*windowTestFrames*defaultFileFrameLogicalSize)
	rand.New(rand.NewSource(11)).Read(data)
	deps, txferID, path := multiWindowFile(t, data)

	total, compressed := 0, 0
	for w := range windows {
		raw := sendWindow(t, deps, txferID, path, w*windowTestFrames, windowTestFrames, 5)
		comps, err := frameComps(raw)
		if err != nil {
			t.Fatalf("frameComps window %d: %v", w, err)
		}
		if len(comps) != windowTestFrames {
			t.Fatalf("window %d has %d frames, want %d", w, len(comps), windowTestFrames)
		}
		for _, c := range comps {
			total++
			if c != "none" {
				compressed++
			}
		}
		t.Logf("window %d: %v", w, comps)
	}
	if compressed != 0 {
		t.Fatalf("incompressible file sent %d of %d frames compressed; candidates must stay in the background", compressed, total)
	}
}

// Each window starts with the codec the policy chose after the previous
// window's last frame, whatever the timing decided that codec was: the last
// frame of a window another follows is decided too.
func TestSendWindowsStartWhereThePreviousWindowEnded(t *testing.T) {
	const windows = 6
	deps, txferID, path := multiWindowFile(t, compressibleBytes(windows*windowTestFrames*defaultFileFrameLogicalSize))

	next := "none"
	switched := false
	for w := range windows {
		raw := sendWindow(t, deps, txferID, path, w*windowTestFrames, windowTestFrames, 15)
		comps, err := frameComps(raw)
		if err != nil || len(comps) == 0 {
			t.Fatalf("frameComps window %d: %v (%d frames)", w, err, len(comps))
		}
		if comps[0] != next {
			t.Fatalf("window %d starts with %q, the previous window chose %q; frames = %v", w, comps[0], next, comps)
		}
		st, ok := deps.LoadFileCompression(txferID, 1)
		if w < windows-1 && !ok {
			t.Fatalf("window %d saved no state for the next", w)
		}
		next = policy.FrameCompTokenForMode(st.Mode)
		switched = switched || next != comps[len(comps)-1]
	}
	t.Logf("a window's last frame chose a new codec for the next: %v", switched)
}

// sendOneItem streams one adaptive item of file 1 through a fresh output and
// applies what it leaves, as handleSENDWithOptions does.
func sendOneItem(t *testing.T, deps Deps, txferID, path string, item sendItem) sentItem {
	t.Helper()
	refs, errs := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Fatalf("GetFileRefs: %v", errs[0])
	}
	item.FileID, item.Comp, item.Path = 1, "adapt", path
	var raw bytes.Buffer
	sent, err := streamSendItem(context.Background(), unbufferedSendOutput(&raw), deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes)
	if err != nil {
		t.Fatalf("streamSendItem: %v", err)
	}
	applyWindowState(deps, txferID, sent)
	return sent
}

// A window saves state only when another window follows. The last window of a
// file, a resumed tail window, and a file that shrank since its manifest would
// otherwise leave an entry that no ACK frees.
func TestSendWindowsSaveOnlyWhenAnotherWindowFollows(t *testing.T) {
	const frames = 4
	size := int64(frames) * defaultFileFrameLogicalSize
	tests := []struct {
		name     string
		shrinkTo int64 // truncate the file after the manifest when non-zero
		item     sendItem
		wantSave bool
	}{
		{"first of several windows", 0, sendItem{Size: size / 2}, true},
		{"middle window", 0, sendItem{Offset: size / 4, Size: size / 2}, true},
		{"last window", 0, sendItem{Offset: size / 2, Size: size / 2}, false},
		{"resumed tail window", 0, sendItem{Offset: size / 2}, false},
		{"whole file", 0, sendItem{}, false},
		{"shrunk file, whole-file SEND", size / 2, sendItem{}, false},
		{"shrunk file, window ends at the new end", size / 2, sendItem{Size: size / 2}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps, txferID, path := multiWindowFile(t, compressibleBytes(size))
			if tc.shrinkTo > 0 {
				if err := os.Truncate(path, tc.shrinkTo); err != nil {
					t.Fatalf("Truncate: %v", err)
				}
			}
			sent := sendOneItem(t, deps, txferID, path, tc.item)
			if sent.hasComp != tc.wantSave {
				t.Fatalf("window reported state = %v, want %v", sent.hasComp, tc.wantSave)
			}
			if _, ok := deps.LoadFileCompression(txferID, 1); ok != tc.wantSave {
				t.Fatalf("store holds state = %v, want %v", ok, tc.wantSave)
			}
		})
	}
}

// A request loads state only for a window an earlier one preceded and saves it
// only for a window another follows, and applies the saves in one store call
// with the window hashes.
func TestHandleSENDCompressionStateLoadsAndSaves(t *testing.T) {
	const frames = 3
	size := int64(frames) * defaultFileFrameLogicalSize
	path := writeTempSendFile(t, compressibleBytes(size))
	quoted := strconv.Quote(path)
	half := strconv.FormatInt(size/2, 10)
	tests := []struct {
		name                string
		item                string
		wantLoads, wantSave int
	}{
		{"single window", `fd=1 ` + quoted, 0, 0},
		{"first window", `fd=1 ` + quoted + ` size=` + half, 0, 1},
		{"middle window", `fd=1 ` + quoted + ` offset=` + strconv.FormatInt(size/4, 10) + ` size=` + half, 1, 1},
		{"resumed tail window", `fd=1 ` + quoted + ` offset=` + half, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := &mockDeps{filePath: path}
			req, err := ParseRequest([]byte("SEND tx1"))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			var out bytes.Buffer
			if err := handleSENDWithOptions(context.Background(), req, framedItemBody(t, tc.item), &out, deps, nil, defaultZeroCopyMinFrameBytes, 25); err != nil {
				t.Fatalf("handleSENDWithOptions: %v", err)
			}
			if deps.compLoads != tc.wantLoads || deps.compSaves != tc.wantSave {
				t.Fatalf("loads, saves = %d, %d; want %d, %d", deps.compLoads, deps.compSaves, tc.wantLoads, tc.wantSave)
			}
			if deps.windowBatches != 1 {
				t.Fatalf("window hashes and state took %d store calls, want 1", deps.windowBatches)
			}
		})
	}
}

// failAfterWriter accepts budget bytes, then fails every write.
type failAfterWriter struct {
	budget int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if len(p) > w.budget {
		return 0, errors.New("link down")
	}
	w.budget -= len(p)
	return len(p), nil
}

// A SEND that fails partway still reports what its frames taught the policy,
// so the request saves it with its hashes.
func TestSendWindowStateReportedOnErrorExit(t *testing.T) {
	size := 4 * defaultFileFrameLogicalSize
	data := make([]byte, size)
	rand.New(rand.NewSource(3)).Read(data)
	deps, txferID, path := multiWindowFile(t, data)
	refs, _ := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	// Room for the first frame and part of the second.
	out := &failAfterWriter{budget: int(defaultFileFrameLogicalSize * 3 / 2)}
	item := sendItem{FileID: 1, Size: 3 * defaultFileFrameLogicalSize, Comp: "adapt", Path: path}
	sent, err := streamSendItem(context.Background(), unbufferedSendOutput(out), deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes)
	if err == nil {
		t.Fatalf("window succeeded through a failing writer")
	}
	if !sent.hasComp {
		t.Fatalf("a window that sent a frame before failing reported no state")
	}
}

// A file sent as one window never stores state, so memory follows in-flight
// multi-window files and not the file count.
func TestSendSingleWindowFileLeavesNoCompressionState(t *testing.T) {
	deps, txferID, path := multiWindowFile(t, compressibleBytes(4*defaultFileFrameLogicalSize+1))

	refs, errs := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Fatalf("GetFileRefs: %v", errs[0])
	}
	var raw bytes.Buffer
	item := sendItem{FileID: 1, Comp: "adapt", Path: path}
	if _, err := streamSendItem(context.Background(), unbufferedSendOutput(&raw), deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes); err != nil {
		t.Fatalf("streamSendItem: %v", err)
	}
	if _, ok := deps.LoadFileCompression(txferID, 1); ok {
		t.Fatalf("single-window file left compression state in the store")
	}
}

// Windows of one file on different connections run concurrently; each owns its
// copy of the state, so both must complete with the right bytes (and, under
// -race, without a data race).
func TestSendConcurrentWindowsOfOneFile(t *testing.T) {
	data := compressibleBytes(2 * windowTestFrames * defaultFileFrameLogicalSize)
	deps, txferID, path := multiWindowFile(t, data)

	raws := make([][]byte, 2)
	var wg sync.WaitGroup
	for i := range raws {
		wg.Add(1)
		go func() {
			defer wg.Done()
			raws[i] = sendWindow(t, deps, txferID, path, i*windowTestFrames, windowTestFrames, 2)
		}()
	}
	wg.Wait()

	var got []byte
	for i, raw := range raws {
		frames, err := decodeFrameStream(raw)
		if err != nil {
			t.Fatalf("decode window %d: %v", i, err)
		}
		if len(frames) != windowTestFrames {
			t.Fatalf("window %d has %d frames, want %d", i, len(frames), windowTestFrames)
		}
		for _, f := range frames {
			got = append(got, f.Logical...)
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("reassembled windows differ from file: got %d bytes, want %d", len(got), len(data))
	}
}

func wordyBytes(n int) []byte {
	rng := rand.New(rand.NewSource(7))
	words := make([]string, 256)
	for i := range words {
		w := make([]byte, 3+rng.Intn(6))
		for j := range w {
			w[j] = byte('a' + rng.Intn(26))
		}
		words[i] = string(w)
	}
	out := make([]byte, 0, n+16)
	for len(out) < n {
		out = append(out, words[rng.Intn(len(words))]...)
		out = append(out, ' ')
	}
	return out[:n]
}

// TestFramePrepareLatencyCoversCompression checks that a frame's
// PrepareLatency includes all of the codec's work, Close included. pierrec lz4
// in fast mode compresses its blocks while Close waits, so a timer that
// stops before Close sees a fraction of the cost. The reference is the time
// to compress the same bytes with the same settings; the margin is wide
// because the frame is taken at its slowest run and the reference at its
// fastest.
func TestFramePrepareLatencyCoversCompression(t *testing.T) {
	data := wordyBytes(int(defaultFileFrameLogicalSize))
	path := writeTempSendFile(t, data)
	for _, comp := range []string{encoding.EncodingLz4, encoding.EncodingZstd} {
		for _, mode := range []string{loadStrategyFast, loadStrategyGentle} {
			t.Run(comp+"/"+mode, func(t *testing.T) {
				const runs = 4
				ref := time.Duration(1 << 62)
				for i := 0; i < runs; i++ {
					var sink bytes.Buffer
					start := time.Now()
					w, closeW, _, err := encoding.WrapCompressedWriter(&sink, comp, mode)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := w.Write(data); err != nil {
						t.Fatal(err)
					}
					if err := closeW(); err != nil {
						t.Fatal(err)
					}
					ref = min(ref, time.Since(start))
				}
				var best time.Duration
				for i := 0; i < runs; i++ {
					fd, err := os.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					var off int64
					stats, err := streamFramePayloadBuffered(fd, &off, frameStreamArgs{
						Ctx:          context.Background(),
						FileID:       1,
						FrameSize:    int64(len(data)),
						Comp:         comp,
						Mode:         mode,
						WindowHasher: &xxh3.Hasher128{},
						Output:       io.Discard,
						Timed:        true,
					})
					_ = fd.Close()
					if err != nil {
						t.Fatal(err)
					}
					best = max(best, stats.PrepareLatency)
				}
				if best < ref/2 {
					t.Fatalf("PrepareLatency %v is under half the codec's compression time %v: Close is not timed", best, ref)
				}
			})
		}
	}
}

// frameMetricsRecorder installs sendFrameMetricsHook for one test.
type frameMetricsRecorder struct {
	mu     sync.Mutex
	frames []policy.CompressionMetrics
}

func recordFrameMetrics(t *testing.T) *frameMetricsRecorder {
	t.Helper()
	r := &frameMetricsRecorder{}
	sendFrameMetricsHook = func(m policy.CompressionMetrics) {
		r.mu.Lock()
		r.frames = append(r.frames, m)
		r.mu.Unlock()
	}
	t.Cleanup(func() { sendFrameMetricsHook = nil })
	return r
}

func (r *frameMetricsRecorder) snapshot() []policy.CompressionMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]policy.CompressionMetrics(nil), r.frames...)
}

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

// recordRateReads replaces readDeliveryRate for one test: read i returns
// script(i, reader), and is recorded.
func recordRateReads(t *testing.T, script func(i int, r *sysio.TCPRateReader) (uint64, bool)) *rateReads {
	t.Helper()
	rr := &rateReads{}
	prev := readDeliveryRate
	readDeliveryRate = func(r *sysio.TCPRateReader) (uint64, bool) {
		rr.mu.Lock()
		i := len(rr.got)
		rr.mu.Unlock()
		rate, limited := script(i, r)
		rr.mu.Lock()
		rr.got = append(rr.got, rateRead{rate, limited})
		rr.mu.Unlock()
		return rate, limited
	}
	t.Cleanup(func() { readDeliveryRate = prev })
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
// offset 0, and each record carries the kernel's reading and the rate the
// writes showed.
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
	sendOverLoopback(t, deps, nil, items...)
	if r := deps.rates; r.Windows != 2 || r.Files != 2 || r.Kernel.N != 2 || r.User.N != 2 {
		t.Fatalf("rates %+v; want 2 windows and files, each with both readings", r)
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

// TestSendTerminalFrameDecidedOnlyWhenSaved checks which frames reach the
// policy: every adaptive frame but a terminal one whose window leaves no
// state. A saved terminal frame's decision picks the next window's first
// codec, so it is timed and decided too.
func TestSendTerminalFrameDecidedOnlyWhenSaved(t *testing.T) {
	const f = defaultFileFrameLogicalSize
	tests := []struct {
		name string
		item sendItem
		want int
	}{
		{"whole file", sendItem{}, 2},
		{"first window of two frames", sendItem{Size: 2 * f}, 2},
		{"one-frame window another follows", sendItem{Offset: f, Size: f}, 1},
		{"last window", sendItem{Offset: 2 * f, Size: f}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps, txferID, path := multiWindowFile(t, compressibleBytes(3*f))
			rec := recordFrameMetrics(t)
			sendOneItem(t, deps, txferID, path, tc.item)
			ms := rec.snapshot()
			if len(ms) != tc.want {
				t.Fatalf("policy saw %d frames, want %d", len(ms), tc.want)
			}
			for i, m := range ms {
				if m.PrepareLatency <= 0 || m.WriteLatency <= 0 {
					t.Errorf("frame %d reached the policy untimed: %+v", i, m)
				}
			}
		})
	}
}

// TestFramePrepareLatencyCoversHashing checks an uncompressed frame's
// PrepareLatency includes hashing on buffered, staged direct I/O, and
// zero-copy paths. Prepare beyond reads must be at least half the fastest
// reference hash time; the frame is taken at its slowest run. Each reference
// hash runs just before a frame, so both see the same load: a reference
// timed once up front can run twice as slow as the frames under a parallel
// test run. Background codec work is measured separately and never charged
// to prepare time.
func TestFramePrepareLatencyCoversHashing(t *testing.T) {
	random := make([]byte, defaultFileFrameLogicalSize)
	rand.New(rand.NewSource(8)).Read(random)
	server, wait := drainedLoopback(t)
	defer wait()
	const runs = 4
	for _, c := range []struct {
		name string
		data []byte
	}{
		{"hash", random},
		{"small", wordyBytes(64 << 10)},
	} {
		path := writeTempSendFile(t, c.data)
		for _, p := range []struct {
			name     string
			stream   func(*os.File, *int64, frameStreamArgs) (frameStreamStats, error)
			directIO bool
		}{
			{"buffered", streamFramePayloadBuffered, false},
			{"staged direct I/O", streamFramePayloadBuffered, true},
			{"zero-copy", streamFramePayloadZeroCopy, false},
		} {
			t.Run(c.name+"/"+p.name, func(t *testing.T) {
				var best time.Duration
				ref := time.Duration(1 << 62)
				for range runs {
					var h xxh3.Hasher128
					start := time.Now()
					_, _ = h.Write(c.data)
					ref = min(ref, time.Since(start))
					fd, err := os.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					var off int64
					size := int64(len(c.data))
					stats, err := p.stream(fd, &off, frameStreamArgs{
						Ctx:           context.Background(),
						FileID:        1,
						FrameSize:     size,
						Comp:          "none",
						Mode:          loadStrategyFast,
						WindowHasher:  &xxh3.Hasher128{},
						Output:        server,
						OutputTCPConn: server,
						PipeSizeBytes: desiredPipeSizeBytes(size, size),
						DirectIO:      p.directIO,
						Timed:         true,
					})
					_ = fd.Close()
					if err != nil {
						t.Fatal(err)
					}
					best = max(best, stats.PrepareLatency-stats.ReadLatency)
				}
				if best < ref/2 {
					t.Fatalf("prepare beyond reads %v is under half the time to hash the frame, %v: that work is not timed", best, ref)
				}
			})
		}
	}
}

// slowWriter takes delay for every write.
type slowWriter struct{ delay time.Duration }

func (w slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	return len(p), nil
}

// TestZeroCopyFrameChargesFlushToWrite checks a zero-copy frame's write time
// includes flushing the response buffer ahead of it: earlier bytes the frame
// had to push out before it could splice.
func TestZeroCopyFrameChargesFlushToWrite(t *testing.T) {
	data := make([]byte, defaultFileFrameLogicalSize+1<<10)
	rand.New(rand.NewSource(6)).Read(data)
	deps, txferID, path := multiWindowFile(t, data)
	refs, errs := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Fatal(errs[0])
	}
	server, wait := drainedLoopback(t)
	const delay = 50 * time.Millisecond
	buf := bufio.NewWriterSize(slowWriter{delay}, sendOutputBufferBytes)
	_, _ = buf.WriteString("bytes an earlier item left in the response buffer\n")
	out := &sendOutput{w: buf, buf: buf, conn: server}
	out.rate = &out.ownRate
	rec := recordFrameMetrics(t)
	item := sendItem{FileID: 1, Comp: "adapt", Path: path, Mode: loadStrategyFast}
	if _, err := streamSendItem(context.Background(), out, deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes); err != nil {
		t.Fatal(err)
	}
	if n := wait(); n < defaultFileFrameLogicalSize {
		t.Fatalf("the socket got %d bytes: the first frame was not spliced", n)
	}
	ms := rec.snapshot()
	if len(ms) != 1 {
		t.Fatalf("policy saw %d frames, want 1", len(ms))
	}
	if ms[0].WriteLatency < delay {
		t.Fatalf("zero-copy frame write %v is under the %v flush it forced", ms[0].WriteLatency, delay)
	}
}

// learnedFileCompression returns a state that differs from a fresh one in both
// fields.
func learnedFileCompression() policy.FileCompression {
	p := policy.NewCompressionPolicy()
	for range 6 {
		p.Decide(policy.CompressionModeNone, policy.CompressionMetrics{
			LogicalSize: 4 << 20, WireSize: 4 << 20,
			PrepareLatency: time.Millisecond, WriteLatency: 20 * time.Millisecond,
		})
	}
	return policy.FileCompression{Mode: policy.CompressionModeLz4, Policy: *p}
}

// sendMockWindow streams one adaptive window of a file of frames frames
// through mockDeps, which holds any preloaded state, and returns the result.
func sendMockWindow(t *testing.T, deps *mockDeps, path string, offsetFrames, windowFrames int64) sentItem {
	t.Helper()
	ref, err := deps.GetFileRef("tx1", 1, path)
	if err != nil {
		t.Fatalf("GetFileRef: %v", err)
	}
	item := sendItem{FileID: 1, Offset: offsetFrames * defaultFileFrameLogicalSize, Size: windowFrames * defaultFileFrameLogicalSize, Comp: "adapt", Path: path}
	var raw bytes.Buffer
	sent, err := streamSendItem(context.Background(), unbufferedSendOutput(&raw), deps, "tx1", item, ref, defaultZeroCopyMinFrameBytes)
	if err != nil {
		t.Fatalf("streamSendItem: %v", err)
	}
	return sent
}

// A window starts from the loaded mode and policy together.
func TestSendWindowStartsFromLoadedPolicy(t *testing.T) {
	path := writeTempSendFile(t, compressibleBytes(4*defaultFileFrameLogicalSize))
	loaded := learnedFileCompression()

	// The last window loads the state but saves nothing, since no window
	// follows, so the stored state stays as it was.
	deps := &mockDeps{filePath: path, compression: map[uint64]policy.FileCompression{1: loaded}}
	if sent := sendMockWindow(t, deps, path, 3, 1); sent.hasComp {
		t.Fatalf("last window reported state %+v", sent.comp.State)
	}
	if deps.compLoads != 1 || deps.compSaves != 0 {
		t.Fatalf("loads, saves = %d, %d; want 1, 0", deps.compLoads, deps.compSaves)
	}
	if got := deps.compression[1]; got != loaded {
		t.Fatalf("stored state changed: %+v, want %+v", got, loaded)
	}

	// A window with decisions continues from the loaded policy, so it does
	// not end where the same window from a fresh policy does.
	fromLoaded := sendMockWindow(t, &mockDeps{filePath: path, compression: map[uint64]policy.FileCompression{1: loaded}}, path, 1, 2)
	fromFresh := sendMockWindow(t, &mockDeps{filePath: path}, path, 1, 2)
	if !fromLoaded.hasComp || !fromFresh.hasComp {
		t.Fatalf("windows with decisions saved no state: %v, %v", fromLoaded.hasComp, fromFresh.hasComp)
	}
	if fromLoaded.comp.State.Policy == fromFresh.comp.State.Policy {
		t.Fatalf("window ignored the loaded policy")
	}
}

// A request whose only item fails mid-window has no window hash, yet still
// saves what its frames taught the policy, through the batched store call.
func TestHandleSENDFailedOnlyItemSavesCompressionState(t *testing.T) {
	size := 4 * defaultFileFrameLogicalSize
	data := make([]byte, size)
	rand.New(rand.NewSource(5)).Read(data)
	path := writeTempSendFile(t, data)
	deps := &mockDeps{filePath: path}
	req, err := ParseRequest([]byte("SEND tx1"))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	item := `fd=1 ` + strconv.Quote(path) + ` size=` + strconv.FormatInt(3*defaultFileFrameLogicalSize, 10)
	out := &failAfterWriter{budget: int(defaultFileFrameLogicalSize * 3 / 2)}
	err = handleSENDWithOptions(context.Background(), req, framedItemBody(t, item), out, deps, nil, defaultZeroCopyMinFrameBytes, 25)
	if err == nil {
		t.Fatalf("request succeeded through a failing writer")
	}
	if deps.compSaves != 1 || deps.windowBatches != 1 {
		t.Fatalf("saves, store calls = %d, %d; want 1, 1", deps.compSaves, deps.windowBatches)
	}
	if len(deps.windowHash) != 0 {
		t.Fatalf("failed item recorded window hashes: %v", deps.windowHash)
	}
}

// The compression updates of a request reuse one pooled slice.
func TestCompUpdatesReuseTheirSlice(t *testing.T) {
	update := FileCompressionUpdate{FileID: 1}
	use := func() {
		c := compUpdatesPool.Get().(*compUpdates)
		for range 4 {
			c.s = append(c.s, update)
		}
		c.release()
	}
	use()
	if !raceEnabled {
		if allocs := testing.AllocsPerRun(100, use); allocs != 0 {
			t.Fatalf("pooled updates allocate %v times per request", allocs)
		}
	}
	first := compUpdatesPool.Get().(*compUpdates)
	defer first.release()
	second := compUpdatesPool.Get().(*compUpdates)
	if first == second {
		t.Fatal("outstanding borrowers share compression updates")
	}
	defer second.release()
	if len(first.s) != 0 || len(second.s) != 0 {
		t.Fatal("pooled updates retained entries from a previous borrower")
	}
	var none *compUpdates
	if none.list() != nil {
		t.Fatalf("nil updates have a list")
	}
	none.release()
}
