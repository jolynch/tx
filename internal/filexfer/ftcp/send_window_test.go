package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/filexfer/store"
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
		p.DecideSampled(policy.CompressionModeNone, policy.CompressionMetrics{
			LogicalSize: 4 << 20, WireSize: 4 << 20,
			PrepareLatency: time.Millisecond, WriteLatency: 20 * time.Millisecond,
		}, nil)
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
