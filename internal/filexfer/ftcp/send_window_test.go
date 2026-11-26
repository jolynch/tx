package ftcp

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/events"
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
	return sendWindowOn(t, deps, txferID, path, nil, firstFrame, frames, nsPerByte)
}

// sendWindowOn is sendWindow on a connection whose measurements src keeps
// across its SENDs, or on a fresh one when src is nil.
func sendWindowOn(t *testing.T, deps Deps, txferID, path string, src *sendRateSource, firstFrame, frames int, nsPerByte int64) []byte {
	t.Helper()
	refs, errs := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Errorf("GetFileRefs: %v", errs[0])
		return nil
	}
	var raw bytes.Buffer
	link := &rateWriter{w: &raw, nsPerByte: nsPerByte}
	item := sendItem{FileID: 1, Offset: int64(firstFrame) * defaultFileFrameLogicalSize, Size: int64(frames) * defaultFileFrameLogicalSize, Comp: "adapt", Path: path}
	out := unbufferedSendOutput(link)
	out.ownRate.src = src
	if _, err := streamSendItem(context.Background(), out, deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes); err != nil {
		t.Errorf("streamSendItem window at frame %d: %v", firstFrame, err)
	}
	return raw.Bytes()
}

// streamSendItem sends one window as a SEND of that window alone does,
// resolving its file's chain first.
func streamSendItem(ctx context.Context, out *sendOutput, deps Deps, txferID string, item sendItem, fileRef FileRef, zeroCopyMinBytes int64) (sentItem, error) {
	var chain *policy.FileChain
	if want, create := windowChain(item, fileRef); want {
		reqs := [1]ChainRequest{{FileID: item.FileID, Create: create}}
		deps.FileCompressionChains(txferID, reqs[:])
		chain = reqs[0].Chain
	}
	return streamSendWindow(ctx, out, deps, txferID, item, fileRef, chain, zeroCopyMinBytes)
}

// fileChain returns a file's chain, creating it when create is set.
func fileChain(deps Deps, txferID string, fileID uint64, create bool) *policy.FileChain {
	reqs := [1]ChainRequest{{FileID: fileID, Create: create}}
	deps.FileCompressionChains(txferID, reqs[:])
	return reqs[0].Chain
}

// storeFileCompression sets a file's shared state, creating its chain.
func storeFileCompression(t *testing.T, deps Deps, txferID string, fileID uint64, st policy.FileCompression) {
	t.Helper()
	c := fileChain(deps, txferID, fileID, true)
	if c == nil {
		t.Fatalf("no chain for file %d", fileID)
	}
	held := c.TryHold()
	*held = st
	c.Release()
}

// loadFileCompression returns a copy of a file's shared state, and false
// when the file has no chain. Nothing else may hold the chain.
func loadFileCompression(deps Deps, txferID string, fileID uint64) (policy.FileCompression, bool) {
	c := fileChain(deps, txferID, fileID, false)
	if c == nil {
		return policy.FileCompression{}, false
	}
	held := c.TryHold()
	st := *held
	c.Release()
	return st, true
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
		st, ok := loadFileCompression(deps, txferID, 1)
		if w < windows-1 && !ok {
			t.Fatalf("window %d saved no state for the next", w)
		}
		next = policy.FrameCompTokenForMode(st.Mode)
		switched = switched || next != comps[len(comps)-1]
	}
	t.Logf("a window's last frame chose a new codec for the next: %v", switched)
}

// sendOneItem streams one adaptive item of file 1 through a fresh output.
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
	return sent
}

// A window creates its file's chain only when another window follows within
// the manifest size. The last window of a file, a resumed tail window, and a
// whole-file SEND would otherwise leave an entry that no ACK frees. A request
// resolves chains before it opens files, so a file that shrank since its
// manifest gets one; it stays until the transfer is reaped.
func TestSendWindowsCreateChainOnlyWhenAnotherWindowFollows(t *testing.T) {
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
		{"shrunk file, window ends at the new end", size / 2, sendItem{Size: size / 2}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps, txferID, path := multiWindowFile(t, compressibleBytes(size))
			if tc.shrinkTo > 0 {
				if err := os.Truncate(path, tc.shrinkTo); err != nil {
					t.Fatalf("Truncate: %v", err)
				}
			}
			sendOneItem(t, deps, txferID, path, tc.item)
			if _, ok := loadFileCompression(deps, txferID, 1); ok != tc.wantSave {
				t.Fatalf("store holds state = %v, want %v", ok, tc.wantSave)
			}
		})
	}
}

// requestChains resolves only the windows that want a chain, hands each
// record its own, and keeps its slices in a pool: a request allocates
// nothing once its file has a chain, two requests in flight never share
// slices, and a released set keeps no chain alive.
func TestRequestChainsReuseTheirSlices(t *testing.T) {
	const frame = defaultFileFrameLogicalSize
	deps := &mockDeps{}
	records := []sendRequestRecord{
		{FileID: 1, Size: frame},                // first window: creates
		{FileID: 2, Size: frame, Codec: 1},      // fixed codec: none
		{FileID: 1, Offset: frame, Size: frame}, // middle window: creates
		{FileID: 3},                             // whole file: none
		{FileID: 1, Offset: 2 * frame},          // tail window: takes
	}
	refs := []FileRef{{FileSize: 3 * frame}, {FileSize: 3 * frame}, {FileSize: 3 * frame}, {FileSize: frame}, {FileSize: 3 * frame}}
	refErrs := make([]error, len(records))
	use := func() *sendChains { return requestChains(deps, "t", loadStrategyFast, records, refs, refErrs) }

	c := use()
	want := deps.chains[1]
	if want == nil || deps.chainCalls != 1 || len(c.reqs) != 3 {
		t.Fatalf("resolved %d windows in %d calls, chain %p; want 3 in 1", len(c.reqs), deps.chainCalls, want)
	}
	for i, wantChain := range []*policy.FileChain{want, nil, want, nil, want} {
		if got := c.chain(i); got != wantChain {
			t.Fatalf("record %d got chain %p, want %p", i, got, wantChain)
		}
	}
	c.release()
	if !raceEnabled {
		if n := testing.AllocsPerRun(100, func() { use().release() }); n != 0 {
			t.Fatalf("resolving a request's chains allocated %v times", n)
		}
	}

	first, second := use(), use()
	if first == second || &first.reqs[0] == &second.reqs[0] {
		t.Fatal("requests in flight share chain slices")
	}
	reqs := first.reqs
	first.release()
	second.release()
	if reqs[:cap(reqs)][0].Chain != nil {
		t.Fatal("a released set kept a chain alive")
	}
	if requestChains(deps, "t", loadStrategyFast, records[1:2], refs[1:2], refErrs[1:2]) != nil {
		t.Fatal("a request without chained windows took a set")
	}
	var none *sendChains
	if none.chain(0) != nil {
		t.Fatal("a nil set has a chain")
	}
	none.release()
}

// A request takes a file's chain only for a window another precedes or
// follows and creates it only for a window another follows. It resolves
// all its windows' chains in one store call, and records its window hashes
// in one more.
func TestHandleSENDCompressionChainTakenOnlyForSeveralWindows(t *testing.T) {
	const frames = 3
	size := int64(frames) * defaultFileFrameLogicalSize
	path := writeTempSendFile(t, compressibleBytes(size))
	quoted := strconv.Quote(path)
	half := strconv.FormatInt(size/2, 10)
	tests := []struct {
		name                   string
		item                   string
		wantCalls, wantCreates int
	}{
		{"single window", `fd=1 ` + quoted, 0, 0},
		{"first window", `fd=1 ` + quoted + ` size=` + half, 1, 1},
		{"middle window", `fd=1 ` + quoted + ` offset=` + strconv.FormatInt(size/4, 10) + ` size=` + half, 1, 1},
		{"resumed tail window", `fd=1 ` + quoted + ` offset=` + half, 1, 0},
		{"two windows", `fd=1 ` + quoted + ` size=` + half + "\n" + `fd=1 ` + quoted + ` offset=` + half, 1, 1},
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
			if deps.chainCalls != tc.wantCalls || deps.chainCreates != tc.wantCreates {
				t.Fatalf("chain calls, creates = %d, %d; want %d, %d", deps.chainCalls, deps.chainCreates, tc.wantCalls, tc.wantCreates)
			}
			if deps.windowBatches != 1 {
				t.Fatalf("window hashes and rates took %d store calls, want 1", deps.windowBatches)
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

// A SEND that fails partway leaves what its decided frames taught the policy
// on the file's chain.
func TestSendWindowStateKeptOnErrorExit(t *testing.T) {
	size := 4 * defaultFileFrameLogicalSize
	data := make([]byte, size)
	rand.New(rand.NewSource(3)).Read(data)
	deps, txferID, path := multiWindowFile(t, data)
	refs, _ := deps.GetFileRefs(txferID, []FileLookup{{FileID: 1, Path: path}})
	// Room for the first frame and part of the second.
	out := &failAfterWriter{budget: int(defaultFileFrameLogicalSize * 3 / 2)}
	item := sendItem{FileID: 1, Size: 3 * defaultFileFrameLogicalSize, Comp: "adapt", Path: path}
	if _, err := streamSendItem(context.Background(), unbufferedSendOutput(out), deps, txferID, item, refs[0], defaultZeroCopyMinFrameBytes); err == nil {
		t.Fatalf("window succeeded through a failing writer")
	}
	if st, ok := loadFileCompression(deps, txferID, 1); !ok || st == (policy.FileCompression{}) {
		t.Fatalf("a window that sent a frame before failing left no state: %v", ok)
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
	if _, ok := loadFileCompression(deps, txferID, 1); ok {
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

// sendSlidingWindows sends file 1 as one-frame windows, inFlight at a time
// on as many connections, each taking the next window when its last one
// ends, as the client's sliding window does. It returns each frame's codec,
// in file order.
func sendSlidingWindows(t *testing.T, deps Deps, txferID, path string, frames, inFlight int, nsPerByte int64) []string {
	t.Helper()
	comps := make([]string, frames)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range inFlight {
		wg.Add(1)
		go func() {
			defer wg.Done()
			src := &sendRateSource{}
			for {
				w := int(next.Add(1) - 1)
				if w >= frames {
					return
				}
				got, err := frameComps(sendWindowOn(t, deps, txferID, path, src, w, 1, nsPerByte))
				if err != nil || len(got) != 1 {
					t.Errorf("window %d: %d frames: %v", w, len(got), err)
					return
				}
				comps[w] = got[0]
			}
		}()
	}
	wg.Wait()
	return comps
}

// Concurrent one-frame windows of a file advance one shared policy, so the
// file adapts as one stream of its frames would. On a slow link, where zstd
// sends compressible data several times faster, the file switches within
// its first half and sends most of its second half compressed. On a fast
// link, where compression costs more than it saves, the file stays
// uncompressed. The race detector slows the codecs past the slow link, so
// the slow link goes unchecked there.
func TestSendConcurrentSmallWindowsAdapt(t *testing.T) {
	const frames, inFlight = 64, 8
	for _, tc := range []struct {
		name              string
		nsPerByte         int64
		minLate, maxShare float64
	}{
		{"slow link", 20, 0.4, 1},
		{"fast link", 0, 0, 0.2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if raceEnabled {
				t.Skip("the race detector slows the codecs, which skews both cases' timing")
			}
			deps, txferID, path := multiWindowFile(t, compressibleBytes(frames*defaultFileFrameLogicalSize))
			comps := sendSlidingWindows(t, deps, txferID, path, frames, inFlight, tc.nsPerByte)
			compressed, late := 0, 0
			for i, c := range comps {
				if c != "none" {
					compressed++
					if i >= frames/2 {
						late++
					}
				}
			}
			t.Logf("codecs by frame: %v", comps)
			// A check between compressed frames sends at most
			// MaxCheckFrames frames in none, however many windows are in
			// flight.
			for i, run := 0, 0; i < len(comps); i++ {
				if comps[i] == "none" {
					run++
					continue
				}
				if run > policy.MaxCheckFrames && i > run && comps[i-run-1] != "none" {
					t.Errorf("a check sent %d frames in none before frame %d, want at most %d", run, i, policy.MaxCheckFrames)
				}
				run = 0
			}
			if share := float64(late) / (frames / 2); share < tc.minLate {
				t.Errorf("sent %.0f%% of the second half compressed, want at least %.0f%%", 100*share, 100*tc.minLate)
			}
			if share := float64(compressed) / frames; share > tc.maxShare {
				t.Errorf("sent %.0f%% of frames compressed, want at most %.0f%%", 100*share, 100*tc.maxShare)
			}
		})
	}
}

// decideChained decides a frame only when it holds the chain and the frame
// was sent in the chain's codec; otherwise the window follows the chain's
// codec. None of the three cases allocates, which keeps a chained frame's
// per-frame path free of allocations.
func TestDecideChainedFeedsOnlyMatchingFrames(t *testing.T) {
	m := policy.CompressionMetrics{LogicalSize: defaultFileFrameLogicalSize, WireSize: defaultFileFrameLogicalSize,
		PrepareLatency: time.Millisecond, WriteLatency: 20 * time.Millisecond}
	rate := &sendRate{src: &sendRateSource{}}
	var chain policy.FileChain
	set := func(st policy.FileCompression) {
		*chain.TryHold() = st
		chain.Release()
	}
	var local policy.FileCompression
	if _, fed := decideChained(&chain, &local, rate, chainedFrame{Mode: policy.CompressionModeNone, Metrics: m}); !fed || local == (policy.FileCompression{}) {
		t.Fatalf("a frame in the chain's codec was not decided: fed=%v local=%+v", fed, local)
	}
	if st := *chain.TryHold(); st != local {
		t.Fatalf("the window's copy %+v differs from the chain's %+v", local, st)
	}
	chain.Release()

	// A sibling moved the codec to lz4 while a none frame was in flight.
	moved := learnedFileCompression()
	set(moved)
	if _, fed := decideChained(&chain, &local, rate, chainedFrame{Mode: policy.CompressionModeNone, Metrics: m}); fed || local != moved {
		t.Fatalf("a frame in a codec the chain left was decided (%v), or the window did not follow: %+v", fed, local)
	}

	// A sibling holds the chain: the frame is skipped and nothing waits.
	chain.TryHold()
	local = policy.FileCompression{}
	if _, fed := decideChained(&chain, &local, rate, chainedFrame{Mode: policy.CompressionModeLz4, Metrics: m}); fed || local != (policy.FileCompression{}) {
		t.Fatalf("a frame was decided on a held chain")
	}
	chain.Release()

	for _, tc := range []struct {
		name string
		mode policy.CompressionMode
		hold bool
	}{
		{"decided", policy.CompressionModeLz4, false},
		{"codec moved", policy.CompressionModeZstdLevel1, false},
		{"held", policy.CompressionModeLz4, true},
	} {
		if tc.hold {
			chain.TryHold()
		}
		if n := testing.AllocsPerRun(100, func() {
			if !tc.hold {
				set(moved)
			}
			decideChained(&chain, &local, rate, chainedFrame{Mode: tc.mode, Metrics: m})
		}); n != 0 {
			t.Errorf("%s: a chained decision allocated %v times", tc.name, n)
		}
		if tc.hold {
			chain.Release()
		}
	}
}

// A recovery check judges frames flushed and timed on their own. When one
// window's decision starts a check on the file's chain, a sibling whose frame
// began before it sent that frame untimed, in the home codec the check's
// baseline also uses. That frame must not become a baseline frame, where
// the fastest one can lower the bar: it is skipped, the sibling adopts the
// chain's state, and its next frame, measured for recovery, is decided.
func TestDecideChainedRecoveryNeedsMeasuredFrames(t *testing.T) {
	p, m, d := zstdRecoveryBaseline(t)
	if !p.RecoveryActive() {
		t.Fatal("setup started no check")
	}
	var chain policy.FileChain
	started := policy.FileCompression{Mode: d.Next, Policy: *p}
	*chain.TryHold() = started
	chain.Release()
	rate := &sendRate{src: &sendRateSource{}}

	// The sibling's view predates the check, and its untimed frame looks
	// fast: buffered output left its write time out.
	untimed := m
	untimed.WriteLatency = time.Microsecond
	sibling := policy.FileCompression{Mode: d.Next}
	if _, fed := decideChained(&chain, &sibling, rate, chainedFrame{Mode: d.Next, Metrics: untimed}); fed {
		t.Fatal("a frame not measured for recovery fed the check")
	}
	if st := *chain.TryHold(); st != started {
		t.Fatalf("a skipped frame changed the chain")
	}
	chain.Release()
	if !sibling.Policy.RecoveryActive() {
		t.Fatal("the sibling did not adopt the running check")
	}

	// Its next frame starts with the check in view, so it is timed and fed.
	if _, fed := decideChained(&chain, &sibling, rate, chainedFrame{
		Mode: sibling.Mode, Recovering: sibling.Policy.RecoveryActive(), Metrics: m}); !fed {
		t.Fatal("a frame measured for recovery was skipped")
	}
	if st := *chain.TryHold(); st == started {
		t.Fatal("the measured frame did not reach the check")
	}
	chain.Release()
}

// A check frame that a held chain kept from the check is charged once and
// goes back to the chain, so the check still finishes on frames claimed
// again; a window that claimed nothing sends home's codec and is charged
// nothing.
func TestSettleCheckFrameKeepsChecksFinishing(t *testing.T) {
	p, m, d := zstdRecoveryBaseline(t)
	for d.Reason == "recovery-baseline" {
		d = p.DecideSampled(d.Next, m, nil)
	}
	if !p.CheckRunning() {
		t.Fatalf("no check started: %+v", d)
	}
	var chain policy.FileChain
	*chain.TryHold() = policy.FileCompression{Mode: d.Next, Policy: *p}
	chain.Release()
	rate := &sendRate{src: &sendRateSource{}}
	check := m
	check.WireSize, check.PrepareLatency, check.WriteLatency = check.LogicalSize, 0, 30*time.Millisecond
	frame := func(local *policy.FileCompression, claim *policy.CheckClaim) time.Duration {
		mode, recovering := local.Mode, local.Policy.RecoveryActive()
		if local.Policy.CheckRunning() && mode != local.Policy.Home() {
			if *claim = chain.ClaimCheckFrame(); !claim.Held() {
				mode, recovering = local.Policy.Home(), false
			}
		}
		decision, fed := decideChained(&chain, local, rate, chainedFrame{Mode: mode, Recovering: recovering, Metrics: check})
		return settleCheckFrame(&chain, claim, fed, &local.Policy, decision.RecoveryLoss, check)
	}

	// Every window claims a check frame, and a sibling holds the chain
	// when they finish.
	windows := make([]policy.FileCompression, policy.MaxCheckFrames)
	claims := make([]policy.CheckClaim, len(windows))
	for i := range windows {
		windows[i] = policy.FileCompression{Mode: d.Next, Policy: *p}
	}
	chain.TryHold()
	for i := range windows {
		if loss := frame(&windows[i], &claims[i]); loss <= 0 || loss != windows[i].Policy.CheckFrameLoss(check) || claims[i].Held() {
			t.Fatalf("window %d: a lost check frame was charged %v, or kept its claim", i, loss)
		}
	}
	chain.Release()
	var unclaimed policy.CheckClaim
	extra := policy.FileCompression{Mode: d.Next, Policy: *p}
	for range policy.MaxCheckFrames {
		frame(&windows[0], &claims[0])
		if !chain.TryHold().Policy.CheckRunning() {
			chain.Release()
			break
		}
		chain.Release()
	}
	if st := chain.TryHold(); st.Policy.CheckRunning() {
		t.Fatal("the check never finished after its frames were lost")
	}
	chain.Release()
	if loss := frame(&extra, &unclaimed); loss != 0 || unclaimed.Held() {
		t.Fatalf("a frame of a stale view was charged %v or claimed a frame", loss)
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
// PrepareLatency includes all of the codec's work, which runs after the frame
// is read. The reference is the time to compress the same bytes with the same
// settings; the margin is wide because the frame is taken at its slowest run
// and the reference at its fastest.
func TestFramePrepareLatencyCoversCompression(t *testing.T) {
	data := wordyBytes(int(defaultFileFrameLogicalSize))
	path := writeTempSendFile(t, data)
	for _, comp := range []string{encoding.EncodingLz4, encoding.EncodingZstd} {
		for _, mode := range []string{loadStrategyFast, loadStrategyGentle} {
			t.Run(comp+"/"+mode, func(t *testing.T) {
				const runs = 4
				ref := time.Duration(1 << 62)
				for i := 0; i < runs; i++ {
					// A window's first frame compresses on one goroutine.
					opts := encoding.FrameCompression{Codec: comp, MaxConcurrency: 1}
					dst := make([]byte, 0, 2*len(data))
					start := time.Now()
					if _, _, err := encoding.AppendCompressedFrame(dst, data, opts); err != nil {
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

// sendMockWindow streams one adaptive window of file 1 through deps, which
// holds any preset chain, and returns the file's state afterward.
func sendMockWindow(t *testing.T, deps *mockDeps, path string, offsetFrames, windowFrames int64) policy.FileCompression {
	t.Helper()
	ref, err := deps.GetFileRef("tx1", 1, path)
	if err != nil {
		t.Fatalf("GetFileRef: %v", err)
	}
	item := sendItem{FileID: 1, Offset: offsetFrames * defaultFileFrameLogicalSize, Size: windowFrames * defaultFileFrameLogicalSize, Comp: "adapt", Path: path}
	var raw bytes.Buffer
	if _, err := streamSendItem(context.Background(), unbufferedSendOutput(&raw), deps, "tx1", item, ref, defaultZeroCopyMinFrameBytes); err != nil {
		t.Fatalf("streamSendItem: %v", err)
	}
	st, _ := loadFileCompression(deps, "tx1", 1)
	return st
}

// mockWithChain returns mockDeps serving path whose file 1 has a chain
// holding st.
func mockWithChain(path string, st policy.FileCompression) *mockDeps {
	c := new(policy.FileChain)
	*c.TryHold() = st
	c.Release()
	return &mockDeps{filePath: path, chains: map[uint64]*policy.FileChain{1: c}}
}

// A window starts from the chain's mode and policy together.
func TestSendWindowStartsFromLoadedPolicy(t *testing.T) {
	path := writeTempSendFile(t, compressibleBytes(4*defaultFileFrameLogicalSize))
	loaded := learnedFileCompression()

	// The last window takes the chain but decides nothing, since its one
	// frame is terminal and no window follows, so the state stays as it was.
	deps := mockWithChain(path, loaded)
	if got := sendMockWindow(t, deps, path, 3, 1); got != loaded {
		t.Fatalf("last window changed the state: %+v, want %+v", got, loaded)
	}
	if deps.chainCalls != 2 || deps.chainCreates != 0 {
		t.Fatalf("chain calls, creates = %d, %d; want 2 (window and check), 0", deps.chainCalls, deps.chainCreates)
	}

	// A window with decisions continues from the chain's policy, so it does
	// not end where the same window from a fresh policy does.
	fromLoaded := sendMockWindow(t, mockWithChain(path, loaded), path, 1, 2)
	fromFresh := sendMockWindow(t, &mockDeps{filePath: path}, path, 1, 2)
	if fromLoaded == loaded || fromFresh == (policy.FileCompression{}) {
		t.Fatalf("windows with decisions left their state as it was")
	}
	if fromLoaded.Policy == fromFresh.Policy {
		t.Fatalf("window ignored the loaded policy")
	}
}

// A request whose only item fails mid-window has no window hash, yet its
// frames still teach the file's chain, and its rates reach the store in one
// call.
func TestHandleSENDFailedOnlyItemKeepsCompressionState(t *testing.T) {
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
	if st, _ := loadFileCompression(deps, "tx1", 1); deps.chainCreates != 1 || st == (policy.FileCompression{}) || deps.windowBatches != 1 {
		t.Fatalf("chains, store calls = %d, %d, state %+v; want 1, 1, a learned state", deps.chainCreates, deps.windowBatches, st)
	}
	if len(deps.windowHash) != 0 {
		t.Fatalf("failed item recorded window hashes: %v", deps.windowHash)
	}
}

// A compressed frame reports its encoder's wall time and goroutines so the
// window can decide the next frame's width; an uncompressed one reports
// neither. A narrow frame uses one goroutine; a wide 4 MiB lz4 frame on an
// idle budget uses more.
func TestBufferedFrameReportsCompressConcurrency(t *testing.T) {
	data := wordyBytes(int(defaultFileFrameLogicalSize))
	path := writeTempSendFile(t, data)
	for _, comp := range []string{"none", encoding.EncodingLz4, encoding.EncodingZstd} {
		for _, wide := range []bool{false, true} {
			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			var off int64
			var out bytes.Buffer
			stats, err := streamFramePayloadBuffered(fd, &off, frameStreamArgs{
				Ctx: context.Background(), FileID: 1, FrameSize: int64(len(data)), Comp: comp,
				Mode: loadStrategyFast, WindowHasher: xxh3.New128(), Output: &out, CompressWide: wide,
			})
			_ = fd.Close()
			if err != nil {
				t.Fatalf("%s wide=%v: %v", comp, wide, err)
			}
			c, lat := stats.CompressConcurrency, stats.CompressLatency
			switch {
			case comp == "none":
				if c != 0 || lat != 0 {
					t.Errorf("none: concurrency %d, latency %v; want 0, 0", c, lat)
				}
			case lat <= 0:
				t.Errorf("%s wide=%v: no compression latency", comp, wide)
			case !wide && c != 1:
				t.Errorf("%s narrow frame used %d goroutines", comp, c)
			case wide && runtime.GOMAXPROCS(0) >= 4 && c < 2:
				t.Errorf("%s wide frame on an idle %d-CPU budget used %d goroutine", comp, runtime.GOMAXPROCS(0), c)
			}
		}
	}
}

// windowEncoderConcurrency sends one item of data through streamSendItem and
// returns the encoder goroutines of each frame, from its window events.
func windowEncoderConcurrency(t *testing.T, data []byte, comp, mode string) []int {
	t.Helper()
	path := writeTempSendFile(t, data)
	sink := events.NewSink()
	var mu sync.Mutex
	var conc []int
	sink.Subscribe(func(e events.Event) {
		if e.Name != "window" {
			return
		}
		for _, f := range e.Fields {
			if f.Key == "enc_conc" {
				mu.Lock()
				conc = append(conc, f.Val.(int))
				mu.Unlock()
			}
		}
	})
	ctx := events.WithScope(context.Background(), events.Scope{Sink: sink, Conn: 1})
	deps := &mockDeps{filePath: path}
	var out bytes.Buffer
	if _, err := streamSendItem(ctx, unbufferedSendOutput(&out), deps, "tx1",
		sendItem{FileID: 1, Comp: comp, Path: path, Mode: mode},
		mustFileRef(t, deps, 1, path), zeroCopyOff); err != nil {
		t.Fatalf("streamSendItem: %v", err)
	}
	frames, err := decodeFrameStream(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, f := range frames {
		got = append(got, f.Logical...)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("%s %s: decoded %d bytes, not the %d sent", comp, mode, len(got), len(data))
	}
	mu.Lock()
	defer mu.Unlock()
	return conc
}

// A window widens only after a frame shows its data compresses slowly. Its
// first frame, every frame of random data, which compresses far faster than
// the threshold, and every frame of a gentle window use one goroutine; slow
// text frames after the first go wide when cores are free.
func TestStreamSendItemWidensOnlySlowCompression(t *testing.T) {
	const frames = 4
	random := make([]byte, frames*defaultFileFrameLogicalSize)
	_, _ = cryptorand.Read(random)
	text := wordyBytes(frames * int(defaultFileFrameLogicalSize))
	for _, comp := range []string{encoding.EncodingLz4, encoding.EncodingZstd} {
		t.Run(comp, func(t *testing.T) {
			if conc := windowEncoderConcurrency(t, text, comp, loadStrategyGentle); !slices.Equal(conc, []int{1, 1, 1, 1}) {
				t.Errorf("gentle text frames used %v goroutines, want one each", conc)
			}
			conc := windowEncoderConcurrency(t, text, comp, loadStrategyFast)
			if len(conc) != frames || conc[0] != 1 {
				t.Fatalf("fast text frames used %v goroutines, want %d frames starting at 1", conc, frames)
			}
			if runtime.GOMAXPROCS(0) >= 4 && !raceEnabled {
				for i, c := range conc[1:] {
					if c < 2 {
						t.Errorf("text frame %d used %d goroutine after slow frames on an idle %d-CPU budget: %v", i+1, c, runtime.GOMAXPROCS(0), conc)
					}
				}
			}
			// Under the race detector, the encoders run slowly enough that
			// even random data can fall below the threshold.
			if !raceEnabled {
				if conc := windowEncoderConcurrency(t, random, comp, loadStrategyFast); !slices.Equal(conc, []int{1, 1, 1, 1}) {
					t.Errorf("random frames used %v goroutines, want one each", conc)
				}
			}
		})
	}
}
