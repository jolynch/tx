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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/zeebo/xxh3"
)

func sampleFrameMetrics() policy.CompressionMetrics {
	return policy.CompressionMetrics{LogicalSize: defaultFileFrameLogicalSize,
		PrepareLatency: time.Millisecond, WriteLatency: 8 * time.Millisecond}
}

// pinSampleCost sets the process's average sample cost for one test, since
// earlier tests' samples, slow under -race, leave it wherever they ended.
func pinSampleCost(t *testing.T, d time.Duration) {
	t.Helper()
	old := sendSampleCost.Swap(int64(d))
	rejected := sendSampleRejected.Swap(0)
	t.Cleanup(func() {
		sendSampleCost.Store(old)
		sendSampleRejected.Store(rejected)
	})
}

func fakeCodecSamples() encoding.CodecSamples {
	return encoding.CodecSamples{LogicalSize: encoding.CodecSampleBytes,
		Lz4:  encoding.CodecMeasurement{WireSize: encoding.CodecSampleBytes / 2, Elapsed: time.Microsecond},
		Zstd: encoding.CodecMeasurement{WireSize: encoding.CodecSampleBytes / 4, Elapsed: 2 * time.Microsecond}}
}

func waitSample(t *testing.T, sampler *sendSampler) encoding.CodecSamples {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if result, ok := sampler.take(); ok {
			return result
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background sample did not complete")
	return encoding.CodecSamples{}
}

// A blocked worker never delays capture, polling, or cleanup. Its input is
// an owned copy, so the sender may overwrite its read buffer immediately.
func TestSendSamplingNonblockingOwnership(t *testing.T) {
	pinSampleCost(t, time.Millisecond)
	slots := make(chan struct{}, 1)
	sampler := sendSampler{slots: slots}
	p := policy.NewCompressionPolicy()
	job := sampler.request(context.Background(), sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}, p, sampleFrameMetrics())
	if job == nil {
		t.Fatal("sample was not admitted")
	}
	started, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	t.Cleanup(func() { resumeOnce.Do(func() { close(resume) }); sampler.close() })
	var wrong atomic.Bool
	job.measure = func(data []byte, _ encoding.SampleSet) (encoding.CodecSamples, error) {
		close(started)
		<-resume
		if !bytes.Equal(data, bytes.Repeat([]byte{7}, encoding.CodecSampleBytes)) {
			wrong.Store(true)
		}
		close(finished)
		return fakeCodecSamples(), nil
	}
	data := bytes.Repeat([]byte{7}, encoding.CodecSampleBytes)
	job.capture(data)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start")
	}
	clear(data)
	if _, ready := sampler.take(); ready {
		t.Fatal("blocked worker returned a result")
	}
	closed := make(chan struct{})
	go func() { sampler.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("cleanup waited for the background codec")
	}
	if len(slots) != 1 {
		t.Fatal("abandoned worker lost ownership of its slot")
	}
	resumeOnce.Do(func() { close(resume) })
	<-finished
	deadline := time.Now().Add(time.Second)
	for len(slots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(slots) != 0 || wrong.Load() {
		t.Fatalf("worker leaked its slot or saw overwritten bytes: slots=%d wrong=%v", len(slots), wrong.Load())
	}
}

func TestSendSamplingRequiresUsefulRemainingWork(t *testing.T) {
	p := policy.NewCompressionPolicy()
	need := p.BackgroundFramesNeeded(sendSniffEvery)
	// sampleFrameMetrics sends a frame in 9 ms.
	for _, tc := range []struct {
		name    string
		work    sampleWork
		metrics policy.CompressionMetrics
		cost    time.Duration
		want    bool
	}{
		{"no frame after a switch", sampleWork{need - 1, 1 << 30, 64, 64 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), time.Millisecond, false},
		{"one frame after a switch", sampleWork{need, 1 << 30, 64, 64 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), time.Millisecond, true},
		{"no rate yet", sampleWork{256, 1 << 30, 64, 64 * defaultFileFrameLogicalSize}, policy.CompressionMetrics{}, time.Millisecond, false},
		{"no time", sampleWork{256, 1 << 30, 64, 64 * defaultFileFrameLogicalSize}, policy.CompressionMetrics{LogicalSize: defaultFileFrameLogicalSize, WriteLatency: time.Nanosecond}, time.Millisecond, false},
		{"slow sample would arrive late", sampleWork{256, 64 << 20, 64, 64 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), time.Second, false},
		{"large slow copy", sampleWork{256, 1 << 30, 64, 64 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), time.Millisecond, true},
		{"no decided frame left in the window", sampleWork{256, 1 << 30, 0, 0 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), time.Millisecond, false},
		{"sample outlasts its window", sampleWork{256, 1 << 30, 2, 2 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), 20 * time.Millisecond, false},
		{"sample fits its window twice", sampleWork{256, 1 << 30, 5, 5 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), 20 * time.Millisecond, false},
		{"sample fits its window four times", sampleWork{256, 1 << 30, 10, 10 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), 20 * time.Millisecond, true},
		{"sample fits its window once", sampleWork{256, 1 << 30, 3, 3 * defaultFileFrameLogicalSize}, sampleFrameMetrics(), 20 * time.Millisecond, false},
		{"one-frame window on a slow link", sampleWork{256, 1 << 30, 1, defaultFileFrameLogicalSize}, sampleFrameMetrics(), 2 * time.Millisecond, true},
		// A short frame sends in a fraction of a full frame's time.
		{"one short frame on a slow link", sampleWork{256, 1 << 30, 1, 256 << 10}, sampleFrameMetrics(), 2 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sampleUseful(tc.work, need, tc.metrics, tc.cost); got != tc.want {
				t.Fatalf("sampleUseful=%v want %v", got, tc.want)
			}
		})
	}
	// Frames, not bytes: a window's last frame can be short, so a 6 MiB
	// window and then the 7 MiB rest of a 13 MiB file is 4+2 then 4+3, four
	// frames.
	const mib = 1 << 20
	for _, tc := range []struct {
		cursor, windowEnd, fileEnd int64
		want                       int
	}{
		{0, 13 * mib, 13 * mib, 4},
		{0, 6 * mib, 13 * mib, 4},
		{4 * mib, 6 * mib, 13 * mib, 3},
		{12 * mib, 13 * mib, 13 * mib, 1},
		{13 * mib, 13 * mib, 13 * mib, 0},
	} {
		if got := framesLeft(tc.cursor, tc.windowEnd, tc.fileEnd); got != tc.want {
			t.Errorf("framesLeft(%d, %d, %d) = %d, want %d", tc.cursor, tc.windowEnd, tc.fileEnd, got, tc.want)
		}
	}
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	sampler := sendSampler{slots: slots}
	if job := sampler.request(context.Background(), sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}, p, sampleFrameMetrics()); job != nil {
		t.Fatal("a busy worker queued a sample")
	}
	<-slots
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if job := sampler.request(ctx, sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}, p, sampleFrameMetrics()); job != nil {
		t.Fatal("a canceled request started a sample")
	}
	if allocs := testing.AllocsPerRun(20, func() {
		if sampler.request(context.Background(), sampleWork{4, 15 << 20, 4, 4 * defaultFileFrameLogicalSize}, p, sampleFrameMetrics()) != nil {
			panic("small file sampled")
		}
	}); allocs != 0 {
		t.Fatalf("ineligible frames allocate %.1f times", allocs)
	}
}

// A completed result may wait for its frame while another stream samples.
// Canceled or abandoned queued jobs must return their buffers without encoding.
func TestSendSamplingWorkerAndResultBudgets(t *testing.T) {
	pinSampleCost(t, time.Millisecond)
	for _, end := range []string{"result", "cancel", "abandon"} {
		t.Run(end, func(t *testing.T) {
			slots, workers := make(chan struct{}, 2), make(chan struct{}, 1)
			first := sendSampler{slots: slots, workers: workers}
			second := sendSampler{slots: slots, workers: workers}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() {
				once.Do(func() { close(resume) })
				first.close()
				second.close()
			})
			work := sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}
			one := first.request(context.Background(), work, policy.NewCompressionPolicy(), sampleFrameMetrics())
			two := second.request(ctx, work, policy.NewCompressionPolicy(), sampleFrameMetrics())
			if one == nil || two == nil {
				t.Fatal("two result slots did not admit two jobs")
			}
			one.measure = func([]byte, encoding.SampleSet) (encoding.CodecSamples, error) {
				close(started)
				<-resume
				return fakeCodecSamples(), nil
			}
			var calls atomic.Int32
			two.measure = func([]byte, encoding.SampleSet) (encoding.CodecSamples, error) {
				calls.Add(1)
				return fakeCodecSamples(), nil
			}
			data := bytes.Repeat([]byte{7}, encoding.CodecSampleBytes)
			one.capture(data)
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("first worker did not start")
			}
			two.capture(data)
			if end == "abandon" {
				second.close()
			}
			if end == "cancel" {
				cancel()
				deadline := time.Now().Add(2 * time.Second)
				for two.state.Load() != sampleDone && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if two.state.Load() != sampleDone {
					t.Fatal("canceled job waited for the busy worker")
				}
				if _, ok := second.take(); ok || calls.Load() != 0 || len(slots) != 1 {
					t.Fatal("canceled job encoded data, published a result, or held its buffer")
				}
			}
			once.Do(func() { close(resume) })
			if end == "result" {
				waitSample(t, &second) // first's result still owns its slot.
				if len(slots) != 1 {
					t.Fatal("waiting result lost its slot")
				}
			}
			waitSample(t, &first)
			deadline := time.Now().Add(2 * time.Second)
			for len(slots) != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if end == "abandon" && calls.Load() != 0 {
				t.Fatal("abandoned queued job ran a codec")
			}
			if len(slots) != 0 || len(workers) != 0 {
				t.Fatal("sample buffers or workers leaked")
			}
		})
	}
}

// Every path supplies the same already-read prefix, including zero-copy's
// small hash-pipe reads. No sampler can hold or read a file descriptor.
func TestSendSamplingCapturesEveryReadPath(t *testing.T) {
	pinSampleCost(t, time.Millisecond)
	data := make([]byte, defaultFileFrameLogicalSize)
	rand.New(rand.NewSource(32)).Read(data)
	path := writeTempSendFile(t, data)
	server, wait := drainedLoopback(t)
	defer wait()
	for _, tc := range []struct {
		name   string
		direct bool
		stream func(*os.File, *int64, frameStreamArgs) (frameStreamStats, error)
	}{
		{"buffered", false, streamFramePayloadBuffered},
		{"staged", true, streamFramePayloadBuffered},
		{"zero-copy", false, streamFramePayloadZeroCopy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sampler := sendSampler{slots: make(chan struct{}, 1)}
			defer sampler.close()
			job := sampler.request(context.Background(), sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}, policy.NewCompressionPolicy(), sampleFrameMetrics())
			var wrong atomic.Bool
			job.measure = func(p []byte, _ encoding.SampleSet) (encoding.CodecSamples, error) {
				wrong.Store(!bytes.Equal(p, data[:encoding.CodecSampleBytes]))
				return fakeCodecSamples(), nil
			}
			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()
			var off int64
			_, err = tc.stream(fd, &off, frameStreamArgs{Ctx: context.Background(),
				FileID: 1, FrameSize: int64(len(data)), Comp: "none", Mode: loadStrategyFast,
				WindowHasher: &xxh3.Hasher128{}, Output: server, OutputTCPConn: server,
				PipeSizeBytes: desiredPipeSizeBytes(int64(len(data)), int64(len(data))),
				DirectIO:      tc.direct, Timed: true, Sample: job})
			if err != nil {
				t.Fatal(err)
			}
			waitSample(t, &sampler)
			if wrong.Load() {
				t.Fatal("sample differs from the already-read file prefix")
			}
		})
	}
}

// A small file cannot supply four baseline frames and four winning samples,
// so it must never pay codec exploration or send a trial frame.
func TestSendSamplingSkipsSmallFiles(t *testing.T) {
	data := compressibleBytes(3 * defaultFileFrameLogicalSize)
	path := writeTempSendFile(t, data)
	deps := &mockDeps{filePath: path}
	ref, err := deps.GetFileRef("tx1", 1, path)
	if err != nil {
		t.Fatal(err)
	}
	rec := recordFrameMetrics(t)
	_, err = streamSendItem(context.Background(), unbufferedSendOutput(io.Discard), deps, "tx1",
		sendItem{FileID: 1, Comp: "adapt", Path: path, Mode: loadStrategyFast}, ref, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range rec.snapshot() {
		if m.SniffRatio != 0 {
			t.Fatalf("small file sampled: %+v", m)
		}
	}
}

// Long-distance repetition is invisible to lz4 but profitable for zstd.
// The upward comparison must discover it without sending an lz4 trial.
// Once zstd is established, a bounded recovery episode may check lz4.
func TestSendSamplingDiscoversZstdPastLz4(t *testing.T) {
	pattern := make([]byte, 128<<10)
	rand.New(rand.NewSource(91)).Read(pattern)
	data := bytes.Repeat(pattern, 640) // 80 MiB; ample remaining evidence.
	pace := int64(10)
	if raceEnabled {
		pace = 80 // Instrumented codecs need a slower link to remain worthwhile.
	}
	for _, windowFrames := range []int{20, 1} {
		t.Run(fmt.Sprintf("%d-frame windows", windowFrames), func(t *testing.T) {
			deps, tid, path := multiWindowFile(t, data)
			found := false
			lz4Checks := 0
			for first := 0; first < 20; first += windowFrames {
				raw := sendWindow(t, deps, tid, path, first, windowFrames, pace)
				frames, err := decodeFrameStream(raw)
				if err != nil {
					t.Fatal(err)
				}
				if len(frames) != windowFrames {
					t.Fatalf("decoded %d frames, want %d", len(frames), windowFrames)
				}
				for _, frame := range frames {
					if frame.Header.Comp == encoding.EncodingLz4 {
						if !found {
							t.Fatal("an upward lz4 trial was sent before zstd was established")
						}
						lz4Checks++
					}
					lo, hi := frame.Header.Offset, frame.Header.Offset+frame.Header.Size
					if !bytes.Equal(frame.Logical, data[lo:hi]) {
						t.Fatalf("sampled transfer corrupted data at %d", lo)
					}
					found = found || frame.Header.Comp == encoding.EncodingZstd
				}
			}
			if !found {
				t.Fatal("zstd was never discovered")
			}
			if lz4Checks > 2 {
				t.Fatalf("short transfer sent %d lz4 check frames, want at most two", lz4Checks)
			}
		})
	}
}

// Errors and cancellation are discarded observations, not transfer errors.
// Both return the admitted slot and pooled input without publishing a hint.
func TestSendSamplingDiscardsFailedOrCanceledResults(t *testing.T) {
	pinSampleCost(t, time.Millisecond)
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			slots := make(chan struct{}, 1)
			sampler := sendSampler{slots: slots}
			defer sampler.close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			job := sampler.request(ctx, sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}, policy.NewCompressionPolicy(), sampleFrameMetrics())
			if job == nil {
				t.Fatal("sample was not admitted")
			}
			var calls atomic.Int32
			job.measure = func([]byte, encoding.SampleSet) (encoding.CodecSamples, error) {
				calls.Add(1)
				return encoding.CodecSamples{}, errors.New("sample failed")
			}
			if canceled {
				cancel()
			}
			job.capture(make([]byte, encoding.CodecSampleBytes))
			deadline := time.Now().Add(time.Second)
			for job.state.Load() != sampleDone && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if job.state.Load() != sampleDone {
				t.Fatal("worker did not finish")
			}
			if _, ready := sampler.take(); ready || sampler.job != nil || len(slots) != 0 {
				t.Fatal("failed or canceled sample published a result or retained resources")
			}
			if canceled && calls.Load() != 0 {
				t.Fatal("canceled job ran a codec")
			}
		})
	}
}

// A settled file samples only the codecs its policy still reads: both on
// none, zstd alone on lz4, and nothing on zstd, whose frames show its ratio.
func TestSendSamplingMeasuresOnlyWhatThePolicyNeeds(t *testing.T) {
	pinSampleCost(t, time.Millisecond)
	for _, tc := range []struct {
		mode policy.CompressionMode
		want encoding.SampleSet
	}{
		{policy.CompressionModeNone, encoding.SampleBoth},
		{policy.CompressionModeLz4, encoding.SampleZstd},
		{policy.CompressionModeZstdLevel1, 0},
	} {
		t.Run(policy.FrameCompTokenForMode(tc.mode), func(t *testing.T) {
			p := policy.NewCompressionPolicy()
			p.DecideSampled(tc.mode, sampleFrameMetrics(), nil)
			if p.BackgroundPending() {
				t.Fatal("one frame opened a comparison")
			}
			slots := make(chan struct{}, 1)
			sampler := sendSampler{slots: slots}
			defer sampler.close()
			job := sampler.request(context.Background(), sampleWork{256, 1 << 30, 256, 256 * defaultFileFrameLogicalSize}, p, sampleFrameMetrics())
			if tc.want == 0 {
				if job != nil || len(slots) != 0 {
					t.Fatal("a policy that reads no sample took a slot")
				}
				return
			}
			if job == nil {
				t.Fatal("sample was not admitted")
			}
			var asked atomic.Uint32
			job.measure = func(data []byte, set encoding.SampleSet) (encoding.CodecSamples, error) {
				asked.Store(uint32(set))
				return encoding.SampleCodecs(data, set, nil)
			}
			job.capture(bytes.Repeat([]byte("segment page "), encoding.CodecSampleBytes/13+1))
			got := waitSample(t, &sampler)
			if encoding.SampleSet(asked.Load()) != tc.want {
				t.Fatalf("measured %b, want %b", asked.Load(), tc.want)
			}
			if got.Has(encoding.EncodingLz4) != (tc.want&encoding.SampleLz4 != 0) || !got.Has(encoding.EncodingZstd) {
				t.Fatalf("result %+v does not match set %b", got, tc.want)
			}
		})
	}
}

// Later outliers have bounded influence; sustained costs are still learned.
func TestSampleCostAverageBoundsOutliers(t *testing.T) {
	pinSampleCost(t, 0)
	noteSampleCost(2*time.Millisecond, false)
	if got := time.Duration(sendSampleCost.Load()); got != 2*time.Millisecond {
		t.Fatalf("first cost %v, want 2ms", got)
	}
	noteSampleCost(time.Second, false)
	if got := time.Duration(sendSampleCost.Load()); got != 2500*time.Microsecond {
		t.Fatalf("after an outlier %v, want 2.5ms", got)
	}
	for range 32 {
		noteSampleCost(10*time.Millisecond, false)
	}
	if got := time.Duration(sendSampleCost.Load()); got < 9*time.Millisecond || got > 10*time.Millisecond {
		t.Fatalf("after a steady 10ms %v", got)
	}
}

// Only admitted jobs can correct the cost estimate. A cold or preempted
// first job must not permanently close sampling for ordinary short windows.
func TestSendSamplingRecoversFromAStaleCost(t *testing.T) {
	pinSampleCost(t, 0)
	noteSampleCost(time.Second, false)
	metrics := sampleFrameMetrics()
	work := sampleWork{fileFrames: 256, fileBytes: 1 << 30, windowFrames: 1, windowBytes: defaultFileFrameLogicalSize}
	p := policy.NewCompressionPolicy()
	for range 4 {
		p.DecideSampled(policy.CompressionModeNone, metrics, nil)
	}
	sampler := sendSampler{slots: make(chan struct{}, 1)}
	defer sampler.close()
	for range sampleRetryEvery - 1 {
		if sampler.request(context.Background(), work, p, metrics) != nil {
			t.Fatal("a rejected-cost retry bypassed its backoff")
		}
	}
	job := sampler.request(context.Background(), work, p, metrics)
	if job == nil || !job.retry {
		t.Fatal("stale cost prevented a bounded retry")
	}
	sampler.close()
	// A completed retry establishes the current cost without a second
	// long warmup. Normal admission must then resume for this window.
	noteSampleCost(2*time.Millisecond, true)
	if sampler.request(context.Background(), work, p, metrics) == nil {
		t.Fatal("a fresh cost did not restore normal admission")
	}
}

// A server's connections share one recovery budget: a new connection, or
// a later SEND on the same one, must not get an unpaid check.
func TestRecoveryBudgetBelongsToServer(t *testing.T) {
	var budget policy.RecoveryBudget
	first := sendRate{src: &sendRateSource{budget: &budget}}
	first.noteRecovery(time.Millisecond, time.Millisecond)
	for name, later := range map[string]sendRate{
		"same connection":  {src: first.src},
		"other connection": {src: &sendRateSource{budget: &budget}},
	} {
		if later.recoveryReady() {
			t.Fatalf("%s discarded the server's recovery debt", name)
		}
	}
	other := sendRate{src: &sendRateSource{budget: &budget}}
	other.noteRecovery(0, 100*time.Millisecond)
	if !first.recoveryReady() {
		t.Fatal("progress on another connection did not repay the shared budget")
	}
	// A SEND outside a server keeps its own budget.
	alone := sendRate{src: &sendRateSource{}}
	alone.noteRecovery(time.Millisecond, time.Millisecond)
	if alone.recoveryReady() || !first.recoveryReady() {
		t.Fatal("a sessionless SEND shared the server's budget")
	}
}

// The receiver paces decoded bytes, so compression cannot remove this
// bottleneck. Default buffers delay backpressure; small buffers expose it
// promptly. Every frame crosses a saved FX/1 window boundary.
func TestRecoveryAcrossBufferedTCPWindows(t *testing.T) {
	for _, buffer := range []int{0, 64 << 10} {
		t.Run(fmt.Sprintf("socket-buffer=%d", buffer), func(t *testing.T) {
			testRecoveryAcrossTCPWindows(t, buffer)
		})
	}
}

func testRecoveryAcrossTCPWindows(t *testing.T, buffer int) {
	t.Helper()
	const frames = 32
	data := bytes.Repeat(wordyBytes(int(defaultFileFrameLogicalSize)), frames+1)
	for frame := range frames + 1 {
		data[int64(frame)*defaultFileFrameLogicalSize] = byte(frame)
	}
	deps, tid, path := multiWindowFile(t, data)
	p := policy.NewCompressionPolicy()
	for range 16 {
		p.DecideSampled(policy.CompressionModeZstdLevel1, policy.CompressionMetrics{
			LogicalSize: defaultFileFrameLogicalSize, WireSize: defaultFileFrameLogicalSize / 3,
			PrepareLatency: time.Millisecond, WriteLatency: 400 * time.Millisecond}, nil)
	}
	deps.RecordTransferWindows(tid, WindowRecords{Compression: []FileCompressionUpdate{{FileID: 1,
		State: policy.FileCompression{Mode: policy.CompressionModeZstdLevel1, Policy: *p}}}})
	server, client := newTCPPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	for _, c := range []*net.TCPConn{server, client} {
		if err := c.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if buffer > 0 {
			if err := c.SetWriteBuffer(buffer); err != nil {
				t.Fatal(err)
			}
			if err := c.SetReadBuffer(buffer); err != nil {
				t.Fatal(err)
			}
		}
	}
	result := make(chan error, 1)
	go func() {
		br := bufio.NewReader(client)
		none := 0
		for frame := range frames {
			line, err := br.ReadString('\n')
			if err != nil {
				result <- err
				return
			}
			header, err := encoding.ParseFXHeader(strings.TrimSpace(line))
			if err != nil {
				result <- err
				return
			}
			r, err := encoding.DecodePayloadReaderByComp(io.LimitReader(br, header.WireSize), header.Comp)
			if err != nil {
				result <- err
				return
			}
			logical, err := io.ReadAll(r)
			closeErr := r.Close()
			if err != nil {
				result <- err
				return
			}
			if closeErr != nil {
				result <- closeErr
				return
			}
			start := int64(frame+1) * defaultFileFrameLogicalSize
			if !bytes.Equal(logical, data[start:start+defaultFileFrameLogicalSize]) {
				result <- fmt.Errorf("frame %d differs from source", frame)
				return
			}
			if _, err = (&rateWriter{w: io.Discard, nsPerByte: 5}).Write(logical); err != nil {
				result <- err
				return
			}
			line, err = br.ReadString('\n')
			if err == nil {
				_, err = encoding.ParseFXTrailer(strings.TrimSpace(line))
			}
			if err != nil {
				result <- err
				return
			}
			if header.Comp == "none" {
				none++
			}
		}
		if none == 0 {
			result <- errors.New("compressed stream never checked none")
			return
		}
		result <- nil
	}()
	out := newBufferedSendOutput(server)
	defer out.release()
	out.ownRate.src = &sendRateSource{conn: server}
	refs, errs := deps.GetFileRefs(tid, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Fatal(errs[0])
	}
	for first := 1; first <= frames; first++ {
		sent, err := streamSendItem(context.Background(), out, deps, tid,
			sendItem{FileID: 1, Offset: int64(first) * defaultFileFrameLogicalSize, Size: defaultFileFrameLogicalSize, Comp: "adapt", Path: path}, refs[0], 0)
		if err != nil {
			t.Fatal(err)
		}
		applyWindowState(deps, tid, sent)
		if err = out.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryTerminalFrameChargesLoss(t *testing.T) {
	deps, tid, path := multiWindowFile(t, compressibleBytes(2*defaultFileFrameLogicalSize))
	p := policy.NewCompressionPolicy()
	var d policy.CompressionDecision
	for range 16 {
		d = p.DecideSampled(policy.CompressionModeLz4, policy.CompressionMetrics{
			LogicalSize: defaultFileFrameLogicalSize, WireSize: defaultFileFrameLogicalSize / 3,
			PrepareLatency: time.Nanosecond, WriteLatency: time.Nanosecond,
			RecoveryAllowed: true, RecoveryReady: true}, nil)
		if d.Reason == "recovery-check" {
			break
		}
	}
	if !p.RecoveryActive() {
		t.Fatal("setup did not request a check")
	}
	deps.RecordTransferWindows(tid, WindowRecords{Compression: []FileCompressionUpdate{{FileID: 1,
		State: policy.FileCompression{Mode: d.Next, Policy: *p}}}})
	refs, errs := deps.GetFileRefs(tid, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Fatal(errs[0])
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Cancellation interrupts the transport. An unfinished frame must not
	// become a throughput observation or charge recovery debt.
	reader, writer := io.Pipe()
	_ = reader.CloseWithError(ctx.Err())
	defer writer.Close()
	canceledOut := unbufferedSendOutput(writer)
	canceledOut.ownRate.src = &sendRateSource{}
	_, err := streamSendItem(ctx, canceledOut, deps, tid, sendItem{
		FileID: 1, Offset: defaultFileFrameLogicalSize, Size: defaultFileFrameLogicalSize, Comp: "adapt", Path: path}, refs[0], 0)
	if !errors.Is(err, context.Canceled) || !canceledOut.rate.recoveryReady() {
		t.Fatalf("canceled check: err=%v ready=%v", err, canceledOut.rate.recoveryReady())
	}
	out := unbufferedSendOutput(io.Discard)
	out.ownRate.src = &sendRateSource{}
	_, err = streamSendItem(context.Background(), out, deps, tid, sendItem{
		FileID: 1, Offset: defaultFileFrameLogicalSize, Size: defaultFileFrameLogicalSize, Comp: "adapt", Path: path}, refs[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if out.rate.recoveryReady() {
		t.Fatal("terminal recovery frame did not charge its excess time")
	}
}

type samplingRateConn struct {
	net.Conn
	rate rateWriter
}

func (c *samplingRateConn) Write(p []byte) (int, error) { return c.rate.Write(p) }

func sampledNoiseWriter(t *testing.T, wireCost int64) (*aead.MessageWriter, *aead.Session) {
	t.Helper()
	server, client := newTCPPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	for _, c := range []*net.TCPConn{server, client} {
		if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	conn := &samplingRateConn{Conn: server, rate: rateWriter{w: server}}
	serverKey, _ := clientKey(t)
	clientIdentity, _ := clientKey(t)
	type handshake struct {
		session *aead.Session
		err     error
	}
	ready := make(chan handshake, 1)
	go func() {
		br := bufio.NewReader(conn)
		line, err := br.ReadBytes('\n')
		if err != nil {
			ready <- handshake{err: err}
			return
		}
		s, _, err := aead.ServerHandshake(conn, br, line, aead.ServerConfig{Static: serverKey, AnyKey: true})
		ready <- handshake{s, err}
	}()
	cs, err := aead.ClientHandshake(client, aead.ClientConfig{
		Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: clientIdentity, ServerKey: serverKey.Public})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	result := <-ready
	if result.err != nil {
		t.Fatal(result.err)
	}
	t.Cleanup(func() { _ = result.session.Close() })
	conn.rate.nsPerByte = wireCost
	return result.session.Writer(true), cs
}

// lz4RecoveryCheck returns a zstd policy whose recovery check lost with none
// and now checks lz4, with home's 20 ms frames as the baseline; m is home's
// last frame and d the decision to send lz4.
func lz4RecoveryCheck(t *testing.T) (*policy.CompressionPolicy, policy.CompressionMetrics, policy.CompressionDecision) {
	t.Helper()
	p := policy.NewCompressionPolicy()
	m := policy.CompressionMetrics{
		LogicalSize: defaultFileFrameLogicalSize, WireSize: defaultFileFrameLogicalSize / 1000,
		PrepareLatency: 5 * time.Millisecond, WriteLatency: 15 * time.Millisecond,
		RecoveryAllowed: true, RecoveryReady: true}
	var d policy.CompressionDecision
	for range 32 {
		d = p.DecideSampled(policy.CompressionModeZstdLevel1, m, nil)
		if d.Reason == "recovery-check" {
			break
		}
	}
	if d.Next != policy.CompressionModeNone {
		t.Fatalf("no uncompressed check: %+v", d)
	}
	m.WireSize, m.WriteLatency = m.LogicalSize, time.Second
	d = p.DecideSampled(d.Next, m, nil)
	if d.Next != policy.CompressionModeLz4 {
		t.Fatalf("no lz4 check after none lost: %+v", d)
	}
	return p, m, d
}

// A fast response buffers small compressed frames, and in a one-frame window
// sends them only when the request ends, after the policy decided. Frames a
// recovery check measures must still carry their wire time: on a 128 KiB/s
// link, lz4 is far slower than the zstd baseline and must be restored, and
// the loss charged to the server's budget.
func TestRecoveryMeasuresFastResponseFlush(t *testing.T) {
	const wireCost = int64(time.Second) / (128 << 10)
	for _, transport := range []string{"plain", "aead"} {
		t.Run(transport, func(t *testing.T) {
			p, _, d := lz4RecoveryCheck(t)
			data := compressibleBytes(12 * defaultFileFrameLogicalSize)
			deps, tid, path := multiWindowFile(t, data)
			deps.RecordTransferWindows(tid, WindowRecords{Compression: []FileCompressionUpdate{{FileID: 1,
				State: policy.FileCompression{Mode: d.Next, Policy: *p}}}})
			req, err := ParseRequest([]byte("SEND " + tid + " mode=fast"))
			if err != nil {
				t.Fatal(err)
			}
			var budget policy.RecoveryBudget
			for frame := 1; frame <= 10; frame++ {
				var w io.Writer
				var finish func() ([]byte, error)
				if transport == "plain" {
					var raw bytes.Buffer
					w = &rateWriter{w: &raw, nsPerByte: wireCost}
					finish = func() ([]byte, error) { return raw.Bytes(), nil }
				} else {
					writer, receiver := sampledNoiseWriter(t, wireCost)
					type received struct {
						raw []byte
						err error
					}
					done := make(chan received, 1)
					go func() {
						r, err := receiver.Reader()
						if err != nil {
							done <- received{err: err}
							return
						}
						raw, err := io.ReadAll(r)
						done <- received{raw, err}
					}()
					w = writer
					finish = func() ([]byte, error) {
						if err := writer.End(); err != nil {
							return nil, err
						}
						result := <-done
						return result.raw, result.err
					}
				}
				// Each window is a new SEND on a new connection of one server.
				ctx := context.WithValue(context.Background(), sendRateKey{}, &sendRateSource{budget: &budget})
				off := int64(frame) * defaultFileFrameLogicalSize
				item := fmt.Sprintf("fd=1 %q offset=%d size=%d comp=adapt", path, off, defaultFileFrameLogicalSize)
				if err := handleSENDWithOptions(ctx, req, framedItemBody(t, item), w, deps, nil, 0, 25); err != nil {
					t.Fatal(err)
				}
				raw, err := finish()
				if err != nil {
					t.Fatal(err)
				}
				frames, err := decodeFrameStream(raw)
				if err != nil || len(frames) != 1 {
					t.Fatalf("decoded %d frames: %v", len(frames), err)
				}
				if !bytes.Equal(frames[0].Logical, data[off:off+defaultFileFrameLogicalSize]) {
					t.Fatalf("frame %d differs from source", frame)
				}
			}
			st, _ := deps.LoadFileCompression(tid, 1)
			if st.Mode != policy.CompressionModeZstdLevel1 {
				t.Fatalf("slower lz4 kept: its buffered frames' wire time escaped the check (mode %v)", st.Mode)
			}
			if budget.Ready() {
				t.Fatal("the losing check charged nothing to the server's budget")
			}
		})
	}
}

// writeLog records the size of each write that reaches it.
type writeLog struct {
	raw    bytes.Buffer
	writes []int
}

func (w *writeLog) Write(p []byte) (int, error) {
	w.writes = append(w.writes, len(p))
	return w.raw.Write(p)
}

// A recovery frame must reach the writer before the policy times it, or
// the time its bytes take escapes the check; and its flush must write its
// own bytes alone, not output earlier items left in a fast response's
// buffer, or the baseline counts their wire time as home's. This checks
// which bytes had been written rather than how long that took, so it does
// not depend on timing.
func TestRecoveryFlushTimesOnlyItsFrame(t *testing.T) {
	const frame = defaultFileFrameLogicalSize
	data := compressibleBytes(8 * frame)
	deps, tid, path := multiWindowFile(t, data)
	p := policy.NewCompressionPolicy()
	m := policy.CompressionMetrics{LogicalSize: frame, WireSize: frame / 1000,
		PrepareLatency: time.Millisecond, WriteLatency: time.Millisecond,
		RecoveryAllowed: true, RecoveryReady: true}
	var d policy.CompressionDecision
	for range 32 {
		if d = p.DecideSampled(policy.CompressionModeZstdLevel1, m, nil); d.Reason == "recovery-baseline" {
			break
		}
	}
	if d.Reason != "recovery-baseline" {
		t.Fatalf("no baseline: %+v", d)
	}
	deps.RecordTransferWindows(tid, WindowRecords{Compression: []FileCompressionUpdate{{FileID: 1,
		State: policy.FileCompression{Mode: d.Next, Policy: *p}}}})
	req, err := ParseRequest([]byte("SEND " + tid + " mode=fast"))
	if err != nil {
		t.Fatal(err)
	}
	// Seven small items leave about 56 KiB buffered before the baseline.
	var items []string
	for range 7 {
		items = append(items, fmt.Sprintf("fd=1 %q offset=0 size=8192 comp=none", path))
	}
	items = append(items, fmt.Sprintf("fd=1 %q offset=%d size=%d comp=adapt", path, frame, 4*frame))
	w := &writeLog{}
	type observed struct {
		wire                int64
		lastWrite, received int
	}
	var seen []observed
	sendFrameMetricsHook = func(m policy.CompressionMetrics) {
		seen = append(seen, observed{m.WireSize, w.writes[len(w.writes)-1], w.raw.Len()})
	}
	t.Cleanup(func() { sendFrameMetricsHook = nil })
	if err := handleSENDWithOptions(context.Background(), req, framedItemBody(t, items...), w, deps, nil, 0, 25); err != nil {
		t.Fatal(err)
	}
	frames, err := decodeFrameStream(w.raw.Bytes())
	if err != nil || len(frames) != 11 {
		t.Fatalf("decoded %d frames: %v", len(frames), err)
	}
	for i, f := range frames {
		if !bytes.Equal(f.Logical, data[f.Header.Offset:f.Header.Offset+int64(len(f.Logical))]) {
			t.Fatalf("frame %d differs from source", i)
		}
	}
	if len(seen) < 3 {
		t.Fatalf("policy saw %d frames, want two baseline frames and a check", len(seen))
	}
	// A frame's own flush writes its header, payload, and trailer.
	const overhead = 4 << 10
	for i, o := range seen[:3] {
		if got, err := decodeFrameStream(w.raw.Bytes()[:o.received]); err != nil || len(got) != 7+i+1 {
			t.Fatalf("recovery frame %d was timed before it reached the writer (%d whole frames there: %v)", i, len(got), err)
		}
		if o.lastWrite > int(o.wire)+overhead {
			t.Fatalf("recovery frame %d's flush wrote %d bytes for a %d-byte frame: earlier output was timed with it", i, o.lastWrite, o.wire)
		}
	}
}

// A small compressed frame reaches the socket only when its AEAD record is
// flushed. Recovery must measure that wait when checking and validating lz4.
func TestRecoveryMeasuresAuthenticatedFlush(t *testing.T) {
	for _, phase := range []string{"check", "validation"} {
		t.Run(phase, func(t *testing.T) {
			p, m, d := lz4RecoveryCheck(t)
			frames := 1
			if phase == "validation" {
				// Resume a saved episode whose two buffered check frames won.
				m.WireSize = m.LogicalSize / 256
				m.PrepareLatency, m.WriteLatency = time.Millisecond, time.Millisecond
				for range 2 {
					d = p.DecideSampled(d.Next, m, nil)
				}
				if d.Reason != "recovery-won" {
					t.Fatalf("no validation after winning checks: %+v", d)
				}
				frames = 8
			}
			data := compressibleBytes(10 * defaultFileFrameLogicalSize)
			deps, tid, path := multiWindowFile(t, data)
			deps.RecordTransferWindows(tid, WindowRecords{Compression: []FileCompressionUpdate{{FileID: 1,
				State: policy.FileCompression{Mode: d.Next, Policy: *p}}}})
			refs, errs := deps.GetFileRefs(tid, []FileLookup{{FileID: 1, Path: path}})
			if errs[0] != nil {
				t.Fatal(errs[0])
			}
			const wireCost = int64(time.Second) / (128 << 10)
			writer, receiver := sampledNoiseWriter(t, wireCost)
			type received struct {
				data []byte
				err  error
			}
			done := make(chan received, 1)
			go func() {
				r, err := receiver.Reader()
				if err != nil {
					done <- received{err: err}
					return
				}
				raw, err := io.ReadAll(r)
				done <- received{raw, err}
			}()
			// The configured limiter permits far more than the downstream link.
			limiter, err := limit.NewLimiterFromBps(1<<30, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			out := unbufferedSendOutput(limiter.WrapRateLimitedWriter(writer, context.Background()))
			out.frameFlush = writer.FlushFunc()
			out.ownRate.src = &sendRateSource{}
			rec := recordFrameMetrics(t)
			sent, err := streamSendItem(context.Background(), out, deps, tid, sendItem{
				FileID: 1, Offset: defaultFileFrameLogicalSize, Size: int64(frames) * defaultFileFrameLogicalSize,
				Comp: "adapt", Path: path, Mode: loadStrategyGentle}, refs[0], 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.End(); err != nil {
				t.Fatal(err)
			}
			result := <-done
			if result.err != nil {
				t.Fatal(result.err)
			}
			decoded, err := decodeFrameStream(result.data)
			if err != nil || len(decoded) != frames {
				t.Fatalf("decoded %d frames, want %d: %v", len(decoded), frames, err)
			}
			for i, frame := range decoded {
				start := int64(i+1) * defaultFileFrameLogicalSize
				if frame.Header.Offset != start || !bytes.Equal(frame.Logical, data[start:start+defaultFileFrameLogicalSize]) {
					t.Fatalf("frame %d differs from source", i)
				}
			}
			observations := rec.snapshot()
			if len(observations) != frames {
				t.Fatalf("observed %d frames, want %d", len(observations), frames)
			}
			for _, observed := range observations {
				if observed.WireSize >= 60<<10 {
					t.Fatalf("frame no longer fits one AEAD record: %+v", observed)
				}
				wait := time.Duration(observed.WireSize * wireCost)
				if observed.WriteLatency < wait || observed.FrameLatency < observed.PrepareLatency+wait {
					t.Fatalf("flush wait %v missing from frame metrics: %+v", wait, observed)
				}
			}
			if !sent.hasComp || sent.comp.State.Mode != policy.CompressionModeZstdLevel1 {
				t.Fatalf("slower lz4 retained: %+v", sent.comp.State)
			}
			if out.rate.recoveryReady() {
				t.Fatal("unsuccessful recovery did not charge its flush cost")
			}
		})
	}
}

func TestSendFailedFlushLeavesNoObservation(t *testing.T) {
	deps, tid, path := multiWindowFile(t, compressibleBytes(2*defaultFileFrameLogicalSize))
	refs, errs := deps.GetFileRefs(tid, []FileLookup{{FileID: 1, Path: path}})
	if errs[0] != nil {
		t.Fatal(errs[0])
	}
	out := unbufferedSendOutput(io.Discard)
	out.frameFlush = func() error { return context.Canceled }
	out.ownRate.src = &sendRateSource{}
	rec := recordFrameMetrics(t)
	sent, err := streamSendItem(context.Background(), out, deps, tid, sendItem{
		FileID: 1, Size: defaultFileFrameLogicalSize, Comp: "adapt", Path: path}, refs[0], 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("flush error = %v, want cancellation", err)
	}
	if len(rec.snapshot()) != 0 || sent.decided || sent.hasComp || sent.hash.HashToken != "" {
		t.Fatalf("failed flush became a completed observation: %+v", sent)
	}
	if !out.rate.recoveryReady() {
		t.Fatal("failed flush charged recovery debt")
	}
}
