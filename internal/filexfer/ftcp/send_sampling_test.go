package ftcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
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
// The transfer must discover it without ever sending a losing lz4 frame.
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
						t.Fatal("a losing lz4 trial was sent")
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
