package ftcp

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/sysio"
)

// Samples use already-read bytes. Slots bound copied input, queued work, and
// results waiting for a frame to end. The separate worker limit bounds CPU
// without letting a completed result hold up another stream's codec work.
var sendSampleSlots = make(chan struct{}, max(2, runtime.GOMAXPROCS(0)/4))
var sendSampleWorkers = make(chan struct{}, max(1, runtime.GOMAXPROCS(0)/8))

// sendSampleCost averages the latency, in nanoseconds, of the samples SENDs
// ran, used or abandoned, from capture to result: queueing for a CPU and
// encoder setup included. Windows are short-lived, so admission reads the
// process's average rather than its own. Costs are clipped after the first
// observation; occasional retries can replace a stale high estimate.
var sendSampleCost atomic.Int64
var sendSampleRejected atomic.Uint64

const initialSampleCost = time.Millisecond

// Headroom covers variation in both job latency and remaining window time.
const sampleWindowMargin = 4
const sampleRetryEvery = 64

// noteSampleCost adds a job's latency. A complete retry can lower a stale
// estimate immediately. Concurrent updates may slow convergence.
func noteSampleCost(d time.Duration, retry bool) {
	avg := sendSampleCost.Load()
	if avg == 0 || retry && int64(d) < avg {
		sendSampleCost.Store(int64(d))
		return
	}
	sendSampleCost.Store(avg + (min(int64(d), 2*avg)-avg)/4)
}

var sendSamplePool = sync.Pool{New: func() any { return new(sendSampleJob) }}

const (
	sampleCapturing uint32 = iota
	sampleRunning
	sampleDone
	sampleAbandoned
)

type sendSampleJob struct {
	state   atomic.Uint32
	ctx     context.Context
	data    *[]byte
	used    int
	result  encoding.CodecSamples
	cost    time.Duration
	slots   chan struct{}
	workers chan struct{}
	codecs  encoding.SampleSet
	queued  time.Time // when capture handed the job to its worker
	retry   bool      // admitted to refresh an otherwise prohibitive cost
	// measure is normally nil; lifecycle tests can block this job without
	// replacing a function other workers read.
	measure func([]byte, encoding.SampleSet) (encoding.CodecSamples, error)
}

// capture copies at most one bounded sample. After starting the worker it
// never touches the source buffer again, even when SEND returns or reuses it.
func (j *sendSampleJob) capture(p []byte) {
	if j == nil || j.state.Load() != sampleCapturing {
		return
	}
	j.used += copy((*j.data)[j.used:encoding.CodecSampleBytes], p)
	if j.used == encoding.CodecSampleBytes {
		j.state.Store(sampleRunning)
		j.queued = time.Now()
		go j.run()
	}
}

// measureEach samples one codec at a time, stopping between codecs when
// the window no longer needs the result.
func (j *sendSampleJob) measureEach() (encoding.CodecSamples, error) {
	p := (*j.data)[:j.used]
	if j.codecs != encoding.SampleBoth {
		return measureCodecs(p, j.codecs)
	}
	result, err := measureCodecs(p, encoding.SampleLz4)
	if err != nil || j.state.Load() == sampleAbandoned {
		return result, err
	}
	zstd, err := measureCodecs(p, encoding.SampleZstd)
	result.Zstd = zstd.Zstd
	return result, err
}

// measureCodecs uses thread CPU time so preemption does not inflate encode
// costs, falling back to wall time when the thread clock is unavailable.
func measureCodecs(p []byte, set encoding.SampleSet) (encoding.CodecSamples, error) {
	if _, ok := sysio.ThreadCPUTime(); !ok {
		return encoding.SampleCodecs(p, set, nil)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return encoding.SampleCodecs(p, set, threadCPUTime)
}

func threadCPUTime() time.Duration {
	d, _ := sysio.ThreadCPUTime()
	return d
}

func (j *sendSampleJob) run() {
	select {
	case j.workers <- struct{}{}:
		if j.ctx.Err() == nil && j.state.Load() != sampleAbandoned {
			var result encoding.CodecSamples
			var err error
			if j.measure != nil {
				result, err = j.measure((*j.data)[:j.used], j.codecs)
			} else {
				result, err = j.measureEach()
			}
			if err == nil && j.ctx.Err() == nil {
				j.result = result
			}
		}
		<-j.workers
	case <-j.ctx.Done():
	}
	// The cost a window waits for includes the time this worker spent
	// queued behind the SENDs it shares the CPUs with.
	j.cost = time.Since(j.queued)
	if j.measure == nil {
		// Abandoned jobs count too: they are the ones admission must learn
		// to avoid. A test's stand-in measures nothing about the codecs.
		complete := (j.codecs&encoding.SampleLz4 == 0 || j.result.Has(encoding.EncodingLz4)) &&
			(j.codecs&encoding.SampleZstd == 0 || j.result.Has(encoding.EncodingZstd))
		noteSampleCost(j.cost, j.retry && complete)
	}
	if !j.state.CompareAndSwap(sampleRunning, sampleDone) {
		// The sender abandoned this job. Only the worker still owns it.
		j.release()
	}
}

func (j *sendSampleJob) release() {
	bufpool.Put(j.data)
	<-j.slots
	j.ctx, j.data, j.slots, j.measure, j.codecs = nil, nil, nil, nil, 0
	j.workers = nil
	j.used, j.result, j.cost = 0, encoding.CodecSamples{}, 0
	j.queued = time.Time{}
	j.retry = false
	j.state.Store(sampleCapturing)
	sendSamplePool.Put(j)
}

// abandon never waits for a codec. Ownership passes to a running worker;
// a completed or not-yet-started job can be returned immediately.
func (j *sendSampleJob) abandon() {
	for {
		switch state := j.state.Load(); state {
		case sampleRunning:
			if j.state.CompareAndSwap(sampleRunning, sampleAbandoned) {
				return
			}
		case sampleCapturing, sampleDone:
			j.release()
			return
		default:
			return
		}
	}
}

type sendSampler struct {
	job     *sendSampleJob
	slots   chan struct{} // nil uses the process limit; tests use their own
	workers chan struct{} // nil uses the process's codec worker limit
}

// sampleWork is what remains to send when a frame asks for a sample: the
// file's frames and bytes, and the window's frames that still reach the
// policy, this one included. A result must arrive within those window
// frames, since the window abandons a pending job when it ends.
type sampleWork struct {
	fileFrames   int
	fileBytes    int64
	windowFrames int
	// windowBytes is the logical size of windowFrames, whose last frame
	// may be short.
	windowBytes int64
}

// framesLeft estimates the frames still to send from cursor: the rest of this
// window, whose last frame may be short, then the rest of the file at full
// frames. It undercounts when later windows are not multiples of 4 MiB, and
// overcounts frames this window's evidence reaches, since concurrent windows
// of the file load their state before this one saves. The process-wide
// sample slots bound the cost of a wrong guess.
func framesLeft(cursor, windowEnd, fileEnd int64) int {
	ceil := func(n int64) int64 {
		return (max(0, n) + defaultFileFrameLogicalSize - 1) / defaultFileFrameLogicalSize
	}
	return int(ceil(min(windowEnd, fileEnd)-cursor) + ceil(fileEnd-windowEnd))
}

// sampleUseful reports whether a sample could still change the codec in time
// to matter: the file must have the frames a switch needs (need), through one
// sent in the new codec, and its remaining send time must exceed the time
// those samples take. The sample itself must also finish before its window's
// last decided frame, or its work is thrown away: the window's remaining
// frames must take several times the sample's average latency, since both vary
// with load. A frame that sends faster than a 256 KiB sample compresses
// could not gain from compressing either.
func sampleUseful(work sampleWork, need int, last policy.CompressionMetrics, sampleCost time.Duration) bool {
	if work.fileFrames < need || work.windowFrames < 1 {
		return false
	}
	if last.LogicalSize <= 0 || last.PrepareLatency+last.WriteLatency <= 0 {
		return false // No frame has measured the wall time yet.
	}
	perByte := float64(last.PrepareLatency+last.WriteLatency) / float64(last.LogicalSize)
	cost := float64(max(initialSampleCost, sampleCost))
	if float64(work.fileBytes)*perByte <= cost*float64(need) {
		return false
	}
	return float64(work.windowBytes)*perByte > sampleWindowMargin*cost
}

// request never waits for a slot. Only bounded background jobs wait for a
// codec worker; the stream continues in its current mode.
func (s *sendSampler) request(ctx context.Context, work sampleWork, p *policy.CompressionPolicy, last policy.CompressionMetrics) *sendSampleJob {
	codecs := p.SampleNeeds()
	if codecs == 0 {
		return nil
	}
	if s.job != nil || ctx.Err() != nil {
		return nil
	}
	need := p.BackgroundFramesNeeded(sendSniffEvery)
	retry := !sampleUseful(work, need, last, time.Duration(sendSampleCost.Load()))
	if retry {
		// Rejected jobs cannot correct a stale estimate. Occasionally try
		// one that would fit at the initial cost, still under the slot cap.
		if !sampleUseful(work, need, last, initialSampleCost) || sendSampleRejected.Add(1)%sampleRetryEvery != 0 {
			return nil
		}
	}
	slots := s.slots
	if slots == nil {
		slots = sendSampleSlots
	}
	select {
	case slots <- struct{}{}:
	default:
		return nil
	}
	data, err := bufpool.Get(encoding.CodecSampleBytes)
	if err != nil {
		<-slots
		return nil
	}
	j := sendSamplePool.Get().(*sendSampleJob)
	j.ctx, j.data, j.slots, j.codecs = ctx, data, slots, codecs
	j.workers = s.workers
	if j.workers == nil {
		j.workers = sendSampleWorkers
	}
	j.retry = retry
	s.job = j
	return j
}

// take returns only a completed result. A busy worker retains its input and
// the stream keeps sending in its learned mode.
func (s *sendSampler) take() (encoding.CodecSamples, bool) {
	if s.job == nil || s.job.state.Load() != sampleDone {
		return encoding.CodecSamples{}, false
	}
	j := s.job
	result := j.result
	s.job = nil
	j.release()
	return result, result.Valid()
}

func (s *sendSampler) close() {
	if s.job != nil {
		s.job.abandon()
		s.job = nil
	}
}
