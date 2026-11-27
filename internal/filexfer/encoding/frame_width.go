package encoding

// The adaptive concurrency of SEND data-frame encoders. Three pieces decide
// how many goroutines a frame's encoder uses:
//
//   - FrameWidth, per window: may this frame use more than one? Only when the
//     window's previous frame, of the same codec, compressed slowly; for a
//     window's first lz4 frame, when the process's last lz4 frame did. A
//     window's zstd frames also keep the ratio of its narrow ones (see
//     wideLosesMatches).
//   - frameConcurrencyCap, per frame: at most one goroutine per 1 MiB block,
//     within the codec's maximum.
//   - frameCoreBudget, per process: 1, 2, or 4 goroutines within that cap,
//     more than one only while the encoders of all frames compressing at
//     once fit in GOMAXPROCS.

import (
	"runtime"
	"sync/atomic"
	"time"
)

// WideCompressMaxBytesPerCoreSec is the compression rate per core below which
// a window's next frame may use several encoder goroutines. Below it, a 4 MiB
// frame takes over 2 ms of CPU, and spreading it saves wall time for a few
// percent more CPU. Above it, as for random or zero-filled data, the extra
// goroutines cost more than the encoding itself.
const WideCompressMaxBytesPerCoreSec = 2 << 30

// compressesSlowly reports whether logical bytes that one encoder goroutine
// compresses in narrowWall ran below WideCompressMaxBytesPerCoreSec.
func compressesSlowly(logical int64, narrowWall time.Duration) bool {
	return logical > 0 && narrowWall > 0 && float64(logical) < WideCompressMaxBytesPerCoreSec*narrowWall.Seconds()
}

// frameSpeedup is the wall-time speedup of a data-frame encoder on
// concurrency goroutines over one, measured on 4 MiB frames of text. A wide
// frame's wall time times this speedup estimates what one goroutine would
// take. Wall time times the goroutine count would overstate it, since the
// encoders scale sublinearly, and would keep a window of fast data wide once
// one frame was misjudged as slow, such as a window's first frame on a cold
// encoder pool.
//
// Cheap data gains little from extra goroutines, so the estimate overstates
// its single-goroutine time by nearly the full speedup. zstd therefore uses
// its 2-goroutine speedup, 1.8x, at 4 goroutines as well, where text
// measures 3.4x: at 3.4x, a wide frame of cheap data that a loaded host
// slowed past 0.57 ms would read as slow and keep the window wide. Observe
// counts incompressible frames as fast regardless; this margin covers cheap
// data that does compress, such as zero-filled regions.
func frameSpeedup(codec string, concurrency int) float64 {
	switch {
	case concurrency <= 1:
		return 1
	case codec == EncodingZstd:
		return 1.8
	case concurrency == 2:
		return 1.6
	default:
		return 2.9
	}
}

// frameHints records whether the process's most recent lz4 frame
// compressed slowly. A window's first lz4 frame follows it, so a file that
// is a single window still compresses wide when its data is slow, and a
// workload of fast data stays narrow from its first frame. zstd has no hint:
// a window's first zstd frame is narrow, since it measures the ratio that
// the window's wide frames must keep (see wideLosesMatches).
type frameHints struct {
	lz4 atomic.Bool
}

func (h *frameHints) slow(codec string) *atomic.Bool {
	if codec == EncodingLz4 {
		return &h.lz4
	}
	return nil
}

// wideLosesMatches reports whether codec's wide frames can find fewer
// matches than its narrow ones. A wide zstd frame compresses 1 MiB jobs in
// parallel, each seeing only the last 32 KiB of the job before it and 256 KiB
// within its own, while a narrow frame matches across the whole frame. Data
// that repeats farther apart, such as duplicate files in an archive or
// disk images, can compress several times worse wide. lz4 matches within
// 64 KiB either way.
func wideLosesMatches(codec string) bool {
	return codec == EncodingZstd
}

// wideRatioLossDivisor sets how much worse than the window's last narrow
// frame a wide frame may compress before it counts as a loss: by more than
// 1/4 of the narrow ratio. Neighboring frames of an archive differ by more
// than that often enough that a tighter bound keeps windows of ordinary data
// narrow, while each frame's own wide and narrow ratios agree within a few
// percent; data that repeats farther apart than a job sees loses 25-90%.
const wideRatioLossDivisor = 4

// zstdRecheckFrames is how many wide zstd frames a window sends before one
// narrow frame refreshes the reference ratio, since a window can span a
// whole file whose data changes; that takes about 14% more time than
// staying wide at 3.4x. It is also the first narrow run after a confirmed
// ratio loss. Each later loss, on the wide frame that ends a narrow run,
// doubles the run, up to zstdMaxNarrowFrames, so data that keeps losing
// sends few wide frames.
const (
	zstdRecheckFrames   = 16
	zstdMaxNarrowFrames = 256
)

// losesRatio reports whether a frame compressed to wire bytes from logical
// compressed by more than 1/wideRatioLossDivisor worse than the reference
// frame. Both frames are at most a few MiB, so the products fit in int64.
func losesRatio(logical, wire, refLogical, refWire int64) bool {
	const d = wideRatioLossDivisor
	return d*logical*refWire < (d-1)*refLogical*wire
}

var processFrameHints frameHints

// FrameWidth tracks one window's frames and decides whether the next may use
// more than one encoder goroutine. The zero value uses the process's hints.
type FrameWidth struct {
	// Gentle keeps every frame on one goroutine.
	Gentle bool

	hints *frameHints // nil means processFrameHints
	slow  bool
	codec string
	// refLogical and refWire are the sizes of the window's last narrow zstd
	// frame, the ratio its wide zstd frames must keep.
	refLogical, refWire int64
	// suspect records a wide zstd frame that lost ratio against a reference
	// that may hold different data. The next zstd frame is narrow, to take
	// a reference next to it, and if the zstd frame after that is wide and
	// loses ratio too, the loss is confirmed.
	suspect bool
	// narrowOnly keeps the window's zstd frames narrow after a confirmed
	// ratio loss, until narrowRun narrow frames have passed. Frames of other
	// codecs leave it.
	narrowOnly bool
	narrowRun  int
	// zstdRun counts the window's zstd frames since the reference was taken
	// or, while narrowOnly, since the ratio loss.
	zstdRun int
}

func (w *FrameWidth) hintFor(codec string) *atomic.Bool {
	if w.hints == nil {
		return processFrameHints.slow(codec)
	}
	return w.hints.slow(codec)
}

// Wide reports whether the next frame, compressed with codec, may use more
// than one encoder goroutine: the previous frame used the same codec and
// compressed slowly, or, for a window's first lz4 frame, the process's last
// lz4 frame did. A codec whose wide frames can lose matches widens only
// after a narrow frame has set the ratio to keep, sends a narrow frame to
// refresh it every zstdRecheckFrames frames or after a wide frame lost it,
// and stays narrow for a while once a loss is confirmed.
func (w *FrameWidth) Wide(codec string) bool {
	if w.Gentle {
		return false
	}
	if w.codec == "" {
		h := w.hintFor(codec)
		return h != nil && h.Load()
	}
	if w.codec != codec || !w.slow {
		return false
	}
	if !wideLosesMatches(codec) {
		return true
	}
	if w.narrowOnly {
		return w.zstdRun >= w.narrowRun
	}
	return w.refWire > 0 && w.zstdRun < zstdRecheckFrames
}

// FrameSample describes a sent frame to FrameWidth.Observe.
type FrameSample struct {
	Codec string
	// Logical and Wire are the frame's size before and after compression.
	Logical, Wire int64
	// Concurrency and Wall are the goroutines and wall time its encoder
	// used. An uncompressed frame passes a concurrency of 0.
	Concurrency int
	Wall        time.Duration
}

// Observe records a sent frame. A frame that did not shrink counts as fast
// whatever its wall time: both codecs store incompressible input, such as
// random, media, or encrypted data, at several GiB/s on one goroutine, and
// on a loaded host the scheduling of a wide frame's goroutines can stretch
// its wall time past the threshold. A wide zstd frame that compressed worse
// than the window's last narrow zstd frame makes the next one narrow, for a
// reference next to it. A second loss in a row keeps the window's zstd
// frames narrow for zstdRecheckFrames frames, and a loss on the wide frame
// that ends such a run doubles it; a wide frame that kept the ratio ends
// that.
func (w *FrameWidth) Observe(s FrameSample) {
	w.codec = s.Codec
	w.slow = s.Concurrency > 0 && s.Wire < s.Logical &&
		compressesSlowly(s.Logical, time.Duration(float64(s.Wall)*frameSpeedup(s.Codec, s.Concurrency)))
	switch {
	case !wideLosesMatches(s.Codec):
	case s.Concurrency == 1:
		w.refLogical, w.refWire = s.Logical, s.Wire
		switch {
		case w.narrowOnly:
			w.zstdRun++
		case w.suspect && w.zstdRun < zstdRecheckFrames:
			// A second narrow frame since the loss: the next loss would not
			// follow it.
			w.suspect, w.zstdRun = false, 0
		default:
			w.zstdRun = 0
		}
	case s.Concurrency > 1 && w.refWire > 0 && losesRatio(s.Logical, s.Wire, w.refLogical, w.refWire):
		switch {
		case w.narrowOnly:
			w.narrowRun, w.zstdRun = min(2*w.narrowRun, zstdMaxNarrowFrames), 0
		case w.suspect:
			w.suspect, w.narrowOnly, w.narrowRun, w.zstdRun = false, true, zstdRecheckFrames, 0
		default:
			// A full run makes Wide send the next frame narrow.
			w.suspect, w.zstdRun = true, zstdRecheckFrames
		}
	case s.Concurrency > 1 && w.narrowOnly:
		w.narrowOnly, w.zstdRun = false, 1
	case s.Concurrency > 1:
		w.suspect = false
		w.zstdRun++
	}
	if h := w.hintFor(s.Codec); h != nil && s.Concurrency > 0 && h.Load() != w.slow {
		h.Store(w.slow)
	}
}

// frameBlockBytes is the lz4 block size of a data frame, and the unit a
// frame's input is divided into across encoder goroutines.
const frameBlockBytes = 1 << 20

// Encoder concurrency for a frame is 1, 2, or 4, so each codec keeps at most
// three encoder pools. frameSpeedup holds the measured scaling.
const (
	lz4MaxFrameConcurrency  = 4
	zstdMaxFrameConcurrency = 4
)

// frameCoreBudget limits the encoder goroutines of all frames compressing at
// once to GOMAXPROCS. Fast mode runs at least one connection per core, so
// under load nearly every frame compresses on its own goroutine, which reads
// its input in place and allocates nothing. A frame compressing while cores
// sit idle may take more. The budget counts only encoder goroutines, not the
// hashing, encryption, and socket work around them, so it estimates free CPU
// to a first order; callers ask for more than one goroutine only for data
// slow enough to gain from it.
var frameCoreBudget = coreBudget{limit: func() int64 { return int64(runtime.GOMAXPROCS(0)) }}

type coreBudget struct {
	inUse atomic.Int64
	limit func() int64
}

// reserve takes one goroutine without waiting, so compression never queues
// behind the budget, then tries to raise the frame's concurrency to 4 and
// then 2, within maxConcurrency. It returns the concurrency granted; release
// must return exactly that.
func (b *coreBudget) reserve(maxConcurrency int) int {
	cur := b.inUse.Add(1)
	if maxConcurrency <= 1 {
		return 1
	}
	limit := b.limit()
	for _, want := range [...]int{4, 2} {
		if want > maxConcurrency {
			continue
		}
		extra := int64(want - 1)
		for cur+extra <= limit {
			if b.inUse.CompareAndSwap(cur, cur+extra) {
				return want
			}
			cur = b.inUse.Load()
		}
	}
	return 1
}

func (b *coreBudget) release(concurrency int) {
	b.inUse.Add(-int64(concurrency))
}

// frameConcurrencyCap is the most goroutines a frame of n bytes can use:
// at most one per 1 MiB block, within the codec's and the caller's maximum.
// reserve rounds it down to 1, 2, or 4.
func frameConcurrencyCap(n int, codecMax int, callerMax int) int {
	c := min(codecMax, max(1, (n+frameBlockBytes-1)/frameBlockBytes))
	if callerMax > 0 {
		c = min(c, callerMax)
	}
	return c
}
