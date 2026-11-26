package encoding

// The adaptive concurrency of SEND data-frame encoders. Three pieces decide
// how many goroutines a frame's encoder uses:
//
//   - FrameWidth, per window: may this frame use more than one? Only when the
//     window's previous frame, of the same codec, compressed slowly; for a
//     window's first frame, when the process's last frame of that codec did.
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
func frameSpeedup(codec string, concurrency int) float64 {
	switch {
	case concurrency <= 1:
		return 1
	case codec == EncodingZstd:
		return 1.3
	case concurrency == 2:
		return 1.6
	default:
		return 2.9
	}
}

// frameHints records, per codec, whether the process's most recent
// compressed frame compressed slowly. A window's first frame follows it, so
// a file that is a single window still compresses wide when its data is
// slow, and a workload of fast data stays narrow from its first frame.
type frameHints struct {
	lz4, zstd atomic.Bool
}

func (h *frameHints) slow(codec string) *atomic.Bool {
	switch codec {
	case EncodingLz4:
		return &h.lz4
	case EncodingZstd:
		return &h.zstd
	}
	return nil
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
}

func (w *FrameWidth) hintFor(codec string) *atomic.Bool {
	if w.hints == nil {
		return processFrameHints.slow(codec)
	}
	return w.hints.slow(codec)
}

// Wide reports whether the next frame, compressed with codec, may use more
// than one encoder goroutine: the previous frame used the same codec and
// compressed slowly, or, for a window's first frame, the process's last frame
// of that codec did.
func (w *FrameWidth) Wide(codec string) bool {
	if w.Gentle {
		return false
	}
	if w.codec == "" {
		h := w.hintFor(codec)
		return h != nil && h.Load()
	}
	return w.slow && w.codec == codec
}

// Observe records a sent frame: its codec, its logical size, and the
// goroutines and wall time its encoder used. An uncompressed frame passes a
// concurrency of 0.
func (w *FrameWidth) Observe(codec string, logical int64, concurrency int, wall time.Duration) {
	w.codec = codec
	w.slow = concurrency > 0 && compressesSlowly(logical, time.Duration(float64(wall)*frameSpeedup(codec, concurrency)))
	if h := w.hintFor(codec); h != nil && concurrency > 0 && h.Load() != w.slow {
		h.Store(w.slow)
	}
}

// frameBlockBytes is the lz4 block size of a data frame, and the unit a
// frame's input is divided into across encoder goroutines.
const frameBlockBytes = 1 << 20

// Encoder concurrency for a frame is 1, 2, or 4, so each codec keeps at most
// three encoder pools. Measured on 4 MiB frames, lz4 scales to about 2.9x at
// 4 goroutines; zstd gains about 1.3x at 2 and nothing more at 4.
const (
	lz4MaxFrameConcurrency  = 4
	zstdMaxFrameConcurrency = 2
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
