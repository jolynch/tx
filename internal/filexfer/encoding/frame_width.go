package encoding

// The adaptive concurrency of SEND data-frame encoders. Three pieces decide
// how many goroutines a frame's encoder uses:
//
//   - FrameWidth, per window: may this frame use more than one? Only when the
//     window's previous frame, of the same codec, compressed slowly.
//   - frameConcurrencyCap, per frame: at most one goroutine per 1 MiB block,
//     within the codec's maximum, rounded down to 1, 2, or 4.
//   - frameCoreBudget, per process: extra goroutines only while the encoders
//     of all frames compressing at once fit in GOMAXPROCS.

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

// compressesSlowly reports whether logical bytes compressed by concurrency
// goroutines in wall time ran below WideCompressMaxBytesPerCoreSec per core.
// The encoder's CPU time is estimated as wall time times its goroutines.
func compressesSlowly(logical int64, concurrency int, wall time.Duration) bool {
	cpu := wall.Seconds() * float64(concurrency)
	return logical > 0 && cpu > 0 && float64(logical) < WideCompressMaxBytesPerCoreSec*cpu
}

// FrameWidth tracks one window's frames and decides whether the next may use
// more than one encoder goroutine. The zero value starts narrow: a window's
// first frame always compresses on one goroutine, which costs nothing extra
// and shows how fast its data compresses.
type FrameWidth struct {
	// Gentle keeps every frame on one goroutine.
	Gentle bool

	slow  bool
	codec string
}

// Wide reports whether the next frame, compressed with codec, may use more
// than one encoder goroutine: the previous frame used the same codec and
// compressed slowly.
func (w *FrameWidth) Wide(codec string) bool {
	return !w.Gentle && w.slow && w.codec == codec
}

// Observe records a sent frame: its codec, its logical size, and the
// goroutines and wall time its encoder used. An uncompressed frame passes a
// concurrency of 0.
func (w *FrameWidth) Observe(codec string, logical int64, concurrency int, wall time.Duration) {
	w.codec = codec
	w.slow = compressesSlowly(logical, concurrency, wall)
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
// at most one per 1 MiB block, within the codec's and the caller's maximum,
// rounded down to 1, 2, or 4.
func frameConcurrencyCap(n int, codecMax int, callerMax int) int {
	c := min(codecMax, max(1, (n+frameBlockBytes-1)/frameBlockBytes))
	if callerMax > 0 {
		c = min(c, callerMax)
	}
	switch {
	case c >= 4:
		return 4
	case c >= 2:
		return 2
	default:
		return 1
	}
}
