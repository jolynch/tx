package encoding

// SEND data-frame compression: pooled lz4 and zstd encoders, and the
// process-wide core budget that decides how many goroutines a frame's encoder
// may use.

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// FrameCompression selects the codec for one SEND data frame and caps the
// goroutines its encoder may use.
type FrameCompression struct {
	// Codec is EncodingLz4 or EncodingZstd.
	Codec string
	// MaxConcurrency caps the frame's encoder goroutines. Gentle mode passes
	// 1. Zero lets the core budget decide, up to the codec's maximum.
	MaxConcurrency int
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

// frameConcurrencyIndex maps a concurrency of 1, 2, or 4 to its pool. Any
// other value is a bug: a pool must only hold encoders of its concurrency.
func frameConcurrencyIndex(concurrency int) int {
	switch concurrency {
	case 1:
		return 0
	case 2:
		return 1
	case 4:
		return 2
	}
	panic("encoding: frame concurrency must be 1, 2, or 4")
}

// AppendCompressedFrame compresses src as one lz4 or zstd frame and appends
// it to dst, returning the extended slice and the encoder goroutines it used.
// The frame carries no codec checksum, since the receiver verifies the window
// hash; do not store it where nothing else checks it.
// MaxEncodedFrameSizeBytes bounds what it appends, so a dst with that much
// spare capacity is never reallocated.
func AppendCompressedFrame(dst, src []byte, opts FrameCompression) ([]byte, int, error) {
	switch opts.Codec {
	case EncodingLz4:
		c := frameCoreBudget.reserve(frameConcurrencyCap(len(src), lz4MaxFrameConcurrency, opts.MaxConcurrency))
		defer frameCoreBudget.release(c)
		out, err := appendLz4Frame(dst, src, c)
		return out, c, err
	case EncodingZstd:
		c := frameCoreBudget.reserve(frameConcurrencyCap(len(src), zstdMaxFrameConcurrency, opts.MaxConcurrency))
		defer frameCoreBudget.release(c)
		out, err := appendZstdFrame(dst, src, c)
		return out, c, err
	default:
		return dst, 0, errors.New("unsupported frame compression codec")
	}
}

// appendBuffer is an io.Writer that appends to a caller's slice. Each pooled
// encoder owns one, so pointing an encoder at a new dst allocates nothing.
type appendBuffer struct {
	b []byte
}

func (a *appendBuffer) Write(p []byte) (int, error) {
	a.b = append(a.b, p...)
	return len(p), nil
}

// lz4FrameEncoder is a pooled lz4 frame writer configured once for data
// frames: 1 MiB blocks, the Fast level, no content or block checksums, and a
// fixed concurrency. Reset keeps those options.
type lz4FrameEncoder struct {
	w   *lz4.Writer
	out appendBuffer
}

var lz4FrameEncoderPools [3]sync.Pool

func newLz4FrameEncoder(concurrency int) (*lz4FrameEncoder, error) {
	e := &lz4FrameEncoder{w: lz4.NewWriter(nil)}
	err := e.w.Apply(
		lz4.BlockSizeOption(lz4.Block1Mb),
		lz4.CompressionLevelOption(lz4.Fast),
		lz4.ConcurrencyOption(concurrency),
		lz4.ChecksumOption(false),
		lz4.BlockChecksumOption(false),
	)
	return e, err
}

// appendLz4Frame compresses src with a pooled writer. At concurrency 1, the
// writer compresses each full 1 MiB block of src in place; at 2 and 4, it
// copies src into block buffers and compresses them on goroutines.
func appendLz4Frame(dst, src []byte, concurrency int) ([]byte, error) {
	pool := &lz4FrameEncoderPools[frameConcurrencyIndex(concurrency)]
	e, _ := pool.Get().(*lz4FrameEncoder)
	if e == nil {
		var err error
		if e, err = newLz4FrameEncoder(concurrency); err != nil {
			return dst, err
		}
	}
	e.out.b = dst
	e.w.Reset(&e.out)
	_, err := e.w.Write(src)
	if err == nil {
		err = e.w.Close()
	}
	dst = e.out.b
	e.out.b = nil
	if err != nil {
		// A writer that failed may hold state Reset does not clear; drop it.
		return dst, err
	}
	pool.Put(e)
	return dst, nil
}

// zstdFrameEncoderOptions configures the data-frame zstd encoders. They
// differ from zstdEncoderOptions, which serves framed bodies, in leaving out
// the CRC, since framed bodies keep theirs because tx recv copy stores TXFER
// frames as-is, where nothing else checks them; and in lower encoder memory,
// which saves about 4 MiB per pooled encoder at the same speed and ratio on
// 4 MiB frames.
func zstdFrameEncoderOptions(concurrency int) []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithEncoderCRC(false),
		zstd.WithLowerEncoderMem(true),
	}
}

// zstdFrameEncoder is a pooled data-frame zstd encoder with a fixed
// concurrency.
type zstdFrameEncoder struct {
	enc *zstd.Encoder
	out appendBuffer
}

var zstdFrameEncoderPools [2]sync.Pool

// appendZstdFrame compresses src with a pooled encoder. At concurrency 1,
// EncodeAll reads src in place with no streaming state. At 2, the streaming
// encoder pipelines block compression on a second goroutine.
func appendZstdFrame(dst, src []byte, concurrency int) ([]byte, error) {
	pool := &zstdFrameEncoderPools[frameConcurrencyIndex(concurrency)]
	e, _ := pool.Get().(*zstdFrameEncoder)
	if e == nil {
		enc, err := zstd.NewWriter(nil, zstdFrameEncoderOptions(concurrency)...)
		if err != nil {
			return dst, err
		}
		e = &zstdFrameEncoder{enc: enc}
	}
	if concurrency == 1 {
		dst = e.enc.EncodeAll(src, dst)
		pool.Put(e)
		return dst, nil
	}
	e.out.b = dst
	e.enc.Reset(&e.out)
	_, err := e.enc.Write(src)
	if err == nil {
		err = e.enc.Close()
	}
	dst = e.out.b
	e.out.b = nil
	if err != nil {
		return dst, err
	}
	pool.Put(e)
	return dst, nil
}
