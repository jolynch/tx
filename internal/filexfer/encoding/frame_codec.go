package encoding

// SEND data-frame compression: pooled lz4 and zstd encoders, one pool per
// codec and concurrency. frame_width.go decides each frame's concurrency.

import (
	"errors"
	"sync"

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
