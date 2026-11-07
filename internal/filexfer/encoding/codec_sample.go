package encoding

import (
	"time"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/pierrec/lz4/v4"
)

// wallStart anchors the fallback monotonic clock.
var wallStart = time.Now()

func wallClock() time.Duration { return time.Since(wallStart) }

// CodecSampleBytes includes matches beyond lz4's 64 KiB distance limit, so
// zstd can find repetition that an lz4-only sniff would miss.
const CodecSampleBytes = 256 << 10

// CodecMeasurement is one independent compression of sampled file data.
type CodecMeasurement struct {
	WireSize int64
	Elapsed  time.Duration
}

// SampleSet names the codecs a sample measures.
type SampleSet uint8

const (
	SampleLz4 SampleSet = 1 << iota
	SampleZstd

	SampleBoth = SampleLz4 | SampleZstd
)

// CodecSamples measures codecs on the same input: both while the policy
// compares codecs, fewer once it has settled. lz4's gain does not decide
// whether zstd is measured. Encode time excludes acquiring the pooled
// encoder and output buffer. A codec left unmeasured has a zero measurement.
type CodecSamples struct {
	LogicalSize int64
	Lz4, Zstd   CodecMeasurement
}

func (m CodecMeasurement) measured() bool { return m.WireSize > 0 && m.Elapsed > 0 }

// Has reports whether the sample measured codec.
func (s CodecSamples) Has(codec string) bool {
	switch codec {
	case EncodingLz4:
		return s.LogicalSize > 0 && s.Lz4.measured()
	case EncodingZstd:
		return s.LogicalSize > 0 && s.Zstd.measured()
	default:
		return false
	}
}

// Valid reports whether the sample measured at least one codec.
func (s CodecSamples) Valid() bool {
	return s.LogicalSize > 0 && (s.Lz4.measured() || s.Zstd.measured())
}

func (s CodecSamples) Ratio(codec string) float32 {
	wire := s.LogicalSize
	switch codec {
	case EncodingLz4:
		wire = s.Lz4.WireSize
	case EncodingZstd:
		wire = s.Zstd.WireSize
	}
	if wire <= 0 || s.LogicalSize <= 0 {
		return 1
	}
	return max(1, float32(s.LogicalSize)/float32(wire))
}

func (s CodecSamples) Cost(codec string) time.Duration {
	switch codec {
	case EncodingLz4:
		return s.Lz4.Elapsed
	case EncodingZstd:
		return s.Zstd.Elapsed
	default:
		return 0
	}
}

// SampleCodecs compresses at most CodecSampleBytes with each codec in set,
// timing each with clock, or with wall time when clock is nil. A thread CPU
// clock keeps a busy host's preemption out of the codecs' measured cost.
// Call it on a background worker: it does no file or socket I/O, and never
// retains p. The zstd encoder is SpeedFastest (level 1), with one worker, as
// in the adaptive ladder.
func SampleCodecs(p []byte, set SampleSet, clock func() time.Duration) (CodecSamples, error) {
	if clock == nil {
		clock = wallClock
	}
	p = p[:min(len(p), CodecSampleBytes)]
	if len(p) == 0 || set&SampleBoth == 0 {
		return CodecSamples{}, nil
	}
	s := CodecSamples{LogicalSize: int64(len(p))}
	if set&SampleLz4 != 0 {
		out, err := bufpool.Get(lz4.CompressBlockBound(len(p)))
		if err != nil {
			return CodecSamples{}, err
		}
		start := clock()
		n, err := lz4.CompressBlock(p, *out, nil)
		s.Lz4 = CodecMeasurement{WireSize: int64(n), Elapsed: max(time.Nanosecond, clock()-start)}
		bufpool.Put(out)
		if err != nil {
			return CodecSamples{}, err
		}
		if n == 0 {
			s.Lz4.WireSize = int64(len(p))
		}
	}
	if set&SampleZstd != 0 {
		enc, err := acquirePooledZstdEncoder(nil)
		if err != nil {
			return CodecSamples{}, err
		}
		// EncodeAll retains no writer. Reset would prepare unused stream state.
		defer zstdEncoderPool.Put(enc)
		out, err := bufpool.Get(enc.MaxEncodedSize(len(p)))
		if err != nil {
			return CodecSamples{}, err
		}
		defer bufpool.Put(out)
		start := clock()
		encoded := enc.EncodeAll(p, (*out)[:0])
		s.Zstd = CodecMeasurement{WireSize: int64(len(encoded)), Elapsed: max(time.Nanosecond, clock()-start)}
	}
	return s, nil
}
