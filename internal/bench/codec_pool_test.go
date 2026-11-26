package main

import (
	"bytes"
	"io"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

func BenchmarkWrapDecompressedReader(b *testing.B) {
	payload := bytes.Repeat([]byte("tx-filexfer-bench-"), 8192)
	for _, comp := range []string{encoding.EncodingZstd, encoding.EncodingLz4} {
		encoded := encodeForCodecPoolBench(b, comp, payload)
		b.Run(comp, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				reader, err := encoding.WrapDecompressedReader(bytes.NewReader(encoded), comp)
				if err != nil {
					b.Fatalf("decode setup: %v", err)
				}
				if _, err := io.Copy(io.Discard, reader); err != nil {
					_ = reader.Close()
					b.Fatalf("decode read: %v", err)
				}
				if err := reader.Close(); err != nil {
					b.Fatalf("decode close: %v", err)
				}
			}
		})
	}
}

func encodeForCodecPoolBench(b *testing.B, comp string, payload []byte) []byte {
	b.Helper()
	out, _, err := encoding.AppendCompressedFrame(nil, payload, encoding.FrameCompression{Codec: comp})
	if err != nil {
		b.Fatalf("compress (%s): %v", comp, err)
	}
	return out
}

// compressibleContent returns n deterministic bytes that compress about as
// well as the Silesia corpus (lz4 about 2.0x, zstd about 2.6x on 1 MiB
// blocks) without downloading it: structured log lines with varying numbers,
// prose drawn from a small vocabulary, and short random binary records.
func compressibleContent(n int64) []byte {
	words := []string{"the", "of", "and", "to", "in", "a", "is", "that", "for", "it", "as", "was", "with",
		"be", "by", "on", "not", "he", "this", "are", "or", "his", "from", "at", "which", "but", "have", "an",
		"had", "they", "you", "were", "their", "one", "all", "we", "can", "her", "has", "there", "been", "if",
		"more", "when", "will", "would", "who", "so", "no", "transfer", "window", "frame", "offset", "server",
		"client", "manifest", "checksum", "compression", "connection", "directory"}
	levels := []string{"DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"}
	keys := []string{"conn", "file", "offset", "size", "wsize", "codec", "ms", "rtt", "tid", "status"}
	r := rand.New(rand.NewPCG(1, 0x636f6d70))
	out := make([]byte, 0, n+4096)
	ts := int64(1_790_000_000_000)
	for int64(len(out)) < n {
		switch k := r.IntN(10); {
		case k < 6: // a log line
			ts += int64(r.IntN(5000))
			out = strconv.AppendInt(out, ts, 10)
			out = append(out, ' ')
			out = append(out, levels[r.IntN(len(levels))]...)
			for range 3 + r.IntN(5) {
				out = append(out, ' ')
				out = append(out, keys[r.IntN(len(keys))]...)
				out = append(out, '=')
				out = strconv.AppendUint(out, uint64(r.IntN(1<<(4+r.IntN(14)))), 10)
			}
			out = append(out, '\n')
		case k < 9: // a sentence
			for range 8 + r.IntN(24) {
				out = append(out, words[r.IntN(len(words))]...)
				out = append(out, ' ')
			}
			out = append(out, ".\n"...)
		default: // a binary record
			for range 16 + r.IntN(24) {
				out = append(out, byte(r.Uint32()))
			}
		}
	}
	return out[:n]
}

// BenchmarkAppendCompressedFrame compresses one 4 MiB SEND frame of
// compressible data into a frame-bucket buffer, as the SEND path does. c1 is
// the one-goroutine path every frame takes under load, which should allocate
// nothing; auto lets the core budget widen the encoder, as a slowly
// compressing window's later frames may.
func BenchmarkAppendCompressedFrame(b *testing.B) {
	src := compressibleContent(4 << 20)
	for _, comp := range []string{encoding.EncodingLz4, encoding.EncodingZstd} {
		for _, mode := range []struct {
			name           string
			maxConcurrency int
		}{{"c1", 1}, {"auto", 0}} {
			b.Run(comp+"/"+mode.name, func(b *testing.B) {
				dst := make([]byte, 0, bufpool.FrameBucket)
				opts := encoding.FrameCompression{Codec: comp, MaxConcurrency: mode.maxConcurrency}
				b.ReportAllocs()
				b.SetBytes(int64(len(src)))
				for b.Loop() {
					if _, _, err := encoding.AppendCompressedFrame(dst, src, opts); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
