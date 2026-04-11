package main

import (
	"bytes"
	"io"
	"testing"

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
	var out bytes.Buffer
	writer, closeFn, selected, err := encoding.WrapCompressedWriter(&out, comp, "")
	if err != nil {
		b.Fatalf("compress setup (%s): %v", comp, err)
	}
	if selected != comp {
		b.Fatalf("compress mode mismatch: got=%s want=%s", selected, comp)
	}
	if _, err := writer.Write(payload); err != nil {
		b.Fatalf("compress write (%s): %v", comp, err)
	}
	if err := closeFn(); err != nil {
		b.Fatalf("compress close (%s): %v", comp, err)
	}
	return out.Bytes()
}
