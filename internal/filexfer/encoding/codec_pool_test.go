package encoding

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestPooledZstdDecoderBoundsWindowAfterReset(t *testing.T) {
	good, err := CompressZstd([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	// One raw byte with a 128 MiB history window, above every frame ceiling.
	bad := []byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0x88, 9, 0, 0, 'x'}
	dec, err := acquirePooledZstdDecoder(bytes.NewReader(good))
	if err != nil {
		t.Fatal(err)
	}
	defer releasePooledZstdDecoder(dec)
	for _, wire := range [][]byte{good, bad, good} {
		if err := dec.Reset(bytes.NewReader(wire)); err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(dec)
		if bytes.Equal(wire, bad) {
			if !errors.Is(err, zstd.ErrWindowSizeExceeded) && !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
				t.Fatalf("oversized window: got %v", err)
			}
		} else if err != nil || string(out) != "x" {
			t.Fatalf("normal frame: out=%q err=%v", out, err)
		}
	}
}

func TestWrapDecompressedReaderSequentialDecode(t *testing.T) {
	payload := bytes.Repeat([]byte("tx-filexfer-pool-test-"), 4096)
	for _, comp := range []string{EncodingZstd, EncodingLz4} {
		encoded := encodeForCodecPoolTest(t, comp, payload)
		for i := 0; i < 32; i++ {
			reader, err := WrapDecompressedReader(bytes.NewReader(encoded), comp)
			if err != nil {
				t.Fatalf("comp=%s decode setup %d: %v", comp, i, err)
			}
			got, err := io.ReadAll(reader)
			if err != nil {
				_ = reader.Close()
				t.Fatalf("comp=%s decode read %d: %v", comp, i, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatalf("comp=%s decode close %d: %v", comp, i, err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("comp=%s decode mismatch at iteration %d", comp, i)
			}
		}
	}
}

func TestWrapDecompressedReaderCloseIdempotent(t *testing.T) {
	payload := bytes.Repeat([]byte("tx-filexfer-close-test-"), 1024)
	for _, comp := range []string{EncodingZstd, EncodingLz4} {
		encoded := encodeForCodecPoolTest(t, comp, payload)
		reader, err := WrapDecompressedReader(bytes.NewReader(encoded), comp)
		if err != nil {
			t.Fatalf("comp=%s decode setup: %v", comp, err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			t.Fatalf("comp=%s decode read: %v", comp, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("comp=%s first close: %v", comp, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("comp=%s second close: %v", comp, err)
		}
	}
}

func encodeForCodecPoolTest(t *testing.T, comp string, payload []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	writer, closeFn, selected, err := WrapCompressedWriter(&out, comp, "")
	if err != nil {
		t.Fatalf("compress setup (%s): %v", comp, err)
	}
	if selected != comp {
		t.Fatalf("compress mode mismatch: got=%s want=%s", selected, comp)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("compress write (%s): %v", comp, err)
	}
	if err := closeFn(); err != nil {
		t.Fatalf("compress close (%s): %v", comp, err)
	}
	return out.Bytes()
}
