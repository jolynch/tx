package encoding

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/bufpool"
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
	out, _, err := AppendCompressedFrame(nil, payload, FrameCompression{Codec: comp})
	if err != nil {
		t.Fatalf("compress (%s): %v", comp, err)
	}
	return out
}

// frameTestContent returns n deterministic bytes: random when compressible is
// false, otherwise text-like runs drawn from a small vocabulary.
func frameTestContent(seed uint64, n int, compressible bool) []byte {
	out := make([]byte, n)
	r := rand.New(rand.NewPCG(seed, 0x7478))
	if !compressible {
		for i := range out {
			out[i] = byte(r.Uint32())
		}
		return out
	}
	words := []string{"frame ", "window ", "transfer ", "offset=", "4096 ", "zstd ", "lz4 ", "\n", "status=ok "}
	for i := 0; i < n; {
		i += copy(out[i:], words[r.IntN(len(words))])
	}
	return out
}

// frameEncoders lists every data-frame encoder: each codec at each
// concurrency its pools hold.
var frameEncoders = []struct {
	codec       string
	concurrency int
	appendFrame func(dst, src []byte, concurrency int) ([]byte, error)
}{
	{EncodingLz4, 1, appendLz4Frame},
	{EncodingLz4, 2, appendLz4Frame},
	{EncodingLz4, 4, appendLz4Frame},
	{EncodingZstd, 1, appendZstdFrame},
	{EncodingZstd, 2, appendZstdFrame},
}

// checkFrameRoundTrip appends src as a frame after prefix, then checks the
// prefix survives, the frame fits MaxEncodedFrameSizeBytes, and the pooled
// readers decode it back to src under the client's window limits.
func checkFrameRoundTrip(t *testing.T, codec string, prefix, src []byte, appendFrame func([]byte, []byte) ([]byte, error)) {
	t.Helper()
	dst := append([]byte(nil), prefix...)
	out, err := appendFrame(dst, src)
	if err != nil {
		t.Fatalf("%s: compress %d bytes: %v", codec, len(src), err)
	}
	if !bytes.Equal(out[:len(prefix)], prefix) {
		t.Fatalf("%s: frame overwrote the %d-byte prefix of dst", codec, len(prefix))
	}
	frame := out[len(prefix):]
	if len(src) > 0 {
		bound, err := MaxEncodedFrameSizeBytes(codec, int64(len(src)))
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(frame)) > bound {
			t.Fatalf("%s: %d bytes encoded to %d, above the bound %d", codec, len(src), len(frame), bound)
		}
	}
	rc, err := WrapDecompressedReader(bytes.NewReader(frame), codec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("%s: decode %d-byte frame: %v", codec, len(frame), err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("%s: decoded %d bytes, want the %d bytes compressed", codec, len(got), len(src))
	}
}

// FuzzAppendCompressedFrame round-trips frames of up to 8 MiB through every
// data-frame encoder and through AppendCompressedFrame itself.
func FuzzAppendCompressedFrame(f *testing.F) {
	f.Add(uint64(1), uint32(0), uint8(0), false, uint8(0))
	f.Add(uint64(2), uint32(1), uint8(1), true, uint8(3))
	f.Add(uint64(3), uint32(1<<20), uint8(2), false, uint8(0))
	f.Add(uint64(4), uint32(1<<20+1), uint8(3), true, uint8(17))
	f.Add(uint64(5), uint32(4<<20), uint8(4), false, uint8(1))
	f.Add(uint64(6), uint32(4<<20), uint8(5), true, uint8(0))
	f.Fuzz(func(t *testing.T, seed uint64, size uint32, which uint8, compressible bool, prefixLen uint8) {
		n := int(size % (8<<20 + 1))
		src := frameTestContent(seed, n, compressible)
		prefix := frameTestContent(seed+1, int(prefixLen), false)
		i := int(which) % (len(frameEncoders) + 1)
		if i == len(frameEncoders) {
			codec := EncodingLz4
			if seed%2 == 1 {
				codec = EncodingZstd
			}
			checkFrameRoundTrip(t, codec, prefix, src, func(dst, src []byte) ([]byte, error) {
				out, c, err := AppendCompressedFrame(dst, src, FrameCompression{Codec: codec, MaxConcurrency: int(prefixLen % 5)})
				if err == nil && c != 1 && c != 2 && c != 4 {
					t.Fatalf("granted concurrency %d", c)
				}
				return out, err
			})
			return
		}
		e := frameEncoders[i]
		checkFrameRoundTrip(t, e.codec, prefix, src, func(dst, src []byte) ([]byte, error) {
			return e.appendFrame(dst, src, e.concurrency)
		})
	})
}

func TestAppendCompressedFrameRejectsUnknownCodec(t *testing.T) {
	for _, codec := range []string{"", "none", "identity", "gzip"} {
		if _, _, err := AppendCompressedFrame(nil, []byte("x"), FrameCompression{Codec: codec}); err == nil {
			t.Errorf("codec %q: want an error", codec)
		}
	}
}

// Incompressible input drives output toward the worst case, so it is what
// checks MaxEncodedFrameSizeBytes against every data-frame encoder.
func TestMaxEncodedFrameSizeBoundsDataFrames(t *testing.T) {
	for _, e := range frameEncoders {
		for _, size := range []int{1, 64, 4 << 10, 1 << 20, 1<<20 + 1, 4 << 20} {
			t.Run(fmt.Sprintf("%s/c%d/%d", e.codec, e.concurrency, size), func(t *testing.T) {
				checkFrameRoundTrip(t, e.codec, nil, frameTestContent(uint64(size), size, false), func(dst, src []byte) ([]byte, error) {
					return e.appendFrame(dst, src, e.concurrency)
				})
			})
		}
	}
}

// A compressed 4 MiB SEND frame's output buffer comes from bufpool's frame
// bucket, which must hold the worst case of both codecs.
func TestFrameBucketHoldsWorstCase(t *testing.T) {
	for _, codec := range []string{EncodingLz4, EncodingZstd} {
		bound, err := MaxEncodedFrameSizeBytes(codec, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		if bound > bufpool.FrameBucket || bufpool.BucketSize(bound) != bufpool.FrameBucket {
			t.Errorf("%s: a 4 MiB frame's bound %d does not land in the %d-byte frame bucket", codec, bound, bufpool.FrameBucket)
		}
	}
}

// At concurrency 1, which every frame gets under load, compressing a 4 MiB
// frame into a buffer sized by MaxEncodedFrameSizeBytes allocates nothing.
func TestAppendCompressedFrameDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are meaningless under the race detector")
	}
	src := frameTestContent(7, 4<<20, true)
	for _, codec := range []string{EncodingLz4, EncodingZstd} {
		bound, err := MaxEncodedFrameSizeBytes(codec, int64(len(src)))
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 0, bound)
		opts := FrameCompression{Codec: codec, MaxConcurrency: 1}
		if _, _, err := AppendCompressedFrame(dst, src, opts); err != nil { // warm the pools
			t.Fatal(err)
		}
		allocs := testing.AllocsPerRun(20, func() {
			if _, _, err := AppendCompressedFrame(dst, src, opts); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 {
			t.Errorf("%s: %.1f allocations per 4 MiB frame at concurrency 1, want 0", codec, allocs)
		}
	}
}

// Wide encoders reuse their pooled state: once warm, a frame at concurrency 2
// or 4 allocates only its goroutines and channels, never an encoder, block
// buffers, or history sized by the frame. Frames at concurrency 1 stay free
// of allocations while wide frames run between them.
func TestFrameEncodersReusePooledState(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are meaningless under the race detector")
	}
	// A collection would empty the pools mid-test, and a goroutine moving
	// between Ps misses the pooled encoder it left in the old P's private
	// slot. Neither is what this test measures.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	src := frameTestContent(9, 4<<20, true)
	dst := make([]byte, 0, bufpool.FrameBucket)
	run := func(i int) {
		e := frameEncoders[i]
		if _, err := e.appendFrame(dst, src, e.concurrency); err != nil {
			t.Fatalf("%s c%d: %v", e.codec, e.concurrency, err)
		}
	}
	for range 2 {
		for i := range frameEncoders {
			run(i)
		}
	}
	// A wide frame's block goroutines may still be returning buffers to
	// their pools after it returns; let them finish before measuring the
	// next frame, so their allocations are not charged to it.
	idle := runtime.NumGoroutine()
	settle := func() {
		for deadline := time.Now().Add(time.Second); runtime.NumGoroutine() > idle && time.Now().Before(deadline); {
			runtime.Gosched()
		}
	}
	const rounds = 20
	mallocs := make([]uint64, len(frameEncoders))
	allocated := make([]uint64, len(frameEncoders))
	var before, after runtime.MemStats
	for range rounds {
		for i := range frameEncoders {
			settle()
			runtime.ReadMemStats(&before)
			run(i)
			runtime.ReadMemStats(&after)
			mallocs[i] += after.Mallocs - before.Mallocs
			allocated[i] += after.TotalAlloc - before.TotalAlloc
		}
	}
	for i, e := range frameEncoders {
		perFrame := allocated[i] / rounds
		switch {
		case e.concurrency == 1 && mallocs[i] != 0:
			t.Errorf("%s c1: %d allocations in %d frames interleaved with wide ones, want 0", e.codec, mallocs[i], rounds)
		case perFrame > uint64(len(src))/8:
			t.Errorf("%s c%d: %d bytes allocated per warm 4 MiB frame, want at most %d", e.codec, e.concurrency, perFrame, len(src)/8)
		}
	}
}
