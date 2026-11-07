package encoding

import (
	"bytes"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestMaxEncodedFrameSizeBytesZstdStableAcrossCalls(t *testing.T) {
	const logicalSize = int64(4 * 1024 * 1024)
	first, err := MaxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	for i := 0; i < 20; i++ {
		next, err := MaxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
		if err != nil {
			t.Fatalf("repeat call %d failed: %v", i, err)
		}
		if next != first {
			t.Fatalf("repeat call %d mismatch: got=%d want=%d", i, next, first)
		}
	}
}

func TestMaxEncodedFrameSizeBytesMultipleModesUnaffected(t *testing.T) {
	const logicalSize = int64(1024)

	noneSize, err := MaxEncodedFrameSizeBytes("none", logicalSize)
	if err != nil {
		t.Fatalf("none sizing failed: %v", err)
	}
	if noneSize != logicalSize {
		t.Fatalf("none sizing mismatch: got=%d want=%d", noneSize, logicalSize)
	}

	lz4Size, err := MaxEncodedFrameSizeBytes(EncodingLz4, logicalSize)
	if err != nil {
		t.Fatalf("lz4 sizing failed: %v", err)
	}
	if lz4Size < logicalSize {
		t.Fatalf("lz4 sizing unexpectedly small: %d", lz4Size)
	}

	zstdSize, err := MaxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
	if err != nil {
		t.Fatalf("zstd sizing failed: %v", err)
	}
	if zstdSize < logicalSize {
		t.Fatalf("zstd sizing unexpectedly small: %d", zstdSize)
	}
}

func TestMaxEncodedFrameSizeBytesZstdConcurrent(t *testing.T) {
	const goroutines = 32
	const logicalSize = int64(4 * 1024 * 1024)

	want, err := MaxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
	if err != nil {
		t.Fatalf("baseline call failed: %v", err)
	}

	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, callErr := MaxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
			if callErr != nil {
				errCh <- callErr
				return
			}
			if got != want {
				errCh <- fmt.Errorf("concurrent size mismatch: got=%d want=%d", got, want)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for callErr := range errCh {
		if callErr != nil {
			t.Fatalf("concurrent call failed: %v", callErr)
		}
	}
}

// A small zstd frame can expand to an arbitrarily large payload. DecompressZstdN
// bounds the output by the size the frame header already declared, so a frame
// that lies about its size is rejected instead of being materialized.
func TestDecompressZstdNRejectsOversizedExpansion(t *testing.T) {
	logical := DefaultMaxFrameLogicalBytes()
	bomb, err := CompressZstd(make([]byte, logical))
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if len(bomb) > 1<<20 {
		t.Fatalf("expected a highly compressible payload, got %d wire bytes", len(bomb))
	}

	if _, _, err := DecompressZstdN(bomb, 1024); err == nil {
		t.Fatal("frame expanding past its declared size was accepted")
	}

	out, release, err := DecompressZstdN(bomb, logical)
	if err != nil {
		t.Fatalf("honest size rejected: %v", err)
	}
	defer release()
	if int64(len(out)) != logical {
		t.Fatalf("got %d bytes, want %d", len(out), logical)
	}
}

func TestDecompressZstdNRejectsUndersizedExpansion(t *testing.T) {
	frame, err := CompressZstd([]byte("short"))
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if _, _, err := DecompressZstdN(frame, 4096); err == nil {
		t.Fatal("frame shorter than its declared size was accepted")
	}
}

func TestDecompressZstdNRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		[]byte("x"),
		[]byte(strings.Repeat("manifest line\n", 1000)),
	} {
		frame, err := CompressZstd(payload)
		if err != nil {
			t.Fatalf("compress: %v", err)
		}
		got, release, err := DecompressZstdN(frame, int64(len(payload)))
		if err != nil {
			t.Fatalf("decompress %d bytes: %v", len(payload), err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip mismatch for %d bytes", len(payload))
		}
		release()
	}
}

// manifestLikeChunk builds a chunk resembling what the framed-body writer
// actually compresses: front-coded manifest lines, which are highly
// compressible.
func manifestLikeChunk(size int) []byte {
	var b bytes.Buffer
	for i := 0; b.Len() < size; i++ {
		fmt.Fprintf(&b, "F%d 4096 13:17357712340%05d 0644 11:12:module-%06d\n", i+1, i%100000, i)
	}
	return b.Bytes()[:size]
}

// The cost the worst-case bound used to impose: MaxEncodedFrameSizeBytes for a
// 4 MiB chunk is 4 MiB + 113, which rounds to the 8 MiB bucket, so every full
// chunk reserved 8 MiB no matter how little the encoder emitted. Real manifest
// chunks compress to a small fraction of that.
func TestCompressZstdPooledAllocatesForOutputNotWorstCase(t *testing.T) {
	chunk := manifestLikeChunk(int(DefaultBodyChunkSize))

	// Establish what the output actually is, and warm the pools.
	var encodedLen int
	for i := 0; i < 3; i++ {
		out, release, err := CompressZstdPooled(chunk)
		if err != nil {
			t.Fatalf("CompressZstdPooled: %v", err)
		}
		encodedLen = len(out)
		release()
	}
	worstCase, err := MaxEncodedFrameSizeBytes(EncodingZstd, DefaultBodyChunkSize)
	if err != nil {
		t.Fatalf("MaxEncodedFrameSizeBytes: %v", err)
	}
	t.Logf("4 MiB manifest-like chunk encodes to %d bytes; worst-case bound is %d (bucket %d)",
		encodedLen, worstCase, 8*1024*1024)

	if raceEnabled {
		// The functional half above still ran; only the cost assertion is
		// skipped, since race instrumentation dominates the measurement.
		t.Skip("allocation measurement is not meaningful under -race")
	}

	const iterations = 10
	used := allocatedBytes(func() {
		for i := 0; i < iterations; i++ {
			out, release, err := CompressZstdPooled(chunk)
			if err != nil {
				t.Fatalf("CompressZstdPooled: %v", err)
			}
			if len(out) == 0 {
				t.Fatal("no output")
			}
			release()
		}
	})

	// Reserving the worst-case bound measured 68.9 MB for this loop; growing
	// with the output measures about 9.5 MB, the remainder being the encoder's
	// own working memory rather than the output buffer. The threshold sits
	// between the two so it fails if the reservation ever comes back, without
	// being brittle about encoder internals.
	reservation := uint64(iterations) * uint64(8*1024*1024)
	budget := reservation / 3
	if used > budget {
		t.Fatalf("allocated %d bytes compressing %d full chunks that encode to %d bytes each; "+
			"reserving the worst-case bound costs about %d, growing with the output about a seventh of that",
			used, iterations, encodedLen, reservation)
	}
	t.Logf("allocated %d bytes over %d full chunks; worst-case reservation would be about %d",
		used, iterations, reservation)
}

// The other side of the trade: incompressible input grows through buckets
// instead of landing in one reservation, and must still produce correct output.
func TestCompressZstdPooledHandlesIncompressibleInput(t *testing.T) {
	chunk := make([]byte, DefaultBodyChunkSize)
	if _, err := rand.Read(chunk); err != nil {
		t.Fatalf("rand: %v", err)
	}

	out, release, err := CompressZstdPooled(chunk)
	if err != nil {
		t.Fatalf("CompressZstdPooled: %v", err)
	}
	encodedLen := len(out)
	roundTrip, releaseRT, err := DecompressZstdN(out, DefaultBodyChunkSize)
	if err != nil {
		release()
		t.Fatalf("round trip: %v", err)
	}
	if !bytes.Equal(roundTrip, chunk) {
		releaseRT()
		release()
		t.Fatal("incompressible chunk did not survive the round trip")
	}
	releaseRT()
	release()
	t.Logf("4 MiB of random data encodes to %d bytes (%+d)", encodedLen, encodedLen-int(DefaultBodyChunkSize))
}

// MaxEncodedFrameSizeBytes must bound what this package's encoder actually
// emits. It did not: the bound was measured on a default zstd encoder while the
// data was written by a SpeedFastest one, which emits smaller blocks and so
// more block headers. A 4 MiB incompressible chunk encoded to 4,194,509 bytes
// against a reported bound of 4,194,417.
//
// Incompressible input is the case that matters — it is what drives output
// toward the worst case, and it is exactly what a compressible-payload test
// never reaches.
func TestMaxEncodedFrameSizeBoundsActualOutput(t *testing.T) {
	sizes := []int64{
		1,
		64,
		4 * 1024,
		64 * 1024,
		1 << 20,
		DefaultBodyChunkSize,
		DefaultBodyChunkSize + 1,
	}
	for _, size := range sizes {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			src := make([]byte, size)
			if _, err := rand.Read(src); err != nil {
				t.Fatalf("rand: %v", err)
			}
			bound, err := MaxEncodedFrameSizeBytes(EncodingZstd, size)
			if err != nil {
				t.Fatalf("MaxEncodedFrameSizeBytes: %v", err)
			}
			out, release, err := CompressZstdPooled(src)
			if err != nil {
				t.Fatalf("CompressZstdPooled: %v", err)
			}
			actual := int64(len(out))
			release()
			if actual > bound {
				t.Fatalf("%d bytes of incompressible input encoded to %d, above the reported bound %d",
					size, actual, bound)
			}
			t.Logf("size=%d encoded=%d bound=%d (slack %d)", size, actual, bound, bound-actual)
		})
	}
}

// A tiny chunk should start in a small bucket, not the 64 KiB default that
// NewGrowing uses when given no hint.
func TestCompressZstdPooledSmallInputStaysSmall(t *testing.T) {
	for _, size := range []int{0, 1, 100, 3000} {
		out, release, err := CompressZstdPooled(make([]byte, size))
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		gotCap := cap(out)
		release()
		if gotCap > 16*1024 {
			t.Errorf("a %d-byte chunk landed in a %d-byte buffer; small inputs should start small", size, gotCap)
		}
	}
}

// Framed bodies must keep zstd's frame checksum: tx recv copy saves TXFER
// frames as-is to manifest.server.zst, where it is the only integrity check.
// Data frames, verified by the client's window hash, omit it.
func TestZstdChecksumOnlyWhereNothingElseVerifies(t *testing.T) {
	payload := bytes.Repeat([]byte("fm/1 manifest body "), 1024)
	body, release, err := CompressZstdPooled(payload)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var h zstd.Header
	if err := h.Decode(body); err != nil {
		t.Fatal(err)
	}
	if !h.HasCheckSum {
		t.Fatal("CompressZstdPooled frame has no zstd checksum")
	}

	var data bytes.Buffer
	w, closeFn, _, err := WrapCompressedWriter(&data, EncodingZstd, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := closeFn(); err != nil {
		t.Fatal(err)
	}
	h = zstd.Header{}
	if err := h.Decode(data.Bytes()); err != nil {
		t.Fatal(err)
	}
	if h.HasCheckSum {
		t.Fatal("data frame carries a zstd checksum the window hash makes redundant")
	}
}

func TestSampleCodecsFindsZstdPastLz4(t *testing.T) {
	pattern := make([]byte, 128<<10)
	mrand.New(mrand.NewSource(91)).Read(pattern)
	data := bytes.Repeat(pattern, 2)
	original := bytes.Clone(data)
	s, err := SampleCodecs(data, SampleBoth, nil)
	if err != nil || !s.Valid() || !s.Has(EncodingLz4) || !s.Has(EncodingZstd) {
		t.Fatalf("sample: %+v, %v", s, err)
	}
	if s.Ratio(EncodingLz4) > 1.05 || s.Ratio(EncodingZstd) < 1.8 {
		t.Fatalf("want poor lz4 and useful zstd: lz4 %.3f zstd %.3f", s.Ratio(EncodingLz4), s.Ratio(EncodingZstd))
	}
	if !bytes.Equal(data, original) {
		t.Fatal("sample modified the caller's data")
	}
	// Bytes beyond the bounded sample do not affect either measured size.
	s2, err := SampleCodecs(append(data, make([]byte, CodecSampleBytes)...), SampleBoth, nil)
	if err != nil || s2.LogicalSize != CodecSampleBytes || s2.Lz4.WireSize != s.Lz4.WireSize || s2.Zstd.WireSize != s.Zstd.WireSize {
		t.Fatalf("unbounded sample: %+v, %v", s2, err)
	}
	if empty, err := SampleCodecs(nil, SampleBoth, nil); err != nil || empty.Valid() {
		t.Fatalf("empty sample: %+v, %v", empty, err)
	}
}

// A sample measures exactly the codecs it is asked for, and allocates
// nothing for any of them.
func TestSampleCodecsMeasuresOnlyTheRequestedSet(t *testing.T) {
	data := bytes.Repeat([]byte("segment page "), CodecSampleBytes/13)
	for _, set := range []SampleSet{0, SampleLz4, SampleZstd, SampleBoth} {
		s, err := SampleCodecs(data, set, nil)
		if err != nil {
			t.Fatal(err)
		}
		lz4, zstd := set&SampleLz4 != 0, set&SampleZstd != 0
		if s.Has(EncodingLz4) != lz4 || s.Has(EncodingZstd) != zstd || s.Valid() != (lz4 || zstd) {
			t.Fatalf("set %b: %+v", set, s)
		}
		if !lz4 && (s.Lz4 != CodecMeasurement{}) || !zstd && (s.Zstd != CodecMeasurement{}) {
			t.Fatalf("set %b measured an unrequested codec: %+v", set, s)
		}
		if raceEnabled {
			continue // The race detector drops pooled items on purpose.
		}
		if allocs := testing.AllocsPerRun(10, func() {
			if _, err := SampleCodecs(data, set, nil); err != nil {
				panic(err)
			}
		}); allocs != 0 {
			t.Fatalf("set %b allocates %.1f times per sample", set, allocs)
		}
	}
}

func TestSampleCodecsUsesTheSuppliedClock(t *testing.T) {
	var now time.Duration
	clock := func() time.Duration {
		now += 10 * time.Microsecond
		return now
	}
	sample, err := SampleCodecs(bytes.Repeat([]byte("sample "), 1024), SampleBoth, clock)
	if err != nil {
		t.Fatal(err)
	}
	if sample.Lz4.Elapsed != 10*time.Microsecond || sample.Zstd.Elapsed != 10*time.Microsecond {
		t.Fatalf("codec costs did not use the supplied clock: %+v", sample)
	}
}
