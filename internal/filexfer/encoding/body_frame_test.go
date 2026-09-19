package encoding

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
)

func TestChunkedManifestWriterEmitsMultipleFramesAtSizeThreshold(t *testing.T) {
	var out bytes.Buffer
	cw := NewFramedBodyWriter(&out, "none", 16, 0)
	payload := bytes.Repeat([]byte("abcdefgh"), 8) // 64 bytes -> 4 chunks of 16
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 4 {
		t.Fatalf("expected at least 4 frames, got %d", len(frames))
	}
	var reassembled bytes.Buffer
	for _, fr := range frames {
		reassembled.Write(fr.Payload)
	}
	if !bytes.Equal(reassembled.Bytes(), payload) {
		t.Fatalf("reassembled bytes differ from input")
	}
	if frames[len(frames)-1].TerminalNext != 0 {
		t.Fatalf("last frame should have next=0, got %d", frames[len(frames)-1].TerminalNext)
	}
}

func TestChunkedManifestWriterTimeBasedFlush(t *testing.T) {
	var out bytes.Buffer
	cw := NewFramedBodyWriter(&out, "none", 1024*1024, 50*time.Millisecond)
	if _, err := cw.Write([]byte("hello ")); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := cw.Write([]byte("world")); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected >=2 frames due to time-based flush, got %d", len(frames))
	}
}

func TestChunkedManifestWriterEmptyEmitsTerminalFrame(t *testing.T) {
	var out bytes.Buffer
	cw := NewFramedBodyWriter(&out, "none", 16, 0)
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 terminal frame, got %d", len(frames))
	}
	if frames[0].Meta.Size != 0 || frames[0].Meta.WireSize != 0 {
		t.Fatalf("expected size=0 wsize=0, got %+v", frames[0].Meta)
	}
	if frames[0].TerminalNext != 0 {
		t.Fatalf("expected next=0, got %d", frames[0].TerminalNext)
	}
}

func TestChunkedManifestWriterCompZstd(t *testing.T) {
	var out bytes.Buffer
	cw := NewFramedBodyWriter(&out, EncodingZstd, 32, 0)
	payload := []byte(strings.Repeat("FM/1 line: payload data here\n", 4))
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected >=2 zstd frames, got %d", len(frames))
	}
	var reassembled bytes.Buffer
	for _, fr := range frames {
		if fr.Meta.Comp != EncodingZstd {
			t.Fatalf("frame comp=%q, want zstd", fr.Meta.Comp)
		}
		if fr.Meta.WireSize == 0 && fr.Meta.Size > 0 {
			t.Fatalf("non-empty chunk has wsize=0")
		}
		if fr.Meta.Size > 0 {
			decoded, err := DecompressZstd(fr.Payload)
			if err != nil {
				t.Fatalf("DecompressZstd: %v", err)
			}
			reassembled.Write(decoded)
		}
	}
	if !bytes.Equal(reassembled.Bytes(), payload) {
		t.Fatalf("decoded bytes differ from input")
	}
}

func TestChunkedManifestWriterTerminalCarriesFileHashIntermediatesDoNot(t *testing.T) {
	var out bytes.Buffer
	cw := NewFramedBodyWriter(&out, "none", 4, 0)
	if _, err := cw.Write([]byte("abcdefghij")); err != nil { // 10 bytes -> 3 chunks
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected multiple frames, got %d", len(frames))
	}
	for i, fr := range frames {
		hasFile := fr.Trailer.FileHashToken != ""
		if i == len(frames)-1 && !hasFile {
			t.Fatalf("terminal frame missing file-hash")
		}
		if i != len(frames)-1 && hasFile {
			t.Fatalf("non-terminal frame %d has unexpected file-hash %q", i, fr.Trailer.FileHashToken)
		}
	}
}

type testFrame struct {
	Meta         FileFrameMeta
	Payload      []byte
	Trailer      FrameTrailer
	TerminalNext int64
}

func TestChunkedManifestReaderRoundTripNone(t *testing.T) {
	payload := []byte(strings.Repeat("FM/1 manifest line content here\n", 8))
	var wire bytes.Buffer
	cw := NewFramedBodyWriter(&wire, "none", 16, 0)
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r := NewFramedBodyReader(&wire, FramedBodyReaderOpts{})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, payload)
	}
	if r.FileHash() == "" {
		t.Fatalf("FileHash empty after EOF")
	}
}

func TestChunkedManifestReaderRoundTripZstd(t *testing.T) {
	payload := []byte(strings.Repeat("FM/1 entry: alpha bravo charlie delta echo\n", 8))
	var wire bytes.Buffer
	cw := NewFramedBodyWriter(&wire, EncodingZstd, 32, 0)
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var stats []FrameStats
	var raw bytes.Buffer
	r := NewFramedBodyReader(&wire, FramedBodyReaderOpts{
		OnFrame: func(s FrameStats) { stats = append(stats, s) },
		RawSink: &raw,
	})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, payload)
	}
	if len(stats) < 2 {
		t.Fatalf("expected >=2 frame stats, got %d", len(stats))
	}
	if !stats[len(stats)-1].Terminal {
		t.Fatalf("last stat must be terminal")
	}
	if raw.Len() == 0 {
		t.Fatalf("RawSink received no bytes")
	}
}

func TestChunkedManifestReaderEmpty(t *testing.T) {
	var wire bytes.Buffer
	cw := NewFramedBodyWriter(&wire, "none", 16, 0)
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r := NewFramedBodyReader(&wire, FramedBodyReaderOpts{})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", got)
	}
	if r.FileHash() == "" {
		t.Fatalf("FileHash empty after EOF")
	}
}

func TestChunkedManifestReaderRejectsCorruptedPayload(t *testing.T) {
	payload := []byte("abcdefghij")
	var wire bytes.Buffer
	cw := NewFramedBodyWriter(&wire, "none", 4, 0)
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Flip a byte in the first frame's payload. The frame layout is
	// "FX/1 ...\n<payload>\nFXT/1 ...\n", so find the first newline and
	// mutate the next byte (start of payload).
	buf := wire.Bytes()
	nl := bytes.IndexByte(buf, '\n')
	if nl < 0 || nl+1 >= len(buf) {
		t.Fatalf("malformed test fixture")
	}
	buf[nl+1] ^= 0xFF
	r := NewFramedBodyReader(bytes.NewReader(buf), FramedBodyReaderOpts{})
	if _, err := io.ReadAll(r); err == nil {
		t.Fatalf("expected error from corrupted chunk, got nil")
	}
}

func TestChunkedManifestReaderRejectsTruncatedStream(t *testing.T) {
	payload := []byte("abcdefghijklmnop")
	var wire bytes.Buffer
	cw := NewFramedBodyWriter(&wire, "none", 4, 0)
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Truncate the last frame entirely so no terminal trailer is seen.
	trimmed := wire.Bytes()[:wire.Len()/2]
	r := NewFramedBodyReader(bytes.NewReader(trimmed), FramedBodyReaderOpts{})
	_, err := io.ReadAll(r)
	if err == nil {
		t.Fatalf("expected error from truncated stream")
	}
	if err == io.EOF { //nolint:errorlint
		t.Fatalf("truncation must not surface as clean EOF: %v", err)
	}
}

func TestChunkedManifestReaderReturnsEOFAtTerminalAndLeavesTrailingBytes(t *testing.T) {
	payload := []byte("hello world")
	var wire bytes.Buffer
	cw := NewFramedBodyWriter(&wire, "none", 16, 0)
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wire.WriteString("OK\r\n") // simulate a trailing protocol line.
	r := NewFramedBodyReader(&wire, FramedBodyReaderOpts{})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %q", got)
	}
	rest, err := io.ReadAll(r.Buffered())
	if err != nil {
		t.Fatalf("read trailing bytes: %v", err)
	}
	if string(rest) != "OK\r\n" {
		t.Fatalf("trailing bytes: got %q, want %q", rest, "OK\r\n")
	}
}

func parseAllFrames(t *testing.T, wire []byte) []testFrame {
	t.Helper()
	var frames []testFrame
	for len(wire) > 0 {
		nl := bytes.IndexByte(wire, '\n')
		if nl < 0 {
			t.Fatalf("missing header newline; remaining=%q", wire)
		}
		meta, err := ParseFXHeader(string(wire[:nl]))
		if err != nil {
			t.Fatalf("ParseFXHeader: %v", err)
		}
		wire = wire[nl+1:]
		if int64(len(wire)) < meta.WireSize {
			t.Fatalf("wire short: have=%d need=%d", len(wire), meta.WireSize)
		}
		payload := append([]byte(nil), wire[:meta.WireSize]...)
		wire = wire[meta.WireSize:]
		nl = bytes.IndexByte(wire, '\n')
		if nl < 0 {
			t.Fatalf("missing trailer newline; remaining=%q", wire)
		}
		trailer, err := ParseFXTrailer(string(wire[:nl]))
		if err != nil {
			t.Fatalf("ParseFXTrailer: %v", err)
		}
		wire = wire[nl+1:]
		nextVal := int64(-1)
		if trailer.Next != nil {
			nextVal = *trailer.Next
		}
		frames = append(frames, testFrame{Meta: meta, Payload: payload, Trailer: trailer, TerminalNext: nextVal})
	}
	return frames
}

func TestChunkedManifestReaderSurfacesServerErr(t *testing.T) {
	r := NewFramedBodyReader(strings.NewReader("ERR UNPROCESSABLE path does not exist\r\n"), FramedBodyReaderOpts{})
	_, err := io.ReadAll(r)
	if err == nil {
		t.Fatalf("expected error for ERR status line")
	}
	if !strings.Contains(err.Error(), "UNPROCESSABLE path does not exist") {
		t.Fatalf("server ERR not surfaced: %v", err)
	}
	if strings.Contains(err.Error(), "invalid FX/1 header") {
		t.Fatalf("server ERR masked as frame parse error: %v", err)
	}
}

// bodyBuilder makes valid frames for tests that mutate a header field.
type bodyBuilder struct {
	buf    bytes.Buffer
	offset int64
	file   *xxh3.Hasher128
}

func newBodyBuilder() *bodyBuilder {
	return &bodyBuilder{file: xxh3.New128()}
}

func (b *bodyBuilder) add(t *testing.T, chunk []byte, terminal bool) {
	t.Helper()
	wire, err := CompressZstd(chunk)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	b.file.Write(chunk)
	header := fmt.Sprintf("FX/1 0 offset=%d size=%d wsize=%d comp=zstd hash=%s ts=1",
		b.offset, len(chunk), len(wire), FormatXXH128HashToken(xxh3.Hash128(chunk)))
	b.offset += int64(len(chunk))
	trailerPrefix := fmt.Sprintf("FXT/1 0 status=ok ts=1 next=%d", b.offset)
	if terminal {
		trailerPrefix = fmt.Sprintf("FXT/1 0 status=ok ts=1 file-hash=%s next=0",
			FormatXXH128HashToken(b.file.Sum128()))
	}
	b.buf.WriteString(header + "\n")
	b.buf.Write(wire)
	b.buf.WriteString(trailerPrefix + " hash=" + FrameHashToken(header, wire, trailerPrefix) + "\n")
}

func (b *bodyBuilder) bytes() []byte { return b.buf.Bytes() }

// hostileFrame builds a frame whose header lies about its logical size.
func hostileFrame(offset int64, declaredSize int64) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "FX/1 0 offset=%d size=%d wsize=4 comp=zstd hash=xxh128:%032d ts=1\n",
		offset, declaredSize, 0)
	buf.WriteString("AAAA")
	fmt.Fprintf(&buf, "FXT/1 0 status=ok ts=1 next=0 hash=xxh64:%016d\n", 0)
	return buf.Bytes()
}

// Oversized frame declarations must be rejected even without a cumulative cap.
func TestFramedBodyReaderRejectsOversizedDeclaredSize(t *testing.T) {
	cases := []struct {
		name string
		opts FramedBodyReaderOpts
	}{
		{"no caps set at all", FramedBodyReaderOpts{}},
		{"cumulative cap set", FramedBodyReaderOpts{MaxLogicalBytes: 1 << 30}},
		{"small cumulative cap", FramedBodyReaderOpts{MaxLogicalBytes: 4096}},
	}
	sizes := []int64{
		9000000000000000000,
		math.MaxInt64,
		DefaultMaxFrameLogicalBytes() + 1,
	}
	for _, tc := range cases {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%s/size=%d", tc.name, size), func(t *testing.T) {
				r := NewFramedBodyReader(bytes.NewReader(hostileFrame(0, size)), tc.opts)
				if _, err := io.ReadAll(r); err == nil {
					t.Fatal("frame declaring an oversized payload was accepted")
				}
			})
		}
	}
}

// A wrapped offset + size must not bypass the cumulative cap.
func TestFramedBodyReaderCumulativeCheckDoesNotOverflow(t *testing.T) {
	chunk := []byte("0123456789")
	b := newBodyBuilder()
	b.add(t, chunk, false)
	// The next declared size overflows offset + size.
	body := append(b.bytes(), hostileFrame(int64(len(chunk)), math.MaxInt64-int64(len(chunk))+1)...)

	r := NewFramedBodyReader(bytes.NewReader(body), FramedBodyReaderOpts{
		MaxLogicalBytes: 1 << 30,
	})
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("overflowing declared size bypassed the cumulative cap")
	}
}

// Check both sides of the cumulative cap.
func TestFramedBodyReaderCumulativeCapBoundaries(t *testing.T) {
	chunk := bytes.Repeat([]byte("x"), 100)
	b := newBodyBuilder()
	b.add(t, chunk, false)
	b.add(t, chunk, true)
	body := b.bytes()
	total := int64(2 * len(chunk))

	if _, err := io.ReadAll(NewFramedBodyReader(bytes.NewReader(body),
		FramedBodyReaderOpts{MaxLogicalBytes: total})); err != nil {
		t.Fatalf("body of exactly the cap was rejected: %v", err)
	}
	if _, err := io.ReadAll(NewFramedBodyReader(bytes.NewReader(body),
		FramedBodyReaderOpts{MaxLogicalBytes: total - 1})); err == nil {
		t.Fatal("body one byte over the cap was accepted")
	}
}

func TestFramedBodyReaderDefaultsPerFrameCaps(t *testing.T) {
	r := NewFramedBodyReader(bytes.NewReader(nil), FramedBodyReaderOpts{})
	want := frameCeilingBytes()
	if r.opts.MaxWireSize != want {
		t.Errorf("MaxWireSize = %d, want %d", r.opts.MaxWireSize, want)
	}
	if r.opts.MaxFrameLogicalBytes != want {
		t.Errorf("MaxFrameLogicalBytes = %d, want %d", r.opts.MaxFrameLogicalBytes, want)
	}
}

// allocatedBytes reports heap bytes allocated while fn runs.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A permitted declaration must not drive allocation before payload arrives.
func TestFramedBodyReaderAllocatesForPayloadNotForDeclaration(t *testing.T) {
	ceiling := DefaultMaxFrameLogicalBytes()
	body := hostileFrame(0, ceiling) // permitted size, 4-byte payload

	// Measure steady-state allocations.
	for i := 0; i < 3; i++ {
		_, _ = io.ReadAll(NewFramedBodyReader(bytes.NewReader(body), FramedBodyReaderOpts{}))
	}

	if raceEnabled {
		t.Skip("allocation measurement is not meaningful under -race")
	}

	const iterations = 20
	used := allocatedBytes(func() {
		for i := 0; i < iterations; i++ {
			// The short payload should fail without a large allocation.
			_, _ = io.ReadAll(NewFramedBodyReader(bytes.NewReader(body), FramedBodyReaderOpts{}))
		}
	})

	// Sizing from the declaration would exceed this budget.
	budget := uint64(ceiling) / 2
	if used > budget {
		t.Fatalf("allocated %d bytes over %d frames declaring %d bytes each with 4-byte payloads; "+
			"the declaration is sizing the allocation", used, iterations, ceiling)
	}
	t.Logf("allocated %d bytes total for %d frames each declaring %d bytes (naive sizing would be %d)",
		used, iterations, ceiling, uint64(iterations)*uint64(ceiling))
}

// The frame ceiling must admit a full chunk even when compression expands it.
func TestFramedBodyReaderAcceptsAFullSizeIncompressibleFrame(t *testing.T) {
	payload := make([]byte, DefaultBodyChunkSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	encoded, err := CompressZstd(payload)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if int64(len(encoded)) <= DefaultBodyChunkSize {
		t.Skipf("payload compressed to %d bytes; this test needs one that expands", len(encoded))
	}
	t.Logf("4 MiB of random data encodes to %d bytes (%d over the chunk size)",
		len(encoded), int64(len(encoded))-DefaultBodyChunkSize)

	b := newBodyBuilder()
	b.add(t, payload, true)

	got, err := io.ReadAll(NewFramedBodyReader(bytes.NewReader(b.bytes()), FramedBodyReaderOpts{}))
	if err != nil {
		t.Fatalf("a legitimate full-size frame was rejected: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload did not survive the round trip")
	}
}

// The socket limit cannot push the ceiling outside its fixed range.
func TestFrameCeilingClampsIntoRange(t *testing.T) {
	got := frameCeilingBytes()
	if got < minFrameCeilingBytes || got > maxFrameCeilingBytes {
		t.Fatalf("ceiling %d outside [%d, %d]", got, minFrameCeilingBytes, maxFrameCeilingBytes)
	}
	if a, b := frameCeilingBytes(), frameCeilingBytes(); a != b {
		t.Fatalf("not memoized: %d then %d", a, b)
	}
	// The ceiling must admit a full frame.
	maxEncoded, err := MaxEncodedFrameSizeBytes(EncodingZstd, DefaultBodyChunkSize)
	if err != nil {
		t.Fatalf("MaxEncodedFrameSizeBytes: %v", err)
	}
	if got < maxEncoded {
		t.Fatalf("ceiling %d is below the %d bytes a full chunk can encode to", got, maxEncoded)
	}
	t.Logf("ceiling=%d (rmem_max=%d, floor=%d, cap=%d); a full chunk encodes to at most %d",
		got, utils.MaxSocketReadBufferBytes(), minFrameCeilingBytes, maxFrameCeilingBytes, maxEncoded)
}
