package encoding

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
)

const DefaultBodyChunkSize int64 = 4 * 1024 * 1024
const DefaultBodyFlushInterval = 2 * time.Second

// Request budgets count decoded metadata, independently of file-data work windows
// and transport framing. Frame and decoder buffers are additional allocations.
const (
	DefaultTargetRequestBytes  int64 = 8 * 1024 * 1024
	MaxRequestBytes            int64 = 64 * 1024 * 1024
	DefaultMaxSyncRequestBytes int64 = 1 << 30
)

const FramedBodyFileID uint64 = 0

// Frame limits follow the socket receive limit within 8–64 MiB. The 8 MiB
// floor admits a full 4 MiB chunk even when compression adds overhead; the
// 64 MiB cap keeps socket tuning from raising the allocation limit.
const (
	minFrameCeilingBytes int64 = 8 * 1024 * 1024
	maxFrameCeilingBytes int64 = 64 * 1024 * 1024
)

var (
	frameCeilingOnce  sync.Once
	frameCeilingValue int64
)

// Cache the /proc lookup across requests.
func frameCeilingBytes() int64 {
	frameCeilingOnce.Do(func() {
		v := int64(utils.MaxSocketReadBufferBytes())
		if v > maxFrameCeilingBytes {
			v = maxFrameCeilingBytes
		}
		if v < minFrameCeilingBytes {
			v = minFrameCeilingBytes
		}
		frameCeilingValue = v
	})
	return frameCeilingValue
}

// DefaultMaxFrameWireBytes bounds one frame's wire payload.
func DefaultMaxFrameWireBytes() int64 { return frameCeilingBytes() }

// DefaultMaxFrameLogicalBytes bounds one frame's decoded size.
func DefaultMaxFrameLogicalBytes() int64 { return frameCeilingBytes() }

const maxFrameLineBytes = 4 * 1024 * 1024

type FramedBodyWriter struct {
	dst           io.Writer
	comp          string
	chunkSize     int64
	flushInterval time.Duration
	offset        int64
	buf           bytes.Buffer
	fileHasher    *xxh3.Hasher128
	lastFlush     time.Time
	closed        bool
}

func NewFramedBodyWriter(dst io.Writer, comp string, chunkSize int64, flushInterval time.Duration) *FramedBodyWriter {
	if chunkSize <= 0 {
		chunkSize = DefaultBodyChunkSize
	}
	if comp == "" {
		comp = "none"
	}
	return &FramedBodyWriter{
		dst:           dst,
		comp:          comp,
		chunkSize:     chunkSize,
		flushInterval: flushInterval,
		fileHasher:    xxh3.New128(),
		lastFlush:     time.Now(),
	}
}

func (w *FramedBodyWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("write on closed FramedBodyWriter")
	}
	n, _ := w.buf.Write(p)
	for int64(w.buf.Len()) >= w.chunkSize {
		if err := w.flushChunk(int(w.chunkSize), false); err != nil {
			return 0, err
		}
	}
	if w.flushInterval > 0 && w.buf.Len() > 0 && time.Since(w.lastFlush) >= w.flushInterval {
		if err := w.flushChunk(w.buf.Len(), false); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (w *FramedBodyWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.flushChunk(w.buf.Len(), true)
}

func (w *FramedBodyWriter) flushChunk(n int, terminal bool) error {
	var chunk []byte
	if n > 0 {
		chunk = w.buf.Next(n)
	}
	var wire []byte
	switch w.comp {
	case EncodingZstd:
		c, release, err := CompressZstdPooled(chunk)
		if err != nil {
			return fmt.Errorf("compress manifest chunk: %w", err)
		}
		// Release after WriteFrame consumes wire.
		defer release()
		wire = c
	case "none":
		wire = chunk
	default:
		return fmt.Errorf("unsupported manifest comp %q", w.comp)
	}

	headerHash := FormatXXH128HashToken(xxh3.Hash128(chunk))
	w.fileHasher.Write(chunk)

	next := w.offset + int64(len(chunk))
	var fileHashes []string
	if terminal {
		next = 0
		fileHashes = []string{FormatXXH128HashToken(w.fileHasher.Sum128())}
	}

	now := time.Now().UnixMilli()
	if _, err := WriteFrame(w.dst, WriteArgs{
		FileID:     FramedBodyFileID,
		Offset:     w.offset,
		Size:       int64(len(chunk)),
		WSize:      int64(len(wire)),
		Comp:       w.comp,
		HeaderHash: headerHash,
		HeaderTS:   now,
		Payload:    wire,
		TrailerTS:  now,
		FileHashes: fileHashes,
		Next:       next,
	}); err != nil {
		return err
	}
	w.offset += int64(len(chunk))
	w.lastFlush = time.Now()
	return nil
}

// FrameStats is emitted to a FramedBodyReader's OnFrame callback
// after each successfully-validated FX/1+FXT/1 frame in the stream.
type FrameStats struct {
	FrameIndex   int
	WireBytes    int64
	LogicalBytes int64
	TotalWire    int64
	TotalLogical int64
	Terminal     bool
}

// FramedBodyReaderOpts tunes a FramedBodyReader.
type FramedBodyReaderOpts struct {
	// MaxWireSize caps a single frame's wire payload. Zero selects
	// DefaultMaxFrameWireBytes.
	MaxWireSize int64
	// MaxFrameLogicalBytes caps a single frame's declared logical (decoded)
	// size. Zero selects DefaultMaxFrameLogicalBytes.
	//
	// This cap is always active, including when MaxLogicalBytes is unset.
	MaxFrameLogicalBytes int64
	// MaxLogicalBytes caps the cumulative decompressed size of the whole
	// body across all frames. Zero disables this cap; per-frame caps remain.
	MaxLogicalBytes int64
	// OnFrame, if set, is invoked after each validated frame.
	OnFrame func(FrameStats)
	// RawSink, if set, receives the raw wire bytes of frames whose codec is
	// "zstd". Concatenated, the bytes form a standalone multi-frame zstd
	// archive that decodes to the full logical manifest.
	RawSink io.Writer
}

// FramedBodyReader is the streaming counterpart to FramedBodyWriter.
// It consumes FX/1 + FXT/1 framed body payloads (file_id=0), validates
// per-chunk and cumulative integrity, and exposes the decompressed logical
// bytes via io.Reader. It returns io.EOF after consuming the terminal frame;
// any bytes that follow (e.g. a verb-level "OK\r\n" line) remain in the
// caller's buffered reader.
type FramedBodyReader struct {
	br            *bufio.Reader
	fileHasher    *xxh3.Hasher128
	opts          FramedBodyReaderOpts
	buf           bytes.Buffer
	offset        int64
	totalWire     int64
	frameIdx      int
	done          bool
	err           error
	fileHashToken string
}

// NewFramedBodyReader wraps src and reads framed manifest bytes from it.
// If src is already a *bufio.Reader it is used as-is so the caller can keep
// reading subsequent bytes from the same buffer after this reader hits EOF.
func NewFramedBodyReader(src io.Reader, opts FramedBodyReaderOpts) *FramedBodyReader {
	var br *bufio.Reader
	if b, ok := src.(*bufio.Reader); ok {
		br = b
	} else {
		br = bufio.NewReader(src)
	}
	if opts.MaxWireSize <= 0 {
		opts.MaxWireSize = DefaultMaxFrameWireBytes()
	}
	if opts.MaxFrameLogicalBytes <= 0 {
		opts.MaxFrameLogicalBytes = DefaultMaxFrameLogicalBytes()
	}
	return &FramedBodyReader{
		br:         br,
		fileHasher: xxh3.New128(),
		opts:       opts,
	}
}

// FileHash returns the cumulative xxh128 file-hash carried on the terminal
// trailer. It is only populated after Read returns io.EOF.
func (r *FramedBodyReader) FileHash() string { return r.fileHashToken }

// Buffered exposes the underlying buffered reader so callers that wrapped a
// non-bufio source can continue reading subsequent connection bytes.
func (r *FramedBodyReader) Buffered() *bufio.Reader { return r.br }

func (r *FramedBodyReader) Read(p []byte) (int, error) {
	for r.buf.Len() == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.done {
			r.err = io.EOF
			return 0, io.EOF
		}
		if err := r.readNextFrame(); err != nil {
			r.err = err
			if r.buf.Len() == 0 {
				return 0, err
			}
			break
		}
	}
	return r.buf.Read(p)
}

func (r *FramedBodyReader) readNextFrame() error {
	headerLine, err := readFrameLine(r.br)
	if err != nil {
		return fmt.Errorf("read frame header: %w", err)
	}
	// A status line here means the server aborted before (or instead of)
	// streaming frames — surface its message rather than a frame parse error.
	if rest, ok := strings.CutPrefix(headerLine, "ERR "); ok {
		return fmt.Errorf("server error: %s", strings.TrimSpace(rest))
	}
	if headerLine == "OK" || strings.HasPrefix(headerLine, "OK ") {
		return fmt.Errorf("unexpected status line before manifest frame: %q", headerLine)
	}
	meta, err := ParseFXHeader(headerLine)
	if err != nil {
		return fmt.Errorf("invalid frame header: %w", err)
	}
	if meta.FileID != FramedBodyFileID {
		return fmt.Errorf("unexpected frame file_id=%d", meta.FileID)
	}
	if meta.HashToken == "" {
		return fmt.Errorf("frame %d missing header hash", r.frameIdx)
	}
	if meta.Offset != r.offset {
		return fmt.Errorf("frame %d offset mismatch: expected=%d got=%d", r.frameIdx, r.offset, meta.Offset)
	}
	if meta.WireSize > r.opts.MaxWireSize {
		return fmt.Errorf("frame %d wire size too large: %d", r.frameIdx, meta.WireSize)
	}
	// Reject the peer's size before reading or decoding the payload.
	if meta.Size > r.opts.MaxFrameLogicalBytes {
		return fmt.Errorf("frame %d logical size too large: %d", r.frameIdx, meta.Size)
	}
	// Subtraction avoids overflow from offset + size.
	if r.opts.MaxLogicalBytes > 0 && meta.Size > r.opts.MaxLogicalBytes-r.offset {
		return fmt.Errorf("body exceeds maximum logical size %d", r.opts.MaxLogicalBytes)
	}

	// Grow with received bytes, not the peer's declared wire size.
	wireBuf, err := bufpool.NewGrowing(meta.WireSize)
	if err != nil {
		return err
	}
	defer wireBuf.Release()
	if meta.WireSize > 0 {
		n, readErr := wireBuf.ReadFrom(io.LimitReader(r.br, meta.WireSize))
		if readErr != nil {
			return fmt.Errorf("read frame payload: %w", readErr)
		}
		if n != meta.WireSize {
			return fmt.Errorf("read frame payload: %w", io.ErrUnexpectedEOF)
		}
	}
	wire := wireBuf.Bytes()

	var chunk []byte
	switch meta.Comp {
	case EncodingZstd:
		if r.opts.RawSink != nil && len(wire) > 0 {
			if _, werr := r.opts.RawSink.Write(wire); werr != nil {
				return fmt.Errorf("write raw sink: %w", werr)
			}
		}
		// meta.Size is the declared logical size, so decoding is bounded by
		// it rather than checked against it afterwards.
		var releaseChunk func()
		chunk, releaseChunk, err = DecompressZstdN(wire, meta.Size)
		if err != nil {
			return fmt.Errorf("decompress frame %d: %w", r.frameIdx, err)
		}
		// r.buf takes a copy before release.
		defer releaseChunk()
	case "none":
		if meta.WireSize != meta.Size {
			return fmt.Errorf("frame %d none-comp wsize=%d != size=%d", r.frameIdx, meta.WireSize, meta.Size)
		}
		chunk = wire
	default:
		return fmt.Errorf("unsupported manifest frame comp=%q", meta.Comp)
	}
	wantChunkHash := FormatXXH128HashToken(xxh3.Hash128(chunk))
	if !strings.EqualFold(meta.HashToken, wantChunkHash) {
		return fmt.Errorf("frame %d header hash mismatch: want=%s got=%s", r.frameIdx, wantChunkHash, meta.HashToken)
	}

	trailerLine, err := readFrameLine(r.br)
	if err != nil {
		return fmt.Errorf("read frame trailer: %w", err)
	}
	trailer, err := ParseFXTrailer(trailerLine)
	if err != nil {
		return fmt.Errorf("invalid frame trailer: %w", err)
	}
	if trailer.FileID != FramedBodyFileID {
		return fmt.Errorf("unexpected trailer file_id=%d", trailer.FileID)
	}
	if trailer.HashToken == "" {
		return fmt.Errorf("frame %d missing trailer hash", r.frameIdx)
	}
	wantFrameHash := FrameHashToken(headerLine, wire, trailer.ChecksumPrefix)
	if !strings.EqualFold(trailer.HashToken, wantFrameHash) {
		return fmt.Errorf("frame %d trailer hash mismatch: want=%s got=%s", r.frameIdx, wantFrameHash, trailer.HashToken)
	}
	if trailer.Next == nil {
		return fmt.Errorf("frame %d missing trailer next", r.frameIdx)
	}

	r.fileHasher.Write(chunk)
	r.buf.Write(chunk)
	r.offset += meta.Size
	r.totalWire += meta.WireSize

	nextOffset := *trailer.Next
	terminal := nextOffset == 0
	if terminal {
		if trailer.FileHashToken == "" {
			return fmt.Errorf("terminal manifest frame missing file-hash")
		}
		want := FormatXXH128HashToken(r.fileHasher.Sum128())
		if !strings.EqualFold(trailer.FileHashToken, want) {
			return fmt.Errorf("manifest file-hash mismatch: want=%s got=%s", want, trailer.FileHashToken)
		}
		r.fileHashToken = trailer.FileHashToken
		r.done = true
	} else {
		if nextOffset != r.offset {
			return fmt.Errorf("frame %d next mismatch: expected=%d got=%d", r.frameIdx, r.offset, nextOffset)
		}
		if trailer.FileHashToken != "" {
			return fmt.Errorf("non-terminal manifest frame %d has file-hash", r.frameIdx)
		}
	}

	if r.opts.OnFrame != nil {
		r.opts.OnFrame(FrameStats{
			FrameIndex:   r.frameIdx,
			WireBytes:    meta.WireSize,
			LogicalBytes: meta.Size,
			TotalWire:    r.totalWire,
			TotalLogical: r.offset,
			Terminal:     terminal,
		})
	}
	r.frameIdx++
	return nil
}

// FrameHashToken computes the xxh64 frame hash carried in the
// FXT/1 trailer. The input is the header line (without trailing newline),
// the raw wire payload, and the trailer prefix (everything up to but not
// including the " hash=..." token).
func FrameHashToken(headerLine string, payload []byte, trailerPrefix string) string {
	h := xxh3.New()
	_, _ = h.Write([]byte(headerLine))
	_, _ = h.Write([]byte("\n"))
	if len(payload) > 0 {
		_, _ = h.Write(payload)
	}
	_, _ = h.Write([]byte(trailerPrefix))
	return FormatXXH64HashToken(h.Sum64())
}

func readFrameLine(br *bufio.Reader) (string, error) {
	line, err := utils.ReadLineLimit(br, maxFrameLineBytes)
	if err != nil {
		if errors.Is(err, utils.ErrLineTooLarge) {
			return "", errors.New("manifest frame line too large")
		}
		if errors.Is(err, io.EOF) {
			return "", io.ErrUnexpectedEOF
		}
		return "", err
	}
	return string(utils.TrimLineTerminator(line)), nil
}
