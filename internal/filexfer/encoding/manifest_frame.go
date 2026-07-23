package encoding

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
)

const DefaultManifestChunkSize int64 = 4 * 1024 * 1024
const DefaultManifestFlushInterval = 2 * time.Second

const ManifestFrameFileID uint64 = 0

// DefaultMaxManifestFrameWireBytes is the default upper bound on a single
// manifest frame's wire payload size. Frames larger than this are rejected
// as malformed.
const DefaultMaxManifestFrameWireBytes int64 = 64 * 1024 * 1024

const maxManifestFrameLineBytes = 4 * 1024 * 1024

type ChunkedManifestWriter struct {
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

func NewChunkedManifestWriter(dst io.Writer, comp string, chunkSize int64, flushInterval time.Duration) *ChunkedManifestWriter {
	if chunkSize <= 0 {
		chunkSize = DefaultManifestChunkSize
	}
	if comp == "" {
		comp = "none"
	}
	return &ChunkedManifestWriter{
		dst:           dst,
		comp:          comp,
		chunkSize:     chunkSize,
		flushInterval: flushInterval,
		fileHasher:    xxh3.New128(),
		lastFlush:     time.Now(),
	}
}

func (w *ChunkedManifestWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("write on closed ChunkedManifestWriter")
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

func (w *ChunkedManifestWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.flushChunk(w.buf.Len(), true)
}

func (w *ChunkedManifestWriter) flushChunk(n int, terminal bool) error {
	var chunk []byte
	if n > 0 {
		chunk = w.buf.Next(n)
	}
	var wire []byte
	switch w.comp {
	case EncodingZstd:
		c, err := CompressZstd(chunk)
		if err != nil {
			return fmt.Errorf("compress manifest chunk: %w", err)
		}
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
		FileID:     ManifestFrameFileID,
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

// ManifestFrameStats is emitted to a ChunkedManifestReader's OnFrame callback
// after each successfully-validated FX/1+FXT/1 frame in the stream.
type ManifestFrameStats struct {
	FrameIndex   int
	WireBytes    int64
	LogicalBytes int64
	TotalWire    int64
	TotalLogical int64
	Terminal     bool
}

// ChunkedManifestReaderOpts tunes a ChunkedManifestReader.
type ChunkedManifestReaderOpts struct {
	// MaxWireSize caps a single frame's wire payload. Zero selects
	// DefaultMaxManifestFrameWireBytes.
	MaxWireSize int64
	// OnFrame, if set, is invoked after each validated frame.
	OnFrame func(ManifestFrameStats)
	// RawSink, if set, receives the raw wire bytes of frames whose codec is
	// "zstd". Concatenated, the bytes form a standalone multi-frame zstd
	// archive that decodes to the full logical manifest.
	RawSink io.Writer
}

// ChunkedManifestReader is the streaming counterpart to ChunkedManifestWriter.
// It consumes FX/1 + FXT/1 framed manifest payloads (file_id=0), validates
// per-chunk and cumulative integrity, and exposes the decompressed logical
// bytes via io.Reader. It returns io.EOF after consuming the terminal frame;
// any bytes that follow (e.g. a verb-level "OK\r\n" line) remain in the
// caller's buffered reader.
type ChunkedManifestReader struct {
	br            *bufio.Reader
	fileHasher    *xxh3.Hasher128
	opts          ChunkedManifestReaderOpts
	buf           bytes.Buffer
	offset        int64
	totalWire     int64
	frameIdx      int
	done          bool
	err           error
	fileHashToken string
}

// NewChunkedManifestReader wraps src and reads framed manifest bytes from it.
// If src is already a *bufio.Reader it is used as-is so the caller can keep
// reading subsequent bytes from the same buffer after this reader hits EOF.
func NewChunkedManifestReader(src io.Reader, opts ChunkedManifestReaderOpts) *ChunkedManifestReader {
	var br *bufio.Reader
	if b, ok := src.(*bufio.Reader); ok {
		br = b
	} else {
		br = bufio.NewReader(src)
	}
	if opts.MaxWireSize <= 0 {
		opts.MaxWireSize = DefaultMaxManifestFrameWireBytes
	}
	return &ChunkedManifestReader{
		br:         br,
		fileHasher: xxh3.New128(),
		opts:       opts,
	}
}

// FileHash returns the cumulative xxh128 file-hash carried on the terminal
// trailer. It is only populated after Read returns io.EOF.
func (r *ChunkedManifestReader) FileHash() string { return r.fileHashToken }

// Buffered exposes the underlying buffered reader so callers that wrapped a
// non-bufio source can continue reading subsequent connection bytes.
func (r *ChunkedManifestReader) Buffered() *bufio.Reader { return r.br }

func (r *ChunkedManifestReader) Read(p []byte) (int, error) {
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

func (r *ChunkedManifestReader) readNextFrame() error {
	headerLine, err := readManifestFrameLine(r.br)
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
	if meta.FileID != ManifestFrameFileID {
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

	wire := make([]byte, meta.WireSize)
	if meta.WireSize > 0 {
		if _, err := io.ReadFull(r.br, wire); err != nil {
			return fmt.Errorf("read frame payload: %w", err)
		}
	}

	var chunk []byte
	switch meta.Comp {
	case EncodingZstd:
		if r.opts.RawSink != nil && len(wire) > 0 {
			if _, werr := r.opts.RawSink.Write(wire); werr != nil {
				return fmt.Errorf("write raw sink: %w", werr)
			}
		}
		chunk, err = DecompressZstd(wire)
		if err != nil {
			return fmt.Errorf("decompress frame %d: %w", r.frameIdx, err)
		}
	case "none":
		if meta.WireSize != meta.Size {
			return fmt.Errorf("frame %d none-comp wsize=%d != size=%d", r.frameIdx, meta.WireSize, meta.Size)
		}
		chunk = wire
	default:
		return fmt.Errorf("unsupported manifest frame comp=%q", meta.Comp)
	}
	if int64(len(chunk)) != meta.Size {
		return fmt.Errorf("frame %d size mismatch: header=%d actual=%d", r.frameIdx, meta.Size, len(chunk))
	}
	wantChunkHash := FormatXXH128HashToken(xxh3.Hash128(chunk))
	if !strings.EqualFold(meta.HashToken, wantChunkHash) {
		return fmt.Errorf("frame %d header hash mismatch: want=%s got=%s", r.frameIdx, wantChunkHash, meta.HashToken)
	}

	trailerLine, err := readManifestFrameLine(r.br)
	if err != nil {
		return fmt.Errorf("read frame trailer: %w", err)
	}
	trailer, err := ParseFXTrailer(trailerLine)
	if err != nil {
		return fmt.Errorf("invalid frame trailer: %w", err)
	}
	if trailer.FileID != ManifestFrameFileID {
		return fmt.Errorf("unexpected trailer file_id=%d", trailer.FileID)
	}
	if trailer.HashToken == "" {
		return fmt.Errorf("frame %d missing trailer hash", r.frameIdx)
	}
	wantFrameHash := ManifestFrameHashToken(headerLine, wire, trailer.ChecksumPrefix)
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
		r.opts.OnFrame(ManifestFrameStats{
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

// ManifestFrameHashToken computes the xxh64 frame hash carried in the
// FXT/1 trailer. The input is the header line (without trailing newline),
// the raw wire payload, and the trailer prefix (everything up to but not
// including the " hash=..." token).
func ManifestFrameHashToken(headerLine string, payload []byte, trailerPrefix string) string {
	h := xxh3.New()
	_, _ = h.Write([]byte(headerLine))
	_, _ = h.Write([]byte("\n"))
	if len(payload) > 0 {
		_, _ = h.Write(payload)
	}
	_, _ = h.Write([]byte(trailerPrefix))
	return FormatXXH64HashToken(h.Sum64())
}

func readManifestFrameLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > maxManifestFrameLineBytes {
		return "", errors.New("manifest frame line too large")
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}
