package encoding

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/zeebo/xxh3"
)

const DefaultManifestChunkSize int64 = 4 * 1024 * 1024
const DefaultManifestFlushInterval = 2 * time.Second

const ManifestFrameFileID uint64 = 0

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
