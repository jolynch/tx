package encoding

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/zeebo/xxh3"
)

const (
	EncodingIdentity = "identity"
	EncodingZstd     = "zstd"
	EncodingLz4      = "lz4"
)

const defaultZstdFrameSize = 4 * 1024 * 1024

type zstdMaxEncodedSizeCache struct {
	once           sync.Once
	mu             sync.RWMutex
	enc            *zstd.Encoder
	err            error
	size           map[int]int
	defaultMaxSize atomic.Int64
}

var zstdMaxSizer zstdMaxEncodedSizeCache
var zstdDecoderPool sync.Pool
var zstdEncoderPool sync.Pool
var lz4ReaderPool sync.Pool

func (c *zstdMaxEncodedSizeCache) maxEncodedSize(n int) (int, error) {
	c.once.Do(func() {
		// Same configuration as acquirePooledZstdEncoder: a bound measured on a
		// differently-tuned encoder does not bound this one's output.
		c.enc, c.err = zstd.NewWriter(io.Discard, zstdEncoderOptions()...)
		if c.err == nil {
			c.size = make(map[int]int)
			defaultMax := c.enc.MaxEncodedSize(defaultZstdFrameSize)
			c.defaultMaxSize.Store(int64(defaultMax))
			c.size[defaultZstdFrameSize] = defaultMax
		}
	})
	if c.err != nil {
		return 0, c.err
	}

	// Lock-free fast path for the default frame size.
	if n == defaultZstdFrameSize {
		return int(c.defaultMaxSize.Load()), nil
	}

	// Read-lock fast path for other cached sizes.
	c.mu.RLock()
	if cached, ok := c.size[n]; ok {
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	// Write-lock slow path for cache miss.
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.size[n]; ok {
		return cached, nil
	}
	maxSize := c.enc.MaxEncodedSize(n)
	if maxSize <= 0 {
		return 0, errors.New("invalid zstd max encoded size")
	}
	c.size[n] = maxSize
	return maxSize, nil
}

const (
	maxWSizeBucket1MiB  int64 = 1 * 1024 * 1024
	maxWSizeBucket2MiB  int64 = 2 * 1024 * 1024
	maxWSizeBucket4MiB  int64 = 4 * 1024 * 1024
	maxWSizeBucket8MiB  int64 = 8 * 1024 * 1024
	maxWSizeBucket16MiB int64 = 16 * 1024 * 1024
	maxWSizeBucket32MiB int64 = 32 * 1024 * 1024
	maxWSizeBucket64MiB int64 = 64 * 1024 * 1024
)

func SelectEncoding(acceptEncoding string) string {
	best := EncodingIdentity
	bestQ := 0.0

	for _, token := range strings.Split(acceptEncoding, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		parts := strings.Split(token, ";")
		encoding := strings.ToLower(strings.TrimSpace(parts[0]))
		q := 1.0
		for _, p := range parts[1:] {
			p = strings.TrimSpace(p)
			if !strings.HasPrefix(strings.ToLower(p), "q=") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(p[2:]), 64)
			if err != nil {
				q = 0
				break
			}
			q = parsed
		}
		if q <= 0 {
			continue
		}

		switch encoding {
		case EncodingZstd, EncodingLz4, EncodingIdentity:
		default:
			continue
		}

		if q > bestQ {
			bestQ = q
			best = encoding
		}
	}

	return best
}

func WrapCompressedWriter(dst io.Writer, acceptEncoding string, strategy string) (io.Writer, func() error, string, error) {
	if dst == nil {
		return nil, nil, "", errors.New("nil destination writer")
	}

	concurrency := 0 // fast: all cores (both lz4 and zstd treat 0 as GOMAXPROCS)
	if strategy == "gentle" {
		concurrency = 1
	}

	switch SelectEncoding(acceptEncoding) {
	case EncodingZstd:
		zw, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(1), zstd.WithEncoderConcurrency(concurrency))
		if err != nil {
			return nil, nil, "", err
		}
		return zw, zw.Close, EncodingZstd, nil
	case EncodingLz4:
		lw := lz4.NewWriter(dst)
		lw.Apply(
			lz4.BlockSizeOption(lz4.Block1Mb),
			lz4.CompressionLevelOption(lz4.Fast),
			lz4.ConcurrencyOption(concurrency),
			lz4.ChecksumOption(false),
			lz4.BlockChecksumOption(false),
		)
		return lw, lw.Close, EncodingLz4, nil
	default:
		return dst, func() error { return nil }, "", nil
	}
}

func WrapDecompressedReader(src io.Reader, contentEncoding string) (io.ReadCloser, error) {
	if src == nil {
		return nil, errors.New("nil source reader")
	}

	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", EncodingIdentity:
		return io.NopCloser(src), nil
	case EncodingZstd:
		zr, err := acquirePooledZstdDecoder(src)
		if err != nil {
			return nil, err
		}
		return &pooledZstdReadCloser{decoder: zr}, nil
	case EncodingLz4:
		return &pooledLZ4ReadCloser{reader: acquirePooledLZ4Reader(src)}, nil
	default:
		return nil, errors.New("unsupported content encoding")
	}
}

type pooledZstdReadCloser struct {
	decoder *zstd.Decoder
}

func (r *pooledZstdReadCloser) Read(p []byte) (int, error) {
	if r == nil || r.decoder == nil {
		return 0, io.EOF
	}
	return r.decoder.Read(p)
}

func (r *pooledZstdReadCloser) Close() error {
	if r == nil || r.decoder == nil {
		return nil
	}
	releasePooledZstdDecoder(r.decoder)
	r.decoder = nil
	return nil
}

type pooledLZ4ReadCloser struct {
	reader *lz4.Reader
}

func (r *pooledLZ4ReadCloser) Read(p []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, io.EOF
	}
	return r.reader.Read(p)
}

func (r *pooledLZ4ReadCloser) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	releasePooledLZ4Reader(r.reader)
	r.reader = nil
	return nil
}

func acquirePooledZstdDecoder(src io.Reader) (*zstd.Decoder, error) {
	if raw := zstdDecoderPool.Get(); raw != nil {
		if decoder, ok := raw.(*zstd.Decoder); ok && decoder != nil {
			if err := decoder.Reset(src); err == nil {
				return decoder, nil
			}
			decoder.Close()
		}
	}
	// Limit decoder history as well as the caller's output buffer. Every
	// decoder in this pool has the same limits, including after Reset.
	ceiling := uint64(DefaultMaxFrameLogicalBytes())
	return zstd.NewReader(src, zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(ceiling), zstd.WithDecoderMaxWindow(ceiling))
}

func releasePooledZstdDecoder(decoder *zstd.Decoder) {
	if decoder == nil {
		return
	}
	if err := decoder.Reset(nil); err != nil {
		decoder.Close()
		return
	}
	zstdDecoderPool.Put(decoder)
}

// zstdEncoderOptions is the single definition of how this package configures a
// zstd encoder.
//
// It exists so the encoder and the worst-case size bound cannot drift apart.
// They did: the bound was measured on a default encoder while the data was
// written by a SpeedFastest one, and SpeedFastest emits smaller blocks and so
// more block headers. For a 4 MiB chunk the default bound is 4,194,417 bytes
// while real output reached 4,194,509 — an underestimate, which silently costs
// callers that size a buffer from it an extra grow and copy.
func zstdEncoderOptions() []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
	}
}

func acquirePooledZstdEncoder(dst io.Writer) (*zstd.Encoder, error) {
	if raw := zstdEncoderPool.Get(); raw != nil {
		if encoder, ok := raw.(*zstd.Encoder); ok && encoder != nil {
			encoder.Reset(dst)
			return encoder, nil
		}
	}
	return zstd.NewWriter(dst, zstdEncoderOptions()...)
}

func releasePooledZstdEncoder(encoder *zstd.Encoder) {
	if encoder == nil {
		return
	}
	encoder.Reset(io.Discard)
	zstdEncoderPool.Put(encoder)
}

// CompressZstd returns an owned zstd frame. Use CompressZstdPooled when the
// caller can release the output promptly.
func CompressZstd(src []byte) ([]byte, error) {
	out, release, err := CompressZstdPooled(src)
	if err != nil {
		return nil, err
	}
	defer release()
	return bytes.Clone(out), nil
}

// CompressZstdPooled returns a zstd frame valid until release is called.
//
// The buffer grows with the output the encoder actually produces rather than
// being reserved from the worst-case bound. That bound is the point: for a
// 4 MiB chunk it is 4 MiB + 113 bytes, which rounds up to the 8 MiB bucket,
// while real manifest chunks are front-coded text compressing to a small
// fraction of that. Reserving the worst case would double the output buffer for
// every full chunk to serve a case that almost never happens.
//
// The size here is our own chunk size rather than anything a peer declares, so
// this is about the garbage a per-frame reservation would generate, not about
// bounding hostile input.
// zstdEmptyFrameOverheadBytes covers the frame header and checksum a zstd
// frame carries even for empty input, so a tiny chunk's hint is not zero.
const zstdEmptyFrameOverheadBytes = 64

func CompressZstdPooled(src []byte) ([]byte, func(), error) {
	// Hint from the input so a small chunk starts in a small bucket rather
	// than the 64 KiB default. NewGrowing caps the hint at that default, so a
	// full chunk still starts modestly and grows with the output.
	out, err := bufpool.NewGrowing(int64(len(src)) + zstdEmptyFrameOverheadBytes)
	if err != nil {
		return nil, nil, err
	}
	enc, err := acquirePooledZstdEncoder(out)
	if err != nil {
		out.Release()
		return nil, nil, err
	}
	if _, err := enc.Write(src); err != nil {
		releasePooledZstdEncoder(enc)
		out.Release()
		return nil, nil, err
	}
	if err := enc.Close(); err != nil {
		releasePooledZstdEncoder(enc)
		out.Release()
		return nil, nil, err
	}
	releasePooledZstdEncoder(enc)
	return out.Bytes(), out.Release, nil
}

// DecompressZstd returns the decompressed bytes of a single zstd frame. It
// reads to EOF, so the output size is whatever the frame expands to — only use
// it on locally trusted input. Network input must use DecompressZstdN.
func DecompressZstd(src []byte) ([]byte, error) {
	dec, err := acquirePooledZstdDecoder(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(dec)
	releasePooledZstdDecoder(dec)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DecompressZstdN decompresses a single zstd frame that must expand to exactly
// maxOut bytes, returning a pooled buffer and the func that releases it.
//
// Framed wire formats declare their logical size in the frame header, so the
// expected size is known before decoding, and checking during the read rather
// than decompressing to EOF and comparing afterwards is what stops a small
// adversarial frame from expanding without bound.
//
// Two properties matter for memory safety, because maxOut comes from a peer:
//
//   - It is bounded here against the frame ceiling rather than trusted. Callers
//     decoding network input are expected to have bounded it already, but the
//     guarantee cannot rest on every caller remembering to.
//   - The buffer grows with bytes actually decoded, never by sizing an
//     allocation from maxOut. Pooling alone would not give this: acquiring a
//     pooled buffer of the declared size still lets a four-byte payload commit
//     the largest permitted size, repeatedly.
func DecompressZstdN(src []byte, maxOut int64) ([]byte, func(), error) {
	if maxOut < 0 {
		return nil, nil, errors.New("invalid decompressed size bound")
	}
	if ceiling := DefaultMaxFrameLogicalBytes(); maxOut > ceiling {
		return nil, nil, fmt.Errorf("decompressed size bound %d exceeds maximum %d", maxOut, ceiling)
	}
	dec, err := acquirePooledZstdDecoder(bytes.NewReader(src))
	if err != nil {
		return nil, nil, err
	}
	defer releasePooledZstdDecoder(dec)

	out, err := bufpool.NewGrowing(maxOut)
	if err != nil {
		return nil, nil, err
	}
	// Reading one byte past maxOut is what detects a frame that expands beyond
	// what it declared.
	n, err := out.ReadFrom(io.LimitReader(dec, maxOut+1))
	if err != nil {
		out.Release()
		return nil, nil, err
	}
	if n > maxOut {
		out.Release()
		return nil, nil, fmt.Errorf("decompressed frame exceeds declared %d bytes", maxOut)
	}
	if n < maxOut {
		out.Release()
		return nil, nil, fmt.Errorf("decompressed frame shorter than declared %d bytes", maxOut)
	}
	return out.Bytes(), out.Release, nil
}

func acquirePooledLZ4Reader(src io.Reader) *lz4.Reader {
	if raw := lz4ReaderPool.Get(); raw != nil {
		if reader, ok := raw.(*lz4.Reader); ok && reader != nil {
			reader.Reset(src)
			return reader
		}
	}
	return lz4.NewReader(src)
}

func releasePooledLZ4Reader(reader *lz4.Reader) {
	if reader == nil {
		return
	}
	reader.Reset(nil)
	lz4ReaderPool.Put(reader)
}

func MaxFrameWireSizeHintBytes(comp string, logicalSize int64) (int64, error) {
	maxWire, err := MaxEncodedFrameSizeBytes(comp, logicalSize)
	if err != nil {
		return 0, err
	}
	return ceilingMaxWSizeBucketBytes(maxWire), nil
}

func MaxEncodedFrameSizeBytes(comp string, logicalSize int64) (int64, error) {
	if logicalSize <= 0 {
		return 0, errors.New("logical size must be positive")
	}
	if logicalSize > int64(^uint(0)>>1) {
		return 0, errors.New("logical size overflows int")
	}
	n := int(logicalSize)

	switch comp {
	case "none", EncodingIdentity:
		return logicalSize, nil
	case EncodingLz4:
		return int64(lz4.CompressBlockBound(n)), nil
	case EncodingZstd:
		maxSize, err := zstdMaxSizer.maxEncodedSize(n)
		if err != nil {
			return 0, err
		}
		return int64(maxSize), nil
	default:
		return 0, errors.New("unsupported compression mode")
	}
}

func ceilingMaxWSizeBucketBytes(size int64) int64 {
	if size <= maxWSizeBucket1MiB {
		return maxWSizeBucket1MiB
	}
	if size <= maxWSizeBucket2MiB {
		return maxWSizeBucket2MiB
	}
	if size <= maxWSizeBucket4MiB {
		return maxWSizeBucket4MiB
	}
	if size <= maxWSizeBucket8MiB {
		return maxWSizeBucket8MiB
	}
	if size <= maxWSizeBucket16MiB {
		return maxWSizeBucket16MiB
	}
	if size <= maxWSizeBucket32MiB {
		return maxWSizeBucket32MiB
	}
	return maxWSizeBucket64MiB
}

func FormatXXH128HashToken(v xxh3.Uint128) string {
	b := v.Bytes()
	return "xxh128:" + hex.EncodeToString(b[:])
}

func FormatXXH64HashToken(v uint64) string {
	return "xxh64:" + hex.EncodeToString([]byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}
