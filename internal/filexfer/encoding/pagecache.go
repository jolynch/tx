package encoding

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/jolynch/tx/internal/pagecache"
)

// EncodePageCacheEntry serializes a pagecache.CacheEntry into a compact binary
// blob suitable for embedding in an FM/1 manifest entry. The blob layout
// is [1 byte: padding 0..7][zstd(packed_bitmap_LE)] where padding is the
// number of unused high-order bits in the final bitmap byte. Returns
// (nil, nil) when entry is empty — empty entries should not be sent.
func EncodePageCacheEntry(entry *pagecache.CacheEntry) ([]byte, error) {
	if entry == nil || entry.Empty() {
		return nil, nil
	}
	bits, numPages := entry.PageBits()
	if numPages <= 0 || len(bits) == 0 {
		return nil, nil
	}
	expected := (numPages + 7) / 8
	if len(bits) != expected {
		return nil, fmt.Errorf("pagecache entry bitmap length %d does not match numPages %d", len(bits), numPages)
	}
	padding := (8 - (numPages % 8)) % 8

	// Pre-sized local buffer rather than a pool: the blob escapes to the
	// caller, so a pooled buffer would force a defensive copy at return.
	// Sizing capacity up front keeps this to a single allocation.
	hint, err := MaxEncodedFrameSizeBytes(EncodingZstd, int64(len(bits)))
	if err != nil || hint <= 0 {
		hint = int64(len(bits)) + 64
	}
	buf := bytes.NewBuffer(make([]byte, 0, hint+1))
	buf.WriteByte(byte(padding))
	enc, err := acquirePooledZstdEncoder(buf)
	if err != nil {
		return nil, fmt.Errorf("acquire zstd encoder: %w", err)
	}
	if _, err := enc.Write(bits); err != nil {
		releasePooledZstdEncoder(enc)
		return nil, fmt.Errorf("zstd write pagecache entry: %w", err)
	}
	if err := enc.Close(); err != nil {
		releasePooledZstdEncoder(enc)
		return nil, fmt.Errorf("zstd close pagecache entry: %w", err)
	}
	releasePooledZstdEncoder(enc)
	return buf.Bytes(), nil
}

// DecodePageCacheEntry reverses EncodePageCacheEntry, populating entry in place.
// An empty (nil or zero-length) blob clears the entry.
func DecodePageCacheEntry(blob []byte, entry *pagecache.CacheEntry) error {
	if entry == nil {
		return errors.New("nil pagecache entry")
	}
	if len(blob) == 0 {
		return entry.SetPageBits(nil, 0)
	}
	if len(blob) < 1 {
		return errors.New("pagecache entry blob missing padding header")
	}
	padding := int(blob[0])
	if padding > 7 {
		return fmt.Errorf("pagecache entry padding %d out of range", padding)
	}
	dec, err := acquirePooledZstdDecoder(bytes.NewReader(blob[1:]))
	if err != nil {
		return fmt.Errorf("acquire zstd decoder: %w", err)
	}
	bits, err := io.ReadAll(dec)
	releasePooledZstdDecoder(dec)
	if err != nil {
		return fmt.Errorf("zstd decode pagecache entry: %w", err)
	}
	if len(bits) == 0 {
		return errors.New("pagecache entry bitmap is empty after decompression")
	}
	numPages := len(bits)*8 - padding
	if numPages <= 0 {
		return errors.New("pagecache entry numPages is non-positive")
	}
	return entry.SetPageBits(bits, numPages)
}

// EncodePageCacheToken returns "pc:<hex>" for a non-empty blob, or ""
// for nil/empty input.
func EncodePageCacheToken(blob []byte) string {
	if len(blob) == 0 {
		return ""
	}
	return "pc:" + hex.EncodeToString(blob)
}

// ParsePageCacheToken decodes a leading "pc:<hex>" from raw and returns
// (blob, consumed, true). consumed is the number of bytes from raw that
// belong to the token (including the "pc:" prefix). Parsing stops at
// the first whitespace after "pc:" so the caller can continue reading
// trailing tokens.
//
// On a malformed hex payload the token is treated as not present
// (returns ok=false). Callers that wish to enforce well-formedness must
// check the surrounding context — typically the manifest entry parser
// will reject unknown trailing tokens which catches typos.
func ParsePageCacheToken(raw string) ([]byte, int, bool) {
	const prefix = "pc:"
	if len(raw) < len(prefix) || raw[:len(prefix)] != prefix {
		return nil, 0, false
	}
	end := len(prefix)
	for end < len(raw) && raw[end] != ' ' && raw[end] != '\t' && raw[end] != '\n' && raw[end] != '\r' {
		end++
	}
	hexBytes := raw[len(prefix):end]
	if len(hexBytes) == 0 {
		return nil, 0, false
	}
	blob, err := hex.DecodeString(hexBytes)
	if err != nil {
		return nil, 0, false
	}
	return blob, end, true
}
