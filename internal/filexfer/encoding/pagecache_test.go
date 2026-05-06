package encoding

import (
	"bytes"
	"testing"

	"github.com/jolynch/tx/internal/pagecache"
)

func TestEncodePageCacheEntryRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		bits     []byte
		numPages int
	}{
		{"single resident page", []byte{0b00000001}, 1},
		{"multi-byte resident", []byte{0b10101010, 0b00010001}, 13},
		{"all resident byte", []byte{0xff}, 8},
		{"sparse 100 pages", makeSparseBitmap(100, []int{0, 7, 64, 99}), 100},
		{"odd numPages padding 5", []byte{0b00000111}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var src pagecache.CacheEntry
			if err := src.SetPageBits(tc.bits, tc.numPages); err != nil {
				t.Fatalf("SetPageBits: %v", err)
			}
			blob, err := EncodePageCacheEntry(&src)
			if err != nil {
				t.Fatalf("EncodePageCacheEntry: %v", err)
			}
			if len(blob) == 0 {
				t.Fatalf("expected non-empty blob")
			}
			var dst pagecache.CacheEntry
			if err := DecodePageCacheEntry(blob, &dst); err != nil {
				t.Fatalf("DecodePageCacheEntry: %v", err)
			}
			gotBits, gotPages := dst.PageBits()
			if gotPages != tc.numPages {
				t.Fatalf("numPages = %d, want %d", gotPages, tc.numPages)
			}
			if !bytes.Equal(gotBits, tc.bits) {
				t.Fatalf("bits = %08b, want %08b", gotBits, tc.bits)
			}
		})
	}
}

func TestEncodePageCacheEntryEmptyReturnsNil(t *testing.T) {
	var empty pagecache.CacheEntry
	blob, err := EncodePageCacheEntry(&empty)
	if err != nil {
		t.Fatalf("EncodePageCacheEntry: %v", err)
	}
	if blob != nil {
		t.Fatalf("expected nil blob for empty entry, got %x", blob)
	}
}

func TestEncodePageCacheEntryNilEntry(t *testing.T) {
	blob, err := EncodePageCacheEntry(nil)
	if err != nil {
		t.Fatalf("EncodePageCacheEntry(nil): %v", err)
	}
	if blob != nil {
		t.Fatalf("expected nil blob for nil entry")
	}
}

func TestDecodePageCacheEntryEmptyBlob(t *testing.T) {
	dst := pagecache.CacheEntry{}
	// First populate so we can verify clear behavior.
	if err := dst.SetPageBits([]byte{1}, 1); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	if err := DecodePageCacheEntry(nil, &dst); err != nil {
		t.Fatalf("DecodePageCacheEntry(nil): %v", err)
	}
	if !dst.Empty() {
		t.Fatalf("expected empty after decode of nil blob")
	}
}

func TestDecodePageCacheEntryRejectsGarbage(t *testing.T) {
	var dst pagecache.CacheEntry
	if err := DecodePageCacheEntry([]byte{0x00, 0x01, 0x02, 0x03}, &dst); err == nil {
		t.Fatalf("expected error on non-zstd payload")
	}
}

func TestDecodePageCacheEntryRejectsBadPadding(t *testing.T) {
	// Build a valid zstd payload manually to control the padding header.
	var src pagecache.CacheEntry
	if err := src.SetPageBits([]byte{1}, 1); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	blob, err := EncodePageCacheEntry(&src)
	if err != nil {
		t.Fatalf("EncodePageCacheEntry: %v", err)
	}
	corrupted := append([]byte{}, blob...)
	corrupted[0] = 8 // padding > 7
	var dst pagecache.CacheEntry
	if err := DecodePageCacheEntry(corrupted, &dst); err == nil {
		t.Fatalf("expected error for padding > 7")
	}
}

func TestEncodePageCacheToken(t *testing.T) {
	cases := []struct {
		name string
		blob []byte
		want string
	}{
		{"empty", nil, ""},
		{"zero-length", []byte{}, ""},
		{"single byte", []byte{0xab}, "pc:ab"},
		{"multi byte", []byte{0xde, 0xad, 0xbe, 0xef}, "pc:deadbeef"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EncodePageCacheToken(tc.blob); got != tc.want {
				t.Fatalf("EncodePageCacheToken = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParsePageCacheToken(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantOk   bool
		wantBlob []byte
		wantUsed int
	}{
		{"empty", "", false, nil, 0},
		{"missing prefix", "deadbeef", false, nil, 0},
		{"old prefix", "cache=deadbeef", false, nil, 0},
		{"valid eof", "pc:deadbeef", true, []byte{0xde, 0xad, 0xbe, 0xef}, len("pc:deadbeef")},
		{"valid trailing space", "pc:ab next=1", true, []byte{0xab}, len("pc:ab")},
		{"empty payload", "pc: rest", false, nil, 0},
		{"odd hex", "pc:abc", false, nil, 0},
		{"non hex", "pc:xx", false, nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blob, used, ok := ParsePageCacheToken(tc.raw)
			if ok != tc.wantOk {
				t.Fatalf("ok = %v, want %v (blob=%x used=%d)", ok, tc.wantOk, blob, used)
			}
			if !ok {
				return
			}
			if !bytes.Equal(blob, tc.wantBlob) {
				t.Fatalf("blob = %x, want %x", blob, tc.wantBlob)
			}
			if used != tc.wantUsed {
				t.Fatalf("used = %d, want %d", used, tc.wantUsed)
			}
		})
	}
}

func TestParsePageCacheTokenLeavesTrailing(t *testing.T) {
	raw := "pc:01ff foo=bar"
	blob, used, ok := ParsePageCacheToken(raw)
	if !ok {
		t.Fatalf("expected ok")
	}
	if !bytes.Equal(blob, []byte{0x01, 0xff}) {
		t.Fatalf("blob = %x", blob)
	}
	rest := raw[used:]
	if rest != " foo=bar" {
		t.Fatalf("remainder = %q, want %q", rest, " foo=bar")
	}
}

func makeSparseBitmap(numPages int, residentPages []int) []byte {
	bits := make([]byte, (numPages+7)/8)
	for _, p := range residentPages {
		bits[p/8] |= 1 << uint(p%8)
	}
	return bits
}
