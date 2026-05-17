//go:build linux

package pagecache

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadRealMincore exercises the actual loadResidency path against a
// real file, then round-trips through Touch. We do not assert exact
// residency (the kernel may evict pages between calls) — we just verify
// the syscall plumbing succeeds and reports plausible state.
func TestLoadRealMincore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	const numPages = 9
	pageSize := os.Getpagesize()
	data := make([]byte, pageSize*numPages)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Force the kernel to read the file so some pages are likely resident.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, len(data))
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	f.Close()

	var entry CacheEntry
	if err := entry.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if entry.numPages != numPages {
		t.Fatalf("numPages = %d, want %d", entry.numPages, numPages)
	}
	if bits, pages := entry.PageBits(); pages != numPages || len(bits) != 2 {
		t.Fatalf("PageBits = (%d bytes, %d pages), want (2 bytes, %d pages)", len(bits), pages, numPages)
	}

	if err := entry.Touch(path); err != nil {
		t.Fatalf("Touch: %v", err)
	}
}

func TestLoadRealMincoreEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	var entry CacheEntry
	if err := entry.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !entry.Empty() {
		t.Fatalf("expected empty entry for 0-byte file")
	}
}
