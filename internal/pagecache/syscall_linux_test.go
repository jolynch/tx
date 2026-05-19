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

	if _, err := entry.Touch(path, true); err != nil {
		t.Fatalf("Touch: %v", err)
	}
}

// TestTouchPagesAdviseFalseRoundTrip exercises the synchronous mmap+touch
// branch of touchPages. We write a multi-page file, drop its pages from
// the page cache via DONTNEED, then call touchPages with advise=false and
// confirm mincore reports the pages resident afterwards. Skipped if the
// page cache eviction can't be done (sandboxed runners) — the test still
// covers the happy path of "marked all pages resident, ran touchPages,
// got back nil error."
func TestTouchPagesAdviseFalseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	pageSize := os.Getpagesize()
	const numPages = 4
	data := make([]byte, pageSize*numPages)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Evict so we measure read-touch actually faulting pages back in.
	if err := evictPages(path); err != nil {
		t.Logf("evictPages failed (continuing): %v", err)
	}

	// Mark all 4 pages resident in the bitmap; touchPages should mmap and
	// touch each one, synchronously faulting them in.
	bits := []byte{0x0f}
	if _, err := touchPages(path, bits, numPages, false); err != nil {
		t.Fatalf("touchPages(advise=false): %v", err)
	}

	// Verify by re-probing residency.
	got, gotPages, err := loadResidencyRange(path, 0, numPages)
	if err != nil {
		t.Fatalf("loadResidencyRange: %v", err)
	}
	if gotPages != numPages {
		t.Fatalf("actualPages = %d, want %d", gotPages, numPages)
	}
	if len(got) != 1 {
		t.Fatalf("len(bits) = %d, want 1", len(got))
	}
	// Bits.OnesCount over got should be > 0 (we just touched them).
	if got[0] == 0 {
		t.Fatalf("expected at least one page resident after touchPages(advise=false), got 0 bits set")
	}
}

// TestLoadResidencyRangeReal exercises the partial-range mincore path
// against a real file. Like TestLoadRealMincore, we don't assert exact
// residency (the kernel may evict), only that the syscall plumbing works
// and returns a bitmap of the expected length.
func TestLoadResidencyRangeReal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	const numPages = 16
	pageSize := os.Getpagesize()
	data := make([]byte, pageSize*numPages)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, len(data))
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	f.Close()

	bits, got, err := loadResidencyRange(path, 4, 8)
	if err != nil {
		t.Fatalf("loadResidencyRange: %v", err)
	}
	if got != 8 {
		t.Fatalf("actualPages = %d, want 8", got)
	}
	if len(bits) != 1 {
		t.Fatalf("len(bits) = %d, want 1 (ceil(8/8))", len(bits))
	}
}

// TestLoadResidencyRangeBeyondEOF clamps numPages so the effective range
// stays within the file.
func TestLoadResidencyRangeBeyondEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.bin")
	pageSize := os.Getpagesize()
	if err := os.WriteFile(path, make([]byte, pageSize*2), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Request pages [1, 1+10) but the file only has 2 pages → effective range = 1 page.
	_, got, err := loadResidencyRange(path, 1, 10)
	if err != nil {
		t.Fatalf("loadResidencyRange: %v", err)
	}
	if got != 1 {
		t.Fatalf("actualPages = %d, want 1 (clamped to file size)", got)
	}
}

// TestLoadResidencyRangeOffsetAtEOF returns (nil, 0, nil) — nothing to probe.
func TestLoadResidencyRangeOffsetAtEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.bin")
	pageSize := os.Getpagesize()
	if err := os.WriteFile(path, make([]byte, pageSize), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	bits, got, err := loadResidencyRange(path, 5, 1)
	if err != nil {
		t.Fatalf("loadResidencyRange: %v", err)
	}
	if bits != nil || got != 0 {
		t.Fatalf("result = (%v, %d), want (nil, 0)", bits, got)
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
