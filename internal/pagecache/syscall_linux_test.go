//go:build linux

package pagecache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jolynch/tx/internal/faults"
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

// TestSetFaultsClear checks that clearing removes only the caller's own
// injector and never reinstates an earlier one.
func TestSetFaultsClear(t *testing.T) {
	t.Cleanup(func() { injector.Store(nil) })
	a, b := faults.New(faults.Config{Seed: 1}), faults.New(faults.Config{Seed: 2})
	clearA := SetFaults(a)
	if injector.Load() != a {
		t.Fatal("SetFaults did not install its injector")
	}
	clearB := SetFaults(b)
	clearA() // out of order: b is still in use
	if injector.Load() != b {
		t.Fatal("an earlier clear removed a later injector")
	}
	clearB()
	if injector.Load() != nil {
		t.Fatal("clear reinstated an earlier injector")
	}
}

// TestPageCacheSyscallsGoThroughFaults checks every page-cache syscall site
// asks the installed injector's gate first.
func TestPageCacheSyscallsGoThroughFaults(t *testing.T) {
	t.Cleanup(func() { injector.Store(nil) })
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, make([]byte, 4*os.Getpagesize()), 0o644); err != nil {
		t.Fatal(err)
	}
	inj := faults.New(faults.Config{Seed: 1})
	defer SetFaults(inj)()
	step := func(name string, sc faults.Syscall, run func() error) {
		t.Helper()
		before := inj.Counts().Sys[sc]
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if inj.Counts().Sys[sc] == before {
			t.Fatalf("%s skipped the gate", name)
		}
	}
	step("loadResidency mmap", faults.SysMmap, func() error { _, _, err := loadResidency(path); return err })
	step("loadResidency mincore", faults.SysMincore, func() error { _, _, err := loadResidency(path); return err })
	step("loadResidencyRange mmap", faults.SysMmap, func() error { _, _, err := loadResidencyRange(path, 0, 2); return err })
	step("loadResidencyRange mincore", faults.SysMincore, func() error { _, _, err := loadResidencyRange(path, 0, 2); return err })
	step("touchPages mmap", faults.SysMmap, func() error { _, err := touchPages(path, []byte{0x0f}, 4, false); return err })
	step("touchPages fadvise", faults.SysFadvise, func() error { _, err := touchPages(path, []byte{0x0f}, 4, true); return err })
	step("evictPages fadvise", faults.SysFadvise, func() error { return evictPages(path) })
}

func TestPageCacheInjectedFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, make([]byte, 4*os.Getpagesize()), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		sc   faults.Syscall
		run  func() error
	}{
		{"load mmap", faults.SysMmap, func() error { _, _, err := loadResidency(path); return err }},
		{"load mincore", faults.SysMincore, func() error { _, _, err := loadResidency(path); return err }},
		{"range mmap", faults.SysMmap, func() error { _, _, err := loadResidencyRange(path, 1, 2); return err }},
		{"range mincore", faults.SysMincore, func() error { _, _, err := loadResidencyRange(path, 1, 2); return err }},
		{"evict fadvise", faults.SysFadvise, func() error { return evictPages(path) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: tc.sc.String() + ".fail=1"}})
			defer SetFaults(inj)()
			if err := tc.run(); err == nil {
				t.Fatal("injected syscall failure was ignored")
			}
			if got := inj.Counts().Sys[tc.sc]; got != 1 {
				t.Fatalf("%s gate asked %d times, want 1", tc.sc, got)
			}
		})
	}
}

func TestTouchPagesInjectedFailuresAreAdvisory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, make([]byte, 4*os.Getpagesize()), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		sc         faults.Syscall
		advise     bool
		wantErrors int
	}{
		{"mmap", faults.SysMmap, false, 0},
		{"fadvise", faults.SysFadvise, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: tc.sc.String() + ".fail=1"}})
			defer SetFaults(inj)()
			// Pages 0 and 2 are separate runs, each requiring its own call.
			adviseErrors, err := touchPages(path, []byte{0x05}, 4, tc.advise)
			if err != nil || adviseErrors != tc.wantErrors {
				t.Fatalf("touchPages = (%d, %v), want (%d, nil)", adviseErrors, err, tc.wantErrors)
			}
			if got := inj.Counts().Sys[tc.sc]; got != 2 {
				t.Fatalf("%s gate asked %d times, want 2", tc.sc, got)
			}
		})
	}
}
