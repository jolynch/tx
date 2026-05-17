package pagecache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func writeFile(t *testing.T, dir, name string, size int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", p, err)
	}
	return p
}

func TestLoadDirectoryWalksRegularFiles(t *testing.T) {
	root := t.TempDir()
	a := writeFile(t, root, "a.bin", 4096)
	b := writeFile(t, root, "sub/b.bin", 4096)
	writeFile(t, root, "empty.bin", 0)

	withStubLoad(t, func(path string) ([]byte, int, error) {
		switch path {
		case a, b:
			return []byte{1}, 1, nil
		}
		return nil, 0, nil
	})

	got, err := LoadDirectory(root, 4)
	if err != nil {
		t.Fatalf("LoadDirectory: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2 (paths: %v)", len(got), keys(got))
	}
	for _, p := range []string{a, b} {
		if _, ok := got[p]; !ok {
			t.Fatalf("missing entry for %s", p)
		}
	}
}

func TestLoadDirectorySkipsLoadErrors(t *testing.T) {
	root := t.TempDir()
	good := writeFile(t, root, "good.bin", 4096)
	bad := writeFile(t, root, "bad.bin", 4096)

	withStubLoad(t, func(path string) ([]byte, int, error) {
		if path == bad {
			return nil, 0, errors.New("simulated failure")
		}
		return []byte{1}, 1, nil
	})

	got, err := LoadDirectory(root, 2)
	if err != nil {
		t.Fatalf("LoadDirectory: %v", err)
	}
	if _, ok := got[good]; !ok {
		t.Fatalf("expected good path in results")
	}
	if _, ok := got[bad]; ok {
		t.Fatalf("bad path should have been skipped")
	}
}

func TestLoadDirectorySkipsEmptyResidency(t *testing.T) {
	root := t.TempDir()
	cold := writeFile(t, root, "cold.bin", 4096)
	hot := writeFile(t, root, "hot.bin", 4096)

	withStubLoad(t, func(path string) ([]byte, int, error) {
		if path == hot {
			return []byte{1}, 1, nil
		}
		return []byte{0}, 1, nil // file mapped, but no resident pages
	})

	got, err := LoadDirectory(root, 2)
	if err != nil {
		t.Fatalf("LoadDirectory: %v", err)
	}
	if _, ok := got[hot]; !ok {
		t.Fatalf("expected hot path in results")
	}
	if _, ok := got[cold]; ok {
		t.Fatalf("cold path should be filtered out (Empty entry)")
	}
}

func TestLoadDirectoryParallelFanOut(t *testing.T) {
	root := t.TempDir()
	const numFiles = 4
	for i := 0; i < numFiles; i++ {
		writeFile(t, root, filepath.Join("d", "f-"+itoa(i)+".bin"), 4096)
	}

	// Block every worker on a barrier until all expected workers have arrived.
	// If LoadDirectory ran serially, the second worker would never enter and
	// the test would hang — guarded by t.Deadline / -timeout.
	var arrivals int32
	gate := make(chan struct{})
	withStubLoad(t, func(string) ([]byte, int, error) {
		if atomic.AddInt32(&arrivals, 1) == int32(numFiles) {
			close(gate)
		}
		<-gate
		return []byte{1}, 1, nil
	})

	got, err := LoadDirectory(root, numFiles)
	if err != nil {
		t.Fatalf("LoadDirectory: %v", err)
	}
	if len(got) != numFiles {
		t.Fatalf("got %d entries, want %d", len(got), numFiles)
	}
}

func TestLoadDirectoryDefaultParallelism(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.bin", 4096)
	withStubLoad(t, func(string) ([]byte, int, error) {
		return []byte{1}, 1, nil
	})
	got, err := LoadDirectory(root, 0)
	if err != nil {
		t.Fatalf("LoadDirectory: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
}

func TestLoadDirectoryReturnsAbsolutePathsForRelativeRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	file := writeFile(t, root, "a.bin", 4096)
	absFile, err := filepath.Abs(file)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(parent); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	})

	var mu sync.Mutex
	var loadPath string
	withStubLoad(t, func(path string) ([]byte, int, error) {
		mu.Lock()
		loadPath = path
		mu.Unlock()
		return []byte{1}, 1, nil
	})
	got, err := LoadDirectory("root", 1)
	if err != nil {
		t.Fatalf("LoadDirectory: %v", err)
	}
	mu.Lock()
	seenLoadPath := loadPath
	mu.Unlock()
	if seenLoadPath != absFile {
		t.Fatalf("Load path = %q, want absolute %q", seenLoadPath, absFile)
	}
	if _, ok := got[absFile]; !ok {
		t.Fatalf("missing absolute file path %q in results: %v", absFile, keys(got))
	}
}

func TestLoadDirectoryWalkErrorPropagates(t *testing.T) {
	withStubLoad(t, func(string) ([]byte, int, error) {
		return []byte{1}, 1, nil
	})
	_, err := LoadDirectory(filepath.Join(t.TempDir(), "does-not-exist"), 2)
	if err == nil {
		t.Fatalf("expected walk error for missing root")
	}
}

func keys(m map[string]*CacheEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestCacheEntryNumResidentPages(t *testing.T) {
	var e CacheEntry
	if got := e.NumResidentPages(); got != 0 {
		t.Fatalf("zero-value: want 0, got %d", got)
	}
	// 17 pages, set bits at indices 0, 1, 5, 8, 16
	bits := make([]byte, 3)
	for _, idx := range []int{0, 1, 5, 8, 16} {
		bits[idx/8] |= 1 << uint(idx%8)
	}
	if err := e.SetPageBits(bits, 17); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	if got := e.NumResidentPages(); got != 5 {
		t.Fatalf("want 5 resident pages, got %d", got)
	}
}

func touchEntryGenerator(entries ...TouchEntry) TouchEntryFunc {
	return func(yield func(TouchEntry) bool) {
		for _, entry := range entries {
			if !yield(entry) {
				return
			}
		}
	}
}

func TestTouchEntriesEmpty(t *testing.T) {
	touched, err := TouchEntries(context.Background(), nil, -1, 0)
	if err != nil || touched != 0 {
		t.Fatalf("nil: got (%d, %v), want (0, nil)", touched, err)
	}
	touched, err = TouchEntries(context.Background(), touchEntryGenerator(), -1, 0)
	if err != nil || touched != 0 {
		t.Fatalf("empty: got (%d, %v), want (0, nil)", touched, err)
	}
}

func TestTouchEntriesSkipsWhenUnsupported(t *testing.T) {
	var touchCalls int32
	withStubTouch(t, func(string, []byte, int) error {
		atomic.AddInt32(&touchCalls, 1)
		return nil
	})
	withTouchSupport(t, false)

	full := mustSetEntry(t, []byte{0x01}, 1)
	var yielded int32
	touched, err := TouchEntries(context.Background(), func(yield func(TouchEntry) bool) {
		atomic.AddInt32(&yielded, 1)
		yield(TouchEntry{Path: "/a", Entry: full})
	}, -1, 1)
	if err != nil {
		t.Fatalf("TouchEntries: %v", err)
	}
	if touched != 0 {
		t.Fatalf("unsupported TouchEntries should touch 0 entries, got %d", touched)
	}
	if got := atomic.LoadInt32(&yielded); got != 0 {
		t.Fatalf("unsupported TouchEntries should not consume generator, yielded %d entries", got)
	}
	if got := atomic.LoadInt32(&touchCalls); got != 0 {
		t.Fatalf("unsupported TouchEntries should not call Touch, got %d calls", got)
	}
}

func TestTouchEntriesFanOut(t *testing.T) {
	withStubTouch(t, func(string, []byte, int) error { return nil })

	makeEntry := func(setBits int) *CacheEntry {
		bits := make([]byte, (setBits+7)/8)
		for i := 0; i < setBits; i++ {
			bits[i/8] |= 1 << uint(i%8)
		}
		return mustSetEntry(t, bits, setBits)
	}
	entries := []TouchEntry{
		{Path: "/a", Entry: makeEntry(4)},
		{Path: "/b", Entry: makeEntry(4)},
		{Path: "/c", Entry: makeEntry(4)},
	}
	touched, err := TouchEntries(context.Background(), touchEntryGenerator(entries...), -1, 4)
	if err != nil {
		t.Fatalf("TouchEntries: %v", err)
	}
	if touched != 3 {
		t.Fatalf("want touched=3, got %d", touched)
	}
}

func TestTouchEntriesRespectsBudget(t *testing.T) {
	var touchCalls int32
	withStubTouch(t, func(string, []byte, int) error {
		atomic.AddInt32(&touchCalls, 1)
		return nil
	})

	mk := func(setBits int) *CacheEntry {
		bits := make([]byte, (setBits+7)/8)
		for i := 0; i < setBits; i++ {
			bits[i/8] |= 1 << uint(i%8)
		}
		return mustSetEntry(t, bits, setBits)
	}
	entries := []TouchEntry{
		{Path: "/a", Entry: mk(10)},
		{Path: "/b", Entry: mk(10)},
		{Path: "/c", Entry: mk(10)},
	}
	// Budget of 20 pages — first two fit (10+10=20), third would overflow.
	touched, err := TouchEntries(context.Background(), touchEntryGenerator(entries...), 20, 2)
	if err != nil {
		t.Fatalf("TouchEntries: %v", err)
	}
	if touched != 2 {
		t.Fatalf("budget=20 with 10/10/10 entries: want touched=2, got %d", touched)
	}
	if got := atomic.LoadInt32(&touchCalls); got != 2 {
		t.Fatalf("want 2 Touch calls, got %d", got)
	}

	// Tighter budget: only the first entry fits.
	touchCalls = 0
	touched, err = TouchEntries(context.Background(), touchEntryGenerator(entries...), 10, 2)
	if err != nil {
		t.Fatalf("TouchEntries: %v", err)
	}
	if touched != 1 {
		t.Fatalf("budget=10: want touched=1, got %d", touched)
	}
}

func TestTouchEntriesStopsConsumingGeneratorOnBudgetOverflow(t *testing.T) {
	withStubTouch(t, func(string, []byte, int) error { return nil })

	mk := func(setBits int) *CacheEntry {
		bits := make([]byte, (setBits+7)/8)
		for i := 0; i < setBits; i++ {
			bits[i/8] |= 1 << uint(i%8)
		}
		return mustSetEntry(t, bits, setBits)
	}
	entries := []TouchEntry{
		{Path: "/a", Entry: mk(10)},
		{Path: "/b", Entry: mk(10)},
		{Path: "/c", Entry: mk(10)},
		{Path: "/d", Entry: mk(10)},
	}
	var yielded int32
	touched, err := TouchEntries(context.Background(), func(yield func(TouchEntry) bool) {
		for _, entry := range entries {
			atomic.AddInt32(&yielded, 1)
			if !yield(entry) {
				return
			}
		}
	}, 20, 1)
	if err != nil {
		t.Fatalf("TouchEntries: %v", err)
	}
	if touched != 2 {
		t.Fatalf("want touched=2, got %d", touched)
	}
	if got := atomic.LoadInt32(&yielded); got != 3 {
		t.Fatalf("want generator to stop after overflow candidate, yielded %d entries", got)
	}
}

func TestTouchEntriesStopsConsumingGeneratorOnCanceledContext(t *testing.T) {
	withStubTouch(t, func(string, []byte, int) error { return nil })
	full := mustSetEntry(t, []byte{0x01}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var yielded int32
	touched, err := TouchEntries(ctx, func(yield func(TouchEntry) bool) {
		atomic.AddInt32(&yielded, 1)
		yield(TouchEntry{Path: "/a", Entry: full})
	}, -1, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TouchEntries err = %v, want context.Canceled", err)
	}
	if touched != 0 {
		t.Fatalf("want touched=0, got %d", touched)
	}
	if got := atomic.LoadInt32(&yielded); got != 0 {
		t.Fatalf("want canceled context to avoid consuming generator, yielded %d entries", got)
	}
}

func TestTouchEntriesSkipsEmptyEntries(t *testing.T) {
	withStubTouch(t, func(string, []byte, int) error { return nil })

	full := mustSetEntry(t, []byte{0x01}, 1)
	emptyByZero := &CacheEntry{} // never SetPageBits'd
	emptyByBits := mustSetEntry(t, []byte{0x00}, 1)
	entries := []TouchEntry{
		{Path: "/full1", Entry: full},
		{Path: "/zero", Entry: emptyByZero},
		{Path: "/empty-bits", Entry: emptyByBits},
		{Path: "/full2", Entry: full},
		{Path: "/nil", Entry: nil},
	}
	touched, err := TouchEntries(context.Background(), touchEntryGenerator(entries...), -1, 2)
	if err != nil {
		t.Fatalf("TouchEntries: %v", err)
	}
	if touched != 2 {
		t.Fatalf("want touched=2 (full1+full2 only), got %d", touched)
	}
}

func TestTouchEntriesDropsPerFileErrors(t *testing.T) {
	withStubTouch(t, func(path string, _ []byte, _ int) error {
		if path == "/bad" {
			return errors.New("simulated touch error")
		}
		return nil
	})
	full := mustSetEntry(t, []byte{0x01}, 1)
	entries := []TouchEntry{
		{Path: "/ok", Entry: full},
		{Path: "/bad", Entry: full},
	}
	touched, err := TouchEntries(context.Background(), touchEntryGenerator(entries...), -1, 1)
	if err != nil {
		t.Fatalf("TouchEntries should drop per-file errors, got %v", err)
	}
	if touched != 2 {
		t.Fatalf("want touched=2 (both handed to worker), got %d", touched)
	}
}

func TestSystemPageBudget(t *testing.T) {
	// On Linux/Darwin we should get a positive budget with a zero reserve,
	// and 0 (or close to it) when the reserve eats all of RAM. On other
	// platforms we get -1 (unlimited).
	budget := SystemPageBudget(0)
	if budget == -1 {
		t.Skip("SystemPageBudget not supported on this platform")
	}
	if budget <= 0 {
		t.Fatalf("SystemPageBudget(0) on supported platform: want > 0, got %d", budget)
	}
	huge := SystemPageBudget(1 << 60) // 1 EiB reserve — exceeds any plausible RAM
	if huge != 0 {
		t.Fatalf("SystemPageBudget(huge reserve): want 0, got %d", huge)
	}
}

func TestTouchSupportedPlatform(t *testing.T) {
	got := TouchSupported()
	want := runtime.GOOS == "linux"
	if got != want {
		t.Fatalf("TouchSupported() = %v; want %v (GOOS=%s)", got, want, runtime.GOOS)
	}
}

func mustSetEntry(t *testing.T, bits []byte, numPages int) *CacheEntry {
	t.Helper()
	e := &CacheEntry{}
	if err := e.SetPageBits(bits, numPages); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	return e
}
