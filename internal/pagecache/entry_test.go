package pagecache

import (
	"errors"
	"reflect"
	"testing"
)

func withStubLoad(t *testing.T, fn func(string) ([]byte, int, error)) {
	t.Helper()
	prev := loadResidencyFn
	loadResidencyFn = fn
	t.Cleanup(func() { loadResidencyFn = prev })
}

func withStubTouch(t *testing.T, fn func(string, []byte, int, bool) (int, error)) {
	t.Helper()
	prev := touchPagesFn
	prevSupported := touchSupportedFn
	touchPagesFn = fn
	touchSupportedFn = func() bool { return true }
	t.Cleanup(func() {
		touchPagesFn = prev
		touchSupportedFn = prevSupported
	})
}

func withTouchSupport(t *testing.T, supported bool) {
	t.Helper()
	prev := touchSupportedFn
	touchSupportedFn = func() bool { return supported }
	t.Cleanup(func() { touchSupportedFn = prev })
}

func TestLoadAssignsPackedBitmap(t *testing.T) {
	// Pages 0, 3, and 7 resident across two bytes; page 8 not resident.
	withStubLoad(t, func(string) ([]byte, int, error) {
		return []byte{0b10001001, 0b00000000}, 9, nil
	})

	var e CacheEntry
	if err := e.Load("ignored"); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if e.numPages != 9 {
		t.Fatalf("numPages = %d, want 9", e.numPages)
	}
	want := []byte{0b10001001, 0b00000000}
	if !reflect.DeepEqual(e.bits, want) {
		t.Fatalf("bits = %08b, want %08b", e.bits, want)
	}
}

func TestLoadEmptyFile(t *testing.T) {
	withStubLoad(t, func(string) ([]byte, int, error) {
		return nil, 0, nil
	})

	e := CacheEntry{bits: []byte{0xff}, numPages: 8}
	if err := e.Load("ignored"); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !e.Empty() {
		t.Fatalf("expected entry to be empty after loading 0-page file")
	}
	if e.numPages != 0 || e.bits != nil {
		t.Fatalf("expected zero state, got numPages=%d bits=%v", e.numPages, e.bits)
	}
}

func TestLoadPropagatesError(t *testing.T) {
	stubErr := errors.New("boom")
	withStubLoad(t, func(string) ([]byte, int, error) {
		return nil, 0, stubErr
	})

	var e CacheEntry
	if err := e.Load("ignored"); !errors.Is(err, stubErr) {
		t.Fatalf("Load err = %v, want %v", err, stubErr)
	}
}

func TestEmpty(t *testing.T) {
	cases := []struct {
		name     string
		entry    CacheEntry
		wantTrue bool
	}{
		{"zero", CacheEntry{}, true},
		{"all zero bits", CacheEntry{bits: []byte{0, 0}, numPages: 16}, true},
		{"single bit", CacheEntry{bits: []byte{1}, numPages: 1}, false},
		{"high bit", CacheEntry{bits: []byte{0, 0x80}, numPages: 16}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.entry.Empty(); got != tc.wantTrue {
				t.Fatalf("Empty = %v, want %v", got, tc.wantTrue)
			}
		})
	}
}

func TestSetPageBitsRoundTrip(t *testing.T) {
	bits := []byte{0b10001001, 0b00000001}
	var e CacheEntry
	if err := e.SetPageBits(bits, 9); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	gotBits, gotPages := e.PageBits()
	if gotPages != 9 {
		t.Fatalf("numPages = %d, want 9", gotPages)
	}
	if !reflect.DeepEqual(gotBits, bits) {
		t.Fatalf("bits = %v, want %v", gotBits, bits)
	}
	// Ensure SetPageBits copies its input.
	bits[0] = 0
	if e.bits[0] == 0 {
		t.Fatalf("SetPageBits did not copy input slice")
	}
}

func TestSetPageBitsValidation(t *testing.T) {
	cases := []struct {
		name     string
		bits     []byte
		numPages int
	}{
		{"negative pages", []byte{0}, -1},
		{"bits too short", []byte{0}, 9},
		{"bits too long", []byte{0, 0}, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var e CacheEntry
			if err := e.SetPageBits(tc.bits, tc.numPages); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestSetPageBitsZeroPages(t *testing.T) {
	e := CacheEntry{bits: []byte{1}, numPages: 1}
	if err := e.SetPageBits(nil, 0); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	if !e.Empty() || e.bits != nil {
		t.Fatalf("expected reset to empty state")
	}
}

func TestTouchEmpty(t *testing.T) {
	called := false
	withStubTouch(t, func(string, []byte, int, bool) (int, error) {
		called = true
		return 0, nil
	})
	var e CacheEntry
	if _, err := e.Touch("ignored", true); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if called {
		t.Fatalf("Touch on empty entry should not invoke syscall layer")
	}
}

func TestTouchDispatch(t *testing.T) {
	var gotPath string
	var gotPages int
	var gotBits []byte
	var gotAdvise bool
	withStubTouch(t, func(path string, bits []byte, numPages int, advise bool) (int, error) {
		gotPath = path
		gotBits = bits
		gotPages = numPages
		gotAdvise = advise
		return 0, nil
	})

	e := CacheEntry{bits: []byte{0b10001001}, numPages: 4}
	if _, err := e.Touch("/tmp/file", true); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if gotPath != "/tmp/file" || gotPages != 4 || !gotAdvise {
		t.Fatalf("Touch args: path=%q numPages=%d advise=%v", gotPath, gotPages, gotAdvise)
	}
	if !reflect.DeepEqual(gotBits, []byte{0b10001001}) {
		t.Fatalf("Touch bits = %08b", gotBits)
	}

	// Same entry, advise=false should reach the stub with advise=false.
	if _, err := e.Touch("/tmp/file", false); err != nil {
		t.Fatalf("Touch advise=false: %v", err)
	}
	if gotAdvise {
		t.Fatalf("expected stub to see advise=false on second call")
	}
}
