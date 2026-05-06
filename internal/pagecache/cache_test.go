package pagecache

import (
	"errors"
	"os"
	"path/filepath"
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
