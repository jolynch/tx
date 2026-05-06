// Package pagecache captures and replays OS page-cache residency for
// individual files using mincore() and posix_fadvise(WILLNEED). It is the
// happycache equivalent inside tx: the sender records which pages of each
// file are resident at transfer time, ships that hint alongside the file
// data, and the receiver replays the hint to warm its own page cache.
//
// The package is Linux-only at runtime; on other platforms Load and Touch
// return ErrUnsupported. The wire encoding for a CacheEntry lives in
// internal/filexfer/encoding so this package stays free of compression and
// transport concerns.
package pagecache

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// ErrUnsupported is returned by Load and Touch on platforms that do not
// support mincore / posix_fadvise.
var ErrUnsupported = errors.New("page-cache probing not supported on this platform")

// CacheEntry holds page-cache residency information for one file.
// Internal representation is opaque to callers; the encoding package uses
// PageBits / SetPageBits for serialization, while everyone else uses
// Load / Touch / Empty.
type CacheEntry struct {
	// bits is a packed bitmap. Bit (8*i + j) corresponds to page index
	// (8*i + j); a set bit means the page was resident when probed.
	bits []byte
	// numPages is the logical page count covered by bits. The valid bit
	// range is [0, numPages); higher-order bits in the final byte are
	// padding and must be zero.
	numPages int
}

// Load probes the OS page cache for the file at path and populates the
// entry. On non-Linux platforms returns ErrUnsupported. A 0-byte file
// leaves the entry empty without error.
func (e *CacheEntry) Load(path string) error {
	vec, numPages, err := loadResidencyFn(path)
	if err != nil {
		return err
	}
	if numPages == 0 {
		e.bits = nil
		e.numPages = 0
		return nil
	}
	bits := make([]byte, (numPages+7)/8)
	for i := 0; i < numPages; i++ {
		if vec[i]&1 != 0 {
			bits[i/8] |= 1 << uint(i%8)
		}
	}
	e.bits = bits
	e.numPages = numPages
	return nil
}

// Touch issues posix_fadvise(WILLNEED) over every resident page run.
// The file is opened O_RDONLY; reads past the file's actual size are
// clipped so partial files do not produce errors. On non-Linux returns
// ErrUnsupported. A no-op on empty entries.
func (e *CacheEntry) Touch(path string) error {
	if e.Empty() {
		return nil
	}
	return touchPagesFn(path, e.bits, e.numPages)
}

// Empty reports whether the entry holds any resident-page hints.
func (e *CacheEntry) Empty() bool {
	if e.numPages == 0 {
		return true
	}
	for _, b := range e.bits {
		if b != 0 {
			return false
		}
	}
	return true
}

// PageBits returns the packed bitmap and page count. Intended for the
// filexfer/encoding package to serialize an entry. The returned slice
// aliases the entry's storage; callers must not mutate it.
func (e *CacheEntry) PageBits() (bits []byte, numPages int) {
	return e.bits, e.numPages
}

// SetPageBits replaces the entry's bitmap. Intended for deserialization.
// len(bits) must equal ceil(numPages/8); higher-order padding bits in the
// final byte should be zero.
func (e *CacheEntry) SetPageBits(bits []byte, numPages int) error {
	if numPages < 0 {
		return errors.New("numPages must be >= 0")
	}
	expected := (numPages + 7) / 8
	if len(bits) != expected {
		return errors.New("bits length does not match numPages")
	}
	if numPages == 0 {
		e.bits = nil
		e.numPages = 0
		return nil
	}
	e.bits = append([]byte(nil), bits...)
	e.numPages = numPages
	return nil
}

// PageSize returns the OS page size used by Load and Touch.
func PageSize() int {
	return os.Getpagesize()
}

// LoadDirectory walks root and concurrently calls (*CacheEntry).Load for
// every regular file under it. The returned map is keyed by absolute
// filesystem path and contains exactly one entry per regular file that
// loaded with at least one resident page. Files that fail Load (including
// ErrUnsupported on non-Linux platforms) are silently skipped — cache
// hints are advisory and partial coverage is fine.
//
// parallelism <= 0 defaults to runtime.NumCPU() * 4 since Load is mostly
// syscall-bound (mmap / mincore / munmap) and benefits from
// oversubscription.
//
// Only an error returned from filepath.WalkDir surfaces here; per-file
// Load errors are dropped.
func LoadDirectory(root string, parallelism int) (map[string]*CacheEntry, error) {
	if parallelism <= 0 {
		parallelism = runtime.NumCPU() * 4
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	pathsCh := make(chan string, parallelism*2)
	type loadResult struct {
		path  string
		entry *CacheEntry
	}
	resultsCh := make(chan loadResult, parallelism*2)

	var workersWg sync.WaitGroup
	workersWg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer workersWg.Done()
			for path := range pathsCh {
				entry := &CacheEntry{}
				if err := entry.Load(path); err != nil {
					continue
				}
				if entry.Empty() {
					continue
				}
				resultsCh <- loadResult{path: path, entry: entry}
			}
		}()
	}

	walkErrCh := make(chan error, 1)
	go func() {
		defer close(pathsCh)
		walkErrCh <- filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type().IsRegular() {
				pathsCh <- path
			}
			return nil
		})
	}()

	go func() {
		workersWg.Wait()
		close(resultsCh)
	}()

	out := make(map[string]*CacheEntry)
	for r := range resultsCh {
		out[r.path] = r.entry
	}
	return out, <-walkErrCh
}
