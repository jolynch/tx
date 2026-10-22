//go:build linux

package pagecache

import (
	"os"
	"runtime"

	"github.com/jolynch/tx/internal/sysio"
)

const touchSupported = true

// mincoreChunkPages bounds the mincore call size so the temporary vector
// stays small on huge files (1M pages = 4 GiB at 4 KiB pages).
const mincoreChunkPages = 1 << 20

var (
	loadResidencyFn      = loadResidency
	touchPagesFn         = touchPages
	loadResidencyRangeFn = loadResidencyRange
	evictPagesFn         = evictPages
)

func loadResidency(path string) ([]byte, int, error) {
	fd, err := os.Open(path) //nolint:forbidigo // Page-cache bookkeeping; mmap, mincore, and fadvise use sysio gates.
	if err != nil {
		return nil, 0, err
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	if size == 0 {
		return nil, 0, nil
	}

	pageSize := os.Getpagesize()
	numPages := int((size + int64(pageSize) - 1) / int64(pageSize))
	mappedBytes := numPages * pageSize

	addr, err := sysio.Mmap(injector.Load(), fd.Fd(), 0, mappedBytes, sysio.PROT_NONE, sysio.MAP_SHARED)
	if err != nil {
		return nil, 0, err
	}
	defer sysio.Munmap(addr)

	bits := make([]byte, (numPages+7)/8)
	vecPages := mincoreChunkPages
	if numPages < vecPages {
		vecPages = numPages
	}
	vec := make([]byte, vecPages)
	for offset := 0; offset < numPages; offset += mincoreChunkPages {
		end := offset + mincoreChunkPages
		if end > numPages {
			end = numPages
		}
		chunkBytes := (end - offset) * pageSize
		chunk := addr[offset*pageSize : offset*pageSize+chunkBytes]
		chunkVec := vec[:end-offset]
		if mErr := mincore(chunk, chunkVec); mErr != nil {
			return nil, 0, mErr
		}
		for i, v := range chunkVec {
			if v&1 != 0 {
				page := offset + i
				bits[page/8] |= 1 << uint(page%8)
			}
		}
	}
	return bits, numPages, nil
}

// loadResidencyRange probes residency for pages [startPage, startPage+numPages)
// of the file at path and returns a packed bitmap. End-of-file clamps the
// effective range; actualPages reflects the number of pages actually probed.
// A 0-byte effective range returns (nil, 0, nil).
func loadResidencyRange(path string, startPage, numPages int) ([]byte, int, error) {
	if startPage < 0 || numPages <= 0 {
		return nil, 0, nil
	}
	fd, err := os.Open(path) //nolint:forbidigo // Page-cache bookkeeping; mmap, mincore, and fadvise use sysio gates.
	if err != nil {
		return nil, 0, err
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	pageSize := os.Getpagesize()
	offsetBytes := int64(startPage) * int64(pageSize)
	if offsetBytes >= size {
		return nil, 0, nil
	}
	end := offsetBytes + int64(numPages)*int64(pageSize)
	if end > size {
		numPages = int((size - offsetBytes + int64(pageSize) - 1) / int64(pageSize))
		end = offsetBytes + int64(numPages)*int64(pageSize)
	}
	mappedBytes := int(end - offsetBytes)

	addr, err := sysio.Mmap(injector.Load(), fd.Fd(), offsetBytes, mappedBytes, sysio.PROT_NONE, sysio.MAP_SHARED)
	if err != nil {
		return nil, 0, err
	}
	defer sysio.Munmap(addr)

	bits := make([]byte, (numPages+7)/8)
	vecPages := mincoreChunkPages
	if numPages < vecPages {
		vecPages = numPages
	}
	vec := make([]byte, vecPages)
	for offset := 0; offset < numPages; offset += mincoreChunkPages {
		chunkEnd := offset + mincoreChunkPages
		if chunkEnd > numPages {
			chunkEnd = numPages
		}
		chunkBytes := (chunkEnd - offset) * pageSize
		chunk := addr[offset*pageSize : offset*pageSize+chunkBytes]
		chunkVec := vec[:chunkEnd-offset]
		if mErr := mincore(chunk, chunkVec); mErr != nil {
			return nil, 0, mErr
		}
		for i, v := range chunkVec {
			if v&1 != 0 {
				page := offset + i
				bits[page/8] |= 1 << uint(page%8)
			}
		}
	}
	return bits, numPages, nil
}

// touchPages walks the resident-run structure described by bits. When
// advise=true it issues posix_fadvise(WILLNEED) per run — asynchronous; the
// kernel schedules readahead and returns immediately. When advise=false it
// mmaps each run PROT_READ/MAP_SHARED and touches one byte per page so any
// not-yet-resident page faults in synchronously.
//
// Returned adviseErrs counts non-nil fadvise returns; always 0 when
// advise=false. err is a setup error (open / stat). Per-run mmap failures
// in the advise=false branch are dropped (advisory).
func touchPages(path string, bits []byte, numPages int, advise bool) (int, error) {
	fd, err := os.Open(path) //nolint:forbidigo // Page-cache bookkeeping; mmap, mincore, and fadvise use sysio gates.
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		return 0, err
	}
	pageSize := int64(os.Getpagesize())
	actualPages := int((info.Size() + pageSize - 1) / pageSize)
	if actualPages < numPages {
		numPages = actualPages
	}

	const batchPages = 8 // 32 KiB at 4 KiB pages — happycache uses this batch size

	adviseErrs := 0
	var sink byte
	page := 0
	for page < numPages {
		for page < numPages && (bits[page/8]>>(page%8))&1 == 0 {
			page++
		}
		if page >= numPages {
			break
		}
		runStart := page
		for page < numPages && (bits[page/8]>>(page%8))&1 == 1 {
			page++
		}
		if advise {
			for chunkStart := runStart; chunkStart < page; chunkStart += batchPages {
				chunkEnd := chunkStart + batchPages
				if chunkEnd > page {
					chunkEnd = page
				}
				offset := int64(chunkStart) * pageSize
				length := int64(chunkEnd-chunkStart) * pageSize
				if offset+length > info.Size() {
					length = info.Size() - offset
				}
				if err := fadviseGated(fd.Fd(), offset, length, sysio.FADV_WILLNEED); err != nil {
					adviseErrs++
				}
			}
		} else {
			offset := int64(runStart) * pageSize
			length := int64(page-runStart) * pageSize
			if offset+length > info.Size() {
				length = info.Size() - offset
			}
			if length <= 0 {
				continue
			}
			addr, mErr := sysio.Mmap(injector.Load(), fd.Fd(), offset, int(length), sysio.PROT_READ, sysio.MAP_SHARED)
			if mErr != nil {
				continue // advisory; drop per-run mmap failures
			}
			// Prevents the compiler skipping
			for p := int64(0); p < length; p += pageSize {
				sink ^= addr[p]
			}
			_ = sysio.Munmap(addr)
		}
	}
	runtime.KeepAlive(sink)
	return adviseErrs, nil
}

// evictPages issues posix_fadvise(FADV_DONTNEED) over the full length of
// the file at path so the kernel may release its page-cache state. Used to
// wipe a receiver's writer-populated cache before warming with the sender's
// snapshot. Returns the fadvise error if it failed.
func evictPages(path string) error {
	fd, err := os.Open(path) //nolint:forbidigo // Page-cache bookkeeping; mmap, mincore, and fadvise use sysio gates.
	if err != nil {
		return err
	}
	defer fd.Close()
	return fadviseGated(fd.Fd(), 0, 0, sysio.FADV_DONTNEED)
}

// fadviseGated issues posix_fadvise behind the fault gate.
func fadviseGated(fd uintptr, offset, length int64, advice int) error {
	return sysio.Fadvise(injector.Load(), fd, offset, length, advice)
}

// systemMemoryBytes returns total system RAM via sysinfo(2).
func systemMemoryBytes() (int64, error) {
	return sysio.TotalMemoryBytes()
}

// mincore reports one byte per page in vec; bit 0 marks a resident page.
func mincore(b []byte, vec []byte) error {
	return sysio.Mincore(injector.Load(), b, vec)
}
