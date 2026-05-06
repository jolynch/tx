//go:build linux

package pagecache

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mincoreChunkPages bounds the mincore call size so the returned vector
// stays small on huge files (1M pages = 4 GiB at 4 KiB pages).
const mincoreChunkPages = 1 << 20

var (
	loadResidencyFn = loadResidency
	touchPagesFn    = touchPages
)

func loadResidency(path string) ([]byte, int, error) {
	fd, err := os.Open(path)
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

	addr, err := unix.Mmap(int(fd.Fd()), 0, mappedBytes, unix.PROT_NONE, unix.MAP_SHARED)
	if err != nil {
		return nil, 0, err
	}
	defer unix.Munmap(addr)

	vec := make([]byte, numPages)
	for offset := 0; offset < numPages; offset += mincoreChunkPages {
		end := offset + mincoreChunkPages
		if end > numPages {
			end = numPages
		}
		chunkBytes := (end - offset) * pageSize
		chunk := addr[offset*pageSize : offset*pageSize+chunkBytes]
		if mErr := mincore(chunk, vec[offset:end]); mErr != nil {
			return nil, 0, mErr
		}
	}
	return vec, numPages, nil
}

func touchPages(path string, bits []byte, numPages int) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		return err
	}
	pageSize := int64(os.Getpagesize())
	actualPages := int((info.Size() + pageSize - 1) / pageSize)
	if actualPages < numPages {
		numPages = actualPages
	}

	const batchPages = 8 // 32 KiB at 4 KiB pages — happycache uses this batch size

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
			_ = unix.Fadvise(int(fd.Fd()), offset, length, unix.FADV_WILLNEED)
		}
	}
	return nil
}

// mincore wraps the SYS_MINCORE syscall. It returns one byte per page in
// vec; bit 0 of each byte is set when the page is resident in the cache.
// golang.org/x/sys/unix does not export Mincore, so we invoke it directly.
func mincore(b []byte, vec []byte) error {
	if len(b) == 0 {
		return nil
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		uintptr(unsafe.Pointer(&vec[0])),
	)
	if errno != 0 {
		return errno
	}
	return nil
}
