//go:build linux

package filexfercli

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// localCopyFile copies size bytes from src to dst using copy_file_range(2),
// which moves data entirely in the kernel (and becomes a metadata-only reflink
// on copy-on-write filesystems such as btrfs/XFS). It falls back to a buffered
// userspace copy when the kernel or filesystem does not support copy_file_range
// for this pair: old kernels (ENOSYS), unsupported filesystems (EOPNOTSUPP),
// cross-device on kernels < 5.3 (EXDEV), or pseudo-files like /dev/null
// (EINVAL). The fallback only triggers before any bytes are copied; a mid-copy
// error on a single fd pair is a genuine I/O error and propagates.
func localCopyFile(dst, src *os.File, size int64, onBytes func(int64)) (int64, error) {
	var copied int64
	for copied < size {
		n, err := unix.CopyFileRange(int(src.Fd()), nil, int(dst.Fd()), nil, int(size-copied), 0)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if copied == 0 && isCopyFileRangeUnsupported(err) {
				return localCopyFileBuffered(dst, src, onBytes)
			}
			return copied, err
		}
		if n == 0 {
			break // EOF: source shorter than its stat size
		}
		copied += int64(n)
		if onBytes != nil {
			onBytes(int64(n))
		}
	}
	return copied, nil
}

func isCopyFileRangeUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.EXDEV) ||
		errors.Is(err, unix.EINVAL)
}

// localFileOwner extracts the numeric uid/gid from a file's stat result.
func localFileOwner(info os.FileInfo) (uid, gid int, ok bool) {
	if st, o := info.Sys().(*syscall.Stat_t); o {
		return int(st.Uid), int(st.Gid), true
	}
	return 0, 0, false
}
