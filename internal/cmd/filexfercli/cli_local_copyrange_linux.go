//go:build linux

package filexfercli

import (
	"errors"
	"os"

	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/sysio"
)

// localCopyFile copies size bytes from src to dst using copy_file_range(2),
// which moves data entirely in the kernel (and becomes a metadata-only reflink
// on copy-on-write filesystems such as btrfs/XFS). It falls back to a buffered
// userspace copy when the kernel or filesystem does not support copy_file_range
// for this pair: old kernels (ENOSYS), unsupported filesystems (EOPNOTSUPP),
// cross-device on kernels < 5.3 (EXDEV), or pseudo-files like /dev/null
// (EINVAL). The fallback only triggers before any bytes are copied; a mid-copy
// error on a single fd pair is a genuine I/O error and propagates.
func localCopyFile(inj *faults.Injector, dst, src *os.File, size int64, onBytes func(int64)) (int64, error) {
	var copied int64
	for copied < size {
		n, err := sysio.CopyFileRange(inj, dst, src, int(size-copied))
		if err != nil {
			if errors.Is(err, sysio.EINTR) {
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
	return errors.Is(err, sysio.ENOSYS) ||
		errors.Is(err, sysio.EOPNOTSUPP) ||
		errors.Is(err, sysio.EXDEV) ||
		errors.Is(err, sysio.EINVAL)
}

// localFileOwner extracts the numeric uid/gid from a file's stat result.
func localFileOwner(info os.FileInfo) (uid, gid int, ok bool) {
	if st, ok := sysio.StatOf(info); ok {
		return int(st.UID), int(st.GID), true
	}
	return 0, 0, false
}
