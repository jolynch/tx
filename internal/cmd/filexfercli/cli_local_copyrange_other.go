//go:build !linux

package filexfercli

import "os"

// localCopyFile on non-Linux platforms has no copy_file_range; it always uses
// the buffered userspace copy.
func localCopyFile(dst, src *os.File, _ int64, onBytes func(int64)) (int64, error) {
	return localCopyFileBuffered(dst, src, onBytes)
}

// localFileOwner cannot portably extract uid/gid off Linux; ownership
// preservation is skipped.
func localFileOwner(_ os.FileInfo) (uid, gid int, ok bool) {
	return 0, 0, false
}
