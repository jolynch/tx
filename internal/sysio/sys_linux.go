//go:build linux

package sysio

import (
	"math"
	"net"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/faults"
)

// Flags and types the zero-copy and page-cache callers pass through.
const (
	O_DIRECT        = unix.O_DIRECT
	SPLICE_F_MOVE   = unix.SPLICE_F_MOVE
	POLLOUT         = unix.POLLOUT
	POLLERR         = unix.POLLERR
	POLLHUP         = unix.POLLHUP
	POLLNVAL        = unix.POLLNVAL
	FADV_SEQUENTIAL = unix.FADV_SEQUENTIAL
	FADV_WILLNEED   = unix.FADV_WILLNEED
	FADV_DONTNEED   = unix.FADV_DONTNEED
	PROT_NONE       = unix.PROT_NONE
	PROT_READ       = unix.PROT_READ
	MAP_SHARED      = unix.MAP_SHARED
)

// PollFd is unix.PollFd.
type PollFd = unix.PollFd

// ZeroCopyCalls returns splice, tee, and poll behind inj's gates, which can
// fail, delay, or shorten each call; with a nil injector they are the kernel
// calls themselves.
func ZeroCopyCalls(inj *faults.Injector) (faults.SpliceFunc, faults.TeeFunc, faults.PollFunc) {
	return inj.WrapSplice(unix.Splice), inj.WrapTee(unix.Tee), inj.WrapPoll(unix.Poll)
}

// Pipe returns a pipe, behind the pipe gate.
func Pipe(inj *faults.Injector) (r, w *os.File, err error) {
	if err := inj.Sys(faults.SysPipe); err != nil {
		return nil, nil, err
	}
	return os.Pipe()
}

// SetPipeSize asks the kernel for a pipe buffer of size bytes, behind the
// fcntl gate. It is best effort: a failure, real or injected, leaves the
// pipe as it was.
func SetPipeSize(inj *faults.Injector, f *os.File, size int) {
	if size <= 0 || inj.Sys(faults.SysFcntl) != nil {
		return
	}
	_, _ = unix.FcntlInt(f.Fd(), unix.F_SETPIPE_SZ, size)
}

// DupConn returns a new descriptor for conn's socket, behind the dup gate.
// The caller closes it with Close.
func DupConn(inj *faults.Injector, conn *net.TCPConn) (int, error) {
	if err := inj.Sys(faults.SysDup); err != nil {
		return -1, err
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, err
	}
	dupFD := -1
	var dupErr error
	if err := raw.Control(func(fd uintptr) {
		dupFD, dupErr = unix.Dup(int(fd))
	}); err != nil {
		return -1, err
	}
	if dupErr != nil {
		return -1, dupErr
	}
	return dupFD, nil
}

// Dup returns a new descriptor for fd, behind the dup gate.
func Dup(inj *faults.Injector, fd int) (int, error) {
	if err := inj.Sys(faults.SysDup); err != nil {
		return -1, err
	}
	return unix.Dup(fd)
}

// Close closes a raw descriptor from Dup, DupConn, or OpenDir.
func Close(fd int) error {
	return unix.Close(fd)
}

// Fadvise gives the kernel advice about fd's range, behind the fadvise
// gate.
func Fadvise(inj *faults.Injector, fd uintptr, offset, length int64, advice int) error {
	if err := inj.Sys(faults.SysFadvise); err != nil {
		return err
	}
	return unix.Fadvise(int(fd), offset, length, advice)
}

// Readahead asks the kernel to read fd's range into the page cache, behind
// the readahead gate.
func Readahead(inj *faults.Injector, fd uintptr, offset, length int64) error {
	if err := inj.Sys(faults.SysReadahead); err != nil {
		return err
	}
	if _, _, errno := unix.Syscall(unix.SYS_READAHEAD, fd, uintptr(offset), uintptr(length)); errno != 0 {
		return errno
	}
	return nil
}

// FileID returns the device and inode of fd, behind the fstat gate.
func FileID(inj *faults.Injector, fd int) (dev, ino uint64, err error) {
	if err := inj.Sys(faults.SysFstat); err != nil {
		return 0, 0, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return 0, 0, err
	}
	return uint64(st.Dev), st.Ino, nil
}

// Fdatasync flushes fd's data to stable storage, behind the fdatasync gate.
func Fdatasync(inj *faults.Injector, fd int) error {
	if err := inj.Sys(faults.SysFdatasync); err != nil {
		return err
	}
	return unix.Fdatasync(fd)
}

// OpenDir opens a directory read-only for Syncfs. The caller closes it with
// Close.
func OpenDir(path string) (int, error) {
	return unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
}

// Syncfs flushes the filesystem holding fd, behind the syncfs gate.
func Syncfs(inj *faults.Injector, fd int) error {
	if err := inj.Sys(faults.SysSyncfs); err != nil {
		return err
	}
	return unix.Syncfs(fd)
}

// TotalMemoryBytes returns the system's RAM, from sysinfo(2).
func TotalMemoryBytes() (int64, error) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, err
	}
	total := uint64(info.Totalram) * uint64(info.Unit)
	if total > math.MaxInt64 {
		return math.MaxInt64, nil
	}
	return int64(total), nil
}

// Mmap maps length bytes of fd from offset, behind the mmap gate. Release
// the mapping with Munmap.
func Mmap(inj *faults.Injector, fd uintptr, offset int64, length, prot, flags int) ([]byte, error) {
	if err := inj.Sys(faults.SysMmap); err != nil {
		return nil, err
	}
	return unix.Mmap(int(fd), offset, length, prot, flags)
}

// Munmap releases a mapping from Mmap.
func Munmap(b []byte) error {
	return unix.Munmap(b)
}

// Mincore fills vec with one byte per page of the mapping b, bit 0 set when
// the page is resident, behind the mincore gate.
func Mincore(inj *faults.Injector, b, vec []byte) error {
	if len(b) == 0 {
		return nil
	}
	// The kernel writes one byte per page of b, so a short vec would be
	// overrun.
	page := os.Getpagesize()
	if len(vec) < (len(b)+page-1)/page {
		return EINVAL
	}
	if err := inj.Sys(faults.SysMincore); err != nil {
		return err
	}
	// golang.org/x/sys/unix does not export mincore, so call it directly.
	if _, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		return errno
	}
	return nil
}

// CopyFileRange copies up to n bytes from src to dst at their offsets, in
// the kernel, behind the copy_file_range gate, which can also shorten n.
func CopyFileRange(inj *faults.Injector, dst, src *os.File, n int) (int, error) {
	return inj.WrapCopyFileRange(unix.CopyFileRange)(int(src.Fd()), nil, int(dst.Fd()), nil, n, 0)
}

// TCPByteCounts returns the bytes conn has received and sent, from
// TCP_INFO, or zeros when the kernel does not report them.
func TCPByteCounts(conn *net.TCPConn) (received, sent int64) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, 0
	}
	_ = raw.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err == nil {
			received, sent = int64(info.Bytes_received), int64(info.Bytes_sent)
		}
	})
	return received, sent
}

// PeekAlive reports whether raw's socket is still open and holds no unread
// bytes: a nonblocking one-byte MSG_PEEK finds nothing to read rather than
// end of file or data. It never blocks. err is raw.Read's own error, when it
// could not reach the descriptor at all; alive is then false.
func PeekAlive(raw RawConn) (alive bool, err error) {
	// One byte, enough to tell data from EOF; this runs on every pooled
	// connection borrow.
	var peek [1]byte
	err = raw.Read(func(fd uintptr) bool {
		_, _, recvErr := unix.Recvfrom(int(fd), peek[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
		alive = recvErr == unix.EAGAIN || recvErr == unix.EWOULDBLOCK
		return true // never block waiting for readability
	})
	if err != nil {
		return false, err
	}
	return alive, nil
}

// OpenNonblockingWriter opens path for writing without blocking, creating
// it if needed, truncated or for appending, and close-on-exec. Opening a
// FIFO with no reader fails at once rather than waiting for one.
func OpenNonblockingWriter(path string, truncate bool) (*os.File, error) {
	flags := unix.O_WRONLY | unix.O_CREAT | unix.O_NONBLOCK | unix.O_CLOEXEC
	if truncate {
		flags |= unix.O_TRUNC
	} else {
		flags |= unix.O_APPEND
	}
	fd, err := unix.Open(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}
