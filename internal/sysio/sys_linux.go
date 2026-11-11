//go:build linux

package sysio

import (
	"math"
	"net"
	"os"
	"syscall"
	"time"
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

// ThreadCPUTime returns the CPU time the calling OS thread has used. Time
// the thread spent preempted does not count, so a busy host does not inflate
// what it measures. The caller locks its goroutine to its thread around the
// interval it times. It returns false when the clock is unavailable.
func ThreadCPUTime() (time.Duration, bool) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_THREAD_CPUTIME_ID, &ts); err != nil {
		return 0, false
	}
	return time.Duration(ts.Nano()), true
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

// tcpInfoAppLimitedByte is the offset in struct tcp_info of the byte that
// holds tcpi_delivery_rate_app_limited, its first bitfield. unix.TCPInfo
// leaves that byte as padding after Options.
const tcpInfoAppLimitedByte = 7

// tcpInfoAppLimitedMask selects that bitfield. C compilers allocate
// bitfields from the low bit on little-endian targets and from the high bit
// on big-endian ones.
var tcpInfoAppLimitedMask = func() uint8 {
	one := uint16(1)
	if *(*uint8)(unsafe.Pointer(&one)) == 1 {
		return 0x01
	}
	return 0x80
}()

// TCPRateReader reads a TCP socket's delivery rate from TCP_INFO. Build one
// per connection and keep it: a read allocates nothing, where SyscallConn and
// GetsockoptTCPInfo allocate on every call. It is not safe for concurrent
// use.
type TCPRateReader struct {
	inj  *faults.Injector
	raw  syscall.RawConn
	read func(fd uintptr) // bound once, so Control allocates nothing
	info unix.TCPInfo
	size uint32
	ok   bool
}

// NewTCPRateReader returns a reader of conn's TCP_INFO behind the getsockopt
// gate, whose NET rate kind can also replace a reading. If conn has no file
// descriptor, every read reports no rate.
func NewTCPRateReader(inj *faults.Injector, conn *net.TCPConn) *TCPRateReader {
	r := &TCPRateReader{inj: inj}
	raw, err := conn.SyscallConn()
	if err != nil {
		return r
	}
	r.raw = raw
	r.read = func(fd uintptr) {
		r.size = unix.SizeofTCPInfo
		_, _, errno := unix.Syscall6(unix.SYS_GETSOCKOPT, fd, unix.IPPROTO_TCP, unix.TCP_INFO,
			uintptr(unsafe.Pointer(&r.info)), uintptr(unsafe.Pointer(&r.size)), 0)
		r.ok = errno == 0
	}
	return r
}

// DeliveryRate returns the socket's delivery rate, tcpi_delivery_rate in
// bytes per second, and whether that rate came from an application-limited
// sample, one taken while the sender had too little data queued to fill the
// path. It returns 0 when the kernel has no rate sample yet or the read
// fails. The kernel keeps the most recent sample that was not
// application-limited unless a limited one is higher.
func (r *TCPRateReader) DeliveryRate() (bytesPerSec uint64, appLimited bool) {
	l := r.Limits()
	return l.DeliveryRate, l.AppLimited
}

// TCPLimits is one TCP_INFO reading of what limits a socket's sending. The
// limited times are cumulative over the connection's life, so a caller
// compares two readings.
type TCPLimits struct {
	// DeliveryRate and AppLimited are as DeliveryRate returns them.
	DeliveryRate uint64
	AppLimited   bool
	// Busy is the time the socket had data to send; RwndLimited the part of
	// it the peer's receive window held back, and SndbufLimited the part the
	// send buffer did. Network-limited time is the rest.
	Busy, RwndLimited, SndbufLimited time.Duration
	// NoLimits reports a kernel whose TCP_INFO lacks the busy and limited
	// times (before Linux 4.10): they read zero, and only the delivery rate
	// is real.
	NoLimits bool
	// OK reports a successful read; every other field is zero without one.
	OK bool
}

// tcpInfoLimitsEnd is how much of struct tcp_info a kernel must fill to
// report the busy and limited times.
var tcpInfoLimitsEnd = uint32(unsafe.Offsetof(unix.TCPInfo{}.Sndbuf_limited) + unsafe.Sizeof(unix.TCPInfo{}.Sndbuf_limited))

// Limits reads TCP_INFO once and returns what limited the socket's sending.
// The NET rate kind can replace its delivery rate, as in DeliveryRate.
func (r *TCPRateReader) Limits() TCPLimits {
	if r == nil || r.raw == nil {
		return TCPLimits{}
	}
	if r.inj.Sys(faults.SysGetsockopt) != nil {
		return TCPLimits{}
	}
	r.ok = false
	if err := r.raw.Control(r.read); err != nil || !r.ok {
		return TCPLimits{}
	}
	flags := *(*uint8)(unsafe.Add(unsafe.Pointer(&r.info), tcpInfoAppLimitedByte))
	l := TCPLimits{NoLimits: r.size < tcpInfoLimitsEnd, OK: true}
	if !l.NoLimits {
		l.Busy = time.Duration(r.info.Busy_time) * time.Microsecond
		l.RwndLimited = time.Duration(r.info.Rwnd_limited) * time.Microsecond
		l.SndbufLimited = time.Duration(r.info.Sndbuf_limited) * time.Microsecond
	}
	l.DeliveryRate, l.AppLimited = r.inj.DeliveryRate(r.info.Delivery_rate, flags&tcpInfoAppLimitedMask != 0)
	return l
}

// SetTCPUserTimeout sets TCP_USER_TIMEOUT on conn: the kernel closes the
// connection once data it sent, or a keepalive probe, goes unacknowledged
// for d, or the peer's receive window stays closed for d.
func SetTCPUserTimeout(conn *net.TCPConn, d time.Duration) error {
	return setTCPInt(conn, unix.TCP_USER_TIMEOUT, int(d.Milliseconds()))
}

// SetTCPNotSentLowat sets TCP_NOTSENT_LOWAT on conn: a write waits while
// more than n bytes are queued but not yet sent, so writes return at about
// the rate bytes leave.
func SetTCPNotSentLowat(conn *net.TCPConn, n int) error {
	return setTCPInt(conn, unix.TCP_NOTSENT_LOWAT, n)
}

func setTCPInt(conn *net.TCPConn, opt, value int) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := raw.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, opt, value)
	}); err != nil {
		return err
	}
	return serr
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
