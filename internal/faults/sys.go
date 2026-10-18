package faults

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// Syscall names a system call tx makes directly, outside Go's os and net
// packages, so the injector can delay or fail it.
type Syscall uint8

const (
	SysSplice Syscall = iota + 1
	SysTee
	SysPoll
	SysPipe
	SysFcntl
	SysDup
	SysFadvise
	SysReadahead
	SysFstat
	SysFdatasync
	SysSyncfs
	SysCopyFileRange
	SysMmap
	SysMincore
	SysSetsockopt
	sysCount
)

var syscallNames = [sysCount]string{
	SysSplice:        "splice",
	SysTee:           "tee",
	SysPoll:          "poll",
	SysPipe:          "pipe",
	SysFcntl:         "fcntl",
	SysDup:           "dup",
	SysFadvise:       "fadvise",
	SysReadahead:     "readahead",
	SysFstat:         "fstat",
	SysFdatasync:     "fdatasync",
	SysSyncfs:        "syncfs",
	SysCopyFileRange: "copy_file_range",
	SysMmap:          "mmap",
	SysMincore:       "mincore",
	SysSetsockopt:    "setsockopt",
}

func (sc Syscall) String() string { return syscallNames[sc] }

// shortSyscalls return a byte count, which the short kind truncates.
var shortSyscalls = [sysCount]bool{SysSplice: true, SysTee: true, SysCopyFileRange: true}

// syscallErrnos are the errors each syscall's fail kind chooses from: ones
// the call can really return at tx's call sites.
var syscallErrnos = [sysCount][]syscall.Errno{
	SysSplice:        {unix.EINTR, unix.EIO},
	SysTee:           {unix.EINTR, unix.EIO},
	SysPoll:          {unix.EINTR, unix.ENOMEM},
	SysPipe:          {unix.EMFILE, unix.ENFILE},
	SysFcntl:         {unix.EPERM, unix.EBUSY},
	SysDup:           {unix.EMFILE},
	SysFadvise:       {unix.EINVAL, unix.ESPIPE},
	SysReadahead:     {unix.EINVAL},
	SysFstat:         {unix.EIO},
	SysFdatasync:     {unix.EIO, unix.ENOSPC},
	SysSyncfs:        {unix.EIO},
	SysCopyFileRange: {unix.EIO, unix.EXDEV, unix.ENOSPC},
	SysMmap:          {unix.ENOMEM, unix.EAGAIN},
	SysMincore:       {unix.ENOMEM, unix.EAGAIN},
	SysSetsockopt:    {unix.ENOBUFS, unix.EINVAL},
}

// Sys is the gate in front of a syscall site. It may sleep for an injected
// latency, and returns the errno to report instead of making the call, or
// nil to make it. A nil injector returns nil at once.
func (i *Injector) Sys(sc Syscall) error {
	if i == nil {
		return nil
	}
	_, err := i.sys(sc)
	return err
}

// sys is Sys for a non-nil injector, also returning the call's operation
// number for the short kind to use.
func (i *Injector) sys(sc Syscall) (uint64, error) {
	i.counts.sys[sc].Add(1)
	if !i.famOn[FamilySys] {
		return 0, nil
	}
	op := i.sysOps[sc].Add(1)
	key := uint64(sc)
	if k := syscallKind(sc, sysLatency); i.fires(k, key, op) {
		_ = sleep(nil, i.sample(k, key, op), noDeadline, nil)
	}
	if k := syscallKind(sc, sysFail); i.fires(k, key, op) {
		errs := syscallErrnos[sc]
		return op, errs[i.rng(k, key, op).IntN(len(errs))]
	}
	return op, nil
}

// short returns how many of n bytes a short-capable syscall should ask
// for on operation op: n, or fewer when its short kind fires.
func (i *Injector) short(sc Syscall, op uint64, n int) int {
	if n <= 1 || !i.famOn[FamilySys] {
		return n
	}
	k := syscallKind(sc, sysShort)
	if !i.fires(k, uint64(sc), op) {
		return n
	}
	return 1 + i.rng(k, uint64(sc), op).IntN(n-1)
}

// SpliceFunc has unix.Splice's signature.
type SpliceFunc = func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int64, error)

// TeeFunc has unix.Tee's signature.
type TeeFunc = func(rfd int, wfd int, n int, flags int) (int64, error)

// PollFunc has unix.Poll's signature.
type PollFunc = func(fds []unix.PollFd, timeout int) (int, error)

// CopyFileRangeFunc has unix.CopyFileRange's signature.
type CopyFileRangeFunc = func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int, error)

// WrapSplice puts the splice gate in front of f, which may also ask for fewer
// bytes. A nil injector returns f.
func (i *Injector) WrapSplice(f SpliceFunc) SpliceFunc {
	if i == nil {
		return f
	}
	return func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int64, error) {
		op, err := i.sys(SysSplice)
		if err != nil {
			return 0, err
		}
		return f(rfd, roff, wfd, woff, i.short(SysSplice, op, n), flags)
	}
}

// WrapTee puts the tee gate in front of f, which may also ask for fewer bytes.
// A nil injector returns f.
func (i *Injector) WrapTee(f TeeFunc) TeeFunc {
	if i == nil {
		return f
	}
	return func(rfd int, wfd int, n int, flags int) (int64, error) {
		op, err := i.sys(SysTee)
		if err != nil {
			return 0, err
		}
		return f(rfd, wfd, i.short(SysTee, op, n), flags)
	}
}

// WrapPoll puts the poll gate in front of f. A nil injector returns f.
func (i *Injector) WrapPoll(f PollFunc) PollFunc {
	if i == nil {
		return f
	}
	return func(fds []unix.PollFd, timeout int) (int, error) {
		if err := i.Sys(SysPoll); err != nil {
			return 0, err
		}
		return f(fds, timeout)
	}
}

// WrapCopyFileRange puts the copy_file_range gate in front of f, which may
// also ask for fewer bytes. A nil injector returns f.
func (i *Injector) WrapCopyFileRange(f CopyFileRangeFunc) CopyFileRangeFunc {
	if i == nil {
		return f
	}
	return func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int, error) {
		op, err := i.sys(SysCopyFileRange)
		if err != nil {
			return 0, err
		}
		return f(rfd, roff, wfd, woff, i.short(SysCopyFileRange, op, n), flags)
	}
}
