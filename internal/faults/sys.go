package faults

import "golang.org/x/sys/unix"

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

// Sys is the gate in front of a syscall site: it returns the error to report
// instead of making the call, or nil to make it. No syscall fault kinds exist
// yet, so it always returns nil.
func (i *Injector) Sys(sc Syscall) error {
	if i == nil {
		return nil
	}
	i.counts.sys[sc].Add(1)
	return nil
}

// SpliceFunc has unix.Splice's signature.
type SpliceFunc = func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int64, error)

// TeeFunc has unix.Tee's signature.
type TeeFunc = func(rfd int, wfd int, n int, flags int) (int64, error)

// PollFunc has unix.Poll's signature.
type PollFunc = func(fds []unix.PollFd, timeout int) (int, error)

// CopyFileRangeFunc has unix.CopyFileRange's signature.
type CopyFileRangeFunc = func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int, error)

// WrapSplice puts the splice gate in front of f. A nil injector returns f.
func (i *Injector) WrapSplice(f SpliceFunc) SpliceFunc {
	if i == nil {
		return f
	}
	return func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int64, error) {
		if err := i.Sys(SysSplice); err != nil {
			return 0, err
		}
		return f(rfd, roff, wfd, woff, n, flags)
	}
}

// WrapTee puts the tee gate in front of f. A nil injector returns f.
func (i *Injector) WrapTee(f TeeFunc) TeeFunc {
	if i == nil {
		return f
	}
	return func(rfd int, wfd int, n int, flags int) (int64, error) {
		if err := i.Sys(SysTee); err != nil {
			return 0, err
		}
		return f(rfd, wfd, n, flags)
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

// WrapCopyFileRange puts the copy_file_range gate in front of f. A nil
// injector returns f.
func (i *Injector) WrapCopyFileRange(f CopyFileRangeFunc) CopyFileRangeFunc {
	if i == nil {
		return f
	}
	return func(rfd int, roff *int64, wfd int, woff *int64, n int, flags int) (int, error) {
		if err := i.Sys(SysCopyFileRange); err != nil {
			return 0, err
		}
		return f(rfd, roff, wfd, woff, n, flags)
	}
}
