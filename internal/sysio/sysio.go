// Package sysio is where tx's raw system calls on the data path belong, so
// fault injection reaches each one. Every function that can fault takes the
// injector first and applies its gate before the real call; a nil injector
// costs one nil check. Calls the fault catalog has no kind for are plain:
// TCPByteCounts, PeekAlive, OpenDir, OpenNonblockingWriter, Close, Munmap,
// and TotalMemoryBytes. SetTCPUserTimeout and SetTCPNotSentLowat are plain
// too; their callers check the setsockopt gate. golangci-lint (make lint)
// rejects imports of syscall and golang.org/x/sys/unix outside this package,
// internal/faults, the harnesses, signal setup, and the events target writer,
// except in test files. The events writer cannot import sysio because
// faults depends on events. See docs/arch/FAULTS.md.
package sysio

import (
	"os"
	"syscall"
)

// Type aliases for code that passes sockets and errors through without
// making syscalls itself.
type (
	// Conn is syscall.Conn: a connection that exposes its descriptor.
	Conn = syscall.Conn
	// RawConn is syscall.RawConn.
	RawConn = syscall.RawConn
	// Errno is syscall.Errno.
	Errno = syscall.Errno
)

// Error numbers tx compares against.
const (
	EAGAIN      = syscall.EAGAIN
	ECONNRESET  = syscall.ECONNRESET
	EINTR       = syscall.EINTR
	EINVAL      = syscall.EINVAL
	ENOSYS      = syscall.ENOSYS
	EOPNOTSUPP  = syscall.EOPNOTSUPP
	EPIPE       = syscall.EPIPE
	EROFS       = syscall.EROFS
	EWOULDBLOCK = syscall.EWOULDBLOCK
	EXDEV       = syscall.EXDEV
)

// StatInfo is the part of a file's stat result tx uses beyond os.FileInfo:
// ownership, and identity for hardlink grouping.
type StatInfo struct {
	UID, GID uint32
	Dev, Ino uint64
	Nlink    uint64
}

// StatOf returns the StatInfo in info, or false when info carries none, as
// on a platform without a Unix stat.
func StatOf(info os.FileInfo) (StatInfo, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return StatInfo{}, false
	}
	return StatInfo{UID: st.Uid, GID: st.Gid, Dev: uint64(st.Dev), Ino: st.Ino, Nlink: uint64(st.Nlink)}, true
}
