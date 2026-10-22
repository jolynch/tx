//go:build linux && (amd64 || arm64)

package sysio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/faults"
)

// seccompEnv names the case a child process of TestGatesSkipRealCall runs,
// as "<case>:<fail|nil>".
const seccompEnv = "SYSIO_SECCOMP_CHILD"

// childTimeout bounds each child process.
const childTimeout = 5 * time.Second

// Child exit codes other than 0, which means the gated call returned.
const (
	childNoSeccomp = 3 // seccomp is unavailable here
	childSetup     = 4 // the child could not prepare its call
	childNoError   = 5 // the gate was set to fail, and the call returned nil
)

// seccompCase is one gated call and the syscall numbers its real call may
// use. setup runs before the filter goes on, and returns the call.
type seccompCase struct {
	nrs   []uintptr
	sc    faults.Syscall
	setup func() (func(*faults.Injector) error, error)
}

// onMemfd sets up a call on a fresh memfd.
func onMemfd(call func(inj *faults.Injector, fd int) error) func() (func(*faults.Injector) error, error) {
	return func() (func(*faults.Injector) error, error) {
		fd, err := unix.MemfdCreate("seccomp", unix.MFD_CLOEXEC)
		if err != nil {
			return nil, err
		}
		if _, err := unix.Write(fd, []byte("data")); err != nil {
			return nil, err
		}
		return func(inj *faults.Injector) error { return call(inj, fd) }, nil
	}
}

var seccompCases = map[string]seccompCase{
	"fdatasync": {[]uintptr{unix.SYS_FDATASYNC}, faults.SysFdatasync, onMemfd(func(inj *faults.Injector, fd int) error {
		return Fdatasync(inj, fd)
	})},
	"syncfs": {[]uintptr{unix.SYS_SYNCFS}, faults.SysSyncfs, onMemfd(func(inj *faults.Injector, fd int) error {
		return Syncfs(inj, fd)
	})},
	"fadvise": {[]uintptr{unix.SYS_FADVISE64}, faults.SysFadvise, onMemfd(func(inj *faults.Injector, fd int) error {
		return Fadvise(inj, uintptr(fd), 0, 0, FADV_SEQUENTIAL)
	})},
	"readahead": {[]uintptr{unix.SYS_READAHEAD}, faults.SysReadahead, onMemfd(func(inj *faults.Injector, fd int) error {
		return Readahead(inj, uintptr(fd), 0, 4)
	})},
	"fstat": {[]uintptr{unix.SYS_FSTAT, unix.SYS_NEWFSTATAT, unix.SYS_STATX}, faults.SysFstat, onMemfd(func(inj *faults.Injector, fd int) error {
		_, _, err := FileID(inj, fd)
		return err
	})},
	"dup": {[]uintptr{unix.SYS_DUP, unix.SYS_DUP3}, faults.SysDup, onMemfd(func(inj *faults.Injector, fd int) error {
		_, err := Dup(inj, fd)
		return err
	})},
	"pipe": {[]uintptr{unix.SYS_PIPE2}, faults.SysPipe, func() (func(*faults.Injector) error, error) {
		return func(inj *faults.Injector) error { _, _, err := Pipe(inj); return err }, nil
	}},
	"dup conn": {[]uintptr{unix.SYS_DUP, unix.SYS_DUP3}, faults.SysDup, func() (func(*faults.Injector) error, error) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		go func() { _, _ = ln.Accept() }()
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return nil, err
		}
		return func(inj *faults.Injector) error { _, err := DupConn(inj, c.(*net.TCPConn)); return err }, nil
	}},
}

// TestGatesSkipRealCall checks a failed gate stops the real call itself, not
// only its result, for the calls whose effect a test cannot otherwise see.
// Each case runs in a child process that installs a seccomp filter killing
// the process on the call's syscalls, then makes the call with its gate's
// fail kind at rate 1: the child must exit normally. The same child with a
// nil injector must be killed, which shows the filter catches the call.
func TestGatesSkipRealCall(t *testing.T) {
	if spec := os.Getenv(seccompEnv); spec != "" {
		// exit_group directly: os.Exit runs exit hooks, such as a coverage
		// build's, whose own syscalls the filter could catch.
		code := seccompChild(spec)
		_, _, _ = unix.RawSyscall(unix.SYS_EXIT_GROUP, uintptr(code), 0, 0)
	}
	for name := range seccompCases {
		t.Run(name, func(t *testing.T) {
			for _, mode := range []string{"fail", "nil"} {
				ctx, cancel := context.WithTimeout(context.Background(), childTimeout)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGatesSkipRealCall$")
				cmd.WaitDelay = time.Second
				// A race-enabled child otherwise sleeps 1s before it exits.
				gorace := strings.TrimSpace(os.Getenv("GORACE") + " atexit_sleep_ms=0")
				cmd.Env = append(os.Environ(), seccompEnv+"="+name+":"+mode, "GORACE="+gorace)
				out, err := cmd.CombinedOutput()
				if ctx.Err() != nil {
					// Before Linux 4.14, SECCOMP_RET_KILL_PROCESS kills only the
					// calling thread, which can leave the child hung.
					t.Fatalf("%s child (%s) did not exit within %v; killed it\n%s", name, mode, childTimeout, out)
				}
				var exit *exec.ExitError
				killed := errors.As(err, &exit) && exit.Sys().(syscall.WaitStatus).Signal() == syscall.SIGSYS
				switch {
				case exit != nil && exit.ExitCode() == childNoSeccomp:
					t.Skipf("seccomp unavailable: %s", out)
				case mode == "fail" && killed:
					t.Fatalf("%s made its real call although its gate failed", name)
				case mode == "fail" && err != nil:
					t.Fatalf("child with a failing gate: %v\n%s", err, out)
				case mode == "nil" && !killed:
					t.Fatalf("with a nil injector the filter did not catch %s's real call (%v)\n%s", name, err, out)
				}
			}
		})
	}
}

// seccompChild runs one case of TestGatesSkipRealCall and returns its exit
// code. It never returns if the filter kills it.
func seccompChild(spec string) int {
	name, mode, _ := strings.Cut(spec, ":")
	c, ok := seccompCases[name]
	if !ok {
		return childSetup
	}
	call, err := c.setup()
	if err != nil {
		os.Stderr.WriteString("setup: " + err.Error() + "\n")
		return childSetup
	}
	var inj *faults.Injector
	if mode == "fail" {
		inj = failing(c.sc)
	}
	if err := killOn(c.nrs); err != nil {
		os.Stderr.WriteString("seccomp: " + err.Error() + "\n")
		return childNoSeccomp
	}
	if err := call(inj); err == nil && mode == "fail" {
		return childNoError
	}
	return 0
}

// killOn installs a seccomp filter, on every thread, that kills the process
// when it makes any of the syscalls nrs. The process is made non-dumpable
// first, so the kill leaves no core file.
func killOn(nrs []uintptr) error {
	runtime.LockOSThread() // no_new_privs and the filter go on the same thread
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	// Load seccomp_data.nr, at offset 0, and compare it with each number.
	prog := []unix.SockFilter{{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0}}
	for _, nr := range nrs {
		prog = append(prog,
			unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jt: 0, Jf: 1, K: uint32(nr)},
			unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS})
	}
	prog = append(prog, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	fprog := unix.SockFprog{Len: uint16(len(prog)), Filter: &prog[0]}
	tid, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&fprog)))
	if errno != 0 {
		return errno
	}
	// With TSYNC, a thread the filter could not reach comes back as its
	// thread ID, with errno 0, and the filter is installed on no thread.
	if tid != 0 {
		return fmt.Errorf("TSYNC could not reach thread %d", tid)
	}
	return nil
}
