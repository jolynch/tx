//go:build linux

package sysio

import (
	"bytes"
	"errors"
	"io"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/faults"
)

// callTimeout bounds every call that must not block.
const callTimeout = 2 * time.Second

// pattern returns n bytes that differ at every offset in a 251-byte cycle,
// so a copy from the wrong offset shows up.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// memfd returns an in-memory file holding data, with its offset at 0.
func memfd(t *testing.T, data []byte) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate(t.Name(), unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("MemfdCreate: %v", err)
	}
	f := os.NewFile(uintptr(fd), "memfd")
	t.Cleanup(func() { f.Close() })
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	return f
}

// rawPipe returns a pipe's read and write descriptors, closed at cleanup.
func rawPipe(t *testing.T) (r, w int) {
	t.Helper()
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatalf("Pipe2: %v", err)
	}
	t.Cleanup(func() { unix.Close(p[0]); unix.Close(p[1]) })
	return p[0], p[1]
}

// within runs fn and fails the test if it does not return in time.
func within(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(callTimeout):
		t.Fatalf("%s blocked for %v", what, callTimeout)
	}
}

// openFDs returns how many descriptors the process has open.
func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// mappings returns how many mappings of the test's memfd the process holds.
func mappings(t *testing.T) int {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Count(maps, []byte("/memfd:"+t.Name()+" "))
}

// failing returns an injector whose fail kind for sc fires on every call.
func failing(sc faults.Syscall) *faults.Injector {
	return faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: sc.String() + ".fail=1"}})
}

// injectedErrnos returns the errnos sc's fail kind chooses from, by asking
// the gate itself, so the test follows the catalog.
func injectedErrnos(t *testing.T, sc faults.Syscall) []syscall.Errno {
	t.Helper()
	inj := failing(sc)
	var set []syscall.Errno
	for range 64 {
		var errno syscall.Errno
		if !errors.As(inj.Sys(sc), &errno) {
			t.Fatalf("%s.fail=1 did not fail its gate", sc)
		}
		if !slices.Contains(set, errno) {
			set = append(set, errno)
		}
	}
	return set
}

// checkInjected fails unless err is one of sc's injected errnos and inj's
// gate for sc was asked exactly once.
func checkInjected(t *testing.T, inj *faults.Injector, sc faults.Syscall, err error) {
	t.Helper()
	var errno syscall.Errno
	if want := injectedErrnos(t, sc); !errors.As(err, &errno) || !slices.Contains(want, errno) {
		t.Fatalf("%s.fail=1 returned %v, want one of %v", sc, err, want)
	}
	if n := inj.Counts().Sys[sc]; n != 1 {
		t.Fatalf("%s gate asked %d times, want 1", sc, n)
	}
}

// checkGated fails unless inj's gate for sc was asked want times. A nil
// injector counts nothing, so it checks nothing.
func checkGated(t *testing.T, inj *faults.Injector, sc faults.Syscall, want uint64) {
	t.Helper()
	if inj == nil {
		return
	}
	if n := inj.Counts().Sys[sc]; n != want {
		t.Fatalf("%s gate asked %d times, want %d", sc, n, want)
	}
}

// TestCallsWork checks every gated function makes its real call correctly,
// both with a nil injector and with an injector that has no fault kinds; the
// second must ask each call's gate exactly once per call, which is how the
// per-site fault tests see a call site.
func TestCallsWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		inj  func() *faults.Injector
	}{
		{"nil", func() *faults.Injector { return nil }},
		{"empty", func() *faults.Injector { return faults.New(faults.Config{Seed: 1}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("ZeroCopyCalls", func(t *testing.T) { testZeroCopyCalls(t, tc.inj()) })
			t.Run("Pipe", func(t *testing.T) { testPipe(t, tc.inj()) })
			t.Run("SetPipeSize", func(t *testing.T) { testSetPipeSize(t, tc.inj()) })
			t.Run("DupClose", func(t *testing.T) { testDupClose(t, tc.inj()) })
			t.Run("DupConn", func(t *testing.T) { testDupConn(t, tc.inj()) })
			t.Run("Hints", func(t *testing.T) { testHints(t, tc.inj()) })
			t.Run("FileID", func(t *testing.T) { testFileID(t, tc.inj()) })
			t.Run("Durability", func(t *testing.T) { testDurability(t, tc.inj()) })
			t.Run("MmapMincore", func(t *testing.T) { testMmapMincore(t, tc.inj()) })
			t.Run("CopyFileRange", func(t *testing.T) { testCopyFileRange(t, tc.inj()) })
		})
	}
}

// TestGatesFail checks every gate runs before its real call: with the gate's
// fail kind at rate 1, each function returns one of that syscall's injected
// errnos and the call has no effect: no bytes move, no descriptor or mapping
// is left open. Calls whose effect a test cannot see get an invalid
// descriptor, so a real call made first and returning its own error would
// return EBADF; TestGatesSkipRealCall catches one whose error is dropped.
func TestGatesFail(t *testing.T) {
	const badFD = -1
	t.Run("splice", func(t *testing.T) {
		inj := failing(faults.SysSplice)
		splice, _, _ := ZeroCopyCalls(inj)
		src := memfd(t, pattern(100))
		r, w := rawPipe(t)
		roff := int64(0)
		n, err := splice(int(src.Fd()), &roff, w, nil, 100, 0)
		checkInjected(t, inj, faults.SysSplice, err)
		if n != 0 || roff != 0 || buffered(t, r) != 0 {
			t.Fatalf("failed splice moved bytes: n=%d, offset %d, %d bytes in the pipe", n, roff, buffered(t, r))
		}
	})
	t.Run("tee", func(t *testing.T) {
		inj := failing(faults.SysTee)
		_, tee, _ := ZeroCopyCalls(inj)
		r1, w1 := rawPipe(t)
		r2, w2 := rawPipe(t)
		if _, err := unix.Write(w1, pattern(100)); err != nil {
			t.Fatal(err)
		}
		n, err := tee(r1, w2, 100, 0)
		checkInjected(t, inj, faults.SysTee, err)
		if n != 0 || buffered(t, r2) != 0 {
			t.Fatalf("failed tee copied bytes: n=%d, %d bytes in the target", n, buffered(t, r2))
		}
	})
	t.Run("poll", func(t *testing.T) {
		inj := failing(faults.SysPoll)
		_, _, poll := ZeroCopyCalls(inj)
		_, w := rawPipe(t)
		fds := []PollFd{{Fd: int32(w), Events: POLLOUT}}
		n, err := poll(fds, 0)
		checkInjected(t, inj, faults.SysPoll, err)
		if n != 0 || fds[0].Revents != 0 {
			t.Fatalf("failed poll reported %d ready, revents %#x", n, fds[0].Revents)
		}
	})
	t.Run("pipe", func(t *testing.T) {
		inj := failing(faults.SysPipe)
		before := openFDs(t)
		r, w, err := Pipe(inj)
		checkInjected(t, inj, faults.SysPipe, err)
		if r != nil || w != nil || openFDs(t) != before {
			t.Fatalf("failed Pipe returned files %v, %v; %d descriptors open, want %d", r, w, openFDs(t), before)
		}
	})
	t.Run("fcntl", func(t *testing.T) {
		inj := failing(faults.SysFcntl)
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		defer w.Close()
		before := pipeSize(t, w)
		// Without a real F_SETPIPE_SZ that can grow the pipe, an unchanged
		// size would not show the call was skipped.
		requirePipeGrowth(t, 4*before)
		SetPipeSize(inj, w, 4*before)
		if got := pipeSize(t, w); got != before {
			t.Fatalf("pipe size %d after a failed SetPipeSize, want %d", got, before)
		}
		if n := inj.Counts().Sys[faults.SysFcntl]; n != 1 {
			t.Fatalf("fcntl gate asked %d times, want 1", n)
		}
	})
	t.Run("dup", func(t *testing.T) {
		inj := failing(faults.SysDup)
		f := memfd(t, nil)
		before := openFDs(t)
		fd, err := Dup(inj, int(f.Fd()))
		checkInjected(t, inj, faults.SysDup, err)
		if fd != -1 || openFDs(t) != before {
			t.Fatalf("failed Dup returned descriptor %d; %d descriptors open, want -1 and %d", fd, openFDs(t), before)
		}
	})
	t.Run("dup conn", func(t *testing.T) {
		inj := failing(faults.SysDup)
		client, _ := tcpPair(t)
		before := openFDs(t)
		fd, err := DupConn(inj, client)
		checkInjected(t, inj, faults.SysDup, err)
		if fd != -1 || openFDs(t) != before {
			t.Fatalf("failed DupConn returned descriptor %d; %d descriptors open, want -1 and %d", fd, openFDs(t), before)
		}
	})
	for sc, call := range map[faults.Syscall]func(*faults.Injector) error{
		faults.SysFadvise:   func(inj *faults.Injector) error { return Fadvise(inj, ^uintptr(0), 0, 0, FADV_SEQUENTIAL) },
		faults.SysReadahead: func(inj *faults.Injector) error { return Readahead(inj, ^uintptr(0), 0, 4096) },
		faults.SysFdatasync: func(inj *faults.Injector) error { return Fdatasync(inj, badFD) },
		faults.SysSyncfs:    func(inj *faults.Injector) error { return Syncfs(inj, badFD) },
	} {
		t.Run(sc.String(), func(t *testing.T) {
			inj := failing(sc)
			checkInjected(t, inj, sc, call(inj))
		})
	}
	t.Run("fstat", func(t *testing.T) {
		inj := failing(faults.SysFstat)
		dev, ino, err := FileID(inj, badFD)
		checkInjected(t, inj, faults.SysFstat, err)
		if dev != 0 || ino != 0 {
			t.Fatalf("failed FileID returned %d, %d", dev, ino)
		}
	})
	t.Run("mmap", func(t *testing.T) {
		inj := failing(faults.SysMmap)
		f := memfd(t, pattern(os.Getpagesize()))
		b, err := Mmap(inj, f.Fd(), 0, os.Getpagesize(), PROT_READ, MAP_SHARED)
		checkInjected(t, inj, faults.SysMmap, err)
		if b != nil || mappings(t) != 0 {
			t.Fatalf("failed Mmap returned a %d-byte mapping; the file has %d mappings", len(b), mappings(t))
		}
	})
	t.Run("mincore", func(t *testing.T) {
		inj := failing(faults.SysMincore)
		ps := os.Getpagesize()
		f := memfd(t, pattern(2*ps))
		b, err := Mmap(nil, f.Fd(), 0, 2*ps, PROT_READ, MAP_SHARED)
		if err != nil {
			t.Fatalf("Mmap: %v", err)
		}
		defer Munmap(b)
		vec := []byte{0xaa, 0xaa}
		checkInjected(t, inj, faults.SysMincore, Mincore(inj, b, vec))
		if vec[0] != 0xaa || vec[1] != 0xaa {
			t.Fatalf("failed Mincore wrote %v", vec)
		}
	})
	t.Run("copy_file_range", func(t *testing.T) {
		inj := failing(faults.SysCopyFileRange)
		src, dst := memfd(t, pattern(100)), memfd(t, nil)
		n, err := CopyFileRange(inj, dst, src, 100)
		checkInjected(t, inj, faults.SysCopyFileRange, err)
		srcOff, _ := src.Seek(0, io.SeekCurrent)
		info, _ := dst.Stat()
		if n != 0 || srcOff != 0 || info.Size() != 0 {
			t.Fatalf("failed CopyFileRange copied: n=%d, source offset %d, destination %d bytes", n, srcOff, info.Size())
		}
	})
}

// TestNilInjectorDoesNotAllocate checks a nil injector adds no allocation
// to the gated calls that allocate nothing themselves, since they run per
// frame, file, or connection. Each call also has to succeed, so the loop
// measures the real call and not an early error.
func TestNilInjectorDoesNotAllocate(t *testing.T) {
	var inj *faults.Injector
	ps := os.Getpagesize()
	f := memfd(t, pattern(2*ps))
	fd := int(f.Fd())
	src, dst := memfd(t, pattern(10)), memfd(t, nil)
	if _, err := src.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	_, w := rawPipe(t)
	fds := []PollFd{{Fd: int32(w), Events: POLLOUT}}
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Close()
	defer pw.Close()
	size := pipeSize(t, pw)
	mapping, err := Mmap(inj, f.Fd(), 0, 2*ps, PROT_READ, MAP_SHARED)
	if err != nil {
		t.Fatalf("Mmap: %v", err)
	}
	defer Munmap(mapping)
	vec := make([]byte, 2)
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var failed error
	check := func(err error) {
		if err != nil && failed == nil {
			failed = err
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		_, _, poll := ZeroCopyCalls(inj)
		_, err := poll(fds, 0)
		check(err)
		SetPipeSize(inj, pw, size)
		dup, err := Dup(inj, fd)
		check(err)
		check(Close(dup))
		check(Fadvise(inj, f.Fd(), 0, 0, FADV_SEQUENTIAL))
		check(Readahead(inj, f.Fd(), 0, int64(ps)))
		_, _, err = FileID(inj, fd)
		check(err)
		check(Fdatasync(inj, fd))
		check(Syncfs(inj, fd))
		check(Mincore(inj, mapping, vec))
		_, err = CopyFileRange(inj, dst, src, 1)
		check(err)
		_, _ = StatOf(info)
	})
	if failed != nil {
		t.Fatalf("a call failed: %v", failed)
	}
	if allocs != 0 {
		t.Fatalf("nil injector allocated %.1f times per run", allocs)
	}
}
