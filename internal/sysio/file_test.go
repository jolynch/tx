//go:build linux

package sysio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/faults"
)

// TestStatOf checks StatOf reports the ownership, identity, and link count
// that os.Stat's syscall.Stat_t holds, copies each field of a hand-built
// Stat_t whose fields all differ to its own place, and reports false for a
// FileInfo that carries no Stat_t.
func TestStatOf(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, filepath.Join(dir, "b")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	got, ok := StatOf(info)
	want := StatInfo{UID: st.Uid, GID: st.Gid, Dev: uint64(st.Dev), Ino: st.Ino, Nlink: uint64(st.Nlink)}
	if !ok || got != want {
		t.Fatalf("StatOf = %+v, %v; want %+v, true", got, ok, want)
	}
	if got.Nlink != 2 {
		t.Fatalf("StatOf Nlink = %d for a file with one hard link, want 2", got.Nlink)
	}
	distinct := &syscall.Stat_t{Uid: 1001, Gid: 2002, Dev: 3003, Ino: 4004, Nlink: 5005}
	if got, ok := StatOf(fakeInfo{sys: distinct}); !ok || got != (StatInfo{UID: 1001, GID: 2002, Dev: 3003, Ino: 4004, Nlink: 5005}) {
		t.Fatalf("StatOf(%+v) = %+v, %v", *distinct, got, ok)
	}
	for _, sys := range []any{nil, (*syscall.Stat_t)(nil), "not a stat"} {
		if got, ok := StatOf(fakeInfo{sys: sys}); ok || got != (StatInfo{}) {
			t.Fatalf("StatOf(Sys() = %#v) = %+v, %v; want zero, false", sys, got, ok)
		}
	}
}

// fakeInfo is a FileInfo whose Sys returns sys.
type fakeInfo struct {
	fs.FileInfo
	sys any
}

func (f fakeInfo) Sys() any { return f.sys }

// testDupClose writes through a duplicate descriptor, reads the bytes back
// through the original, and then closes the duplicate.
func testDupClose(t *testing.T, inj *faults.Injector) {
	f := memfd(t, nil)
	fd := int(f.Fd())
	dup, err := Dup(inj, fd)
	if err != nil || dup < 0 || dup == fd {
		t.Fatalf("Dup(%d) = %d, %v", fd, dup, err)
	}
	if _, err := unix.Pwrite(dup, []byte("dup"), 0); err != nil {
		t.Fatalf("write through the duplicate: %v", err)
	}
	got := make([]byte, 3)
	if _, err := f.ReadAt(got, 0); err != nil || string(got) != "dup" {
		t.Fatalf("read %q, %v through the original; want dup", got, err)
	}
	checkGated(t, inj, faults.SysDup, 1)
	if err := Close(dup); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := unix.FcntlInt(uintptr(dup), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("descriptor %d still open after Close (%v)", dup, err)
	}
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatalf("closing the duplicate closed the original: %v", err)
	}
}

func testHints(t *testing.T, inj *faults.Injector) {
	f := memfd(t, pattern(16<<10))
	if err := Fadvise(inj, f.Fd(), 0, 0, FADV_SEQUENTIAL); err != nil {
		t.Fatalf("Fadvise: %v", err)
	}
	if err := Readahead(inj, f.Fd(), 0, 16<<10); err != nil {
		t.Fatalf("Readahead: %v", err)
	}
	checkGated(t, inj, faults.SysFadvise, 1)
	checkGated(t, inj, faults.SysReadahead, 1)
}

func testFileID(t *testing.T, inj *faults.Injector) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	dev, ino, err := FileID(inj, int(f.Fd()))
	if err != nil || dev != uint64(st.Dev) || ino != st.Ino {
		t.Fatalf("FileID = %d, %d, %v; want %d, %d, nil", dev, ino, err, st.Dev, st.Ino)
	}
	checkGated(t, inj, faults.SysFstat, 1)
}

func testDurability(t *testing.T, inj *faults.Injector) {
	f := memfd(t, []byte("durable"))
	if err := Fdatasync(inj, int(f.Fd())); err != nil {
		t.Fatalf("Fdatasync: %v", err)
	}
	dir, err := OpenDir(tmpfsDir(t))
	if err != nil || dir < 0 {
		t.Fatalf("OpenDir = %d, %v", dir, err)
	}
	if err := Syncfs(inj, dir); err != nil {
		t.Fatalf("Syncfs: %v", err)
	}
	if err := Close(dir); err != nil {
		t.Fatalf("Close: %v", err)
	}
	checkGated(t, inj, faults.SysFdatasync, 1)
	checkGated(t, inj, faults.SysSyncfs, 1)
}

// tmpfsDir returns /dev/shm when it is tmpfs, so a syncfs there flushes no
// disk, and a test directory otherwise.
func tmpfsDir(t *testing.T) string {
	var st unix.Statfs_t
	if err := unix.Statfs("/dev/shm", &st); err == nil && st.Type == unix.TMPFS_MAGIC {
		return "/dev/shm"
	}
	return t.TempDir()
}

// testMmapMincore maps a file just written, from its first page and from its
// second, checks the mapped bytes, checks mincore reports every page
// resident, and checks Munmap removes both mappings from /proc/self/maps.
func testMmapMincore(t *testing.T, inj *faults.Injector) {
	ps := os.Getpagesize()
	data := pattern(3*ps + 10)
	f := memfd(t, data)
	whole, err := Mmap(inj, f.Fd(), 0, 4*ps, PROT_READ, MAP_SHARED)
	if err != nil || len(whole) != 4*ps {
		t.Fatalf("Mmap = %d bytes, %v", len(whole), err)
	}
	if !bytes.Equal(whole[:len(data)], data) {
		t.Fatal("mapping holds the wrong bytes")
	}
	vec := make([]byte, 4)
	if err := Mincore(inj, whole, vec); err != nil {
		t.Fatalf("Mincore: %v", err)
	}
	for i, v := range vec {
		if v&1 == 0 {
			t.Fatalf("Mincore reports page %d of a file just written not resident: %v", i, vec)
		}
	}
	second, err := Mmap(inj, f.Fd(), int64(ps), ps, PROT_READ, MAP_SHARED)
	if err != nil || len(second) != ps {
		t.Fatalf("Mmap at page 1 = %d bytes, %v", len(second), err)
	}
	if !bytes.Equal(second, data[ps:2*ps]) {
		t.Fatal("mapping at page 1 holds the wrong bytes")
	}
	if n := mappings(t); n == 0 {
		t.Fatal("/proc/self/maps shows no mapping of the file")
	}
	for _, b := range [][]byte{second, whole} {
		if err := Munmap(b); err != nil {
			t.Fatalf("Munmap: %v", err)
		}
	}
	if n := mappings(t); n != 0 {
		t.Fatalf("%d mappings of the file remain after Munmap", n)
	}
	checkGated(t, inj, faults.SysMmap, 2)
	checkGated(t, inj, faults.SysMincore, 1)
}

// testCopyFileRange copies from the source's current offset to the
// destination's, and checks both offsets advance and that a copy at end of
// file returns 0.
func testCopyFileRange(t *testing.T, inj *faults.Injector) {
	data := pattern(5000)
	src, dst := memfd(t, data), memfd(t, nil)
	if _, err := src.Seek(3, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := CopyFileRange(inj, dst, src, 4000); n != 4000 || err != nil {
		t.Fatalf("CopyFileRange = %d, %v; want 4000, nil", n, err)
	}
	if n, err := CopyFileRange(inj, dst, src, 4000); n != 997 || err != nil {
		t.Fatalf("CopyFileRange of the rest = %d, %v; want 997, nil", n, err)
	}
	if n, err := CopyFileRange(inj, dst, src, 4000); n != 0 || err != nil {
		t.Fatalf("CopyFileRange at end of file = %d, %v; want 0, nil", n, err)
	}
	got, err := io.ReadAll(io.NewSectionReader(dst, 0, 1<<20))
	if err != nil || !bytes.Equal(got, data[3:]) {
		t.Fatalf("copied %d bytes, %v; want the source from offset 3", len(got), err)
	}
	checkGated(t, inj, faults.SysCopyFileRange, 3)
}

// TestMincoreShortVec checks Mincore refuses a vec shorter than one byte
// per page of the mapping, counting a partial last page, rather than letting
// the kernel write past it.
func TestMincoreShortVec(t *testing.T) {
	ps := os.Getpagesize()
	f := memfd(t, pattern(3*ps))
	for _, tc := range []struct{ length, vec int }{
		{3 * ps, 2}, // whole pages
		{ps + 1, 1}, // one byte into a second page
	} {
		b, err := Mmap(nil, f.Fd(), 0, tc.length, PROT_READ, MAP_SHARED)
		if err != nil {
			t.Fatalf("Mmap: %v", err)
		}
		backing := bytes.Repeat([]byte{0xaa}, tc.vec+1)
		err = Mincore(nil, b, backing[:tc.vec])
		Munmap(b)
		if err == nil {
			t.Fatalf("Mincore accepted %d vec bytes for a %d-byte mapping", tc.vec, tc.length)
		}
		if !bytes.Equal(backing, bytes.Repeat([]byte{0xaa}, tc.vec+1)) {
			t.Fatalf("Mincore with a short vec wrote %v", backing)
		}
	}
}

// TestOpenDir checks OpenDir opens a directory close-on-exec, and refuses a
// path that is not a directory.
func TestOpenDir(t *testing.T) {
	dir := t.TempDir()
	fd, err := OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer Close(fd)
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("descriptor flags %#x, %v; want FD_CLOEXEC", flags, err)
	}
	path := filepath.Join(dir, "file")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if fd, err := OpenDir(path); err == nil {
		Close(fd)
		t.Fatal("OpenDir opened a regular file")
	}
}

// TestOpenNonblockingWriter checks OpenNonblockingWriter fails at once on a
// FIFO with no reader, opens one with a reader, creates a missing file,
// truncates or appends to an existing one, and opens close-on-exec.
func TestOpenNonblockingWriter(t *testing.T) {
	open := func(t *testing.T, path string, truncate bool) (f *os.File, err error) {
		t.Helper()
		done := make(chan struct{})
		go func() { defer close(done); f, err = OpenNonblockingWriter(path, truncate) }()
		select {
		case <-done:
			return f, err
		case <-time.After(callTimeout):
			// Release a blocked open with a reader before failing.
			if fd, rerr := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0); rerr == nil {
				<-done
				unix.Close(fd)
				if f != nil {
					f.Close()
				}
			}
			t.Fatalf("OpenNonblockingWriter(%s) blocked for %v", path, callTimeout)
			return nil, nil
		}
	}
	cloexec := func(t *testing.T, f *os.File) {
		t.Helper()
		flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("descriptor flags %#x, %v; want FD_CLOEXEC", flags, err)
		}
	}
	write := func(t *testing.T, path string, truncate bool, s string) {
		t.Helper()
		f, err := open(t, path, truncate)
		if err != nil {
			t.Fatalf("OpenNonblockingWriter: %v", err)
		}
		defer f.Close()
		cloexec(t, f)
		if _, err := f.WriteString(s); err != nil {
			t.Fatalf("WriteString: %v", err)
		}
	}
	contents := func(t *testing.T, path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	dir := t.TempDir()

	t.Run("fifo without reader", func(t *testing.T) {
		path := filepath.Join(dir, "fifo-alone")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		if f, err := open(t, path, true); err == nil {
			f.Close()
			t.Fatal("opened a FIFO with no reader")
		}
	})
	t.Run("fifo with reader", func(t *testing.T) {
		path := filepath.Join(dir, "fifo-read")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		rfd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(rfd)
		write(t, path, true, "event")
		if got := readFull(t, rfd, 5); string(got) != "event" {
			t.Fatalf("reader got %q, want event", got)
		}
	})
	t.Run("create", func(t *testing.T) {
		path := filepath.Join(dir, "new")
		write(t, path, false, "first")
		if got := contents(t, path); got != "first" {
			t.Fatalf("new file holds %q, want first", got)
		}
	})
	t.Run("truncate", func(t *testing.T) {
		path := filepath.Join(dir, "trunc")
		if err := os.WriteFile(path, []byte("hello world"), 0o600); err != nil {
			t.Fatal(err)
		}
		write(t, path, true, "hi")
		if got := contents(t, path); got != "hi" {
			t.Fatalf("truncated file holds %q, want hi", got)
		}
	})
	t.Run("append", func(t *testing.T) {
		path := filepath.Join(dir, "append")
		if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
			t.Fatal(err)
		}
		write(t, path, false, " world")
		if got := contents(t, path); got != "hello world" {
			t.Fatalf("appended file holds %q, want hello world", got)
		}
	})
}

// TestTotalMemoryBytes checks TotalMemoryBytes is positive and, where
// /proc/meminfo is readable, agrees with its MemTotal to within 1 MiB.
// Containers that virtualize /proc/meminfo (lxcfs) report the smaller of
// the container's limit and the host's RAM there, while sysinfo(2) reports
// the host's, so a MemTotal below the result skips the comparison; one
// above it is always wrong.
func TestTotalMemoryBytes(t *testing.T) {
	got, err := TotalMemoryBytes()
	if err != nil || got <= 0 {
		t.Fatalf("TotalMemoryBytes = %d, %v; want a positive size", got, err)
	}
	meminfo, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	var kib int64
	for line := range strings.Lines(string(meminfo)) {
		if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &kib); err == nil {
			break
		}
	}
	switch diff := got - kib<<10; {
	case kib == 0:
		t.Skip("no MemTotal in /proc/meminfo")
	case diff < -1<<20:
		t.Fatalf("TotalMemoryBytes = %d, below MemTotal = %d KiB", got, kib)
	case diff > 1<<20:
		t.Skipf("/proc/meminfo looks virtualized: MemTotal = %d KiB, sysinfo = %d bytes", kib, got)
	}
}
