//go:build linux && (amd64 || arm64)

package filexfercli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/jolynch/tx/internal/faults"
	"golang.org/x/sys/unix"
)

func localCopyPair(t *testing.T, data []byte) (dst, src *os.File) {
	t.Helper()
	dir := t.TempDir()
	var err error
	src, err = os.Create(filepath.Join(dir, "src"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	if _, err := src.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	dst, err = os.Create(filepath.Join(dir, "dst"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dst.Close() })
	return dst, src
}

func TestLocalCopyFileGateFailures(t *testing.T) {
	data := patternBytes(257)
	// The catalog chooses EIO, EXDEV, or ENOSPC. Exercise every choice,
	// including EXDEV's buffered fallback, with independently predicted errors.
	seen := map[error]bool{}
	for seed := uint64(1); seed <= 12; seed++ {
		cfg := faults.Config{Seed: seed, Specs: map[faults.Family]string{faults.FamilySys: "copy_file_range.fail=1"}}
		wantErr := faults.New(cfg).Sys(faults.SysCopyFileRange)
		if seen[wantErr] {
			continue
		}
		seen[wantErr] = true
		t.Run(wantErr.Error(), func(t *testing.T) {
			dst, src := localCopyPair(t, data)
			inj := faults.New(cfg)
			var reported int64
			n, err := localCopyFile(inj, dst, src, int64(len(data)), func(n int64) { reported += n })
			if errors.Is(wantErr, unix.EXDEV) {
				if err != nil || n != int64(len(data)) || reported != n {
					t.Fatalf("fallback = %d, %v, progress %d", n, err, reported)
				}
				got, err := os.ReadFile(dst.Name())
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("fallback data %q, %v", got, err)
				}
			} else if !errors.Is(err, wantErr) || n != 0 || reported != 0 {
				t.Fatalf("failed gate = %d, %v, progress %d; want %v without writes", n, err, reported, wantErr)
			}
			if got := inj.Counts().Sys[faults.SysCopyFileRange]; got != 1 {
				t.Fatalf("gate calls %d, want 1", got)
			}
			if !errors.Is(wantErr, unix.EXDEV) {
				info, err := dst.Stat()
				if err != nil || info.Size() != 0 {
					t.Fatalf("failed gate changed output: %v, %v", info, err)
				}
			}
		})
	}
	if len(seen) != 3 {
		t.Fatalf("covered %d catalog errors, want EIO, EXDEV, ENOSPC", len(seen))
	}
}

func TestLocalCopyFileShortCallsAndEOF(t *testing.T) {
	data := patternBytes(257)
	for _, tc := range []struct {
		name  string
		size  int64
		short bool
	}{
		{"nil", 97, false}, {"short", int64(len(data)), true}, {"early-eof", 400, true}, {"empty", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst, src := localCopyPair(t, data)
			var inj *faults.Injector
			if tc.short {
				inj = faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "copy_file_range.short=1"}})
			}
			var progress []int64
			n, err := localCopyFile(inj, dst, src, tc.size, func(n int64) { progress = append(progress, n) })
			want := min(tc.size, int64(len(data)))
			if err != nil || n != want {
				t.Fatalf("copy = %d, %v; want %d", n, err, want)
			}
			got, err := os.ReadFile(dst.Name())
			if err != nil || !bytes.Equal(got, data[:want]) {
				t.Fatalf("copy data length %d, %v; want %d bytes", len(got), err, want)
			}
			var sum int64
			for _, n := range progress {
				if n <= 0 {
					t.Fatalf("nonpositive progress %v", progress)
				}
				sum += n
			}
			if sum != want {
				t.Fatalf("progress %v totals %d, want %d", progress, sum, want)
			}
			if tc.short && want > 0 && len(progress) < 2 {
				t.Fatalf("short gate did not split copy: %v", progress)
			}
			if tc.short {
				wantCalls := uint64(len(progress))
				if tc.size > int64(len(data)) {
					wantCalls++
				}
				if got := inj.Counts().Sys[faults.SysCopyFileRange]; got != wantCalls {
					t.Fatalf("gate calls %d, want %d", got, wantCalls)
				}
			}
		})
	}
}

func TestCopyFileRangeUnsupportedErrors(t *testing.T) {
	for _, errno := range []error{unix.ENOSYS, unix.EOPNOTSUPP, unix.EXDEV, unix.EINVAL, unix.EINTR, unix.EIO, unix.ENOSPC, unix.EBADF} {
		want := errno == unix.ENOSYS || errno == unix.EOPNOTSUPP || errno == unix.EXDEV || errno == unix.EINVAL
		for _, err := range []error{errno, fmt.Errorf("copy: %w", errno)} {
			if got := isCopyFileRangeUnsupported(err); got != want {
				t.Errorf("unsupported(%v) = %v, want %v", err, got, want)
			}
		}
	}
}

type localOwnerInfo struct {
	os.FileInfo
	stat any
}

func (i localOwnerInfo) Sys() any { return i.stat }

func TestLocalFileOwnerMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stat     any
		uid, gid int
		ok       bool
	}{
		{"numeric", &syscall.Stat_t{Uid: 1234567, Gid: 7654321}, 1234567, 7654321, true},
		{"root", &syscall.Stat_t{}, 0, 0, true}, {"missing", nil, 0, 0, false}, {"foreign", "other stat", 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uid, gid, ok := localFileOwner(localOwnerInfo{stat: tc.stat})
			if uid != tc.uid || gid != tc.gid || ok != tc.ok {
				t.Fatalf("owner = %d, %d, %v; want %d, %d, %v", uid, gid, ok, tc.uid, tc.gid, tc.ok)
			}
		})
	}
}

// TestLocalCopyFileKernelErrors uses seccomp in a child to supply one
// EINTR, or an unsupported error after progress. Matching a seeded short
// request's length lets later copy calls proceed without a production seam.
func TestLocalCopyFileKernelErrors(t *testing.T) {
	const env = "TX_LOCAL_COPY_KERNEL_ERROR"
	if mode := os.Getenv(env); mode != "" {
		localCopyKernelErrorChild(t, mode)
		return
	}
	for _, mode := range []string{"eintr", "mid-copy-exdev"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLocalCopyFileKernelErrors$")
			cmd.Env = append(os.Environ(), env+"="+mode, "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
			out, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("kernel-error child timed out: %s", out)
			}
			var exit *exec.ExitError
			if errors.As(err, &exit) && exit.ExitCode() == 77 {
				t.Skipf("seccomp unavailable: %s", out)
			}
			if err != nil {
				t.Fatalf("kernel-error child: %v\n%s", err, out)
			}
		})
	}
}

func localCopyKernelErrorChild(t *testing.T, mode string) {
	data := patternBytes(257)
	dst, src := localCopyPair(t, data)
	cfg := faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "copy_file_range.short=1"}}
	probe := faults.New(cfg).WrapCopyFileRange(func(_ int, _ *int64, _ int, _ *int64, n int, _ int) (int, error) { return n, nil })
	first, _ := probe(0, nil, 0, nil, len(data), 0)
	var match int
	var errno syscall.Errno
	if mode == "eintr" {
		match, errno = first, unix.EINTR
	} else {
		match, _ = probe(0, nil, 0, nil, len(data)-first, 0)
		errno = unix.EXDEV
	}
	if match <= 1 || match == len(data) || (mode != "eintr" && match == first) {
		t.Fatalf("invalid seeded short requests: first %d, match %d", first, match)
	}
	// seccomp_data.args[4] is copy_file_range's byte count, at byte 48.
	runtime.LockOSThread()
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(77)
	}
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_COPY_FILE_RANGE, Jf: 3},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 48},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: uint32(match), Jf: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(errno)},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	}
	prog := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&prog)), 0, 0); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(77)
	}
	inj := faults.New(cfg)
	var progress []int64
	n, err := localCopyFile(inj, dst, src, int64(len(data)), func(n int64) { progress = append(progress, n) })
	if mode == "eintr" {
		if err != nil || n != int64(len(data)) {
			t.Fatalf("EINTR copy = %d, %v", n, err)
		}
		if len(progress) == 0 || progress[0] == int64(first) {
			t.Fatalf("EINTR did not interrupt first short request: %v", progress)
		}
		if got := inj.Counts().Sys[faults.SysCopyFileRange]; got <= uint64(len(progress)) {
			t.Fatalf("gate calls %d, progress %v: no retry", got, progress)
		}
	} else if !errors.Is(err, unix.EXDEV) || n != int64(first) || len(progress) != 1 || progress[0] != n {
		t.Fatalf("mid-copy EXDEV = %d, %v, progress %v; want %d bytes then error", n, err, progress, first)
	}
	got, readErr := os.ReadFile(dst.Name())
	if readErr != nil || !bytes.Equal(got, data[:n]) {
		t.Fatalf("kernel-error copy data %d bytes, %v; want %d", len(got), readErr, n)
	}
}
