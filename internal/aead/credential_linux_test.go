//go:build linux && (amd64 || arm64)

package aead

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Return EROFS only for openat(O_CREAT), so reading the empty key
// directory still succeeds. The filter stays in a disposable child.
func TestReadOnlyKeysDirectory(t *testing.T) {
	const env = "TX_READONLY_KEYS_CHILD"
	if os.Getenv(env) != "" {
		dir := t.TempDir()
		runtime.LockOSThread()
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(77)
		}
		// seccomp_data.args[2] is openat's flags word, at byte 32.
		filter := []unix.SockFilter{
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_OPENAT, Jf: 3},
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 32},
			{Code: unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K, K: unix.O_CREAT, Jf: 1},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EROFS)},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
		}
		prog := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
		if err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&prog)), 0, 0); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(77)
		}
		id, ephemeral, err := LoadOrGenerateAgeIdentity(dir, true)
		if err != nil || id == nil || !ephemeral {
			t.Fatalf("default read-only = (%v, %v, %v), want ephemeral", id, ephemeral, err)
		}
		id, ephemeral, err = LoadOrGenerateAgeIdentity(dir, false)
		if !errors.Is(err, unix.EROFS) || id != nil || ephemeral {
			t.Fatalf("explicit read-only = (%v, %v, %v), want EROFS", id, ephemeral, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("read-only directory changed: %v, %v", entries, err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReadOnlyKeysDirectory$")
	cmd.Env = append(os.Environ(), env+"=1", "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("read-only key child timed out: %s", out)
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 77 {
		t.Skipf("seccomp unavailable: %s", out)
	}
	if err != nil {
		t.Fatalf("read-only key child: %v\n%s", err, out)
	}
}
