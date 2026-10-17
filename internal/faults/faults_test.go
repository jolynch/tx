package faults

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// A nil injector must hand back exactly what it was given and allocate
// nothing, since production code calls it on every connection, file, and
// syscall site.
func TestNilInjectorIsIdentity(t *testing.T) {
	var inj *Injector
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if got := inj.WrapConn(a, Client); got != a {
		t.Fatalf("WrapConn returned %T, want the connection itself", got)
	}
	r := strings.NewReader("data")
	if got := inj.WrapReaderAt(context.Background(), SiteSendRead, r); got != io.ReaderAt(r) {
		t.Fatalf("WrapReaderAt returned %T, want the reader itself", got)
	}
	w := nopWriteCloser{&bytes.Buffer{}}
	if got := inj.WrapWriter(SiteOutputWrite, w); got != io.WriteCloser(w) {
		t.Fatalf("WrapWriter returned %T, want the writer itself", got)
	}
	if err := inj.Sys(SysSplice); err != nil {
		t.Fatalf("Sys = %v", err)
	}
	if err := inj.OpenErr(SiteSendOpen); err != nil {
		t.Fatalf("OpenErr = %v", err)
	}
	samePointer := func(name string, got, want any) {
		t.Helper()
		if reflect.ValueOf(got).Pointer() != reflect.ValueOf(want).Pointer() {
			t.Fatalf("%s wrapped the function", name)
		}
	}
	samePointer("WrapSplice", inj.WrapSplice(unix.Splice), unix.Splice)
	samePointer("WrapTee", inj.WrapTee(unix.Tee), unix.Tee)
	samePointer("WrapPoll", inj.WrapPoll(unix.Poll), unix.Poll)
	samePointer("WrapCopyFileRange", inj.WrapCopyFileRange(unix.CopyFileRange), unix.CopyFileRange)
	if FromContext(NewContext(context.Background(), inj)) != nil {
		t.Fatal("a nil injector reached the context")
	}
	if inj.Repro() != "" || inj.Seed() != 0 {
		t.Fatal("a nil injector reports a schedule")
	}
}

func TestNilInjectorDoesNotAllocate(t *testing.T) {
	var inj *Injector
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		_ = inj.WrapConn(a, Server)
		_ = inj.WrapReaderAt(ctx, SiteCXSUMRead, f)
		_ = inj.WrapWriter(SiteOutputWrite, f)
		_ = inj.Sys(SysFadvise)
		_ = inj.OpenErr(SiteCXSUMOpen)
		_ = inj.WrapSplice(unix.Splice)
		_ = inj.WrapCopyFileRange(unix.CopyFileRange)
		_ = NewContext(ctx, inj)
	})
	if allocs != 0 {
		t.Fatalf("nil injector allocated %.1f times per run", allocs)
	}
}

// An injector with no fault kinds wraps everything and changes nothing.
func TestEmptyInjectorIsTransparent(t *testing.T) {
	inj := New(Config{Seed: 7})

	t.Run("conn", func(t *testing.T) {
		a, b := net.Pipe()
		wa, wb := inj.WrapConn(a, Client), inj.WrapConn(b, Server)
		defer wa.Close()
		defer wb.Close()
		if _, ok := wa.(*Conn); !ok {
			t.Fatalf("WrapConn returned %T, want *Conn", wa)
		}
		msg := []byte("SEND tid=1\r\n")
		go func() { _, _ = wa.Write(msg) }()
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(wb, got); err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("read %q, %v; want %q", got, err, msg)
		}
		if wa.(*Conn).ZeroCopyConn() != nil {
			t.Fatal("a pipe offered a zero-copy socket")
		}
		if _, err := wa.(*Conn).SyscallConn(); err == nil {
			t.Fatal("a pipe offered a syscall.Conn")
		}
		if err := wa.(*Conn).CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("CloseWrite on a pipe = %v, want ErrUnsupported", err)
		}
	})

	t.Run("tcp", func(t *testing.T) {
		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
		if err != nil {
			t.Fatal(err)
		}
		server, err := ln.AcceptTCP()
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		wrapped := inj.WrapConn(client, Client).(*Conn)
		defer wrapped.Close()
		if wrapped.ZeroCopyConn() != client || wrapped.Unwrap() != net.Conn(client) {
			t.Fatal("wrapper hid its TCP socket")
		}
		if _, err := wrapped.SyscallConn(); err != nil {
			t.Fatalf("SyscallConn: %v", err)
		}
		if _, err := wrapped.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := wrapped.CloseWrite(); err != nil {
			t.Fatalf("CloseWrite: %v", err)
		}
		got, err := io.ReadAll(server)
		if err != nil || string(got) != "x" {
			t.Fatalf("server read %q, %v after CloseWrite", got, err)
		}
	})

	t.Run("disk", func(t *testing.T) {
		src := strings.NewReader("0123456789")
		r := inj.WrapReaderAt(context.Background(), SiteVerifyRead, src)
		buf := make([]byte, 4)
		if n, err := r.ReadAt(buf, 3); n != 4 || err != nil || string(buf) != "3456" {
			t.Fatalf("ReadAt = %d %v %q", n, err, buf)
		}
		var out bytes.Buffer
		w := inj.WrapWriter(SiteOutputWrite, nopWriteCloser{&out})
		if _, err := w.Write([]byte("abc")); err != nil || out.String() != "abc" {
			t.Fatalf("Write: %v, wrote %q", err, out.String())
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("syscalls", func(t *testing.T) {
		calls := 0
		splice := inj.WrapSplice(func(int, *int64, int, *int64, int, int) (int64, error) {
			calls++
			return 5, nil
		})
		if n, err := splice(0, nil, 0, nil, 5, 0); n != 5 || err != nil || calls != 1 {
			t.Fatalf("splice = %d, %v after %d calls", n, err, calls)
		}
	})

	if FromContext(NewContext(context.Background(), inj)) != inj {
		t.Fatal("injector did not round-trip through a context")
	}
	if got := inj.Repro(); got != "TX_FAULTS_SEED=7" {
		t.Fatalf("Repro = %q", got)
	}
}

func TestFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		enabled bool
		seed    uint64
		wantErr string
	}{
		{name: "unset", env: nil},
		{name: "seed", env: map[string]string{EnvSeed: "42"}, enabled: true, seed: 42},
		{name: "bad seed", env: map[string]string{EnvSeed: "x"}, wantErr: EnvSeed},
		{name: "unknown kind", env: map[string]string{EnvNet: "teleport=0.1"}, wantErr: `unknown NET fault kind "teleport"`},
		{name: "unbalanced", env: map[string]string{EnvSys: "splice.latency=1:β(1ms,2ms"}, wantErr: "unbalanced"},
		{name: "blank variables stay off", env: map[string]string{EnvDisk: "", EnvSeed: " "}},
		{name: "separators alone stay off", env: map[string]string{EnvNet: " , ,"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj, err := FromEnv(func(k string) (string, bool) {
				v, ok := tc.env[k]
				return v, ok
			})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (inj != nil) != tc.enabled {
				t.Fatalf("enabled = %t, want %t", inj != nil, tc.enabled)
			}
			if tc.seed != 0 && inj.Seed() != tc.seed {
				t.Fatalf("seed = %d, want %d", inj.Seed(), tc.seed)
			}
		})
	}
}

func TestSplitTopLevel(t *testing.T) {
	got, err := splitTopLevel(" reset=0.1, pause=0.01:β(100ms,500ms,4s,5s) ,,flip=1e-6")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"reset=0.1", "pause=0.01:β(100ms,500ms,4s,5s)", "flip=1e-6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if _, err := splitTopLevel("a=1)"); err == nil {
		t.Fatal("accepted an unbalanced ')'")
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
