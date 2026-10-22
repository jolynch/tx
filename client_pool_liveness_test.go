package tx

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Keep liveness at the syscall boundary independent of listener timing.
// The socket pair is still a real nonblocking transport exposed by net.Conn.
func livenessSocketPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	var conns [2]net.Conn
	for i, fd := range fds {
		f := os.NewFile(uintptr(fd), "liveness-test")
		conns[i], err = net.FileConn(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conns[i].Close() })
	}
	return conns[0], conns[1]
}

func TestSessionConnAlivePeek(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		name := "plain"
		if authenticated {
			name = "authenticated socket"
		}
		t.Run(name, func(t *testing.T) {
			client, server := livenessSocketPair(t)
			conn := client
			if authenticated {
				conn = &noiseConn{Conn: client}
			}
			sc := &sessionTCPConn{Conn: conn}
			if !sessionConnAlive(sc) {
				t.Fatal("idle socket reported dead")
			}
			if _, err := server.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if sessionConnAlive(sc) {
				t.Fatal("socket with pending protocol bytes reported alive")
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			var got [1]byte
			if _, err := io.ReadFull(client, got[:]); err != nil || got[0] != 'x' {
				t.Fatalf("peek consumed pending bytes: %q, %v", got, err)
			}
			if !sessionConnAlive(sc) {
				t.Fatal("drained socket reported dead")
			}
			server.Close()
			if sessionConnAlive(sc) {
				t.Fatal("peer EOF reported alive")
			}
		})
	}
}

// A custom dialer may expose no syscalls, fail to acquire them, or fail
// raw.Read. All three retain the connection; PeekAlive's error result must
// not be mistaken for a definitive dead peer.
func TestSessionConnAliveUnavailablePeek(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	errUnavailable := errors.New("custom transport cannot peek")
	raw := &livenessRawConn{readErr: errUnavailable}
	for name, conn := range map[string]net.Conn{
		"no syscall access": a,
		"SyscallConn error": &livenessSyscallConn{Conn: a, err: errUnavailable},
		"raw Read error":    &livenessSyscallConn{Conn: a, raw: raw},
	} {
		t.Run(name, func(t *testing.T) {
			if !sessionConnAlive(&sessionTCPConn{Conn: conn}) {
				t.Fatal("unavailable peek evicted a custom transport")
			}
		})
	}
	if raw.reads != 1 {
		t.Fatalf("raw.Read called %d times; want 1", raw.reads)
	}
}

func TestNoiseConnForwardsSyscallAccess(t *testing.T) {
	var _ syscall.Conn = (*noiseConn)(nil)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	raw := &livenessRawConn{}
	errAccess := errors.New("syscall access failed")
	for _, wantErr := range []error{nil, errAccess} {
		conn := &noiseConn{Conn: &livenessSyscallConn{Conn: a, raw: raw, err: wantErr}}
		got, err := conn.SyscallConn()
		if got != raw || !errors.Is(err, wantErr) {
			t.Fatalf("forwarded syscall access = %v, %v; want original raw connection, %v", got, err, wantErr)
		}
	}
	if raw, err := (&noiseConn{Conn: a}).SyscallConn(); raw != nil || err == nil {
		t.Fatalf("custom transport syscall access = %v, %v; want nil, error", raw, err)
	}
}

type livenessSyscallConn struct {
	net.Conn
	raw syscall.RawConn
	err error
}

func (c *livenessSyscallConn) SyscallConn() (syscall.RawConn, error) { return c.raw, c.err }

type livenessRawConn struct {
	syscall.RawConn
	readErr error
	reads   int
}

func (r *livenessRawConn) Read(func(uintptr) bool) error {
	r.reads++
	return r.readErr
}
