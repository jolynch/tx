//go:build linux

package sysio

import (
	"bytes"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/faults"
)

// buffered returns how many bytes wait unread in a pipe.
func buffered(t *testing.T, fd int) int {
	t.Helper()
	n, err := unix.IoctlGetInt(fd, unix.TIOCINQ) // FIONREAD
	if err != nil {
		t.Fatalf("FIONREAD: %v", err)
	}
	return n
}

// readFull reads exactly n bytes from a pipe that already holds them.
func readFull(t *testing.T, fd, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	for got := 0; got < n; {
		m, err := unix.Read(fd, b[got:])
		if err != nil || m == 0 {
			t.Fatalf("read %d of %d bytes: %v", got, n, err)
		}
		got += m
	}
	return b
}

func pipeSize(t *testing.T, f *os.File) int {
	t.Helper()
	n, err := unix.FcntlInt(f.Fd(), unix.F_GETPIPE_SZ, 0)
	if err != nil {
		t.Fatalf("F_GETPIPE_SZ: %v", err)
	}
	return n
}

// tcpPair returns both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (client, server *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	s := <-accepted
	if s == nil {
		c.Close()
		t.Fatal("Accept failed")
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	return c.(*net.TCPConn), s.(*net.TCPConn)
}

// testZeroCopyCalls splices a file range into a pipe, tees it into a second
// pipe, splices the first pipe out to a file, and polls a pipe for writing.
func testZeroCopyCalls(t *testing.T, inj *faults.Injector) {
	splice, tee, poll := ZeroCopyCalls(inj)
	if splice == nil || tee == nil || poll == nil {
		t.Fatal("ZeroCopyCalls returned a nil function")
	}
	data := pattern(3000)
	src, dst := memfd(t, data), memfd(t, nil)
	r1, w1 := rawPipe(t)
	r2, w2 := rawPipe(t)

	roff := int64(100)
	if n, err := splice(int(src.Fd()), &roff, w1, nil, 1000, SPLICE_F_MOVE); n != 1000 || err != nil {
		t.Fatalf("splice file to pipe = %d, %v; want 1000, nil", n, err)
	}
	if roff != 1100 {
		t.Fatalf("splice left the source offset at %d, want 1100", roff)
	}
	if n, err := tee(r1, w2, 1000, 0); n != 1000 || err != nil {
		t.Fatalf("tee = %d, %v; want 1000, nil", n, err)
	}
	woff := int64(0)
	if n, err := splice(r1, nil, int(dst.Fd()), &woff, 1000, 0); n != 1000 || err != nil {
		t.Fatalf("splice pipe to file = %d, %v; want 1000, nil", n, err)
	}
	got := make([]byte, 1000)
	if _, err := dst.ReadAt(got, 0); err != nil || !bytes.Equal(got, data[100:1100]) {
		t.Fatalf("spliced file holds the wrong bytes (%v)", err)
	}
	if got := readFull(t, r2, 1000); !bytes.Equal(got, data[100:1100]) {
		t.Fatal("teed pipe holds the wrong bytes")
	}
	fds := []PollFd{{Fd: int32(w1), Events: POLLOUT}}
	if n, err := poll(fds, 0); n != 1 || err != nil || fds[0].Revents&POLLOUT == 0 {
		t.Fatalf("poll = %d, %v, revents %#x; want 1, nil, POLLOUT", n, err, fds[0].Revents)
	}
	checkGated(t, inj, faults.SysSplice, 2)
	checkGated(t, inj, faults.SysTee, 1)
	checkGated(t, inj, faults.SysPoll, 1)
}

func testPipe(t *testing.T, inj *faults.Injector) {
	r, w, err := Pipe(inj)
	if err != nil || r == nil || w == nil {
		t.Fatalf("Pipe = %v, %v, %v", r, w, err)
	}
	defer r.Close()
	defer w.Close()
	if _, err := w.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(r, got); err != nil || string(got) != "ping" {
		t.Fatalf("read %q, %v; want ping", got, err)
	}
	checkGated(t, inj, faults.SysPipe, 1)
}

func testSetPipeSize(t *testing.T, inj *faults.Injector) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	const want = 256 << 10
	if before := pipeSize(t, w); before >= want {
		t.Skipf("pipes already start at %d bytes", before)
	}
	requirePipeGrowth(t, want)
	SetPipeSize(inj, w, want)
	if got := pipeSize(t, w); got < want {
		t.Fatalf("pipe size %d after SetPipeSize(%d)", got, want)
	}
	checkGated(t, inj, faults.SysFcntl, 1)
}

// requirePipeGrowth skips the test unless F_SETPIPE_SZ, called directly,
// can grow a fresh pipe to size bytes here. It cannot when the user is over
// /proc/sys/fs/pipe-user-pages-soft, or size is over pipe-max-size; a test
// of SetPipeSize would then fail falsely, or pass without a real call.
func requirePipeGrowth(t *testing.T, size int) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if _, err := unix.FcntlInt(w.Fd(), unix.F_SETPIPE_SZ, size); err != nil {
		t.Skipf("F_SETPIPE_SZ cannot grow a pipe to %d bytes here: %v", size, err)
	}
	if got := pipeSize(t, w); got < size {
		t.Skipf("F_SETPIPE_SZ left a pipe at %d bytes, asked for %d", got, size)
	}
}

// testDupConn writes through a duplicate of a TCP socket, and checks the
// connection still works after the duplicate closes.
func testDupConn(t *testing.T, inj *faults.Injector) {
	client, server := tcpPair(t)
	dup, err := DupConn(inj, client)
	if err != nil || dup < 0 {
		t.Fatalf("DupConn = %d, %v", dup, err)
	}
	checkGated(t, inj, faults.SysDup, 1)
	if _, err := unix.Write(dup, []byte("hi")); err != nil {
		t.Fatalf("write through the duplicate: %v", err)
	}
	if err := Close(dup); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := client.Write([]byte("yo")); err != nil {
		t.Fatalf("write after closing the duplicate: %v", err)
	}
	got := make([]byte, 4)
	_ = server.SetReadDeadline(time.Now().Add(callTimeout))
	if _, err := io.ReadFull(server, got); err != nil || string(got) != "hiyo" {
		t.Fatalf("peer read %q, %v; want hiyo", got, err)
	}
}

// TestZeroCopyLatency checks splice, tee, and poll from ZeroCopyCalls sleep
// for their latency kinds and then make the real call.
func TestZeroCopyLatency(t *testing.T) {
	const lat = "1:β(5ms,6ms,8ms,10ms)"
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{
		faults.FamilySys: "splice.latency=" + lat + ", tee.latency=" + lat + ", poll.latency=" + lat,
	}})
	splice, tee, poll := ZeroCopyCalls(inj)
	src := memfd(t, pattern(100))
	r1, w1 := rawPipe(t)
	_, w2 := rawPipe(t)
	slow := func(name string, call func() (int64, error)) {
		t.Helper()
		start := time.Now()
		n, err := call()
		if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
			t.Fatalf("%s returned after %v, want at least 5ms", name, elapsed)
		}
		if n != 100 && !(name == "poll" && n == 1) || err != nil {
			t.Fatalf("%s = %d, %v after its latency", name, n, err)
		}
	}
	roff := int64(0)
	slow("splice", func() (int64, error) { return splice(int(src.Fd()), &roff, w1, nil, 100, 0) })
	slow("tee", func() (int64, error) { return tee(r1, w2, 100, 0) })
	fds := []PollFd{{Fd: int32(w1), Events: POLLOUT}}
	slow("poll", func() (int64, error) { n, err := poll(fds, 0); return int64(n), err })
}

// TestShortCalls checks the short kinds make splice, tee, and
// copy_file_range ask for fewer bytes than requested, but at least one, and
// that the bytes they do move are the right ones.
func TestShortCalls(t *testing.T) {
	data := pattern(100)
	t.Run("splice", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "splice.short=1"}})
		splice, _, _ := ZeroCopyCalls(inj)
		src := memfd(t, data)
		r, w := rawPipe(t)
		roff := int64(0)
		n, err := splice(int(src.Fd()), &roff, w, nil, 100, 0)
		if err != nil || n < 1 || n >= 100 {
			t.Fatalf("short splice of 100 bytes = %d, %v", n, err)
		}
		if got := readFull(t, r, int(n)); !bytes.Equal(got, data[:n]) || buffered(t, r) != 0 {
			t.Fatal("short splice moved the wrong bytes")
		}
	})
	t.Run("tee", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "tee.short=1"}})
		_, tee, _ := ZeroCopyCalls(inj)
		r1, w1 := rawPipe(t)
		r2, w2 := rawPipe(t)
		if _, err := unix.Write(w1, data); err != nil {
			t.Fatal(err)
		}
		n, err := tee(r1, w2, 100, 0)
		if err != nil || n < 1 || n >= 100 {
			t.Fatalf("short tee of 100 bytes = %d, %v", n, err)
		}
		if got := readFull(t, r2, int(n)); !bytes.Equal(got, data[:n]) || buffered(t, r2) != 0 {
			t.Fatal("short tee copied the wrong bytes")
		}
	})
	t.Run("copy_file_range", func(t *testing.T) {
		inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: "copy_file_range.short=1"}})
		src, dst := memfd(t, data), memfd(t, nil)
		n, err := CopyFileRange(inj, dst, src, 100)
		if err != nil || n < 1 || n >= 100 {
			t.Fatalf("short CopyFileRange of 100 bytes = %d, %v", n, err)
		}
		got, err := io.ReadAll(io.NewSectionReader(dst, 0, 1<<20))
		if err != nil || !bytes.Equal(got, data[:n]) {
			t.Fatalf("short CopyFileRange wrote %d bytes, %v; want the first %d", len(got), err, n)
		}
	})
}

// TestTCPByteCounts checks TCPByteCounts tells received from sent bytes:
// after a one-way write, the writer reports the bytes sent and none
// received, and the reader the reverse.
func TestTCPByteCounts(t *testing.T) {
	client, server := tcpPair(t)
	const n = 1000
	_ = server.SetDeadline(time.Now().Add(callTimeout))
	buf := pattern(n)
	if _, err := client.Write(buf); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(server, buf); err != nil {
		t.Fatal(err)
	}
	if received, sent := TCPByteCounts(client); received != 0 || sent < n {
		t.Fatalf("writer: TCPByteCounts = %d received, %d sent; want 0, at least %d", received, sent, n)
	}
	if received, sent := TCPByteCounts(server); received < n || sent != 0 {
		t.Fatalf("reader: TCPByteCounts = %d received, %d sent; want at least %d, 0", received, sent, n)
	}
}

// tcpOption returns conn's IPPROTO_TCP option opt.
func tcpOption(t *testing.T, conn *net.TCPConn, opt int) int {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var serr error
	if err := raw.Control(func(fd uintptr) { v, serr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, opt) }); err != nil {
		t.Fatal(err)
	}
	if serr != nil {
		t.Fatalf("getsockopt %d: %v", opt, serr)
	}
	return v
}

// TestTCPSocketOptions checks SetTCPUserTimeout sets TCP_USER_TIMEOUT in
// milliseconds and SetTCPNotSentLowat sets TCP_NOTSENT_LOWAT in bytes, as
// getsockopt reads them back, and that both report a closed connection.
func TestTCPSocketOptions(t *testing.T) {
	client, _ := tcpPair(t)
	if err := SetTCPUserTimeout(client, 2500*time.Millisecond); err != nil {
		t.Fatalf("SetTCPUserTimeout: %v", err)
	}
	if got := tcpOption(t, client, unix.TCP_USER_TIMEOUT); got != 2500 {
		t.Fatalf("TCP_USER_TIMEOUT = %d, want 2500 (milliseconds)", got)
	}
	if err := SetTCPNotSentLowat(client, 24<<10); err != nil {
		t.Fatalf("SetTCPNotSentLowat: %v", err)
	}
	if got := tcpOption(t, client, unix.TCP_NOTSENT_LOWAT); got != 24<<10 {
		t.Fatalf("TCP_NOTSENT_LOWAT = %d, want %d", got, 24<<10)
	}
	_ = client.Close()
	if err := SetTCPUserTimeout(client, time.Second); err == nil {
		t.Fatal("SetTCPUserTimeout on a closed connection succeeded")
	}
	if err := SetTCPNotSentLowat(client, 1<<10); err == nil {
		t.Fatal("SetTCPNotSentLowat on a closed connection succeeded")
	}
}

// TestPeekAlive checks PeekAlive reports an idle open connection alive, and
// a connection whose peer closed or reset it, or that holds unread bytes,
// not alive, without consuming those bytes. Each case waits until the
// socket is readable, then peeks once: a second peek after a reset would see
// end of file rather than ECONNRESET. Each call is bounded, since PeekAlive
// must never block. A connection already closed locally returns raw.Read's
// error.
func TestPeekAlive(t *testing.T) {
	peek := func(t *testing.T, raw RawConn) (alive bool, err error) {
		t.Helper()
		within(t, "PeekAlive", func() { alive, err = PeekAlive(raw) })
		return alive, err
	}
	rawOf := func(t *testing.T, c *net.TCPConn) RawConn {
		t.Helper()
		raw, err := c.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// readable waits for the peer's FIN, RST, or bytes to reach the socket.
	// poll leaves both the data and a pending socket error in place.
	readable := func(t *testing.T, raw RawConn) {
		t.Helper()
		var n int
		var err error
		_ = raw.Control(func(fd uintptr) {
			n, err = unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, int(callTimeout/time.Millisecond))
		})
		if n != 1 || err != nil {
			t.Fatalf("socket not readable after %v: %d, %v", callTimeout, n, err)
		}
	}
	notAlive := func(t *testing.T, raw RawConn) {
		t.Helper()
		readable(t, raw)
		if alive, err := peek(t, raw); alive || err != nil {
			t.Fatalf("PeekAlive = %v, %v; want false, nil", alive, err)
		}
	}
	// open returns a fresh connection's client end, checked alive while idle.
	open := func(t *testing.T) (*net.TCPConn, *net.TCPConn, RawConn) {
		t.Helper()
		client, server := tcpPair(t)
		raw := rawOf(t, client)
		if alive, err := peek(t, raw); !alive || err != nil {
			t.Fatalf("idle connection: PeekAlive = %v, %v; want true, nil", alive, err)
		}
		return client, server, raw
	}

	t.Run("idle", func(t *testing.T) { open(t) })
	t.Run("peer closed", func(t *testing.T) {
		_, server, raw := open(t)
		server.Close()
		notAlive(t, raw)
	})
	t.Run("peer reset", func(t *testing.T) {
		_, server, raw := open(t)
		if err := server.SetLinger(0); err != nil {
			t.Fatal(err)
		}
		server.Close()
		notAlive(t, raw)
	})
	t.Run("unread bytes", func(t *testing.T) {
		client, server, raw := open(t)
		if _, err := server.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		notAlive(t, raw)
		got := make([]byte, 1)
		_ = client.SetReadDeadline(time.Now().Add(callTimeout))
		if _, err := io.ReadFull(client, got); err != nil || got[0] != 'x' {
			t.Fatalf("read %q, %v after PeekAlive; want the unread byte", got, err)
		}
	})
	t.Run("closed locally", func(t *testing.T) {
		client, _, raw := open(t)
		client.Close()
		if alive, err := peek(t, raw); alive || err == nil {
			t.Fatalf("closed connection: PeekAlive = %v, %v; want false and an error", alive, err)
		}
	})
}
