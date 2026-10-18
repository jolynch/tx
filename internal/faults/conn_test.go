package faults

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// NET faults: the connection wrapper.
// pipePair returns a wrapped client end and the server end of a pipe, and
// sends data from the server end.
func pipePair(t *testing.T, i *Injector, data []byte) net.Conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	if data != nil {
		go func() {
			_, _ = b.Write(data)
			_ = b.Close()
		}()
	}
	return i.WrapConn(a, Client)
}

func readAll(t *testing.T, c net.Conn) []byte {
	t.Helper()
	got, err := io.ReadAll(c)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return got
}

func TestNetReadCorruption(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	t.Run("flip", func(t *testing.T) {
		got := readAll(t, pipePair(t, one(t, FamilyNet, "flip=1"), data))
		if len(got) != len(data) || bitsDiffer(got, data) != 1 {
			t.Fatalf("flip read %q", got)
		}
	})
	t.Run("drop", func(t *testing.T) {
		got := readAll(t, pipePair(t, one(t, FamilyNet, "drop=1"), data))
		// Removing one contiguous span: a prefix and a suffix of data.
		cut := len(data) - len(got)
		ok := false
		for a := 0; a <= len(got) && cut > 0 && !ok; a++ {
			ok = bytes.Equal(got[:a], data[:a]) && bytes.Equal(got[a:], data[a+cut:])
		}
		if !ok {
			t.Fatalf("drop read %q", got)
		}
	})
	t.Run("dup", func(t *testing.T) {
		isDup := func(got []byte) bool {
			extra := len(got) - len(data)
			for b := 1; b <= len(data) && extra > 0; b++ {
				if a := b - extra; a >= 0 && bytes.Equal(got, slices.Concat(data[:b], data[a:b], data[b:])) {
					return true
				}
			}
			return false
		}
		if got := readAll(t, pipePair(t, one(t, FamilyNet, "dup=1"), data)); !isDup(got) {
			t.Fatalf("dup read %q", got)
		}
		// A read buffer the data fills leaves no room for the duplicate,
		// which waits for the next read instead of being cut off.
		c := pipePair(t, one(t, FamilyNet, "dup=1"), data)
		var got []byte
		for buf := make([]byte, len(data)); ; {
			n, err := c.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if !isDup(got) {
			t.Fatalf("dup read %q through a %d-byte buffer", got, len(data))
		}
	})
	t.Run("reorder", func(t *testing.T) {
		got := readAll(t, pipePair(t, one(t, FamilyNet, "reorder=1"), data))
		sorted := func(b []byte) []byte { c := slices.Clone(b); slices.Sort(c); return c }
		if len(got) != len(data) || bytes.Equal(got, data) || !bytes.Equal(sorted(got), sorted(data)) {
			t.Fatalf("reorder read %q", got)
		}
	})
	t.Run("eof", func(t *testing.T) {
		// Once the stream ends early it stays ended, though data waits
		// behind it and later reads would not all draw the fault.
		c := pipePair(t, one(t, FamilyNet, "eof=0.5"), data)
		for {
			n, err := c.Read(make([]byte, 1))
			if err == io.EOF {
				break
			}
			if err != nil || n != 1 {
				t.Fatalf("read %d, %v before any eof", n, err)
			}
		}
		for range 10 {
			if n, err := c.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("read %d, %v after an early eof", n, err)
			}
		}
		// The connection is broken both ways, as when the peer has gone.
		// The deadline bounds a write the peer, blocked on its own unread
		// write, would never take.
		_ = c.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := c.Write([]byte("x")); !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("write after an early eof: %v", err)
		}
	})
}

func TestNetConnFaults(t *testing.T) {
	t.Run("reset", func(t *testing.T) {
		c := pipePair(t, one(t, FamilyNet, "reset=1"), []byte("data"))
		if _, err := c.Read(make([]byte, 4)); !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("read with reset=1: %v", err)
		}
		c = pipePair(t, one(t, FamilyNet, "reset=1"), nil)
		if _, err := c.Write([]byte("x")); !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("write after reset: %v", err)
		}
		// Once reset, reads keep failing as reset rather than ending cleanly.
		if _, err := c.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("read on a reset connection: %v", err)
		}
	})
	t.Run("partial", func(t *testing.T) {
		a, b := net.Pipe()
		defer b.Close()
		c := one(t, FamilyNet, "partial=1").WrapConn(a, Client)
		got := make(chan []byte, 1)
		go func() { d, _ := io.ReadAll(b); got <- d }()
		n, err := c.Write([]byte("0123456789"))
		if n <= 0 || n >= 10 || !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("partial write %d, %v", n, err)
		}
		if d := <-got; string(d) != "0123456789"[:n] {
			t.Fatalf("peer received %q after a %d-byte partial write", d, n)
		}
	})
	t.Run("pause honors deadlines and close", func(t *testing.T) {
		c := pipePair(t, one(t, FamilyNet, "pause=1:β(1s,2s,3s,4s)"), nil)
		_ = c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		start := time.Now()
		if _, err := c.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("paused read returned %v after %v", err, time.Since(start))
		}
		// A deadline set during the pause ends it too, as the client's
		// cancellation does with SetDeadline(now).
		_ = c.SetReadDeadline(time.Time{})
		done := make(chan error, 1)
		go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
		time.Sleep(10 * time.Millisecond)
		_ = c.SetDeadline(time.Now())
		select {
		case err := <-done:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("paused read ended with %v after a new deadline", err)
			}
		case <-time.After(time.Second):
			t.Fatal("a deadline set during the pause did not end it")
		}
		_ = c.SetDeadline(time.Time{})
		go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
		time.Sleep(10 * time.Millisecond)
		_ = c.Close()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("paused read succeeded after close")
			}
		case <-time.After(time.Second):
			t.Fatal("close did not end the pause")
		}
	})
	t.Run("delay", func(t *testing.T) {
		c := pipePair(t, one(t, FamilyNet, "delay=1:β(20ms,25ms,30ms,40ms)"), []byte("x"))
		start := time.Now()
		if _, err := c.Read(make([]byte, 1)); err != nil || time.Since(start) < 20*time.Millisecond {
			t.Fatalf("delayed read %v after %v", err, time.Since(start))
		}
		a, b := net.Pipe()
		defer b.Close()
		go func() { _, _ = io.Copy(io.Discard, b) }()
		w := one(t, FamilyNet, "delay=1:β(20ms,25ms,30ms,40ms)").WrapConn(a, Client)
		defer w.Close()
		start = time.Now()
		if _, err := w.Write([]byte("x")); err != nil || time.Since(start) < 20*time.Millisecond {
			t.Fatalf("delayed write %v after %v", err, time.Since(start))
		}
	})
	t.Run("pause on write honors its deadline", func(t *testing.T) {
		a, b := net.Pipe()
		defer b.Close()
		go func() { _, _ = io.Copy(io.Discard, b) }()
		w := one(t, FamilyNet, "pause=1:β(1s,2s,3s,4s)").WrapConn(a, Client)
		defer w.Close()
		_ = w.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
		start := time.Now()
		if _, err := w.Write([]byte("x")); !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("paused write returned %v after %v", err, time.Since(start))
		}
	})
	t.Run("sides have their own schedules", func(t *testing.T) {
		// Client and server connection N must not share decisions, or a
		// fault on one end would always come with the same fault on the
		// other.
		i := one(t, FamilyNet, "reset=0.5")
		var client, server []bool
		for range 32 {
			a, b := net.Pipe()
			c, s := i.WrapConn(a, Client).(*Conn), i.WrapConn(b, Server).(*Conn)
			client = append(client, i.fires(NetReset, c.key, 1))
			server = append(server, i.fires(NetReset, s.key, 1))
			_, _ = a.Close(), b.Close()
		}
		if slices.Equal(client, server) {
			t.Fatal("client and server connections drew the same faults")
		}
	})
	t.Run("ZeroCopyConn follows the schedule", func(t *testing.T) {
		// With write faults on, a connection that gives up zero-copy must
		// not hand SEND its socket; the others must.
		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		i := one(t, FamilyNet, "reset=0.001")
		var kept, gave int
		for range 64 {
			c := i.WrapConn(client, Server).(*Conn)
			switch got := c.ZeroCopyConn(); {
			case c.zeroCopy && got == client:
				kept++
			case !c.zeroCopy && got == nil:
				gave++
			default:
				t.Fatalf("ZeroCopyConn returned %v for a connection whose zero-copy choice is %t", got, c.zeroCopy)
			}
		}
		if kept == 0 || gave == 0 {
			t.Fatalf("kept %d, gave up %d of 64; want both", kept, gave)
		}
	})
	t.Run("zero-copy share", func(t *testing.T) {
		// Every write fault costs half the connections their zero-copy path.
		for _, spec := range []string{"reset=0.001", "partial=0.001", "delay=0.001:β(1us,2us,3us,4us)", "pause=0.001:β(1us,2us,3us,4us)"} {
			i := one(t, FamilyNet, spec)
			kept := 0
			for range 200 {
				a, b := net.Pipe()
				if i.WrapConn(a, Server).(*Conn).zeroCopy {
					kept++
				}
				a.Close()
				b.Close()
			}
			if kept < 60 || kept > 140 {
				t.Fatalf("%s: %d of 200 connections kept zero-copy, want about half", spec, kept)
			}
		}
		quiet := one(t, FamilyNet, "eof=0.5")
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		if !quiet.WrapConn(a, Server).(*Conn).zeroCopy {
			t.Fatal("read faults alone cost a connection zero-copy")
		}
	})
	t.Run("dial and accept", func(t *testing.T) {
		if err := one(t, FamilyNet, "dial.fail=1").DialErr(context.Background()); !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("dial.fail: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := one(t, FamilyNet, "dial.slow=1:β(1s,2s,3s,4s)").DialErr(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("dial.slow with a cancelled context: %v", err)
		}
		var ne net.Error
		// Accept loops that check Temporary, deprecated as it is, must
		// retry the error rather than stop.
		if err := one(t, FamilyNet, "accept.fail=1").AcceptErr(); !errors.As(err, &ne) || !ne.Temporary() || !errors.Is(err, syscall.ECONNABORTED) {
			t.Fatalf("accept.fail returned %v", err)
		}
	})
}

// TestConcurrentFaultedReads checks two goroutines may read one faulted
// connection at once, as net.Conn allows: together they receive every byte
// the peer sent and every duplicate dup made, with no race on the bytes a
// dup holds back.
func TestConcurrentFaultedReads(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 256)
	inj := one(t, FamilyNet, "dup=1")
	c := pipePair(t, inj, data)
	var total atomic.Int64
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			buf := make([]byte, 16)
			for {
				n, err := c.Read(buf)
				total.Add(int64(n))
				if err != nil {
					return
				}
			}
		})
	}
	wg.Wait()
	if got := total.Load(); got <= int64(len(data)) {
		t.Fatalf("read %d bytes of %d sent with every read duplicating a span", got, len(data))
	}
}
