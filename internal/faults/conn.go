package faults

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Conn is a connection an injector wraps. Its writes stay synchronous and
// never queue bytes: the zero-copy SEND path splices past a Conn straight to
// its socket, so anything a Conn held back would reach the peer out of order.
// Read faults may hold bytes back, since every received byte passes through
// the receiver's Conn.
type Conn struct {
	net.Conn
	inj      *Injector
	key      uint64 // side and connection ordinal
	zeroCopy bool

	reads, writes atomic.Uint64
	broken        atomic.Bool // reset or ended by a fault
	reset         atomic.Bool // reset by a fault

	// readMu serializes faulted reads, which share pending. It is apart
	// from mu, so a read blocked under it never keeps Close or a deadline
	// change from ending it.
	readMu  sync.Mutex
	pending []byte // read bytes a dup fault held back

	mu              sync.Mutex
	readDL, writeDL time.Time
	// dlChanged is closed and replaced whenever a deadline changes, so a
	// pause in progress notices a deadline set after it began.
	dlChanged chan struct{}
	closeOnce sync.Once
	closed    chan struct{}
}

// WrapConn wraps c so the injector can fault it. A nil injector returns c.
func (i *Injector) WrapConn(c net.Conn, side Side) net.Conn {
	if i == nil || c == nil {
		return c
	}
	if side == Server {
		i.counts.serverConns.Add(1)
	} else {
		i.counts.clientConns.Add(1)
	}
	key := uint64(side)<<56 | i.conns[side].Add(1)
	// With write faults on, half the connections give up zero-copy, so
	// those faults reach SEND payloads as well as everything else.
	zeroCopy := !i.writeFaults || mix(i.cfg.Seed, key, 0x7a65726f)&1 == 0
	return &Conn{Conn: c, inj: i, key: key, zeroCopy: zeroCopy, closed: make(chan struct{}), dlChanged: make(chan struct{})}
}

// Unwrap returns the wrapped connection.
func (c *Conn) Unwrap() net.Conn { return c.Conn }

// ZeroCopyConn returns the raw socket a zero-copy SEND may splice into, or nil
// when the wrapped connection is not one or this connection's schedule faults
// its writes.
func (c *Conn) ZeroCopyConn() *net.TCPConn {
	if !c.zeroCopy {
		return nil
	}
	tc, _ := c.Conn.(*net.TCPConn)
	return tc
}

var errNoSyscallConn = errors.New("faults: wrapped connection has no syscall.Conn")

// SyscallConn exposes the wrapped connection's descriptor, which the client's
// liveness check peeks at.
func (c *Conn) SyscallConn() (syscall.RawConn, error) {
	sc, ok := c.Conn.(syscall.Conn)
	if !ok {
		return nil, errNoSyscallConn
	}
	return sc.SyscallConn()
}

// CloseWrite half-closes the wrapped connection, or returns
// errors.ErrUnsupported when it cannot half-close.
func (c *Conn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// Close closes the connection and ends any injected pause on it.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// Reset closes the connection abruptly, as a peer's RST would: the socket
// closes with linger 0, so pending data is discarded.
func (c *Conn) Reset() {
	c.reset.Store(true)
	c.broken.Store(true)
	if tc, ok := c.Conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.setDeadline(&t, &t)
	return c.Conn.SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.setDeadline(&t, nil)
	return c.Conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.setDeadline(nil, &t)
	return c.Conn.SetWriteDeadline(t)
}

func (c *Conn) setDeadline(read, write *time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if read != nil {
		c.readDL = *read
	}
	if write != nil {
		c.writeDL = *write
	}
	close(c.dlChanged)
	c.dlChanged = make(chan struct{})
}

// deadline returns a direction's deadline and a channel closed when any
// deadline next changes.
func (c *Conn) deadline(read bool) (time.Time, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if read {
		return c.readDL, c.dlChanged
	}
	return c.writeDL, c.dlChanged
}

// sleep pauses for d, ending early with an error when the direction's
// deadline passes, even one set during the pause, or the connection closes.
func (c *Conn) sleep(d time.Duration, read bool) error {
	end := time.Now().Add(d)
	for {
		deadline, changed := c.deadline(read)
		wait, expired := time.Until(end), false
		if !deadline.IsZero() && time.Until(deadline) < wait {
			wait, expired = max(time.Until(deadline), 0), true
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
			if expired {
				return errDeadline
			}
			return nil
		case <-c.closed:
			t.Stop()
			return errClosed
		case <-changed:
			t.Stop()
		}
	}
}

func netErr(op string, errno syscall.Errno) error {
	return &net.OpError{Op: op, Net: "tcp", Err: os.NewSyscallError(op, errno)}
}

// pause applies the delay and pause kinds before an operation.
func (c *Conn) pause(op uint64, read bool) error {
	i := c.inj
	op = op<<1 | boolBit(read)
	for _, k := range []Kind{NetDelay, NetPause} {
		if i.fires(k, c.key, op) {
			if err := c.sleep(i.sample(k, c.key, op), read); err != nil {
				return err
			}
		}
	}
	return nil
}

func boolBit(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func (c *Conn) Read(p []byte) (int, error) {
	i := c.inj
	if !i.famOn[FamilyNet] || len(p) == 0 {
		return c.Conn.Read(p)
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	if c.reset.Load() {
		return 0, netErr("read", syscall.ECONNRESET)
	}
	if c.broken.Load() {
		return 0, io.EOF
	}
	op := c.reads.Add(1)
	if err := c.pause(op, true); err != nil {
		return 0, err
	}
	switch {
	case i.fires(NetReset, c.key, op<<1|1):
		c.Reset()
		return 0, netErr("read", syscall.ECONNRESET)
	case i.fires(NetEOF, c.key, op<<1|1):
		c.broken.Store(true)
		return 0, io.EOF
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		n = c.corrupt(p, n, op) // never to zero: drop keeps at least a byte
	}
	return n, err
}

// corrupt applies the broken-TCP kinds to the n bytes just read into p and
// returns how many bytes p now holds. Bytes a dup pushes past p wait in
// pending for the next Read.
func (c *Conn) corrupt(p []byte, n int, op uint64) int {
	i := c.inj
	key := c.key
	if i.fires(NetFlip, key, op) {
		r := i.rng(NetFlip, key, op)
		p[r.IntN(n)] ^= 1 << r.IntN(8)
	}
	if n >= 2 && i.fires(NetDrop, key, op) {
		r := i.rng(NetDrop, key, op)
		a := r.IntN(n - 1)
		l := 1 + r.IntN(n-1-a)
		copy(p[a:], p[a+l:n])
		n -= l
	}
	if n >= 1 && i.fires(NetDup, key, op) {
		r := i.rng(NetDup, key, op)
		a := r.IntN(n)
		b := a + 1 + r.IntN(n-a)
		out := make([]byte, 0, n+b-a)
		out = append(append(append(out, p[:b]...), p[a:b]...), p[b:n]...)
		n = copy(p, out)
		c.pending = append(c.pending, out[n:]...)
	}
	if n >= 2 && i.fires(NetReorder, key, op) {
		r := i.rng(NetReorder, key, op)
		a := r.IntN(n - 1)
		b := a + 1 + r.IntN(n-1-a)
		e := b + 1 + r.IntN(n-b)
		swapped := append(append([]byte{}, p[b:e]...), p[a:b]...)
		copy(p[a:e], swapped)
	}
	return n
}

func (c *Conn) Write(p []byte) (int, error) {
	i := c.inj
	if !i.famOn[FamilyNet] {
		return c.Conn.Write(p)
	}
	if c.broken.Load() {
		return 0, netErr("write", syscall.EPIPE)
	}
	op := c.writes.Add(1)
	if err := c.pause(op, false); err != nil {
		return 0, err
	}
	if i.fires(NetReset, c.key, op<<1) {
		c.Reset()
		return 0, netErr("write", syscall.ECONNRESET)
	}
	if len(p) > 1 && i.fires(NetPartial, c.key, op<<1) {
		k := 1 + i.rng(NetPartial, c.key, op<<1).IntN(len(p)-1)
		n, _ := c.Conn.Write(p[:k])
		c.Reset()
		return n, netErr("write", syscall.EPIPE)
	}
	return c.Conn.Write(p)
}
