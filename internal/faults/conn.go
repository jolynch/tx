package faults

import (
	"errors"
	"net"
	"syscall"
)

// Conn is a connection an injector wraps. Its writes stay synchronous and
// never queue bytes: the zero-copy SEND path splices past a Conn straight to
// its socket, so anything a Conn held back would reach the peer out of order.
type Conn struct {
	net.Conn
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
	return &Conn{Conn: c}
}

// Unwrap returns the wrapped connection.
func (c *Conn) Unwrap() net.Conn { return c.Conn }

// ZeroCopyConn returns the raw socket a zero-copy SEND may splice into, or nil
// when the wrapped connection is not one.
func (c *Conn) ZeroCopyConn() *net.TCPConn {
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
