package ftcp

import (
	"bufio"
	"io"
	"net"
	"runtime"
	"sync"

	"github.com/jolynch/tx/internal/faults"
	"github.com/zeebo/xxh3"
)

// sendOutput is where one SEND item writes its frames. Frames go to w, which
// is a buffer shared by the whole response or, unbuffered, the response
// writer itself. A zero-copy frame splices its payload straight to conn, so
// it flushes the buffer first and writes its header and trailer to conn too;
// nothing can then reach the socket ahead of bytes buffered before it.
type sendOutput struct {
	w    io.Writer
	buf  *bufio.Writer // nil when unbuffered
	conn *net.TCPConn  // the response as a raw socket, or nil (AEAD, limiters)
	// frameFlush, when set, sends a completed frame through a transport that
	// batches writes; gentle responses use it so frames keep their pacing.
	frameFlush func() error
	// recoveryFlush, when set, sends a fast response's buffered frames to
	// the socket; frames measured by a recovery check use it.
	recoveryFlush func() error
	// hasher is reset and reused by each item, rather than allocating 1.2 KiB
	// per file.
	hasher xxh3.Hasher128
	// faults gates the request's file and syscall sites; nil disables it.
	faults *faults.Injector
	// rate turns measured writes into the write times the compression
	// policy sees; it points at ownRate unless the response shares one
	// across outputs.
	rate    *sendRate
	ownRate sendRate
}

var sendOutputBufferPool = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, sendOutputBufferBytes) },
}

// newBufferedSendOutput takes a pooled buffer; release returns it.
func newBufferedSendOutput(out io.Writer) *sendOutput {
	buf := sendOutputBufferPool.Get().(*bufio.Writer)
	buf.Reset(out)
	o := &sendOutput{w: buf, buf: buf, conn: extractTCPConn(out)}
	o.rate = &o.ownRate
	return o
}

func unbufferedSendOutput(out io.Writer) *sendOutput {
	o := &sendOutput{w: out, conn: extractTCPConn(out)}
	o.rate = &o.ownRate
	return o
}

// Flush writes any buffered frames to the response.
func (o *sendOutput) Flush() error {
	if o.buf == nil {
		return nil
	}
	return o.buf.Flush()
}

// release returns a buffered output's buffer to the pool, discarding
// anything not flushed. The output must not be used afterwards.
func (o *sendOutput) release() {
	if o.buf == nil {
		return
	}
	o.buf.Reset(nil)
	sendOutputBufferPool.Put(o.buf)
	o.w, o.buf = nil, nil
}

// canZeroCopy reports whether a frame can use the tee+splice zero-copy send
// path, which avoids copying payload bytes into userspace for the network
// write. The path requires all of:
//   - Linux (tee/splice are Linux-only syscalls)
//   - No compression (compressed data must be assembled in userspace)
//   - No O_DIRECT (direct I/O requires aligned userspace buffers, not pipes)
//   - A raw TCP socket (no encryption or rate-limit wrapper over the conn)
//   - No fault injection that must see the payload (read.flip)
func canZeroCopy(args frameStreamArgs) bool {
	return runtime.GOOS == "linux" &&
		args.Comp == "none" &&
		!args.DirectIO &&
		args.OutputTCPConn != nil &&
		args.Faults.ZeroCopyReads()
}

// extractTCPConn returns the raw socket a SEND response may splice into, or
// nil. The exact-type check comes first, so a raw socket pays nothing for the
// fault-injection case after it; other writers pay one more type check per
// SEND output.
func extractTCPConn(w io.Writer) *net.TCPConn {
	if tc, ok := w.(*net.TCPConn); ok {
		return tc
	}
	if zc, ok := w.(zeroCopyConn); ok {
		return zc.ZeroCopyConn()
	}
	return nil
}

// zeroCopyConn is a wrapped connection, such as a faults.Conn, that may still
// let a SEND splice into its socket.
type zeroCopyConn interface {
	ZeroCopyConn() *net.TCPConn
}

// rawTCPConn returns the TCP socket under conn, looking through one
// fault-injection wrapper, or nil for other transports. Socket options,
// half-close, and TCP_INFO go to the socket itself, whatever the wrapper's
// fault schedule; data never does. extractTCPConn, by contrast, asks the
// wrapper whether a SEND may splice past it.
func rawTCPConn(conn net.Conn) *net.TCPConn {
	if tc, ok := conn.(*net.TCPConn); ok {
		return tc
	}
	if fc, ok := conn.(*faults.Conn); ok {
		tc, _ := fc.Unwrap().(*net.TCPConn)
		return tc
	}
	return nil
}
