package tx

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"

	"github.com/jolynch/tx/internal/events"
)

// ClientEvent is one record in a client's event timeline.
type ClientEvent = events.Event

// ClientEventSink delivers client events to subscribed callbacks.
type ClientEventSink = events.Sink

// NewClientEventSink returns an enabled sink ready for subscriptions.
func NewClientEventSink() *ClientEventSink { return events.NewSink() }

// WithEventSink makes the client emit its event timeline (connections,
// requests, files, windows, ACKs, retries) into s; the "events" progress
// format consumes it. A nil s, the default, emits nothing.
func WithEventSink(s *ClientEventSink) ClientOption {
	return clientOptionFunc(func(c *Client) { c.eventSink = s })
}

// sink returns the event sink, nil when events are off.
func (c *Client) sink() *events.Sink {
	if c == nil {
		return nil
	}
	return c.eventSink
}

// syncDialKey marks a dial made because the warm pool was empty.
type syncDialKey struct{}

// rawConn unwraps the pool's session wrapper so a connection keeps one id.
func rawConn(conn net.Conn) net.Conn {
	if sc, ok := conn.(*sessionTCPConn); ok {
		return sc.Conn
	}
	return conn
}

// noteDial numbers a new connection and emits conn_dial.
func (c *Client) noteDial(ctx context.Context, conn net.Conn, start time.Time) {
	s := c.sink()
	if !s.Enabled() {
		return
	}
	id := s.NextConn()
	c.connIDs.Store(conn, id)
	s.Emit("conn_dial", "", events.F("conn", id), events.Dur("dur", time.Since(start)),
		events.F("sync", ctx.Value(syncDialKey{}) != nil))
}

// connID returns a connection's number, or 0 when events are off.
func (c *Client) connID(conn net.Conn) uint64 {
	if !c.sink().Enabled() || conn == nil {
		return 0
	}
	id, _ := c.connIDs.Load(rawConn(conn))
	n, _ := id.(uint64)
	return n
}

func (c *Client) noteConnEvent(ev string, conn net.Conn, reason string) {
	s := c.sink()
	if !s.Enabled() || conn == nil {
		return
	}
	id := c.connID(conn)
	if ev == "conn_close" {
		c.connIDs.Delete(rawConn(conn))
		s.Emit(ev, "", events.F("conn", id), events.F("reason", reason))
		return
	}
	s.Emit(ev, "", events.F("conn", id))
}

// reqTrace follows one SEND, ACK, or CXSUM from req_start to req_end. The
// caller that will consume the response puts it in the context; the request
// function fills in the connection and emits req_start.
type reqTrace struct {
	verb  string
	tid   string
	start time.Time
	conn  uint64
	// lastByte is when the previous window of this request finished, for
	// first_byte.
	lastByte time.Time
}

type reqTraceKey struct{}

func withReqTrace(ctx context.Context, rt *reqTrace) context.Context {
	if rt == nil {
		return ctx
	}
	return context.WithValue(ctx, reqTraceKey{}, rt)
}

func reqTraceFrom(ctx context.Context) *reqTrace {
	rt, _ := ctx.Value(reqTraceKey{}).(*reqTrace)
	return rt
}

// beginReq starts a request trace when events are on.
func (c *Client) beginReq(verb, tid string) *reqTrace {
	if !c.sink().Enabled() {
		return nil
	}
	now := time.Now()
	return &reqTrace{verb: verb, tid: tid, start: now, lastByte: now}
}

// emitReqStart records the connection a traced request went out on.
// Requests without a trace in ctx (no consumer will emit req_end) are not
// reported.
func (c *Client) emitReqStart(ctx context.Context, verb, tid string, conn net.Conn, body []byte) {
	rt := reqTraceFrom(ctx)
	if rt == nil {
		return
	}
	id := c.connID(conn)
	rt.conn = id
	c.sink().Emit("req_start", tid, events.F("verb", verb), events.F("conn", id),
		events.F("items", bytes.Count(body, []byte{'\n'})), events.F("body_bytes", len(body)))
}

// end emits req_end for a traced request.
func (rt *reqTrace) end(c *Client, err error) {
	if rt == nil {
		return
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	c.sink().Emit("req_end", rt.tid, events.F("verb", rt.verb), events.F("conn", rt.conn),
		events.Dur("dur", time.Since(rt.start)), events.F("err", msg))
}

// window emits one received window: arrival is when its header arrived, so
// t - dur is the arrival on the client's clock, and first_byte is how long
// the client waited for that header after the request started or the
// previous window finished.
func (rt *reqTrace) window(c *Client, meta FileFrameMeta, arrival time.Time, writeDur time.Duration) {
	if rt == nil {
		return
	}
	now := time.Now()
	c.sink().Emit("window", rt.tid,
		events.F("file", meta.FileID), events.F("off", meta.Offset), events.F("len", meta.Size),
		events.F("wire", meta.WireSize), events.F("codec", meta.Comp), events.F("server_ts_ms", meta.HeaderTS),
		events.Dur("first_byte", arrival.Sub(rt.lastByte)), events.Dur("dur", now.Sub(arrival)),
		events.Dur("write_dur", writeDur), events.F("conn", rt.conn))
	rt.lastByte = now
}

// timedWriter accumulates time spent in Write.
type timedWriter struct {
	w io.Writer
	d time.Duration
}

func (t *timedWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := t.w.Write(p)
	t.d += time.Since(start)
	return n, err
}

// take returns and resets the accumulated time.
func (t *timedWriter) take() time.Duration {
	d := t.d
	t.d = 0
	return d
}
