// Package events carries tx's event timeline: a nil-safe Sink that client
// and server code emit into, and the JSON-lines encoding that the --stats
// writers and the "events" progress format consume. A nil *Sink is the
// disabled state; emitting into it costs one nil check, so call sites that
// would allocate fields guard with Enabled first. See docs/bench/TRACE.md.
package events

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Event is one timeline record. Fields keep their emission order so encoded
// records are stable and readable.
type Event struct {
	T      time.Time
	Name   string
	TID    string
	Fields []Field
}

// Field is one event-specific key/value.
type Field struct {
	Key string
	Val any
}

// F builds a Field.
func F(key string, val any) Field { return Field{Key: key, Val: val} }

// Dur encodes a duration as integer nanoseconds, the unit every dur field uses.
func Dur(key string, d time.Duration) Field { return Field{Key: key, Val: int64(d)} }

// Get returns the value of key, if present.
func (e Event) Get(key string) (any, bool) {
	for _, f := range e.Fields {
		if f.Key == key {
			return f.Val, true
		}
	}
	return nil, false
}

// Int returns an integer field, or 0.
func (e Event) Int(key string) int64 {
	v, _ := e.Get(key)
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case uint64:
		return int64(n)
	case uint32:
		return int64(n)
	case time.Duration:
		return int64(n)
	}
	return 0
}

// Str returns a string field, or "".
func (e Event) Str(key string) string {
	v, _ := e.Get(key)
	s, _ := v.(string)
	return s
}

// Sink fans events out to subscribers. Subscribers run synchronously on the
// emitting goroutine and must not block or call back into the emitter.
type Sink struct {
	mu   sync.RWMutex
	subs []func(Event)
	conn atomic.Uint64
}

// NewSink returns an enabled sink with no subscribers.
func NewSink() *Sink { return &Sink{} }

// Enabled reports whether emitting does anything.
func (s *Sink) Enabled() bool { return s != nil }

// Subscribe adds fn to every later Emit.
func (s *Sink) Subscribe(fn func(Event)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.subs = append(s.subs, fn)
	s.mu.Unlock()
}

// Emit stamps and delivers an event.
func (s *Sink) Emit(name, tid string, fields ...Field) {
	if s == nil {
		return
	}
	s.Deliver(Event{T: time.Now(), Name: name, TID: tid, Fields: fields})
}

// Deliver hands a fully formed event to the subscribers, stamping it if its
// time is zero.
func (s *Sink) Deliver(e Event) {
	if s == nil {
		return
	}
	if e.T.IsZero() {
		e.T = time.Now()
	}
	s.mu.RLock()
	subs := s.subs
	s.mu.RUnlock()
	for _, fn := range subs {
		fn(e)
	}
}

// NextConn allocates a connection number for conn_* events.
func (s *Sink) NextConn() uint64 {
	if s == nil {
		return 0
	}
	return s.conn.Add(1)
}

type ctxKey struct{}

// Scope is a sink bound to one connection, carried through a context so
// deep call sites can emit without threading the sink through every
// signature.
type Scope struct {
	Sink *Sink
	Conn uint64
}

// WithScope returns ctx carrying scope.
func WithScope(ctx context.Context, scope Scope) context.Context {
	if scope.Sink == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, scope)
}

// FromContext returns the scope ctx carries; its Sink is nil when none.
func FromContext(ctx context.Context) Scope {
	if ctx == nil {
		return Scope{}
	}
	s, _ := ctx.Value(ctxKey{}).(Scope)
	return s
}

// Header is the fixed part of every encoded record.
type Header struct {
	Side string // "c" or "s"
	Run  string // tx-bench run label; empty omits the key
}

// AppendJSON encodes e as one JSON object followed by a newline:
// {"t":..,"side":..,["run":..,]"tid":..,"ev":..,<fields>}. t is unix
// nanoseconds; an empty tid encodes as "-".
func AppendJSON(buf []byte, h Header, e Event) []byte {
	buf = append(buf, `{"t":`...)
	buf = strconv.AppendInt(buf, e.T.UnixNano(), 10)
	buf = append(buf, `,"side":`...)
	buf = appendString(buf, h.Side)
	if h.Run != "" {
		buf = append(buf, `,"run":`...)
		buf = appendString(buf, h.Run)
	}
	tid := e.TID
	if tid == "" {
		tid = "-"
	}
	buf = append(buf, `,"tid":`...)
	buf = appendString(buf, tid)
	buf = append(buf, `,"ev":`...)
	buf = appendString(buf, e.Name)
	for _, f := range e.Fields {
		buf = append(buf, ',')
		buf = appendString(buf, f.Key)
		buf = append(buf, ':')
		buf = appendValue(buf, f.Val)
	}
	return append(buf, '}', '\n')
}

// appendString appends s as a JSON string, escaping as encoding/json does
// (invalid UTF-8 becomes U+FFFD).
func appendString(buf []byte, s string) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				buf = append(buf, '\\', c)
			case c == '\n':
				buf = append(buf, '\\', 'n')
			case c == '\r':
				buf = append(buf, '\\', 'r')
			case c == '\t':
				buf = append(buf, '\\', 't')
			case c < 0x20 || c == '<' || c == '>' || c == '&':
				buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			default:
				buf = append(buf, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, `\ufffd`...)
		} else if r == '\u2028' || r == '\u2029' {
			buf = append(buf, '\\', 'u', '2', '0', '2', hex[r&0xf])
		} else {
			buf = append(buf, s[i:i+size]...)
		}
		i += size
	}
	return append(buf, '"')
}

func appendValue(buf []byte, v any) []byte {
	switch x := v.(type) {
	case string:
		return appendString(buf, x)
	case int:
		return strconv.AppendInt(buf, int64(x), 10)
	case int64:
		return strconv.AppendInt(buf, x, 10)
	case uint64:
		return strconv.AppendUint(buf, x, 10)
	case uint32:
		return strconv.AppendUint(buf, uint64(x), 10)
	case time.Duration:
		return strconv.AppendInt(buf, int64(x), 10)
	case bool:
		return strconv.AppendBool(buf, x)
	case float64:
		return strconv.AppendFloat(buf, x, 'g', -1, 64)
	case nil:
		return append(buf, "null"...)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return append(buf, "null"...)
	}
	return append(buf, raw...)
}
