package aead

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/flynn/noise"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/sys/unix"
)

// bufConn is an in-memory net.Conn: writes append to a buffer that a reader
// can drain afterwards. Deadlines are no-ops.
type bufConn struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	writes int
	reads  int
	closed bool
}

func (c *bufConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.buf.Len() == 0 {
		return 0, io.EOF
	}
	c.reads++
	return c.buf.Read(p)
}

func (c *bufConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	c.writes++
	return c.buf.Write(p)
}

func (c *bufConn) Close() error                     { c.mu.Lock(); c.closed = true; c.mu.Unlock(); return nil }
func (c *bufConn) LocalAddr() net.Addr              { return nil }
func (c *bufConn) RemoteAddr() net.Addr             { return nil }
func (c *bufConn) SetDeadline(time.Time) error      { return nil }
func (c *bufConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bufConn) SetWriteDeadline(time.Time) error { return nil }

// sessionPair returns a writer session and a reader session joined by one
// in-memory buffer, keyed by an in-memory NN handshake.
func sessionPair(tb testing.TB, alg Algorithm) (*Session, *Session, *bufConn) {
	tb.Helper()
	i1, i2, r1, r2 := nnCipherStates(tb, alg)
	wire := &bufConn{}
	w, err := newSession(wire, bufio.NewReader(wire), alg, i1, i2)
	if err != nil {
		tb.Fatal(err)
	}
	r, err := newSession(&bufConn{}, bufio.NewReader(wire), alg, r2, r1)
	if err != nil {
		tb.Fatal(err)
	}
	return w, r, wire
}

// nnCipherStates runs an in-memory NN handshake and returns the initiator's
// send and receive states, then the responder's.
func nnCipherStates(tb testing.TB, alg Algorithm) (i1, i2, r1, r2 *noise.CipherState) {
	tb.Helper()
	cs, err := cipherSuite(alg)
	if err != nil {
		tb.Fatal(err)
	}
	hi, _ := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Random: rand.Reader, Pattern: noise.HandshakeNN, Initiator: true})
	hr, _ := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Random: rand.Reader, Pattern: noise.HandshakeNN})
	m1, _, _, _ := hi.WriteMessage(nil, nil)
	if _, _, _, err := hr.ReadMessage(nil, m1); err != nil {
		tb.Fatal(err)
	}
	m2, r1, r2, _ := hr.WriteMessage(nil, nil)
	_, i1, i2, err = hi.ReadMessage(nil, m2)
	if err != nil {
		tb.Fatal(err)
	}
	return i1, i2, r1, r2
}

func readMessage(tb testing.TB, s *Session) ([]byte, error) {
	tb.Helper()
	mr, err := s.Reader()
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(mr)
	got, err := io.ReadAll(br)
	if err != nil {
		return got, err
	}
	return got, FinishMessage(br)
}

func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), uint16(3), uint16(0), uint8(0))
	f.Add(bytes.Repeat([]byte{7}, 200000), uint16(4096), uint16(70000%65536), uint8(1))
	f.Add([]byte{}, uint16(1), uint16(0), uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint16, flushAt uint16, flags uint8) {
		// Bound each execution: at most 1 MiB in at most 64Ki writes, so a
		// grown input cannot stall the fuzzer with millions of tiny writes.
		if len(data) > 1<<20 {
			data = data[:1<<20]
		}
		alg := testAlgorithms[int(flags)%2]
		w, r, _ := sessionPair(t, alg)
		mw := w.Writer(flags&2 != 0)
		step := max(1, int(chunk), len(data)>>16)
		for off := 0; off < len(data); off += step {
			end := min(len(data), off+step)
			if _, err := mw.Write(data[off:end]); err != nil {
				t.Fatal(err)
			}
			if flushAt != 0 && off <= int(flushAt) && int(flushAt) < end {
				if err := mw.Flush(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if flags&4 != 0 {
			if err := mw.Flush(); err != nil { // an empty flush emits nothing
				t.Fatal(err)
			}
		}
		if err := mw.End(); err != nil {
			t.Fatal(err)
		}
		got, err := readMessage(t, r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("round trip changed %d bytes into %d", len(data), len(got))
		}
	})
}

func TestRecordSizesAndBatching(t *testing.T) {
	w, r, wire := sessionPair(t, AlgorithmAES)
	data := make([]byte, 5*maxEmitPayload+10)
	rand.Read(data)
	mw := w.Writer(false)
	if _, err := mw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := mw.End(); err != nil {
		t.Fatal(err)
	}
	// Five full records and a final END carrying 10 bytes.
	if want := 5*recordWireSize + recordPrefixSize + 1 + 10 + aeadTagSize; wire.buf.Len() != want {
		t.Fatalf("wire bytes = %d, want %d", wire.buf.Len(), want)
	}
	if wire.writes != 2 {
		t.Fatalf("writes = %d, want 2 (one batch of four records, then the rest)", wire.writes)
	}
	if got, err := readMessage(t, r); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, err %v", len(got), err)
	}

	// A paced message writes every record as soon as it is sealed.
	wire.writes = 0
	mw = w.Writer(true)
	if _, err := mw.Write(data[:3*maxEmitPayload]); err != nil {
		t.Fatal(err)
	}
	if wire.writes != 3 {
		t.Fatalf("paced writes = %d, want 3", wire.writes)
	}
	_ = mw.End()
	if _, err := readMessage(t, r); err != nil {
		t.Fatal(err)
	}

	// Flush seals a short record without ending the message.
	mw = w.Writer(false)
	_, _ = mw.Write([]byte("progress"))
	if err := mw.Flush(); err != nil {
		t.Fatal(err)
	}
	mr, _ := r.Reader()
	buf := make([]byte, 64)
	n, err := mr.Read(buf)
	if err != nil || string(buf[:n]) != "progress" {
		t.Fatalf("flushed read = (%q, %v)", buf[:n], err)
	}
	_ = mw.End()
	if n, err := mr.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("after END read = (%d, %v), want EOF", n, err)
	}
}

// With bytes waiting, the reader takes one receive per full record: each
// read brings a record's body and the next header, and the next record never
// arrives partly, so nothing is moved within the buffer.
func TestReadAheadOneReceivePerRecord(t *testing.T) {
	for _, rsize := range []int{64 << 10, 4 << 10} { // direct open, and in place
		w, r, wire := sessionPair(t, AlgorithmAES)
		const records = 16
		data := make([]byte, records*maxEmitPayload)
		for i := range data {
			data[i] = byte(i)
		}
		mw := w.Writer(false)
		_, _ = mw.Write(data)
		_ = mw.End()
		mr, err := r.Reader()
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 0, len(data))
		buf := make([]byte, rsize)
		for {
			n, err := mr.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("read %d bytes back, want %d", len(got), len(data))
		}
		// 16 full records plus the END record.
		if wire.reads > records+2 {
			t.Fatalf("read buffer %d: %d receives for %d records", rsize, wire.reads, records+1)
		}
		if r.Buffered() != 0 {
			t.Fatalf("Buffered = %d after the message", r.Buffered())
		}
	}
}

// A reader waiting for a message holds no record buffer, so idle
// connections cost only their small bufio buffer.
func TestIdleReaderHoldsNoBuffer(t *testing.T) {
	_, r, _ := sessionPair(t, AlgorithmAES)
	held := true
	// The connection's Read runs on the reader's goroutine, while it waits.
	r.br = bufio.NewReader(readFunc(func([]byte) (int, error) {
		held = r.rbuf != nil
		return 0, io.EOF
	}))
	mr, err := r.Reader()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mr.Read(make([]byte, 16)); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Read = %v, want ErrPeerClosed", err)
	}
	if held {
		t.Fatal("a reader waiting on an idle connection held a record buffer")
	}
}

type readFunc func([]byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

// Bytes read ahead past one message's END stay buffered for the next: a
// client may send FINISH and its first command back to back.
func TestReadAheadKeepsTheNextMessage(t *testing.T) {
	w, r, _ := sessionPair(t, AlgorithmChaCha20)
	for _, m := range []string{"first", "second"} {
		mw := w.Writer(false)
		_, _ = mw.Write([]byte(m))
		_ = mw.End()
	}
	if got, err := readMessage(t, r); err != nil || string(got) != "first" {
		t.Fatalf("first = (%q, %v)", got, err)
	}
	if r.Buffered() == 0 {
		t.Fatal("the second message was not read ahead")
	}
	if got, err := readMessage(t, r); err != nil || string(got) != "second" {
		t.Fatalf("second = (%q, %v)", got, err)
	}
	if r.Buffered() != 0 || r.rbuf != nil {
		t.Fatalf("Buffered = %d, buffer held = %v after the last message", r.Buffered(), r.rbuf != nil)
	}
}

// A message ends once: a second End, or a Write or Flush after it, is an
// error rather than a phantom message, until Writer starts the next one.
func TestMessageEndsOnce(t *testing.T) {
	w, r, _ := sessionPair(t, AlgorithmAES)
	mw := w.Writer(false)
	_, _ = mw.Write([]byte("hello"))
	if err := mw.End(); err != nil {
		t.Fatal(err)
	}
	if err := mw.End(); !errors.Is(err, errMessageEnded) {
		t.Fatalf("second End = %v", err)
	}
	if _, err := mw.Write([]byte("x")); !errors.Is(err, errMessageEnded) {
		t.Fatalf("Write after End = %v", err)
	}
	if err := mw.Flush(); !errors.Is(err, errMessageEnded) {
		t.Fatalf("Flush after End = %v", err)
	}
	mw = w.Writer(false)
	_, _ = mw.Write([]byte("next"))
	if err := mw.End(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"hello", "next"} {
		if got, err := readMessage(t, r); err != nil || string(got) != want {
			t.Fatalf("message = (%q, %v), want %q", got, err, want)
		}
	}
}

func TestRekeyBoundary(t *testing.T) {
	w, r, _ := sessionPair(t, AlgorithmChaCha20)
	// Each one-byte message is one END record; cross the boundary twice.
	// Both directions must switch keys exactly at each multiple of
	// rekeyInterval, FINISH included, and at no other record.
	sendKey, recvKey := w.send.aead, r.recv.aead
	rekeys := 0
	for i := range 2*rekeyInterval + 3 {
		mw := w.Writer(false)
		_, _ = mw.Write([]byte{byte(i)})
		if err := mw.End(); err != nil {
			t.Fatal(err)
		}
		got, err := readMessage(t, r)
		if err != nil || len(got) != 1 || got[0] != byte(i) {
			t.Fatalf("message %d = (%v, %v)", i, got, err)
		}
		boundary := w.send.n%rekeyInterval == 0
		if changed := w.send.aead != sendKey; changed != boundary {
			t.Fatalf("after record %d: send key changed=%v, want %v", w.send.n, changed, boundary)
		}
		if changed := r.recv.aead != recvKey; changed != boundary {
			t.Fatalf("after record %d: receive key changed=%v, want %v", r.recv.n, changed, boundary)
		}
		if boundary {
			rekeys++
		}
		sendKey, recvKey = w.send.aead, r.recv.aead
	}
	if w.send.n != 2*rekeyInterval+3 || r.recv.n != w.send.n || rekeys != 2 {
		t.Fatalf("records sent %d, received %d, rekeys %d", w.send.n, r.recv.n, rekeys)
	}
}

// writeRawRecord seals one record of any kind, bypassing the writer's rules.
func writeRawRecord(t *testing.T, s *Session, kind byte, payload []byte) {
	t.Helper()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.openRecordLocked(); err != nil {
		t.Fatal(err)
	}
	copy(s.wbuf[s.rec+recordPrefixSize+1:], payload)
	s.pending = len(payload)
	if err := s.sealRecordLocked(kind); err != nil {
		t.Fatal(err)
	}
	if err := s.writeQueuedLocked(); err != nil {
		t.Fatal(err)
	}
}

func TestUnexpectedRecordsAreFatal(t *testing.T) {
	for name, rec := range map[string]struct {
		kind    byte
		payload []byte
	}{
		"empty DATA":   {recordData, nil},
		"FINISH":       {recordFinish, nil},
		"unknown kind": {3, []byte("x")},
		"kind 255":     {255, nil},
	} {
		t.Run(name, func(t *testing.T) {
			w, r, _ := sessionPair(t, AlgorithmAES)
			writeRawRecord(t, w, rec.kind, rec.payload)
			if _, err := readMessage(t, r); err == nil {
				t.Fatal("read succeeded")
			}
			if !r.Failed() {
				t.Fatal("session not poisoned")
			}
			if _, err := r.Reader(); err == nil {
				t.Fatal("poisoned session started a message")
			}
		})
	}
}

func TestCorruptAndTruncatedRecords(t *testing.T) {
	// Each case gets the wire bytes of two messages, "first" then "second",
	// each one END record, and returns what the reader receives.
	two := func(t *testing.T, w *Session, wire *bufConn) (rec1, rec2 []byte) {
		for _, m := range []string{"first", "second"} {
			mw := w.Writer(false)
			_, _ = mw.Write([]byte(m))
			_ = mw.End()
		}
		raw := append([]byte(nil), wire.buf.Bytes()...)
		wire.buf.Reset()
		n := recordPrefixSize + int(binary.BigEndian.Uint16(raw)) + 1
		return raw[:n], raw[n:]
	}
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	cases := map[string]func(rec1, rec2 []byte) []byte{
		"flipped tag":      func(a, _ []byte) []byte { a[len(a)-1] ^= 1; return a },
		"flipped kind":     func(a, _ []byte) []byte { a[recordPrefixSize] ^= 1; return a },
		"truncated body":   func(a, _ []byte) []byte { return a[:len(a)-5] },
		"truncated header": func(a, _ []byte) []byte { return a[:1] },
		"short length": func(a, _ []byte) []byte {
			binary.BigEndian.PutUint16(a, minRecordCiphertext-1)
			return a
		},
		"grown length": func(a, _ []byte) []byte {
			binary.BigEndian.PutUint16(a, binary.BigEndian.Uint16(a)+1)
			return append(a, 0)
		},
		"reordered": func(a, b []byte) []byte { return cat(b, a) },
		"dropped":   func(_, b []byte) []byte { return b },
		"replayed":  func(a, _ []byte) []byte { return cat(a, a) },
	}
	for _, alg := range testAlgorithms {
		for name, mutate := range cases {
			t.Run(string(alg)+"/"+name, func(t *testing.T) {
				w, r, wire := sessionPair(t, alg)
				rec1, rec2 := two(t, w, wire)
				wire.buf.Write(mutate(rec1, rec2))
				var err error
				for range 2 {
					if _, err = readMessage(t, r); err != nil {
						break
					}
				}
				if err == nil {
					t.Fatal("both messages read")
				}
				if !r.Failed() {
					t.Fatal("session not poisoned")
				}
			})
		}
		// A writer's own records reflected back fail: each direction has
		// its own key.
		t.Run(string(alg)+"/reflected", func(t *testing.T) {
			w, _, wire := sessionPair(t, alg)
			mw := w.Writer(false)
			_, _ = mw.Write([]byte("first"))
			_ = mw.End()
			w.br = bufio.NewReader(bytes.NewReader(append([]byte(nil), wire.buf.Bytes()...)))
			if _, err := readMessage(t, w); err == nil || !w.Failed() {
				t.Fatalf("reflected record = %v, want a failed session", err)
			}
		})
	}
	// A clean EOF before a message starts is a benign disconnect.
	_, r, _ := sessionPair(t, AlgorithmAES)
	if _, err := readMessage(t, r); !errors.Is(err, ErrPeerClosed) || !errors.Is(err, io.EOF) {
		t.Fatalf("idle EOF = %v, want ErrPeerClosed wrapping io.EOF", err)
	}
}

func TestFinishMessageDetectsTrailingBytes(t *testing.T) {
	w, r, _ := sessionPair(t, AlgorithmAES)
	mw := w.Writer(false)
	_, _ = mw.Write([]byte("OK\r\nextra"))
	_ = mw.End()
	mr, _ := r.Reader()
	br := bufio.NewReader(mr)
	if line, err := br.ReadString('\n'); err != nil || line != "OK\r\n" {
		t.Fatalf("line = (%q, %v)", line, err)
	}
	if err := FinishMessage(br); !errors.Is(err, errTrailingBytes) {
		t.Fatalf("FinishMessage = %v, want trailing bytes", err)
	}
}

func TestReaderRequiresPreviousMessageEnd(t *testing.T) {
	w, r, _ := sessionPair(t, AlgorithmAES)
	mw := w.Writer(false)
	_, _ = mw.Write([]byte("abc"))
	_ = mw.Flush()
	mr, _ := r.Reader()
	_, _ = mr.Read(make([]byte, 1))
	if _, err := r.Reader(); err == nil {
		t.Fatal("started a message before the previous one ended")
	}
}

// --- handshake ---

type testServer struct {
	cfg  ServerConfig
	ln   net.Listener
	done chan serverResult
}

type serverResult struct {
	s    *Session
	peer Peer
	err  error
}

func newKeyPair(t testing.TB) (KeyPair, *age.X25519Identity) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	kp, err := KeyPairFromIdentity(id)
	if err != nil {
		t.Fatal(err)
	}
	return kp, id
}

func startServer(t *testing.T, cfg ServerConfig) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts := &testServer{cfg: cfg, ln: ln, done: make(chan serverResult, 16)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = conn.SetDeadline(time.Now().Add(HandshakeTimeout))
				br := bufio.NewReader(conn)
				line, err := br.ReadSlice('\n')
				if err != nil {
					ts.done <- serverResult{err: err}
					conn.Close()
					return
				}
				s, peer, err := ServerHandshake(conn, br, append([]byte(nil), line...), cfg)
				var diag *DiagnosticError
				if errors.As(err, &diag) {
					// Write, then drain message 1 so the close does not
					// reset the connection before the client reads the line.
					_, _ = conn.Write([]byte(diag.Line + "\r\n"))
					_ = conn.(*net.TCPConn).CloseWrite()
					_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
					_, _ = io.Copy(io.Discard, br)
				}
				if err != nil {
					conn.Close()
				} else {
					_ = conn.SetDeadline(time.Time{})
				}
				ts.done <- serverResult{s: s, peer: peer, err: err}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ts
}

func dialClient(t *testing.T, ts *testServer, cfg ClientConfig) (*Session, error) {
	t.Helper()
	conn, err := net.Dial("tcp", ts.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(HandshakeTimeout))
	s, err := ClientHandshake(conn, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	t.Cleanup(func() { s.Close() })
	return s, nil
}

func exchange(t *testing.T, client, server *Session) {
	t.Helper()
	for _, dir := range []struct {
		from, to *Session
		msg      string
	}{{client, server, "PING\r\n"}, {server, client, "OK pong\r\n"}} {
		mw := dir.from.Writer(false)
		go func() { _, _ = mw.Write([]byte(dir.msg)); _ = mw.End() }()
		got, err := readMessage(t, dir.to)
		if err != nil || string(got) != dir.msg {
			t.Fatalf("exchange = (%q, %v), want %q", got, err, dir.msg)
		}
	}
}

func TestHandshakeModes(t *testing.T) {
	serverKP, _ := newKeyPair(t)
	clientKP, clientID := newKeyPair(t)
	token := strings.Repeat("k", 48)
	allow, err := NewAllowlist([]string{token}, []string{clientID.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	for _, alg := range testAlgorithms {
		for _, tc := range []struct {
			name   string
			server ServerConfig
			client ClientConfig
		}{
			{"token", ServerConfig{Static: serverKP, Allowlist: allow}, ClientConfig{Mode: ModeToken, Token: token}},
			{"age allowlisted", ServerConfig{Static: serverKP, Allowlist: allow}, ClientConfig{Mode: ModeAge}},
			{"age any key", ServerConfig{Static: serverKP, AnyKey: true}, ClientConfig{Mode: ModeAge}},
		} {
			t.Run(tc.name+"/"+string(alg), func(t *testing.T) {
				ts := startServer(t, tc.server)
				cfg := tc.client
				cfg.Cipher, cfg.Static, cfg.ServerKey = alg, clientKP, serverKP.Public
				cs, err := dialClient(t, ts, cfg)
				if err != nil {
					t.Fatalf("client: %v", err)
				}
				res := <-ts.done
				if res.err != nil {
					t.Fatalf("server: %v", res.err)
				}
				defer res.s.Close()
				if res.peer.Mode != cfg.Mode || res.peer.Static != clientKP.Public {
					t.Fatalf("peer = %+v", res.peer)
				}
				exchange(t, cs, res.s)
			})
		}
	}
}

func TestHandshakeRefusals(t *testing.T) {
	serverKP, _ := newKeyPair(t)
	otherServerKP, _ := newKeyPair(t)
	clientKP, clientID := newKeyPair(t)
	strangerKP, _ := newKeyPair(t)
	token := strings.Repeat("k", 48)
	tokensOnly, _ := NewAllowlist([]string{token}, nil)
	agesOnly, _ := NewAllowlist(nil, []string{clientID.Recipient().String()})
	base := ClientConfig{Cipher: AlgorithmAES, Static: clientKP, ServerKey: serverKP.Public}
	for _, tc := range []struct {
		name   string
		server ServerConfig
		client func(ClientConfig) ClientConfig
		want   error
	}{
		{"unknown token", ServerConfig{Static: serverKP, Allowlist: tokensOnly},
			func(c ClientConfig) ClientConfig { c.Mode, c.Token = ModeToken, strings.Repeat("x", 48); return c }, ErrNotAuthorized},
		{"token not configured", ServerConfig{Static: serverKP, Allowlist: agesOnly},
			func(c ClientConfig) ClientConfig { c.Mode, c.Token = ModeToken, token; return c }, ErrTokenNotConfigured},
		{"token on any-key server", ServerConfig{Static: serverKP, AnyKey: true},
			func(c ClientConfig) ClientConfig { c.Mode, c.Token = ModeToken, token; return c }, ErrTokenNotConfigured},
		{"unlisted age key", ServerConfig{Static: serverKP, Allowlist: agesOnly},
			func(c ClientConfig) ClientConfig { c.Mode, c.Static = ModeAge, strangerKP; return c }, ErrNotAuthorized},
		{"age without any-key or list", ServerConfig{Static: serverKP},
			func(c ClientConfig) ClientConfig { c.Mode = ModeAge; return c }, ErrNotAuthorized},
		{"wrong server key", ServerConfig{Static: otherServerKP, Allowlist: tokensOnly},
			func(c ClientConfig) ClientConfig { c.Mode, c.Token = ModeToken, token; return c }, ErrNotAuthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := startServer(t, tc.server)
			_, err := dialClient(t, ts, tc.client(base))
			if !errors.Is(err, tc.want) {
				t.Fatalf("client error = %v, want %v", err, tc.want)
			}
			if res := <-ts.done; res.err == nil {
				t.Fatal("server authenticated")
			}
		})
	}
}

// rawOpening returns a client's preface and message 1 as sent on the wire.
func rawOpening(t *testing.T, cfg ClientConfig) []byte {
	t.Helper()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() { _, _ = ClientHandshake(a, cfg) }()
	br := bufio.NewReader(b)
	line, _ := br.ReadSlice('\n')
	out := append([]byte(nil), line...)
	msg := make([]byte, recordPrefixSize+handshakeMsg1Size)
	if _, err := io.ReadFull(br, msg); err != nil {
		t.Fatal(err)
	}
	return append(out, msg...)
}

// forgeOpening builds an age-mode preface and message 1 the way a client
// would, except that the tag names claimed while the handshake proves
// static, as a peer that knows a recipient but not its private key would.
func forgeOpening(t *testing.T, serverPub [keySize]byte, claimed []byte, static KeyPair) []byte {
	t.Helper()
	ephemeral := make([]byte, keySize)
	if _, err := rand.Read(ephemeral); err != nil {
		t.Fatal(err)
	}
	dh, err := curve25519.X25519(ephemeral, serverPub[:])
	if err != nil {
		t.Fatal(err)
	}
	line := []byte("AUTH noise-v1 age aes " + credentialTag(ModeAge, claimed, dh) + "\r\n")
	config, err := handshakeConfig(handshakeParams{
		mode: ModeAge, alg: AlgorithmAES, initiator: true, line: line,
		static: static, peer: serverPub[:], random: bytes.NewReader(ephemeral),
	})
	if err != nil {
		t.Fatal(err)
	}
	hs, err := noise.NewHandshakeState(config)
	if err != nil {
		t.Fatal(err)
	}
	msg1, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := binary.BigEndian.AppendUint16(line, uint16(len(msg1)))
	return append(out, msg1...)
}

// sendOpening writes opening to ts and returns the server's reply.
func sendOpening(t *testing.T, ts *testServer, opening []byte) []byte {
	t.Helper()
	conn, err := net.Dial("tcp", ts.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(HandshakeTimeout))
	_, _ = conn.Write(opening)
	reply, _ := io.ReadAll(conn)
	return reply
}

func TestCredentialTagIsPerConnection(t *testing.T) {
	serverKP, _ := newKeyPair(t)
	clientKP, _ := newKeyPair(t)
	token := strings.Repeat("t", 16)
	for _, cfg := range []ClientConfig{
		{Mode: ModeToken, Cipher: AlgorithmAES, Token: token, Static: clientKP, ServerKey: serverKP.Public},
		{Mode: ModeAge, Cipher: AlgorithmAES, Static: clientKP, ServerKey: serverKP.Public},
	} {
		a, b := rawOpening(t, cfg), rawOpening(t, cfg)
		lineA := a[:bytes.IndexByte(a, '\n')]
		lineB := b[:bytes.IndexByte(b, '\n')]
		if bytes.Equal(lineA, lineB) {
			t.Fatalf("%s: two connections sent the same preface %q", cfg.Mode, lineA)
		}
		credential := []byte(token)
		if cfg.Mode == ModeAge {
			credential = clientKP.Public[:]
		}
		// Without the DH, the stable fingerprint never appears on the wire.
		if bytes.Contains(a, []byte(CredentialID(cfg.Mode, credential))) {
			t.Fatalf("%s: preface carries the stable credential ID", cfg.Mode)
		}
	}
}

func TestImpersonationAndReplay(t *testing.T) {
	serverKP, _ := newKeyPair(t)
	clientKP, clientID := newKeyPair(t)
	strangerKP, _ := newKeyPair(t)
	allow, _ := NewAllowlist(nil, []string{clientID.Recipient().String()})
	ts := startServer(t, ServerConfig{Static: serverKP, Allowlist: allow})

	// A peer that knows Alice's public recipient but not her private key
	// tags her credential correctly but proves its own key: the tag check
	// against the proven key rejects it.
	alicePub, err := ParseRecipientKey(clientID.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	reply := sendOpening(t, ts, forgeOpening(t, serverKP.Public, alicePub[:], strangerKP))
	if !strings.HasPrefix(string(reply), DiagNotAuthorized) {
		t.Fatalf("forged opening reply = %q", reply)
	}
	if res := <-ts.done; res.err == nil {
		t.Fatal("forged opening authenticated")
	}

	// On an any-key server, a tag that names a key other than the proven
	// one is refused too.
	anyKey := startServer(t, ServerConfig{Static: serverKP, AnyKey: true})
	reply = sendOpening(t, anyKey, forgeOpening(t, serverKP.Public, alicePub[:], strangerKP))
	if !strings.HasPrefix(string(reply), DiagNotAuthorized) {
		t.Fatalf("any-key forged opening reply = %q", reply)
	}
	if res := <-anyKey.done; res.err == nil {
		t.Fatal("any-key server authenticated a mismatched tag")
	}

	// A replayed genuine opening gets message 2, but the replayer cannot
	// produce FINISH, so the server never authenticates it.
	genuine := rawOpening(t, ClientConfig{Mode: ModeAge, Cipher: AlgorithmAES, Static: clientKP, ServerKey: serverKP.Public})
	conn, _ := net.Dial("tcp", ts.ln.Addr().String())
	_, _ = conn.Write(genuine)
	msg2 := make([]byte, recordPrefixSize+handshakeMsg2Size)
	if _, err := io.ReadFull(conn, msg2); err != nil {
		t.Fatalf("replay got no message 2: %v", err)
	}
	// A FINISH captured from another session is under different keys.
	_, _ = conn.Write([]byte{0x00, 0x11})
	_, _ = conn.Write(make([]byte, 17))
	if res := <-ts.done; res.err == nil {
		t.Fatal("replayed opening authenticated")
	}
	conn.Close()
}

func TestLowOrderEphemeralRejected(t *testing.T) {
	serverKP, _ := newKeyPair(t)
	clientKP, _ := newKeyPair(t)
	ts := startServer(t, ServerConfig{Static: serverKP, AnyKey: true})
	opening := rawOpening(t, ClientConfig{Mode: ModeAge, Cipher: AlgorithmChaCha20, Static: clientKP, ServerKey: serverKP.Public})
	start := bytes.IndexByte(opening, '\n') + 1 + recordPrefixSize
	for _, point := range [][]byte{make([]byte, 32), append([]byte{1}, make([]byte, 31)...)} {
		mutated := append([]byte(nil), opening...)
		copy(mutated[start:], point)
		conn, _ := net.Dial("tcp", ts.ln.Addr().String())
		_, _ = conn.Write(mutated)
		reply, _ := io.ReadAll(conn)
		conn.Close()
		if res := <-ts.done; res.err == nil {
			t.Fatal("low-order ephemeral authenticated")
		}
		if bytes.HasPrefix(reply, []byte{0, handshakeMsg2Size}) {
			t.Fatal("server answered a low-order ephemeral with message 2")
		}
	}
}

func TestEmptyPSKCannotDowngradeTokenMode(t *testing.T) {
	kp, _ := newKeyPair(t)
	line := []byte("AUTH noise-v1 token aes " + strings.Repeat("0", 64) + "\r\n")
	if _, err := handshakeConfig(handshakeParams{mode: ModeToken, alg: AlgorithmAES, line: line, static: kp}); err == nil {
		t.Fatal("token mode accepted an empty PSK")
	}
	if _, err := handshakeConfig(handshakeParams{mode: ModeAge, alg: AlgorithmAES, line: line, psk: make([]byte, 32), static: kp}); err == nil {
		t.Fatal("age mode accepted a PSK")
	}
}

func TestParsePreface(t *testing.T) {
	id := strings.Repeat("a", 64)
	good := "AUTH noise-v1 token chacha20 " + id + "\r\n"
	if _, err := ParsePreface([]byte(good)); err != nil {
		t.Fatalf("valid preface rejected: %v", err)
	}
	for _, bad := range []string{
		"AUTH noise-v1 token chacha20 " + id + "\n",
		"AUTH noise-v1 token chacha20  " + id + "\r\n",
		"AUTH noise-v2 token aes " + id + "\r\n",
		"AUTH noise-v1 tok aes " + id + "\r\n",
		"AUTH noise-v1 token des " + id + "\r\n",
		"AUTH noise-v1 token aes " + strings.Repeat("A", 64) + "\r\n",
		"AUTH noise-v1 token aes " + id[:63] + "\r\n",
		"AUTH noise-v1 token aes " + id + " extra\r\n",
	} {
		if _, err := ParsePreface([]byte(bad)); err == nil {
			t.Errorf("ParsePreface accepted %q", bad)
		}
	}
}

func FuzzParsePreface(f *testing.F) {
	f.Add([]byte("AUTH noise-v1 token aes " + strings.Repeat("a", 64) + "\r\n"))
	f.Add([]byte("AUTH key\r\n"))
	f.Fuzz(func(t *testing.T, line []byte) {
		pf, err := ParsePreface(line)
		if err != nil {
			return
		}
		rebuilt := "AUTH noise-v1 " + pf.Mode + " " + string(pf.Cipher) + " " + pf.ID + "\r\n"
		if rebuilt != string(line) {
			t.Fatalf("accepted a non-canonical preface %q", line)
		}
	})
}

// FuzzReadRecords seals a fuzz-chosen sequence of records, each a kind and a
// payload length, optionally followed by unauthenticated bytes, and checks
// the reader against a model of the record grammar: DATA must be non-empty,
// END ends a message and may carry payload, FINISH and kinds 3+ are fatal,
// and EOF or junk inside a message is fatal.
func FuzzReadRecords(f *testing.F) {
	f.Add([]byte{0, 3, 1, 0}, []byte(nil))
	f.Add([]byte{0, 0, 1, 0}, []byte(nil))
	f.Add([]byte{1, 5, 2, 0, 1, 0}, []byte(nil))
	f.Add([]byte{3, 1}, []byte(nil))
	f.Add([]byte{0x80, 0, 1, 0}, []byte(nil))
	f.Add([]byte{0, 7}, []byte{0xff, 0xff, 1, 2, 3})
	f.Add([]byte{}, []byte{0x00, 0x11})
	f.Fuzz(func(t *testing.T, script, junk []byte) {
		w, r, wire := sessionPair(t, AlgorithmAES)
		var msgs [][]byte
		var cur []byte
		inMsg, invalid := false, false
		for i := 0; i+1 < len(script) && i < 128 && !invalid; i += 2 {
			kind := script[i] & 0x7f % 5
			n := int(script[i+1])
			if script[i]&0x80 != 0 {
				n = maxEmitPayload - n
			}
			payload := bytes.Repeat([]byte{byte(i)}, n)
			writeRawRecord(t, w, kind, payload)
			switch {
			case kind == recordData && n > 0:
				cur, inMsg = append(cur, payload...), true
			case kind == recordEnd:
				msgs = append(msgs, append(cur, payload...))
				cur, inMsg = nil, false
			default:
				invalid = true
			}
		}
		wire.buf.Write(junk)
		// The reader fails at message len(msgs) unless the stream ends
		// cleanly between messages.
		fails := invalid || inMsg || len(junk) > 0

		for m := 0; ; m++ {
			if m > len(msgs) {
				t.Fatalf("reader produced %d messages, model %d", m, len(msgs))
			}
			mr, err := r.Reader()
			var got []byte
			if err == nil {
				got, err = io.ReadAll(mr)
			}
			if m < len(msgs) {
				if err != nil || !bytes.Equal(got, msgs[m]) {
					t.Fatalf("message %d = (%d bytes, %v), want %d bytes", m, len(got), err, len(msgs[m]))
				}
				continue
			}
			if !fails {
				if !errors.Is(err, ErrPeerClosed) || len(got) > 0 {
					t.Fatalf("clean end = (%d bytes, %v), want ErrPeerClosed", len(got), err)
				}
				return
			}
			if err == nil || !r.Failed() {
				t.Fatalf("message %d: err=%v failed=%v, want a failed session", m, err, r.Failed())
			}
			if !bytes.HasPrefix(cur, got) {
				t.Fatalf("failed message returned %d bytes that were not sealed as DATA", len(got))
			}
			return
		}
	})
}

// TestRecordCipherMatchesNoise checks the record layer's own nonce encoding
// and REKEY against flynn/noise's CipherState, record by record, across two
// rekeys in each cipher.
func TestRecordCipherMatchesNoise(t *testing.T) {
	for _, alg := range testAlgorithms {
		cs, _ := cipherSuite(alg)
		hi, _ := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Random: rand.Reader, Pattern: noise.HandshakeNN, Initiator: true})
		hr, _ := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Random: rand.Reader, Pattern: noise.HandshakeNN})
		m1, _, _, _ := hi.WriteMessage(nil, nil)
		_, _, _, _ = hr.ReadMessage(nil, m1)
		m2, _, _, _ := hr.WriteMessage(nil, nil)
		_, ref, _, err := hi.ReadMessage(nil, m2)
		if err != nil {
			t.Fatal(err)
		}
		ours, err := newRecordCipher(alg, ref)
		if err != nil {
			t.Fatal(err)
		}
		msg := []byte("record")
		ad := []byte{recordEnd}
		for i := range 3*7 + 2 {
			want, err := ref.Encrypt(nil, ad, msg)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ours.seal(nil, msg, ad)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s record %d differs from flynn/noise", alg, i)
			}
			if i%7 == 6 {
				ref.Rekey()
				if err := ours.rekey(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestSessionCloseOnTCPIsQuiet(t *testing.T) {
	serverKP, _ := newKeyPair(t)
	clientKP, _ := newKeyPair(t)
	ts := startServer(t, ServerConfig{Static: serverKP, AnyKey: true})
	s, err := dialClient(t, ts, ClientConfig{Mode: ModeAge, Cipher: AlgorithmAES, Static: clientKP, ServerKey: serverKP.Public})
	if err != nil {
		t.Fatal(err)
	}
	res := <-ts.done
	defer res.s.Close()
	if err := s.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	// A session that failed already closed its socket; Close is quiet.
	_ = res.s.fail(errors.New("boom"))
	if err := res.s.Close(); err != nil {
		t.Fatalf("Close after failure = %v", err)
	}
}

// pacedStream moves bytes chunk at a time, sleeping gap before each chunk.
// With stallAfter > 0 it stops for good once it has moved that many bytes,
// closing stalled and recording when, until stop closes.
type pacedStream struct {
	chunk      int
	gap        time.Duration
	stallAfter int
}

// stallRecord reports when a pacedStream stalled.
type stallRecord struct {
	stalled chan struct{}
	at      time.Time // set before stalled closes
}

func newStallRecord() *stallRecord { return &stallRecord{stalled: make(chan struct{})} }

func (r *stallRecord) stall() {
	r.at = time.Now()
	close(r.stalled)
}

// since reports how long ago the stream stalled, or false if it has not.
func (r *stallRecord) since() (time.Duration, bool) {
	select {
	case <-r.stalled:
		return time.Since(r.at), true
	default:
		return 0, false
	}
}

// step waits out one gap. It reports false once stop closes.
func (p pacedStream) step(stop <-chan struct{}) bool {
	select {
	case <-time.After(p.gap):
		return true
	case <-stop:
		return false
	}
}

// A read progress deadline bounds each wait for the peer's next bytes, not
// a whole record: a 64 KiB record arriving 4 KiB every 25ms takes four
// deadlines in all and is read whole. A record that stops arriving halfway
// fails within a few deadlines of the stall.
func TestReadProgressDeadlineCountsPartialRecords(t *testing.T) {
	const d = 100 * time.Millisecond
	for _, tc := range []struct {
		name string
		pace pacedStream
	}{
		{"record keeps arriving", pacedStream{chunk: 4 << 10, gap: 25 * time.Millisecond}},
		{"record stops halfway", pacedStream{chunk: 4 << 10, gap: 25 * time.Millisecond, stallAfter: 32 << 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i1, i2, r1, r2 := nnCipherStates(t, AlgorithmAES)
			wire := &bufConn{}
			w, err := newSession(wire, bufio.NewReader(wire), AlgorithmAES, i1, i2)
			if err != nil {
				t.Fatal(err)
			}
			data := make([]byte, maxEmitPayload)
			rand.Read(data)
			mw := w.Writer(false)
			if _, err := mw.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := mw.End(); err != nil {
				t.Fatal(err)
			}
			sealed := bytes.Clone(wire.buf.Bytes())

			local, remote := net.Pipe()
			r, err := newSession(local, bufio.NewReader(local), AlgorithmAES, r2, r1)
			if err != nil {
				t.Fatal(err)
			}
			r.SetReadProgressDeadline(d)
			stop := make(chan struct{})
			stall := newStallRecord()
			var wg sync.WaitGroup
			t.Cleanup(func() {
				close(stop)
				_ = local.Close()
				_ = remote.Close()
				wg.Wait()
			})
			wg.Add(1)
			go func() {
				defer wg.Done()
				for off := 0; off < len(sealed); {
					if !tc.pace.step(stop) {
						return
					}
					n := min(tc.pace.chunk, len(sealed)-off)
					if _, err := remote.Write(sealed[off : off+n]); err != nil {
						return
					}
					off += n
					if tc.pace.stallAfter > 0 && off >= tc.pace.stallAfter {
						stall.stall()
						<-stop
						return
					}
				}
			}()

			start := time.Now()
			got, err := readMessage(t, r)
			elapsed := time.Since(start)
			if tc.pace.stallAfter == 0 {
				if err != nil {
					t.Fatalf("steadily arriving record failed after %v under a %v read progress deadline: %v", elapsed, d, err)
				}
				if !bytes.Equal(got, data) {
					t.Fatalf("read %d bytes, want the %d written", len(got), len(data))
				}
				if elapsed < 2*d {
					t.Fatalf("record arrived in %v; the test needs one slower than two %v deadlines", elapsed, d)
				}
				return
			}
			if err == nil {
				t.Fatal("read a record that stopped arriving")
			}
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("stalled read failed with %v, want a deadline error", err)
			}
			if since, ok := stall.since(); ok && since > 5*d {
				t.Fatalf("stalled read gave up %v after the stall, want within %v", since, 5*d)
			}
		})
	}
}

// A write progress deadline bounds each 16 KiB piece of a batch, not the
// whole batch: a 256 KiB batch to a peer that reads 4 KiB every 10ms takes
// several deadlines and arrives whole. A peer that stops reading fails the
// write within a few deadlines of the stall. Both sockets have small
// buffers, so the writer blocks on the reader's pace.
func TestWriteProgressDeadlineCoversEachPiece(t *testing.T) {
	const d = 150 * time.Millisecond
	for _, tc := range []struct {
		name string
		pace pacedStream
	}{
		{"peer reads steadily", pacedStream{chunk: 4 << 10, gap: 10 * time.Millisecond}},
		{"peer stops reading", pacedStream{chunk: 4 << 10, gap: 10 * time.Millisecond, stallAfter: 64 << 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := smallBufferTCPPair(t, 16<<10)
			i1, i2, r1, r2 := nnCipherStates(t, AlgorithmAES)
			w, err := newSession(client, bufio.NewReader(client), AlgorithmAES, i1, i2)
			if err != nil {
				t.Fatal(err)
			}
			w.SetWriteProgressDeadline(d)

			stop := make(chan struct{})
			stall := newStallRecord()
			received := make(chan []byte, 1)
			var wg sync.WaitGroup
			t.Cleanup(func() {
				close(stop)
				_ = client.Close()
				_ = server.Close()
				wg.Wait()
			})
			wg.Add(1)
			go func() {
				defer wg.Done()
				var got []byte
				defer func() { received <- got }()
				buf := make([]byte, tc.pace.chunk)
				for tc.pace.step(stop) {
					n, err := server.Read(buf)
					got = append(got, buf[:n]...)
					if tc.pace.stallAfter > 0 && len(got) >= tc.pace.stallAfter {
						stall.stall()
						<-stop
						return
					}
					if err != nil {
						return
					}
				}
			}()

			data := make([]byte, writeBatchRecords*maxEmitPayload)
			rand.Read(data)
			start := time.Now()
			mw := w.Writer(false)
			_, err = mw.Write(data)
			if err == nil {
				err = mw.End()
			}
			elapsed := time.Since(start)
			if tc.pace.stallAfter == 0 {
				if err != nil {
					t.Fatalf("write to a steadily reading peer failed after %v under a %v write progress deadline: %v", elapsed, d, err)
				}
				if elapsed < 2*d {
					t.Fatalf("batch left in %v; the test needs one slower than two %v deadlines", elapsed, d)
				}
				_ = client.CloseWrite()
				sealed := <-received
				wire := &bufConn{}
				wire.buf.Write(sealed)
				r, err := newSession(&bufConn{}, bufio.NewReader(wire), AlgorithmAES, r2, r1)
				if err != nil {
					t.Fatal(err)
				}
				if got, err := readMessage(t, r); err != nil || !bytes.Equal(got, data) {
					t.Fatalf("peer read back %d of %d bytes: %v", len(got), len(data), err)
				}
				return
			}
			if err == nil {
				t.Fatal("write to a peer that stopped reading succeeded")
			}
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("stalled write failed with %v, want a deadline error", err)
			}
			since, ok := stall.since()
			if !ok {
				t.Fatalf("write failed before the peer stopped reading: %v", err)
			}
			if since > 5*d {
				t.Fatalf("stalled write gave up %v after the stall, want within %v", since, 5*d)
			}
		})
	}
}

// smallBufferTCPPair returns both ends of a loopback TCP connection whose
// client send buffer and server receive buffer are about size bytes. The
// listener sets the receive buffer, so the server's first advertised window
// already fits it.
func smallBufferTCPPair(t *testing.T, size int) (client, server *net.TCPConn) {
	t.Helper()
	lc := net.ListenConfig{Control: func(_, _ string, rc syscall.RawConn) error {
		var serr error
		if err := rc.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF, size)
		}); err != nil {
			return err
		}
		return serr
	}}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	s := <-accepted
	if s == nil {
		c.Close()
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	client = c.(*net.TCPConn)
	if err := client.SetWriteBuffer(size); err != nil {
		t.Fatal(err)
	}
	return client, s.(*net.TCPConn)
}
