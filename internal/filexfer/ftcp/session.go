package ftcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

const (
	// defaultMaxUnauthenticatedConns bounds sockets that have not completed
	// FINISH. It is deliberately large: a cap sized to CPU count would let a
	// few idle sockets exclude every legitimate client.
	defaultMaxUnauthenticatedConns = 1024
	// defaultReadProgressTimeout is the read progress deadline on every
	// connection, plaintext or authenticated, when keep-alive is disabled.
	defaultReadProgressTimeout = 60 * time.Second
	// DeadPeerTimeout is the TCP_USER_TIMEOUT of client and server
	// sockets: the kernel closes a connection whose sent data or keepalive
	// probes go unacknowledged this long, or whose peer keeps its receive
	// window closed this long, because it is dead or has stopped reading.
	// The work on it then fails, to be retried, rather than waiting on a
	// longer deadline.
	DeadPeerTimeout = 30 * time.Second
	// busyWriteTimeout bounds the BUSY reply write.
	busyWriteTimeout = 10 * time.Millisecond
	// busyDrainTimeout and busyDrainBytes bound reading the refused
	// client's opening, so closing does not reset the connection and
	// discard the BUSY line before the client reads it.
	busyDrainTimeout = 200 * time.Millisecond
	busyDrainBytes   = aead.MaxPrefaceBytes + 2 + 96
)

// authKeyLine asks for the server's key and cipher hint. It is versioned so
// the server can tell clients from before noise-v1, which send the bare
// legacyKeyLine first, and answer them with a message they print.
var (
	authKeyLine   = []byte("AUTH key " + aead.PrefaceVersion + "\r\n")
	legacyKeyLine = []byte("AUTH key\r\n")
)

// isLegacyAuth reports whether a first line is a form only clients from
// before noise-v1 send: bare discovery or an `AUTH aes|chacha20 <blob>`.
func isLegacyAuth(line []byte) bool {
	upper := bytes.ToUpper(line[:min(len(line), len("AUTH CHACHA20 "))])
	return bytes.EqualFold(line, legacyKeyLine) ||
		bytes.HasPrefix(upper, []byte("AUTH AES ")) || bytes.HasPrefix(upper, []byte("AUTH CHACHA20 "))
}

// authPolicy is a server's authentication configuration, shared by its
// connections.
type authPolicy struct {
	required  bool
	server    aead.ServerConfig
	recipient string
	permits   chan struct{}
}

func newAuthPolicy(opts ServerOptions) (*authPolicy, error) {
	if opts.MaxUnauthenticatedConns < 0 {
		return nil, errors.New("MaxUnauthenticatedConns must not be negative")
	}
	allow, err := aead.NewAllowlist(opts.AllowedAuthTokens, opts.AllowedClientRecipients)
	if err != nil {
		return nil, err
	}
	identity := opts.ServerIdentity
	if identity == nil {
		if identity, err = age.GenerateX25519Identity(); err != nil {
			return nil, err
		}
	}
	kp, err := aead.KeyPairFromIdentity(identity)
	if err != nil {
		return nil, fmt.Errorf("server identity: %w", err)
	}
	limit := opts.MaxUnauthenticatedConns
	if limit == 0 {
		limit = defaultMaxUnauthenticatedConns
	}
	return &authPolicy{
		required:  opts.RequireAuth || !allow.Empty(),
		server:    aead.ServerConfig{Static: kp, Allowlist: allow, AnyKey: allow.Empty()},
		recipient: identity.Recipient().String(),
		permits:   make(chan struct{}, limit),
	}, nil
}

// tryAcquire takes an admission permit without waiting. The returned
// release is idempotent.
func (p *authPolicy) tryAcquire() (func(), bool) {
	select {
	case p.permits <- struct{}{}:
		released := false
		return func() {
			if !released {
				released = true
				<-p.permits
			}
		}, true
	default:
		return nil, false
	}
}

// refuseBusy answers a connection the server has no permit for. It runs in
// its own goroutine so the accept loop never waits: one bounded write, a
// bounded read of the client's opening, then close.
func refuseBusy(conn net.Conn, br *bufio.Reader) {
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(busyWriteTimeout))
	if _, err := conn.Write([]byte(aead.DiagBusy + "\r\n")); err != nil {
		return
	}
	if tc := rawTCPConn(conn); tc != nil {
		_ = tc.CloseWrite()
	}
	_ = conn.SetReadDeadline(time.Now().Add(busyDrainTimeout))
	var src io.Reader = conn
	if br != nil {
		src = br
	}
	_, _ = io.CopyN(io.Discard, src, busyDrainBytes)
}

// writeDiagnostic sends a plaintext refusal before message 2, then reads
// the client's opening for at most busyDrainTimeout, so closing does not
// reset the connection and discard the line before the client reads it.
// The connection's handshake deadline bounds the write; the drain never
// extends past it.
func writeDiagnostic(conn net.Conn, line string, deadline time.Time) {
	if _, err := conn.Write([]byte(line + "\r\n")); err != nil {
		return
	}
	if tc := rawTCPConn(conn); tc != nil {
		_ = tc.CloseWrite()
	}
	_ = conn.SetReadDeadline(minTime(deadline, time.Now().Add(busyDrainTimeout)))
	_, _ = io.CopyN(io.Discard, conn, busyDrainBytes)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// noiseRequest is a command's input on an authenticated session. Framed
// body readers take its buffered reader directly (requestBodySource), and a
// handler calls completeRequest once the body is verified and before it
// changes any state, so trailing bytes cannot hide in a second buffer.
type noiseRequest struct {
	br        *bufio.Reader
	completed bool
}

func (r *noiseRequest) Read(p []byte) (int, error) { return r.br.Read(p) }

// requestBodySource is the reader a framed codec should consume.
func requestBodySource(in io.Reader) io.Reader {
	if r, ok := in.(*noiseRequest); ok {
		return r.br
	}
	return in
}

// errUnfinishedRequest marks a Noise request whose authenticated END was not
// where the grammar says the request ends. The connection closes without a
// response.
var errUnfinishedRequest = errors.New("request did not end at its authenticated END")

// completeRequest verifies that a Noise request ended exactly: no logical
// bytes remain and the next read is its authenticated END. Plaintext
// requests have no such boundary and always pass.
func completeRequest(in io.Reader) error {
	r, ok := in.(*noiseRequest)
	if !ok || r.completed {
		return nil
	}
	if err := aead.FinishMessage(r.br); err != nil {
		return fmt.Errorf("%w: %w", errUnfinishedRequest, err)
	}
	r.completed = true
	return nil
}

// drainRequest reads an unfinished request to its END after a handler
// failed before completing it, so the error can still be reported. It reads
// at most limit bytes; past that, or on a malformed END, it reports false.
func drainRequest(r *noiseRequest, limit int64) bool {
	if r.completed {
		return true
	}
	if _, err := io.CopyN(io.Discard, r.br, limit); err != nil && !errors.Is(err, io.EOF) {
		return false
	}
	return completeRequest(r) == nil
}

// runAuth handles a first line that begins with AUTH: public key discovery,
// or a Noise handshake followed by the authenticated command loop.
func (s *connSession) runAuth(br *bufio.Reader, line []byte) error {
	if s.releasePermit == nil {
		release, ok := s.auth.tryAcquire()
		if !ok {
			refuseBusy(s.conn, br)
			return nil
		}
		s.releasePermit = release
	}
	deadline := s.acceptedAt.Add(aead.HandshakeTimeout)
	_ = s.conn.SetDeadline(deadline)
	if bytes.Equal(line, authKeyLine) {
		_, err := io.WriteString(s.conn, "OK "+string(aead.RecommendedCipher())+" "+s.auth.recipient+"\r\n")
		return err
	}
	if isLegacyAuth(line) {
		// Never log the line itself: a legacy blob carries the client's
		// tokens, encrypted only to this server's key.
		log.Printf("legacy AUTH from %s: client predates %s; upgrade tx", s.conn.RemoteAddr(), aead.PrefaceVersion)
		s.emitAuth("legacy", false)
		diag := aead.DiagUnsupportedAuth
		if bytes.EqualFold(line, legacyKeyLine) {
			diag = aead.DiagClientTooOld
		}
		writeDiagnostic(s.conn, diag, deadline)
		return nil
	}
	if !aead.IsNoisePreface(line) {
		writeDiagnostic(s.conn, aead.DiagUnsupportedAuth, deadline)
		return nil
	}
	sess, peer, err := aead.ServerHandshake(s.conn, br, line, s.auth.server)
	if err != nil {
		var diag *aead.DiagnosticError
		if errors.As(err, &diag) {
			writeDiagnostic(s.conn, diag.Line, deadline)
		}
		s.emitAuth("", false)
		return nil
	}
	s.releasePermit()
	_ = s.conn.SetDeadline(time.Time{})
	s.noise = sess
	s.emitAuth(peer.Mode, true)
	defer sess.Close()

	sess.SetReadProgressDeadline(s.readProgressTimeout())
	return s.runNoiseCommands()
}

func (s *connSession) emitAuth(mode string, ok bool) {
	if s.events.Sink.Enabled() {
		s.events.Sink.Emit("auth", "", events.F("conn", s.events.Conn), events.F("mode", mode), events.F("ok", ok))
	}
}

// runNoiseCommands serves commands on an authenticated session until the
// client closes, a deadline expires, or the session fails.
func (s *connSession) runNoiseCommands() error {
	ctx, end := s.commandContext()
	defer end()
	cmdReader := bufio.NewReader(nil)
	in := &noiseRequest{} // reused for every command, so serving one allocates nothing here
	for {
		mr, err := s.noise.Reader()
		if err != nil {
			return nil
		}
		cmdReader.Reset(mr)
		payload, err := readCommandLine(cmdReader, maxCommandLineBytes)
		if err != nil {
			return nil // benign close, deadline, or an unfinished request
		}
		*in = noiseRequest{br: cmdReader}
		req, err := ParseRequest(payload)
		if err != nil {
			// A malformed command line can only be answered once the
			// request is known to have ended.
			if completeRequest(in) != nil {
				return nil
			}
			_ = s.respondNoiseError(err)
			return nil
		}
		if !requestHasBody(req.Verb) {
			if completeRequest(in) != nil {
				return nil
			}
		}
		if !s.serveOne(ctx, req, in) {
			return nil
		}
	}
}

// requestHasBody reports whether a verb's request carries bytes after its
// command line; those verbs complete their own request.
func requestHasBody(v Verb) bool {
	return VerbHasRequestItems(v) || v == VerbSYNC || v == VerbPROBE
}

// respondNoiseError writes one encrypted ERR response.
func (s *connSession) respondNoiseError(err error) error {
	w := s.noise.Writer(false)
	_ = writeErrFrame(w, err)
	return w.End()
}

// requestDrainLimit bounds how much of a failed request the server reads to
// reach its END: the largest request any verb may send. The limits are on
// decoded bytes, so 1/64 extra covers frame headers and incompressible
// zstd blocks on the wire.
func (s *connSession) requestDrainLimit() int64 {
	decoded := max(encoding.MaxRequestBytes, s.maxSyncBodyBytes)
	return decoded + decoded/64 + maxCommandLineBytes
}

// cxsumProgressInterval bounds how long finished checksums wait in a
// batching transport before they are sent.
const cxsumProgressInterval = 250 * time.Millisecond

// transportFlusher returns w's flush when w is a transport that batches
// writes into records, so a handler can send a completed unit early. Plain
// connections have nothing to flush.
func transportFlusher(w io.Writer) func() error {
	if mw, ok := w.(*aead.MessageWriter); ok {
		return mw.FlushFunc()
	}
	return nil
}
