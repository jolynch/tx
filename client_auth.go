package tx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/utils"
)

const (
	// maxConcurrentHandshakes bounds a client's handshakes in flight, shared
	// by its pools, discovery, and token selection, so filling a large pool
	// cannot occupy a server's whole admission capacity.
	maxConcurrentHandshakes = 16
	// busyRetries is how many times a client retries a server's ERR BUSY,
	// backing off from busyBackoffMin to busyBackoffMax.
	busyRetries    = 4
	busyBackoffMin = 50 * time.Millisecond
	busyBackoffMax = 400 * time.Millisecond
)

var (
	errNoiseRawIO   = errors.New("authenticated connection used without its session")
	errPlainOnNoise = errors.New("encrypted client has no authenticated session on this connection")
	errMissingOK    = errors.New("response ended without its terminal status")
)

// tcpAuthState is a client's resolved transport configuration, shared by its
// pools. When hasAuth is set every connection runs a Noise handshake; the
// session itself belongs to the connection.
type tcpAuthState struct {
	hasAuth   bool
	mode      string   // aead.ModeToken or aead.ModeAge
	tokens    []string // token candidates in configured order
	static    aead.KeyPair
	serverKey [32]byte
	encMode   string // resolved cipher: "aes" or "chacha20"
	// tokenPref is the index of the token that last authenticated, shared
	// by every pool; -1 until one succeeds.
	tokenPref *atomic.Int32
}

// noiseConn is an authenticated connection. Its bytes travel only through
// its session; direct Read and Write fail so nothing can bypass it.
type noiseConn struct {
	net.Conn
	sess *aead.Session
}

func (c *noiseConn) Read([]byte) (int, error)  { return 0, errNoiseRawIO }
func (c *noiseConn) Write([]byte) (int, error) { return 0, errNoiseRawIO }
func (c *noiseConn) Close() error              { return c.sess.Close() }

// SyscallConn exposes the socket for liveness peeks.
func (c *noiseConn) SyscallConn() (syscall.RawConn, error) {
	if sc, ok := c.Conn.(syscall.Conn); ok {
		return sc.SyscallConn()
	}
	return nil, errors.New("connection has no syscall access")
}

// noiseSessionOf returns conn's authenticated session, or nil.
func noiseSessionOf(conn net.Conn) *aead.Session {
	if sc, ok := conn.(*sessionTCPConn); ok {
		conn = sc.Conn
	}
	if nc, ok := conn.(*noiseConn); ok {
		return nc.sess
	}
	return nil
}

// streamSession returns the session under a managed response stream, or nil.
func streamSession(rc any) *aead.Session {
	r, ok := rc.(*readerWithCloser)
	if !ok {
		return nil
	}
	closer, ok := r.Closer.(*managedTCPConnCloser)
	if !ok {
		return nil
	}
	return noiseSessionOf(closer.conn)
}

// responseEndTimeout bounds waiting for END after a response's status line
// when the caller set no response deadline. A server writes END with the
// status line, so it is normally already buffered.
const responseEndTimeout = 5 * time.Second

// finishResponse verifies, on an authenticated connection, that the
// response read through br ended exactly at its authenticated END. br must be
// the outermost parser buffer so trailing bytes cannot hide in read-ahead.
func finishResponse(conn net.Conn, br io.Reader) error {
	if noiseSessionOf(conn) == nil {
		return nil
	}
	if err := aead.FinishMessage(br); err != nil {
		return fmt.Errorf("response did not end cleanly: %w", err)
	}
	return nil
}

// finishStreamResponse marks a consumed stream reusable once its response
// is known to have ended exactly.
func finishStreamResponse(rc io.Closer, br io.Reader) error {
	if streamSession(rc) != nil {
		if err := aead.FinishMessage(br); err != nil {
			return fmt.Errorf("response did not end cleanly: %w", err)
		}
	}
	markStreamReusable(rc)
	return nil
}

// newTCPRequestWriter returns a writer for one outbound command plus a close
// function that finalizes it. On an authenticated connection each command is
// one Noise message ending in END; plain connections write straight through.
func (c *Client) newTCPRequestWriter(conn net.Conn, state tcpAuthState) (io.Writer, func() error, error) {
	if sess := noiseSessionOf(conn); sess != nil {
		w := sess.Writer(false)
		return w, w.EndFunc(), nil
	}
	if state.hasAuth {
		return nil, nil, errPlainOnNoise
	}
	return conn, func() error { return nil }, nil
}

// responseReaderForTCP returns the reader for the next response: one Noise
// message on an authenticated connection, the socket otherwise.
func (c *Client) responseReaderForTCP(conn net.Conn, state tcpAuthState) (io.Reader, error) {
	if sess := noiseSessionOf(conn); sess != nil {
		return sess.Reader()
	}
	if state.hasAuth {
		return nil, errPlainOnNoise
	}
	return conn, nil
}

// handshakeSlots is the client's shared bound on handshakes in flight.
func (c *Client) handshakeSlots() chan struct{} {
	c.hsOnce.Do(func() { c.hsSlots = make(chan struct{}, maxConcurrentHandshakes) })
	return c.hsSlots
}

// handshakeSlot is one held handshake slot. It covers a dial and a
// handshake, which on a server that requires auth hold one of its admission
// permits; retryBusy gives it up while backing off.
type handshakeSlot struct {
	slots chan struct{}
	held  bool
}

func (c *Client) acquireHandshakeSlot(ctx context.Context) (*handshakeSlot, error) {
	s := &handshakeSlot{slots: c.handshakeSlots()}
	if err := s.reacquire(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// reacquire waits for a slot, unless one is already held.
func (s *handshakeSlot) reacquire(ctx context.Context) error {
	if s.held {
		return nil
	}
	select {
	case s.slots <- struct{}{}:
		s.held = true
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release gives the slot up; it is idempotent.
func (s *handshakeSlot) release() {
	if s.held {
		s.held = false
		<-s.slots
	}
}

// localKeyPair returns the client's key pair: its configured age identity,
// or one in-memory pair generated once per client and never written.
func (c *Client) localKeyPair() (aead.KeyPair, error) {
	identity := c.ClientAgeIdentity
	recipient := c.ClientAgePublicKey
	if identity == "" {
		if recipient != "" {
			return aead.KeyPair{}, errors.New("an age recipient alone cannot authenticate; supply its identity")
		}
		if c.localKey == nil {
			kp, err := aead.GenerateKeyPair()
			if err != nil {
				return aead.KeyPair{}, err
			}
			c.localKey = &kp
		}
		return *c.localKey, nil
	}
	kp, err := aead.ParseIdentityKey(identity)
	if err != nil {
		return aead.KeyPair{}, err
	}
	if recipient != "" {
		pub, err := aead.ParseRecipientKey(recipient)
		if err != nil {
			return aead.KeyPair{}, fmt.Errorf("client age recipient: %w", err)
		}
		if pub != kp.Public {
			return aead.KeyPair{}, errors.New("client age recipient does not match its identity")
		}
	}
	return kp, nil
}

// resolveTCPAuthState validates the client's credentials and, when the
// client encrypts, discovers the server's key once and caches the result for
// every pool. Discovery failures are not cached.
func (c *Client) resolveTCPAuthState(ctx context.Context) (tcpAuthState, error) {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	if c.authState != nil {
		return *c.authState, nil
	}
	encMode := strings.ToLower(strings.TrimSpace(c.EncryptMode))
	switch encMode {
	case "", "none", "auto", "aes", "chacha20":
	default:
		return tcpAuthState{}, fmt.Errorf("unsupported encrypt mode: %s", encMode)
	}
	for i, tok := range c.ClientAuthTokens {
		if err := aead.ValidateAuthToken(tok); err != nil {
			return tcpAuthState{}, fmt.Errorf("auth token %d: %w", i+1, err)
		}
	}
	hasToken := len(c.ClientAuthTokens) > 0
	hasIdentity := c.ClientAgeIdentity != "" || c.ClientAgePublicKey != ""
	if (hasToken || hasIdentity) && encMode == "none" {
		return tcpAuthState{}, errors.New("encryption cannot be none when a credential is configured")
	}
	var pin [32]byte
	pinned := strings.TrimSpace(c.RequiredServerRecipient) != ""
	if pinned {
		key, err := aead.ParseRecipientKey(c.RequiredServerRecipient)
		if err != nil {
			return tcpAuthState{}, fmt.Errorf("required server recipient: %w", err)
		}
		pin = key
	}
	if !hasToken && !hasIdentity && (encMode == "" || encMode == "none") {
		if pinned {
			return tcpAuthState{}, errors.New("a required server recipient requires authenticated encryption")
		}
		c.authState = &tcpAuthState{}
		return *c.authState, nil
	}
	if encMode == "" {
		encMode = "auto"
	}
	static, err := c.localKeyPair()
	if err != nil {
		return tcpAuthState{}, err
	}

	hint, serverKey, err := c.discoverServerKey(ctx)
	if err != nil {
		return tcpAuthState{}, fmt.Errorf("discover server key: %w", err)
	}
	if pinned && serverKey != pin {
		return tcpAuthState{}, errors.New("server key does not match the required server recipient")
	}
	cipher, err := aead.ResolveCipher(encMode, hint)
	if err != nil {
		return tcpAuthState{}, err
	}
	state := &tcpAuthState{
		hasAuth:   true,
		mode:      aead.ModeAge,
		static:    static,
		serverKey: serverKey,
		encMode:   string(cipher),
		tokenPref: new(atomic.Int32),
	}
	state.tokenPref.Store(-1)
	if hasToken {
		state.mode = aead.ModeToken
		state.tokens = append([]string(nil), c.ClientAuthTokens...)
	}
	c.authState = state
	return *state, nil
}

// authDeadline is the shared deadline for one authentication: five seconds,
// or the caller's earlier deadline.
func authDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(aead.HandshakeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return deadline
}

// retryBusy runs attempt, retrying a server's ERR BUSY with jittered
// exponential backoff within deadline and ctx. It gives up slot while it
// sleeps, so a backing-off client does not hold back other handshakes.
func retryBusy(ctx context.Context, deadline time.Time, slot *handshakeSlot, attempt func() error) error {
	// Waiting to get the slot back counts against the same deadline.
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	backoff := busyBackoffMin
	for i := 0; ; i++ {
		err := attempt()
		if !errors.Is(err, aead.ErrServerBusy) || i == busyRetries {
			return err
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		if time.Until(deadline) <= wait {
			return err
		}
		slot.release()
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := slot.reacquire(ctx); err != nil {
			return err
		}
		backoff = min(backoff*2, busyBackoffMax)
	}
}

// discoverServerKey asks for the server's public key and cipher hint on a
// connection of its own: `AUTH key noise-v1` answered by
// `OK <cipher> <recipient>`.
func (c *Client) discoverServerKey(ctx context.Context) (aead.Algorithm, [32]byte, error) {
	var hint aead.Algorithm
	var key [32]byte
	slot, err := c.acquireHandshakeSlot(ctx)
	if err != nil {
		return "", key, err
	}
	defer slot.release()
	deadline := authDeadline(ctx)
	err = retryBusy(ctx, deadline, slot, func() error {
		dctx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		conn, err := c.dialTCP(dctx)
		if err != nil {
			return fmt.Errorf("dial for key exchange: %w", err)
		}
		defer conn.Close()
		defer watchManagedTCPConnContext(dctx, conn)()
		_ = conn.SetDeadline(deadline)
		if err := writeTCPLine(conn, "AUTH key "+aead.PrefaceVersion); err != nil {
			return err
		}
		line, err := readBoundedLine(bufio.NewReader(conn), aead.MaxPrefaceBytes)
		if err != nil {
			return fmt.Errorf("read key exchange response: %w", err)
		}
		if strings.TrimSpace(line) == aead.DiagBusy {
			return aead.ErrServerBusy
		}
		msg, ok := parseOKStatusLine(line)
		if !ok {
			if lineErr := parseErrControlFrame(c.metrics(), line); lineErr != nil {
				return lineErr
			}
			return fmt.Errorf("unexpected key exchange response: %q", line)
		}
		parts := strings.Split(msg, " ")
		if len(parts) != 2 {
			return fmt.Errorf("malformed AUTH key response: %q", msg)
		}
		if parts[0] != string(aead.AlgorithmAES) && parts[0] != string(aead.AlgorithmChaCha20) {
			return fmt.Errorf("unsupported server cipher: %s", parts[0])
		}
		k, err := aead.ParseRecipientKey(parts[1])
		if err != nil {
			return fmt.Errorf("invalid server public key: %w", err)
		}
		hint, key = aead.Algorithm(parts[0]), k
		return nil
	})
	return hint, key, err
}

// readBoundedLine reads one CRLF line of at most limit bytes.
func readBoundedLine(br *bufio.Reader, limit int) (string, error) {
	line, err := utils.ReadLineLimit(br, limit)
	if err != nil {
		return "", err
	}
	text, ok := strings.CutSuffix(string(line), "\r\n")
	if !ok {
		return "", errors.New("line not terminated by CRLF")
	}
	return text, nil
}

// dialAndAuthWithState dials a connection and, when the client encrypts,
// authenticates it. Token candidates are tried in configured order,
// starting with the one that last succeeded; each attempt uses a fresh
// connection and handshake.
func (c *Client) dialAndAuthWithState(ctx context.Context, state tcpAuthState) (net.Conn, error) {
	if !state.hasAuth {
		conn, err := c.dialTCP(ctx)
		if err != nil {
			return nil, fmt.Errorf("dial file listener: %w", err)
		}
		return conn, nil
	}
	slot, err := c.acquireHandshakeSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer slot.release()
	start := time.Now()
	deadline := authDeadline(ctx)
	order := []int{-1}
	if state.mode == aead.ModeToken {
		order = order[:0]
		pref := int(state.tokenPref.Load())
		if pref >= 0 && pref < len(state.tokens) {
			order = append(order, pref)
		}
		for i := range state.tokens {
			if i != pref {
				order = append(order, i)
			}
		}
	}
	attempts := 0
	var lastErr error
	for _, idx := range order {
		cfg := aead.ClientConfig{
			Mode:      state.mode,
			Cipher:    aead.Algorithm(state.encMode),
			Static:    state.static,
			ServerKey: state.serverKey,
		}
		if idx >= 0 {
			cfg.Token = state.tokens[idx]
		}
		var conn net.Conn
		err := retryBusy(ctx, deadline, slot, func() error {
			attempts++
			var err error
			conn, err = c.handshakeOnce(ctx, deadline, cfg)
			return err
		})
		if err == nil {
			if idx >= 0 {
				state.tokenPref.Store(int32(idx))
			}
			c.emitHandshake(conn, start, attempts, nil)
			return conn, nil
		}
		lastErr = err
		// Only an authentication refusal moves on to the next token.
		if !errors.Is(err, aead.ErrNotAuthorized) && !errors.Is(err, aead.ErrHandshakeRejected) {
			break
		}
	}
	c.emitHandshake(nil, start, attempts, lastErr)
	return nil, fmt.Errorf("authenticate: %w", lastErr)
}

// handshakeOnce dials and runs one handshake under deadline.
func (c *Client) handshakeOnce(ctx context.Context, deadline time.Time, cfg aead.ClientConfig) (net.Conn, error) {
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	raw, err := c.dialTCP(dctx)
	if err != nil {
		return nil, fmt.Errorf("dial file listener: %w", err)
	}
	stopWatch := watchManagedTCPConnContext(dctx, raw)
	_ = raw.SetDeadline(deadline)
	sess, err := aead.ClientHandshake(raw, cfg)
	if stopWatch() {
		if sess != nil {
			_ = sess.Close()
		}
		_ = raw.Close()
		return nil, fmt.Errorf("authenticate: %w", context.Cause(dctx))
	}
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	_ = raw.SetDeadline(time.Time{})
	return &noiseConn{Conn: raw, sess: sess}, nil
}

func (c *Client) emitHandshake(conn net.Conn, start time.Time, attempts int, err error) {
	s := c.sink()
	if !s.Enabled() {
		return
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	s.Emit("handshake", "", events.F("conn", c.connID(conn)), events.Dur("dur", time.Since(start)),
		events.F("attempts", attempts), events.F("err", msg))
}

// dialAndAuth resolves auth state, then dials and authenticates one
// connection. The caller closes the returned connection.
func (c *Client) dialAndAuth(ctx context.Context) (net.Conn, tcpAuthState, error) {
	state, err := c.resolveTCPAuthState(ctx)
	if err != nil {
		return nil, tcpAuthState{}, err
	}
	conn, err := c.dialAndAuthWithState(ctx, state)
	if err != nil {
		return nil, tcpAuthState{}, err
	}
	return conn, state, nil
}
