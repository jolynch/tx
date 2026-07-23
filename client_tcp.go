package tx

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/aead"
	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
	ftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	intlimit "github.com/jolynch/tx/internal/filexfer/limit"
	"golang.org/x/sys/unix"
)

const maxTCPLineBytes = 4 * 1024 * 1024

const (
	// minKeepAliveHeartbeatInterval floors the heartbeat cadence so tiny
	// server grants cannot busy-loop the pool.
	minKeepAliveHeartbeatInterval = 100 * time.Millisecond
	// keepAliveHeartbeatTimeout bounds one heartbeat round trip.
	keepAliveHeartbeatTimeout = 5 * time.Second
	// keepAliveHeartbeatConcurrency caps concurrent heartbeat probes so a
	// tick never drains the whole pool at once.
	keepAliveHeartbeatConcurrency = 16
)

// heartbeatIntervalForGrant returns how often idle session connections are
// probed: one quarter of the server's granted idle window, so a healthy
// connection is always refreshed well before the server-side reaper fires.
func heartbeatIntervalForGrant(grantMS int64) time.Duration {
	interval := time.Duration(grantMS) * time.Millisecond / 4
	if interval < minKeepAliveHeartbeatInterval {
		return minKeepAliveHeartbeatInterval
	}
	return interval
}

type tcpAuthState struct {
	publicKey      string // client's age public key
	identity       string // client's age identity (private key)
	parsedIdentity age.Identity
	serverKey      string // server's age public key (discovered via AUTH key)
	hasAuth        bool
	encMode        string // resolved cipher: "aes" or "chacha20"
}

type probeResponse struct {
	ServerCPU       int
	ServerIODepth   int
	GentleCPUPct    int
	GentleBWPct     int
	CTS0            int64
	CTS1            int64
	STS0            int64
	STS1            int64
	ProbeBytes      int64
	ServerWmemBytes int64
	LimiterBps      int64
	KeepAliveMS     int64
}

type tcpConnPool struct {
	ctx       context.Context
	cancel    context.CancelFunc
	authState tcpAuthState
	target    int
	ready     chan net.Conn

	// sessionMS is the server's keep-alive grant in milliseconds when the
	// pool warms reusable session connections; zero selects legacy
	// single-use warm connections.
	sessionMS atomic.Int64
	refilling atomic.Int64
	stopped   atomic.Bool

	// hbSem bounds concurrent heartbeat probes; pool-lifetime so ticks don't
	// reallocate it.
	hbSem chan struct{}
}

// sessionTCPConn marks a pooled connection that negotiated keep-alive via
// PROBE. Only session connections are recycled back into the pool; sync
// fallback dials stay single-use because the server closes them after one
// command.
type sessionTCPConn struct {
	net.Conn
	// lastActive is touched whenever the connection completes a command or
	// heartbeat. Owned by whichever goroutine currently holds the
	// connection (it is never in the ready channel at the same time).
	lastActive time.Time
}

type managedTCPConnCloser struct {
	client *Client
	conn   net.Conn
	pool   *tcpConnPool

	reusable atomic.Bool
	once     sync.Once
	err      error
}

func (c *managedTCPConnCloser) markReusable() {
	if c == nil {
		return
	}
	c.reusable.Store(true)
}

// contextManagedTCPReadCloser wraps a managed connection read-closer with
// context-cancellation cleanup (stopWatch) for streaming reads (CXSUM). It
// deliberately never marks the underlying connection reusable: a CXSUM stream
// may be cancelled mid-response by the verifier's context, leaving the
// connection at an indeterminate boundary, so on Close the connection is
// always released (single-use) rather than recycled into the keep-alive pool.
type contextManagedTCPReadCloser struct {
	io.ReadCloser
	stopWatch func()

	once sync.Once
	err  error
}

func (c *managedTCPConnCloser) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		if c.reusable.Load() {
			c.err = c.client.recycleManagedTCPConn(c.conn, c.pool)
		} else {
			c.err = c.client.releaseManagedTCPConn(c.conn, c.pool)
		}
	})
	return c.err
}

func (r *contextManagedTCPReadCloser) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		if r.stopWatch != nil {
			r.stopWatch()
		}
		if r.ReadCloser != nil {
			r.err = r.ReadCloser.Close()
		}
	})
	return r.err
}

func warmTCPPoolTarget(suggestedConcurrency int) int {
	if suggestedConcurrency <= 0 {
		return 0
	}
	// 25% extra connections
	return max(1, (suggestedConcurrency*5+3)/4)
}

func newTCPConnPool(state tcpAuthState, target int, sessionMS int64) *tcpConnPool {
	if target <= 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &tcpConnPool{
		ctx:       ctx,
		cancel:    cancel,
		authState: state,
		target:    target,
		ready:     make(chan net.Conn, target),
		hbSem:     make(chan struct{}, keepAliveHeartbeatConcurrency),
	}
	p.sessionMS.Store(sessionMS)
	return p
}

func (p *tcpConnPool) borrow() (net.Conn, bool) {
	if p == nil {
		return nil, false
	}
	if p.stopped.Load() {
		return nil, false
	}
	// Non blocking, if there isn't one ready
	// just proceed
	select {
	case conn := <-p.ready:
		if conn == nil {
			return nil, false
		}
		return conn, true
	default:
		return nil, false
	}
}

func (p *tcpConnPool) enqueue(conn net.Conn) bool {
	if p == nil || conn == nil {
		return false
	}
	if p.stopped.Load() {
		return false
	}
	select {
	case p.ready <- conn:
		return true
	default:
		return false
	}
}

func (p *tcpConnPool) stop() {
	if p == nil {
		return
	}
	if p.stopped.Swap(true) {
		return
	}
	p.cancel()
	for {
		select {
		case conn := <-p.ready:
			if conn != nil {
				_ = conn.Close()
			}
		default:
			return
		}
	}
}

func (p *tcpConnPool) triggerRefill(c *Client) {
	if p == nil || c == nil {
		return
	}
	for {
		if p.stopped.Load() {
			return
		}
		inFlight := int(p.refilling.Add(1))
		if len(p.ready)+inFlight > p.target {
			remaining := int(p.refilling.Add(-1))
			if p.stopped.Load() || len(p.ready)+remaining >= p.target {
				return
			}
			continue
		}
		go c.fillTCPPoolConn(p, p.ctx, p.authState)
	}
}

func (c *Client) fillTCPPoolConn(pool *tcpConnPool, ctx context.Context, state tcpAuthState) {
	defer func() {
		remaining := int(pool.refilling.Add(-1))
		needMore := !pool.stopped.Load() && len(pool.ready)+remaining < pool.target
		if needMore {
			pool.triggerRefill(c)
		}
	}()

	conn, err := c.dialAndAuthWithState(ctx, state)
	if err != nil {
		return
	}
	if pool.sessionMS.Load() > 0 {
		granted, _, probeErr := c.probeKeepAliveSessionConn(conn, state)
		if probeErr != nil {
			_ = conn.Close()
			return
		}
		if !granted {
			// The server stopped granting keep-alive (e.g. restarted with
			// it disabled) and will close this connection after the probe;
			// fall back to single-use warm connections.
			pool.sessionMS.Store(0)
			_ = conn.Close()
			return
		}
		conn = &sessionTCPConn{Conn: conn, lastActive: time.Now()}
	}
	if !pool.enqueue(conn) {
		_ = conn.Close()
	}
}

// startHeartbeats launches the pool's keep-alive loop: idle pooled session
// connections get a zero-payload PROBE round trip at least once per
// interval, so a silently dead connection is evicted here instead of
// failing a borrower mid-transfer.
func (p *tcpConnPool) startHeartbeats(c *Client) {
	if p == nil || p.sessionMS.Load() <= 0 {
		return
	}
	interval := heartbeatIntervalForGrant(p.sessionMS.Load())
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				p.heartbeatIdleConns(c, interval)
			}
		}
	}()
}

// heartbeatIdleConns is only ever called from the single ticker goroutine in
// startHeartbeats, serially, and it wg.Wait()s for all probe goroutines (each
// of which releases its p.hbSem slot) before returning — so p.hbSem is fully
// drained at the start of every tick, making pool-lifetime reuse safe.
func (p *tcpConnPool) heartbeatIdleConns(c *Client, interval time.Duration) {
	n := len(p.ready)
	if n == 0 {
		return
	}
	var wg sync.WaitGroup
	// Each tick momentarily borrows all ready connections to check lastActive
	// and re-enqueues those that don't need probing, briefly draining the
	// pool — acceptable because a concurrent borrower racing the tick simply
	// falls through to the sync-dial fallback.
	for i := 0; i < n; i++ {
		conn, ok := p.borrow()
		if !ok {
			break
		}
		sc, isSession := conn.(*sessionTCPConn)
		if !isSession || time.Since(sc.lastActive) < interval {
			// Legacy warm connection, or one that just carried traffic —
			// no probe needed this tick.
			if !p.enqueue(conn) {
				_ = conn.Close()
			}
			continue
		}
		wg.Add(1)
		p.hbSem <- struct{}{}
		go func(sc *sessionTCPConn) {
			defer wg.Done()
			defer func() { <-p.hbSem }()
			_, rttMillis, err := c.probeKeepAliveSessionConn(sc, p.authState)
			if err != nil {
				c.clientMetrics.IncHeartbeatFailure()
				_ = sc.Close()
				p.triggerRefill(c)
				return
			}
			c.clientMetrics.ObserveHeartbeat(rttMillis)
			sc.lastActive = time.Now()
			if !p.enqueue(sc) {
				_ = sc.Close()
			}
		}(sc)
	}
	wg.Wait()
}

// probeKeepAliveSessionConn sends a zero-payload keep-alive PROBE on conn and
// reads the full response. Callers decide whether the probe is pool warm-up
// negotiation or a scheduled heartbeat and record metrics accordingly.
func (c *Client) probeKeepAliveSessionConn(conn net.Conn, state tcpAuthState) (bool, int64, error) {
	_ = conn.SetDeadline(time.Now().Add(keepAliveHeartbeatTimeout))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	start := time.Now()
	cmd := fmt.Sprintf("PROBE cpu=%d probe-bytes=0 cts0=%d keep-alive=auto", runtime.NumCPU(), start.UnixMilli())
	br, err := c.sendAndReadTCP(conn, state, cmd)
	if err != nil {
		return false, 0, err
	}
	line, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return false, 0, err
	}
	if err := parseErrControlFrame(line); err != nil {
		return false, 0, err
	}
	resp, err := parseProbeResponseLine(line)
	if err != nil {
		return false, 0, err
	}
	if _, err := readTCPStatus(br); err != nil {
		return false, 0, err
	}
	rttMillis := time.Since(start).Milliseconds()
	if resp.KeepAliveMS > 0 {
		c.keepAliveMS.Store(resp.KeepAliveMS)
		return true, rttMillis, nil
	}
	return false, rttMillis, nil
}

func (c *Client) ensureTCPPool(state tcpAuthState, suggestedConcurrency int) int {
	target := warmTCPPoolTarget(suggestedConcurrency)
	if target <= 0 {
		return 0
	}
	c.tcpPoolMu.Lock()
	if c.tcpPool != nil {
		target = c.tcpPool.target
		c.tcpPoolMu.Unlock()
		return target
	}
	pool := newTCPConnPool(state, target, c.sessionKeepAliveMS())
	c.tcpPool = pool
	c.tcpPoolMu.Unlock()
	pool.startHeartbeats(c)
	pool.triggerRefill(c)
	return target
}

// sessionKeepAliveMS returns the cached server keep-alive grant, or zero
// when reuse is disabled or no probe has observed support yet.
func (c *Client) sessionKeepAliveMS() int64 {
	if c == nil || c.DisableKeepAlive {
		return 0
	}
	return c.keepAliveMS.Load()
}

func (c *Client) acquireManagedTCPConn(ctx context.Context) (net.Conn, tcpAuthState, *tcpConnPool, error) {
	c.tcpPoolMu.Lock()
	pool := c.tcpPool
	c.tcpPoolMu.Unlock()
	if pool != nil {
		for {
			conn, ok := pool.borrow()
			if !ok {
				break
			}
			sc, isSession := conn.(*sessionTCPConn)
			if !isSession || sessionConnAlive(sc) {
				return conn, pool.authState, pool, nil
			}
			// Dead session (e.g. server restarted since the last
			// heartbeat) — evict and try the next pooled connection.
			_ = sc.Close()
			pool.triggerRefill(c)
		}
		c.clientMetrics.IncSyncConnectionFallback()
		conn, err := c.dialAndAuthWithState(ctx, pool.authState)
		if err != nil {
			return nil, tcpAuthState{}, pool, err
		}
		return conn, pool.authState, pool, nil
	}
	conn, state, err := c.dialAndAuth(ctx)
	if err != nil {
		return nil, tcpAuthState{}, nil, err
	}
	return conn, state, nil, nil
}

func (c *Client) releaseManagedTCPConn(conn net.Conn, pool *tcpConnPool) error {
	if conn == nil {
		if pool != nil {
			pool.triggerRefill(c)
		}
		return nil
	}
	err := conn.Close()
	if pool != nil {
		pool.triggerRefill(c)
	}
	return err
}

// sessionConnAlive does a non-blocking peek on an idle session connection: a
// healthy idle connection has nothing to read (EAGAIN), a server-closed one
// has a pending EOF/RST, and pending data means protocol desync. This closes
// the borrow-time race where the server went away after the last heartbeat.
func sessionConnAlive(sc *sessionTCPConn) bool {
	syscallConn, ok := sc.Conn.(syscall.Conn)
	if !ok {
		// Cannot peek this transport (custom dialer); assume alive rather
		// than evicting every pooled connection.
		return true
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return true
	}
	alive := true
	// The closure runs synchronously inside raw.Read before this function
	// returns, so capturing the stack array by reference keeps the peek
	// buffer off-heap on this per-borrow hot path.
	var peek [1]byte
	ctrlErr := raw.Read(func(fd uintptr) bool {
		_, _, recvErr := unix.Recvfrom(int(fd), peek[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
		alive = recvErr == unix.EAGAIN || recvErr == unix.EWOULDBLOCK
		return true // never block waiting for readability
	})
	if ctrlErr != nil {
		return true
	}
	return alive
}

// recycleManagedTCPConn returns a session connection to the pool for reuse;
// non-session connections (sync fallback dials, legacy warm connections)
// close as before. Callers must only recycle a connection whose response was
// consumed through its terminal status line.
func (c *Client) recycleManagedTCPConn(conn net.Conn, pool *tcpConnPool) error {
	sc, ok := conn.(*sessionTCPConn)
	if !ok || pool == nil {
		return c.releaseManagedTCPConn(conn, pool)
	}
	sc.lastActive = time.Now()
	if !pool.enqueue(sc) {
		return c.releaseManagedTCPConn(conn, pool)
	}
	c.clientMetrics.IncConnectionReuse()
	return nil
}

// finishManagedTCPConn recycles conn into the pool when the response was
// cleanly consumed, otherwise closes it.
func (c *Client) finishManagedTCPConn(conn net.Conn, pool *tcpConnPool, clean bool) {
	if clean {
		_ = c.recycleManagedTCPConn(conn, pool)
		return
	}
	_ = c.releaseManagedTCPConn(conn, pool)
}

func (c *Client) newManagedTCPReadCloser(reader io.Reader, conn net.Conn, pool *tcpConnPool) io.ReadCloser {
	return &readerWithCloser{
		Reader: reader,
		Closer: &managedTCPConnCloser{
			client: c,
			conn:   conn,
			pool:   pool,
		},
	}
}

func watchManagedTCPConnContext(ctx context.Context, conn net.Conn) func() {
	if ctx == nil || conn == nil {
		return func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
		})
	}
}

func (c *Client) dialTCP(ctx context.Context) (net.Conn, error) {
	if c == nil {
		return nil, errors.New("nil client")
	}
	addr := strings.TrimSpace(c.FileAddr)
	if addr == "" {
		return nil, errors.New("missing file listener address")
	}
	dialer := c.contextDialer
	if dialer == nil {
		dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			if c.SocketReadBufferBytes > 0 {
				_ = tc.SetReadBuffer(c.SocketReadBufferBytes)
			}
		}
		return conn, nil
	}
	conn, err := dialer(ctx, addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		if c.SocketReadBufferBytes > 0 {
			_ = tc.SetReadBuffer(c.SocketReadBufferBytes)
		}
	}
	return conn, nil
}

func makeLenToken(raw string) string {
	return strconv.Itoa(len(raw)) + ":" + raw
}

func parseErrControlFrame(line string) error {
	code, msg, ok := parseErrControlPayload(line)
	if !ok {
		return nil
	}
	return controlFrameError{Code: code, Message: msg}
}

type controlFrameError struct {
	Code    string
	Message string
}

func (e controlFrameError) Error() string {
	if strings.TrimSpace(e.Message) == "" {
		return e.Code
	}
	return strings.TrimSpace(e.Code + " " + e.Message)
}

func parseErrControlPayload(line string) (code string, message string, ok bool) {
	msg := strings.TrimSpace(line)
	if msg == "" {
		return "", "", false
	}
	if strings.HasPrefix(msg, "ERR ") {
		rest := strings.TrimSpace(strings.TrimPrefix(msg, "ERR "))
		if rest == "" {
			return "ERR", "", true
		}
		code, message, hasMessage := strings.Cut(rest, " ")
		if !hasMessage {
			return strings.TrimSpace(code), "", true
		}
		return strings.TrimSpace(code), strings.TrimSpace(message), true
	}
	return "", "", false
}

func parseOKStatusLine(line string) (message string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "OK" {
		return "", true
	}
	if strings.HasPrefix(trimmed, "OK ") {
		return strings.TrimSpace(strings.TrimPrefix(trimmed, "OK ")), true
	}
	return "", false
}

func isStatusLine(line string) bool {
	if _, ok := parseOKStatusLine(line); ok {
		return true
	}
	_, _, ok := parseErrControlPayload(line)
	return ok
}

func readTCPLine(br *bufio.Reader, maxBytes int) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if maxBytes > 0 && len(line) > maxBytes {
		return "", errors.New("line too large")
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func writeTCPLine(w io.Writer, line string) error {
	_, err := io.WriteString(w, line+"\r\n")
	return err
}

func (c *Client) resolveTCPAuthState(ctx context.Context) (tcpAuthState, error) {
	encMode := strings.ToLower(strings.TrimSpace(c.EncryptMode))
	if encMode == "" || encMode == "none" {
		return tcpAuthState{}, nil
	}
	if encMode != "auto" && encMode != "aes" && encMode != "chacha20" {
		return tcpAuthState{}, fmt.Errorf("unsupported encrypt mode: %s", encMode)
	}

	// Generate ephemeral client identity if not provided.
	requestPub := strings.TrimSpace(c.ClientAgePublicKey)
	requestIdentity := strings.TrimSpace(c.ClientAgeIdentity)
	if requestPub == "" || requestIdentity == "" {
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			return tcpAuthState{}, fmt.Errorf("generate age identity: %w", err)
		}
		requestPub = identity.Recipient().String()
		requestIdentity = identity.String()
	}

	parsed, err := parseAgeIdentity(requestIdentity)
	if err != nil {
		return tcpAuthState{}, err
	}

	// Discover the server's public key and recommended cipher via AUTH key.
	recommendedCipher, serverKey, err := c.discoverServerKey(ctx)
	if err != nil {
		return tcpAuthState{}, fmt.Errorf("discover server key: %w", err)
	}

	// Resolve "auto" to the server's recommended cipher.
	if encMode == "auto" {
		encMode = recommendedCipher
	}

	return tcpAuthState{
		publicKey:      requestPub,
		identity:       requestIdentity,
		parsedIdentity: parsed,
		serverKey:      serverKey,
		hasAuth:        true,
		encMode:        encMode,
	}, nil
}

// discoverServerKey sends AUTH key on a fresh connection and reads back the
// server's recommended cipher and public key from the OK response.
// Response format: "OK <cipher> <pubkey>\r\n"
func (c *Client) discoverServerKey(ctx context.Context) (recommendedCipher string, serverKey string, err error) {
	conn, dialErr := c.dialTCP(ctx)
	if dialErr != nil {
		return "", "", fmt.Errorf("dial for key exchange: %w", dialErr)
	}
	defer conn.Close()
	if err := writeTCPLine(conn, "AUTH key"); err != nil {
		return "", "", err
	}
	br := bufio.NewReader(conn)
	line, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return "", "", fmt.Errorf("read key exchange response: %w", err)
	}
	msg, ok := parseOKStatusLine(line)
	if !ok {
		if lineErr := parseErrControlFrame(line); lineErr != nil {
			return "", "", lineErr
		}
		return "", "", fmt.Errorf("unexpected key exchange response: %s", line)
	}
	parts := strings.Fields(msg)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("malformed AUTH key response: expected '<cipher> <pubkey>', got %q", msg)
	}
	cipher := parts[0]
	key := parts[1]
	if cipher != "aes" && cipher != "chacha20" {
		return "", "", fmt.Errorf("unsupported server cipher: %s", cipher)
	}
	if _, err := age.ParseX25519Recipient(key); err != nil {
		return "", "", fmt.Errorf("invalid server public key: %w", err)
	}
	return cipher, key, nil
}

func (c *Client) sendTCPAuth(conn net.Conn, state tcpAuthState) error {
	if !state.hasAuth {
		return nil
	}
	recipient, err := age.ParseX25519Recipient(state.serverKey)
	if err != nil {
		return fmt.Errorf("parse server key: %w", err)
	}
	// Encrypt the client's public key to the server using AEAD. If the
	// client has auth tokens, append them space-delimited after the pubkey.
	encrypted := c.acquireScratchBuffer(0)
	defer c.releaseScratchBuffer(encrypted)
	ew, encErr := aead.Encrypt(encrypted, recipient, aeadOptionsForMode(state.encMode))
	if encErr != nil {
		return encErr
	}
	plain := state.publicKey
	for _, tok := range c.ClientAuthTokens {
		plain += " " + tok
	}
	if _, err := ew.Write([]byte(plain)); err != nil {
		return err
	}
	if err := ew.Close(); err != nil {
		return err
	}
	encoded := base64.StdEncoding.EncodeToString(encrypted.Bytes())
	return writeTCPLine(conn, "AUTH "+state.encMode+" "+encoded)
}

func aeadOptionsForMode(mode string) aead.Options {
	switch mode {
	case "aes":
		return aead.Options{Algorithm: aead.AlgorithmAES}
	case "chacha20":
		return aead.Options{Algorithm: aead.AlgorithmChaCha20}
	default:
		return aead.Options{} // RecommendedCipher
	}
}

// newTCPRequestWriter returns a writer for one outbound command plus a close
// function that finalizes it. Encrypted sessions get a fresh AEAD stream per
// command (each command self-delimiting on a kept-alive connection); plain
// sessions write straight to the conn. Mirrors the server's newResponseWriter.
func (c *Client) newTCPRequestWriter(conn net.Conn, state tcpAuthState) (io.Writer, func() error, error) {
	if !state.hasAuth {
		return conn, func() error { return nil }, nil
	}
	recipient, err := age.ParseX25519Recipient(state.serverKey)
	if err != nil {
		return nil, nil, err
	}
	ew, err := aead.Encrypt(conn, recipient, aeadOptionsForMode(state.encMode))
	if err != nil {
		return nil, nil, err
	}
	return ew, ew.Close, nil
}

func (c *Client) sendTCPCommand(conn net.Conn, state tcpAuthState, payload string) error {
	w, closeRequest, err := c.newTCPRequestWriter(conn, state)
	if err != nil {
		return err
	}
	if err := writeTCPLine(w, payload); err != nil {
		_ = closeRequest()
		return err
	}
	return closeRequest()
}

func (c *Client) responseReaderForTCP(conn net.Conn, state tcpAuthState) (io.Reader, error) {
	if !state.hasAuth {
		return conn, nil
	}
	identity := state.parsedIdentity
	if identity == nil {
		return nil, errors.New("missing identity for encrypted response")
	}
	decReader, err := aead.Decrypt(conn, identity)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}
	return decReader, nil
}

func (c *Client) dialAndAuthWithState(ctx context.Context, state tcpAuthState) (net.Conn, error) {
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial file listener: %w", err)
	}
	if err := c.sendTCPAuth(conn, state); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("send AUTH: %w", err)
	}
	return conn, nil
}

// dialAndAuth resolves auth state, dials a TCP connection, and sends the
// AUTH handshake. The caller is responsible for closing the returned connection.
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

// sendAndReadTCP sends a command on the connection and returns a buffered
// reader for the (possibly decrypted) response stream.
func (c *Client) sendAndReadTCP(conn net.Conn, state tcpAuthState, cmd string) (*bufio.Reader, error) {
	if err := c.sendTCPCommand(conn, state, cmd); err != nil {
		return nil, err
	}
	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(responseReader), nil
}

func (c *Client) getManifestTCP(ctx context.Context, request GetManifestRequest) (GetManifestResponse, error) {
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return GetManifestResponse{}, err
	}
	clean := false
	defer func() { c.finishManagedTCPConn(conn, pool, clean) }()

	comp := request.Comp
	if comp == "" {
		comp = intencoding.EncodingZstd
	}
	cmd := "TXFER " + makeLenToken(request.Directory)
	if request.Verbose {
		cmd += " verbose=1"
	}
	cmd += " mode=" + request.Mode
	cmd += " link-mbps=" + strconv.FormatInt(request.LinkMbps, 10)
	cmd += " concurrency=" + strconv.Itoa(request.Concurrency)
	if request.DeadlineMS > 0 {
		cmd += " deadline-ms=" + strconv.FormatInt(request.DeadlineMS, 10)
	}
	cacheMap := strings.ToLower(strings.TrimSpace(request.CacheMap))
	if cacheMap != "" && cacheMap != "none" {
		cmd += " cache-map=" + cacheMap
	}
	cmd += " comp=" + comp
	br, err := c.sendAndReadTCP(conn, state, cmd)
	if err != nil {
		return GetManifestResponse{}, fmt.Errorf("TXFER: %w", err)
	}

	mr := intencoding.NewChunkedManifestReader(br, intencoding.ChunkedManifestReaderOpts{
		RawSink: request.RawSink,
		OnFrame: func(s intencoding.ManifestFrameStats) {
			if request.ManifestProgress == nil {
				return
			}
			select {
			case request.ManifestProgress <- ManifestProgressUpdate{
				FrameIndex:   s.FrameIndex,
				WireBytes:    s.WireBytes,
				LogicalBytes: s.LogicalBytes,
				TotalWire:    s.TotalWire,
				TotalLogical: s.TotalLogical,
				Terminal:     s.Terminal,
			}:
			default:
			}
		},
	})
	raw, err := io.ReadAll(mr)
	if err != nil {
		return GetManifestResponse{}, fmt.Errorf("read TXFER manifest: %w", err)
	}
	statusLine, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return GetManifestResponse{}, fmt.Errorf("read TXFER status: %w", err)
	}
	if _, ok := parseOKStatusLine(statusLine); !ok {
		if err := parseErrControlFrame(statusLine); err != nil {
			return GetManifestResponse{}, err
		}
		return GetManifestResponse{}, fmt.Errorf("unexpected TXFER terminator: %q", statusLine)
	}
	clean = true
	manifest, err := parseManifest(raw)
	if err != nil {
		return GetManifestResponse{}, err
	}
	return GetManifestResponse{Manifest: manifest}, nil
}

func (c *Client) syncManifestTCP(ctx context.Context, request SyncManifestRequest) (SyncManifestResponse, error) {
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return SyncManifestResponse{}, err
	}
	clean := false
	defer func() { c.finishManagedTCPConn(conn, pool, clean) }()

	comp := request.Comp
	if comp == "" {
		comp = intencoding.EncodingZstd
	}

	// Send SYNC command line.
	cmd := "SYNC " + makeLenToken(request.Directory) +
		" mode=" + request.Mode +
		" link-mbps=" + strconv.FormatInt(request.LinkMbps, 10) +
		" concurrency=" + strconv.Itoa(request.Concurrency)
	if request.DeadlineMS > 0 {
		cmd += " deadline-ms=" + strconv.FormatInt(request.DeadlineMS, 10)
	}
	cmd += " comp=" + comp
	cacheMap := strings.ToLower(strings.TrimSpace(request.CacheMap))
	if cacheMap != "" && cacheMap != "none" {
		cmd += " cache-map=" + cacheMap
	}
	// The command line and the framed manifest body must travel in the same
	// AEAD stream on encrypted sessions (the server reads both through one
	// per-command decrypt reader), mirroring how sendTCPProbe carries the
	// probe payload.
	requestDst, closeRequest, err := c.newTCPRequestWriter(conn, state)
	if err != nil {
		return SyncManifestResponse{}, err
	}
	if err := writeTCPLine(requestDst, cmd); err != nil {
		_ = closeRequest()
		return SyncManifestResponse{}, fmt.Errorf("send SYNC: %w", err)
	}

	// Send the old manifest as an FX/1 + FXT/1 framed stream — mirrors TXFER's
	// response framing so the server can validate the body chunk-by-chunk.
	oldManifestBytes, err := marshalManifest(request.OldManifest)
	if err != nil {
		_ = closeRequest()
		return SyncManifestResponse{}, fmt.Errorf("marshal old manifest: %w", err)
	}
	requestWriter := intencoding.NewChunkedManifestWriter(requestDst, comp, intencoding.DefaultManifestChunkSize, intencoding.DefaultManifestFlushInterval)
	if _, err := requestWriter.Write(oldManifestBytes); err != nil {
		_ = closeRequest()
		return SyncManifestResponse{}, fmt.Errorf("write old manifest: %w", err)
	}
	if err := requestWriter.Close(); err != nil {
		_ = closeRequest()
		return SyncManifestResponse{}, fmt.Errorf("flush old manifest frames: %w", err)
	}
	if err := closeRequest(); err != nil {
		return SyncManifestResponse{}, fmt.Errorf("finalize SYNC request stream: %w", err)
	}

	// Build ID→path index from the old manifest so we can resolve RM fileIDs.
	oldByID := make(map[uint64]string, len(request.OldManifest.Entries))
	for _, e := range request.OldManifest.Entries {
		oldByID[e.ID] = e.Path
	}

	// Read response: framed FM/1 + interleaved RM lines, then verb OK.
	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		return SyncManifestResponse{}, fmt.Errorf("initialize SYNC response stream: %w", err)
	}
	br := bufio.NewReader(responseReader)
	mr := intencoding.NewChunkedManifestReader(br, intencoding.ChunkedManifestReaderOpts{})

	manifestBuf := c.acquireScratchBuffer(maxTCPLineBytes)
	defer c.releaseScratchBuffer(manifestBuf)
	var rmPaths []string

	deframed := bufio.NewReader(mr)
	for {
		line, readErr := deframed.ReadString('\n')
		if len(line) > 0 {
			trimmed := strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(trimmed, "RM ") {
				fileID, parseErr := strconv.ParseUint(strings.TrimSpace(trimmed[3:]), 10, 64)
				if parseErr != nil {
					return SyncManifestResponse{}, fmt.Errorf("parse RM line: %w", parseErr)
				}
				path, ok := oldByID[fileID]
				if !ok {
					return SyncManifestResponse{}, fmt.Errorf("RM fileID %d not in old manifest", fileID)
				}
				rmPaths = append(rmPaths, path)
			} else if trimmed != "" {
				manifestBuf.WriteString(trimmed)
				manifestBuf.WriteByte('\n')
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return SyncManifestResponse{}, fmt.Errorf("read SYNC response: %w", readErr)
		}
	}

	statusLine, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return SyncManifestResponse{}, fmt.Errorf("read SYNC status: %w", err)
	}
	if _, ok := parseOKStatusLine(statusLine); !ok {
		if err := parseErrControlFrame(statusLine); err != nil {
			return SyncManifestResponse{}, err
		}
		return SyncManifestResponse{}, fmt.Errorf("unexpected SYNC terminator: %q", statusLine)
	}
	clean = true

	manifest, parseErr := parseManifest(manifestBuf.Bytes())
	if parseErr != nil {
		return SyncManifestResponse{}, parseErr
	}
	return SyncManifestResponse{
		Manifest:     manifest,
		RemovedPaths: rmPaths,
	}, nil
}

func (c *Client) fetchFileBatchTCP(
	ctx context.Context,
	txferID string,
	targets []FetchFileTarget,
) (io.ReadCloser, error) {
	if txferID == "" {
		return nil, errors.New("missing transfer id")
	}
	if len(targets) == 0 {
		return nil, errors.New("missing file targets")
	}
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	loadStrategy := normalizeLoadStrategy(c.LoadStrategy)
	b.WriteString("SEND ")
	b.WriteString(txferID)
	for _, t := range targets {
		if strings.TrimSpace(t.FullPath) == "" {
			_ = c.releaseManagedTCPConn(conn, pool)
			return nil, errors.New("missing full path")
		}
		b.WriteString(" fd=")
		b.WriteString(strconv.FormatUint(t.FileID, 10))
		b.WriteString(" ")
		b.WriteString(makeLenToken(t.FullPath))
		b.WriteString(" mode=")
		b.WriteString(loadStrategy)
		if t.Comp != "" {
			b.WriteString(" comp=")
			b.WriteString(t.Comp)
		}
		if t.Offset != 0 {
			b.WriteString(" offset=")
			b.WriteString(strconv.FormatInt(t.Offset, 10))
		}
		if t.Size > 0 {
			b.WriteString(" size=")
			b.WriteString(strconv.FormatInt(t.Size, 10))
		}
	}
	br, err := c.sendAndReadTCP(conn, state, b.String())
	if err != nil {
		_ = c.releaseManagedTCPConn(conn, pool)
		return nil, fmt.Errorf("SEND batch: %w", err)
	}
	firstLine, err := readSENDFirstLine(br)
	if err != nil {
		_ = c.releaseManagedTCPConn(conn, pool)
		return nil, err
	}
	return c.newManagedTCPReadCloser(io.MultiReader(strings.NewReader(firstLine), br), conn, pool), nil
}

func readTCPStatus(br *bufio.Reader) (string, error) {
	line, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return "", err
	}
	if err := parseErrControlFrame(line); err != nil {
		return "", err
	}
	message, ok := parseOKStatusLine(line)
	if !ok {
		return "", fmt.Errorf("unexpected response: %s", strings.TrimSpace(line))
	}
	return message, nil
}

// readStreamFirstLine reads and validates the first line of a streaming
// response (SEND, CXSUM). Returns the raw first line (including trailing
// newline) for prefixing back onto the stream, or an error if the server
// sent ERR or an unexpected OK.
func readStreamFirstLine(br *bufio.Reader, verb string) (string, error) {
	firstLine, err := br.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read %s response: %w", verb, err)
	}
	trimmed := strings.TrimRight(firstLine, "\r\n")
	if err := parseErrControlFrame(trimmed); err != nil {
		return "", err
	}
	if _, ok := parseOKStatusLine(trimmed); ok {
		return "", fmt.Errorf("unexpected OK response for %s", verb)
	}
	return firstLine, nil
}

// readSENDFirstLine reads the first line of a SEND streaming response,
// wrapping NOT_FOUND errors as ErrFileMissing.
func readSENDFirstLine(br *bufio.Reader) (string, error) {
	firstLine, err := readStreamFirstLine(br, "SEND")
	if err != nil {
		var controlErr controlFrameError
		if errors.As(err, &controlErr) && strings.EqualFold(controlErr.Code, "NOT_FOUND") {
			return "", fmt.Errorf("%w: %w", ErrFileMissing, &fileMissingError{Status: 404, Body: strings.TrimSpace(controlErr.Message)})
		}
		return "", err
	}
	return firstLine, nil
}

func (c *Client) probeTCP(ctx context.Context, req ProbeRequest, probeBytes int64) (probeResponse, error) {
	if probeBytes <= 0 {
		return probeResponse{}, errors.New("probe bytes must be > 0")
	}
	conn, state, err := c.dialAndAuth(ctx)
	if err != nil {
		return probeResponse{}, err
	}
	defer conn.Close()

	cts0 := time.Now().UnixMilli()
	localCPU := runtime.NumCPU()
	cmd := fmt.Sprintf("PROBE cpu=%d probe-bytes=%d cts0=%d", localCPU, probeBytes, cts0)
	if !c.DisableKeepAlive {
		cmd += " keep-alive=auto"
	}
	if txferID := strings.TrimSpace(req.TransferID); txferID != "" {
		cmd += " txferid=" + txferID
		if req.ObservedLinkMbps > 0 {
			cmd += " obs-link-mbps=" + strconv.FormatInt(req.ObservedLinkMbps, 10)
		}
	}
	if err := c.sendTCPProbe(conn, state, cmd, probeBytes); err != nil {
		return probeResponse{}, fmt.Errorf("send PROBE: %w", err)
	}

	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		return probeResponse{}, fmt.Errorf("initialize PROBE response stream: %w", err)
	}
	capture := &firstReadTimestampReader{reader: responseReader}
	br := bufio.NewReader(capture)
	line, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return probeResponse{}, fmt.Errorf("read PROBE response line: %w", err)
	}
	if err := parseErrControlFrame(line); err != nil {
		return probeResponse{}, err
	}
	probeResp, err := parseProbeResponseLine(line)
	if err != nil {
		return probeResponse{}, err
	}
	if probeResp.ProbeBytes != probeBytes {
		return probeResponse{}, fmt.Errorf("probe byte mismatch: sent=%d got=%d", probeBytes, probeResp.ProbeBytes)
	}
	if _, err := io.CopyN(io.Discard, br, probeResp.ProbeBytes); err != nil {
		return probeResponse{}, fmt.Errorf("read PROBE payload: %w", err)
	}
	if _, err := readTCPStatus(br); err != nil {
		return probeResponse{}, fmt.Errorf("read PROBE status: %w", err)
	}
	probeResp.CTS1 = capture.FirstTS()
	if probeResp.CTS1 == 0 {
		probeResp.CTS1 = time.Now().UnixMilli()
	}
	if probeResp.KeepAliveMS > 0 {
		c.keepAliveMS.Store(probeResp.KeepAliveMS)
	}
	return probeResp, nil
}

func (c *Client) sendTCPProbe(conn net.Conn, state tcpAuthState, cmd string, probeBytes int64) error {
	w, closeRequest, err := c.newTCPRequestWriter(conn, state)
	if err != nil {
		return err
	}
	if err := writeTCPLine(w, cmd); err != nil {
		_ = closeRequest()
		return err
	}
	if _, err := io.CopyN(w, rand.Reader, probeBytes); err != nil {
		_ = closeRequest()
		return err
	}
	return closeRequest()
}

type firstReadTimestampReader struct {
	reader io.Reader
	first  int64
}

func (r *firstReadTimestampReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.first == 0 {
		r.first = time.Now().UnixMilli()
	}
	return n, err
}

func (r *firstReadTimestampReader) FirstTS() int64 {
	if r == nil {
		return 0
	}
	return r.first
}

func parseProbeResponseLine(line string) (probeResponse, error) {
	req, err := ftcp.ParseRequest([]byte(strings.TrimSpace(line)))
	if err != nil {
		return probeResponse{}, fmt.Errorf("parse PROBE response: %w", err)
	}
	if req.Verb != ftcp.VerbPROBE || len(req.Params) != 1 {
		return probeResponse{}, errors.New("invalid PROBE response")
	}
	p := req.Params[0]
	serverCPU, err := strconv.Atoi(strings.TrimSpace(p["cpu"]))
	if err != nil || serverCPU <= 0 {
		return probeResponse{}, errors.New("invalid PROBE response cpu")
	}
	cts0, err := strconv.ParseInt(strings.TrimSpace(p["cts0"]), 10, 64)
	if err != nil || cts0 < 0 {
		return probeResponse{}, errors.New("invalid PROBE response cts0")
	}
	sts0, err := strconv.ParseInt(strings.TrimSpace(p["sts0"]), 10, 64)
	if err != nil || sts0 < 0 {
		return probeResponse{}, errors.New("invalid PROBE response sts0")
	}
	sts1, err := strconv.ParseInt(strings.TrimSpace(p["sts1"]), 10, 64)
	if err != nil || sts1 < 0 {
		return probeResponse{}, errors.New("invalid PROBE response sts1")
	}
	probeBytes, err := strconv.ParseInt(strings.TrimSpace(p["probe-bytes"]), 10, 64)
	if err != nil || probeBytes < 0 {
		return probeResponse{}, errors.New("invalid PROBE response probe-bytes")
	}
	ioDepth, _ := strconv.Atoi(strings.TrimSpace(p["io-depth"]))
	if ioDepth <= 0 {
		ioDepth = 1
	}
	gentleCPUPct, _ := strconv.Atoi(strings.TrimSpace(p["gentle-cpu-pct"]))
	gentleCPUPct = intlimit.NormalizeGentleCPUPct(gentleCPUPct)
	gentleBWPct, _ := strconv.Atoi(strings.TrimSpace(p["gentle-bw-pct"]))
	gentleBWPct = intlimit.NormalizeGentleBWPct(gentleBWPct)
	wmemBytes, _ := strconv.ParseInt(strings.TrimSpace(p["wmem"]), 10, 64)
	if wmemBytes < 0 {
		wmemBytes = 0
	}
	limiterBps, _ := strconv.ParseInt(strings.TrimSpace(p["limiter-bps"]), 10, 64)
	if limiterBps < 0 {
		limiterBps = 0
	}
	keepAliveMS, _ := strconv.ParseInt(strings.TrimSpace(p["keep-alive-ms"]), 10, 64)
	if keepAliveMS < 0 {
		keepAliveMS = 0
	}
	return probeResponse{
		ServerCPU:       serverCPU,
		ServerIODepth:   ioDepth,
		GentleCPUPct:    gentleCPUPct,
		GentleBWPct:     gentleBWPct,
		CTS0:            cts0,
		STS0:            sts0,
		STS1:            sts1,
		ProbeBytes:      probeBytes,
		ServerWmemBytes: wmemBytes,
		LimiterBps:      limiterBps,
		KeepAliveMS:     keepAliveMS,
	}, nil
}

func (c *Client) acknowledgeFileProgressTCP(ctx context.Context, request AcknowledgeFileProgressRequest, ackToken string) (AcknowledgeFileProgressResponse, error) {
	return c.acknowledgeFileProgressBatchTCP(ctx, []acknowledgeFileProgressCommand{
		{request: request, ackToken: ackToken},
	})
}

func (c *Client) acknowledgeFileProgressBatchTCP(ctx context.Context, commands []acknowledgeFileProgressCommand) (AcknowledgeFileProgressResponse, error) {
	if len(commands) == 0 {
		return AcknowledgeFileProgressResponse{}, errors.New("missing ack requests")
	}
	txferID := strings.TrimSpace(commands[0].request.TransferID)
	if txferID == "" {
		return AcknowledgeFileProgressResponse{}, errors.New("missing transfer id")
	}
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return AcknowledgeFileProgressResponse{}, err
	}
	clean := false
	defer func() { c.finishManagedTCPConn(conn, pool, clean) }()

	var cmd strings.Builder
	cmd.WriteString("ACK ")
	cmd.WriteString(txferID)
	for _, ack := range commands {
		request := ack.request
		if request.TransferID != txferID {
			return AcknowledgeFileProgressResponse{}, errors.New("ack requests must share transfer id")
		}
		cmd.WriteString(" fd=")
		cmd.WriteString(strconv.FormatUint(request.FileID, 10))
		cmd.WriteString(" ")
		cmd.WriteString(makeLenToken(request.FullPath))
		cmd.WriteString(" ack-token=")
		cmd.WriteString(ack.ackToken)
		if request.AckBytes >= 0 {
			cmd.WriteString(" delta-bytes=")
			cmd.WriteString(strconv.FormatInt(request.DeltaBytes, 10))
			cmd.WriteString(" recv-ms=")
			cmd.WriteString(strconv.FormatInt(request.RecvMS, 10))
			cmd.WriteString(" sync-ms=")
			cmd.WriteString(strconv.FormatInt(request.SyncMS, 10))
		}
	}
	br, err := c.sendAndReadTCP(conn, state, cmd.String())
	if err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("ACK: %w", err)
	}
	if _, err := readTCPStatus(br); err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("read ACK response: %w", err)
	}
	clean = true
	return AcknowledgeFileProgressResponse{}, nil
}

func (c *Client) getStatusTCP(ctx context.Context, request GetStatusRequest) (GetStatusResponse, error) {
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return GetStatusResponse{}, err
	}
	clean := false
	defer func() { c.finishManagedTCPConn(conn, pool, clean) }()

	br, err := c.sendAndReadTCP(conn, state, "STATUS "+request.TransferID)
	if err != nil {
		return GetStatusResponse{}, fmt.Errorf("STATUS: %w", err)
	}
	message, err := readTCPStatus(br)
	if err != nil {
		return GetStatusResponse{}, fmt.Errorf("read STATUS response: %w", err)
	}
	clean = true
	if strings.TrimSpace(message) == "" {
		return GetStatusResponse{}, errors.New("missing STATUS JSON payload")
	}
	var status TransferStatus
	if err := json.NewDecoder(strings.NewReader(message)).Decode(&status); err != nil {
		return GetStatusResponse{}, fmt.Errorf("decode transfer status: %w", err)
	}
	return GetStatusResponse{Status: &status}, nil
}

func (c *Client) listStatusesTCP(ctx context.Context, request ListStatusesRequest) (ListStatusesResponse, error) {
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return ListStatusesResponse{}, err
	}
	clean := false
	defer func() { c.finishManagedTCPConn(conn, pool, clean) }()

	br, err := c.sendAndReadTCP(conn, state, "STATUS")
	if err != nil {
		return ListStatusesResponse{}, fmt.Errorf("STATUS list: %w", err)
	}
	countLine, err := readTCPStatus(br)
	if err != nil {
		return ListStatusesResponse{}, fmt.Errorf("read STATUS response: %w", err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(countLine))
	if err != nil {
		return ListStatusesResponse{}, fmt.Errorf("parse STATUS count %q: %w", countLine, err)
	}
	statuses := make([]TransferStatus, 0, count)
	for i := 0; i < count; i++ {
		line, lineErr := readTCPLine(br, maxTCPLineBytes)
		if lineErr != nil {
			return ListStatusesResponse{}, fmt.Errorf("read STATUS entry %d: %w", i, lineErr)
		}
		var s TransferStatus
		if jsonErr := json.Unmarshal([]byte(strings.TrimSpace(line)), &s); jsonErr != nil {
			return ListStatusesResponse{}, fmt.Errorf("decode STATUS entry %d: %w", i, jsonErr)
		}
		statuses = append(statuses, s)
	}
	clean = true
	return ListStatusesResponse{Statuses: statuses}, nil
}

func (c *Client) getChecksumTCP(ctx context.Context, request GetChecksumRequest) (io.ReadCloser, error) {
	conn, state, pool, err := c.acquireManagedTCPConn(ctx)
	if err != nil {
		return nil, err
	}
	stopWatch := watchManagedTCPConnContext(ctx, conn)

	var cmd strings.Builder
	cmd.WriteString("CXSUM ")
	cmd.WriteString(request.TransferID)
	for _, target := range request.Targets {
		cmd.WriteString(" fd=")
		cmd.WriteString(strconv.FormatUint(target.FileID, 10))
		cmd.WriteByte(' ')
		cmd.WriteString(makeLenToken(target.FullPath))
		if target.Offset > 0 {
			cmd.WriteString(" offset=")
			cmd.WriteString(strconv.FormatInt(target.Offset, 10))
		}
		if target.Size > 0 {
			cmd.WriteString(" size=")
			cmd.WriteString(strconv.FormatInt(target.Size, 10))
		}
		if algo := strings.TrimSpace(target.Algo); algo != "" {
			cmd.WriteString(" algo=")
			cmd.WriteString(algo)
		}
	}
	br, err := c.sendAndReadTCP(conn, state, cmd.String())
	if err != nil {
		stopWatch()
		_ = c.releaseManagedTCPConn(conn, pool)
		return nil, fmt.Errorf("CXSUM: %w", err)
	}
	firstLine, err := readStreamFirstLine(br, "CXSUM")
	if err != nil {
		stopWatch()
		_ = c.releaseManagedTCPConn(conn, pool)
		return nil, err
	}
	return &contextManagedTCPReadCloser{
		ReadCloser: c.newManagedTCPReadCloser(io.MultiReader(strings.NewReader(firstLine), br), conn, pool),
		stopWatch:  stopWatch,
	}, nil
}
