package tx

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jolynch/tx/internal/events"
	"golang.org/x/sys/unix"
)

// A probed client keeps two connection pools that never share connections.
// The data pool carries SEND and CXSUM streams, which run for a whole batch;
// it holds a fixed number of connections and makes callers wait when all are
// in use, which also bounds how many streams run at once. The control pool
// carries ACK, STATUS, TXFER, and SYNC; it dials whenever it has no idle
// connection, so quick commands never queue behind file transfers.

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

// tcpPoolClass selects which of a client's pools a request uses.
type tcpPoolClass uint8

const (
	poolControl tcpPoolClass = iota
	poolData
)

var (
	// errTCPPoolStopped reports an acquire on a pool that Client.Close stopped.
	errTCPPoolStopped = errors.New("connection pool stopped")
	// errKeepAliveWithdrawn reports a server that stopped granting keep-alive;
	// the pool has switched to single-use connections.
	errKeepAliveWithdrawn = errors.New("server withdrew keep-alive")
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

// sendSlots is a transfer's budget of concurrent data streams, sized to match
// the data pool. A batch waits for one slot, then splits only into slots free
// at that moment, so no SEND is issued unless a data connection is ready for
// it. A nil sendSlots is unlimited.
type sendSlots chan struct{}

func (s sendSlots) acquire(ctx context.Context) error {
	if s == nil {
		return nil
	}
	select {
	case s <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryAcquire takes up to n free slots without waiting and returns how many it
// took.
func (s sendSlots) tryAcquire(n int) int {
	if s == nil {
		return n
	}
	for got := 0; got < n; got++ {
		select {
		case s <- struct{}{}:
		default:
			return got
		}
	}
	return n
}

func (s sendSlots) release(n int) {
	if s == nil {
		return
	}
	for i := 0; i < n; i++ {
		<-s
	}
}

// tcpPoolOptions sizes one pool. A pool opens size connections when it
// starts, keeps at most size idle, and never scales down below one.
type tcpPoolOptions struct {
	size int
	// limit caps open connections: idle, borrowed, and dialing. Zero means
	// unbounded, so acquire dials instead of waiting.
	limit int
}

// keepFor is how many connections a pool keeps open after an interval whose
// peak use was peak: that many plus a quarter for bursts, at least one.
func keepFor(peak int) int {
	return max(1, peak+(peak+3)/4)
}

type tcpConnPool struct {
	ctx       context.Context
	cancel    context.CancelFunc
	authState tcpAuthState
	opts      tcpPoolOptions

	// idle is a stack of idle connections, the most recently used on top, so
	// borrowers reuse the same few connections and the least recently used
	// sink to the bottom, where scale-down closes them. ready holds one token
	// per idle connection: len(ready) is the idle count, and a waiter can
	// select on it. Taking a token always precedes removing a connection,
	// which keeps the two in step under concurrent borrowers.
	idleMu sync.Mutex
	idle   []net.Conn
	ready  chan struct{}
	// slots holds one token per open connection when opts.limit > 0: a send
	// takes a slot and a receive returns it. Nil when unbounded.
	slots chan struct{}

	// sessionMS is the server's keep-alive grant in milliseconds when the
	// pool keeps reusable session connections; zero selects single-use
	// connections.
	sessionMS atomic.Int64
	// refilling counts background fills in flight; each holds a slot.
	refilling atomic.Int64
	// waits counts acquires that found the pool full and had to wait.
	waits atomic.Int64
	// borrowed counts connections handed out by acquire and not yet returned;
	// peak is the most borrowed at once since the last heartbeat tick, and
	// used reports whether anything was ever borrowed.
	borrowed atomic.Int64
	peak     atomic.Int64
	used     atomic.Bool
	// target is the open count fills restore to: opts.size at first, then
	// what the last heartbeat tick decided to keep.
	target  atomic.Int64
	stopped atomic.Bool

	// hbSem bounds concurrent heartbeat probes; pool-lifetime so ticks don't
	// reallocate it.
	hbSem chan struct{}
}

// sessionTCPConn marks a pooled connection that negotiated keep-alive via
// PROBE. Only session connections are recycled back into the pool; other
// connections stay single-use because the server closes them after one
// command.
type sessionTCPConn struct {
	net.Conn
	// lastActive is touched whenever the connection completes a command or
	// heartbeat. Owned by whichever goroutine currently holds the
	// connection, or by the pool, under idleMu, while it is idle.
	lastActive time.Time
	// lastUsed orders idle sessions by their last command return or initial
	// fill. Ordinary enqueue updates it under idleMu; probes leave it alone.
	lastUsed time.Time
}

func newTCPConnPool(state tcpAuthState, opts tcpPoolOptions, sessionMS int64) *tcpConnPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &tcpConnPool{
		ctx:       ctx,
		cancel:    cancel,
		authState: state,
		opts:      opts,
		ready:     make(chan struct{}, max(1, opts.size)),
		hbSem:     make(chan struct{}, keepAliveHeartbeatConcurrency),
	}
	if opts.limit > 0 {
		p.slots = make(chan struct{}, opts.limit)
	}
	p.sessionMS.Store(sessionMS)
	p.target.Store(int64(opts.size))
	return p
}

// takeSlot reserves room for one more open connection without waiting.
func (p *tcpConnPool) takeSlot() bool {
	if p.slots == nil {
		return true
	}
	select {
	case p.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// putSlot frees the slot of a connection that closed or never opened.
func (p *tcpConnPool) putSlot() {
	if p.slots == nil {
		return
	}
	select {
	case <-p.slots:
	default:
	}
}

// rejectReason names why enqueue refused a connection, for conn_close events.
func (p *tcpConnPool) rejectReason() string {
	if p.stopped.Load() {
		return "stopped"
	}
	return "idle_full"
}

// discard closes a pool-owned connection and frees its slot.
func (p *tcpConnPool) discard(c *Client, conn net.Conn, reason string) error {
	c.noteConnEvent("conn_close", conn, reason)
	err := conn.Close()
	p.putSlot()
	return err
}

// takeTop removes the most recently used idle connection. The caller holds
// the token for it.
func (p *tcpConnPool) takeTop() net.Conn {
	p.idleMu.Lock()
	defer p.idleMu.Unlock()
	n := len(p.idle) - 1
	conn := p.idle[n]
	p.idle[n] = nil
	p.idle = p.idle[:n]
	return conn
}

// claimIdle takes an idle token without waiting. Callers hold idleMu when
// they then remove a connection other than the top one.
func (p *tcpConnPool) claimIdle() bool {
	select {
	case <-p.ready:
		return true
	default:
		return false
	}
}

// borrow takes the most recently used idle connection without waiting.
func (p *tcpConnPool) borrow() (net.Conn, bool) {
	if p == nil || p.stopped.Load() || !p.claimIdle() {
		return nil, false
	}
	return p.takeTop(), true
}

// enqueue puts conn on top of the idle stack, reporting false when the stack
// is full or the pool stopped; the caller still owns conn in that case.
func (p *tcpConnPool) enqueue(conn net.Conn) bool {
	if p == nil || conn == nil || p.stopped.Load() {
		return false
	}
	now := time.Now()
	p.idleMu.Lock()
	if len(p.idle) >= cap(p.ready) {
		p.idleMu.Unlock()
		return false
	}
	if sc, ok := conn.(*sessionTCPConn); ok {
		sc.lastUsed = now
	}
	p.idle = append(p.idle, conn)
	p.idleMu.Unlock()
	p.ready <- struct{}{}
	if p.stopped.Load() {
		// stop drained the stack before this connection landed.
		p.drain()
	}
	return true
}

// enqueueProbed returns a healthy session in last-use order. Probe completion
// order must not change which idle connections scale-down closes first.
func (p *tcpConnPool) enqueueProbed(c *Client, conn *sessionTCPConn) {
	if p.stopped.Load() {
		_ = p.discard(c, conn, "stopped")
		return
	}
	p.idleMu.Lock()
	if len(p.idle) >= cap(p.ready) {
		p.idleMu.Unlock()
		_ = p.discard(c, conn, "idle_full")
		return
	}
	at := slices.IndexFunc(p.idle, func(idle net.Conn) bool {
		sc, ok := idle.(*sessionTCPConn)
		return !ok || sc.lastUsed.After(conn.lastUsed)
	})
	if at < 0 {
		at = len(p.idle)
	}
	p.idle = slices.Insert(p.idle, at, net.Conn(conn))
	p.idleMu.Unlock()
	p.ready <- struct{}{}
	if p.stopped.Load() {
		p.drain()
	}
}

// trimIdle closes up to k of the least recently used idle connections.
func (p *tcpConnPool) trimIdle(c *Client, k int) {
	p.idleMu.Lock()
	n := 0
	for n < k && n < len(p.idle) && p.claimIdle() {
		n++
	}
	victims := append([]net.Conn(nil), p.idle[:n]...)
	p.idle = append(p.idle[:0], p.idle[n:]...)
	p.idleMu.Unlock()
	for _, conn := range victims {
		_ = p.discard(c, conn, "idle")
	}
}

// takeStale removes idle sessions with no traffic or probe for a whole
// interval, oldest first, for the heartbeat to probe.
func (p *tcpConnPool) takeStale(interval time.Duration) []*sessionTCPConn {
	p.idleMu.Lock()
	defer p.idleMu.Unlock()
	var stale []*sessionTCPConn
	kept := p.idle[:0]
	for i, conn := range p.idle {
		sc, isSession := conn.(*sessionTCPConn)
		if isSession && time.Since(sc.lastActive) >= interval && p.claimIdle() {
			stale = append(stale, sc)
			continue
		}
		kept = append(kept, p.idle[i])
	}
	clear(p.idle[len(kept):])
	p.idle = kept
	return stale
}

func (p *tcpConnPool) stop() {
	if p == nil || p.stopped.Swap(true) {
		return
	}
	p.cancel()
	p.drain()
}

func (p *tcpConnPool) drain() {
	for p.claimIdle() {
		_ = p.takeTop().Close()
		p.putSlot()
	}
}

// acquire returns a live idle connection, or dials a new one while the pool
// is under its limit, or waits until a connection or slot comes back.
func (p *tcpConnPool) acquire(ctx context.Context, c *Client) (net.Conn, error) {
	conn, err := p.acquireConn(ctx, c)
	if err != nil {
		return nil, err
	}
	n := p.borrowed.Add(1)
	for peak := p.peak.Load(); n > peak && !p.peak.CompareAndSwap(peak, n); peak = p.peak.Load() {
	}
	p.used.Store(true)
	return conn, nil
}

// returned records that a connection acquire handed out came back, whether
// it was recycled or closed.
func (p *tcpConnPool) returned() {
	p.borrowed.Add(-1)
}

func (p *tcpConnPool) acquireConn(ctx context.Context, c *Client) (net.Conn, error) {
	for {
		if p.stopped.Load() {
			return nil, errTCPPoolStopped
		}
		if conn, ok := p.borrow(); ok {
			if p.alive(c, conn) {
				return conn, nil
			}
			continue
		}
		if !p.takeSlot() {
			p.waits.Add(1)
			select {
			case <-p.ready:
				if conn := p.takeTop(); p.alive(c, conn) {
					return conn, nil
				}
				continue
			case p.slots <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-p.ctx.Done():
				return nil, errTCPPoolStopped
			}
		}
		c.metrics().IncSyncConnectionFallback()
		conn, err := c.dialPoolConn(context.WithValue(ctx, syncDialKey{}, true), p)
		if errors.Is(err, errKeepAliveWithdrawn) {
			continue
		}
		return conn, err
	}
}

// alive reports whether a borrowed connection is usable, discarding it if
// not. A session can die between heartbeats, e.g. when the server restarts.
func (p *tcpConnPool) alive(c *Client, conn net.Conn) bool {
	sc, isSession := conn.(*sessionTCPConn)
	if !isSession || sessionConnAlive(sc) {
		return true
	}
	_ = p.discard(c, sc, "dead")
	return false
}

// dialPoolConn opens a connection for a slot the caller already holds,
// upgrading it to a keep-alive session when the server grants one. It frees
// the slot if no connection results.
func (c *Client) dialPoolConn(ctx context.Context, p *tcpConnPool) (net.Conn, error) {
	conn, err := c.dialAndAuthWithState(ctx, p.authState)
	if err != nil {
		p.putSlot()
		return nil, err
	}
	if p.sessionMS.Load() <= 0 {
		return conn, nil
	}
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.Close()
		close(cancelDone)
	})
	granted, _, err := c.probeKeepAliveSessionConn(conn, p.authState)
	if !stopCancel() {
		// The callback closed the connection. Join it before returning, so
		// it cannot close a connection handed out afterward.
		<-cancelDone
		err = ctx.Err()
	}
	if err == nil && !granted {
		// The server stopped granting keep-alive (e.g. restarted with it
		// disabled) and closes this connection after the probe.
		p.sessionMS.Store(0)
		err = errKeepAliveWithdrawn
	}
	if err != nil {
		_ = conn.Close()
		p.putSlot()
		return nil, err
	}
	sc := &sessionTCPConn{Conn: conn, lastActive: time.Now()}
	if err := ctx.Err(); err != nil {
		// The caller gave up after the upgrade finished: keep the healthy
		// session for the next borrower instead of wasting the dial.
		if !p.enqueue(sc) {
			_ = p.discard(c, sc, p.rejectReason())
		}
		return nil, err
	}
	return sc, nil
}

// openCount is the pool's idle, borrowed, and filling connections.
func (p *tcpConnPool) openCount() int {
	return len(p.ready) + int(p.borrowed.Load()) + int(p.refilling.Load())
}

// triggerRefill starts background fills until the pool's open connections
// reach its target. Each fill holds a slot, so refills never push the pool
// past its limit, and a scale-down lowers the target so replacement fills
// never undo it.
func (p *tcpConnPool) triggerRefill(c *Client) {
	if p == nil || c == nil {
		return
	}
	for !p.stopped.Load() {
		p.refilling.Add(1)
		if p.openCount() > int(p.target.Load()) || !p.takeSlot() {
			p.refilling.Add(-1)
			return
		}
		go c.fillTCPPoolConn(p)
	}
}

func (c *Client) fillTCPPoolConn(p *tcpConnPool) {
	filled := false
	defer func() {
		p.refilling.Add(-1)
		// A failed fill does not retry: the next acquire dials on demand
		// instead of this loop hammering an unreachable server.
		if filled && !p.stopped.Load() && p.openCount() < int(p.target.Load()) {
			p.triggerRefill(c)
		}
	}()
	conn, err := c.dialPoolConn(p.ctx, p)
	if err != nil {
		return
	}
	if !p.enqueue(conn) {
		_ = p.discard(c, conn, p.rejectReason())
		return
	}
	filled = true
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

// heartbeatIdleConns runs once per interval. It first scales the pool down to
// the interval's peak use plus a quarter, closing the least recently used
// idle connections, and lowers the refill target to match; a pool nothing has
// borrowed yet keeps its warm connections. It then probes every idle session
// with no traffic or probe for the whole interval and returns the healthy
// ones immediately, preserving their last-use order.
//
// It is only ever called from the single ticker goroutine in startHeartbeats,
// serially, and it wg.Wait()s for all probe goroutines (each of which
// releases its p.hbSem slot) before returning — so p.hbSem is fully drained
// at the start of every tick, making pool-lifetime reuse safe.
func (p *tcpConnPool) heartbeatIdleConns(c *Client, interval time.Duration) {
	borrowed := p.borrowed.Load()
	peak := max(p.peak.Swap(borrowed), borrowed)
	if p.used.Load() {
		keep := keepFor(int(peak))
		p.target.Store(int64(min(p.opts.size, keep)))
		if excess := len(p.ready) + int(borrowed) - keep; excess > 0 {
			p.trimIdle(c, excess)
		}
	}

	stale := p.takeStale(interval)
	var failed atomic.Bool
	var wg sync.WaitGroup
	for _, sc := range stale {
		wg.Add(1)
		p.hbSem <- struct{}{}
		go func(sc *sessionTCPConn) {
			defer wg.Done()
			defer func() { <-p.hbSem }()
			_, rttMillis, err := c.probeKeepAliveSessionConn(sc, p.authState)
			if err != nil {
				failed.Store(true)
				c.metrics().IncHeartbeatFailure()
				if s := c.sink(); s.Enabled() {
					s.Emit("heartbeat_fail", "", events.F("conn", c.connID(sc)), events.F("err", err.Error()))
				}
				_ = p.discard(c, sc, "heartbeat_fail")
				return
			}
			c.metrics().ObserveHeartbeat(rttMillis)
			if s := c.sink(); s.Enabled() {
				s.Emit("heartbeat", "", events.F("conn", c.connID(sc)), events.Dur("rtt", time.Duration(rttMillis)*time.Millisecond))
			}
			sc.lastActive = time.Now()
			p.enqueueProbed(c, sc)
		}(sc)
	}
	wg.Wait()
	if failed.Load() {
		p.triggerRefill(c)
	}
}

// ensureTCPPools creates the client's data and control pools on first use,
// sizing the data pool to concurrency, and returns the data pool size.
// Later calls keep the existing pools: the size is fixed for the life of
// the client.
func (c *Client) ensureTCPPools(state tcpAuthState, concurrency int) int {
	if concurrency <= 0 {
		return 0
	}
	c.tcpPoolMu.Lock()
	if c.dataPool != nil {
		size := c.dataPool.opts.limit
		c.tcpPoolMu.Unlock()
		return size
	}
	sessionMS := c.sessionKeepAliveMS()
	data := newTCPConnPool(state, tcpPoolOptions{size: concurrency, limit: concurrency}, sessionMS)
	control := newTCPConnPool(state, tcpPoolOptions{size: concurrency}, sessionMS)
	c.dataPool, c.controlPool = data, control
	c.tcpPoolMu.Unlock()
	for _, p := range []*tcpConnPool{data, control} {
		p.startHeartbeats(c)
		p.triggerRefill(c)
	}
	return concurrency
}

// sessionKeepAliveMS returns the cached server keep-alive grant, or zero
// when reuse is disabled or no probe has observed support yet.
func (c *Client) sessionKeepAliveMS() int64 {
	if c == nil || c.DisableKeepAlive {
		return 0
	}
	return c.keepAliveMS.Load()
}

func (c *Client) tcpPool(class tcpPoolClass) *tcpConnPool {
	c.tcpPoolMu.Lock()
	defer c.tcpPoolMu.Unlock()
	if class == poolData {
		return c.dataPool
	}
	return c.controlPool
}

// acquireManagedTCPConn returns a connection from the class's pool, or a
// direct single-use dial when the client has no pools (never probed, or
// closed). A caller already waiting on a pool when Client.Close stops it gets
// errTCPPoolStopped rather than a dial, so closing a busy client cannot set
// off a burst of direct dials.
func (c *Client) acquireManagedTCPConn(ctx context.Context, class tcpPoolClass) (net.Conn, tcpAuthState, *tcpConnPool, error) {
	if pool := c.tcpPool(class); pool != nil {
		conn, err := pool.acquire(ctx, c)
		if err != nil {
			return nil, tcpAuthState{}, nil, err
		}
		return conn, pool.authState, pool, nil
	}
	conn, state, err := c.dialAndAuth(ctx)
	if err != nil {
		return nil, tcpAuthState{}, nil, err
	}
	return conn, state, nil, nil
}

// releaseManagedTCPConn closes a connection whose response was not consumed
// cleanly, freeing its pool slot and starting a background replacement.
func (c *Client) releaseManagedTCPConn(conn net.Conn, pool *tcpConnPool) error {
	if conn == nil {
		return nil
	}
	if pool == nil {
		c.noteConnEvent("conn_close", conn, "released")
		return conn.Close()
	}
	pool.returned()
	err := pool.discard(c, conn, "released")
	pool.triggerRefill(c)
	return err
}

// recycleManagedTCPConn returns a session connection to the pool for reuse;
// non-session connections close as before. A full idle stack (the control
// pool after a burst) or a stopped pool closes the connection instead.
// Callers must only recycle a connection whose response was consumed through
// its terminal status line.
func (c *Client) recycleManagedTCPConn(conn net.Conn, pool *tcpConnPool) error {
	sc, ok := conn.(*sessionTCPConn)
	if !ok || pool == nil {
		return c.releaseManagedTCPConn(conn, pool)
	}
	pool.returned()
	sc.lastActive = time.Now()
	if !pool.enqueue(sc) {
		return pool.discard(c, conn, pool.rejectReason())
	}
	c.metrics().IncConnectionReuse()
	c.noteConnEvent("conn_reuse", conn, "")
	return nil
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
