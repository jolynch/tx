package tx

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
)

// waitForTCPPoolsReady waits until the data and control pools hold the given
// idle connection counts with no fills in flight.
func waitForTCPPoolsReady(t *testing.T, client *Client, data, control int) {
	t.Helper()
	settled := func(p *tcpConnPool, want int) bool {
		return p != nil && len(p.ready) == want && p.refilling.Load() == 0
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if settled(client.tcpPool(poolData), data) && settled(client.tcpPool(poolControl), control) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	describe := func(p *tcpConnPool) string {
		if p == nil {
			return "nil"
		}
		return fmt.Sprintf("ready=%d refilling=%d", len(p.ready), p.refilling.Load())
	}
	t.Fatalf("timed out waiting for pools data=%d control=%d: data %s, control %s",
		data, control, describe(client.tcpPool(poolData)), describe(client.tcpPool(poolControl)))
}

func TestProbeLinkWarmsTCPPool(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		case intftcp.VerbSTATUS:
			_, err := io.WriteString(out, "OK {\"transfer_id\":\"tx123\"}\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	probe, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1})
	if err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	if probe.SuggestedConcurrency != 2 {
		t.Fatalf("expected suggested concurrency 2, got %d", probe.SuggestedConcurrency)
	}
	if probe.WarmConnectionPoolSize != 2 {
		t.Fatalf("expected data pool size 2, got %d", probe.WarmConnectionPoolSize)
	}

	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	pool := client.tcpPool(poolData)
	if pool == nil {
		t.Fatal("expected data pool to be initialized")
	}
	if pool.opts.limit != 2 {
		t.Fatalf("expected data pool limit 2, got %d", pool.opts.limit)
	}
}

func TestTCPPoolUsesWarmedConnectionForStatus(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		case intftcp.VerbSTATUS:
			_, err := io.WriteString(out, "OK {\"transfer_id\":\"tx123\"}\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	dialer.SetSuccessLimit(5)
	statusResp, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"})
	if err != nil {
		t.Fatalf("GetStatus failed using warmed connection: %v", err)
	}
	if statusResp.Status == nil || statusResp.Status.TransferID != "tx123" {
		t.Fatalf("unexpected status response: %+v", statusResp.Status)
	}
	if got := dialer.SuccessCount(); got != 5 {
		t.Fatalf("expected no new successful dial for warmed status request, got %d", got)
	}
}

func TestTCPPoolRefillsAfterShortResponse(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		case intftcp.VerbSTATUS:
			_, err := io.WriteString(out, "OK {\"transfer_id\":\"tx123\"}\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	dialer.SetSuccessLimit(6)
	if _, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"}); err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 6)
	waitForTCPPoolsReady(t, client, 2, 2)
}

// TestTCPPoolsIsolateDataFromControl fills the data pool with open CXSUM
// streams. Another data request waits rather than dialing, while STATUS still
// runs on the control pool, which dials once it has no idle connection.
func TestTCPPoolsIsolateDataFromControl(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		case intftcp.VerbSTATUS:
			_, err := io.WriteString(out, "OK {\"transfer_id\":\"tx123\"}\r\n")
			return err
		case intftcp.VerbCXSUM:
			_, err := io.WriteString(out, "CXSUM fid=1 algo=xxh128 token=deadbeef\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	dialer.SetSuccessLimit(5)
	readers := make([]io.ReadCloser, 0, 2)
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()
	checksum := func(ctx context.Context, fileID uint64) (GetChecksumResponse, error) {
		return client.GetChecksum(ctx, GetChecksumRequest{
			TransferID: "tx123",
			Targets:    []ChecksumTarget{{FileID: fileID, FullPath: "/tmp/file"}},
		})
	}
	for i := 0; i < 2; i++ {
		resp, err := checksum(context.Background(), uint64(i+1))
		if err != nil {
			t.Fatalf("GetChecksum %d failed: %v", i+1, err)
		}
		readers = append(readers, resp.Reader)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := checksum(ctx, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetChecksum on a full data pool = %v, want context.DeadlineExceeded", err)
	}

	getStatus := func() {
		t.Helper()
		resp, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"})
		if err != nil {
			t.Fatalf("GetStatus with a full data pool: %v", err)
		}
		if resp.Status == nil || resp.Status.TransferID != "tx123" {
			t.Fatalf("unexpected status response: %+v", resp.Status)
		}
	}
	// Both warm control connections serve a STATUS each. They are single-use,
	// so wait for their blocked replacement fills; the third STATUS then has
	// to dial on demand.
	for i := 0; i < 2; i++ {
		getStatus()
		waitForDialAttemptCount(t, dialer, 6+i)
	}
	if got := dialer.SuccessCount(); got != 5 {
		t.Fatalf("expected no dial for the warm control connections, got %d successful dials", got)
	}
	dialer.SetSuccessLimit(6)
	getStatus()
	if got := dialer.SuccessCount(); got != 6 {
		t.Fatalf("expected one on-demand control dial, got %d successful dials", got)
	}
	if got := client.MetricSnapshot().SyncConnectionCount; got != 1 {
		t.Fatalf("SyncConnectionCount = %d, want 1 (the control dial; the waiting data request never dials)", got)
	}

	// Close wakes a waiting data request with an error instead of a dial.
	waitErr := make(chan error, 1)
	go func() {
		_, err := checksum(context.Background(), 4)
		waitErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_ = client.Close()
	select {
	case err := <-waitErr:
		if !errors.Is(err, errTCPPoolStopped) {
			t.Fatalf("waiting GetChecksum after Close = %v, want errTCPPoolStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting GetChecksum never returned after Close")
	}
	if got := dialer.SuccessCount(); got != 6 {
		t.Fatalf("Close set off %d dials, want none", got-6)
	}
}

func TestTCPPoolRefillsAfterStreamClose(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		case intftcp.VerbSTATUS:
			_, err := io.WriteString(out, "OK {\"transfer_id\":\"tx123\"}\r\n")
			return err
		case intftcp.VerbCXSUM:
			_, err := io.WriteString(out, "CXSUM fid=1 algo=xxh128 token=deadbeef\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	dialer.SetSuccessLimit(6)
	resp, err := client.GetChecksum(context.Background(), GetChecksumRequest{
		TransferID: "tx123",
		Targets: []ChecksumTarget{{
			FileID:   1,
			FullPath: "/tmp/file",
		}},
	})
	if err != nil {
		t.Fatalf("GetChecksum failed: %v", err)
	}
	if err := resp.Reader.Close(); err != nil {
		t.Fatalf("close checksum reader: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 6)
	waitForTCPPoolsReady(t, client, 2, 2)

	dialer.SetSuccessLimit(6)
	statusResp, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"})
	if err != nil {
		t.Fatalf("GetStatus failed using refilled pool: %v", err)
	}
	if statusResp.Status == nil || statusResp.Status.TransferID != "tx123" {
		t.Fatalf("unexpected status response: %+v", statusResp.Status)
	}
	if got := client.MetricSnapshot().SyncConnectionCount; got != 0 {
		t.Fatalf("expected no sync fallbacks after pool refill, got %d", got)
	}
}

func TestProbeLinkDoesNotResizeTCPPoolOnLaterMiniProbe(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	probeCPU.Store(4)
	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("second ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 6)
	time.Sleep(100 * time.Millisecond)

	pool := client.tcpPool(poolData)
	if pool == nil {
		t.Fatal("expected data pool to remain initialized")
	}
	if pool.opts.limit != 2 {
		t.Fatalf("expected one-shot data pool limit 2, got %d", pool.opts.limit)
	}
	if got := dialer.SuccessCount(); got != 6 {
		t.Fatalf("expected only the second probe dial to be added, got %d successful dials", got)
	}
}

func TestClientCloseStopsTCPPoolAndAllowsDirectDialLater(t *testing.T) {
	var probeCPU atomic.Int64
	probeCPU.Store(2)
	handler := func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
			if err != nil {
				return err
			}
			return writeProbeResponse(out, int(probeCPU.Load()), n)
		case intftcp.VerbSTATUS:
			_, err := io.WriteString(out, "OK {\"transfer_id\":\"tx123\"}\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext))
	defer client.Close()

	if _, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	waitForDialSuccessCount(t, dialer, 5)
	waitForTCPPoolsReady(t, client, 2, 2)

	if err := client.Close(); err != nil {
		t.Fatalf("client.Close failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second client.Close failed: %v", err)
	}
	if client.tcpPool(poolData) != nil || client.tcpPool(poolControl) != nil {
		t.Fatal("expected pools to be cleared after Close")
	}

	dialer.SetSuccessLimit(5)
	if _, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"}); err == nil {
		t.Fatal("expected GetStatus to fail after Close when new dials are blocked")
	}

	dialer.SetSuccessLimit(6)
	statusResp, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"})
	if err != nil {
		t.Fatalf("GetStatus failed after allowing one direct dial: %v", err)
	}
	if statusResp.Status == nil || statusResp.Status.TransferID != "tx123" {
		t.Fatalf("unexpected status response: %+v", statusResp.Status)
	}
	if got := dialer.SuccessCount(); got != 6 {
		t.Fatalf("expected one direct dial after client.Close, got %d successful dials", got)
	}
}

func TestClientScheduledHeartbeatMetrics(t *testing.T) {
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{KeepAliveTimeout: 5 * time.Second})
	c := &Client{FileAddr: addr}
	conn, err := c.dialTCP(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	granted, _, err := c.probeKeepAliveSessionConn(conn, tcpAuthState{})
	if err != nil {
		_ = conn.Close()
		t.Fatalf("warm-up probe: %v", err)
	}
	if !granted {
		_ = conn.Close()
		t.Fatal("keep-alive not granted")
	}

	pool := newTCPConnPool(tcpAuthState{}, tcpPoolOptions{size: 1, limit: 1}, 5000)
	defer pool.stop()
	pool.takeSlot()
	session := &sessionTCPConn{
		Conn:       conn,
		lastActive: time.Now().Add(-time.Second),
	}
	if !pool.enqueue(session) {
		_ = conn.Close()
		t.Fatal("enqueue session")
	}

	pool.heartbeatIdleConns(c, 100*time.Millisecond)
	snap := c.MetricSnapshot()
	if snap.HeartbeatCount != 1 {
		t.Fatalf("scheduled heartbeat count = %d, want 1", snap.HeartbeatCount)
	}
}

func TestClientScheduledHeartbeatFailureMetrics(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	_ = serverConn.Close()

	c := &Client{}
	pool := &tcpConnPool{
		ctx:       context.Background(),
		opts:      tcpPoolOptions{size: 1},
		ready:     make(chan struct{}, 1),
		hbSem:     make(chan struct{}, keepAliveHeartbeatConcurrency),
		authState: tcpAuthState{},
	}
	session := &sessionTCPConn{
		Conn:       clientConn,
		lastActive: time.Now().Add(-time.Second),
	}
	if !pool.enqueue(session) {
		t.Fatal("enqueue closed session")
	}

	pool.heartbeatIdleConns(c, 100*time.Millisecond)
	snap := c.MetricSnapshot()
	if snap.HeartbeatFailureCount != 1 {
		t.Fatalf("scheduled heartbeat failures = %d, want 1", snap.HeartbeatFailureCount)
	}
	if snap.HeartbeatCount != 0 {
		t.Fatalf("successful heartbeat count = %d, want 0", snap.HeartbeatCount)
	}
}

func TestPoolSessionNegotiationHonorsContext(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", deadline), func(t *testing.T) {
			conn, peer := net.Pipe()
			t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
			c := NewClient("pipe", WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return conn, nil
			}))
			c.controlPool = newTCPConnPool(tcpAuthState{}, tcpPoolOptions{size: 1, limit: 0}, 5000)
			defer c.Close()
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			defer cancel()
			ready := make(chan error, 1)
			go func() {
				_, err := bufio.NewReader(peer).ReadString('\n')
				ready <- err // Withhold the session PROBE response.
			}()
			done := make(chan error, 1)
			go func() {
				_, err := c.GetStatus(ctx, GetStatusRequest{TransferID: "review"})
				done <- err
			}()
			select {
			case err := <-ready:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("session probe did not reach peer")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, ctx.Err()) {
					t.Fatalf("GetStatus = %v, want %v", err, ctx.Err())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("session negotiation ignored context cancellation")
			}
		})
	}
}

func TestHeartbeatReturnsHealthyConnectionsBeforeSlowProbe(t *testing.T) {
	c := NewClient("pipe")
	pool := newTCPConnPool(tcpAuthState{}, tcpPoolOptions{size: 2, limit: 2}, 5000)
	defer pool.stop()
	slowReady, fastReady := make(chan struct{}), make(chan struct{})
	slowRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseSlow := func() { releaseOnce.Do(func() { close(slowRelease) }) }
	defer releaseSlow()
	for _, slow := range []bool{false, true} {
		conn, peer := net.Pipe()
		t.Cleanup(func() { _ = peer.Close() })
		pool.takeSlot()
		pool.enqueue(&sessionTCPConn{Conn: conn, lastActive: time.Now().Add(-time.Second)})
		go func() {
			defer peer.Close()
			br := bufio.NewReader(peer)
			if _, err := br.ReadString('\n'); err != nil {
				return
			}
			if slow {
				close(slowReady)
				<-slowRelease
			}
			_, _ = io.WriteString(peer, "PROBE cpu=1 cts0=1 sts0=1 sts1=1 probe-bytes=0 keep-alive-ms=5000\r\nOK\r\n")
			if !slow {
				close(fastReady)
			}
			_, _ = br.ReadString('\n') // Keep the session open for reuse.
		}()
	}
	heartbeatDone := make(chan struct{})
	go func() {
		pool.heartbeatIdleConns(c, time.Millisecond)
		close(heartbeatDone)
	}()
	defer func() {
		releaseSlow()
		select {
		case <-heartbeatDone:
		case <-time.After(2 * time.Second):
			t.Error("heartbeat did not finish")
		}
	}()
	for _, ready := range []chan struct{}{slowReady, fastReady} {
		select {
		case <-ready:
		case <-time.After(2 * time.Second):
			t.Fatal("heartbeat did not reach peer")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := pool.acquire(ctx, c)
	if err != nil {
		t.Fatalf("healthy connection unavailable while another probe is pending: %v", err)
	}
	_ = c.recycleManagedTCPConn(conn, pool)
	releaseSlow()
	select {
	case <-heartbeatDone:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not finish")
	}
	if got := len(pool.slots); got != 2 {
		t.Fatalf("pool holds %d slots, want 2", got)
	}
}

func TestHeartbeatPreservesIdleOrder(t *testing.T) {
	pool := newTCPConnPool(tcpAuthState{}, tcpPoolOptions{size: 4, limit: 4}, 5000)
	defer pool.stop()
	// Sessions 0-2 are due for a probe; session 3 just carried traffic.
	sessions := make([]*sessionTCPConn, 4)
	closed := make([]*closeCountConn, 4)
	for i := range sessions {
		conn, peer := net.Pipe()
		t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
		closed[i] = &closeCountConn{Conn: conn}
		lastActive := time.Now().Add(-time.Second)
		if i == 3 {
			lastActive = time.Now()
		}
		sessions[i] = &sessionTCPConn{Conn: closed[i], lastActive: lastActive}
		pool.takeSlot()
		pool.enqueue(sessions[i])
	}
	stale := pool.takeStale(500 * time.Millisecond)
	if len(stale) != 3 {
		t.Fatalf("takeStale returned %d sessions, want 3", len(stale))
	}
	// Probes complete out of order and must not count as use: each probed
	// session returns beneath the fresher session 3, in last-use order.
	for _, i := range []int{1, 2, 0} {
		pool.enqueueProbed(nil, sessions[i])
	}
	pool.trimIdle(nil, 2)
	for i, conn := range closed {
		want := int64(0)
		if i < 2 {
			want = 1
		}
		if got := conn.closes.Load(); got != want {
			t.Fatalf("session %d closed %d times, want %d: scale-down must close the least recently used", i, got, want)
		}
	}
	if got, ok := pool.borrow(); !ok || got != sessions[3] {
		t.Fatal("borrow did not return the most recently used session")
	}
}

func TestClientKeepAlivePoolRecycling(t *testing.T) {
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{KeepAliveTimeout: 5 * time.Second})
	c := &Client{FileAddr: addr}
	defer c.Close()
	ctx := context.Background()

	if _, err := c.probeTCP(ctx, ProbeRequest{}, 1); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got := c.sessionKeepAliveMS(); got != 5000 {
		t.Fatalf("expected probe to cache the 5000ms grant, got %d", got)
	}

	c.ensureTCPPools(tcpAuthState{}, 2)
	deadline := time.Now().Add(3 * time.Second)
	for {
		pool := c.tcpPool(poolControl)
		if pool != nil && len(pool.ready) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session pool never warmed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < 3; i++ {
		if _, err := c.listStatusesTCP(ctx, ListStatusesRequest{}); err != nil {
			t.Fatalf("list statuses %d: %v", i, err)
		}
	}
	snap := c.MetricSnapshot()
	if snap.ConnectionReuseCount == 0 {
		t.Fatalf("expected recycled session connections, metrics: %+v", snap)
	}
}

func TestHeartbeatIntervalForGrant(t *testing.T) {
	if got := heartbeatIntervalForGrant(60000); got != 15*time.Second {
		t.Fatalf("expected 15s for 60s grant, got %v", got)
	}
	if got := heartbeatIntervalForGrant(2000); got != 500*time.Millisecond {
		t.Fatalf("expected 500ms for 2s grant, got %v", got)
	}
	if got := heartbeatIntervalForGrant(100); got != minKeepAliveHeartbeatInterval {
		t.Fatalf("expected floor for tiny grant, got %v", got)
	}
}

// warmKeepAlivePool probes for the keep-alive grant, builds the session
// pools, waits until both are fully warm, and returns the class's pool.
func warmKeepAlivePool(t *testing.T, c *Client, class tcpPoolClass) *tcpConnPool {
	t.Helper()
	ctx := context.Background()
	if _, err := c.probeTCP(ctx, ProbeRequest{}, 1); err != nil {
		t.Fatalf("probe: %v", err)
	}
	c.ensureTCPPools(tcpAuthState{}, 2)
	waitForTCPPoolsReady(t, c, 2, 2)
	return c.tcpPool(class)
}

func TestClientKeepAliveEvictsDeadConnsAtBorrow(t *testing.T) {
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{KeepAliveTimeout: 5 * time.Second})
	c := NewClient(addr)
	defer c.Close()
	pool := warmKeepAlivePool(t, c, poolControl)

	// Drain the pool and provoke a server-side ERR + close on every
	// connection (garbage command), leaving each socket with pending data
	// and a FIN — the state a borrower must detect and evict.
	var conns []net.Conn
	for {
		conn, ok := pool.borrow()
		if !ok {
			break
		}
		conns = append(conns, conn)
	}
	if len(conns) == 0 {
		t.Fatalf("expected warmed connections to drain")
	}
	for _, conn := range conns {
		_, _ = conn.Write([]byte("BOGUS\r\n"))
	}
	time.Sleep(200 * time.Millisecond)
	for _, conn := range conns {
		if !pool.enqueue(conn) {
			t.Fatalf("re-enqueue failed")
		}
	}

	// The next command must still succeed: dead sessions evicted at borrow
	// time, then the pool dials a replacement.
	if _, err := c.listStatusesTCP(context.Background(), ListStatusesRequest{}); err != nil {
		t.Fatalf("list statuses after poisoning pool: %v", err)
	}
}

func TestClientKeepAliveMetadataRecycling(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{KeepAliveTimeout: 5 * time.Second})
	c := NewClient(addr)
	defer c.Close()
	warmKeepAlivePool(t, c, poolData)
	ctx := context.Background()

	mresp, err := c.GetManifest(ctx, GetManifestRequest{Directory: dir, Mode: "fast", LinkMbps: 100, Concurrency: 2})
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	before := c.MetricSnapshot().ConnectionReuseCount
	meta, err := c.GetEntryMetadata(ctx, mresp.Manifest.TransferID, map[uint64]string{0: dir})
	if err != nil {
		t.Fatalf("entry metadata: %v", err)
	}
	if len(meta) != 1 {
		t.Fatalf("expected 1 metadata entry, got %d", len(meta))
	}
	after := c.MetricSnapshot().ConnectionReuseCount
	if after <= before {
		t.Fatalf("expected GetEntryMetadata to recycle its connection: before=%d after=%d", before, after)
	}
}

func TestProbeLinkSizesDataPoolFromExplicitConcurrency(t *testing.T) {
	handler := func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbPROBE {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		n, err := strconv.ParseInt(strings.TrimSpace(req.Params[0]["probe-bytes"]), 10, 64)
		if err != nil {
			return err
		}
		return writeProbeResponse(out, 2, n)
	}
	dialer := newCountingPipeDialer(handler)
	client := NewClient("ignored:0", WithContextDialer(dialer.DialContext), WithConcurrency(5))
	defer client.Close()

	probe, err := client.ProbeLink(context.Background(), ProbeRequest{ProbeBytes: 1})
	if err != nil {
		t.Fatalf("ProbeLink failed: %v", err)
	}
	if probe.SuggestedConcurrency != 2 {
		t.Fatalf("expected suggested concurrency 2, got %d", probe.SuggestedConcurrency)
	}
	if probe.WarmConnectionPoolSize != 5 {
		t.Fatalf("data pool size = %d, want the explicit concurrency 5", probe.WarmConnectionPoolSize)
	}
	waitForTCPPoolsReady(t, client, 5, 5)
}

// trackingDialer dials real TCP connections and records how many it opened
// and the most open at once.
type trackingDialer struct {
	dials, open, peak atomic.Int64
}

func (d *trackingDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	var nd net.Dialer
	conn, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	d.dials.Add(1)
	n := d.open.Add(1)
	for p := d.peak.Load(); n > p && !d.peak.CompareAndSwap(p, n); p = d.peak.Load() {
	}
	return &trackedConn{Conn: conn, d: d}, nil
}

type trackedConn struct {
	net.Conn
	d    *trackingDialer
	once sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() { c.d.open.Add(-1) })
	return c.Conn.Close()
}

// TestStartFromManifestBoundsDataConnections is the regression test for
// connection churn: a transfer whose batches could split into far more SENDs
// than the data pool holds, run by more workers than that, never issues a
// SEND that has to wait for a data connection, and reuses a fixed set of them
// instead of dialing per request. One large file covers the split-window path.
func TestStartFromManifestBoundsDataConnections(t *testing.T) {
	const (
		concurrency = 4
		workers     = 2 * concurrency
		files       = 200
	)
	root := t.TempDir()
	for i := 0; i < files; i++ {
		size := 1024
		if i == 0 {
			size = 64 << 10 // 16 split windows
		}
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%03d", i)), bytes.Repeat([]byte{byte(i)}, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	st := store.NewStore()
	t.Cleanup(st.Close)
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{
		KeepAliveTimeout: 5 * time.Second,
		Deps:             intftcp.NewRuntimeDeps(st, intftcp.WithRoot(root)),
	})
	dialer := &trackingDialer{}
	client := NewClient(addr, WithContextDialer(dialer.DialContext), WithConcurrency(concurrency))
	defer client.Close()

	ctx := context.Background()
	if _, err := client.ProbeLink(ctx, ProbeRequest{ProbeBytes: 1}); err != nil {
		t.Fatalf("ProbeLink: %v", err)
	}
	waitForTCPPoolsReady(t, client, concurrency, concurrency)
	manifestResp, err := client.GetManifest(ctx, GetManifestRequest{Directory: "/", Mode: "fast", LinkMbps: 100, Concurrency: concurrency})
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	// Four 1 KiB files per batch, each batch able to split into four SENDs,
	// would ask for 32 data connections at once.
	resp, err := client.StartFromManifest(ctx, StartFromManifestRequest{
		Manifest:           manifestResp.Manifest,
		Concurrency:        workers,
		BatchMaxBytes:      4096,
		SplitWindowWorkers: concurrency,
		OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
			return noOpWriteCloser{Writer: io.Discard}, func() error { return nil }, nil
		},
	})
	if err != nil {
		t.Fatalf("StartFromManifest: %v", err)
	}
	if resp.Downloaded != files {
		t.Fatalf("downloaded %d files, want %d (errors %v)", resp.Downloaded, files, resp.Errors)
	}
	// The transfer has no more SEND slots than data connections and splits
	// batches only into free ones, so no SEND ever queues for a connection.
	if got := client.tcpPool(poolData).waits.Load(); got != 0 {
		t.Fatalf("SENDs waited for a data connection %d times, want 0", got)
	}
	// At most the discovery probe, the data pool, and one control connection
	// per worker for concurrent ACKs are open at once. Control connections
	// above the pool's size close as they come back, so total dials depend on
	// how ACKs overlap; they must still be far fewer than requests.
	maxOpen := int64(1 + concurrency + workers)
	snap := client.MetricSnapshot()
	if got := dialer.peak.Load(); got > maxOpen {
		t.Fatalf("peak open connections %d, want at most %d", got, maxOpen)
	}
	if got := dialer.dials.Load(); got > files/4 {
		t.Fatalf("dialed %d connections for %d files, want at most %d; metrics %+v", got, files, files/4, snap)
	}
	if snap.ConnectionReuseCount < files/2 {
		t.Fatalf("ConnectionReuseCount = %d, want most requests on reused connections", snap.ConnectionReuseCount)
	}
}

// TestGetFilesReleasesSendSlots checks that GetFiles returns every SEND slot
// it took. Multi-file batches whose sizes split into fewer groups than slots
// reserved cover the group path; a file split into windows covers the window
// path, both when its windows succeed and when all of them fail. The window
// cases hold one slot elsewhere, so windows share the file's own slot with
// whatever extra slots are free.
func TestGetFilesReleasesSendSlots(t *testing.T) {
	root := t.TempDir()
	write := func(name string, size int) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), bytes.Repeat([]byte("x"), size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 17 {
		write(fmt.Sprintf("f%02d", i), 1)
	}
	write("big", 64<<10)
	st := store.NewStore()
	t.Cleanup(st.Close)
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{
		Deps: intftcp.NewRuntimeDeps(st, intftcp.WithRoot(root)),
	})
	c := NewClient(addr)
	defer c.Close()

	type batch struct {
		name      string
		files     int // tiny files in the batch; 0 means the big file alone
		missing   bool
		batchSize int64
		workers   int
		slots     int
		heldElse  int
	}
	var cases []batch
	for _, n := range []int{17, 10, 7, 5, 4} {
		cases = append(cases, batch{name: fmt.Sprintf("%d files", n), files: n, batchSize: int64(n), workers: 16, slots: 16})
	}
	cases = append(cases,
		batch{name: "split file", batchSize: 4 << 10, workers: 8, slots: 4, heldElse: 1},
		batch{name: "split file, all windows fail", missing: true, batchSize: 4 << 10, workers: 8, slots: 4, heldElse: 1},
	)
	for _, tc := range cases {
		resp, err := c.GetManifest(context.Background(), GetManifestRequest{Directory: "/", Mode: "fast", Concurrency: 16})
		if err != nil {
			t.Fatal(err)
		}
		var ids []uint64
		for _, e := range resp.Manifest.Entries {
			name := filepath.Base(e.Path)
			if (tc.files > 0 && strings.HasPrefix(name, "f") && len(ids) < tc.files) || (tc.files == 0 && name == "big") {
				ids = append(ids, e.ID)
			}
		}
		if tc.missing {
			if err := os.Remove(filepath.Join(root, "big")); err != nil {
				t.Fatal(err)
			}
		}
		slots := make(sendSlots, tc.slots)
		for range tc.heldElse {
			slots <- struct{}{}
		}
		got, err := c.GetFiles(context.Background(), GetFilesRequest{
			Manifest: resp.Manifest, FileIDs: ids, BatchMaxBytes: tc.batchSize, SplitWindowWorkers: tc.workers,
			sendSlots: slots,
			OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
				return noOpWriteCloser{Writer: io.Discard}, func() error { return nil }, nil
			},
		})
		if tc.missing != (err != nil) || (!tc.missing && len(got.Files) != len(ids)) {
			t.Fatalf("%s: got %d of %d files, error %v", tc.name, len(got.Files), len(ids), err)
		}
		if len(slots) != tc.heldElse {
			t.Fatalf("%s: %d slots held afterwards, want %d", tc.name, len(slots), tc.heldElse)
		}
	}
}

// canceledAfterUpgrade is a context whose Err reports cancellation while its
// Done channel never closes, as if the caller gave up just after the session
// upgrade finished and before the cancellation callback could run.
type canceledAfterUpgrade struct{ context.Context }

func (canceledAfterUpgrade) Err() error { return context.Canceled }

// TestPoolKeepsSessionWhenCallerCancelsAfterUpgrade checks that a caller who
// gives up after its on-demand connection was upgraded gets its context
// error, while the healthy session goes to the pool for the next borrower.
func TestPoolKeepsSessionWhenCallerCancelsAfterUpgrade(t *testing.T) {
	dialer := &keepAlivePipeDialer{}
	c := &Client{FileAddr: "pipe", contextDialer: dialer.DialContext}
	pool := newTCPConnPool(tcpAuthState{}, tcpPoolOptions{size: 2, limit: 2}, 5000)
	defer pool.stop()
	if !pool.takeSlot() {
		t.Fatal("take slot")
	}
	conn, err := c.dialPoolConn(canceledAfterUpgrade{context.Background()}, pool)
	if !errors.Is(err, context.Canceled) || conn != nil {
		t.Fatalf("dialPoolConn = %v, %v; want nil, context.Canceled", conn, err)
	}
	if dialer.Open() != 1 || len(pool.ready) != 1 || len(pool.slots) != 1 {
		t.Fatalf("%d open, %d idle, %d slots; want the session kept: 1, 1, 1", dialer.Open(), len(pool.ready), len(pool.slots))
	}
	if got, ok := pool.borrow(); !ok {
		t.Fatal("kept session is not available to the next borrower")
	} else if _, isSession := got.(*sessionTCPConn); !isSession {
		t.Fatalf("kept connection %T is not a session", got)
	}
}

// TestPoolIdleStackIsLIFO checks that borrowers get the most recently used
// idle connection and scale-down closes the least recently used.
func TestPoolIdleStackIsLIFO(t *testing.T) {
	pool := newTCPConnPool(tcpAuthState{}, tcpPoolOptions{size: 3, limit: 0}, 0)
	defer pool.stop()
	conns := make([]*closeCountConn, 3)
	for i := range conns {
		clientSide, serverSide := net.Pipe()
		defer serverSide.Close()
		conns[i] = &closeCountConn{Conn: clientSide}
		if !pool.enqueue(conns[i]) {
			t.Fatalf("enqueue %d", i)
		}
	}
	for _, want := range []int{2, 1} {
		if got, ok := pool.borrow(); !ok || got != conns[want] {
			t.Fatalf("borrow = %v, want the most recently used conns[%d]", got, want)
		}
	}
	pool.enqueue(conns[1])
	pool.trimIdle(nil, 1)
	if conns[0].closes.Load() != 1 || conns[1].closes.Load() != 0 {
		t.Fatalf("trim closed conns[0] %d times and conns[1] %d times, want only the least recently used conns[0]",
			conns[0].closes.Load(), conns[1].closes.Load())
	}
	if got, ok := pool.borrow(); !ok || got != conns[1] {
		t.Fatalf("borrow after trim = %v, want conns[1]", got)
	}
}

// TestPoolScalesDownToActivePlusQuarter runs both pools through the same
// scale-down: warm to the concurrency, settle at peak use plus a quarter
// without redialing under steady load, and drop to one connection once idle.
// Only the control pool may grow past the concurrency. A one-hour grant keeps
// the heartbeat ticker from firing, and the test runs each tick itself, so
// every interval is an explicit step.
func TestPoolScalesDownToActivePlusQuarter(t *testing.T) {
	const concurrency = 8
	for _, tc := range []struct {
		name  string
		class tcpPoolClass
	}{{"data", poolData}, {"control", poolControl}} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startRealKeepAliveServer(t, intftcp.ServerOptions{KeepAliveTimeout: time.Hour})
			dialer := &trackingDialer{}
			c := NewClient(addr, WithContextDialer(dialer.DialContext))
			defer c.Close()
			ctx := context.Background()
			if _, err := c.probeTCP(ctx, ProbeRequest{}, 1); err != nil {
				t.Fatalf("probe: %v", err)
			}
			c.ensureTCPPools(tcpAuthState{}, concurrency)
			waitForTCPPoolsReady(t, c, concurrency, concurrency)
			pool := c.tcpPool(tc.class)
			open := func() int { return len(pool.ready) + int(pool.borrowed.Load()) }
			// tick ends an interval. Every connection carried traffic within
			// the hour, so it scales down without probing.
			tick := func() { pool.heartbeatIdleConns(c, time.Hour) }
			acquire := func(ctx context.Context) (net.Conn, error) {
				conn, _, _, err := c.acquireManagedTCPConn(ctx, tc.class)
				return conn, err
			}
			// use borrows n connections at once, then returns them, and
			// reports which ones it used.
			use := func(n int) []net.Conn {
				t.Helper()
				conns := make([]net.Conn, n)
				for i := range conns {
					conn, err := acquire(ctx)
					if err != nil {
						t.Fatalf("acquire: %v", err)
					}
					conns[i] = conn
				}
				for _, conn := range conns {
					if err := c.recycleManagedTCPConn(conn, pool); err != nil {
						t.Fatalf("recycle: %v", err)
					}
				}
				return conns
			}

			// Four in use at once keeps 4 + 1, the most recently used four.
			last := use(4)
			tick()
			if got := open(); got != 5 {
				t.Fatalf("%d open, want 5 (4 active + a quarter)", got)
			}
			pool.idleMu.Lock()
			top := append([]net.Conn(nil), pool.idle[len(pool.idle)-4:]...)
			pool.idleMu.Unlock()
			for _, conn := range last {
				if !slices.Contains(top, conn) {
					t.Fatal("scale-down closed a connection from the most recently used four")
				}
			}
			// More intervals of the same load neither close nor redial anything.
			dials := dialer.dials.Load()
			for range 5 {
				use(4)
				tick()
			}
			if got := dialer.dials.Load(); got != dials || open() != 5 {
				t.Fatalf("steady load redialed %d connections and left %d open, want 0 and 5", got-dials, open())
			}
			// An interval with nothing in use drops the pool to one connection.
			tick()
			if got := open(); got != 1 {
				t.Fatalf("%d open once idle, want 1", got)
			}

			// The pool dials back up on demand. Only the control pool grows
			// past the concurrency; a full data pool makes callers wait.
			conns := make([]net.Conn, concurrency)
			for i := range conns {
				conn, err := acquire(ctx)
				if err != nil {
					t.Fatalf("acquire %d after scale-down: %v", i, err)
				}
				conns[i] = conn
			}
			waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			extra, err := acquire(waitCtx)
			switch {
			case tc.class == poolData && !errors.Is(err, context.DeadlineExceeded):
				t.Fatalf("acquire beyond the data pool's limit = %v, want context.DeadlineExceeded", err)
			case tc.class == poolControl && err != nil:
				t.Fatalf("control pool did not grow past the concurrency: %v", err)
			case err == nil:
				conns = append(conns, extra)
			}
			for _, conn := range conns {
				_ = c.recycleManagedTCPConn(conn, pool)
			}
		})
	}
}

// TestPoolConnReturnedAfterCloseIsClosed returns a borrowed connection after
// Client.Close stopped its pool: it must close and free its slot.
func TestPoolConnReturnedAfterCloseIsClosed(t *testing.T) {
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{KeepAliveTimeout: 5 * time.Second})
	c := NewClient(addr)
	pool := warmKeepAlivePool(t, c, poolData)
	closers := map[bool]*managedTCPConnCloser{}
	for _, reusable := range []bool{true, false} {
		conn, _, got, err := c.acquireManagedTCPConn(context.Background(), poolData)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if got != pool {
			t.Fatal("acquire did not use the data pool")
		}
		closers[reusable] = &managedTCPConnCloser{client: c, conn: conn, pool: pool}
		if reusable {
			closers[reusable].markReusable()
		}
	}
	c.Close()
	for reusable, closer := range closers {
		_ = closer.Close()
		if _, err := closer.conn.Write([]byte("PROBE\r\n")); err == nil {
			t.Fatalf("reusable=%v: connection still open after returning it to a stopped pool", reusable)
		}
	}
	if got := len(pool.slots); got != 0 {
		t.Fatalf("data pool holds %d slots after Close, want 0", got)
	}
}

// keepAlivePipeDialer hands out net.Pipe connections whose peer answers every
// PROBE, granting keep-alive unless withdrawn, and counts connections still
// open. While failing is set, dials fail.
type keepAlivePipeDialer struct {
	mu        sync.Mutex
	open      int
	peers     []net.Conn
	failing   atomic.Bool
	withdrawn atomic.Bool
}

func (d *keepAlivePipeDialer) DialContext(context.Context, string) (net.Conn, error) {
	if d.failing.Load() {
		return nil, errors.New("dial refused")
	}
	clientSide, serverSide := net.Pipe()
	d.mu.Lock()
	d.open++
	d.peers = append(d.peers, serverSide)
	d.mu.Unlock()
	go func() {
		defer serverSide.Close()
		br := bufio.NewReader(serverSide)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "PROBE ") {
				grant := " keep-alive-ms=5000"
				if d.withdrawn.Load() {
					grant = ""
				}
				_, _ = io.WriteString(serverSide, "PROBE cpu=1 cts0=1 sts0=1 sts1=1 probe-bytes=0"+grant+"\r\nOK\r\n")
			}
		}
	}()
	return &pipeOpenConn{Conn: clientSide, d: d}, nil
}

func (d *keepAlivePipeDialer) Open() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.open
}

// kill closes the server side of the i-th dialed connection, so the next
// probe on it fails.
func (d *keepAlivePipeDialer) kill(i int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.peers) > 0 {
		_ = d.peers[i%len(d.peers)].Close()
	}
}

type pipeOpenConn struct {
	net.Conn
	d    *keepAlivePipeDialer
	once sync.Once
}

func (c *pipeOpenConn) Close() error {
	c.once.Do(func() {
		c.d.mu.Lock()
		c.d.open--
		c.d.mu.Unlock()
	})
	return c.Conn.Close()
}

// FuzzTCPConnPool drives one pool, in session or single-use mode, through
// random sequences of acquires, returns, heartbeat ticks (which scale the pool
// down), dead peers, failing dials, keep-alive withdrawal, refills, and stop,
// checking after
// every step that each open connection is idle or borrowed, that a bounded
// pool never exceeds its limit and holds exactly one slot per connection, and
// that nothing stays open once the pool stops and every borrower returns.
func FuzzTCPConnPool(f *testing.F) {
	f.Add([]byte{4, 4, 4, 4, 0, 0, 0, 0, 0, 1, 2, 3, 0, 0})
	f.Add([]byte{0, 2, 1, 1, 0, 0, 0, 0, 9, 17, 25, 3, 3, 4})
	f.Add([]byte{2, 2, 2, 2, 0, 0, 0, 5, 3, 7, 0, 6, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 {
			return
		}
		opts := tcpPoolOptions{size: 1 + int(data[1]%5)}
		if data[0]&1 != 0 {
			opts.limit = opts.size
		}
		sessionMS := int64(5000)
		if data[1]&0x80 != 0 {
			sessionMS = 0
		}
		dialer := &keepAlivePipeDialer{}
		c := &Client{FileAddr: "pipe", contextDialer: dialer.DialContext}
		pool := newTCPConnPool(tcpAuthState{}, opts, sessionMS)
		defer pool.stop()
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		var borrowed []net.Conn
		// peak mirrors the pool's own: the most borrowed at once since the
		// last tick. used reports whether any acquire succeeded.
		peak, used := 0, false
		// killed reports whether any peer was closed. Pipes pass the liveness
		// peek, so acquire never discards one here; the flag keeps the
		// assertion below honest if the fake ever gains a liveness check.
		killed := false
		settle := func() {
			deadline := time.Now().Add(5 * time.Second)
			for pool.refilling.Load() != 0 {
				if time.Now().After(deadline) {
					t.Fatalf("fills never finished: %d in flight", pool.refilling.Load())
				}
				time.Sleep(time.Millisecond)
			}
		}
		check := func(step int) {
			t.Helper()
			settle()
			open, idle := dialer.Open(), len(pool.ready)
			if idle > opts.size {
				t.Fatalf("step %d: %d idle, size %d", step, idle, opts.size)
			}
			if open != idle+len(borrowed) {
				t.Fatalf("step %d: %d open, but %d idle + %d borrowed", step, open, idle, len(borrowed))
			}
			if got := pool.borrowed.Load(); got != int64(len(borrowed)) {
				t.Fatalf("step %d: pool counts %d borrowed, harness holds %d", step, got, len(borrowed))
			}
			pool.idleMu.Lock()
			stacked := len(pool.idle)
			pool.idleMu.Unlock()
			if stacked != idle {
				t.Fatalf("step %d: idle stack holds %d, but %d idle tokens", step, stacked, idle)
			}
			if target := int(pool.target.Load()); target > opts.size {
				t.Fatalf("step %d: target %d above size %d", step, target, opts.size)
			}
			if opts.limit > 0 && (open > opts.limit || len(pool.slots) != open) {
				t.Fatalf("step %d: %d open, %d slots held, limit %d", step, open, len(pool.slots), opts.limit)
			}
		}
		takeBorrowed := func(arg int) net.Conn {
			i := arg % len(borrowed)
			conn := borrowed[i]
			borrowed = append(borrowed[:i], borrowed[i+1:]...)
			return conn
		}

		pool.triggerRefill(c)
		check(-1)
		for step, b := range data[4:] {
			arg := int(b / 10)
			switch b % 10 {
			case 0: // acquire; cancellation covers explicit requests and full pools
				ctx, cancelAcquire := context.WithTimeout(context.Background(), time.Second)
				if arg&16 != 0 || (opts.limit > 0 && len(pool.ready) == 0 && len(pool.slots) == opts.limit) {
					cancelAcquire()
					ctx = cancelled
				}
				idleBefore := len(pool.ready)
				conn, err := pool.acquire(ctx, c)
				cancelAcquire()
				switch {
				case err == nil:
					borrowed = append(borrowed, conn)
					peak, used = max(peak, len(borrowed)), true
				case errors.Is(err, context.Canceled):
					// Dialing a new session honors cancellation, but a live
					// idle connection is lent without dialing.
					if idleBefore > 0 && !killed {
						t.Fatalf("step %d: canceled acquire skipped %d idle connections", step, idleBefore)
					}
				case !errors.Is(err, errTCPPoolStopped) && !dialer.failing.Load():
					t.Fatalf("step %d: acquire: %v", step, err)
				}
			case 1: // return cleanly
				if len(borrowed) > 0 {
					_ = c.recycleManagedTCPConn(takeBorrowed(arg), pool)
				}
			case 2: // return dirty
				if len(borrowed) > 0 {
					_ = c.releaseManagedTCPConn(takeBorrowed(arg), pool)
				}
			case 3, 4: // heartbeat tick: every idle connection is due, or fresh
				interval := time.Duration(0)
				if b%10 == 4 {
					interval = time.Hour
				}
				keep := keepFor(max(peak, len(borrowed)))
				pool.heartbeatIdleConns(c, interval)
				peak = len(borrowed)
				settle()
				// Once used, a tick scales the pool down to its peak use plus a
				// quarter, and replacement fills restore no more than that.
				if open := dialer.Open(); used && open > keep {
					t.Fatalf("step %d: %d open after a tick, want at most %d", step, open, keep)
				}
			case 5:
				dialer.kill(arg)
				killed = true
			case 6:
				pool.triggerRefill(c)
			case 7:
				pool.stop()
				if len(pool.ready) != 0 {
					t.Fatalf("step %d: %d idle connections left after stop", step, len(pool.ready))
				}
			case 8:
				dialer.failing.Store(!dialer.failing.Load())
			case 9:
				dialer.withdrawn.Store(true)
			}
			check(step)
		}

		pool.stop()
		for len(borrowed) > 0 {
			_ = c.recycleManagedTCPConn(takeBorrowed(0), pool)
		}
		settle()
		if open := dialer.Open(); open != 0 {
			t.Fatalf("%d connections open after stop and every return", open)
		}
		if opts.limit > 0 && len(pool.slots) != 0 {
			t.Fatalf("%d slots held after stop and every return", len(pool.slots))
		}
	})
}
