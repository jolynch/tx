package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHandlePROBERoundTrip(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=1024 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	payload := bytes.Repeat([]byte{0x5a}, 1024)
	in := bytes.NewReader(payload)
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, in, &out, &mockDeps{}, 8, 25, 25, 1*1024*1024, 0, defaultMaxSyncBodyBytes); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}

	br := bufio.NewReader(bytes.NewReader(out.Bytes()))
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read response line: %v", err)
	}
	respReq, err := ParseRequest([]byte(strings.TrimRight(line, "\r\n")))
	if err != nil {
		t.Fatalf("parse response line: %v", err)
	}
	if respReq.Verb != VerbPROBE {
		t.Fatalf("expected PROBE response, got %v", respReq.Verb)
	}
	if got := respReq.Params[0]["probe-bytes"]; got != "1024" {
		t.Fatalf("expected probe-bytes=1024, got %q", got)
	}
	if got := respReq.Params[0]["io-depth"]; got != "8" {
		t.Fatalf("expected io-depth=8, got %q", got)
	}
	if got := respReq.Params[0]["gentle-cpu-pct"]; got != "25" {
		t.Fatalf("expected gentle-cpu-pct=25, got %q", got)
	}
	if got := respReq.Params[0]["gentle-bw-pct"]; got != "25" {
		t.Fatalf("expected gentle-bw-pct=25, got %q", got)
	}
	echo := make([]byte, 1024)
	if _, err := io.ReadFull(br, echo); err != nil {
		t.Fatalf("read probe echo bytes: %v", err)
	}
	if len(echo) != 1024 {
		t.Fatalf("unexpected echo length: %d", len(echo))
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected eof after payload, got %v", err)
	}
}

func TestHandlePROBEDefaultIODepth(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=4 probe-bytes=0 cts0=50`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, &mockDeps{}, 0, 30, 25, 1*1024*1024, 0, defaultMaxSyncBodyBytes); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}
	line, err := bufio.NewReader(bytes.NewReader(out.Bytes())).ReadString('\n')
	if err != nil {
		t.Fatalf("read response line: %v", err)
	}
	respReq, err := ParseRequest([]byte(strings.TrimRight(line, "\r\n")))
	if err != nil {
		t.Fatalf("parse response line: %v", err)
	}
	if got := respReq.Params[0]["io-depth"]; got != "8" {
		t.Fatalf("expected default io-depth=8, got %q", got)
	}
	if got := respReq.Params[0]["gentle-cpu-pct"]; got != "30" {
		t.Fatalf("expected gentle-cpu-pct=30, got %q", got)
	}
	if got := respReq.Params[0]["gentle-bw-pct"]; got != "25" {
		t.Fatalf("expected gentle-bw-pct=25, got %q", got)
	}
}

func TestHandlePROBERejectsShortPayload(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=10 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	in := bytes.NewReader([]byte{1, 2, 3})
	var out bytes.Buffer
	err = handlePROBEWithInput(context.Background(), req, in, &out, &mockDeps{}, 8, 25, 25, 1*1024*1024, 0, defaultMaxSyncBodyBytes)
	if err == nil {
		t.Fatalf("expected payload validation error")
	}
}

func TestHandlePROBEReportsObservedLink(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=0 cts0=100 txferid=tx123 obs-link-mbps=900`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	deps := &mockDeps{
		reportReturn:   TransferObservedLinkUpdate{ObservedLinkMbps: 900, EMALinkMbps: 900, OldRateBps: 0, NewRateBps: 28125000},
		reportReturnOK: true,
	}
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, deps, 8, 25, 25, 2*1024*1024, 0, defaultMaxSyncBodyBytes); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}
	if !deps.reportCalled {
		t.Fatalf("expected ReportTransferObservedLink call")
	}
	if deps.reportTxferID != "tx123" || deps.reportObserved != 900 {
		t.Fatalf("unexpected report args: tx=%s observed=%d", deps.reportTxferID, deps.reportObserved)
	}
	if deps.reportBWPct != 25 || deps.reportBurst != 2*1024*1024 {
		t.Fatalf("unexpected limiter args: pct=%d burst=%d", deps.reportBWPct, deps.reportBurst)
	}
	if deps.reportEMAAlpha != 0.2 {
		t.Fatalf("unexpected EMA alpha: %.2f", deps.reportEMAAlpha)
	}
}

func TestParsePROBERequestKeepAlive(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=auto`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parsePROBERequest(req)
	if err != nil {
		t.Fatalf("parsePROBERequest failed: %v", err)
	}
	if !parsed.KeepAlive {
		t.Fatalf("expected KeepAlive to be parsed from keep-alive=auto")
	}

	// Only "auto" is a valid request value; anything else is rejected.
	req, err = ParseRequest([]byte(`PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=1`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	if _, err := parsePROBERequest(req); err == nil {
		t.Fatalf("expected keep-alive=1 to be rejected")
	}
}

func TestHandlePROBEKeepAliveGrant(t *testing.T) {
	cases := []struct {
		name        string
		cmd         string
		keepAliveMS int64
		wantToken   bool
	}{
		{"requested and enabled", `PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=auto`, 60000, true},
		{"requested but disabled", `PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=auto`, 0, false},
		{"not requested", `PROBE cpu=2 probe-bytes=0 cts0=5`, 60000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(tc.cmd))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			var out bytes.Buffer
			if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, &mockDeps{}, 8, 25, 25, 1*1024*1024, tc.keepAliveMS, defaultMaxSyncBodyBytes); err != nil {
				t.Fatalf("handlePROBEWithInput failed: %v", err)
			}
			line, err := bufio.NewReader(bytes.NewReader(out.Bytes())).ReadString('\n')
			if err != nil {
				t.Fatalf("read response line: %v", err)
			}
			gotToken := strings.Contains(line, "keep-alive-ms=60000")
			if gotToken != tc.wantToken {
				t.Fatalf("keep-alive-ms token presence = %v, want %v (line %q)", gotToken, tc.wantToken, line)
			}
		})
	}
}

func TestParsePROBERequestRejectsOversizedProbe(t *testing.T) {
	req := Request{
		Verb: VerbPROBE,
		Params: []map[string]string{{
			"cpu":         "4",
			"probe-bytes": strconv.FormatInt(maxProbeBytes+1, 10),
			"cts0":        "100",
		}},
	}
	_, err := parsePROBERequest(req)
	if err == nil {
		t.Fatalf("expected oversized probe error")
	}
}

func TestParsePROBERequestObservedLinkRequiresTransferID(t *testing.T) {
	req := Request{
		Verb: VerbPROBE,
		Params: []map[string]string{{
			"cpu":           "4",
			"probe-bytes":   "0",
			"cts0":          "100",
			"obs-link-mbps": "900",
		}},
	}
	if _, err := parsePROBERequest(req); err == nil {
		t.Fatalf("expected obs-link-mbps validation error")
	}
}

// A fast transfer has no limiter, so PROBE neither logs one nor advertises
// one, however often the client reports its observed link; the first gentle
// SEND creates it.
func TestHandlePROBEFastTransferHasNoLimiter(t *testing.T) {
	deps := realDeps(t, "/")
	tr, err := deps.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	deps.SetTransferHints(tr.ID, "fast", 1000, 8)

	var logs bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldWriter)

	probe := func() string {
		t.Helper()
		req, err := ParseRequest([]byte("PROBE cpu=8 probe-bytes=0 cts0=100 txferid=" + tr.ID + " obs-link-mbps=900"))
		if err != nil {
			t.Fatalf("ParseRequest failed: %v", err)
		}
		var out bytes.Buffer
		if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, deps, 8, 25, 25, 1*1024*1024, 0, defaultMaxSyncBodyBytes); err != nil {
			t.Fatalf("handlePROBEWithInput failed: %v", err)
		}
		for _, field := range strings.Fields(out.String()) {
			if v, ok := strings.CutPrefix(field, "limiter-bps="); ok {
				return v
			}
		}
		t.Fatalf("PROBE response lacks limiter-bps: %s", out.String())
		return ""
	}
	for range 3 {
		if got := probe(); got != "0" {
			t.Fatalf("fast transfer advertised limiter-bps=%s", got)
		}
	}
	if strings.Contains(logs.String(), "limiter=") {
		t.Fatalf("fast transfer logged a limiter: %s", logs.String())
	}

	if deps.GetTransferGentleLimiter(tr.ID, 0, 25, 1*1024*1024) == nil {
		t.Fatalf("expected a gentle limiter")
	}
	if got := probe(); got == "0" {
		t.Fatalf("gentle limiter not advertised")
	}
}

// serveOneConn serves one real TCP connection with handleConn. It returns the
// client end and a channel closed when handleConn returns, so a test can
// tell that the server let go of the connection rather than only that the
// socket closed.
func serveOneConn(t *testing.T, opts ServerOptions) (net.Conn, <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	auth, err := newAuthPolicy(opts)
	if err != nil {
		t.Fatalf("newAuthPolicy: %v", err)
	}
	deps := opts.Deps
	if deps == nil {
		deps = realDeps(t, "/")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handleConn(conn, opts, auth, nil, deps, nil, nil)
	}()
	// Runs after the client end closes below, so no handler outlives the test.
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("handleConn still running after the client closed")
		}
	})
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return conn, done
}

// A plaintext request that stops arriving mid-way must not hold the
// connection open: once no byte arrives for the session's read-progress
// timeout (KeepAliveTimeout here), the read fails, the handler returns, and
// the server closes the connection without answering the request. It may
// report at most one ERR line; a stalled command line, which has no request
// to answer yet, closes silently.
func TestServePlaintextStalledRequestCloses(t *testing.T) {
	const idle = 200 * time.Millisecond
	const closeWithin = 2 * time.Second

	ackBody, err := io.ReadAll(framedItemBody(t, `fd=1 6:/a/b/c ack-token=1:0`))
	if err != nil {
		t.Fatalf("build ACK body: %v", err)
	}
	payload := bytes.Repeat([]byte{0x5a}, 4096)

	cases := []struct {
		name string
		// prelude runs complete commands before the stalled one.
		prelude func(t *testing.T, conn net.Conn, br *bufio.Reader)
		stalled []byte
		// silent cases must close without writing anything.
		silent bool
	}{
		{
			// The fault-run hang: a PROBE payload truncated by a dropped
			// segment leaves drainProbePayload waiting forever.
			name:    "PROBE payload",
			stalled: append([]byte("PROBE cpu=1 probe-bytes=1048576 cts0=1\r\n"), payload...),
		},
		{
			name:    "PROBE payload before its first byte",
			stalled: []byte("PROBE cpu=1 probe-bytes=1048576 cts0=1\r\n"),
		},
		{
			// readCommand clears its deadline once the command line is read;
			// the body that follows must still be bounded.
			name: "PROBE payload on a kept-alive connection",
			prelude: func(t *testing.T, conn net.Conn, br *bufio.Reader) {
				if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
					t.Fatalf("write keep-alive PROBE: %v", err)
				}
				readLineOrFatal(t, br, "keep-alive PROBE response")
				if ok := readLineOrFatal(t, br, "keep-alive PROBE status"); !strings.HasPrefix(ok, "OK") {
					t.Fatalf("expected OK after keep-alive PROBE, got %q", ok)
				}
			},
			stalled: append([]byte("PROBE cpu=1 probe-bytes=1048576 cts0=2\r\n"), payload...),
		},
		{
			name:    "ACK framed body",
			stalled: append([]byte("ACK tx123\r\n"), ackBody[:len(ackBody)/2]...),
		},
		{
			name:    "first command line",
			stalled: []byte("PROBE cpu=1 probe-by"),
			silent:  true,
		},
		{
			name: "command line on a kept-alive connection",
			prelude: func(t *testing.T, conn net.Conn, br *bufio.Reader) {
				if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
					t.Fatalf("write keep-alive PROBE: %v", err)
				}
				readLineOrFatal(t, br, "keep-alive PROBE response")
				if ok := readLineOrFatal(t, br, "keep-alive PROBE status"); !strings.HasPrefix(ok, "OK") {
					t.Fatalf("expected OK after keep-alive PROBE, got %q", ok)
				}
			},
			stalled: []byte("PROBE cpu=1 probe-by"),
			silent:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, done := serveOneConn(t, ServerOptions{KeepAliveTimeout: idle})
			br := bufio.NewReader(conn)
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if tc.prelude != nil {
				tc.prelude(t, conn, br)
			}
			if _, err := conn.Write(tc.stalled); err != nil {
				t.Fatalf("write stalled request: %v", err)
			}

			// Bound the client's own wait past closeWithin, so a server that
			// never closes fails here instead of hanging the test.
			start := time.Now()
			_ = conn.SetReadDeadline(start.Add(closeWithin + time.Second))
			got, err := io.ReadAll(br)
			elapsed := time.Since(start)
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				t.Fatalf("server held the stalled request open for %v without closing the connection (read %q)", elapsed, got)
			}
			if elapsed > closeWithin {
				t.Fatalf("server took %v to close a request stalled for %v", elapsed, idle)
			}
			if bytes.HasPrefix(got, []byte("PROBE ")) || bytes.HasPrefix(got, []byte("OK")) {
				t.Fatalf("server answered a request it never received in full: %q", got)
			}
			if tc.silent && len(got) > 0 {
				t.Fatalf("server wrote %q before closing a stalled command line, want nothing", got)
			}
			if bytes.Contains(got, []byte("ERR ERR")) || bytes.Count(got, []byte("\n")) > 1 {
				t.Fatalf("server reported the stall more than once: %q", got)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("handleConn did not return after the server closed the connection")
			}
		})
	}
}

// The read-progress timeout bounds each gap between bytes, not the whole
// request: a PROBE payload that keeps trickling in, each gap shorter than the
// timeout but the whole request several times longer, still succeeds.
func TestServePlaintextTrickledProbeSucceeds(t *testing.T) {
	const idle = 200 * time.Millisecond
	const gap = 40 * time.Millisecond
	const chunks = 16
	const chunkBytes = 1024
	const total = chunks * chunkBytes

	conn, _ := serveOneConn(t, ServerOptions{KeepAliveTimeout: idle})
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	start := time.Now()
	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=" + strconv.Itoa(total) + " cts0=1\r\n")); err != nil {
		t.Fatalf("write PROBE: %v", err)
	}
	chunk := bytes.Repeat([]byte{0x5a}, chunkBytes)
	for range chunks {
		time.Sleep(gap)
		if _, err := conn.Write(chunk); err != nil {
			t.Fatalf("write payload chunk after %v: %v", time.Since(start), err)
		}
	}
	if elapsed := time.Since(start); elapsed <= 2*idle {
		t.Fatalf("payload took only %v; the test needs a request longer than the %v timeout", elapsed, idle)
	}

	br := bufio.NewReader(conn)
	resp := readLineOrFatal(t, br, "PROBE response")
	if !strings.HasPrefix(resp, "PROBE ") || !strings.Contains(resp, " probe-bytes="+strconv.Itoa(total)) {
		t.Fatalf("expected a PROBE response for %d bytes, got %q", total, resp)
	}
	if _, err := io.CopyN(io.Discard, br, total); err != nil {
		t.Fatalf("read PROBE echo: %v", err)
	}
	if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after PROBE, got %q", ok)
	}
}

// A client must begin its first command within the read-progress timeout
// (KeepAliveTimeout here), counted from accept. A connection that stays
// silent is closed at that timeout without a reply; one whose first command
// starts inside the timeout is served. Clients keep idle pooled connections
// open with keep-alive PROBEs instead.
func TestServePlaintextFirstCommandDeadline(t *testing.T) {
	const idle = 200 * time.Millisecond
	const closeWithin = time.Second

	t.Run("silent connection closes", func(t *testing.T) {
		conn, done := serveOneConn(t, ServerOptions{KeepAliveTimeout: idle})
		// Bound the client's own wait past closeWithin, so a server that
		// never closes fails here instead of hanging the test.
		start := time.Now()
		_ = conn.SetReadDeadline(start.Add(closeWithin + time.Second))
		got, err := io.ReadAll(conn)
		elapsed := time.Since(start)
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatalf("server held a connection that sent nothing open for %v (timeout %v)", elapsed, idle)
		}
		if elapsed < idle/2 {
			t.Fatalf("server closed a silent connection after %v, before its %v timeout", elapsed, idle)
		}
		if elapsed > closeWithin {
			t.Fatalf("server took %v to close a connection silent for its %v timeout", elapsed, idle)
		}
		if len(got) > 0 {
			t.Fatalf("server wrote %q before closing a silent connection, want nothing", got)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("handleConn did not return after the server closed the connection")
		}
	})

	t.Run("command inside the timeout is served", func(t *testing.T) {
		conn, _ := serveOneConn(t, ServerOptions{KeepAliveTimeout: idle})
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		time.Sleep(idle / 2)
		if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=16 cts0=1\r\n")); err != nil {
			t.Fatalf("write PROBE after %v idle: %v", idle/2, err)
		}
		if _, err := conn.Write(bytes.Repeat([]byte{0x5a}, 16)); err != nil {
			t.Fatalf("write PROBE payload: %v", err)
		}
		br := bufio.NewReader(conn)
		resp, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("server closed a connection whose first command began %v after accept (timeout %v): %v", idle/2, idle, err)
		}
		if !strings.HasPrefix(resp, "PROBE ") || !strings.Contains(resp, " probe-bytes=16") {
			t.Fatalf("expected a PROBE response for 16 bytes, got %q", resp)
		}
		if _, err := io.CopyN(io.Discard, br, 16); err != nil {
			t.Fatalf("read PROBE echo: %v", err)
		}
		if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
			t.Fatalf("expected OK after PROBE, got %q", ok)
		}
	})
}
