package ftcp

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

func TestServeLogsExitAfterConfiguration(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var logBuf bytes.Buffer
	origWriter := log.Writer()
	origFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(origWriter)
		log.SetFlags(origFlags)
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = ln.Close()
	}()

	if err := Serve(ln, ServerOptions{ExitAfter: 5 * time.Second}); err != nil {
		t.Fatalf("Serve returned error: %v", err)
	}

	if got := logBuf.String(); !strings.Contains(got, "exit-after: server will exit 5s after the last activity") {
		t.Fatalf("expected exit-after startup log, got %q", got)
	}
}

func TestIsKeepAliveHeartbeat(t *testing.T) {
	cases := []struct {
		cmd  string
		want bool
	}{
		{"PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto", true},
		{"PROBE cpu=1 probe-bytes=1024 cts0=1 keep-alive=auto", false},
		{"PROBE cpu=1 probe-bytes=0 cts0=1", false},
		{"STATUS", false},
	}
	for _, tc := range cases {
		req, err := ParseRequest([]byte(tc.cmd))
		if err != nil {
			t.Fatalf("parse %q: %v", tc.cmd, err)
		}
		if got := isKeepAliveHeartbeat(req); got != tc.want {
			t.Fatalf("isKeepAliveHeartbeat(%q) = %v, want %v", tc.cmd, got, tc.want)
		}
	}
}

func TestConnSessionNoteClientActivity(t *testing.T) {
	activityCount := 0
	session := &connSession{onClientActivity: func() { activityCount++ }}

	heartbeat, err := ParseRequest([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto"))
	if err != nil {
		t.Fatalf("parse heartbeat: %v", err)
	}
	session.noteClientActivity(heartbeat)
	if activityCount != 0 {
		t.Fatalf("heartbeat activity count = %d, want 0", activityCount)
	}

	status, err := ParseRequest([]byte("STATUS"))
	if err != nil {
		t.Fatalf("parse STATUS: %v", err)
	}
	session.noteClientActivity(status)
	if activityCount != 1 {
		t.Fatalf("STATUS activity count = %d, want 1", activityCount)
	}

	authKey, err := ParseRequest([]byte("AUTH key"))
	if err != nil {
		t.Fatalf("parse AUTH key: %v", err)
	}
	session.noteClientActivity(authKey)
	if activityCount != 2 {
		t.Fatalf("AUTH key activity count = %d, want 2", activityCount)
	}
}

func TestHandleConnInitialHeartbeatDoesNotCountAsActivity(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	activityCount := make(chan struct{}, 1)
	go handleConn(
		serverConn,
		ServerOptions{KeepAliveTimeout: time.Second},
		NewRuntimeDeps(),
		nil,
		func() { activityCount <- struct{}{} },
	)

	if _, err := clientConn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}
	br := bufio.NewReader(clientConn)
	readLineOrFatal(t, br, "heartbeat response")
	readLineOrFatal(t, br, "heartbeat status")

	select {
	case <-activityCount:
		t.Fatal("initial heartbeat counted as client activity")
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := clientConn.Write([]byte("STATUS\r\n")); err != nil {
		t.Fatalf("write STATUS: %v", err)
	}
	statusLine := readLineOrFatal(t, br, "STATUS response")
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(statusLine, "OK")))
	if err != nil {
		t.Fatalf("parse STATUS count from %q: %v", statusLine, err)
	}
	for i := 0; i < count; i++ {
		readLineOrFatal(t, br, "STATUS entry")
	}
	select {
	case <-activityCount:
	case <-time.After(time.Second):
		t.Fatal("STATUS did not count as client activity")
	}
}

// dialKeepAliveServer starts a real Serve loop and dials one connection.
func dialKeepAliveServer(t *testing.T, opts ServerOptions) (net.Conn, *bufio.Reader) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = Serve(ln, opts) }()
	t.Cleanup(func() { _ = ln.Close() })
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn, bufio.NewReader(conn)
}

func readLineOrFatal(t *testing.T, br *bufio.Reader, what string) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read %s: %v", what, err)
	}
	return strings.TrimRight(line, "\r\n")
}

func TestServeKeepAliveReusesConnection(t *testing.T) {
	conn, br := dialKeepAliveServer(t, ServerOptions{KeepAliveTimeout: 2 * time.Second})

	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
		t.Fatalf("write PROBE: %v", err)
	}
	resp := readLineOrFatal(t, br, "PROBE response")
	if !strings.Contains(resp, "keep-alive-ms=2000") {
		t.Fatalf("expected keep-alive-ms=2000 grant, got %q", resp)
	}
	if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after PROBE, got %q", ok)
	}

	// Second and third commands on the same connection.
	if _, err := conn.Write([]byte("STATUS\r\n")); err != nil {
		t.Fatalf("write STATUS on kept-alive conn: %v", err)
	}
	statusOK := readLineOrFatal(t, br, "STATUS response")
	if !strings.HasPrefix(statusOK, "OK") {
		t.Fatalf("expected OK for STATUS on kept-alive conn, got %q", statusOK)
	}
	// Drain the per-transfer JSON lines (the store is global, so other
	// tests in this package may have left transfers behind).
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(statusOK, "OK")))
	if err != nil {
		t.Fatalf("parse STATUS count from %q: %v", statusOK, err)
	}
	for i := 0; i < count; i++ {
		readLineOrFatal(t, br, "STATUS entry")
	}

	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=2 keep-alive=auto\r\n")); err != nil {
		t.Fatalf("write heartbeat PROBE: %v", err)
	}
	resp2 := readLineOrFatal(t, br, "heartbeat PROBE response")
	if !strings.HasPrefix(resp2, "PROBE ") {
		t.Fatalf("expected PROBE heartbeat response, got %q", resp2)
	}
	if ok := readLineOrFatal(t, br, "heartbeat status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after heartbeat, got %q", ok)
	}
}

func TestServeClosesWithoutKeepAlive(t *testing.T) {
	conn, br := dialKeepAliveServer(t, ServerOptions{KeepAliveTimeout: 2 * time.Second})

	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1\r\n")); err != nil {
		t.Fatalf("write PROBE: %v", err)
	}
	resp := readLineOrFatal(t, br, "PROBE response")
	if strings.Contains(resp, "keep-alive-ms=") {
		t.Fatalf("unexpected keep-alive grant without request: %q", resp)
	}
	if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after PROBE, got %q", ok)
	}
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatalf("expected server to close connection without keep-alive")
	}
}

func TestServeKeepAliveClearsSyncWriteDeadline(t *testing.T) {
	dir := t.TempDir()
	conn, br := dialKeepAliveServer(t, ServerOptions{
		KeepAliveTimeout: 5 * time.Second,
		SyncTimeout:      100 * time.Millisecond,
	})

	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
		t.Fatalf("write PROBE: %v", err)
	}
	readLineOrFatal(t, br, "PROBE response")
	if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after PROBE, got %q", ok)
	}

	// SYNC with a minimal framed manifest body arms the write deadline.
	hdr := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID: "syncdl01", Mode: "fast", LinkMbps: 100, Concurrency: 2,
	})
	rootLine, _, _, err := encoding.MarshalManifestEntry(encoding.ManifestEntry{
		Type: encoding.EntryTypeDir, ID: 0, Path: dir, Mode: 0o755, Mtime: time.Now().UnixNano(),
	}, "", "")
	if err != nil {
		t.Fatalf("marshal root entry: %v", err)
	}
	var framed bytes.Buffer
	cw := encoding.NewChunkedManifestWriter(&framed, "none", encoding.DefaultManifestChunkSize, 0)
	if _, err := cw.Write([]byte(hdr + "\n" + rootLine + "\n")); err != nil {
		t.Fatalf("write manifest body: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("close manifest body: %v", err)
	}
	cmd := "SYNC " + strconv.Itoa(len(dir)) + ":" + dir + " mode=fast link-mbps=100 concurrency=2 comp=none\r\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("write SYNC: %v", err)
	}
	if _, err := conn.Write(framed.Bytes()); err != nil {
		t.Fatalf("write SYNC body: %v", err)
	}
	for {
		line := readLineOrFatal(t, br, "SYNC response")
		if line == "OK" {
			break
		}
		if strings.HasPrefix(line, "ERR ") {
			t.Fatalf("SYNC failed: %q", line)
		}
	}

	// Idle past SyncTimeout: the write deadline must have been cleared, so
	// the next response on this kept-alive connection still succeeds.
	time.Sleep(250 * time.Millisecond)
	if _, err := conn.Write([]byte("STATUS\r\n")); err != nil {
		t.Fatalf("write STATUS: %v", err)
	}
	if ok := readLineOrFatal(t, br, "STATUS response"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK for STATUS after sync deadline elapsed, got %q", ok)
	}
}

// TestServeKeepAliveMidSessionGarbageReportsError proves that garbage sent
// as a second command on a kept-alive connection yields a parseable ERR line
// instead of the server just closing the socket silently.
func TestServeKeepAliveMidSessionGarbageReportsError(t *testing.T) {
	conn, br := dialKeepAliveServer(t, ServerOptions{KeepAliveTimeout: 5 * time.Second})

	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
		t.Fatalf("write PROBE: %v", err)
	}
	readLineOrFatal(t, br, "PROBE response")
	if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after PROBE, got %q", ok)
	}

	if _, err := conn.Write([]byte("BOGUS\r\n")); err != nil {
		t.Fatalf("write BOGUS: %v", err)
	}
	line := readLineOrFatal(t, br, "BOGUS response")
	if !strings.HasPrefix(line, "ERR ") {
		t.Fatalf("expected ERR line for mid-session garbage, got %q", line)
	}
}

// TestConnSessionReportError exercises reportError directly: it should write
// a single ERR frame (in writeErrFrame's exact format) to the current
// response writer and then close it, swallowing any secondary error.
func TestConnSessionReportError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	s := &connSession{conn: serverConn, respOut: serverConn}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.reportError(protocolErr{code: "X", message: "y"})
	}()

	br := bufio.NewReader(clientConn)
	line := readLineOrFatal(t, br, "reportError output")
	if line != "ERR X y" {
		t.Fatalf("reportError wrote %q, want %q", line, "ERR X y")
	}
	<-done
}

func TestServeKeepAliveIdleTimeout(t *testing.T) {
	conn, br := dialKeepAliveServer(t, ServerOptions{KeepAliveTimeout: 200 * time.Millisecond})

	if _, err := conn.Write([]byte("PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n")); err != nil {
		t.Fatalf("write PROBE: %v", err)
	}
	resp := readLineOrFatal(t, br, "PROBE response")
	if !strings.Contains(resp, "keep-alive-ms=200") {
		t.Fatalf("expected keep-alive-ms=200 grant, got %q", resp)
	}
	if ok := readLineOrFatal(t, br, "PROBE status"); !strings.HasPrefix(ok, "OK") {
		t.Fatalf("expected OK after PROBE, got %q", ok)
	}

	// Send nothing: the idle reaper must close the connection well before
	// the outer 5s dial deadline.
	start := time.Now()
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatalf("expected idle connection to be closed by server")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("idle reap took too long: %v", elapsed)
	}
}
