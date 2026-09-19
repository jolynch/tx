package ftcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
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

// drainSTATUSList consumes the framed body of a bare STATUS response followed
// by its verb-level OK line, and returns how many transfers it listed.
func drainSTATUSList(t *testing.T, br *bufio.Reader) int {
	t.Helper()
	body, err := io.ReadAll(encoding.NewFramedBodyReader(br, encoding.FramedBodyReaderOpts{}))
	if err != nil {
		t.Fatalf("read STATUS body: %v", err)
	}
	okLine := readLineOrFatal(t, br, "STATUS terminator")
	if !strings.HasPrefix(okLine, "OK") {
		t.Fatalf("expected OK after STATUS body, got %q", okLine)
	}
	count := 0
	for _, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

func TestHandleConnInitialHeartbeatDoesNotCountAsActivity(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	activityCount := make(chan struct{}, 1)
	go handleConn(
		serverConn,
		ServerOptions{KeepAliveTimeout: time.Second},
		realDeps(t, "/"),
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
	drainSTATUSList(t, br)
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
	// Drains the framed body and its terminal OK; the store may already hold
	// transfers left by other tests, so the count is not asserted.
	drainSTATUSList(t, br)

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
	cw := encoding.NewFramedBodyWriter(&framed, "none", encoding.DefaultBodyChunkSize, 0)
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
	drainSTATUSList(t, br)
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

// FuzzServeZeroCopySEND checks the wire contract and ACK state through the real
// server in both zero-copy and buffered modes. Sizes include pipe/frame edges;
// generating the payload keeps multi-megabyte cases out of the corpus files.
func FuzzServeZeroCopySEND(f *testing.F) {
	for _, size := range []uint32{0, 1, 4095, 4096, 4097, 65535, 65536, 65537, 4194303, 4194304, 4194305} {
		f.Add(size, uint32(0), uint32(0), uint32(8191), byte(0))
	}
	f.Add(uint32(4194417), uint32(19), uint32(4194335), uint32(4096), byte(173))
	f.Fuzz(func(t *testing.T, sizeRaw, offsetRaw, lengthRaw, chunkRaw uint32, content byte) {
		data := zeroCopyPayload(int(sizeRaw % (2*uint32(defaultFileFrameLogicalSize) + 1)))
		for i := range data {
			data[i] ^= content
		}
		var offset int64
		if len(data) > 0 {
			offset = int64(offsetRaw) % int64(len(data))
		}
		length := int64(len(data)) - offset
		if length > 0 && lengthRaw != 0 {
			length = 1 + int64(lengthRaw)%length
		}
		want := data[offset : offset+length]
		// Bound syscall count for large files; small cases still allow one-byte reads.
		readLimit := max(1+int(chunkRaw%65536), len(data)/4096)
		path := writeTempSendFile(t, data)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%t", buffered), func(t *testing.T) {
				deps := realDeps(t, "/")
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- Serve(ln, ServerOptions{Deps: deps, DisableZeroCopy: buffered}) }()
				t.Cleanup(func() {
					_ = ln.Close()
					if err := <-done; err != nil {
						t.Error(err)
					}
				})
				// Every command reads through EOF, including closure of its server
				// session. Cap actual reads, not just bufio's internal buffer size.
				exchange := func(command string) []byte {
					t.Helper()
					conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
					if _, err := io.WriteString(conn, command+"\r\n"); err != nil {
						t.Fatal(err)
					}
					raw, err := io.ReadAll(cappedTCPReader{conn, readLimit})
					if err != nil {
						t.Fatalf("%s: %v", strings.Fields(command)[0], err)
					}
					return raw
				}
				// SEND carries its per-file items in a framed request body, so
				// the command line and the body go out together.
				exchangeWithItems := func(command string, items ...string) []byte {
					t.Helper()
					conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
					if _, err := io.WriteString(conn, command+"\r\n"); err != nil {
						t.Fatal(err)
					}
					body := framedItemBody(t, items...)
					if _, err := io.Copy(conn, body); err != nil {
						t.Fatal(err)
					}
					raw, err := io.ReadAll(cappedTCPReader{conn, readLimit})
					if err != nil {
						t.Fatalf("%s: %v", strings.Fields(command)[0], err)
					}
					return raw
				}
				raw := exchange(fmt.Sprintf("TXFER %q mode=fast link-mbps=1000 concurrency=1 comp=none", filepath.Dir(path)))
				if !bytes.HasSuffix(raw, []byte("OK\r\n")) {
					t.Fatalf("TXFER failed: %q", raw)
				}
				manifest, err := io.ReadAll(encoding.NewFramedBodyReader(bytes.NewReader(raw), encoding.FramedBodyReaderOpts{}))
				if err != nil {
					t.Fatal(err)
				}
				header, err := encoding.ParseManifestHeader(strings.SplitN(string(manifest), "\n", 2)[0])
				if err != nil {
					t.Fatal(err)
				}
				entries, _ := parseSYNCResponseEntries(string(manifest), nil)
				if len(entries) != 1 || entries[0].Size != int64(len(data)) {
					t.Fatalf("unexpected manifest: %s", manifest)
				}
				fid, tid := entries[0].ID, header.TransferID
				raw = exchangeWithItems(
					fmt.Sprintf("SEND %s mode=fast", tid),
					fmt.Sprintf("fd=%d %q comp=none offset=%d size=%d", fid, path, offset, length),
				)
				if !bytes.HasSuffix(raw, []byte("OK\r\n")) {
					t.Fatal("SEND missing terminal OK")
				}
				frames, err := decodeFrameStream(bytes.TrimSuffix(raw, []byte("OK\r\n")))
				if err != nil {
					t.Fatal(err)
				}
				if len(frames) != max(1, int((length+defaultFileFrameLogicalSize-1)/defaultFileFrameLogicalSize)) {
					t.Fatalf("unexpected frame count: %d", len(frames))
				}
				hash := encoding.FormatXXH128HashToken(xxh3.Hash128(want))
				cursor := offset
				for i, frame := range frames {
					h, tr := frame.Header, frame.Trailer
					n := min(defaultFileFrameLogicalSize, offset+length-cursor)
					if h.FileID != fid || tr.FileID != fid || h.Offset != cursor || h.Size != n || h.WireSize != n || h.Comp != "none" {
						t.Fatalf("unexpected frame %d: %+v / %+v", i, h, tr)
					}
					if !bytes.Equal(frame.Logical, data[cursor:cursor+n]) {
						t.Fatalf("frame %d changed source bytes", i)
					}
					cursor += n
					next, token := cursor, ""
					if i == len(frames)-1 {
						next, token = 0, hash
						for _, field := range []string{
							fmt.Sprintf("meta:size=%d", info.Size()),
							fmt.Sprintf("meta:mtime_ns=%d", info.ModTime().UnixNano()),
							"meta:mode=" + encoding.FormatManifestMode(info.Mode()),
						} {
							if !strings.Contains(tr.ChecksumPrefix, field) {
								t.Fatalf("terminal metadata missing %s", field)
							}
						}
					}
					if tr.Next == nil || *tr.Next != next || tr.FileHashToken != token {
						t.Fatalf("unexpected trailer %d: %+v", i, tr)
					}
				}
				if !deps.VerifyTransferFileWindowHash(tid, fid, offset+length, hash) {
					t.Fatal("server did not store the window hash at its end offset")
				}
				ackItem := fmt.Sprintf("fd=%d %q ack-token=%d@1@", fid, path, offset+length)
				bad := exchangeWithItems("ACK "+tid, ackItem+"xxh128:00000000000000000000000000000000")
				if !bytes.HasPrefix(bad, []byte("ERR CONFLICT ")) {
					t.Fatalf("bad ACK accepted: %q", bad)
				}
				before, _ := deps.GetTransfer(tid)
				if before.DoneSize != 0 {
					t.Fatal("bad ACK advanced progress")
				}
				if got := string(exchangeWithItems("ACK "+tid, ackItem+hash)); got != "OK\r\n" {
					t.Fatalf("valid ACK rejected: %q", got)
				}
				var status encoding.TransferStatus
				raw = exchange("STATUS " + tid)
				if !bytes.HasPrefix(raw, []byte("OK ")) {
					t.Fatalf("STATUS failed: %q", raw)
				}
				if err := json.Unmarshal(raw[3:], &status); err != nil {
					t.Fatal(err)
				}
				if status.DoneSize != offset+length || status.TotalSize != int64(len(data)) {
					t.Fatalf("unexpected progress: %+v", status)
				}
			})
		}
	})
}

type cappedTCPReader struct {
	io.Reader
	limit int
}

func (r cappedTCPReader) Read(p []byte) (int, error) {
	return r.Reader.Read(p[:min(len(p), r.limit)])
}

// countingReader reports how many bytes a reader actually pulled, so a test can
// distinguish "gave up at the cap" from "buffered everything then complained".
type countingReader struct {
	served int64
	fill   byte
}

func (r *countingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.fill
	}
	r.served += int64(len(p))
	return len(p), nil
}

// The regression: readCommandLine used to call ReadBytes, which grows without
// bound and could only be size-checked after the allocation. A peer that never
// sends '\n' must be cut off, and must be told why in the protocol's own error
// vocabulary rather than the doubled "ERR ERR" a bare error produced.
func TestReadCommandLineBoundsHostileInput(t *testing.T) {
	src := &countingReader{fill: 'A'}
	_, err := readCommandLine(bufio.NewReader(src), maxCommandLineBytes)
	if err == nil {
		t.Fatal("endless command line was accepted")
	}
	var pe protocolErr
	if !errors.As(err, &pe) || pe.code != "BAD_REQUEST" {
		t.Fatalf("want BAD_REQUEST protocolErr, got %v (%T)", err, err)
	}
	if src.served > int64(maxCommandLineBytes)*4 {
		t.Fatalf("consumed %d bytes enforcing a %d byte cap", src.served, maxCommandLineBytes)
	}

	// And the wire form is a single well-formed ERR line.
	var out bytes.Buffer
	if writeErr := writeErrFrame(&out, err); writeErr != nil {
		t.Fatalf("writeErrFrame: %v", writeErr)
	}
	if got := out.String(); got != "ERR BAD_REQUEST command line too large\r\n" {
		t.Fatalf("unexpected error line: %q", got)
	}
}

func TestReadCommandLineAcceptsFullLengthCommand(t *testing.T) {
	// A command exactly at the cap is legal; one byte past it is not.
	body := strings.Repeat("x", maxCommandLineBytes-len("STATUS \r\n"))
	line := "STATUS " + body + "\r\n"
	got, err := readCommandLine(bufio.NewReader(strings.NewReader(line)), maxCommandLineBytes)
	if err != nil {
		t.Fatalf("command at the cap was rejected: %v", err)
	}
	if string(got) != "STATUS "+body {
		t.Fatalf("unexpected command payload of %d bytes", len(got))
	}
	if _, err := readCommandLine(bufio.NewReader(strings.NewReader("STATUS "+body+"y\r\n")), maxCommandLineBytes); err == nil {
		t.Fatal("command one byte past the cap was accepted")
	}
}

// panicDeps injects one mid-request panic to test connection isolation.
type panicDeps struct {
	Deps
	panicked chan struct{}
	fired    atomic.Bool
}

// Panic once so a later request can prove the listener survived.
func (d *panicDeps) GetTransfer(txferID string) (Transfer, bool) {
	if d.fired.CompareAndSwap(false, true) {
		close(d.panicked)
		panic("injected panic from a handler")
	}
	return d.Deps.GetTransfer(txferID)
}

func TestServePanicDropsOnlyItsConnection(t *testing.T) {
	root := t.TempDir()
	deps := &panicDeps{Deps: realDeps(t, "/"), panicked: make(chan struct{})}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = Serve(ln, ServerOptions{Deps: deps, RootDir: root}) }()

	// Trigger the handler panic.
	victim, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = victim.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(victim, "SEND tx1 mode=fast\r\n"); err != nil {
		t.Fatalf("write SEND: %v", err)
	}
	body := framedItemBody(t, `fd=1 "/tmp/a.txt"`)
	if _, err := io.Copy(victim, body); err != nil {
		t.Fatalf("write body: %v", err)
	}
	// The connection should close after the panic.
	_, _ = io.ReadAll(victim)
	_ = victim.Close()

	select {
	case <-deps.panicked:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reached the injected panic")
	}

	// Confirm the listener still serves new connections.
	survivor, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("server stopped accepting after a handler panic: %v", err)
	}
	defer survivor.Close()
	_ = survivor.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(survivor, "STATUS nonexistent\r\n"); err != nil {
		t.Fatalf("write STATUS: %v", err)
	}
	line, err := bufio.NewReader(survivor).ReadString('\n')
	if err != nil {
		t.Fatalf("read STATUS after panic: %v", err)
	}
	if !strings.HasPrefix(line, "ERR NOT_FOUND") {
		t.Fatalf("unexpected reply after a handler panic: %q", line)
	}
}

// A rejected header must yield ERR even while the client writes its body.
func TestServeSENDHeaderErrorIsReadableNotBrokenPipe(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = Serve(ln, ServerOptions{Deps: realDeps(t, "/"), RootDir: t.TempDir()}) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	// mode=slow is rejected by parseSENDHeader, before the body is looked at.
	if _, err := io.WriteString(conn, "SEND tx1 mode=slow\r\n"); err != nil {
		t.Fatalf("write SEND: %v", err)
	}
	// A body large enough that it cannot sit entirely in socket buffers.
	items := make([]string, 20000)
	for i := range items {
		items[i] = fmt.Sprintf(`fd=%d %d:/remote/some/reasonably/long/path/file-%06d.bin`, i+1, 46, i)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := io.Copy(conn, framedItemBody(t, items...))
		writeErr <- err
	}()

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read ERR after a rejected header: %v", err)
	}
	if !strings.HasPrefix(line, "ERR BAD_REQUEST") {
		t.Fatalf("expected a BAD_REQUEST explanation, got %q", line)
	}
	if err := <-writeErr; err != nil {
		t.Errorf("client body write failed instead of completing: %v", err)
	}
}
