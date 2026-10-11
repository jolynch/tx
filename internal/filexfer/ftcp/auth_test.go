package ftcp

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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// authServer starts Serve with opts and a fresh identity, returning its
// address and public key.
func authServer(t *testing.T, opts ServerOptions) (string, [32]byte) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	opts.ServerIdentity = id
	if opts.Deps == nil {
		opts.Deps = realDeps(t, "/")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = Serve(ln, opts) }()
	t.Cleanup(func() { _ = ln.Close() })
	pub, err := aead.ParseRecipientKey(id.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	return ln.Addr().String(), pub
}

func clientKey(t *testing.T) (aead.KeyPair, string) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	kp, err := aead.KeyPairFromIdentity(id)
	if err != nil {
		t.Fatal(err)
	}
	return kp, id.Recipient().String()
}

// noiseDial connects and authenticates.
func noiseDial(t *testing.T, addr string, cfg aead.ClientConfig) (*aead.Session, error) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(aead.HandshakeTimeout))
	s, err := aead.ClientHandshake(conn, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { s.Close() })
	return s, nil
}

// noiseCommand sends one message and returns the response's lines.
func noiseCommand(t *testing.T, s *aead.Session, request string) ([]string, error) {
	t.Helper()
	w := s.Writer(false)
	if _, err := io.WriteString(w, request); err != nil {
		return nil, err
	}
	if err := w.End(); err != nil {
		return nil, err
	}
	mr, err := s.Reader()
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(mr)
	raw, err := io.ReadAll(br)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\r\n"), "\n")
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\r")
	}
	return lines, nil
}

const heartbeat = "PROBE cpu=1 probe-bytes=0 cts0=1 keep-alive=auto\r\n"

func TestServeDiscoveryAndModes(t *testing.T) {
	token := strings.Repeat("s", 40)
	kp, recipient := clientKey(t)
	other, _ := clientKey(t)
	for _, tc := range []struct {
		name    string
		opts    ServerOptions
		client  aead.ClientConfig
		want    error
		plainOK bool
	}{
		{"open server accepts any key", ServerOptions{}, aead.ClientConfig{Mode: aead.ModeAge, Static: other}, nil, true},
		{"require-auth accepts any key", ServerOptions{RequireAuth: true}, aead.ClientConfig{Mode: aead.ModeAge, Static: other}, nil, false},
		{"allowlisted token", ServerOptions{AllowedAuthTokens: []string{token}}, aead.ClientConfig{Mode: aead.ModeToken, Token: token, Static: other}, nil, false},
		{"allowlisted recipient", ServerOptions{AllowedAuthTokens: []string{token}, AllowedClientRecipients: []string{recipient}}, aead.ClientConfig{Mode: aead.ModeAge, Static: kp}, nil, false},
		{"unlisted key", ServerOptions{AllowedClientRecipients: []string{recipient}}, aead.ClientConfig{Mode: aead.ModeAge, Static: other}, aead.ErrNotAuthorized, false},
		{"token without token entries", ServerOptions{RequireAuth: true}, aead.ClientConfig{Mode: aead.ModeToken, Token: token, Static: other}, aead.ErrTokenNotConfigured, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, pub := authServer(t, tc.opts)

			// Every server answers public key discovery.
			conn, _ := net.Dial("tcp", addr)
			_, _ = conn.Write(authKeyLine)
			line, _ := bufio.NewReader(conn).ReadString('\n')
			conn.Close()
			fields := strings.Fields(line)
			if len(fields) != 3 || fields[0] != "OK" {
				t.Fatalf("discovery = %q", line)
			}
			if got, err := aead.ParseRecipientKey(fields[2]); err != nil || got != pub {
				t.Fatalf("discovered key = %q, %v", fields[2], err)
			}

			// Plaintext commands work only on open servers.
			conn, _ = net.Dial("tcp", addr)
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, _ = conn.Write([]byte(heartbeat))
			line, _ = bufio.NewReader(conn).ReadString('\n')
			conn.Close()
			if gotOK := strings.HasPrefix(line, "PROBE "); gotOK != tc.plainOK {
				t.Fatalf("plaintext reply = %q, want accepted=%t", line, tc.plainOK)
			}

			cfg := tc.client
			cfg.Cipher, cfg.ServerKey = aead.AlgorithmAES, pub
			s, err := noiseDial(t, addr, cfg)
			if !errors.Is(err, tc.want) {
				t.Fatalf("handshake = %v, want %v", err, tc.want)
			}
			if err != nil {
				return
			}
			lines, err := noiseCommand(t, s, heartbeat)
			if err != nil || len(lines) != 2 || !strings.HasPrefix(lines[0], "PROBE ") || lines[1] != "OK" {
				t.Fatalf("PROBE over Noise = (%q, %v)", lines, err)
			}
		})
	}
}

func TestServeRejectsLegacyAuth(t *testing.T) {
	for _, opts := range []ServerOptions{{RequireAuth: true}, {}} {
		sink := events.NewSink()
		var mu sync.Mutex
		legacy := 0
		sink.Subscribe(func(e events.Event) {
			if e.Name == "auth" && e.Str("mode") == "legacy" {
				mu.Lock()
				legacy++
				mu.Unlock()
			}
		})
		opts.Events = sink
		addr, _ := authServer(t, opts)
		cases := []struct {
			first, want string
			legacy      bool
		}{
			// Clients before noise-v1 discover first, and print this reply.
			{"AUTH key\r\n", aead.DiagClientTooOld, true},
			{"Auth key\r\n", aead.DiagClientTooOld, true},
			{"AUTH aes c29tZWJsb2I=\r\n", aead.DiagUnsupportedAuth, true},
			{"auth chacha20 c29tZWJsb2I=\r\n", aead.DiagUnsupportedAuth, true},
			{"AUTH\r\n", aead.DiagUnsupportedAuth, false},
			{"AUTH noise-v2 token aes x\r\n", aead.DiagUnsupportedAuth, false},
		}
		wantLegacy := 0
		for _, tc := range cases {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, _ = conn.Write([]byte(tc.first))
			line, _ := bufio.NewReader(conn).ReadString('\n')
			conn.Close()
			if strings.TrimSpace(line) != tc.want {
				t.Fatalf("RequireAuth=%v: %q reply = %q, want %q", opts.RequireAuth, strings.TrimSpace(tc.first), line, tc.want)
			}
			if tc.legacy {
				wantLegacy++
			}
		}
		mu.Lock()
		got := legacy
		mu.Unlock()
		if got != wantLegacy {
			t.Fatalf("RequireAuth=%v: %d legacy auth events, want %d", opts.RequireAuth, got, wantLegacy)
		}
	}
}

func TestServeKeepAliveOverNoise(t *testing.T) {
	addr, pub := authServer(t, ServerOptions{RequireAuth: true, KeepAliveTimeout: 2 * time.Second})
	kp, _ := clientKey(t)
	s, err := noiseDial(t, addr, aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmChaCha20, Static: kp, ServerKey: pub})
	if err != nil {
		t.Fatal(err)
	}
	// The first PROBE advertises the grant it is about to commit.
	lines, err := noiseCommand(t, s, heartbeat)
	if err != nil || !strings.Contains(lines[0], "keep-alive-ms=2000") {
		t.Fatalf("first PROBE = (%q, %v)", lines, err)
	}
	for range 3 {
		lines, err = noiseCommand(t, s, "STATUS\r\n")
		if err != nil || lines[len(lines)-1] != "OK" {
			t.Fatalf("STATUS = (%q, %v)", lines, err)
		}
	}
}

func TestServeNoiseRequestCompletion(t *testing.T) {
	addr, pub := authServer(t, ServerOptions{RequireAuth: true, KeepAliveTimeout: 2 * time.Second})
	kp, _ := clientKey(t)
	cfg := aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub}
	for name, request := range map[string]string{
		"bodyless command with a tail": "STATUS\r\ntrailing",
		"PROBE with extra payload":     "PROBE cpu=1 probe-bytes=1 cts0=1\r\nxy",
		"two commands in one message":  heartbeat + heartbeat,
	} {
		t.Run(name, func(t *testing.T) {
			s, err := noiseDial(t, addr, cfg)
			if err != nil {
				t.Fatal(err)
			}
			// The server closes without responding.
			if lines, err := noiseCommand(t, s, request); err == nil {
				t.Fatalf("server answered an unfinished request: %q", lines)
			}
		})
	}
	// A request that never sends its END is closed at the idle deadline.
	addr, pub = authServer(t, ServerOptions{RequireAuth: true, KeepAliveTimeout: 200 * time.Millisecond})
	s, err := noiseDial(t, addr, aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub})
	if err != nil {
		t.Fatal(err)
	}
	w := s.Writer(false)
	_, _ = io.WriteString(w, "STATUS\r\n")
	_ = w.Flush()
	start := time.Now()
	mr, _ := s.Reader()
	if _, err := io.ReadAll(mr); err == nil {
		t.Fatal("server answered a request without END")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("unfinished request held the connection for %v", elapsed)
	}
}

func TestServeAuthenticatedIdleDeadline(t *testing.T) {
	// Without keep-alive, the first command after FINISH still has a
	// deadline; KeepAliveTimeout bounds it when set.
	addr, pub := authServer(t, ServerOptions{RequireAuth: true, KeepAliveTimeout: 200 * time.Millisecond})
	kp, _ := clientKey(t)
	s, err := noiseDial(t, addr, aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	mr, _ := s.Reader()
	if _, err := io.ReadAll(mr); err == nil {
		t.Fatal("silent authenticated connection was not closed")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("silent connection lived %v", elapsed)
	}
}

func TestServeAdmissionBusy(t *testing.T) {
	addr, pub := authServer(t, ServerOptions{RequireAuth: true, MaxUnauthenticatedConns: 2})
	kp, _ := clientKey(t)
	cfg := aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub}
	var idle []net.Conn
	for range 2 {
		c, _ := net.Dial("tcp", addr)
		idle = append(idle, c)
	}
	time.Sleep(50 * time.Millisecond) // let the server accept both
	if _, err := noiseDial(t, addr, cfg); !errors.Is(err, aead.ErrServerBusy) {
		t.Fatalf("handshake at capacity = %v, want ErrServerBusy", err)
	}
	for _, c := range idle {
		c.Close()
	}
	// Permits come back when connections close, and after FINISH.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := noiseDial(t, addr, cfg)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("permits not released: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for range 4 {
		if _, err := noiseDial(t, addr, cfg); err != nil {
			t.Fatalf("authenticated connections held permits: %v", err)
		}
	}
}

func TestServeIdleSocketsLeaveDefaultCapacity(t *testing.T) {
	addr, pub := authServer(t, ServerOptions{RequireAuth: true})
	for range 32 {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	kp, _ := clientKey(t)
	if _, err := noiseDial(t, addr, aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub}); err != nil {
		t.Fatalf("32 idle sockets excluded a client: %v", err)
	}
}

func TestServeRejectsInvalidAllowlist(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	id, _ := age.GenerateX25519Identity()
	bad := id.Recipient().String()
	bad = bad[:len(bad)-1] + "q"
	if bad == id.Recipient().String() {
		bad = bad[:len(bad)-1] + "p"
	}
	for _, opts := range []ServerOptions{
		{AllowedClientRecipients: []string{bad}},
		{AllowedClientRecipients: []string{id.String()}},
		{AllowedClientRecipients: []string{strings.Repeat("t", 20)}},
		{AllowedAuthTokens: []string{id.Recipient().String()}},
		{AllowedAuthTokens: []string{id.String()}},
		{AllowedAuthTokens: []string{"short"}},
		{MaxUnauthenticatedConns: -1},
	} {
		if err := Serve(ln, opts); err == nil {
			t.Fatalf("Serve accepted tokens %d, recipients %d", len(opts.AllowedAuthTokens), len(opts.AllowedClientRecipients))
		}
	}
}

// An open server takes a permit when a connection starts AUTH; discovery,
// refused handshakes, and successful ones must all return it.
func TestServeOpenServerReturnsPermits(t *testing.T) {
	addr, pub := authServer(t, ServerOptions{MaxUnauthenticatedConns: 2})
	kp, _ := clientKey(t)
	for range 6 {
		conn, _ := net.Dial("tcp", addr)
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = conn.Write(authKeyLine)
		_, _ = bufio.NewReader(conn).ReadString('\n')
		conn.Close()
	}
	other, _ := clientKey(t)
	for range 4 {
		// A handshake against the wrong server key is refused.
		_, _ = noiseDial(t, addr, aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: other.Public})
	}
	for range 4 {
		if _, err := noiseDial(t, addr, aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub}); err != nil {
			t.Fatalf("permits leaked: %v", err)
		}
	}
}

// A body-carrying request must reach its END before it changes state: the
// same ACK or SYNC with one trailing byte closes the connection and leaves
// the store untouched.
func TestServeNoiseBodyRequestsApplyOnlyAtEnd(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.bin"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := realDeps(t, "/")
	req, err := ParseRequest([]byte(fmt.Sprintf("TXFER %q mode=fast link-mbps=100 concurrency=1 comp=none", root)))
	if err != nil {
		t.Fatal(err)
	}
	var manifest bytes.Buffer
	var txferID string
	if err := handleTXFERWithCallback(context.Background(), req, &manifest, deps, func(id string) { txferID = id }); err != nil {
		t.Fatal(err)
	}
	entries, _ := parseSYNCResponseEntries(unframeManifestWire(t, manifest.Bytes()), nil)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	e := entries[0]
	const hash = "xxh128:0000000000000000000000000000000a"
	if !deps.SetTransferFileWindowHashes(txferID, []WindowHash{{FileID: e.ID, EndBytes: e.Size, HashToken: hash}}) {
		t.Fatal("SetTransferFileWindowHashes failed")
	}
	var ackBody bytes.Buffer
	bw := encoding.NewFramedBodyWriter(&ackBody, encoding.EncodingZstd, 4096, 0)
	fmt.Fprintf(bw, "fd=%d %q ack-token=%d@1@%s\n", e.ID, filepath.Join(root, "a.bin"), e.Size, hash)
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	ack := "ACK " + txferID + "\r\n" + ackBody.String()

	hdr := encoding.FormatManifestHeader(encoding.ManifestHeader{TransferID: "syncdl01", Mode: "fast", LinkMbps: 100, Concurrency: 2})
	rootLine, _, _, err := encoding.MarshalManifestEntry(encoding.ManifestEntry{
		Type: encoding.EntryTypeDir, ID: 0, Path: root, Mode: 0o755, Mtime: time.Now().UnixNano(),
	}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var syncBody bytes.Buffer
	cw := encoding.NewFramedBodyWriter(&syncBody, "none", encoding.DefaultBodyChunkSize, 0)
	if _, err := cw.Write([]byte(hdr + "\n" + rootLine + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := cw.Close(); err != nil {
		t.Fatal(err)
	}
	sync := "SYNC " + strconv.Itoa(len(root)) + ":" + root + " mode=fast link-mbps=100 concurrency=2 comp=none\r\n" + syncBody.String()

	addr, pub := authServer(t, ServerOptions{RequireAuth: true, KeepAliveTimeout: 2 * time.Second, Deps: deps})
	kp, _ := clientKey(t)
	cfg := aead.ClientConfig{Mode: aead.ModeAge, Cipher: aead.AlgorithmAES, Static: kp, ServerKey: pub}
	send := func(request string) ([]string, error) {
		s, err := noiseDial(t, addr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return noiseCommand(t, s, request)
	}
	acked := func() int64 {
		got, ok := deps.GetTransferSummary(txferID)
		if !ok {
			t.Fatal("transfer missing")
		}
		return got.DoneSize
	}

	if lines, err := send(ack + "x"); err == nil {
		t.Fatalf("ACK with a trailing byte was answered: %q", lines)
	}
	if got := acked(); got != 0 {
		t.Fatalf("unfinished ACK applied %d bytes", got)
	}
	if lines, err := send(sync + "x"); err == nil {
		t.Fatalf("SYNC with a trailing byte was answered: %q", lines)
	}
	if n := len(deps.ListTransfers()); n != 1 {
		t.Fatalf("unfinished SYNC created a transfer: %d transfers", n)
	}

	// The same requests, ended exactly, apply.
	if lines, err := send(ack); err != nil || lines[len(lines)-1] != "OK" {
		t.Fatalf("ACK = (%q, %v)", lines, err)
	}
	if got := acked(); got != e.Size {
		t.Fatalf("ACK applied %d bytes, want %d", got, e.Size)
	}
	if lines, err := send(sync); err != nil || lines[len(lines)-1] != "OK" {
		t.Fatalf("SYNC = (%q, %v)", lines, err)
	}
	if n := len(deps.ListTransfers()); n != 2 {
		t.Fatalf("SYNC left %d transfers, want 2", n)
	}
}
