package aead

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/flynn/noise"
	"github.com/jolynch/tx/internal/utils"
	"golang.org/x/crypto/curve25519"
)

const (
	// PrefaceVersion names the handshake in the AUTH preface.
	PrefaceVersion = "noise-v1"
	// MaxPrefaceBytes caps an AUTH line, terminator included.
	MaxPrefaceBytes = 256
	// HandshakeTimeout bounds a handshake from accept (server) or start
	// (client) through FINISH.
	HandshakeTimeout = 5 * time.Second

	prologueLabel     = "tx/ftcp/noise-v1"
	handshakeMsg1Size = 96
	handshakeMsg2Size = 48
	credentialIDLen   = 64
)

// Plaintext diagnostics a server may send before message 2. They are
// unauthenticated hints.
const (
	DiagUnsupportedAuth      = "ERR UNSUPPORTED_AUTH expected noise-v1"
	DiagNotAuthorized        = "ERR NOT_AUTHORIZED authorization failed"
	DiagTokenNotConfigured   = "ERR NOT_AUTHORIZED token authentication not configured"
	DiagBusy                 = "ERR BUSY server at handshake capacity"
	diagMaxBytes             = MaxPrefaceBytes
	msg2Prefix0, msg2Prefix1 = 0x00, handshakeMsg2Size
)

// DiagClientTooOld answers the unversioned `AUTH key` that clients from
// before noise-v1 send first; they print it and stop.
const DiagClientTooOld = "ERR UNSUPPORTED_AUTH client predates noise-v1; upgrade tx"

// Client-side handshake outcomes.
var (
	ErrServerBusy          = errors.New("server at handshake capacity")
	ErrNotAuthorized       = errors.New("server rejected authentication")
	ErrTokenNotConfigured  = errors.New("server has no token authentication configured")
	ErrUnsupportedAuth     = errors.New("server does not support noise-v1 authentication")
	ErrHandshakeRejected   = errors.New("handshake failed")
	errMalformedHandshake  = errors.New("malformed handshake")
	errHandshakeDiagnostic = errors.New("unexpected handshake response")
	errServerProof         = errors.New("message 2 failed authentication: the server did not prove the discovered key")
)

// DiagnosticError is a server-side refusal before message 2. Line, without
// its terminator, is the plaintext diagnostic the server may send.
type DiagnosticError struct {
	Line string
}

func (e *DiagnosticError) Error() string { return e.Line }

// Preface is a parsed AUTH noise-v1 line.
type Preface struct {
	Mode   string
	Cipher Algorithm
	ID     string
	// Line is the exact line, including CRLF, bound into the prologue.
	Line []byte
}

// IsNoisePreface reports whether line begins an AUTH noise-v1 preface.
func IsNoisePreface(line []byte) bool {
	return bytes.HasPrefix(line, []byte("AUTH "+PrefaceVersion+" "))
}

// ParsePreface strictly parses `AUTH noise-v1 <mode> <cipher> <id>\r\n`.
func ParsePreface(line []byte) (Preface, error) {
	if len(line) > MaxPrefaceBytes || !bytes.HasSuffix(line, []byte("\r\n")) {
		return Preface{}, errMalformedHandshake
	}
	fields := strings.Split(string(line[:len(line)-2]), " ")
	if len(fields) != 5 || fields[0] != "AUTH" || fields[1] != PrefaceVersion {
		return Preface{}, errMalformedHandshake
	}
	mode := fields[2]
	if mode != ModeToken && mode != ModeAge {
		return Preface{}, errMalformedHandshake
	}
	cipher, err := validateAlgorithm(Algorithm(fields[3]))
	if err != nil {
		return Preface{}, errMalformedHandshake
	}
	id := fields[4]
	if len(id) != credentialIDLen || strings.Trim(id, "0123456789abcdef") != "" {
		return Preface{}, errMalformedHandshake
	}
	return Preface{Mode: mode, Cipher: cipher, ID: id, Line: line}, nil
}

func cipherSuite(alg Algorithm) (noise.CipherSuite, error) {
	switch alg {
	case AlgorithmAES:
		return noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256), nil
	case AlgorithmChaCha20:
		return noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256), nil
	default:
		return nil, fmt.Errorf("unsupported cipher %q", alg)
	}
}

func prologue(line []byte) []byte {
	out := make([]byte, 0, len(prologueLabel)+1+len(line))
	out = append(out, prologueLabel...)
	out = append(out, 0)
	return append(out, line...)
}

// handshakeParams describes one side of a handshake.
type handshakeParams struct {
	mode      string
	alg       Algorithm
	initiator bool
	line      []byte // the AUTH line, bound into the prologue
	psk       []byte // token mode only
	static    KeyPair
	peer      []byte    // the server's static key, initiator only
	random    io.Reader // the initiator's ephemeral source; nil for rand.Reader
}

// handshakeConfig builds the flynn/noise configuration. Token mode must carry
// exactly a 32-byte PSK and age mode none: flynn/noise adds the psk0
// modifier only for a non-empty PSK, so an empty token PSK would silently
// run plain IK without the token.
func handshakeConfig(p handshakeParams) (noise.Config, error) {
	mode, psk, static, peer := p.mode, p.psk, p.static, p.peer
	cs, err := cipherSuite(p.alg)
	if err != nil {
		return noise.Config{}, err
	}
	switch mode {
	case ModeToken:
		if len(psk) != 32 {
			return noise.Config{}, errors.New("token mode requires a 32-byte PSK")
		}
	case ModeAge:
		if len(psk) != 0 {
			return noise.Config{}, errors.New("age mode takes no PSK")
		}
	default:
		return noise.Config{}, fmt.Errorf("unknown mode %q", mode)
	}
	if isZeroKey(static.Private) || !CanonicalPublicKey(static.Public[:]) {
		return noise.Config{}, errors.New("invalid static key pair")
	}
	if peer != nil && !CanonicalPublicKey(peer) {
		return noise.Config{}, errors.New("invalid peer static key")
	}
	random := p.random
	if random == nil {
		random = rand.Reader
	}
	return noise.Config{
		CipherSuite:           cs,
		Random:                random,
		Pattern:               noise.HandshakeIK,
		Initiator:             p.initiator,
		Prologue:              prologue(p.line),
		PresharedKey:          psk,
		PresharedKeyPlacement: 0,
		StaticKeypair:         noise.DHKey{Private: static.Private[:], Public: static.Public[:]},
		PeerStatic:            peer,
	}, nil
}

// ClientConfig selects one credential for a handshake.
type ClientConfig struct {
	Mode      string // ModeToken or ModeAge
	Cipher    Algorithm
	Token     string  // token mode only
	Static    KeyPair // the client's key pair, in both modes
	ServerKey [keySize]byte
}

// ClientHandshake authenticates conn. It writes the preface and message 1
// together, reads message 2 or a diagnostic, and sends FINISH. The caller
// sets conn's deadline and closes conn on error.
func ClientHandshake(conn net.Conn, cfg ClientConfig) (*Session, error) {
	var psk []byte
	var credential []byte
	switch cfg.Mode {
	case ModeToken:
		if err := ValidateAuthToken(cfg.Token); err != nil {
			return nil, err
		}
		psk = tokenPSK(cfg.Token)
		credential = []byte(cfg.Token)
	case ModeAge:
		if cfg.Token != "" {
			return nil, errors.New("age mode takes no token")
		}
		credential = cfg.Static.Public[:]
	default:
		return nil, fmt.Errorf("unknown mode %q", cfg.Mode)
	}
	// The client draws its ephemeral key first so the preface can carry a
	// tag keyed by es = DH(e, server static), which only the server can
	// recompute. flynn/noise generates e itself from Config.Random and reads
	// nothing else from it, so it gets exactly these 32 bytes: a further
	// read fails, and message 1 is checked to have consumed them all.
	var ephemeral [keySize]byte
	if _, err := rand.Read(ephemeral[:]); err != nil {
		return nil, err
	}
	defer clear(ephemeral[:])
	dh, err := curve25519.X25519(ephemeral[:], cfg.ServerKey[:])
	if err != nil {
		return nil, fmt.Errorf("server key: %w", err)
	}
	tag := credentialTag(cfg.Mode, credential, dh)
	clear(dh)
	line := []byte("AUTH " + PrefaceVersion + " " + cfg.Mode + " " + string(cfg.Cipher) + " " + tag + "\r\n")
	eSource := bytes.NewReader(ephemeral[:])
	config, err := handshakeConfig(handshakeParams{
		mode: cfg.Mode, alg: cfg.Cipher, initiator: true, line: line, psk: psk,
		static: cfg.Static, peer: cfg.ServerKey[:], random: eSource,
	})
	if err != nil {
		return nil, err
	}
	hs, err := noise.NewHandshakeState(config)
	if err != nil {
		return nil, err
	}
	msg1, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, err
	}
	if len(msg1) != handshakeMsg1Size {
		return nil, fmt.Errorf("message 1 is %d bytes, want %d", len(msg1), handshakeMsg1Size)
	}
	if eSource.Len() != 0 {
		return nil, errors.New("message 1 does not carry the tagged ephemeral key")
	}
	out := make([]byte, 0, len(line)+recordPrefixSize+len(msg1))
	out = append(out, line...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(msg1)))
	out = append(out, msg1...)
	if err := writeAll(conn, out); err != nil {
		return nil, err
	}

	br := bufio.NewReader(conn)
	head, err := br.Peek(2)
	if err != nil {
		return nil, message2Error(err)
	}
	if head[0] != msg2Prefix0 || head[1] != msg2Prefix1 {
		return nil, readDiagnostic(br)
	}
	msg2 := make([]byte, recordPrefixSize+handshakeMsg2Size)
	if _, err := io.ReadFull(br, msg2); err != nil {
		return nil, message2Error(err)
	}
	payload, cs1, cs2, err := hs.ReadMessage(nil, msg2[recordPrefixSize:])
	if err != nil || len(payload) != 0 || cs1 == nil || cs2 == nil {
		// Not a refusal of this credential: the peer did not prove the
		// server key, so trying another token would not help.
		return nil, errServerProof
	}
	s, err := newSession(conn, br, cfg.Cipher, cs1, cs2)
	if err != nil {
		return nil, err
	}
	if err := s.writeFinish(); err != nil {
		return nil, err
	}
	return s, nil
}

// message2Error classifies a failure to read message 2. A deadline is the
// caller's to report; only a peer that ends the exchange counts as a
// rejection, which moves token selection to the next token.
func message2Error(err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("read message 2: %w", err)
	}
	return fmt.Errorf("%w: read message 2: %w", ErrHandshakeRejected, err)
}

// readDiagnostic maps a bounded plaintext line to a client error.
func readDiagnostic(br *bufio.Reader) error {
	line, err := utils.ReadLineLimit(br, diagMaxBytes)
	if err != nil {
		return fmt.Errorf("%w: %w", errHandshakeDiagnostic, err)
	}
	text := string(utils.TrimLineTerminator(line))
	switch text {
	case DiagBusy:
		return ErrServerBusy
	case DiagNotAuthorized:
		return ErrNotAuthorized
	case DiagTokenNotConfigured:
		return ErrTokenNotConfigured
	case DiagUnsupportedAuth:
		return ErrUnsupportedAuth
	}
	// Name what the server sent, as printable ASCII only: it is
	// unauthenticated, and bounded by the loop above.
	printable := strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return '?'
		}
		return r
	}, text)
	return fmt.Errorf("%w: %q", errHandshakeDiagnostic, printable)
}

// ServerConfig is a server's authentication policy.
type ServerConfig struct {
	Static    KeyPair
	Allowlist *Allowlist
	// AnyKey accepts any age key whose private key the client proves. It
	// applies only when the allowlist is empty.
	AnyKey bool
}

// Peer describes an authenticated client.
type Peer struct {
	Mode   string
	ID     string // the credential's stable CredentialID, never sent
	Static [keySize]byte
}

// ServerHandshake authenticates a client whose AUTH noise-v1 line the
// server has read from br. It reads message 1 before matching the preface's
// credential tag, which is keyed by the client's ephemeral in it. On a
// refusal before message 2 it returns a *DiagnosticError; the caller may
// write its line, then drain the client's opening briefly before closing so
// the close does not reset the connection. It returns only after verifying
// FINISH. The caller sets the deadline.
func ServerHandshake(conn net.Conn, br *bufio.Reader, line []byte, cfg ServerConfig) (*Session, Peer, error) {
	pf, err := ParsePreface(line)
	if err != nil {
		return nil, Peer{}, &DiagnosticError{Line: DiagUnsupportedAuth}
	}
	switch {
	case pf.Mode == ModeToken && !cfg.Allowlist.HasTokens():
		return nil, Peer{}, &DiagnosticError{Line: DiagTokenNotConfigured}
	case pf.Mode == ModeAge && cfg.Allowlist.Empty() && !cfg.AnyKey:
		return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
	}

	var hdr [recordPrefixSize]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, Peer{}, fmt.Errorf("read message 1: %w", err)
	}
	if binary.BigEndian.Uint16(hdr[:]) != handshakeMsg1Size {
		return nil, Peer{}, errMalformedHandshake
	}
	msg1 := make([]byte, handshakeMsg1Size)
	if _, err := io.ReadFull(br, msg1); err != nil {
		return nil, Peer{}, fmt.Errorf("read message 1: %w", err)
	}
	// Message 1 opens with the client's ephemeral in the clear, in both IK
	// and IKpsk0. A low-order key yields an all-zero DH, which X25519 refuses.
	dh, err := curve25519.X25519(cfg.Static.Private[:], msg1[:keySize])
	if err != nil {
		return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
	}
	defer clear(dh)

	var psk []byte
	var entry AuthEntry
	haveEntry := false
	if pf.Mode == ModeToken || !cfg.Allowlist.Empty() {
		e, ok := cfg.Allowlist.match(pf.Mode, dh, pf.ID)
		if !ok {
			return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
		}
		entry, haveEntry = e, true
		if pf.Mode == ModeToken {
			psk = tokenPSK(e.token)
		}
	}
	config, err := handshakeConfig(handshakeParams{
		mode: pf.Mode, alg: pf.Cipher, line: pf.Line, psk: psk, static: cfg.Static,
	})
	if err != nil {
		return nil, Peer{}, err
	}
	hs, err := noise.NewHandshakeState(config)
	if err != nil {
		return nil, Peer{}, err
	}
	payload, _, _, err := hs.ReadMessage(nil, msg1)
	if err != nil || len(payload) != 0 {
		return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
	}
	rs := hs.PeerStatic()
	if !CanonicalPublicKey(rs) {
		return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
	}
	peer := Peer{Mode: pf.Mode}
	copy(peer.Static[:], rs)
	if pf.Mode == ModeToken {
		peer.ID = entry.ID
	} else {
		// The tag must name the key the client proved, whether it matched an
		// allowlisted recipient or the server accepts any key.
		tag := credentialTag(ModeAge, rs, dh)
		if subtle.ConstantTimeCompare([]byte(tag), []byte(pf.ID)) != 1 {
			return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
		}
		if haveEntry && !equalKeys(rs, entry.key[:]) {
			return nil, Peer{}, &DiagnosticError{Line: DiagNotAuthorized}
		}
		peer.ID = CredentialID(ModeAge, rs)
	}
	msg2, cs1, cs2, err := hs.WriteMessage(nil, nil)
	if err != nil || cs1 == nil || cs2 == nil {
		return nil, Peer{}, fmt.Errorf("write message 2: %w", err)
	}
	if len(msg2) != handshakeMsg2Size {
		return nil, Peer{}, fmt.Errorf("message 2 is %d bytes, want %d", len(msg2), handshakeMsg2Size)
	}
	out := binary.BigEndian.AppendUint16(make([]byte, 0, recordPrefixSize+len(msg2)), uint16(len(msg2)))
	if err := writeAll(conn, append(out, msg2...)); err != nil {
		return nil, Peer{}, err
	}
	s, err := newSession(conn, br, pf.Cipher, cs2, cs1)
	if err != nil {
		return nil, Peer{}, err
	}
	if err := s.readFinish(); err != nil {
		return nil, Peer{}, err
	}
	return s, peer, nil
}

// writeAll writes p in one call; io.Writer reports any short write as an
// error, and a writer that breaks that contract is caught here.
func writeAll(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	return err
}
