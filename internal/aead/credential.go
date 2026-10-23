package aead

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/sysio"
)

// Credential modes, as they appear in the AUTH preface.
const (
	ModeToken = "token"
	ModeAge   = "age"
)

const (
	minAuthTokenLen = 16
	maxAuthTokenLen = 4096

	pskLabel = "tx/ftcp/noise-v1/psk/"
	idLabel  = "tx/ftcp/noise-v1/id/"
)

// NewAuthToken returns 64 hexadecimal characters encoding 32 random bytes.
func NewAuthToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate auth token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// hasReservedPrefix reports whether s begins with an age encoding prefix.
// Such strings are never opaque tokens: a mistyped recipient or a pasted
// private key must fail rather than become a usable secret.
func hasReservedPrefix(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "age1") || strings.HasPrefix(lower, identityHRP)
}

// ValidateAuthToken checks an opaque token: 16–4096 bytes, each in ASCII
// 33–126, and neither age prefix. Errors never echo the value.
func ValidateAuthToken(s string) error {
	if hasReservedPrefix(s) {
		return errors.New("auth token must not be an age key; use an age identity for key authentication")
	}
	if len(s) < minAuthTokenLen || len(s) > maxAuthTokenLen {
		return fmt.Errorf("auth token must be %d–%d bytes, got %d", minAuthTokenLen, maxAuthTokenLen, len(s))
	}
	for i := range len(s) {
		if s[i] < 33 || s[i] > 126 {
			return errors.New("auth token must contain only visible ASCII characters")
		}
	}
	return nil
}

// CredentialID is a credential's stable fingerprint: the lowercase hex
// SHA-256 of a domain label, the mode, a zero byte, and the credential: the
// token's bytes, or the age key's raw 32-byte public key. It deduplicates
// allowlist entries and names an authenticated peer. It is never sent: the
// preface carries credentialTag instead, so an observer cannot test token
// guesses against it or link connections.
func CredentialID(mode string, credential []byte) string {
	h := sha256.New()
	h.Write([]byte(idLabel))
	h.Write([]byte(mode))
	h.Write([]byte{0})
	h.Write(credential)
	return hex.EncodeToString(h.Sum(nil))
}

// credentialTag is the per-connection credential identifier in the AUTH
// preface: the lowercase hex HMAC-SHA256, keyed by dh, of the same input as
// CredentialID. dh is X25519 of the client's ephemeral key and the server's
// static key, Noise's "es", so only the two endpoints can compute it.
func credentialTag(mode string, credential, dh []byte) string {
	m := hmac.New(sha256.New, dh)
	writeTagInput(m, mode, credential)
	return hex.EncodeToString(m.Sum(nil))
}

// writeTagInput writes credentialTag's message: the label, the mode, a zero
// byte, and the credential.
func writeTagInput(m hash.Hash, mode string, credential []byte) {
	m.Write([]byte(idLabel))
	m.Write([]byte(mode))
	m.Write([]byte{0})
	m.Write(credential)
}

// tokenPSK derives the token-mode pre-shared key.
func tokenPSK(token string) []byte {
	h := sha256.New()
	h.Write([]byte(pskLabel))
	h.Write([]byte(ModeToken))
	h.Write([]byte{0})
	h.Write([]byte(token))
	return h.Sum(nil)
}

// AuthEntry is one parsed server allowlist entry.
type AuthEntry struct {
	Mode  string // ModeToken or ModeAge
	ID    string // CredentialID, the stable fingerprint
	token string
	key   [keySize]byte
	cred  []byte // the bytes credentialTag takes: the token or the key
}

// tokenEntry parses one allowlisted token.
func tokenEntry(s string) (AuthEntry, error) {
	if err := ValidateAuthToken(s); err != nil {
		return AuthEntry{}, err
	}
	return AuthEntry{Mode: ModeToken, ID: CredentialID(ModeToken, []byte(s)), token: s, cred: []byte(s)}, nil
}

// recipientEntry parses one allowlisted client age recipient. A private key
// is refused with a hint rather than parsed, so it never reaches a log.
func recipientEntry(s string) (AuthEntry, error) {
	if strings.HasPrefix(strings.ToLower(s), identityHRP) {
		return AuthEntry{}, errors.New("value is an age private key; allow its public recipient instead")
	}
	key, err := ParseRecipientKey(s)
	if err != nil {
		return AuthEntry{}, err
	}
	return AuthEntry{Mode: ModeAge, ID: CredentialID(ModeAge, key[:]), key: key, cred: key[:]}, nil
}

// Allowlist is a server's set of authorized tokens and client age
// recipients, deduplicated by credential ID. Entries are typed by the list
// they came from, so a recipient never acts as a token.
type Allowlist struct {
	tokens map[string]AuthEntry
	ages   map[string]AuthEntry
}

// NewAllowlist parses tokens and client age recipients. Any invalid entry is
// an error: an allowlist never silently loses entries and widens access. The
// age prefixes are reserved, so a recipient or private key in the token list
// is an error too.
func NewAllowlist(tokens, recipients []string) (*Allowlist, error) {
	a := &Allowlist{tokens: map[string]AuthEntry{}, ages: map[string]AuthEntry{}}
	for i, raw := range tokens {
		e, err := tokenEntry(raw)
		if err != nil {
			return nil, fmt.Errorf("token %d: %w", i+1, err)
		}
		a.tokens[e.ID] = e
	}
	for i, raw := range recipients {
		e, err := recipientEntry(raw)
		if err != nil {
			return nil, fmt.Errorf("client recipient %d: %w", i+1, err)
		}
		a.ages[e.ID] = e
	}
	return a, nil
}

// Empty reports whether the list has no entries.
func (a *Allowlist) Empty() bool {
	return a == nil || len(a.tokens)+len(a.ages) == 0
}

// HasTokens reports whether the list allows any token.
func (a *Allowlist) HasTokens() bool { return a != nil && len(a.tokens) > 0 }

// Len is the number of distinct entries.
func (a *Allowlist) Len() int {
	if a == nil {
		return 0
	}
	return len(a.tokens) + len(a.ages)
}

// match finds the entry of mode whose credentialTag under dh is tag. It
// checks every entry of the mode in constant time per comparison and does not
// stop at a match, so timing does not reveal which entry matched. The cost is
// one HMAC per entry of the mode, reusing one HMAC state and output buffer.
func (a *Allowlist) match(mode string, dh []byte, tag string) (AuthEntry, bool) {
	if a == nil {
		return AuthEntry{}, false
	}
	want, err := hex.DecodeString(tag)
	if err != nil || len(want) != sha256.Size {
		return AuthEntry{}, false
	}
	entries := a.ages
	if mode == ModeToken {
		entries = a.tokens
	}
	m := hmac.New(sha256.New, dh)
	sum := make([]byte, 0, sha256.Size)
	var found AuthEntry
	ok := 0
	for _, e := range entries {
		m.Reset()
		writeTagInput(m, mode, e.cred)
		sum = m.Sum(sum[:0])
		if subtle.ConstantTimeCompare(sum, want) == 1 {
			found, ok = e, 1
		}
	}
	return found, ok == 1
}

// ---------------------------------------------------------------------------
// Age keys
// ---------------------------------------------------------------------------

const (
	recipientHRP = "age"
	identityHRP  = "age-secret-key-"
	keySize      = 32
)

// KeyPair is an X25519 static key pair as raw bytes, as the Noise handshake
// uses it. Its Public half is the key an age recipient string encodes.
type KeyPair struct {
	Private [keySize]byte
	Public  [keySize]byte
}

// GenerateKeyPair returns a fresh age identity's key pair.
func GenerateKeyPair() (KeyPair, error) {
	id, err := age.GenerateX25519Identity()
	if err != nil {
		return KeyPair{}, fmt.Errorf("generate age identity: %w", err)
	}
	return KeyPairFromIdentity(id)
}

// KeyPairFromIdentity converts a parsed age identity to raw key bytes.
func KeyPairFromIdentity(id *age.X25519Identity) (KeyPair, error) {
	if id == nil {
		return KeyPair{}, errors.New("nil age identity")
	}
	return ParseIdentityKey(id.String())
}

// ParseIdentityKey validates an age identity with age's parser, extracts its
// private key, and derives the public key with crypto/ecdh.
func ParseIdentityKey(s string) (KeyPair, error) {
	s = strings.TrimSpace(s)
	if _, err := age.ParseX25519Identity(s); err != nil {
		return KeyPair{}, errors.New("invalid age identity")
	}
	hrp, data, err := bech32Data(s)
	if err != nil || hrp != identityHRP || len(data) != keySize {
		return KeyPair{}, errors.New("invalid age identity encoding")
	}
	var kp KeyPair
	copy(kp.Private[:], data)
	clear(data)
	priv, err := ecdh.X25519().NewPrivateKey(kp.Private[:])
	if err != nil {
		return KeyPair{}, errors.New("invalid age identity key")
	}
	copy(kp.Public[:], priv.PublicKey().Bytes())
	return kp, nil
}

// ParseRecipientKey validates an age X25519 recipient with age's parser and
// returns its canonical 32-byte public key.
func ParseRecipientKey(s string) ([keySize]byte, error) {
	var out [keySize]byte
	s = strings.TrimSpace(s)
	if _, err := age.ParseX25519Recipient(s); err != nil {
		return out, errors.New("invalid age recipient")
	}
	hrp, data, err := bech32Data(s)
	if err != nil || hrp != recipientHRP || len(data) != keySize {
		return out, errors.New("invalid age recipient encoding")
	}
	if !CanonicalPublicKey(data) {
		return out, errors.New("non-canonical age recipient key")
	}
	copy(out[:], data)
	return out, nil
}

// p25519 is 2^255-19, little-endian.
var p25519 = [keySize]byte{
	0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
}

// CanonicalPublicKey reports whether pub is a 32-byte X25519 u-coordinate in
// canonical form: bit 255 clear and below p. X25519 ignores bit 255 and
// reduces u modulo p, so a non-canonical key aliases a canonical one and would
// give one key several recipient strings and credential IDs.
func CanonicalPublicKey(pub []byte) bool {
	if len(pub) != keySize || pub[keySize-1]&0x80 != 0 {
		return false
	}
	// Compare little-endian from the most significant byte.
	for i := keySize - 1; i >= 0; i-- {
		if pub[i] != p25519[i] {
			return pub[i] < p25519[i]
		}
	}
	return false // equal to p
}

// equalKeys compares two public keys in constant time.
func equalKeys(a, b []byte) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}

// isZeroKey reports whether k is all zero.
func isZeroKey(k [keySize]byte) bool {
	return k == [keySize]byte{}
}

// ---------------------------------------------------------------------------
// Key files
// ---------------------------------------------------------------------------

// LoadAgeIdentity loads an X25519 identity from dir/key. Returns (nil, nil)
// if the file does not exist. Returns an error if the file exists but is
// unreadable or malformed.
func LoadAgeIdentity(dir string) (*age.X25519Identity, error) {
	keyPath := path.Join(dir, "key")
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read key file %s: %w", keyPath, err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		identity, parseErr := age.ParseX25519Identity(line)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid existing key file %s: %w", keyPath, parseErr)
		}
		return identity, nil
	}
	return nil, fmt.Errorf("existing key file %s has no identity", keyPath)
}

// LoadOrGenerateAgeIdentity returns the identity persisted in dir, creating
// and persisting one when dir exists without a key. When the default
// directory (isDefault) is missing or cannot be written, it returns an
// ephemeral in-memory identity and reports ephemeral=true; an explicit
// directory never falls back. A persisted key is never overwritten: if
// another process publishes one first, that key is loaded instead.
func LoadOrGenerateAgeIdentity(dir string, isDefault bool) (identity *age.X25519Identity, ephemeral bool, err error) {
	ephemeralIdentity := func() (*age.X25519Identity, bool, error) {
		id, genErr := age.GenerateX25519Identity()
		if genErr != nil {
			return nil, false, fmt.Errorf("generate ephemeral identity: %w", genErr)
		}
		return id, true, nil
	}
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if !isDefault {
				return nil, false, fmt.Errorf("keys directory %s does not exist", dir)
			}
			return ephemeralIdentity()
		}
		return nil, false, fmt.Errorf("stat keys directory %s: %w", dir, err)
	}

	identity, err = LoadAgeIdentity(dir)
	if err != nil {
		return nil, false, err
	}
	if identity != nil {
		return identity, false, nil
	}

	identity, err = age.GenerateX25519Identity()
	if err != nil {
		return nil, false, fmt.Errorf("generate age identity: %w", err)
	}
	published, err := publishKeyFile(dir, identity)
	if err != nil {
		if isDefault && (errors.Is(err, fs.ErrPermission) || errors.Is(err, sysio.EROFS)) {
			return ephemeralIdentity()
		}
		return nil, false, err
	}
	if !published {
		// Another process published a key first; use it.
		identity, err = LoadAgeIdentity(dir)
		if err != nil {
			return nil, false, err
		}
		if identity == nil {
			return nil, false, fmt.Errorf("key file in %s disappeared after publication", dir)
		}
	}
	return identity, false, nil
}

// publishKeyFile writes identity crash-safely: to a private temporary file
// that is synced, then linked into place, which fails rather than overwrite
// an existing key. A crash leaves at most a stray temporary file, which
// LoadAgeIdentity ignores. It reports false when a key already existed.
func publishKeyFile(dir string, identity *age.X25519Identity) (bool, error) {
	tmp, err := os.CreateTemp(dir, ".key.tmp-*")
	if err != nil {
		return false, fmt.Errorf("create key file in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return false, fmt.Errorf("chmod key file: %w", err)
	}
	_, err = fmt.Fprintf(tmp, "# created: %s\n# public key: %s\n%s\n",
		time.Now().Format(time.RFC3339), identity.Recipient(), identity)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, fmt.Errorf("write key file: %w", err)
	}
	if err := os.Link(tmpPath, path.Join(dir, "key")); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("publish key file: %w", err)
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return true, nil
}
