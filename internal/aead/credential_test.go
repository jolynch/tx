package aead

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"filippo.io/age"
)

var testAlgorithms = []Algorithm{AlgorithmAES, AlgorithmChaCha20}

func TestNewAndValidateAuthToken(t *testing.T) {
	token, err := NewAuthToken()
	if err != nil {
		t.Fatalf("NewAuthToken: %v", err)
	}
	if len(token) != 64 || strings.Trim(token, "0123456789abcdef") != "" {
		t.Fatalf("token = %q, want 64 lowercase hex characters", token)
	}
	if err := ValidateAuthToken(token); err != nil {
		t.Fatalf("generated token rejected: %v", err)
	}
	id, _ := age.GenerateX25519Identity()
	for name, token := range map[string]string{
		"short":             strings.Repeat("a", minAuthTokenLen-1),
		"long":              strings.Repeat("a", maxAuthTokenLen+1),
		"space":             strings.Repeat("a", minAuthTokenLen) + " b",
		"tab":               strings.Repeat("a", minAuthTokenLen) + "\tb",
		"newline":           strings.Repeat("a", minAuthTokenLen) + "\nb",
		"non-ascii":         strings.Repeat("a", minAuthTokenLen) + "é",
		"recipient":         id.Recipient().String(),
		"upper recipient":   strings.ToUpper(id.Recipient().String()),
		"bad recipient":     id.Recipient().String()[:61] + "q",
		"private key":       id.String(),
		"lower private key": strings.ToLower(id.String()),
	} {
		err := ValidateAuthToken(token)
		if err == nil {
			t.Errorf("%s: ValidateAuthToken accepted an invalid token", name)
			continue
		}
		if strings.Contains(err.Error(), token) {
			t.Errorf("%s: error echoes the token", name)
		}
	}
	// 16 hex characters, as earlier releases generated, still qualify.
	for _, token := range []string{"0123456789abcdef", strings.Repeat("a", minAuthTokenLen), strings.Repeat("~", maxAuthTokenLen)} {
		if err := ValidateAuthToken(token); err != nil {
			t.Fatalf("valid token of %d bytes rejected: %v", len(token), err)
		}
	}
}

func TestNewAllowlist(t *testing.T) {
	id, _ := age.GenerateX25519Identity()
	recipient := id.Recipient().String()
	token := strings.Repeat("t", 40)

	pub, _ := ParseRecipientKey(recipient)
	list, err := NewAllowlist([]string{token, token}, []string{recipient, recipient})
	if err != nil || list.Len() != 2 || !list.HasTokens() || list.Empty() {
		t.Fatalf("NewAllowlist = (%+v, %v)", list, err)
	}
	for _, e := range list.ages {
		if e.Mode != ModeAge || e.ID != CredentialID(ModeAge, pub[:]) {
			t.Fatalf("recipient entry = %+v", e)
		}
	}
	for _, e := range list.tokens {
		if e.Mode != ModeToken || e.ID != CredentialID(ModeToken, []byte(token)) {
			t.Fatalf("token entry = %+v", e)
		}
	}
	// A recipient never doubles as a token: its token-mode ID differs.
	if CredentialID(ModeToken, pub[:]) == CredentialID(ModeAge, pub[:]) {
		t.Fatal("token and age IDs collide")
	}
	// Entries are typed by list. The age prefixes are reserved, so a key in
	// the token list fails, and anything but a recipient fails in the other.
	for name, raw := range map[string]string{
		"recipient":     recipient,
		"private key":   id.String(),
		"lower private": strings.ToLower(id.String()),
		"short token":   "short",
	} {
		if _, err := NewAllowlist([]string{token, raw}, nil); err == nil {
			t.Errorf("token list accepted %s", name)
		}
	}
	for name, raw := range map[string]string{
		"bad checksum":    recipient[:len(recipient)-1] + flipBech32(recipient[len(recipient)-1]),
		"truncated":       recipient[:len(recipient)-1],
		"private key":     id.String(),
		"lower private":   strings.ToLower(id.String()),
		"upper recipient": strings.ToUpper(recipient),
		"token":           token,
	} {
		_, err := NewAllowlist(nil, []string{recipient, raw})
		if err == nil {
			t.Errorf("recipient list accepted %s", name)
			continue
		}
		if strings.Contains(err.Error(), raw) {
			t.Errorf("%s: error echoes the entry", name)
		}
	}
}

func TestAllowlistMatch(t *testing.T) {
	id, _ := age.GenerateX25519Identity()
	recipient := id.Recipient().String()
	pub, _ := ParseRecipientKey(recipient)
	tokenA, tokenB := strings.Repeat("a", 20), strings.Repeat("b", 20)
	list, err := NewAllowlist([]string{tokenA, tokenB}, []string{recipient})
	if err != nil {
		t.Fatal(err)
	}
	dh := make([]byte, 32)
	dh[0] = 1
	otherDH := make([]byte, 32)
	otherDH[0] = 2

	if e, ok := list.match(ModeToken, dh, credentialTag(ModeToken, []byte(tokenB), dh)); !ok || e.token != tokenB {
		t.Fatalf("token match = (%+v, %v), want tokenB", e, ok)
	}
	if e, ok := list.match(ModeAge, dh, credentialTag(ModeAge, pub[:], dh)); !ok || e.key != pub {
		t.Fatalf("recipient match = (%+v, %v)", e, ok)
	}
	// A tag made under another DH, or for another mode, matches nothing:
	// the recipient's bytes never act as a token, nor a token as a key.
	for name, tc := range map[string]struct {
		mode string
		tag  string
	}{
		"other dh":              {ModeToken, credentialTag(ModeToken, []byte(tokenA), otherDH)},
		"recipient as token":    {ModeToken, credentialTag(ModeToken, pub[:], dh)},
		"token as recipient":    {ModeAge, credentialTag(ModeAge, []byte(tokenA), dh)},
		"age tag in token mode": {ModeToken, credentialTag(ModeAge, pub[:], dh)},
		"stable ID":             {ModeToken, CredentialID(ModeToken, []byte(tokenA))},
	} {
		if _, ok := list.match(tc.mode, dh, tc.tag); ok {
			t.Errorf("%s: matched", name)
		}
	}
}

func flipBech32(c byte) string {
	if c == 'q' {
		return "p"
	}
	return "q"
}

func TestKeyEncodingRoundTrip(t *testing.T) {
	for range 32 {
		id, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatal(err)
		}
		kp, err := ParseIdentityKey(id.String())
		if err != nil {
			t.Fatalf("ParseIdentityKey: %v", err)
		}
		pub, err := ParseRecipientKey(id.Recipient().String())
		if err != nil || pub != kp.Public {
			t.Fatalf("ParseRecipientKey = (%x, %v), want %x", pub, err, kp.Public)
		}
		// Cross-check against age: the key derived from the identity must
		// encode to the recipient age derives.
		if got := testEncodeRecipient(kp.Public[:]); got != id.Recipient().String() {
			t.Fatalf("derived recipient = %q, want %q", got, id.Recipient())
		}
	}
}

func TestNonCanonicalKeysRejected(t *testing.T) {
	id, _ := age.GenerateX25519Identity()
	pub, err := ParseRecipientKey(id.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	high := pub
	high[31] |= 0x80 // the same X25519 key with bit 255 set
	var atP [32]byte = p25519
	var aboveP [32]byte = p25519
	aboveP[0]++
	for name, k := range map[string][32]byte{"bit 255": high, "p": atP, "p+1": aboveP} {
		if CanonicalPublicKey(k[:]) {
			t.Errorf("%s: CanonicalPublicKey accepted", name)
		}
		s := testEncodeRecipient(k[:])
		if _, err := age.ParseX25519Recipient(s); err != nil {
			continue // age itself rejects it; nothing more to check
		}
		if _, err := ParseRecipientKey(s); err == nil {
			t.Errorf("%s: ParseRecipientKey accepted a non-canonical recipient", name)
		}
		if _, err := NewAllowlist(nil, []string{s}); err == nil {
			t.Errorf("%s: NewAllowlist accepted a non-canonical recipient", name)
		}
	}
	below := p25519
	below[0]--
	if !CanonicalPublicKey(below[:]) {
		t.Fatal("p-1 rejected")
	}
}

func TestLoadOrGenerateAgeIdentity(t *testing.T) {
	t.Run("default missing directory is ephemeral", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "missing")
		identity, ephemeral, err := LoadOrGenerateAgeIdentity(dir, true)
		if err != nil || identity == nil || !ephemeral {
			t.Fatalf("LoadOrGenerateAgeIdentity = (%v, %t, %v), want ephemeral identity", identity, ephemeral, err)
		}
		if _, err := os.Stat(filepath.Join(dir, "key")); !os.IsNotExist(err) {
			t.Fatalf("ephemeral identity wrote a key file: %v", err)
		}
	})

	t.Run("explicit missing directory fails", func(t *testing.T) {
		_, ephemeral, err := LoadOrGenerateAgeIdentity(filepath.Join(t.TempDir(), "missing"), false)
		if err == nil || ephemeral {
			t.Fatalf("LoadOrGenerateAgeIdentity = (_, %t, %v), want non-ephemeral error", ephemeral, err)
		}
	})

	t.Run("new key persists and reloads", func(t *testing.T) {
		dir := t.TempDir()
		created, ephemeral, err := LoadOrGenerateAgeIdentity(dir, false)
		if err != nil || created == nil || ephemeral {
			t.Fatalf("generate identity = (%v, %t, %v)", created, ephemeral, err)
		}
		keyPath := filepath.Join(dir, "key")
		info, err := os.Stat(keyPath)
		if err != nil {
			t.Fatalf("stat persisted key: %v", err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("key permissions = %o, want 600", got)
		}
		reloaded, ephemeral, err := LoadOrGenerateAgeIdentity(dir, false)
		if err != nil || reloaded == nil || ephemeral {
			t.Fatalf("reload identity = (%v, %t, %v)", reloaded, ephemeral, err)
		}
		if reloaded.String() != created.String() {
			t.Fatal("reloaded identity differs from persisted identity")
		}
	})
}

func TestLoadAgeIdentityRejectsMalformedKey(t *testing.T) {
	if identity, err := LoadAgeIdentity(filepath.Join(t.TempDir(), "missing")); err != nil || identity != nil {
		t.Fatalf("missing key = (%v, %v), want (nil, nil)", identity, err)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "key"), []byte("# comment\nnot-an-age-key\n"), 0o600); err != nil {
		t.Fatalf("write malformed key: %v", err)
	}
	if _, err := LoadAgeIdentity(dir); err == nil {
		t.Fatal("LoadAgeIdentity accepted malformed key")
	}
}

func TestPublishKeyFileNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	var wg sync.WaitGroup
	got := make([]string, 8)
	errs := make([]error, 8)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _, err := LoadOrGenerateAgeIdentity(dir, false)
			errs[i] = err
			if id != nil {
				got[i] = id.String()
			}
		}()
	}
	wg.Wait()
	for i := range got {
		if errs[i] != nil {
			t.Fatalf("concurrent create %d: %v", i, errs[i])
		}
		if got[i] != got[0] {
			t.Fatal("concurrent creators returned different identities")
		}
	}
	// A stray temporary file from a crash is ignored.
	if err := os.WriteFile(filepath.Join(dir, ".key.tmp-crashed"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, _, err := LoadOrGenerateAgeIdentity(dir, false)
	if err != nil || id.String() != got[0] {
		t.Fatalf("reload after stray temp = (%v, %v)", id, err)
	}
}

func TestUnwritableKeysDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	id, ephemeral, err := LoadOrGenerateAgeIdentity(dir, true)
	if err != nil || id == nil || !ephemeral {
		t.Fatalf("default unwritable = (%v, %t, %v), want ephemeral", id, ephemeral, err)
	}
	if _, _, err := LoadOrGenerateAgeIdentity(dir, false); err == nil || !errors.Is(err, os.ErrPermission) {
		t.Fatalf("explicit unwritable error = %v, want permission error", err)
	}
}

// testEncodeRecipient Bech32-encodes a key as an age recipient. Production
// code never encodes keys; tests use this to build recipients age generates
// and ones it would not.
func testEncodeRecipient(key []byte) string {
	gen := [5]uint32{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	polymod := func(values []byte) uint32 {
		chk := uint32(1)
		for _, v := range values {
			top := chk >> 25
			chk = (chk&0x1ffffff)<<5 ^ uint32(v)
			for i := range 5 {
				if (top>>uint(i))&1 == 1 {
					chk ^= gen[i]
				}
			}
		}
		return chk
	}
	var data []byte
	var acc uint32
	var bits uint
	for _, b := range key {
		acc = acc<<8 | uint32(b)
		bits += 8
		for bits >= 5 {
			bits -= 5
			data = append(data, byte(acc>>bits&31))
		}
	}
	if bits > 0 {
		data = append(data, byte(acc<<(5-bits)&31))
	}
	values := append([]byte{3, 3, 3, 0, 1, 7, 5}, data...) // "age" expanded
	mod := polymod(append(values, 0, 0, 0, 0, 0, 0)) ^ 1
	out := []byte("age1")
	for _, v := range data {
		out = append(out, bech32Charset[v])
	}
	for i := range 6 {
		out = append(out, bech32Charset[(mod>>uint(5*(5-i)))&31])
	}
	return string(out)
}
