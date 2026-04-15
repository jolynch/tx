package ftcp

import (
	"bytes"
	"encoding/base64"
	"io"
	"testing"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/aead"
)

func TestResolveAuthCipher(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    aead.Algorithm
		wantErr bool
	}{
		{name: "aes", raw: "aes", want: aead.AlgorithmAES},
		{name: "chacha20", raw: "chacha20", want: aead.AlgorithmChaCha20},
		{name: "invalid", raw: "bogus", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveAuthCipher(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAuthCipher err: %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveAuthCipher(%q)=%q want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestProcessAUTHRequestUsesExplicitCipher(t *testing.T) {
	serverID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate server identity: %v", err)
	}
	clientID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate client identity: %v", err)
	}

	var blob bytes.Buffer
	ew, err := aead.Encrypt(&blob, serverID.Recipient(), aead.Options{Algorithm: aead.AlgorithmChaCha20})
	if err != nil {
		t.Fatalf("encrypt auth blob: %v", err)
	}
	if _, err := io.WriteString(ew, clientID.Recipient().String()); err != nil {
		t.Fatalf("write auth blob: %v", err)
	}
	if err := ew.Close(); err != nil {
		t.Fatalf("close auth blob: %v", err)
	}

	req, err := ParseRequest([]byte("AUTH chacha20 " + base64.StdEncoding.EncodeToString(blob.Bytes())))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}

	result, err := processAUTHRequest(req, serverID, nil)
	if err != nil {
		t.Fatalf("processAUTHRequest err: %v", err)
	}
	if result.responseCipher != aead.AlgorithmChaCha20 {
		t.Fatalf("responseCipher=%q want %q", result.responseCipher, aead.AlgorithmChaCha20)
	}
	if !result.encryptedRequests {
		t.Fatal("expected encryptedRequests")
	}
}

// buildAuthBlob returns a base64-encoded AUTH blob plaintext of the form
// "<client_pubkey> [tok1 tok2 ...]" encrypted to the server identity.
func buildAuthBlob(t *testing.T, serverID *age.X25519Identity, algo aead.Algorithm, plaintext string) string {
	t.Helper()
	var blob bytes.Buffer
	ew, err := aead.Encrypt(&blob, serverID.Recipient(), aead.Options{Algorithm: algo})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := io.WriteString(ew, plaintext); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := ew.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return base64.StdEncoding.EncodeToString(blob.Bytes())
}

func TestProcessAUTHRequestAllowlistHit(t *testing.T) {
	serverID, _ := age.GenerateX25519Identity()
	clientID, _ := age.GenerateX25519Identity()
	enc := buildAuthBlob(t, serverID, aead.AlgorithmAES, clientID.Recipient().String()+" shared-secret-123")
	req, _ := ParseRequest([]byte("AUTH aes " + enc))

	result, err := processAUTHRequest(req, serverID, []string{"other-tok", "shared-secret-123"})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if !result.encryptedRequests {
		t.Fatal("expected encryptedRequests")
	}
}

func TestProcessAUTHRequestAllowlistMiss(t *testing.T) {
	serverID, _ := age.GenerateX25519Identity()
	clientID, _ := age.GenerateX25519Identity()
	enc := buildAuthBlob(t, serverID, aead.AlgorithmAES, clientID.Recipient().String()+" wrong-token-xx")
	req, _ := ParseRequest([]byte("AUTH aes " + enc))

	_, err := processAUTHRequest(req, serverID, []string{"right-token-yy"})
	if err == nil {
		t.Fatal("expected NOT_AUTHORIZED on token miss")
	}
}

func TestProcessAUTHRequestAllowlistPubkeyMatches(t *testing.T) {
	serverID, _ := age.GenerateX25519Identity()
	clientID, _ := age.GenerateX25519Identity()
	// Client sends no extra tokens; server allowlist contains the client's pubkey.
	enc := buildAuthBlob(t, serverID, aead.AlgorithmAES, clientID.Recipient().String())
	req, _ := ParseRequest([]byte("AUTH aes " + enc))

	result, err := processAUTHRequest(req, serverID, []string{clientID.Recipient().String()})
	if err != nil {
		t.Fatalf("expected success via pubkey match, got %v", err)
	}
	if !result.encryptedRequests {
		t.Fatal("expected encryptedRequests")
	}
}

func TestProcessAUTHRequestEmptyAllowlistAcceptsNoTokens(t *testing.T) {
	serverID, _ := age.GenerateX25519Identity()
	clientID, _ := age.GenerateX25519Identity()
	enc := buildAuthBlob(t, serverID, aead.AlgorithmAES, clientID.Recipient().String())
	req, _ := ParseRequest([]byte("AUTH aes " + enc))

	if _, err := processAUTHRequest(req, serverID, nil); err != nil {
		t.Fatalf("expected success with empty allowlist, got %v", err)
	}
}

func TestProcessAUTHRequestEmptyAllowlistIgnoresPresentedTokens(t *testing.T) {
	serverID, _ := age.GenerateX25519Identity()
	clientID, _ := age.GenerateX25519Identity()
	enc := buildAuthBlob(t, serverID, aead.AlgorithmAES, clientID.Recipient().String()+" spurious-token-xx")
	req, _ := ParseRequest([]byte("AUTH aes " + enc))

	if _, err := processAUTHRequest(req, serverID, nil); err != nil {
		t.Fatalf("expected success (no allowlist), got %v", err)
	}
}

func TestProcessAUTHRequestMultipleClientTokens(t *testing.T) {
	serverID, _ := age.GenerateX25519Identity()
	clientID, _ := age.GenerateX25519Identity()
	enc := buildAuthBlob(t, serverID, aead.AlgorithmAES, clientID.Recipient().String()+" rotation-old-xx rotation-new-yy")
	req, _ := ParseRequest([]byte("AUTH aes " + enc))

	// Server only trusts the "new" token; the client sends both; should match.
	if _, err := processAUTHRequest(req, serverID, []string{"rotation-new-yy"}); err != nil {
		t.Fatalf("expected success on any-match, got %v", err)
	}
}
