package aead

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const minAuthTokenLen = 9 // > 8 bytes

// NewAuthToken returns 16 hex characters (8 random bytes) suitable for use
// as a server-generated auth token.
func NewAuthToken() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate auth token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// ValidateAuthToken returns an error if s is not a legal auth token: it must
// be longer than 8 bytes and contain no ASCII spaces or newlines (those
// characters would break the space-delimited AUTH blob wire format).
func ValidateAuthToken(s string) error {
	if len(s) < minAuthTokenLen {
		return fmt.Errorf("auth token must be longer than 8 bytes, got %d", len(s))
	}
	if strings.ContainsAny(s, " \t\r\n") {
		return errors.New("auth token must not contain spaces or newlines")
	}
	return nil
}

// MatchAuthToken returns true if presented contains any entry that
// constant-time matches one of the allowed tokens. Returns the matched
// token (from the allowed list) for logging.
func MatchAuthToken(allowed, presented []string) (string, bool) {
	if len(allowed) == 0 {
		return "", true
	}
	for _, a := range allowed {
		ab := []byte(a)
		for _, p := range presented {
			pb := []byte(p)
			if len(ab) != len(pb) {
				continue
			}
			if subtle.ConstantTimeCompare(ab, pb) == 1 {
				return a, true
			}
		}
	}
	return "", false
}

// RedactAuthToken returns a log-safe form of a token: first 4 chars, "…",
// last 4 chars. Short tokens are fully masked.
func RedactAuthToken(s string) string {
	if len(s) <= 8 {
		return strings.Repeat("*", len(s))
	}
	return s[:4] + "…" + s[len(s)-4:]
}
