package filexfercli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	tx "github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/cliflags"
)

// envAuthToken supplies one client token when no explicit credential is set.
const envAuthToken = "TX_AUTH_TOKEN"

// clientAuthFlags are the authentication flags every remote command takes.
type clientAuthFlags struct {
	encrypt    string
	keysDir    string
	tokens     []string
	tokenFiles []string
	// serverRecipient and serverRecipientFile name the age recipient the
	// server must prove; at most one is set.
	serverRecipient     string
	serverRecipientFile string
}

func (f *clientAuthFlags) register(cf *cliflags.Flags) {
	cf.StringVar(&f.encrypt, "", "encrypt", "", "Encryption: none|auto|aes|chacha20 (default: none, or auto with a credential)")
	cf.StringVar(&f.keysDir, "k", "keys", "", "Age keys directory holding this client's identity; selects age authentication without a token")
	cf.StringSliceVar(&f.tokens, "t", "auth-token", "Auth token candidate, tried in order; repeatable. Prefer --auth-token-file: values here are visible in the process list")
	cf.StringSliceVar(&f.tokenFiles, "", "auth-token-file", "File holding one auth token candidate, tried in order; repeatable")
	cf.StringVar(&f.serverRecipient, "", "require-server-recipient", "", "Server age recipient (public key) the server must prove it holds; refuses any other server")
	cf.StringVar(&f.serverRecipientFile, "", "require-server-recipient-file", "", "File holding the server age recipient the server must prove it holds")
}

// clientAuth is a remote command's resolved authentication.
type clientAuth struct {
	agePublicKey    string
	ageIdentity     string
	encMode         string
	tokens          []string
	serverRecipient string
}

// options returns the client options that carry the credentials.
func (a clientAuth) options() []tx.ClientOption {
	return []tx.ClientOption{
		tx.WithClientAgePublicKey(a.agePublicKey),
		tx.WithClientAgeIdentity(a.ageIdentity),
		tx.WithEncryptMode(a.encMode),
		tx.WithClientAuthTokens(a.tokens...),
		tx.WithRequiredServerRecipient(a.serverRecipient),
	}
}

// resolve applies the precedence rules: explicit --auth-token or
// --auth-token-file (never both), then --keys, then TX_AUTH_TOKEN, which
// --keys and --encrypt none suppress. A credential defaults encryption to
// auto; --encrypt none with an explicit credential is an error. Errors
// never echo a credential.
func (f clientAuthFlags) resolve(lookupEnv func(string) (string, bool)) (clientAuth, error) {
	encrypt := strings.ToLower(strings.TrimSpace(f.encrypt))
	switch encrypt {
	case "", "none", "auto", "aes", "chacha20":
	default:
		return clientAuth{}, fmt.Errorf("unsupported --encrypt value %q (supported: none, auto, aes, chacha20)", encrypt)
	}
	if len(f.tokens) > 0 && len(f.tokenFiles) > 0 {
		return clientAuth{}, errors.New("use --auth-token or --auth-token-file, not both")
	}
	var auth clientAuth
	auth.tokens = append([]string(nil), f.tokens...)
	for _, path := range f.tokenFiles {
		tok, err := cliflags.ReadCredentialFile(path)
		if err != nil {
			return clientAuth{}, err
		}
		auth.tokens = append(auth.tokens, tok)
	}
	explicit := len(auth.tokens) > 0 || f.keysDir != ""
	if explicit && encrypt == "none" {
		return clientAuth{}, errors.New("--encrypt none cannot be used with a credential")
	}
	if !explicit && encrypt != "none" {
		if tok, ok := lookupEnv(envAuthToken); ok {
			if tok == "" {
				return clientAuth{}, fmt.Errorf("%s is set but empty", envAuthToken)
			}
			auth.tokens = []string{tok}
		}
	}
	for i, tok := range auth.tokens {
		if err := aead.ValidateAuthToken(tok); err != nil {
			return clientAuth{}, fmt.Errorf("auth token %d: %w", i+1, err)
		}
	}
	if f.keysDir != "" {
		id, _, err := aead.LoadOrGenerateAgeIdentity(f.keysDir, false)
		if err != nil {
			return clientAuth{}, fmt.Errorf("load keys from %s: %w", f.keysDir, err)
		}
		auth.agePublicKey, auth.ageIdentity = id.Recipient().String(), id.String()
	}
	if f.serverRecipient != "" && f.serverRecipientFile != "" {
		return clientAuth{}, errors.New("use --require-server-recipient or --require-server-recipient-file, not both")
	}
	recipient := f.serverRecipient
	if f.serverRecipientFile != "" {
		r, err := cliflags.ReadCredentialFile(f.serverRecipientFile)
		if err != nil {
			return clientAuth{}, fmt.Errorf("--require-server-recipient-file: %w", err)
		}
		recipient = r
	}
	// A credential, or a server recipient to require, implies encryption.
	credential := len(auth.tokens) > 0 || auth.ageIdentity != ""
	switch {
	case (credential || recipient != "") && encrypt == "":
		auth.encMode = "auto"
	case encrypt == "none":
		auth.encMode = ""
	default:
		auth.encMode = encrypt
	}
	if recipient != "" {
		if auth.encMode == "" {
			return clientAuth{}, errors.New("--require-server-recipient cannot be used with --encrypt none")
		}
		if strings.HasPrefix(strings.ToLower(recipient), "age-secret-key-") {
			return clientAuth{}, errors.New("--require-server-recipient: value is an age private key; use the server's public recipient (age1...)")
		}
		if _, err := aead.ParseRecipientKey(recipient); err != nil {
			return clientAuth{}, fmt.Errorf("--require-server-recipient: %w", err)
		}
		auth.serverRecipient = recipient
	}
	return auth, nil
}

// resolveClientAuth resolves f against the process environment.
func resolveClientAuth(f clientAuthFlags) (clientAuth, error) {
	return f.resolve(os.LookupEnv)
}
