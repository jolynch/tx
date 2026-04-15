package aead

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"filippo.io/age"
)

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

// LoadOrGenerateAgeIdentity returns a persistent identity from dir, generating
// and persisting one if the directory exists but has no key file. If the
// directory is missing and isDefault is true, returns an ephemeral identity.
// If the directory is missing and isDefault is false, returns an error.
func LoadOrGenerateAgeIdentity(dir string, isDefault bool) (*age.X25519Identity, bool, error) {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if !isDefault {
				return nil, false, fmt.Errorf("keys directory %s does not exist", dir)
			}
			identity, genErr := age.GenerateX25519Identity()
			if genErr != nil {
				return nil, false, fmt.Errorf("generate ephemeral identity: %w", genErr)
			}
			return identity, true, nil
		}
		return nil, false, fmt.Errorf("stat keys directory %s: %w", dir, err)
	}

	identity, err := LoadAgeIdentity(dir)
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
	keyPath := path.Join(dir, "key")
	out, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("open key file %s: %w", keyPath, err)
	}
	defer out.Close()
	fmt.Fprintf(out, "# created: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(out, "# public key: %s\n", identity.Recipient())
	fmt.Fprintf(out, "%s\n", identity)
	return identity, false, nil
}
