// Package aead owns tx's transport encryption: the Noise handshake that
// authenticates a connection, the record layer that encrypts its traffic
// with its own AEAD state, and the credentials and age keys both use.
package aead

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/sys/cpu"
)

const aeadTagSize = 16

// Algorithm names an AEAD cipher option.
type Algorithm string

const (
	AlgorithmAES      Algorithm = "aes"
	AlgorithmChaCha20 Algorithm = "chacha20"
)

// RecommendedCipher returns AES-GCM when this machine accelerates both AES
// and the GHASH carry-less multiply, and ChaCha20-Poly1305 otherwise.
func RecommendedCipher() Algorithm {
	switch runtime.GOARCH {
	case "386", "amd64":
		if cpu.X86.HasAES && cpu.X86.HasPCLMULQDQ {
			return AlgorithmAES
		}
	case "arm64":
		if cpu.ARM64.HasAES && cpu.ARM64.HasPMULL {
			return AlgorithmAES
		}
	case "s390x":
		if cpu.S390X.HasAES && cpu.S390X.HasGHASH {
			return AlgorithmAES
		}
	}
	return AlgorithmChaCha20
}

// ResolveCipher selects the cipher for a client option: an explicit cipher
// stands; "auto" picks AES only when this machine accelerates it and the
// server's (unauthenticated) hint also prefers AES.
func ResolveCipher(option string, serverHint Algorithm) (Algorithm, error) {
	switch option {
	case "auto", "":
		if RecommendedCipher() == AlgorithmAES && serverHint == AlgorithmAES {
			return AlgorithmAES, nil
		}
		return AlgorithmChaCha20, nil
	default:
		return validateAlgorithm(Algorithm(option))
	}
}

func validateAlgorithm(algorithm Algorithm) (Algorithm, error) {
	switch algorithm {
	case AlgorithmAES, AlgorithmChaCha20:
		return algorithm, nil
	default:
		return "", fmt.Errorf("unsupported AEAD algorithm: %q", algorithm)
	}
}

// newAEAD returns the record cipher for a 32-byte key.
func newAEAD(alg Algorithm, key []byte) (cipher.AEAD, error) {
	switch alg {
	case AlgorithmAES:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case AlgorithmChaCha20:
		return chacha20poly1305.New(key)
	default:
		return nil, fmt.Errorf("unsupported AEAD algorithm: %q", alg)
	}
}

const bech32Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

// bech32Data returns the lowercase human-readable part and data of a Bech32
// string that age's own parser has already accepted, checksum included. age
// exposes no raw key bytes, and the Noise handshake needs them; tx only
// decodes, never encodes, keys.
func bech32Data(s string) (string, []byte, error) {
	lower := strings.ToLower(s)
	sep := strings.LastIndexByte(lower, '1')
	if sep < 1 || sep+7 > len(lower) {
		return "", nil, errors.New("bech32: invalid separator position")
	}
	var acc uint32
	var bits uint
	symbols := lower[sep+1 : len(lower)-6]
	out := make([]byte, 0, len(symbols)*5/8)
	for i := range len(symbols) {
		v := strings.IndexByte(bech32Charset, symbols[i])
		if v < 0 {
			return "", nil, errors.New("bech32: invalid character")
		}
		acc = acc<<5 | uint32(v)
		bits += 5
		if bits >= 8 {
			bits -= 8
			out = append(out, byte(acc>>bits))
		}
	}
	if bits >= 5 || acc&(1<<bits-1) != 0 {
		return "", nil, errors.New("bech32: invalid padding")
	}
	return lower[:sep], out, nil
}
