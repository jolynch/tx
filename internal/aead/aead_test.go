package aead

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"strconv"
	"testing"

	"filippo.io/age"
)

var testAlgorithms = []Algorithm{AlgorithmAES, AlgorithmChaCha20}

func generateTestIdentity(t testing.TB) (*age.X25519Identity, *age.X25519Recipient) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id, id.Recipient()
}

func roundTrip(t testing.TB, algorithm Algorithm, plaintext []byte, chunkSize int) {
	t.Helper()
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: algorithm, ChunkSize: chunkSize})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := Decrypt(&ciphertext, id)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("roundtrip mismatch: wrote %d bytes, got %d bytes", len(plaintext), len(got))
	}
}

func FuzzRoundTrip(f *testing.F) {
	sizes := []int{0, 1, 16, 1023, 1024, 4096, 65535, 65536, 65537, 128*1024 - 1, 128 * 1024, 128*1024 + 1}
	chunkSizes := []int{0, 512, 1024, 4096, 64 * 1024}
	for _, algorithm := range testAlgorithms {
		for _, chunkSize := range chunkSizes {
			for _, size := range sizes {
				data := make([]byte, size)
				if size > 0 {
					rand.Read(data)
				}
				f.Add(string(algorithm), chunkSize, data)
			}
		}
	}

	f.Fuzz(func(t *testing.T, rawAlg string, chunkSize int, data []byte) {
		algorithm := Algorithm(rawAlg)
		switch algorithm {
		case AlgorithmAES, AlgorithmChaCha20:
		default:
			algorithm = AlgorithmAES
		}
		roundTrip(t, algorithm, data, chunkSize)
	})
}

func TestRoundTripLarge(t *testing.T) {
	for _, algorithm := range testAlgorithms {
		for _, size := range []int{4 * 1024 * 1024, 4*1024*1024 + 1} {
			data := make([]byte, size)
			rand.Read(data)
			t.Run(algorithmName(algorithm)+"/"+strconv.Itoa(size), func(t *testing.T) {
				roundTrip(t, algorithm, data, 0)
			})
		}
	}
}

func TestSmallChunkSize(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			roundTrip(t, algorithm, data, 512)
		})
	}
}

func TestTamperedCiphertext(t *testing.T) {
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			id, recipient := generateTestIdentity(t)

			plaintext := []byte("hello world, this is a test of tamper detection")
			var ciphertext bytes.Buffer
			w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: algorithm})
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			if _, err := w.Write(plaintext); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			raw := ciphertext.Bytes()
			if len(raw) < 100 {
				t.Fatal("ciphertext too short")
			}
			raw[len(raw)-10] ^= 0x01

			r, err := Decrypt(bytes.NewReader(raw), id)
			if err != nil {
				t.Fatalf("decrypt init: %v", err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected authentication error for tampered ciphertext")
			}
		})
	}
}

func TestWrongKey(t *testing.T) {
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			_, recipient := generateTestIdentity(t)
			wrongID, _ := generateTestIdentity(t)

			plaintext := []byte("secret message")
			var ciphertext bytes.Buffer
			w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: algorithm})
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			if _, err := w.Write(plaintext); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			_, err = Decrypt(&ciphertext, wrongID)
			if err == nil {
				t.Fatal("expected error decrypting with wrong identity")
			}
		})
	}
}

func TestTruncatedCiphertext(t *testing.T) {
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			id, recipient := generateTestIdentity(t)

			plaintext := make([]byte, 200*1024)
			rand.Read(plaintext)

			var ciphertext bytes.Buffer
			w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: algorithm})
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			if _, err := w.Write(plaintext); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			truncated := ciphertext.Bytes()[:ciphertext.Len()/2]
			r, err := Decrypt(bytes.NewReader(truncated), id)
			if err != nil {
				t.Fatalf("decrypt init: %v", err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected error reading truncated ciphertext")
			}
		})
	}
}

func TestDefaultChunkSize(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			roundTrip(t, algorithm, data, 0)
		})
	}
}

func TestChunkSizeFromHeader(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			roundTrip(t, algorithm, data, 4*1024)
		})
	}
}

func TestAcquireAEADBufferLayout(t *testing.T) {
	buf, chunkSize, release := acquireAEADBuffer(64 * 1024)
	defer release()

	if chunkSize != 64*1024 {
		t.Fatalf("unexpected chunk size: got %d want %d", chunkSize, 64*1024)
	}
	if len(buf) != 2*chunkSize+aeadTagSize {
		t.Fatalf("unexpected buffer len: got %d want %d", len(buf), 2*chunkSize+aeadTagSize)
	}

	cipherBuf := buf[:chunkSize+aeadTagSize]
	plainBuf := buf[chunkSize+aeadTagSize:]
	if len(cipherBuf) != chunkSize+aeadTagSize {
		t.Fatalf("unexpected cipher buffer len: got %d want %d", len(cipherBuf), chunkSize+aeadTagSize)
	}
	if len(plainBuf) != chunkSize {
		t.Fatalf("unexpected plain buffer len: got %d want %d", len(plainBuf), chunkSize)
	}
}

func TestHeaderChunkSizeTamper(t *testing.T) {
	for _, algorithm := range testAlgorithms {
		t.Run(algorithmName(algorithm), func(t *testing.T) {
			id, recipient := generateTestIdentity(t)

			var ciphertext bytes.Buffer
			w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: algorithm, ChunkSize: 4 * 1024})
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			if _, err := w.Write([]byte("tamper me")); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			raw := ciphertext.Bytes()
			offset := stanzaChunkSizeOffset(t, raw)
			binary.BigEndian.PutUint32(raw[offset:offset+4], 8*1024)

			r, err := Decrypt(bytes.NewReader(raw), id)
			if err != nil {
				t.Fatalf("decrypt init: %v", err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected authentication error for tampered chunk size")
			}
		})
	}
}

func TestHeaderAlgorithmTamper(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: AlgorithmAES})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write([]byte("tamper me")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw := append([]byte(nil), ciphertext.Bytes()...)
	if len(raw) < 2 {
		t.Fatal("ciphertext too short")
	}
	raw[1] = 0xff

	if _, err := Decrypt(bytes.NewReader(raw), id); err == nil {
		t.Fatal("expected header parse error for unknown algorithm")
	}
}

func TestReadStanzaHeaderRejectsUnsetAlgorithm(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: AlgorithmAES})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write([]byte("tamper me")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw := append([]byte(nil), ciphertext.Bytes()...)
	raw[1] = 0x00

	if _, err := Decrypt(bytes.NewReader(raw), id); err == nil {
		t.Fatal("expected header parse error for unset algorithm")
	}
}

func TestDecryptWithOptionsRejectsUnexpectedAlgorithm(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := Encrypt(&ciphertext, recipient, Options{Algorithm: AlgorithmAES})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write([]byte("tamper me")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := DecryptWithOptions(bytes.NewReader(ciphertext.Bytes()), id, Options{Algorithm: AlgorithmChaCha20}); err == nil {
		t.Fatal("expected algorithm mismatch error")
	}
}

func TestEncryptDefaultAlgorithmUsesRecommendation(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := Encrypt(&ciphertext, recipient, Options{})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write([]byte("recommended cipher")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw := ciphertext.Bytes()
	if len(raw) < 2 {
		t.Fatal("ciphertext too short")
	}
	got, err := parseAlgorithmID(raw[1])
	if err != nil {
		t.Fatalf("parse algorithm id: %v", err)
	}
	if got != RecommendedCipher() {
		t.Fatalf("expected algorithm %v, got %v", RecommendedCipher(), got)
	}

	r, err := Decrypt(bytes.NewReader(raw), id)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func stanzaChunkSizeOffset(t testing.TB, ciphertext []byte) int {
	t.Helper()
	offset := 0
	if len(ciphertext) < 2 {
		t.Fatal("ciphertext too short")
	}
	offset += 2

	readU16 := func() int {
		if offset+2 > len(ciphertext) {
			t.Fatal("ciphertext missing u16 field")
		}
		n := int(binary.BigEndian.Uint16(ciphertext[offset : offset+2]))
		offset += 2
		return n
	}

	typeLen := readU16()
	offset += typeLen
	if offset > len(ciphertext) {
		t.Fatal("ciphertext missing stanza type")
	}

	numArgs := readU16()
	for range numArgs {
		argLen := readU16()
		offset += argLen
		if offset > len(ciphertext) {
			t.Fatal("ciphertext missing stanza arg")
		}
	}

	bodyLen := readU16()
	offset += bodyLen
	if offset+4 > len(ciphertext) {
		t.Fatal("ciphertext missing chunk size")
	}
	return offset
}

func algorithmName(algorithm Algorithm) string {
	switch algorithm {
	case AlgorithmAES:
		return string(algorithm)
	case AlgorithmChaCha20:
		return string(algorithm)
	default:
		return "unknown"
	}
}

func algorithmBenchmarkName(algorithm Algorithm) string {
	switch algorithm {
	case AlgorithmAES:
		return "aesgcm"
	case AlgorithmChaCha20:
		return "chacha20poly1305"
	default:
		return "unknown"
	}
}
