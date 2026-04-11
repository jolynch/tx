package main

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"filippo.io/age"

	"github.com/jolynch/tx/internal/aead"
)

const aeadDefaultChunk = 64 * 1024

var testAlgorithms = []aead.Algorithm{aead.AlgorithmAES, aead.AlgorithmChaCha20}

var aeadBenchSizes = []struct {
	name string
	n    int
}{
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"4MiB", 4 << 20},
	{"10MiB", 10 << 20},
}

func generateTestIdentity(tb testing.TB) (*age.X25519Identity, *age.X25519Recipient) {
	tb.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		tb.Fatalf("generate identity: %v", err)
	}
	return id, id.Recipient()
}

func algorithmBenchmarkName(algorithm aead.Algorithm) string {
	switch algorithm {
	case aead.AlgorithmAES:
		return "aesgcm"
	case aead.AlgorithmChaCha20:
		return "chacha20poly1305"
	default:
		return "unknown"
	}
}

func encryptForBench(b *testing.B, algorithm aead.Algorithm, plaintext []byte, recipient *age.X25519Recipient) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := aead.Encrypt(&buf, recipient, aead.Options{Algorithm: algorithm})
	if err != nil {
		b.Fatalf("%s encrypt setup: %v", algorithm, err)
	}
	if _, err := w.Write(plaintext); err != nil {
		b.Fatalf("%s encrypt write: %v", algorithm, err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("%s encrypt close: %v", algorithm, err)
	}
	return buf.Bytes()
}

func encryptAgeForBench(b *testing.B, plaintext []byte, recipient *age.X25519Recipient) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		b.Fatalf("age encrypt setup: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		b.Fatalf("age encrypt write: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("age encrypt close: %v", err)
	}
	return buf.Bytes()
}

func BenchmarkEncryptThroughput(b *testing.B) {
	_, recipient := generateTestIdentity(b)
	for _, sz := range aeadBenchSizes {
		data := make([]byte, sz.n)
		rand.Read(data)
		for _, algorithm := range testAlgorithms {
			algorithm := algorithm
			b.Run(sz.name+"/"+algorithmBenchmarkName(algorithm), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					w, err := aead.Encrypt(io.Discard, recipient, aead.Options{Algorithm: algorithm})
					if err != nil {
						b.Fatal(err)
					}
					if _, err := w.Write(data); err != nil {
						b.Fatal(err)
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
			})
		}
		b.Run(sz.name+"/age", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := age.Encrypt(io.Discard, recipient)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
	}
}

func BenchmarkDecryptThroughput(b *testing.B) {
	id, recipient := generateTestIdentity(b)
	for _, sz := range aeadBenchSizes {
		data := make([]byte, sz.n)
		rand.Read(data)

		ciphertexts := map[aead.Algorithm][]byte{
			aead.AlgorithmAES:      encryptForBench(b, aead.AlgorithmAES, data, recipient),
			aead.AlgorithmChaCha20: encryptForBench(b, aead.AlgorithmChaCha20, data, recipient),
		}
		ageCT := encryptAgeForBench(b, data, recipient)

		for _, algorithm := range testAlgorithms {
			b.Run(sz.name+"/"+algorithmBenchmarkName(algorithm), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r, err := aead.Decrypt(bytes.NewReader(ciphertexts[algorithm]), id)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := io.Copy(io.Discard, r); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
			})
		}
		b.Run(sz.name+"/age", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := age.Decrypt(bytes.NewReader(ageCT), id)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, r); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
	}
}

func BenchmarkEncryptLarge(b *testing.B) {
	_, recipient := generateTestIdentity(b)
	data := make([]byte, 64*1024*1024)
	rand.Read(data)

	for _, algorithm := range testAlgorithms {
		algorithm := algorithm
		b.Run(algorithmBenchmarkName(algorithm), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				w, err := aead.Encrypt(io.Discard, recipient, aead.Options{Algorithm: algorithm, ChunkSize: aeadDefaultChunk})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecryptLarge(b *testing.B) {
	id, recipient := generateTestIdentity(b)
	data := make([]byte, 64*1024*1024)
	rand.Read(data)
	discard := make([]byte, 64*1024)

	ciphertexts := map[aead.Algorithm][]byte{
		aead.AlgorithmAES:      encryptForBench(b, aead.AlgorithmAES, data, recipient),
		aead.AlgorithmChaCha20: encryptForBench(b, aead.AlgorithmChaCha20, data, recipient),
	}

	for _, algorithm := range testAlgorithms {
		algorithm := algorithm
		b.Run(algorithmBenchmarkName(algorithm), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r, err := aead.Decrypt(bytes.NewReader(ciphertexts[algorithm]), id)
				if err != nil {
					b.Fatal(err)
				}
				for {
					_, err := r.Read(discard)
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
