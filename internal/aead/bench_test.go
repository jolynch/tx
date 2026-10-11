//go:build !race

package aead

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
)

// Performance properties of the record layer: the zero-allocation test and
// the benchmarks. The race detector makes sync.Pool drop buffers at random
// and distorts timings, so this file builds only without -race.
//
// The benchmarks compare the Noise record layer against the age
// reference. Sub-benchmark names match the previous per-command AEAD stream
// benchmarks, so benchstat can compare the two directly: each operation is
// one command- or response-sized message. Session setup is outside the
// timing; BenchmarkHandshake measures it separately.

var aeadBenchSizes = []struct {
	name string
	n    int
}{
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"4MiB", 4 << 20},
	{"10MiB", 10 << 20},
}

func algorithmBenchmarkName(alg Algorithm) string {
	if alg == AlgorithmAES {
		return "aesgcm"
	}
	return "chacha20poly1305"
}

// discardConn accepts writes without copying them.
type discardConn struct{ bufConn }

func (c *discardConn) Write(p []byte) (int, error) { return len(p), nil }

// writerSession returns a session whose writes go nowhere.
func writerSession(tb testing.TB, alg Algorithm) *Session {
	w, _, _ := sessionPair(tb, alg)
	w.conn = &discardConn{}
	return w
}

func writeOne(b *testing.B, s *Session, data []byte) {
	mw := s.Writer(false)
	if _, err := mw.Write(data); err != nil {
		b.Fatal(err)
	}
	if err := mw.End(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEncryptThroughput(b *testing.B) {
	id, _ := age.GenerateX25519Identity()
	for _, sz := range aeadBenchSizes {
		data := make([]byte, sz.n)
		rand.Read(data)
		for _, alg := range testAlgorithms {
			b.Run(sz.name+"/"+algorithmBenchmarkName(alg), func(b *testing.B) {
				s := writerSession(b, alg)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					writeOne(b, s, data)
				}
				b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
			})
		}
		b.Run(sz.name+"/age", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				w, err := age.Encrypt(io.Discard, id.Recipient())
				if err != nil {
					b.Fatal(err)
				}
				_, _ = w.Write(data)
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
	}
}

func BenchmarkDecryptThroughput(b *testing.B) {
	id, _ := age.GenerateX25519Identity()
	for _, sz := range aeadBenchSizes {
		data := make([]byte, sz.n)
		rand.Read(data)
		for _, alg := range testAlgorithms {
			b.Run(sz.name+"/"+algorithmBenchmarkName(alg), func(b *testing.B) {
				w, r, _ := sessionPair(b, alg)
				buf := make([]byte, 64<<10)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					writeOne(b, w, data)
					b.StartTimer()
					mr, err := r.Reader()
					if err != nil {
						b.Fatal(err)
					}
					// Read into buf directly: io.CopyBuffer to io.Discard
					// would ignore buf for io.Discard's own small buffer.
					var n int
					for {
						m, err := mr.Read(buf)
						n += m
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					if n != len(data) {
						b.Fatalf("read %d bytes, want %d", n, len(data))
					}
				}
				b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
			})
		}
		b.Run(sz.name+"/age", func(b *testing.B) {
			var ct bytes.Buffer
			w, _ := age.Encrypt(&ct, id.Recipient())
			_, _ = w.Write(data)
			_ = w.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := age.Decrypt(bytes.NewReader(ct.Bytes()), id)
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
	data := make([]byte, 64<<20)
	rand.Read(data)
	for _, alg := range testAlgorithms {
		b.Run(algorithmBenchmarkName(alg), func(b *testing.B) {
			s := writerSession(b, alg)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				writeOne(b, s, data)
			}
		})
	}
}

func BenchmarkDecryptLarge(b *testing.B) {
	data := make([]byte, 64<<20)
	rand.Read(data)
	discard := make([]byte, 64<<10)
	for _, alg := range testAlgorithms {
		b.Run(algorithmBenchmarkName(alg), func(b *testing.B) {
			w, r, _ := sessionPair(b, alg)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				writeOne(b, w, data)
				b.StartTimer()
				mr, err := r.Reader()
				if err != nil {
					b.Fatal(err)
				}
				for {
					_, err := mr.Read(discard)
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

// BenchmarkPartialFlush measures deliberate small-record progress flushes
// against the same bytes batched, and reports records and writes per op.
func BenchmarkPartialFlush(b *testing.B) {
	data := make([]byte, 1<<20)
	rand.Read(data)
	for _, unit := range []int{4 << 10, 16 << 10} {
		for _, flush := range []bool{false, true} {
			name := map[bool]string{false: "batched", true: "flush"}[flush]
			b.Run(strings.Join([]string{byteName(unit), name}, "/"), func(b *testing.B) {
				w, _, _ := sessionPair(b, AlgorithmAES)
				counter := &countingConn{}
				w.conn = counter
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					mw := w.Writer(false)
					for off := 0; off < len(data); off += unit {
						_, _ = mw.Write(data[off : off+unit])
						if flush {
							_ = mw.Flush()
						}
					}
					_ = mw.End()
				}
				b.ReportMetric(float64(w.send.n)/float64(b.N), "records/op")
				b.ReportMetric(float64(counter.writes)/float64(b.N), "writes/op")
				b.ReportMetric(float64(b.N)*float64(len(data))/b.Elapsed().Seconds()/1048576, "MiB/s")
			})
		}
	}
}

type countingConn struct {
	bufConn
	writes int
}

func (c *countingConn) Write(p []byte) (int, error) { c.writes++; return len(p), nil }

func byteName(n int) string {
	return map[int]string{4 << 10: "4KiB", 16 << 10: "16KiB"}[n]
}

// BenchmarkHandshake measures one authentication, preface through FINISH,
// over an in-memory pipe.
func BenchmarkHandshake(b *testing.B) {
	serverKP, _ := newKeyPair(b)
	clientKP, clientID := newKeyPair(b)
	token := strings.Repeat("k", 64)
	allow, _ := NewAllowlist([]string{token}, []string{clientID.Recipient().String()})
	for _, mode := range []string{ModeToken, ModeAge} {
		for _, alg := range testAlgorithms {
			b.Run(mode+"/"+string(alg), func(b *testing.B) {
				cfg := ClientConfig{Mode: mode, Cipher: alg, Static: clientKP, ServerKey: serverKP.Public}
				if mode == ModeToken {
					cfg.Token = token
				}
				b.ReportAllocs()
				for range b.N {
					c, s := net.Pipe()
					done := make(chan error, 1)
					go func() {
						br := bufio.NewReader(s)
						line, err := br.ReadSlice('\n')
						if err == nil {
							_, _, err = ServerHandshake(s, br, append([]byte(nil), line...), ServerConfig{Static: serverKP, Allowlist: allow})
						}
						done <- err
					}()
					_ = c.SetDeadline(time.Now().Add(HandshakeTimeout))
					if _, err := ClientHandshake(c, cfg); err != nil {
						b.Fatal(err)
					}
					if err := <-done; err != nil {
						b.Fatal(err)
					}
					c.Close()
					s.Close()
				}
			})
		}
	}
}

func TestRecordPathDoesNotAllocate(t *testing.T) {
	// Whole-record writes seal straight from the caller's slice and reads
	// of at least a record open straight into the caller's buffer; small
	// writes and reads go through the session's buffers. Neither allocates.
	for _, tc := range []struct {
		name         string
		chunk, rsize int
	}{
		{"direct", 16 * maxEmitPayload, 64 << 10},
		{"buffered", 1000, 4 << 10},
	} {
		w, r, wire := sessionPair(t, AlgorithmAES)
		data := make([]byte, 16*maxEmitPayload)
		buf := make([]byte, tc.rsize)
		br := bufio.NewReader(nil)
		allocs := testing.AllocsPerRun(20, func() {
			mw := w.Writer(false)
			for p := data; len(p) > 0; p = p[min(tc.chunk, len(p)):] {
				_, _ = mw.Write(p[:min(tc.chunk, len(p))])
			}
			_ = mw.EndFunc()()
			mr, _ := r.Reader()
			br.Reset(mr)
			for {
				if _, err := br.Read(buf); err != nil {
					break
				}
			}
			_ = FinishMessage(br)
			wire.buf.Reset()
		})
		if allocs != 0 {
			t.Fatalf("%s: one 17-record message, end to end, allocated %.1f times", tc.name, allocs)
		}
	}
}
