package filexfercli

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/sampler"
)

// verifyFuzzEnv is the FTCP servers every FuzzCopyVerifyDetectsCorruption
// iteration shares: one plain and one with a server identity for --encrypt,
// both chrooted at the temp directory. Each iteration builds its tree under
// its own t.TempDir, so no iteration opens a listener of its own or leaves
// files behind.
type verifyFuzzEnv struct {
	once      sync.Once
	plainAddr string
	encAddr   string
	closers   []func()
}

var verifyFuzz verifyFuzzEnv

func (e *verifyFuzzEnv) start(t testing.TB) {
	e.once.Do(func() {
		id, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatal(err)
		}
		for _, srv := range []struct {
			addr *string
			id   *age.X25519Identity
		}{{&e.plainAddr, nil}, {&e.encAddr, id}} {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			st := store.NewStore()
			go func() {
				_ = intftcp.Serve(ln, intftcp.ServerOptions{
					Deps:             intftcp.NewRuntimeDeps(st, intftcp.WithRoot(os.TempDir())),
					ServerIdentity:   srv.id,
					KeepAliveTimeout: 5 * time.Second,
				})
			}()
			*srv.addr = ln.Addr().String()
			e.closers = append(e.closers, func() { _ = ln.Close(); st.Close() })
		}
	})
	if e.plainAddr == "" {
		t.Fatal("verify fuzz servers failed to start")
	}
}

func (e *verifyFuzzEnv) close() {
	for _, c := range e.closers {
		c()
	}
	// -count=N runs the fuzz function again in the same process.
	*e = verifyFuzzEnv{}
}

var (
	verifySeedPattern    = regexp.MustCompile(`verify-data: .*seed=([0-9a-f]{16})`)
	verifySamplesPattern = regexp.MustCompile(`verify-data: \[ok\] .*samples=(\d+)`)
)

// FuzzCopyVerifyDetectsCorruption copies a tree, flips one byte in the copy
// while keeping its size, mode, and mtime, and runs data verification again.
// Metadata cannot see the change, so only data verification can. It drives
// the remote path (a real FTCP listener, plain or with --encrypt auto) and
// the daemonless path.
//
// With --verify full the run must fail and report 100.0% of files and bytes.
//
// With --verify N%data the clean run must pass and draw floor or ceil of N%
// of the tree's 4 MiB slots, counted here from the file sizes. After the flip,
// the samples depend on a per-run seed, which the run prints as seed=<16 hex>.
// The test reads that seed back and replays the sampling plan from the same
// manifest. The only difference between the trees is the flipped byte, so the
// run must fail exactly when the replayed plan samples the slot holding it:
// failing otherwise is a false alarm, and passing otherwise is a miss.
func FuzzCopyVerifyDetectsCorruption(f *testing.F) {
	f.Cleanup(verifyFuzz.close)
	//         seed, mode,         file, offset,   xor,  pct
	f.Add(uint64(1), uint8(0b000), uint8(0), uint64(4096), uint8(1), uint8(50))
	f.Add(uint64(2), uint8(0b001), uint8(1), uint64(5<<20), uint8(0x80), uint8(50))
	f.Add(uint64(3), uint8(0b001), uint8(2), uint64(0), uint8(0xff), uint8(50))
	f.Add(uint64(4), uint8(0b011), uint8(1), uint64(6<<20), uint8(0x10), uint8(50))
	f.Add(uint64(5), uint8(0b100), uint8(0), uint64(9<<20), uint8(0x01), uint8(40))
	f.Add(uint64(6), uint8(0b101), uint8(1), uint64(1<<20), uint8(0x02), uint8(60))
	f.Add(uint64(7), uint8(0b111), uint8(0), uint64(7<<20), uint8(0x04), uint8(100))
	f.Fuzz(func(t *testing.T, seed uint64, mode uint8, fileIdx uint8, offset uint64, xor uint8, rawPct uint8) {
		remote, encrypt, sampled := mode&1 != 0, mode&2 != 0, mode&4 != 0
		if !remote {
			encrypt = false // the daemonless path has no wire to encrypt
		}
		pct := int(rawPct%100) + 1

		work := t.TempDir()
		src, dst := filepath.Join(work, "src"), filepath.Join(work, "dst")
		source := src
		if remote {
			verifyFuzz.start(t)
			addr := verifyFuzz.plainAddr
			if encrypt {
				addr = verifyFuzz.encAddr
			}
			// The servers are chrooted at the temp directory, which holds work.
			rel, err := filepath.Rel(os.TempDir(), src)
			if err != nil {
				t.Fatal(err)
			}
			source = "tx://" + addr + "/" + filepath.ToSlash(rel)
		}

		rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		var chachaSeed [32]byte
		binary.LittleEndian.PutUint64(chachaSeed[:], seed)
		fill := rand.NewChaCha8(chachaSeed)
		// An empty file in a subdirectory counts toward neither total.
		writeLocalTestFile(t, filepath.Join(src, "d", "empty.bin"), nil)
		var names []string
		var total, slots int64
		for i := range 1 + rng.IntN(4) {
			// Sizes cover sub-slot files, multi-slot files, and a short
			// last slot.
			data := make([]byte, 1+rng.IntN(9<<20))
			_, _ = fill.Read(data)
			name := fmt.Sprintf("f%d.bin", i)
			writeLocalTestFile(t, filepath.Join(src, name), data)
			names = append(names, name)
			total += int64(len(data))
			slots += sampler.SlotCount(int64(len(data)), defaultVerifyFrameSize)
		}

		verify := []string{"--verify", "full"}
		if sampled {
			verify = []string{"--verify", fmt.Sprintf("%d%%data", pct)}
		}
		// Gentle mode probes 3 times in sequence instead of once per server
		// CPU, and pooled connections are reused, which keeps the sockets
		// each iteration leaves in TIME_WAIT few.
		args := func(verify ...string) []string {
			a := []string{"copy", "-y", "--progress=false", "--concurrency", "2"}
			if remote {
				a = append(a, "--skip-fsync", "--mode", "gentle", "--probe-size", "1KiB")
			}
			if encrypt {
				a = append(a, "--encrypt", "auto")
			}
			return append(append(a, verify...), source, dst)
		}
		var stdout, stderr bytes.Buffer
		if code := RunCLI(args(verify...), &stdout, &stderr); code != 0 || !strings.Contains(stderr.String(), "verify-data: [ok]") {
			t.Fatalf("copy exit %d: %s", code, stderr.String())
		}
		if !sampled {
			// full must cover every file and byte, not merely catch the flip.
			coverage := fmt.Sprintf("files=%d/%d (100.0%%) bytes=%s/%s (100.0%%)", len(names), len(names), encoding.HumanBytes(total), encoding.HumanBytes(total))
			if !strings.Contains(stderr.String(), coverage) || !verifySeedPattern.MatchString(stderr.String()) {
				t.Fatalf("verify full did not report %q and a seed: %s", coverage, stderr.String())
			}
		} else {
			// The clean run must verify its share of the tree, counted from
			// the file sizes rather than the sampler.
			lo, hi := max(slots*int64(pct)/100, 1), max(slots*int64(pct)/100+min(slots*int64(pct)%100, 1), 1)
			var drawn int64 = -1
			if m := verifySamplesPattern.FindStringSubmatch(stderr.String()); m != nil {
				drawn, _ = strconv.ParseInt(m[1], 10, 64)
			}
			if drawn < lo || drawn > hi {
				t.Fatalf("verify %d%%data of %d slots sampled %d, want %d to %d: %s", pct, slots, drawn, lo, hi, stderr.String())
			}
			if !strings.Contains(stderr.String(), fmt.Sprintf("/%d (", len(names))) || !strings.Contains(stderr.String(), "/"+encoding.HumanBytes(total)+" (") {
				t.Fatalf("verify %d%%data did not report totals of %d files and %s: %s", pct, len(names), encoding.HumanBytes(total), stderr.String())
			}
		}

		victimName := names[int(fileIdx)%len(names)]
		victim := filepath.Join(dst, victimName)
		info, err := os.Stat(victim)
		if err != nil {
			t.Fatal(err)
		}
		at := int64(offset % uint64(info.Size()))
		fd, err := os.OpenFile(victim, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		if _, err := fd.ReadAt(b[:], at); err != nil {
			t.Fatal(err)
		}
		b[0] ^= xor | 1
		if _, err := fd.WriteAt(b[:], at); err != nil {
			t.Fatal(err)
		}
		if err := fd.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(victim, info.ModTime(), info.ModTime()); err != nil {
			t.Fatal(err)
		}

		stdout.Reset()
		stderr.Reset()
		code := RunCLI(args(verify...), &stdout, &stderr)
		failed := code != 0 && strings.Contains(stderr.String(), "verify-data: [fail]")
		if !sampled {
			if !failed {
				t.Fatalf("verify full missed a flipped byte in %s at offset %d of %d (exit %d): %s",
					victimName, at, info.Size(), code, stderr.String())
			}
			return
		}
		if code != 0 && !failed {
			t.Fatalf("verify %d%%data exit %d without a data failure: %s", pct, code, stderr.String())
		}
		m := verifySeedPattern.FindStringSubmatch(stderr.String())
		if m == nil {
			t.Fatalf("verify %d%%data printed no seed: %s", pct, stderr.String())
		}
		runSeed, err := strconv.ParseUint(m[1], 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		root, entries := src, []tx.ManifestEntry(nil)
		if remote {
			root, entries = verifyFuzzManifest(t, source, encrypt)
		} else {
			manifest, err := scanLocalDir(src, &tx.Manifest{})
			if err != nil {
				t.Fatal(err)
			}
			entries = manifest.Entries
		}
		files, _ := newVerifyFiles(root, entries, verifyOptions{pct: pct, frameSize: defaultVerifyFrameSize, seed: runSeed}, func(tx.ManifestEntry) (string, string) { return "", "" })
		covered := false
		for _, vf := range files {
			if vf.entry.Path != victimName {
				continue
			}
			for gen := vf.gen; gen.Remaining() > 0; gen.Advance() {
				s, _ := gen.Peek()
				covered = covered || (at >= s.Offset && at < s.Offset+s.Size)
			}
		}
		if covered != failed {
			t.Fatalf("verify %d%%data seed=%016x: byte %d of %s (size %d) sampled=%v but run failed=%v (exit %d): %s",
				pct, runSeed, at, victimName, info.Size(), covered, failed, code, stderr.String())
		}
	})
}

// verifyFuzzManifest fetches the manifest a remote copy of source plans from.
func verifyFuzzManifest(t *testing.T, source string, encrypt bool) (string, []tx.ManifestEntry) {
	t.Helper()
	rest := strings.TrimPrefix(source, "tx://")
	addr, path, _ := strings.Cut(rest, "/")
	opts := []tx.ClientOption{}
	if encrypt {
		pub, id, mode, err := resolveEncryptionOptionsWithKeys("auto", "")
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, tx.WithClientAgePublicKey(pub), tx.WithClientAgeIdentity(id), tx.WithEncryptMode(mode))
	}
	client := tx.NewClient(addr, opts...)
	defer client.Close()
	resp, err := client.GetManifest(context.Background(), tx.GetManifestRequest{Directory: "/" + path, Mode: "gentle", Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Manifest.Root, resp.Manifest.Entries
}
