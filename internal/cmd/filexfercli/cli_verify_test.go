package filexfercli

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/sampler"
	"github.com/zeebo/xxh3"
)

func TestFormatVerifyDataSummaryLine(t *testing.T) {
	const gib = int64(1) << 30
	for _, tc := range []struct {
		name   string
		prefix string
		result verifyDataResult
		pct    int
		budget time.Duration
		want   string
	}{
		{
			name: "budgeted", prefix: "copy-verify-data", pct: 100, budget: 10 * time.Second,
			result: verifyDataResult{files: 10011, totalFiles: 10011, bytes: 24 * gib, totalBytes: 24 * gib, samples: 14224, seed: 0xdeadbeef, elapsed: 9876 * time.Millisecond},
			want:   "copy-verify-data: [ok] files=10011/10011 (100.0%) bytes=24.00 GiB/24.00 GiB (100.0%) samples=14224 budget=10s seed=00000000deadbeef elapsed=9.876s",
		},
		{
			name: "sampled-percent", prefix: "copy-verify-data", pct: 5,
			result: verifyDataResult{files: 12, totalFiles: 240, bytes: 24 * gib / 20, totalBytes: 24 * gib, samples: 300, elapsed: 1500 * time.Microsecond},
			want:   "copy-verify-data: [ok] files=12/240 (5.0%) bytes=1.20 GiB/24.00 GiB (5.0%) samples=300 pct=5 seed=0000000000000000 elapsed=2ms",
		},
		{
			name: "partial-budgeted", prefix: "local-verify-data", pct: 100, budget: 10 * time.Second,
			result: verifyDataResult{files: 90, totalFiles: 240, bytes: 9 * gib, totalBytes: 24 * gib, samples: 2330, elapsed: 1500 * time.Millisecond, partial: true},
			want:   "local-verify-data: [partial-ok] files=90/240 (37.5%) bytes=9.00 GiB/24.00 GiB (37.5%) samples=2330 budget=10s seed=0000000000000000 elapsed=1.5s",
		},
		{
			name: "nothing-to-verify", prefix: "copy-verify-data", pct: 5,
			want: "copy-verify-data: [ok] files=0/0 (100.0%) bytes=0 B/0 B (100.0%) samples=0 pct=5 seed=0000000000000000 elapsed=0s",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatVerifyDataSummaryLine(tc.prefix, tc.result, tc.pct, tc.budget); got != tc.want {
				t.Fatalf("formatVerifyDataSummaryLine()\n  got:  %q\n  want: %q", got, tc.want)
			}
		})
	}
}

// TestVerifyCopyDataSamplesBatchesChecksumRequests verifies a file whose slots
// all hold different bytes against a server that hashes the range it is asked
// for, so a response matched to the wrong slot, or to the wrong request of a
// split batch, fails. Small probe limits split each batch into several CXSUM
// requests.
func TestVerifyCopyDataSamplesBatchesChecksumRequests(t *testing.T) {
	for _, tc := range []struct {
		name            string
		count           int
		pathLength      int
		target, maximum int64 // probe limits; zero leaves the defaults
		minRequests     int64
		encrypted       bool
	}{
		{name: "fallback", count: 1200, pathLength: 3000, minRequests: 2},
		{name: "split", count: 30, pathLength: 30, target: 180, maximum: 400, minRequests: 3},
		{name: "encrypted", count: 30, pathLength: 30, target: 180, maximum: 400, minRequests: 3, encrypted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const frameSize = int64(4 << 10)
			tmp := t.TempDir()
			fileSize := int64(tc.count) * frameSize
			data := make([]byte, fileSize)
			_, _ = rand.NewChaCha8([32]byte{byte(tc.count)}).Read(data)
			if err := os.WriteFile(filepath.Join(tmp, "huge.bin"), data, 0o644); err != nil {
				t.Fatal(err)
			}
			manifest := &tx.Manifest{
				TransferID:  "txverify-batch",
				Root:        "/" + strings.Repeat("r", tc.pathLength),
				Concurrency: 1,
				Entries:     []tx.ManifestEntry{{ID: 1, Size: fileSize, Path: "huge.bin"}},
			}
			cfg := copyCLIConfig{
				localDst:            tmp,
				verifyDataSamplePct: 100,
				concurrency:         1,
				verifyFrameSize:     frameSize,
			}
			var serverID *age.X25519Identity
			if tc.encrypted {
				var err error
				if serverID, err = age.GenerateX25519Identity(); err != nil {
					t.Fatal(err)
				}
				cfg.encryptMode = "auto"
			}
			// At 100% every slot is verified once.
			var expected []sampler.Sample
			for off := int64(0); off < fileSize; off += frameSize {
				expected = append(expected, sampler.Sample{Offset: off, Size: frameSize})
			}
			// Workers run in parallel, so batches arrive in any order.
			var requests atomic.Int64
			var gotMu sync.Mutex
			var got []sampler.Sample
			srv := newFTCPTestServerWithIdentity(t, serverID, func(req intftcp.Request, out io.Writer) error {
				if req.Verb == intftcp.VerbPROBE {
					if tc.maximum == 0 {
						return writeCLIProbeResponse(req, out)
					}
					_, err := fmt.Fprintf(out, "PROBE cpu=1 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=1 target-request-bytes=%d max-request-bytes=%d\nXOK\r\n", req.Params[0]["cts0"], tc.target, tc.maximum)
					return err
				}
				if req.Verb != intftcp.VerbCXSUM {
					return fmt.Errorf("unexpected verb: %v", req.Verb)
				}
				requests.Add(1)
				for _, target := range checksumTargetsFromRequest(t, req) {
					gotMu.Lock()
					got = append(got, sampler.Sample{Offset: target.Offset, Size: target.Size})
					gotMu.Unlock()
					if err := writeChecksumFrame(out, target.FileID, target.Offset, target.Size, checksumTokenForRange(data, target.Offset, target.Size)); err != nil {
						return err
					}
				}
				return writeChecksumOK(out)
			})
			defer srv.Close()

			result, err := verifyCopyDataSamples(srv.URL, cfg, manifest, io.Discard)
			if err != nil {
				t.Fatalf("verifyCopyDataSamples() err = %v", err)
			}
			if result.partial || result.files != 1 || result.samples != int64(tc.count) || result.bytes != fileSize || result.totalBytes != fileSize {
				t.Fatalf("verifyCopyDataSamples() = %+v, want 1 file, %d samples, %d bytes", result, tc.count, fileSize)
			}
			byOffset := func(a, b sampler.Sample) int { return cmp.Compare(a.Offset, b.Offset) }
			slices.SortFunc(got, byOffset)
			slices.SortFunc(expected, byOffset)
			if !slices.Equal(got, expected) {
				t.Fatalf("checksummed ranges differ from the plan: got %d ranges, want %d", len(got), len(expected))
			}
			if requests.Load() < tc.minRequests {
				t.Fatalf("got %d checksum requests, want at least %d", requests.Load(), tc.minRequests)
			}
		})
	}
}

// writeSparseFile creates a file of size zero bytes.
func writeSparseFile(t testing.TB, path string, size int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	fd, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := fd.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestVerifyCopyDataSamplesBudget covers how a run ends when the budget
// expires: dispatch stops, batches in flight get the grace period, and the
// result is partial exactly when samples went unverified. A mismatch found
// during the grace period still fails the run.
func TestVerifyCopyDataSamplesBudget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		files         int
		size, frame   int64 // frame zero means the default
		budget, grace time.Duration
		slowFirst     int // requests that sleep for delay before answering
		delay         time.Duration
		answer        string // zeros, wrong, or silence
		wantErr       string
		wantPartial   bool
		wantFiles     int
		wantRequests  int64
		wantLog       string
	}{
		// Each file is one slot of verifyBatchBytes, so each is a batch of its
		// own. One data connection runs two workers, so two batches start
		// before the budget expires and the other two never do.
		{name: "stops dispatch", files: 4, size: verifyBatchBytes, frame: verifyBatchBytes, budget: 100 * time.Millisecond, grace: 2 * time.Second,
			slowFirst: 2, delay: 300 * time.Millisecond, answer: "zeros", wantPartial: true, wantFiles: 2, wantRequests: 2,
			wantLog: "copy-verify-data: budget expired, verified files=2/4 (50.0%) bytes=32.00 MiB/64.00 MiB (50.0%) samples=2"},
		// A batch in flight when the budget expires that finishes within the
		// grace period completes the run, which must then report [ok].
		{name: "complete in grace", files: 1, size: 1 << 20, frame: 1 << 20, budget: 50 * time.Millisecond, grace: 2 * time.Second,
			slowFirst: 1, delay: 200 * time.Millisecond, answer: "zeros", wantFiles: 1, wantRequests: 1},
		{name: "mismatch in grace", files: 1, size: 15, budget: 10 * time.Millisecond, grace: 100 * time.Millisecond,
			slowFirst: 1, delay: 30 * time.Millisecond, answer: "wrong", wantErr: "checksum mismatch", wantRequests: 1},
		// Batches in flight that outlast the grace period are cancelled, and
		// the batches never dispatched stay unverified; the run still succeeds.
		{name: "forced stop while dispatching", files: 4, size: verifyBatchBytes, frame: verifyBatchBytes, budget: 100 * time.Millisecond, grace: time.Second,
			slowFirst: 2, delay: 2 * time.Second, answer: "silence", wantPartial: true, wantRequests: 2,
			wantLog: "copy-verify-data: budget expired, verified files=0/4 (0.0%)"},
		// A batch that outlasts the grace period is cancelled, and the run
		// returns what it verified without failing.
		{name: "forced stop", files: 1, size: 15, budget: 10 * time.Millisecond, grace: 20 * time.Millisecond,
			slowFirst: 1, delay: 200 * time.Millisecond, answer: "silence", wantPartial: true, wantRequests: 1,
			wantLog: "copy-verify-data: budget expired, verified files=0/1 (0.0%) bytes=0 B/15 B (0.0%) samples=0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withVerifyBudgetGracePeriod(t, tc.grace)
			tmp := t.TempDir()
			manifest := &tx.Manifest{TransferID: "txverify-budget", Root: "/remote", Concurrency: 1}
			for i := range tc.files {
				name := fmt.Sprintf("f%d.bin", i)
				writeSparseFile(t, filepath.Join(tmp, name), tc.size)
				manifest.Entries = append(manifest.Entries, tx.ManifestEntry{ID: uint64(i + 1), Size: tc.size, Path: name})
			}
			cfg := copyCLIConfig{localDst: tmp, verifyDataSamplePct: 100, verifyBudget: tc.budget, verifyFrameSize: tc.frame, concurrency: 1}
			var started atomic.Int64
			srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
				if req.Verb == intftcp.VerbPROBE {
					return writeCLIProbeResponse(req, out)
				}
				if req.Verb != intftcp.VerbCXSUM {
					return fmt.Errorf("unexpected verb: %v", req.Verb)
				}
				if started.Add(1) <= int64(tc.slowFirst) {
					time.Sleep(tc.delay)
				}
				if tc.answer == "silence" {
					return nil
				}
				for _, target := range checksumTargetsFromRequest(t, req) {
					content := make([]byte, target.Size)
					if tc.answer == "wrong" {
						content = bytes.Repeat([]byte{'!'}, int(target.Size))
					}
					if err := writeChecksumFrame(out, target.FileID, target.Offset, target.Size, encoding.FormatXXH128HashToken(xxh3.Hash128(content))); err != nil {
						return err
					}
				}
				return writeChecksumOK(out)
			})
			defer srv.Close()

			type outcome struct {
				result verifyDataResult
				err    error
			}
			var stderr bytes.Buffer
			done := make(chan outcome, 1)
			go func() {
				result, err := verifyCopyDataSamples(srv.URL, cfg, manifest, &stderr)
				done <- outcome{result, err}
			}()
			var got outcome
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("verifyCopyDataSamples() did not return")
			}
			if tc.wantErr != "" {
				if got.err == nil || !strings.Contains(got.err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", got.err, tc.wantErr)
				}
			} else if got.err != nil {
				t.Fatalf("err = %v", got.err)
			} else if got.result.partial != tc.wantPartial || got.result.files != tc.wantFiles || got.result.samples != int64(tc.wantFiles) {
				t.Fatalf("result = %+v, want partial=%v files=%d", got.result, tc.wantPartial, tc.wantFiles)
			}
			if n := started.Load(); n != tc.wantRequests {
				t.Fatalf("server saw %d checksum requests, want %d", n, tc.wantRequests)
			}
			if tc.wantLog != "" && !strings.Contains(stderr.String(), tc.wantLog) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantLog)
			}
			if tc.wantLog == "" && strings.Contains(stderr.String(), "budget expired") {
				t.Fatalf("a run that finished reported an expired budget: %q", stderr.String())
			}
		})
	}
}

// TestRunVerifyCancellation checks what a check that returns after the run was
// cancelled means. The budget and grace period cancel the run while the check
// waits, so no timing decides the outcome: a mismatch is a finding and fails
// the run, and a plain cancellation is not a failure.
func TestRunVerifyCancellation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		after        func(ctx context.Context) error
		wantMismatch bool
	}{
		{name: "mismatch fails", after: func(context.Context) error { return mismatchError{errors.New("data differs")} }, wantMismatch: true},
		{name: "cancellation does not", after: context.Context.Err},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for attempt := 0; ; attempt++ {
				files, totals := newVerifyFiles("/r", []tx.ManifestEntry{{ID: 1, Size: 10, Path: "a"}}, verifyOptions{pct: 100, frameSize: 4, seed: 1},
					func(tx.ManifestEntry) (string, string) { return "", "" })
				var ran atomic.Bool
				result, err := runVerify(verifyRun{files: files, totals: totals, workers: 1, budget: 10 * time.Millisecond, grace: time.Millisecond,
					check: func(ctx context.Context, _ []verifyItem) error {
						ran.Store(true)
						<-ctx.Done()
						return tc.after(ctx)
					}})
				if !ran.Load() {
					// The budget beat dispatch on a stalled machine; try again.
					if attempt < 5 {
						continue
					}
					t.Fatal("the check never ran")
				}
				if tc.wantMismatch {
					if err == nil || !isMismatch(err) {
						t.Fatalf("err = %v, want the mismatch", err)
					}
				} else if err != nil || !result.partial || result.samples != 0 {
					t.Fatalf("result = %+v, err = %v; want a partial run with no failure", result, err)
				}
				return
			}
		})
	}
}

// TestVerifyOptionsSeed checks that each run draws its own seed.
func TestVerifyOptionsSeed(t *testing.T) {
	cfg := copyCLIConfig{verifyDataSamplePct: 5}
	if a, b := cfg.verifyOptions(), cfg.verifyOptions(); a.seed == b.seed {
		t.Fatalf("two runs drew the same seed %016x", a.seed)
	}
}

// TestNewVerifyFilesOrder checks that a run with no budget reads each file's
// slots in order, while a budgeted run spreads them, so stopping early still
// covers the whole file.
func TestNewVerifyFilesOrder(t *testing.T) {
	entries := []tx.ManifestEntry{{ID: 1, Size: 64, Path: "a"}}
	paths := func(tx.ManifestEntry) (string, string) { return "", "" }
	for _, budget := range []time.Duration{0, time.Hour} {
		files, _ := newVerifyFiles("/r", entries, verifyOptions{pct: 100, frameSize: 1, seed: 1, budget: budget}, paths)
		var offsets []int64
		for gen := files[0].gen; gen.Remaining() > 0; gen.Advance() {
			s, _ := gen.Peek()
			offsets = append(offsets, s.Offset)
		}
		if len(offsets) != 64 {
			t.Fatalf("budget %s: drew %d slots, want 64", budget, len(offsets))
		}
		if ascending := slices.IsSorted(offsets); ascending != (budget == 0) {
			t.Fatalf("budget %s: ascending = %v, offsets %v", budget, ascending, offsets)
		}
		// A rotation of the ascending order is not ascending either, but
		// cut short it covers only one stretch of the file.
		if step := (offsets[1] - offsets[0] + 64) % 64; budget > 0 && (step == 1 || step == 63) {
			t.Fatalf("budget %s: slots advance by %d, a rotation rather than a spread: %v", budget, step, offsets)
		}
	}
}

// TestVerifyLocalCopyDataBudgetReportsPartial covers a budget that expires
// while batches are still being dispatched. Whatever the timing, the result
// must say partial whenever it covers less than every byte.
func TestVerifyLocalCopyDataBudgetReportsPartial(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	for i := range 300 {
		data := patternBytes(1024 + i)
		name := fmt.Sprintf("f%03d.bin", i)
		writeLocalTestFile(t, filepath.Join(src, name), data)
		writeLocalTestFile(t, filepath.Join(dst, name), data)
	}
	entries, _, _, err := enumerateLocalSource(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, budget := range []time.Duration{time.Nanosecond, 20 * time.Microsecond, 200 * time.Microsecond, time.Millisecond} {
		for run := range 5 {
			opts := verifyOptions{pct: 100, budget: budget, frameSize: defaultVerifyFrameSize, seed: uint64(run)}
			result, err := verifyLocalCopyData(src, dst, entries, opts, 4)
			if err != nil {
				t.Fatalf("budget %s: %v", budget, err)
			}
			if result.bytes < result.totalBytes && !result.partial {
				t.Fatalf("budget %s: verified %d of %d bytes but reported complete", budget, result.bytes, result.totalBytes)
			}
			if budget == time.Nanosecond && !result.partial {
				t.Fatalf("a 1ns budget reported complete: %+v", result)
			}
		}
	}
}

// packedVerifyTree writes n zero-filled files of 64-128 KiB into dir and
// returns their manifest entries. Zero files let the fake server answer from
// the size alone.
func packedVerifyTree(t testing.TB, dir string, n int, size int64) ([]tx.ManifestEntry, int64) {
	t.Helper()
	entries := make([]tx.ManifestEntry, n)
	var total int64
	for i := range entries {
		size := size
		if size == 0 {
			size = int64(64<<10 + (i%8)*(8<<10))
		}
		name := fmt.Sprintf("f%04d.bin", i)
		writeSparseFile(t, filepath.Join(dir, name), size)
		entries[i] = tx.ManifestEntry{ID: uint64(i + 1), Size: size, Path: name}
		total += size
	}
	return entries, total
}

// zeroChecksumHandler answers CXSUM as if every remote file were zeros, and
// reports each request's targets to seen. A non-nil echo changes each target
// after its hash is computed, to model a server that answers for the wrong
// range.
func zeroChecksumHandler(t *testing.T, seen func([]tx.ChecksumTarget), echo func(*tx.ChecksumTarget)) func(intftcp.Request, io.Writer) error {
	return func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbPROBE {
			return writeCLIProbeResponse(req, out)
		}
		if req.Verb != intftcp.VerbCXSUM {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		targets := checksumTargetsFromRequest(t, req)
		if seen != nil {
			seen(targets)
		}
		for _, target := range targets {
			hash := encoding.FormatXXH128HashToken(xxh3.Hash128(make([]byte, target.Size)))
			if echo != nil {
				echo(&target)
			}
			if err := writeChecksumFrame(out, target.FileID, target.Offset, target.Size, hash); err != nil {
				return err
			}
		}
		return writeChecksumOK(out)
	}
}

// TestVerifyCopyDataSamplesPacksSmallFiles checks that a tree of small files
// costs one CXSUM request per verifyBatchBytes, with requests mixing files.
func TestVerifyCopyDataSamplesPacksSmallFiles(t *testing.T) {
	tmp := t.TempDir()
	entries, total := packedVerifyTree(t, tmp, 400, 0)
	var requests, mixed atomic.Int64
	var mu sync.Mutex
	seenIDs := map[uint64]bool{}
	srv := newFTCPTestServer(t, zeroChecksumHandler(t, func(targets []tx.ChecksumTarget) {
		requests.Add(1)
		mu.Lock()
		defer mu.Unlock()
		for _, target := range targets {
			seenIDs[target.FileID] = true
			if target.FileID != targets[0].FileID {
				mixed.Store(1)
			}
		}
	}, nil))
	defer srv.Close()
	manifest := &tx.Manifest{TransferID: "txverify-pack", Root: "/remote", Concurrency: 2, Entries: entries}
	cfg := copyCLIConfig{localDst: tmp, verifyDataSamplePct: 100, concurrency: 2}
	result, err := verifyCopyDataSamples(srv.URL, cfg, manifest, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if result.files != len(entries) || result.samples != int64(len(entries)) || result.bytes != total || result.partial {
		t.Fatalf("result = %+v, want %d files, %d bytes", result, len(entries), total)
	}
	if limit := total/verifyBatchBytes + 2; requests.Load() > limit {
		t.Fatalf("%d CXSUM requests for %d bytes, want at most %d", requests.Load(), total, limit)
	}
	if mixed.Load() == 0 {
		t.Fatal("no request mixed file ids")
	}
	if len(seenIDs) != len(entries) {
		t.Fatalf("server saw %d files, want %d", len(seenIDs), len(entries))
	}
}

// FuzzVerifyCursor deals random trees into batches and checks that every
// sampled slot is dealt exactly once, no batch exceeds the caps unless it is
// a single item, and each file completes exactly when all its samples pass.
func FuzzVerifyCursor(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint8(100), uint8(22), uint32(16<<20), uint16(1024), false)
	f.Add(uint64(2), uint64(5), uint8(30), uint8(12), uint32(100000), uint16(7), true)
	f.Add(uint64(3), uint64(9), uint8(100), uint8(10), uint32(1), uint16(0), false)
	f.Fuzz(func(t *testing.T, shape, seed uint64, rawPct, frameShift uint8, maxBytes uint32, maxSamples uint16, budgeted bool) {
		frameSize := int64(1) << (6 + frameShift%17)
		rng := rand.New(rand.NewPCG(shape, seed))
		entries := make([]tx.ManifestEntry, 1+rng.IntN(40))
		for i := range entries {
			size := rng.Int64N(frameSize * int64(1+rng.IntN(20)))
			if rng.IntN(4) == 0 {
				size = rng.Int64N(frameSize)
			}
			entries[i] = tx.ManifestEntry{ID: uint64(i + 1), Size: size, Path: fmt.Sprintf("f%d", i)}
		}
		opts := verifyOptions{pct: int(rawPct%100) + 1, frameSize: frameSize, seed: seed}
		if budgeted {
			opts.budget = time.Hour
		}
		files, _ := newVerifyFiles("/r", entries, opts, func(tx.ManifestEntry) (string, string) { return "", "" })

		want := map[[2]int64]bool{}
		for _, vf := range files {
			for gen := vf.gen; gen.Remaining() > 0; gen.Advance() {
				s, _ := gen.Peek()
				want[[2]int64{int64(vf.entry.ID), s.Offset}] = true
			}
		}
		cursor := verifyCursor{files: files}
		var progress verifyProgress
		got := map[[2]int64]bool{}
		last := map[uint64]int64{}
		total := map[*verifyFile]int64{}
		for _, vf := range files {
			total[vf] = vf.gen.TotalSamples()
		}
		dealt := map[*verifyFile]int64{}
		for batch := cursor.nextBatch(int64(maxBytes), int(maxSamples)); batch != nil; batch = cursor.nextBatch(int64(maxBytes), int(maxSamples)) {
			var bytes int64
			for _, it := range batch {
				bytes += it.sample.Size
				key := [2]int64{int64(it.file.entry.ID), it.sample.Offset}
				if got[key] {
					t.Fatalf("sample %v dealt twice", key)
				}
				got[key] = true
				dealt[it.file]++
				// Without a budget each file's slots ascend.
				if prev, seen := last[it.file.entry.ID]; !budgeted && seen && it.sample.Offset <= prev {
					t.Fatalf("file %s slots out of order: %d after %d", it.file.entry.Path, it.sample.Offset, prev)
				}
				last[it.file.entry.ID] = it.sample.Offset
			}
			if len(batch) > max(1, int(maxSamples)) || (len(batch) > 1 && bytes > int64(maxBytes)) {
				t.Fatalf("batch of %d items, %d bytes exceeds caps (%d bytes, %d samples)", len(batch), bytes, maxBytes, maxSamples)
			}
			progress.done(batch)
			// A file completes when its last sample passes, not before.
			complete := 0
			for _, vf := range files {
				if dealt[vf] == total[vf] {
					complete++
				}
			}
			if int(progress.files.Load()) != complete {
				t.Fatalf("progress counts %d complete files, but %d have had every sample dealt", progress.files.Load(), complete)
			}
		}
		for _, vf := range files {
			if left := vf.remaining.Load(); left != 0 {
				t.Fatalf("file %s has %d samples remaining after every batch passed", vf.entry.Path, left)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("dealt %d samples, want %d", len(got), len(want))
		}
		for key := range want {
			if !got[key] {
				t.Fatalf("sample %v never dealt", key)
			}
		}
		if int(progress.files.Load()) != len(files) || progress.samples.Load() != int64(len(want)) {
			t.Fatalf("completed %d/%d files, %d/%d samples", progress.files.Load(), len(files), progress.samples.Load(), len(want))
		}
	})
}

// assertNoOpenFiles fails if a sample file under dir is still open.
func assertNoOpenFiles(t testing.TB, dir string) {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd")
	}
	for _, e := range ents {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(target, dir) {
			t.Fatalf("sample file left open: %s", target)
		}
	}
}

// verifyTestItems lists every slot of entries, with source paths under
// srcDir and destination paths under dstDir.
func verifyTestItems(entries []tx.ManifestEntry, srcDir, dstDir string, frameSize int64) []verifyItem {
	var items []verifyItem
	for i := range entries {
		f := &verifyFile{entry: entries[i], srcPath: filepath.Join(srcDir, entries[i].Path), dstPath: filepath.Join(dstDir, entries[i].Path)}
		for off := int64(0); off < entries[i].Size; off += frameSize {
			items = append(items, verifyItem{file: f, sample: sampler.Sample{Offset: off, Size: min(frameSize, entries[i].Size-off)}})
		}
	}
	return items
}

// TestVerifyCopyItemsErrorPaths drives one remote batch through each way it
// can fail and checks the error, a prompt return, and no sample file left
// open.
func TestVerifyCopyItemsErrorPaths(t *testing.T) {
	const frame = int64(1 << 20)
	blocked := func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbPROBE {
			return writeCLIProbeResponse(req, out)
		}
		time.Sleep(time.Second)
		return errors.New("released")
	}
	for _, tc := range []struct {
		name    string
		mutate  func(t *testing.T, dir string, entries []tx.ManifestEntry)
		handler func(intftcp.Request, io.Writer) error
		cancel  bool
		want    string
	}{
		{name: "server error mid-response", want: "disk on fire", handler: func(req intftcp.Request, out io.Writer) error {
			if req.Verb == intftcp.VerbPROBE {
				return writeCLIProbeResponse(req, out)
			}
			if err := writeChecksumFrame(out, 1, 0, frame, "xxh128:00000000000000000000000000000000"); err != nil {
				return err
			}
			return errors.New("disk on fire")
		}},
		{name: "local file missing", want: "no such file", mutate: func(t *testing.T, dir string, e []tx.ManifestEntry) {
			_ = os.Remove(filepath.Join(dir, e[1].Path))
		}},
		{name: "local file short", want: "unexpected EOF", mutate: func(t *testing.T, dir string, e []tx.ManifestEntry) {
			_ = os.Truncate(filepath.Join(dir, e[1].Path), e[1].Size/2)
		}},
		{name: "mismatch", want: "checksum mismatch", mutate: func(t *testing.T, dir string, e []tx.ManifestEntry) {
			_ = os.WriteFile(filepath.Join(dir, e[0].Path), bytes.Repeat([]byte{9}, int(e[0].Size)), 0o644)
		}, handler: zeroChecksumHandler(t, nil, nil)},
		// A server that answers for a different range must not be believed,
		// even when the hash it sends is right.
		{name: "shifted offset", want: "does not match request", handler: zeroChecksumHandler(t, nil, func(target *tx.ChecksumTarget) { target.Offset++ })},
		{name: "shifted size", want: "does not match request", handler: zeroChecksumHandler(t, nil, func(target *tx.ChecksumTarget) { target.Size-- })},
		{name: "shifted file id", want: "does not match request", handler: zeroChecksumHandler(t, nil, func(target *tx.ChecksumTarget) { target.FileID++ })},
		// A server that answers part of a batch and then OK must not pass.
		{name: "missing result", want: "count mismatch", handler: func(req intftcp.Request, out io.Writer) error {
			if req.Verb == intftcp.VerbPROBE {
				return writeCLIProbeResponse(req, out)
			}
			if err := writeChecksumFrame(out, 1, 0, frame, encoding.FormatXXH128HashToken(xxh3.Hash128(make([]byte, frame)))); err != nil {
				return err
			}
			return writeChecksumOK(out)
		}},
		{name: "cancelled", handler: blocked, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			entries, _ := packedVerifyTree(t, dir, 3, 2*frame)
			if tc.mutate != nil {
				tc.mutate(t, dir, entries)
			}
			handler := tc.handler
			if handler == nil {
				handler = blocked
			}
			srv := newFTCPTestServer(t, handler)
			defer srv.Close()
			client := tx.NewClient(srv.URL)
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				time.AfterFunc(30*time.Millisecond, cancel)
			}
			start := time.Now()
			err := verifyCopyItems(ctx, client, "txverify-items", verifyTestItems(entries, "/remote", dir, frame))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
			if isMismatch(err) != (tc.name == "mismatch") {
				t.Fatalf("isMismatch(%v) = %v", err, isMismatch(err))
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
			if tc.name == "mismatch" && !strings.Contains(err.Error(), entries[0].Path) {
				t.Fatalf("err = %v, want it to name %s", err, entries[0].Path)
			}
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
				t.Fatalf("took %s to fail", elapsed)
			}
			assertNoOpenFiles(t, dir)
		})
	}
}

// TestVerifyLocalItemsErrorPaths covers the daemonless batch the same way.
func TestVerifyLocalItemsErrorPaths(t *testing.T) {
	const frame = int64(1 << 20)
	for _, tc := range []struct {
		name    string
		mutate  func(src, dst string, entries []tx.ManifestEntry)
		cancel  bool
		want    string
		wantErr error
	}{
		{name: "match"},
		{name: "dst missing", want: "no such file", mutate: func(_, dst string, e []tx.ManifestEntry) {
			_ = os.Remove(filepath.Join(dst, e[1].Path))
		}},
		{name: "src missing", want: "no such file", mutate: func(src, _ string, e []tx.ManifestEntry) {
			_ = os.Remove(filepath.Join(src, e[2].Path))
		}},
		{name: "dst short", want: "unexpected EOF", mutate: func(_, dst string, e []tx.ManifestEntry) {
			_ = os.Truncate(filepath.Join(dst, e[1].Path), frame/2)
		}},
		{name: "mismatch", want: "data mismatch f0001.bin", mutate: func(_, dst string, e []tx.ManifestEntry) {
			_ = os.WriteFile(filepath.Join(dst, e[1].Path), bytes.Repeat([]byte{3}, int(e[1].Size)), 0o644)
		}},
		{name: "cancelled", cancel: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			src, dst := filepath.Join(base, "src"), filepath.Join(base, "dst")
			entries, _ := packedVerifyTree(t, src, 3, 2*frame)
			packedVerifyTree(t, dst, 3, 2*frame)
			if tc.mutate != nil {
				tc.mutate(src, dst, entries)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			err := verifyLocalItems(ctx, verifyTestItems(entries, src, dst, frame))
			switch {
			case tc.want == "" && tc.wantErr == nil:
				if err != nil {
					t.Fatalf("err = %v, want success", err)
				}
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case err == nil || !strings.Contains(err.Error(), tc.want):
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
			assertNoOpenFiles(t, base)
		})
	}
}

// TestChecksumHashTimeout checks the CXSUM response deadline, sized to the
// requested work, with and without encryption. An encrypted response arrives
// only as AEAD chunks fill, so a per-frame idle window could not work there.
// A server hashing each 4 MiB target in time must pass even though the whole
// response outlasts one HashTimeout; a stalled one must fail, not hang, and
// its connection must be closed rather than pooled.
func TestChecksumHashTimeout(t *testing.T) {
	const perSlot = 100 * time.Millisecond
	const slots = 8
	targets := make([]tx.ChecksumTarget, slots)
	for i := range targets {
		targets[i] = tx.ChecksumTarget{FileID: 1, FullPath: "/a", Offset: int64(i) << 22, Size: 1 << 22}
	}
	for _, encrypted := range []bool{false, true} {
		for _, tc := range []struct {
			name        string
			stall, gap  time.Duration
			wantTimeout bool
		}{
			{name: "steady", gap: perSlot / 3},
			{name: "stalled", stall: 2 * slots * perSlot, wantTimeout: true},
		} {
			t.Run(fmt.Sprintf("%s/encrypted=%v", tc.name, encrypted), func(t *testing.T) {
				var serverID *age.X25519Identity
				opts := []tx.ClientOption{}
				if encrypted {
					var err error
					if serverID, err = age.GenerateX25519Identity(); err != nil {
						t.Fatal(err)
					}
					pub, id, mode, err := resolveEncryptionOptionsWithKeys("auto", "")
					if err != nil {
						t.Fatal(err)
					}
					opts = append(opts, tx.WithClientAgePublicKey(pub), tx.WithClientAgeIdentity(id), tx.WithEncryptMode(mode))
				}
				srv := newFTCPTestServerWithIdentity(t, serverID, func(req intftcp.Request, out io.Writer) error {
					time.Sleep(tc.stall)
					for _, target := range targets {
						time.Sleep(tc.gap) // hashing one 4 MiB slot
						if err := writeChecksumFrame(out, target.FileID, target.Offset, target.Size, "xxh128:00"); err != nil {
							return nil
						}
					}
					return writeChecksumOK(out)
				})
				defer srv.Close()
				var closes atomic.Int64
				opts = append(opts, tx.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
					return closeCounter{Conn: conn, closes: &closes}, err
				}))
				c := tx.NewClient(srv.URL, opts...)
				defer c.Close()
				start := time.Now()
				err := c.VisitChecksumBatches(context.Background(), tx.GetChecksumRequest{TransferID: "tid", Targets: targets},
					tx.ChecksumBatchOptions{HashTimeout: perSlot},
					func(_ []tx.ChecksumTarget, r tx.GetChecksumResponse) error {
						_, err := io.Copy(io.Discard, r.Reader)
						return err
					})
				var netErr net.Error
				timedOut := errors.As(err, &netErr) && netErr.Timeout()
				if timedOut != tc.wantTimeout || (!tc.wantTimeout && err != nil) {
					t.Fatalf("err = %v, want timeout %v", err, tc.wantTimeout)
				}
				if !tc.wantTimeout {
					if time.Since(start) < 2*perSlot {
						t.Fatalf("response took %s; it must outlast one HashTimeout to test the sizing", time.Since(start))
					}
					return
				}
				if time.Since(start) >= tc.stall {
					t.Fatalf("stalled request took %s; the deadline did not fire before the server woke", time.Since(start))
				}
				if closes.Load() == 0 {
					t.Fatal("timed-out connection was not closed")
				}
			})
		}
	}
}

// closeCounter counts Close calls on a dialed connection.
type closeCounter struct {
	net.Conn
	closes *atomic.Int64
}

func (c closeCounter) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}
