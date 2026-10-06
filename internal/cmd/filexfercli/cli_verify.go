package filexfercli

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/sampler"
	"github.com/zeebo/xxh3"
)

// defaultVerifyFrameSize is the slot data verification samples whole.
const defaultVerifyFrameSize int64 = 4 * 1024 * 1024

// verifyBatchBytes bounds the sample bytes in one verification batch, and so
// the bytes the server hashes per CXSUM request.
const verifyBatchBytes int64 = 16 * 1024 * 1024
const verifyBatchMaxSamples = 1024
const verifyHashBufferBytes int64 = 1 * 1024 * 1024
const verifyChecksumRequestTargetBytes = 3 * 1024 * 1024

// verifyChecksumRequestTimeout is the probe timeout and the hash time allowed
// per started 4 MiB of a CXSUM request.
const verifyChecksumRequestTimeout = 30 * time.Second

var verifyBudgetGracePeriod = 10 * time.Second

// verifyOptions is the resolved sampled-verification configuration of one
// run.
type verifyOptions struct {
	pct       int
	budget    time.Duration
	frameSize int64
	// seed picks the run's samples. Each run draws a new one, so repeated
	// runs cover different data; it is printed so a run can be reproduced.
	seed uint64
}

// verifyOptions resolves cfg's verification settings and draws the run's
// random seed.
func (cfg copyCLIConfig) verifyOptions() verifyOptions {
	opts := verifyOptions{pct: cfg.verifyDataSamplePct, budget: cfg.verifyBudget, frameSize: cfg.verifyFrameSize}
	if opts.frameSize <= 0 {
		opts.frameSize = defaultVerifyFrameSize
	}
	var b [8]byte
	_, _ = crand.Read(b[:])
	opts.seed = binary.LittleEndian.Uint64(b[:])
	return opts
}

// verifyDataResult is how much sampled data verification covered. Totals
// count every regular file with data and the sum of their sizes, including
// files the sampler skipped.
type verifyDataResult struct {
	files, totalFiles int
	bytes, totalBytes int64
	samples           int64
	planned           int64 // samples the plan drew; fewer verified is partial
	elapsed           time.Duration
	partial           bool
	seed              uint64
}

// coverage reports the files and bytes verified out of the totals.
func (r verifyDataResult) coverage() string {
	return fmt.Sprintf("files=%d/%d (%s) bytes=%s/%s (%s) samples=%d",
		r.files, r.totalFiles, percentOf(int64(r.files), int64(r.totalFiles)),
		encoding.HumanBytes(r.bytes), encoding.HumanBytes(r.totalBytes), percentOf(r.bytes, r.totalBytes), r.samples)
}

// percentOf formats n/total; nothing to verify counts as fully covered.
func percentOf(n, total int64) string {
	if total <= 0 {
		return "100.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total))
}

// finishDataVerify runs data verification when it was requested, prints its
// summary, or its failure with the seed that reproduces the samples, and
// returns the command's exit code.
func finishDataVerify(stderr io.Writer, prefix string, cfg copyCLIConfig, metaFailed bool, verify func() (verifyDataResult, error)) int {
	if cfg.verifyDataSamplePct > 0 {
		result, err := verify()
		if err != nil {
			fmt.Fprintf(stderr, "%s: [fail] seed=%016x %v\n", prefix, result.seed, err)
			return 1
		}
		fmt.Fprintln(stderr, formatVerifyDataSummaryLine(prefix, result, cfg.verifyDataSamplePct, cfg.verifyBudget))
	}
	if metaFailed {
		return 1
	}
	return 0
}

func formatVerifyDataSummaryLine(prefix string, r verifyDataResult, pct int, budget time.Duration) string {
	status := "[ok]"
	if r.partial {
		status = "[partial-ok]"
	}
	limit := fmt.Sprintf("pct=%d", pct)
	if budget > 0 {
		limit = "budget=" + budget.String()
	}
	return fmt.Sprintf("%s: %s %s %s seed=%016x elapsed=%s", prefix, status, r.coverage(), limit, r.seed, r.elapsed.Round(time.Millisecond))
}

// verifyFile is one file's sampled verification. remaining counts samples
// not yet verified, across batches, so the sample that brings it to zero
// completes the file.
type verifyFile struct {
	entry     tx.ManifestEntry
	srcPath   string // server path for remote copies, source path for local ones
	dstPath   string
	gen       sampler.Generator
	remaining atomic.Int64
}

// verifyItem is one sampled slot of one file.
type verifyItem struct {
	file   *verifyFile
	sample sampler.Sample
}

// verifyCursor deals files' samples into batches in manifest order. A batch
// draws from consecutive files, so a tree of small files costs one request
// per batch rather than one per file, and a large file spreads across
// workers.
type verifyCursor struct {
	files []*verifyFile
	next  int
}

// nextBatch takes samples until the next would push the batch past maxBytes
// or maxSamples. It takes at least one sample, so a slot larger than maxBytes
// still makes progress, and returns nil once every sample is dealt.
func (c *verifyCursor) nextBatch(maxBytes int64, maxSamples int) []verifyItem {
	var batch []verifyItem
	var bytes int64
	for c.next < len(c.files) && len(batch) < max(1, maxSamples) {
		f := c.files[c.next]
		sample, ok := f.gen.Peek()
		if !ok {
			c.next++
			continue
		}
		if len(batch) > 0 && bytes+sample.Size > maxBytes {
			break
		}
		batch = append(batch, verifyItem{file: f, sample: sample})
		bytes += sample.Size
		f.gen.Advance()
	}
	return batch
}

// newVerifyFiles plans sampling for every regular file with data and returns
// the files that drew at least one sample, plus the coverage totals.
func newVerifyFiles(root string, entries []tx.ManifestEntry, opts verifyOptions, paths func(tx.ManifestEntry) (src, dst string)) ([]*verifyFile, verifyDataResult) {
	totals := verifyDataResult{seed: opts.seed}
	var data []tx.ManifestEntry
	var slots int64
	for _, entry := range entries {
		if isRegularFileEntry(entry.Type) && entry.Size > 0 {
			data = append(data, entry)
			totals.totalBytes += entry.Size
			slots += sampler.SlotCount(entry.Size, opts.frameSize)
		}
	}
	totals.totalFiles = len(data)
	plan := sampler.NewPlan(sampler.PlanOptions{Root: root, Pct: opts.pct, FrameSize: opts.frameSize, TotalSlots: slots, Seed: opts.seed,
		// A budget can cut the run short, so spread its coverage across each
		// file. Without one every slot is read, and in order.
		Sequential: opts.budget <= 0})
	var files []*verifyFile
	for _, entry := range data {
		gen, ok := plan.Next(entry.Path, entry.ID, entry.Size)
		if !ok {
			continue
		}
		f := &verifyFile{entry: entry, gen: gen}
		f.srcPath, f.dstPath = paths(entry)
		f.remaining.Store(gen.TotalSamples())
		totals.planned += gen.TotalSamples()
		files = append(files, f)
	}
	return files, totals
}

// verifyProgress accumulates verified coverage across workers.
type verifyProgress struct {
	files, samples, bytes atomic.Int64
}

func (p *verifyProgress) done(items []verifyItem) {
	var bytes int64
	for _, it := range items {
		bytes += it.sample.Size
		if it.file.remaining.Add(-1) == 0 {
			p.files.Add(1)
		}
	}
	p.bytes.Add(bytes)
	p.samples.Add(int64(len(items)))
}

// result reports coverage so far. It is partial whenever fewer samples
// verified than were planned, however the run ended.
func (p *verifyProgress) result(totals verifyDataResult, start time.Time) verifyDataResult {
	totals.files = int(p.files.Load())
	totals.samples = p.samples.Load()
	totals.bytes = p.bytes.Load()
	totals.elapsed = time.Since(start)
	totals.partial = totals.samples < totals.planned
	return totals
}

// mismatchError marks a data mismatch, which fails the run even when it is
// found while the run is being cancelled.
type mismatchError struct{ error }

func isMismatch(err error) bool { return errors.As(err, new(mismatchError)) }

// verifyRun configures runVerify.
type verifyRun struct {
	files   []*verifyFile
	totals  verifyDataResult
	workers int
	budget  time.Duration // stops dispatch; zero means none
	grace   time.Duration // how long batches in flight may finish after the budget
	tick    func(verifyDataResult)
	check   func(context.Context, []verifyItem) error
}

// runVerify deals the planned samples to workers that check them, and
// returns the coverage reached. The first failure cancels the rest. Once the
// budget expires, dispatch stops and batches in flight get the grace period
// before they are cancelled. tick, when set, reports progress every 2s.
func runVerify(r verifyRun) (verifyDataResult, error) {
	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var progress verifyProgress
	var failOnce sync.Once
	var failure error
	batches := make(chan []verifyItem)
	var wg sync.WaitGroup
	for range max(1, r.workers) {
		wg.Go(func() {
			for items := range batches {
				err := r.check(ctx, items)
				switch {
				case err == nil:
					progress.done(items)
				// Once cancelled, an error is the cancellation, except a
				// mismatch, which is a finding.
				case ctx.Err() == nil || isMismatch(err):
					failOnce.Do(func() { failure = err; cancel() })
				}
			}
		})
	}
	var budgetC, graceC, tickC <-chan time.Time
	if r.budget > 0 {
		t := time.NewTimer(r.budget)
		defer t.Stop()
		budgetC = t.C
	}
	if r.tick != nil {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		tickC = t.C
	}
	startGrace := func() {
		budgetC, graceC = nil, time.After(r.grace)
	}

	// Small trees still spread across every worker.
	maxSamples := int(min(verifyBatchMaxSamples, max(1, r.totals.planned/int64(max(1, r.workers)))))
	cursor := verifyCursor{files: r.files}
dispatch:
	for items := cursor.nextBatch(verifyBatchBytes, maxSamples); items != nil; items = cursor.nextBatch(verifyBatchBytes, maxSamples) {
		for {
			select {
			case batches <- items:
				continue dispatch
			case <-ctx.Done():
				break dispatch
			case <-budgetC:
				startGrace()
				break dispatch
			case <-tickC:
				r.tick(progress.result(r.totals, start))
			}
		}
	}
	close(batches)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		select {
		case <-done:
			return progress.result(r.totals, start), failure
		case <-budgetC:
			startGrace()
		case <-graceC:
			graceC = nil
			cancel()
		case <-tickC:
			r.tick(progress.result(r.totals, start))
		}
	}
}

// hashItems hashes each item's range of its source or destination file.
// Items of one file are adjacent, so it holds one descriptor at a time. It
// stops with ctx's error once ctx is cancelled.
func hashItems(ctx context.Context, items []verifyItem, src bool) ([]string, error) {
	buf, release, err := bufpool.Acquire(int(verifyHashBufferBytes))
	if err != nil {
		return nil, err
	}
	defer release()
	h := xxh3.New128()
	hashes := make([]string, len(items))
	var fd *os.File
	defer func() {
		if fd != nil {
			_ = fd.Close()
		}
	}()
	for i, it := range items {
		if i == 0 || it.file != items[i-1].file {
			if fd != nil {
				_ = fd.Close()
			}
			path := it.file.dstPath
			if src {
				path = it.file.srcPath
			}
			if fd, err = os.Open(path); err != nil {
				return nil, err
			}
		}
		h.Reset()
		r := io.NewSectionReader(fd, it.sample.Offset, it.sample.Size)
		for remaining := it.sample.Size; remaining > 0; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			n, err := io.ReadFull(r, buf[:min(remaining, int64(len(buf)))])
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			if err != nil {
				return nil, fmt.Errorf("hash %s@%d: %w", fd.Name(), it.sample.Offset, err)
			}
			_, _ = h.Write(buf[:n])
			remaining -= int64(n)
		}
		hashes[i] = encoding.FormatXXH128HashToken(h.Sum128())
	}
	return hashes, nil
}

func verifyCopyDataSamples(serverURL string, cfg copyCLIConfig, manifest *tx.Manifest, stderr io.Writer) (verifyDataResult, error) {
	opts := cfg.verifyOptions()
	agePublicKey, ageIdentity, encMode, err := resolveEncryptionOptionsWithKeys(cfg.encryptMode, cfg.keysDir)
	if err != nil {
		return verifyDataResult{seed: opts.seed}, fmt.Errorf("invalid --encrypt: %w", err)
	}
	files, totals := newVerifyFiles(manifest.Root, manifest.Entries, opts, func(entry tx.ManifestEntry) (string, string) {
		return filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path))),
			filepath.Join(cfg.localDst, filepath.FromSlash(entry.Path))
	})
	if len(files) == 0 {
		return totals, nil
	}
	client, closeClient := phaseClient(cfg.client, serverURL, tx.WithClientAgePublicKey(agePublicKey), tx.WithClientAgeIdentity(ageIdentity), tx.WithEncryptMode(encMode), tx.WithClientAuthTokens(cfg.authTokens...), tx.WithClientMetrics(cfg.stats.ClientMetrics()), tx.WithEventSink(cfg.stats.EventSink()))
	defer closeClient()
	probeCtx, probeCancel := context.WithTimeout(context.Background(), verifyChecksumRequestTimeout)
	_, err = client.ProbeLink(probeCtx, tx.ProbeRequest{ProbeBytes: 1})
	probeCancel()
	if err != nil {
		return totals, fmt.Errorf("probe checksum request limits: %w", err)
	}
	connections := cfg.concurrency
	if connections <= 0 {
		connections = manifest.Concurrency
	}
	run := verifyRun{
		files:  files,
		totals: totals,
		// Twice the data connections: while half the workers wait on the
		// server's hashing, the rest hash locally, so both sides stay busy.
		workers: 2 * max(1, connections),
		budget:  opts.budget,
		grace:   verifyBudgetGracePeriod,
		check: func(ctx context.Context, items []verifyItem) error {
			return verifyCopyItems(ctx, client, manifest.TransferID, items)
		},
	}
	if stderr != nil {
		run.tick = func(r verifyDataResult) {
			fmt.Fprintf(stderr, "copy-verify-data: progress %s pct=%d\n", r.coverage(), opts.pct)
		}
	}
	result, err := runVerify(run)
	if err == nil && result.partial && stderr != nil {
		fmt.Fprintf(stderr, "copy-verify-data: budget expired, verified %s in %s\n", result.coverage(), result.elapsed.Round(time.Millisecond))
	}
	return result, err
}

// verifyCopyItems compares the items' local hashes with the server's CXSUM
// hashes of the same ranges.
func verifyCopyItems(ctx context.Context, client *tx.Client, transferID string, items []verifyItem) error {
	want, err := hashItems(ctx, items, false)
	if err != nil {
		return err
	}
	targets := make([]tx.ChecksumTarget, len(items))
	for i, it := range items {
		targets[i] = tx.ChecksumTarget{FileID: it.file.entry.ID, FullPath: it.file.srcPath, Offset: it.sample.Offset, Size: it.sample.Size, Algo: "xxh128"}
	}
	verified := 0
	err = client.VisitChecksumBatches(ctx, tx.GetChecksumRequest{TransferID: transferID, Targets: targets}, tx.ChecksumBatchOptions{
		TargetRequestBytes: verifyChecksumRequestTargetBytes, HashTimeout: verifyChecksumRequestTimeout,
	}, func(requested []tx.ChecksumTarget, resp tx.GetChecksumResponse) error {
		results, err := readChecksumResults(resp.Reader)
		if err != nil {
			return fmt.Errorf("read checksum response: %w", err)
		}
		if len(results) != len(requested) {
			return fmt.Errorf("checksum response count mismatch: got %d want %d", len(results), len(requested))
		}
		for i, result := range results {
			f, s := items[verified+i].file, items[verified+i].sample
			if result.FileID != f.entry.ID || result.Offset != s.Offset || result.Size != s.Size {
				return fmt.Errorf("checksum response for file %d offset=%d size=%d does not match request for %s offset=%d size=%d",
					result.FileID, result.Offset, result.Size, f.srcPath, s.Offset, s.Size)
			}
			if !strings.EqualFold(result.FileHashToken, want[verified+i]) {
				return mismatchError{fmt.Errorf("checksum mismatch for %s at offset=%d size=%d", f.dstPath, s.Offset, s.Size)}
			}
		}
		verified += len(requested)
		return nil
	})
	if err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}
	return nil
}

func verifyLocalCopyData(srcRoot, dstRoot string, entries []localEntry, opts verifyOptions, concurrency int) (verifyDataResult, error) {
	manifestEntries := make([]tx.ManifestEntry, len(entries))
	for i, le := range entries {
		manifestEntries[i] = le.entry
	}
	files, totals := newVerifyFiles(srcRoot, manifestEntries, opts, func(entry tx.ManifestEntry) (string, string) {
		return filepath.Join(srcRoot, filepath.FromSlash(entry.Path)), filepath.Join(dstRoot, filepath.FromSlash(entry.Path))
	})
	// Twice the concurrency keeps the I/O depth up on both trees.
	return runVerify(verifyRun{files: files, totals: totals, workers: 2 * max(1, concurrency), budget: opts.budget, check: verifyLocalItems})
}

// verifyLocalItems compares the items' ranges between source and destination.
func verifyLocalItems(ctx context.Context, items []verifyItem) error {
	src, err := hashItems(ctx, items, true)
	if err != nil {
		return err
	}
	dst, err := hashItems(ctx, items, false)
	if err != nil {
		return err
	}
	for i, it := range items {
		if !strings.EqualFold(src[i], dst[i]) {
			return mismatchError{fmt.Errorf("data mismatch %s at offset=%d size=%d", it.file.entry.Path, it.sample.Offset, it.sample.Size)}
		}
	}
	return nil
}
