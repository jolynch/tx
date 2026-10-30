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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/benchio"
	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/idset"
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
	// unchecked is whether some planned sample was neither verified nor in
	// a batch that found a mismatch, so a budget cut or a failure left it
	// unexamined. A run that found mismatches is partial yet may be fully
	// checked.
	unchecked bool
	seed      uint64
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
			// A failure joined with mismatches spans lines; keep it on the
			// [fail] line.
			fmt.Fprintf(stderr, "%s: [fail] seed=%016x %s\n", prefix, result.seed, strings.ReplaceAll(err.Error(), "\n", "; "))
			var m mismatchError
			if errors.As(err, &m) {
				m.markForRefetch(stderr, prefix)
			}
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
	mismatched            atomic.Int64 // samples of batches that found a mismatch
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
	totals.unchecked = totals.samples+p.mismatched.Load() < totals.planned
	return totals
}

// mismatchError marks a data mismatch, which fails the run even when it is
// found while the run is being cancelled.
// It names every mismatching file it knows of, once each, by manifest ID: a
// run that finds every file wrong holds a bit per file, not a path.
type mismatchError struct {
	error            // detail of the first mismatch found
	ids   *idset.Set // nil until the first mismatch
	// files is the run's verify files, which Error searches to name the
	// first few of ids. runVerify sets it on the error it returns; without
	// it, Error names IDs.
	files []*verifyFile
}

// maxMismatchPathsShown caps the destination paths in a mismatch message.
const maxMismatchPathsShown = 8

func isMismatch(err error) bool { return errors.As(err, new(mismatchError)) }

// add records a mismatch in item's file. The first mismatch's detail stands,
// so detail runs only for it and the rest cost no formatting.
func (m *mismatchError) add(it verifyItem, detail func(verifyItem) error) {
	if m.ids == nil {
		m.ids = new(idset.Set)
	}
	m.ids.Add(it.file.entry.ID)
	if m.error == nil {
		m.error = detail(it)
	}
}

// merge adds other's files and keeps the first detail.
func (m *mismatchError) merge(other mismatchError) {
	if m.error == nil {
		m.error = other.error
	}
	if other.ids.Len() == 0 {
		return
	}
	if m.ids == nil {
		m.ids = new(idset.Set)
	}
	m.ids.Union(other.ids)
}

// Error names the count and up to maxMismatchPathsShown destination paths,
// then the first mismatch's detail. The paths are the first files in
// manifest order, found by scanning the run's files once.
func (m mismatchError) Error() string {
	detail := "data mismatch"
	if m.error != nil {
		detail = m.error.Error()
	}
	n := m.ids.Len()
	if n == 0 {
		return detail
	}
	shown := make([]string, 0, maxMismatchPathsShown)
	if m.files != nil {
		for _, f := range m.files {
			if len(shown) == maxMismatchPathsShown {
				break
			}
			if m.ids.Has(f.entry.ID) {
				shown = append(shown, f.dstPath)
			}
		}
	} else {
		for id := range m.ids.All() {
			if len(shown) == maxMismatchPathsShown {
				break
			}
			shown = append(shown, fmt.Sprintf("file %d", id))
		}
	}
	list := strings.Join(shown, ", ")
	if more := n - len(shown); more > 0 {
		list += fmt.Sprintf(", and %d more", more)
	}
	noun := "files"
	if n == 1 {
		noun = "file"
	}
	return fmt.Sprintf("%d %s failed data verification: %s (first: %s)", n, noun, list, detail)
}

// MismatchError reports that data verification found files whose bytes
// differ from the source.
type MismatchError struct {
	// IDs are the manifest IDs of the mismatching files, ascending.
	IDs []uint64
	err error // the verification error; nil for one built from IDs alone
}

func (e *MismatchError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("%d files failed verification: %v", len(e.IDs), e.IDs)
	}
	return e.err.Error()
}

// Unwrap returns the verification error, which can join a failure other
// than the mismatches.
func (e *MismatchError) Unwrap() error { return e.err }

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
// returns the coverage reached. The first failure other than a mismatch
// cancels the rest; mismatches are collected from every batch and returned
// together, so a run names all the bad files it reached. Once the
// budget expires, dispatch stops and batches in flight get the grace period
// before they are cancelled. tick, when set, reports progress every 2s.
func runVerify(r verifyRun) (verifyDataResult, error) {
	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var progress verifyProgress
	var failOnce sync.Once
	var failure error
	var mismatchMu sync.Mutex
	var mismatches mismatchError
	batches := make(chan []verifyItem)
	var wg sync.WaitGroup
	for range max(1, r.workers) {
		wg.Go(func() {
			for items := range batches {
				err := r.check(ctx, items)
				switch {
				case err == nil:
					progress.done(items)
				// A mismatch is a finding, not a reason to stop: collect it and
				// keep verifying so one run names every bad file.
				case isMismatch(err):
					var m mismatchError
					errors.As(err, &m)
					progress.mismatched.Add(int64(len(items)))
					mismatchMu.Lock()
					mismatches.merge(m)
					mismatchMu.Unlock()
				// Once cancelled, any other error is the cancellation.
				case ctx.Err() == nil:
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
			// Every worker has returned, so nothing else touches mismatches.
			result := progress.result(r.totals, start)
			if mismatches.error == nil {
				return result, failure
			}
			mismatches.files = r.files
			if failure == nil {
				return result, mismatches
			}
			return result, errors.Join(failure, mismatches)
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
func hashItems(ctx context.Context, items []verifyItem, src bool, inj *faults.Injector) ([]string, error) {
	var largest int64
	for _, it := range items {
		largest = max(largest, it.sample.Size)
	}
	pooled, err := bufpool.Get(bufpool.BucketSize(min(max(largest, 1), verifyHashBufferBytes)))
	if err != nil {
		return nil, err
	}
	defer bufpool.Put(pooled)
	buf := *pooled
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
			if fd, err = os.Open(path); err != nil { //nolint:forbidigo // Reads use WrapReaderAt below; OpenErr applies to SEND and CXSUM.
				return nil, err
			}
		}
		h.Reset()
		var fr io.ReaderAt = fd
		if inj != nil {
			fr = inj.WrapReaderAt(ctx, faults.SiteVerifyRead, it.file.entry.ID, fd)
		}
		r := io.NewSectionReader(benchio.WithReadLatency(ctx, "client", fr), it.sample.Offset, it.sample.Size)
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
	files, totals := newVerifyFiles(manifest.Root, manifest.Entries, opts, func(entry tx.ManifestEntry) (string, string) {
		return filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path))),
			filepath.Join(cfg.localDst, filepath.FromSlash(entry.Path))
	})
	if len(files) == 0 {
		return totals, nil
	}
	client, closeClient := phaseClient(cfg.client, serverURL, append(cfg.auth.options(), tx.WithClientMetrics(cfg.stats.ClientMetrics()), tx.WithEventSink(cfg.stats.EventSink()))...)
	defer closeClient()
	probeCtx, probeCancel := context.WithTimeout(context.Background(), verifyChecksumRequestTimeout)
	probe, err := client.ProbeLink(probeCtx, tx.ProbeRequest{ProbeBytes: 1})
	probeCancel()
	if err != nil {
		return totals, fmt.Errorf("probe checksum request limits: %w", err)
	}
	connections := cfg.concurrency
	if connections <= 0 {
		connections = manifest.Concurrency
	}
	if connections <= 0 {
		connections = probe.WarmConnectionPoolSize
	}
	if probe.WarmConnectionPoolSize > 0 {
		connections = min(connections, probe.WarmConnectionPoolSize)
	}
	run := verifyRun{
		files:  files,
		totals: totals,
		// Each worker owns at most one data connection.
		workers: max(1, connections),
		budget:  opts.budget,
		grace:   verifyBudgetGracePeriod,
		check: func(ctx context.Context, items []verifyItem) error {
			return verifyCopyItems(ctx, client, cfg.stats.EventSink(), manifest.TransferID, items)
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

// VerifyCopyOptions configures VerifyCopy.
type VerifyCopyOptions struct {
	// Workers is how many batches verify at once; zero means one.
	Workers int
	// Budget stops dispatching batches once it expires, and Grace cancels
	// the batches still running that long after; zero means no budget. A
	// run the budget cuts short is an error.
	Budget, Grace time.Duration
}

// VerifyCopy checks every byte of a remote copy, as tx recv copy --verify
// full does: each regular file's local copy, at local(entry), against the
// server's CXSUM hashes of the same ranges. It returns nil only when every
// range matched. When ranges differ and nothing else went wrong, the error
// is a *MismatchError naming every mismatching file; when a failure or a
// budget cut also left files unchecked, it wraps one (errors.As finds it),
// and its IDs name only the files reached. internal/memxfer uses it to run
// this verification against its memfds.
func VerifyCopy(client *tx.Client, manifest *tx.Manifest, local func(tx.ManifestEntry) string, opts VerifyCopyOptions) error {
	sampling := verifyOptions{pct: 100, frameSize: defaultVerifyFrameSize, seed: 1, budget: opts.Budget}
	files, totals := newVerifyFiles(manifest.Root, manifest.Entries, sampling, func(entry tx.ManifestEntry) (string, string) {
		return filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path))), local(entry)
	})
	if len(files) == 0 {
		return nil
	}
	result, err := runVerify(verifyRun{files: files, totals: totals, workers: max(1, opts.Workers), budget: opts.Budget, grace: opts.Grace,
		check: func(ctx context.Context, items []verifyItem) error {
			return verifyCopyItems(ctx, client, nil, manifest.TransferID, items)
		}})
	var m mismatchError
	if !errors.As(err, &m) {
		if err == nil && result.partial {
			err = fmt.Errorf("verified only %s", result.coverage())
		}
		return err
	}
	me := &MismatchError{IDs: slices.Collect(m.ids.All()), err: err}
	if _, pure := err.(mismatchError); pure && !result.unchecked {
		return me
	}
	// The mismatches are not the whole story: a failure or a budget cut
	// left files unchecked. The wrapper still matches *MismatchError with
	// errors.As, but is not one, so a caller that needs every wrong file
	// can tell.
	if result.unchecked {
		return fmt.Errorf("%w; verified only %s", me, result.coverage())
	}
	return fmt.Errorf("verification incomplete: %w", me)
}

// verifyCopyItems compares the items' local hashes with the server's CXSUM
// hashes of the same ranges. The server hashes while this worker hashes the
// local copy. Batch duration approaches the slower side's duration.
// The first failure on either side cancels the other and is the one
// returned. When the hashes differ, the error names every mismatching file
// in the batch. A verify_hash event times the local hashing; the client's CXSUM
// req_start/req_end events time the remote request.
func verifyCopyItems(ctx context.Context, client *tx.Client, sink *events.Sink, transferID string, items []verifyItem) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var failOnce sync.Once
	var failure error
	fail := func(err error) {
		failOnce.Do(func() { failure = err; cancel() })
	}
	remote := make([]string, len(items))
	remoteDone := make(chan struct{})
	go func() {
		defer close(remoteDone)
		if err := remoteChecksums(ctx, client, transferID, items, remote); err != nil {
			fail(err)
		}
	}()
	start := time.Now()
	want, err := hashItems(ctx, items, false, client.Faults())
	if err != nil {
		fail(err)
	} else if sink.Enabled() {
		var bytes int64
		for _, it := range items {
			bytes += it.sample.Size
		}
		sink.Emit("verify_hash", transferID, events.F("items", len(items)), events.F("bytes", bytes), events.Dur("dur", time.Since(start)))
	}
	<-remoteDone
	if failure != nil {
		return failure
	}
	var bad mismatchError
	for i, it := range items {
		if !strings.EqualFold(remote[i], want[i]) {
			bad.add(it, func(it verifyItem) error {
				return fmt.Errorf("checksum mismatch for %s at offset=%d size=%d", it.file.dstPath, it.sample.Offset, it.sample.Size)
			})
		}
	}
	if bad.ids.Len() > 0 {
		return bad
	}
	return nil
}

// remoteChecksums stores the server's CXSUM hash of each item's range in
// hashes, in item order.
func remoteChecksums(ctx context.Context, client *tx.Client, transferID string, items []verifyItem, hashes []string) error {
	targets := make([]tx.ChecksumTarget, len(items))
	for i, it := range items {
		targets[i] = tx.ChecksumTarget{FileID: it.file.entry.ID, FullPath: it.file.srcPath, Offset: it.sample.Offset, Size: it.sample.Size, Algo: "xxh128"}
	}
	verified := 0
	request := tx.GetChecksumRequest{TransferID: transferID, Targets: targets}
	options := tx.ChecksumBatchOptions{TargetRequestBytes: verifyChecksumRequestTargetBytes, HashTimeout: verifyChecksumRequestTimeout}
	err := client.VisitChecksumBatches(ctx, request, options, func(requested []tx.ChecksumTarget, resp tx.GetChecksumResponse) error {
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
			hashes[verified+i] = result.FileHashToken
		}
		verified += len(requested)
		return nil
	})
	if err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}
	return nil
}

func verifyLocalCopyData(srcRoot, dstRoot string, entries []localEntry, opts verifyOptions, concurrency int, inj *faults.Injector) (verifyDataResult, error) {
	manifestEntries := make([]tx.ManifestEntry, len(entries))
	for i, le := range entries {
		manifestEntries[i] = le.entry
	}
	files, totals := newVerifyFiles(srcRoot, manifestEntries, opts, func(entry tx.ManifestEntry) (string, string) {
		return filepath.Join(srcRoot, filepath.FromSlash(entry.Path)), filepath.Join(dstRoot, filepath.FromSlash(entry.Path))
	})
	// Twice the concurrency keeps the I/O depth up on both trees.
	check := func(ctx context.Context, items []verifyItem) error { return verifyLocalItems(ctx, items, inj) }
	return runVerify(verifyRun{files: files, totals: totals, workers: 2 * max(1, concurrency), budget: opts.budget, check: check})
}

// verifyLocalItems compares the items' ranges between source and destination.
func verifyLocalItems(ctx context.Context, items []verifyItem, inj *faults.Injector) error {
	src, err := hashItems(ctx, items, true, inj)
	if err != nil {
		return err
	}
	dst, err := hashItems(ctx, items, false, inj)
	if err != nil {
		return err
	}
	var bad mismatchError
	for i, it := range items {
		if !strings.EqualFold(src[i], dst[i]) {
			bad.add(it, func(it verifyItem) error {
				return fmt.Errorf("data mismatch %s at offset=%d size=%d", it.file.entry.Path, it.sample.Offset, it.sample.Size)
			})
		}
	}
	if bad.ids.Len() > 0 {
		return bad
	}
	return nil
}
