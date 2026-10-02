package dataset

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// maxFillPct is how full generation may leave a filesystem without --fill.
const maxFillPct = 95

// GenerateOptions describes a generated dataset.
type GenerateOptions struct {
	BenchDir     string
	Seed         int64
	Size         SizeSpec
	Mix          Mix
	Jobs         int
	SilesiaCache string
	Logf         func(string, ...any)
}

// GenerateRequest is the resolved shape a generate step asks for, used both
// to generate and to decide whether an existing dataset can be reused.
type GenerateRequest struct {
	Seed  int64
	Mix   Mix
	Bytes int64
	Plans []PartPlan
}

// Resolve sizes the request against the host. reclaimable is the size of a
// dataset the request would replace.
func (o GenerateOptions) Resolve(reclaimable int64) (GenerateRequest, error) {
	total, err := o.Size.Resolve(o.BenchDir, reclaimable)
	if err != nil {
		return GenerateRequest{}, err
	}
	plans, err := Plan(o.Mix, total, o.Seed)
	if err != nil {
		return GenerateRequest{}, err
	}
	return GenerateRequest{Seed: o.Seed, Mix: o.Mix, Bytes: total, Plans: plans}, nil
}

// Matches reports how an existing bench.json differs from the request; an
// empty result means it can be reused.
func (r GenerateRequest) Matches(b *BenchJSON) string {
	switch {
	case !b.Generated():
		return fmt.Sprintf("BENCH_DIR holds an imported dataset (%s)", b.In.Path)
	case b.Seed == nil || *b.Seed != r.Seed:
		return fmt.Sprintf("seed %s, requested %d", fmtPtr(b.Seed), r.Seed)
	case b.Mix == nil || *b.Mix != r.Mix.String():
		return fmt.Sprintf("mix %s, requested %s", fmtPtr(b.Mix), r.Mix)
	case b.Bytes != r.Bytes:
		return fmt.Sprintf("size %s, requested %s", encoding.HumanBytes(b.Bytes), encoding.HumanBytes(r.Bytes))
	}
	return ""
}

func fmtPtr[T any](p *T) string {
	if p == nil {
		return "null"
	}
	return fmt.Sprint(*p)
}

// NumFiles is the number of files the request generates.
func (r GenerateRequest) NumFiles() int {
	n := 0
	for _, p := range r.Plans {
		n += len(p.Sizes)
	}
	return n
}

// Generate writes the dataset under opts.BenchDir/data, replacing any
// previous one, then writes files.tsv and finally bench.json. The caller must
// already own BENCH_DIR (ClaimDir). bench.json is removed first, so an
// interrupted run leaves an incomplete dataset that the next run regenerates.
func Generate(ctx context.Context, opts GenerateOptions, req GenerateRequest) (*BenchJSON, []Entry, error) {
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if err := checkFreeSpace(opts, req.Bytes); err != nil {
		return nil, nil, err
	}

	var corpusNames []string
	for _, p := range req.Plans {
		for _, n := range p.Part.Source.CorpusNames() {
			if !containsString(corpusNames, n) {
				corpusNames = append(corpusNames, n)
			}
		}
	}
	cacheDir := opts.SilesiaCache
	if cacheDir == "" {
		cacheDir = DefaultSilesiaCache()
	}
	corpus, err := LoadCorpus(cacheDir, corpusNames, logf)
	if err != nil {
		return nil, nil, err
	}

	for _, name := range []string{BenchJSONName, FilesTSVName, InCacheName} {
		if err := os.Remove(filepath.Join(opts.BenchDir, name)); err != nil && !os.IsNotExist(err) {
			return nil, nil, err
		}
	}
	dataDir := filepath.Join(opts.BenchDir, DataDirName)
	if err := os.RemoveAll(dataDir); err != nil {
		return nil, nil, err
	}
	if err := os.Mkdir(dataDir, 0o755); err != nil {
		return nil, nil, err
	}
	for _, p := range req.Plans {
		dir := filepath.Join(dataDir, p.Dir())
		if err := os.Mkdir(dir, 0o755); err != nil {
			return nil, nil, err
		}
		if err := os.Chmod(dir, 0o755); err != nil {
			return nil, nil, err
		}
	}

	if err := writeFiles(ctx, opts, req, corpus, dataDir, logf); err != nil {
		return nil, nil, err
	}
	for _, p := range req.Plans {
		if err := fsyncPath(filepath.Join(dataDir, p.Dir())); err != nil {
			return nil, nil, err
		}
	}
	if err := fsyncPath(dataDir); err != nil {
		return nil, nil, err
	}

	walked, err := Walk(dataDir)
	if err != nil {
		return nil, nil, err
	}
	entries := Entries(walked)
	for i := range entries {
		if entries[i].Type == TypeFile {
			entries[i].Hash = hashFromName(entries[i].Path)
		}
	}

	b := &BenchJSON{
		Version:  benchJSONVer,
		Seed:     &req.Seed,
		SizeSpec: &opts.Size.Raw,
		Bytes:    req.Bytes,
		Files:    req.NumFiles(),
		Remote:   Remote{Bench: "/", Data: "/" + DataDirName},
		Created:  time.Now().UTC().Format(time.RFC3339),
	}
	mix := req.Mix.String()
	b.Mix = &mix
	for _, p := range req.Plans {
		b.Parts = append(b.Parts, PartJSON{
			Dir: p.Dir(), Source: p.Part.Source.String(), SharePct: p.Part.SharePct,
			Sizes: p.Part.Sizes(), Files: len(p.Sizes), Bytes: p.Budget,
		})
	}
	if len(corpusNames) > 0 {
		b.Silesia = corpus.Hashes()
	}
	if err := WriteDataset(opts.BenchDir, b, entries); err != nil {
		return nil, nil, err
	}
	return b, entries, nil
}

// WriteDataset writes files.tsv, fills in the fingerprint, and writes
// bench.json last, so bench.json's presence marks a complete dataset.
func WriteDataset(benchDir string, b *BenchJSON, entries []Entry) error {
	var sb strings.Builder
	if err := WriteFilesTSV(&sb, entries); err != nil {
		return err
	}
	if err := WriteFileAtomic(filepath.Join(benchDir, FilesTSVName), []byte(sb.String())); err != nil {
		return err
	}
	b.Fingerprint = Fingerprint(entries)
	return WriteJSONAtomic(filepath.Join(benchDir, BenchJSONName), b)
}

func hashFromName(path string) string {
	base := filepath.Base(path)
	parts := strings.Split(base, ".")
	if len(parts) != 3 {
		return ""
	}
	return "xxh128:" + parts[1]
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func checkFreeSpace(opts GenerateOptions, total int64) error {
	fs, err := StatFS(opts.BenchDir)
	if err != nil {
		return err
	}
	if fs.Capacity <= 0 {
		return nil
	}
	limitPct := maxFillPct
	if opts.Size.FillPct > 0 {
		limitPct = opts.Size.FillPct
	}
	var existing int64
	if b, err := ReadBenchJSON(opts.BenchDir); err == nil && b.Generated() {
		existing = b.Bytes
	}
	used := fs.Capacity - fs.Available - existing
	after := used + total
	if after*100 > fs.Capacity*int64(limitPct) {
		return fmt.Errorf("generating %s would leave the filesystem %d%% full (limit %d%%; %s free of %s)",
			encoding.HumanBytes(total), after*100/fs.Capacity, limitPct,
			encoding.HumanBytes(fs.Available+existing), encoding.HumanBytes(fs.Capacity))
	}
	return nil
}

type genJob struct {
	plan  *PartPlan
	index int
}

func writeFiles(ctx context.Context, opts GenerateOptions, req GenerateRequest, corpus *Corpus, dataDir string, logf func(string, ...any)) error {
	jobs := opts.Jobs
	if jobs <= 0 {
		jobs = runtime.NumCPU()
	}
	rings := make(map[int][]byte)
	for _, p := range req.Plans {
		if p.Part.Source.Kind == SourceSilesia {
			rings[p.Index] = corpus.Ring(p.Part.Source.CorpusNames())
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := make(chan genJob, jobs*2)
	var (
		wg        sync.WaitGroup
		errOnce   sync.Once
		firstErr  error
		doneFiles atomic.Int64
		doneBytes atomic.Int64
	)
	fail := func(err error) {
		errOnce.Do(func() { firstErr = err; cancel() })
	}
	for range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1<<20)
			for j := range work {
				if ctx.Err() != nil {
					continue
				}
				size := j.plan.Sizes[j.index]
				if err := writeOne(dataDir, j, opts.Seed, rings[j.plan.Index], buf); err != nil {
					fail(err)
					continue
				}
				doneFiles.Add(1)
				doneBytes.Add(size)
			}
		}()
	}

	totalFiles := req.NumFiles()
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-t.C:
				logf("generate: %d/%d files  %s/%s", doneFiles.Load(), totalFiles,
					encoding.HumanBytes(doneBytes.Load()), encoding.HumanBytes(req.Bytes))
			}
		}
	}()

feed:
	for i := range req.Plans {
		p := &req.Plans[i]
		for idx := range p.Sizes {
			select {
			case work <- genJob{plan: p, index: idx}:
			case <-ctx.Done():
				break feed
			}
		}
	}
	close(work)
	wg.Wait()
	close(stopProgress)
	<-progressDone
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// writeOne writes one file to a temp name while hashing it, fdatasyncs it so
// its pages are clean and evictable, and renames it to its final
// hash-carrying name.
func writeOne(dataDir string, j genJob, seed int64, ring []byte, buf []byte) error {
	dir := filepath.Join(dataDir, j.plan.Dir())
	tmpPath := filepath.Join(dir, "."+j.plan.FileStem(j.index)+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	h := xxh3.New128()
	w := io.MultiWriter(f, h)
	size := j.plan.Sizes[j.index]
	switch j.plan.Part.Source.Kind {
	case SourceRand:
		err = writeRand(w, size, chachaSeed(seed, j.plan.Index, j.index), buf)
	case SourceSilesia:
		err = writeRing(w, size, ring, ringOffset(seed, j.plan.Index, j.index, len(ring)))
	default:
		err = fmt.Errorf("unknown source %q", j.plan.Part.Source.Kind)
	}
	if err == nil {
		err = f.Chmod(0o644)
	}
	if err == nil {
		err = fdatasync(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("generate %s: %w", tmpPath, err)
	}
	final := filepath.Join(dir, j.plan.FileName(j.index, encoding.FormatXXH128HashToken(h.Sum128())))
	return os.Rename(tmpPath, final)
}

func writeRand(w io.Writer, size int64, key [32]byte, buf []byte) error {
	rng := rand.NewChaCha8(key)
	for size > 0 {
		n := int64(len(buf))
		if size < n {
			n = size
		}
		chunk := buf[:n]
		_, _ = rng.Read(chunk)
		if _, err := w.Write(chunk); err != nil {
			return err
		}
		size -= n
	}
	return nil
}

func writeRing(w io.Writer, size int64, ring []byte, off int) error {
	if len(ring) == 0 {
		return fmt.Errorf("empty silesia ring")
	}
	for size > 0 {
		chunk := ring[off:]
		if int64(len(chunk)) > size {
			chunk = chunk[:size]
		}
		if _, err := w.Write(chunk); err != nil {
			return err
		}
		size -= int64(len(chunk))
		off = 0
	}
	return nil
}

func fsyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
