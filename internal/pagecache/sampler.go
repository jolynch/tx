package pagecache

import (
	"context"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	// DefaultChunkSamplePages is the page-granularity of one verify sample.
	// 256 pages = 1 MiB at a 4 KiB page size — small enough that sampling
	// weight scales with file size on a typical manifest, large enough to
	// keep total sample count bounded. Multiple of 8 so a chunk's bitmap
	// is byte-aligned within the parent CacheEntry's bits.
	DefaultChunkSamplePages = 256

	// DefaultChunkSampleRate is the fraction of total chunks retained as a
	// verification sample. 0.10 = 10%.
	DefaultChunkSampleRate = 0.10

	// MinChunkSampleCap is a floor so very small warm sets still get a few
	// verify probes when 10% of total chunks would round to zero.
	MinChunkSampleCap = 10
)

// ChunkSample identifies a contiguous page range within one file's bitmap
// retained for post-touch verification. Entry is the same *CacheEntry that
// was yielded to TouchEntries; bitmap bytes are not copied — ReprobeChunks
// indexes into Entry.PageBits()[StartPage/8 : (StartPage+NumPages+7)/8].
type ChunkSample struct {
	Path      string
	Entry     *CacheEntry
	StartPage int
	NumPages  int
}

// ChunkSampler keeps a bounded uniform-random sample of fixed-size page
// chunks observed across TouchEntries. Big files contribute proportionally
// more chunks than small files, so the retained sample is weighted by file
// size — unlike a per-file sampler.
//
// Not safe for concurrent Observe calls — intended to be driven from the
// single-goroutine yield closure that iterates a TouchEntryFunc.
type ChunkSampler struct {
	cap        int
	chunkPages int
	seen       int64
	samples    []ChunkSample
	rng        *rand.Rand
}

// NewChunkSampler returns a sampler that retains at most maxSamples chunks
// of chunkPages pages each. maxSamples <= 0 disables sampling (Observe is a
// no-op). chunkPages <= 0 falls back to DefaultChunkSamplePages.
func NewChunkSampler(maxSamples, chunkPages int) *ChunkSampler {
	if maxSamples <= 0 {
		return &ChunkSampler{cap: 0}
	}
	if chunkPages <= 0 {
		chunkPages = DefaultChunkSamplePages
	}
	return &ChunkSampler{
		cap:        maxSamples,
		chunkPages: chunkPages,
		samples:    make([]ChunkSample, 0, maxSamples),
		rng:        rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
}

// Observe folds each chunkPages-sized chunk of e.Entry's bitmap into the
// reservoir. A file with N pages contributes ceil(N / chunkPages) chunks,
// so the retained sample is weighted by file size.
func (s *ChunkSampler) Observe(e TouchEntry) {
	if s.cap == 0 || e.Entry == nil || e.Entry.Empty() {
		return
	}
	_, numPages := e.Entry.PageBits()
	for start := 0; start < numPages; start += s.chunkPages {
		end := start + s.chunkPages
		if end > numPages {
			end = numPages
		}
		s.fold(ChunkSample{
			Path:      e.Path,
			Entry:     e.Entry,
			StartPage: start,
			NumPages:  end - start,
		})
	}
}

// fold runs one step of algorithm-R reservoir sampling.
func (s *ChunkSampler) fold(c ChunkSample) {
	s.seen++
	if len(s.samples) < s.cap {
		s.samples = append(s.samples, c)
		return
	}
	j := s.rng.Int64N(s.seen)
	if j < int64(s.cap) {
		s.samples[j] = c
	}
}

// Samples returns the retained sample. The caller must not mutate it.
func (s *ChunkSampler) Samples() []ChunkSample {
	return s.samples
}

// ChunkProbeResult summarizes a ReprobeChunks call.
type ChunkProbeResult struct {
	// SampledChunks is the number of chunks actually probed (i.e. mincore
	// returned without error and the expected sub-bitmap was non-empty).
	SampledChunks int
	// PlannedChunks is len(samples) — the size of the input to ReprobeChunks.
	PlannedChunks int
	// SampledFiles is the count of distinct paths covered by SampledChunks.
	SampledFiles int
	// ExpectedPages sums popcount of the expected sub-bitmap across each
	// probed chunk — pages the caller asked the kernel to warm.
	ExpectedPages int64
	// HonoredPages sums popcount of (expected AND fresh) across each probed
	// chunk. HonoredPages / ExpectedPages is the kernel honor rate.
	HonoredPages int64
	// Partial is true if ctx fired before all samples were probed.
	Partial bool
}

// ReprobeChunks issues a partial mincore probe for each sample's
// [StartPage, StartPage+NumPages) range, intersects the result with the
// matching sub-range of the sample's stored bitmap, and returns aggregate
// counts. Per-chunk errors are dropped (advisory). Honors ctx.Done()
// between samples; on early exit returns Partial=true with the counts
// gathered so far.
func ReprobeChunks(ctx context.Context, samples []ChunkSample) ChunkProbeResult {
	r := ChunkProbeResult{PlannedChunks: len(samples)}
	if len(samples) == 0 {
		return r
	}
	seenFiles := make(map[string]struct{}, len(samples))
	for _, s := range samples {
		if ctx != nil {
			select {
			case <-ctx.Done():
				r.Partial = true
				return r
			default:
			}
		}
		if s.Entry == nil || s.NumPages <= 0 {
			continue
		}
		expectedBits, entryPages := s.Entry.PageBits()
		if s.StartPage >= entryPages {
			continue
		}
		end := s.StartPage + s.NumPages
		if end > entryPages {
			end = entryPages
		}
		startByte := s.StartPage / 8
		endByte := (end + 7) / 8
		if startByte >= len(expectedBits) {
			continue
		}
		if endByte > len(expectedBits) {
			endByte = len(expectedBits)
		}
		expectedSlice := expectedBits[startByte:endByte]
		if s.StartPage%8 != 0 {
			// Defensive — chunkPages is always a multiple of 8 with the
			// default constant, but a custom value could break alignment.
			// Skip rather than mis-attribute.
			continue
		}

		var expectedThis int64
		for _, b := range expectedSlice {
			expectedThis += int64(bits.OnesCount8(b))
		}
		if expectedThis == 0 {
			continue
		}

		freshBits, _, err := loadResidencyRangeFn(s.Path, s.StartPage, end-s.StartPage)
		if err != nil {
			// Count the chunk's expected pages so the honor-rate denominator
			// reflects pages we asked for, even if we couldn't verify them.
			r.SampledChunks++
			r.ExpectedPages += expectedThis
			if _, dup := seenFiles[s.Path]; !dup {
				seenFiles[s.Path] = struct{}{}
				r.SampledFiles++
			}
			continue
		}

		r.SampledChunks++
		r.ExpectedPages += expectedThis
		if _, dup := seenFiles[s.Path]; !dup {
			seenFiles[s.Path] = struct{}{}
			r.SampledFiles++
		}

		common := len(expectedSlice)
		if len(freshBits) < common {
			common = len(freshBits)
		}
		for i := 0; i < common; i++ {
			r.HonoredPages += int64(bits.OnesCount8(expectedSlice[i] & freshBits[i]))
		}
	}
	return r
}

// EvictPathFunc yields local paths to be FADV_DONTNEED'd. Iterator-shaped so
// callers don't have to materialize a path slice for the whole manifest.
type EvictPathFunc func(yield func(path string) bool)

// EvictResult summarizes one EvictPaths call.
type EvictResult struct {
	// Evicted is the number of paths for which fadvise(DONTNEED) returned
	// without error.
	Evicted int64
	// Errors is the number of paths that either failed to open or whose
	// fadvise call returned a non-nil errno.
	Errors int64
}

// EvictPaths issues posix_fadvise(FADV_DONTNEED, 0, 0) for each yielded path
// using a worker pool. Per-file errors are counted but otherwise dropped —
// the operation is advisory. Returns once the iterator is exhausted and all
// workers drain, or once ctx is canceled (in which case ctx.Err() is
// returned along with whatever counts accumulated).
//
// Used to release page-cache state on a receiver after writes so a
// subsequent WILLNEED warmup reproduces the sender's snapshot rather than
// layering it on top of whatever the writer left in cache.
//
// On platforms where TouchSupported is false this is a no-op that returns
// (EvictResult{}, nil) without consuming paths.
//
// parallelism <= 0 defaults to runtime.NumCPU() * 4, matching TouchEntries.
func EvictPaths(ctx context.Context, paths EvictPathFunc, parallelism int) (EvictResult, error) {
	if paths == nil {
		return EvictResult{}, nil
	}
	if !TouchSupported() {
		return EvictResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return EvictResult{}, err
	}
	if parallelism <= 0 {
		parallelism = runtime.NumCPU() * 4
	}

	jobs := make(chan string, parallelism*2)
	var evicted, errs atomic.Int64
	var wg sync.WaitGroup
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for p := range jobs {
				if err := evictPagesFn(p); err != nil {
					errs.Add(1)
					continue
				}
				evicted.Add(1)
			}
		}()
	}

	var stopErr error
	paths(func(p string) bool {
		select {
		case <-ctx.Done():
			stopErr = ctx.Err()
			return false
		case jobs <- p:
			return true
		}
	})
	close(jobs)
	wg.Wait()
	return EvictResult{
		Evicted: evicted.Load(),
		Errors:  errs.Load(),
	}, stopErr
}
