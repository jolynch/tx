package pagecache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func withStubLoadRange(t *testing.T, fn func(string, int, int) ([]byte, int, error)) {
	t.Helper()
	prev := loadResidencyRangeFn
	loadResidencyRangeFn = fn
	t.Cleanup(func() { loadResidencyRangeFn = prev })
}

func withStubEvict(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := evictPagesFn
	prevSupported := touchSupportedFn
	evictPagesFn = fn
	touchSupportedFn = func() bool { return true }
	t.Cleanup(func() {
		evictPagesFn = prev
		touchSupportedFn = prevSupported
	})
}

// makeEntry returns a CacheEntry whose bitmap has all bits in [0, numPages) set.
func makeEntry(t *testing.T, numPages int) *CacheEntry {
	t.Helper()
	bits := make([]byte, (numPages+7)/8)
	for i := 0; i < numPages; i++ {
		bits[i/8] |= 1 << uint(i%8)
	}
	ce := &CacheEntry{}
	if err := ce.SetPageBits(bits, numPages); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	return ce
}

func TestChunkSamplerObservePartitionsEntry(t *testing.T) {
	// 1000-page entry at chunkPages=256 should produce 4 chunks: 256/256/256/232.
	s := NewChunkSampler(1000, 256)
	s.Observe(TouchEntry{Path: "x", Entry: makeEntry(t, 1000)})
	got := s.Samples()
	if len(got) != 4 {
		t.Fatalf("len(samples) = %d, want 4", len(got))
	}
	wantNumPages := []int{256, 256, 256, 232}
	wantStart := []int{0, 256, 512, 768}
	for i, c := range got {
		if c.StartPage != wantStart[i] {
			t.Fatalf("samples[%d].StartPage = %d, want %d", i, c.StartPage, wantStart[i])
		}
		if c.NumPages != wantNumPages[i] {
			t.Fatalf("samples[%d].NumPages = %d, want %d", i, c.NumPages, wantNumPages[i])
		}
		if c.Path != "x" {
			t.Fatalf("samples[%d].Path = %q, want \"x\"", i, c.Path)
		}
	}
}

func TestChunkSamplerCapsAtMax(t *testing.T) {
	// 1000 chunks streamed (1000 pages × ~1 page chunks) cap at 50.
	s := NewChunkSampler(50, 1)
	s.Observe(TouchEntry{Path: "x", Entry: makeEntry(t, 1000)})
	got := s.Samples()
	if len(got) != 50 {
		t.Fatalf("len(samples) = %d, want 50", len(got))
	}
	seen := make(map[int]int)
	for _, c := range got {
		seen[c.StartPage]++
	}
	for k, v := range seen {
		if v != 1 {
			t.Fatalf("StartPage %d appears %d times, want 1", k, v)
		}
	}
}

func TestChunkSamplerZeroCapNoOp(t *testing.T) {
	s := NewChunkSampler(0, 256)
	s.Observe(TouchEntry{Path: "x", Entry: makeEntry(t, 1000)})
	if got := s.Samples(); len(got) != 0 {
		t.Fatalf("len(samples) = %d, want 0", len(got))
	}
}

func TestChunkSamplerSkipsEmptyEntry(t *testing.T) {
	s := NewChunkSampler(10, 256)
	s.Observe(TouchEntry{Path: "x", Entry: nil})
	s.Observe(TouchEntry{Path: "y", Entry: &CacheEntry{}})
	if got := s.Samples(); len(got) != 0 {
		t.Fatalf("len(samples) = %d, want 0", len(got))
	}
}

func TestChunkSamplerCoversFullRangeAcrossTrials(t *testing.T) {
	const (
		streamPages = 100 // 100 single-page chunks per Observe
		cap         = 10
		trials      = 200
	)
	hit := make(map[int]int, streamPages)
	for trial := 0; trial < trials; trial++ {
		s := NewChunkSampler(cap, 1)
		s.Observe(TouchEntry{Path: "x", Entry: makeEntry(t, streamPages)})
		for _, c := range s.Samples() {
			hit[c.StartPage]++
		}
	}
	// Expected hits per chunk: trials*cap/streamPages = 200*10/100 = 20.
	// A floor of 1 across 200 trials is near-certain.
	for i := 0; i < streamPages; i++ {
		if hit[i] == 0 {
			t.Fatalf("chunk at StartPage=%d never appeared in %d trials", i, trials)
		}
	}
}

func TestReprobeChunksEmpty(t *testing.T) {
	got := ReprobeChunks(context.Background(), nil)
	if got != (ChunkProbeResult{}) {
		t.Fatalf("ReprobeChunks(nil) = %+v, want zero", got)
	}
}

func TestReprobeChunksFullHonor(t *testing.T) {
	entry := makeEntry(t, 16) // two full bytes set
	withStubLoadRange(t, func(_ string, start, num int) ([]byte, int, error) {
		if start != 0 || num != 16 {
			t.Fatalf("loadResidencyRange called with start=%d num=%d, want 0,16", start, num)
		}
		return []byte{0xff, 0xff}, 16, nil
	})
	probe := ReprobeChunks(context.Background(), []ChunkSample{
		{Path: "x", Entry: entry, StartPage: 0, NumPages: 16},
	})
	if probe.SampledChunks != 1 {
		t.Fatalf("SampledChunks = %d, want 1", probe.SampledChunks)
	}
	if probe.SampledFiles != 1 {
		t.Fatalf("SampledFiles = %d, want 1", probe.SampledFiles)
	}
	if probe.PlannedChunks != 1 {
		t.Fatalf("PlannedChunks = %d, want 1", probe.PlannedChunks)
	}
	if probe.ExpectedPages != 16 {
		t.Fatalf("ExpectedPages = %d, want 16", probe.ExpectedPages)
	}
	if probe.HonoredPages != 16 {
		t.Fatalf("HonoredPages = %d, want 16", probe.HonoredPages)
	}
	if probe.Partial {
		t.Fatal("Partial should be false")
	}
}

func TestReprobeChunksPartialHonor(t *testing.T) {
	entry := makeEntry(t, 16) // 0xff 0xff
	withStubLoadRange(t, func(_ string, _, _ int) ([]byte, int, error) {
		// Kernel evicted half the pages.
		return []byte{0xaa, 0xaa}, 16, nil
	})
	probe := ReprobeChunks(context.Background(), []ChunkSample{
		{Path: "x", Entry: entry, StartPage: 0, NumPages: 16},
	})
	if probe.ExpectedPages != 16 {
		t.Fatalf("ExpectedPages = %d, want 16", probe.ExpectedPages)
	}
	if probe.HonoredPages != 8 {
		t.Fatalf("HonoredPages = %d, want 8", probe.HonoredPages)
	}
}

func TestReprobeChunksLoadError(t *testing.T) {
	entry := makeEntry(t, 8)
	withStubLoadRange(t, func(_ string, _, _ int) ([]byte, int, error) {
		return nil, 0, errors.New("boom")
	})
	probe := ReprobeChunks(context.Background(), []ChunkSample{
		{Path: "x", Entry: entry, StartPage: 0, NumPages: 8},
	})
	// Chunk counts toward Sampled/Expected even when Load fails: the user
	// asked for these pages; we just couldn't verify them. Honored stays 0.
	if probe.SampledChunks != 1 {
		t.Fatalf("SampledChunks = %d, want 1", probe.SampledChunks)
	}
	if probe.ExpectedPages != 8 {
		t.Fatalf("ExpectedPages = %d, want 8", probe.ExpectedPages)
	}
	if probe.HonoredPages != 0 {
		t.Fatalf("HonoredPages = %d, want 0", probe.HonoredPages)
	}
}

func TestReprobeChunksMidFileSlice(t *testing.T) {
	// 24-page entry; sample only pages [8, 16). expectedSlice should be
	// the middle byte (all set in our makeEntry). Stub returns the same
	// "all resident" sub-bitmap.
	entry := makeEntry(t, 24)
	var gotStart, gotNum int
	withStubLoadRange(t, func(_ string, start, num int) ([]byte, int, error) {
		gotStart, gotNum = start, num
		return []byte{0xff}, num, nil
	})
	probe := ReprobeChunks(context.Background(), []ChunkSample{
		{Path: "x", Entry: entry, StartPage: 8, NumPages: 8},
	})
	if gotStart != 8 || gotNum != 8 {
		t.Fatalf("loadResidencyRange called with start=%d num=%d, want 8,8", gotStart, gotNum)
	}
	if probe.ExpectedPages != 8 {
		t.Fatalf("ExpectedPages = %d, want 8", probe.ExpectedPages)
	}
	if probe.HonoredPages != 8 {
		t.Fatalf("HonoredPages = %d, want 8", probe.HonoredPages)
	}
}

func TestReprobeChunksCanceledMidway(t *testing.T) {
	entry := makeEntry(t, 8)
	// Stub a slow Load so the ctx deadline triggers between samples.
	withStubLoadRange(t, func(_ string, _, _ int) ([]byte, int, error) {
		time.Sleep(20 * time.Millisecond)
		return []byte{0xff}, 8, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	samples := make([]ChunkSample, 10)
	for i := range samples {
		samples[i] = ChunkSample{Path: "x", Entry: entry, StartPage: 0, NumPages: 8}
	}
	probe := ReprobeChunks(ctx, samples)
	if !probe.Partial {
		t.Fatal("Partial should be true after ctx expired")
	}
	if probe.SampledChunks >= probe.PlannedChunks {
		t.Fatalf("SampledChunks=%d >= PlannedChunks=%d, expected early exit",
			probe.SampledChunks, probe.PlannedChunks)
	}
}

func TestEvictPathsHappyPath(t *testing.T) {
	var got []string
	withStubEvict(t, func(p string) error {
		got = append(got, p)
		return nil
	})
	result, err := EvictPaths(context.Background(), func(yield func(string) bool) {
		for _, p := range []string{"/a", "/b", "/c"} {
			if !yield(p) {
				return
			}
		}
	}, 1) // parallelism=1 so order is deterministic
	if err != nil {
		t.Fatalf("EvictPaths: %v", err)
	}
	if result.Evicted != 3 {
		t.Fatalf("Evicted = %d, want 3", result.Evicted)
	}
	if result.Errors != 0 {
		t.Fatalf("Errors = %d, want 0", result.Errors)
	}
	if len(got) != 3 {
		t.Fatalf("evictPagesFn called %d times, want 3", len(got))
	}
}

func TestEvictPathsCountsErrors(t *testing.T) {
	withStubEvict(t, func(p string) error {
		if p == "/bad" {
			return errors.New("denied")
		}
		return nil
	})
	result, err := EvictPaths(context.Background(), func(yield func(string) bool) {
		for _, p := range []string{"/ok1", "/bad", "/ok2"} {
			if !yield(p) {
				return
			}
		}
	}, 1)
	if err != nil {
		t.Fatalf("EvictPaths: %v", err)
	}
	if result.Evicted != 2 {
		t.Fatalf("Evicted = %d, want 2", result.Evicted)
	}
	if result.Errors != 1 {
		t.Fatalf("Errors = %d, want 1", result.Errors)
	}
}

func TestEvictPathsCanceledContext(t *testing.T) {
	withStubEvict(t, func(string) error {
		// Sleep so the iterator-side ctx check fires before we consume.
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := EvictPaths(ctx, func(yield func(string) bool) {
		yield("/a")
	}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEvictPathsNoOpWhenUnsupported(t *testing.T) {
	withStubEvict(t, func(string) error {
		t.Fatalf("evictPagesFn should not be called when TouchSupported=false")
		return nil
	})
	prev := touchSupportedFn
	touchSupportedFn = func() bool { return false }
	t.Cleanup(func() { touchSupportedFn = prev })

	result, err := EvictPaths(context.Background(), func(yield func(string) bool) {
		yield("/a")
	}, 1)
	if err != nil {
		t.Fatalf("EvictPaths: %v", err)
	}
	if result.Evicted != 0 || result.Errors != 0 {
		t.Fatalf("result = %+v, want zero", result)
	}
}

func TestReprobeChunksMisalignedStartSkipped(t *testing.T) {
	// StartPage not aligned to a byte → defensive skip.
	entry := makeEntry(t, 16)
	called := false
	withStubLoadRange(t, func(_ string, _, _ int) ([]byte, int, error) {
		called = true
		return nil, 0, nil
	})
	probe := ReprobeChunks(context.Background(), []ChunkSample{
		{Path: "x", Entry: entry, StartPage: 3, NumPages: 8}, // 3 % 8 != 0
	})
	if called {
		t.Fatal("loadResidencyRange should not be called on misaligned chunk")
	}
	if probe.SampledChunks != 0 {
		t.Fatalf("SampledChunks = %d, want 0", probe.SampledChunks)
	}
}
