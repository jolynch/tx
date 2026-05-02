package sampler

import (
	"slices"
	"testing"
)

const testFrameSize int64 = 4 * 1024 * 1024
const testSampleBytes int64 = 8
const testMaxFileSize = uint64(1 << 40)

func collectSamples(gen Generator, limit int) []Sample {
	if limit <= 0 || int64(limit) > gen.TotalSamples() {
		limit = int(gen.TotalSamples())
	}
	out := make([]Sample, 0, limit)
	for len(out) < limit {
		sample, ok := gen.Peek()
		if !ok {
			break
		}
		out = append(out, sample)
		gen.Advance()
	}
	return out
}

func sampleSlots(samples []Sample) []int64 {
	out := make([]int64, 0, len(samples))
	for _, sample := range samples {
		out = append(out, sample.Offset/testFrameSize)
	}
	return out
}

func TestGeneratorDeterministic(t *testing.T) {
	genA, ok := New("/remote", "same.bin", 7, 64*testFrameSize, 25, testFrameSize, testSampleBytes)
	if !ok {
		t.Fatal("expected generator")
	}
	genB, ok := New("/remote", "same.bin", 7, 64*testFrameSize, 25, testFrameSize, testSampleBytes)
	if !ok {
		t.Fatal("expected second generator")
	}
	samplesA := collectSamples(genA, 0)
	samplesB := collectSamples(genB, 0)
	if !slices.Equal(samplesA, samplesB) {
		t.Fatalf("expected deterministic samples, got %v vs %v", samplesA[:min(len(samplesA), 4)], samplesB[:min(len(samplesB), 4)])
	}

	genC, ok := New("/remote", "other.bin", 7, 64*testFrameSize, 25, testFrameSize, testSampleBytes)
	if !ok {
		t.Fatal("expected different generator")
	}
	if slices.Equal(samplesA, collectSamples(genC, 0)) {
		t.Fatal("expected different file identity to change sample layout")
	}
}

func TestGeneratorCountsAndOrdering(t *testing.T) {
	tests := []struct {
		pct  int
		want int64
	}{
		{pct: 5, want: 1},
		{pct: 10, want: 2},
		{pct: 33, want: 7},
		{pct: 100, want: 20},
	}
	for _, tt := range tests {
		gen, ok := New("/remote", "counts.bin", 9, 20*testFrameSize, tt.pct, testFrameSize, testSampleBytes)
		if !ok {
			t.Fatalf("pct=%d: expected generator", tt.pct)
		}
		if got := gen.TotalSamples(); got != tt.want {
			t.Fatalf("pct=%d: TotalSamples() = %d, want %d", tt.pct, got, tt.want)
		}
		slots := sampleSlots(collectSamples(gen, 0))
		seen := make(map[int64]struct{}, len(slots))
		for i, slot := range slots {
			if _, ok := seen[slot]; ok {
				t.Fatalf("pct=%d: duplicate slot %d in %v", tt.pct, slot, slots)
			}
			seen[slot] = struct{}{}
			if tt.pct < 100 && i > 0 && slots[i-1] >= slot {
				t.Fatalf("pct=%d: expected ascending slots, got %v", tt.pct, slots)
			}
		}
	}
}

func TestGeneratorFullCoveragePermutation(t *testing.T) {
	var gen Generator
	found := false
	for id := uint64(1); id < 64; id++ {
		candidate, ok := New("/remote", "full.bin", id, 16*testFrameSize, 100, testFrameSize, testSampleBytes)
		if !ok {
			t.Fatal("expected generator")
		}
		slots := sampleSlots(collectSamples(candidate, 4))
		if !slices.Equal(slots, []int64{0, 1, 2, 3}) {
			gen = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected a non-trivial full-coverage permutation seed")
	}
	slots := sampleSlots(collectSamples(gen, 0))
	if len(slots) != 16 {
		t.Fatalf("expected 16 slots, got %d", len(slots))
	}
	for want := int64(0); want < 16; want++ {
		if !slices.Contains(slots, want) {
			t.Fatalf("missing slot %d in %v", want, slots)
		}
	}
	if slices.Equal(slots[:4], []int64{0, 1, 2, 3}) {
		t.Fatalf("expected full coverage to avoid offset-0-first order, got %v", slots[:4])
	}
}

func TestGeneratorHugeFile(t *testing.T) {
	size := int64(256) << 40
	gen, ok := New("/remote", "huge.bin", 11, size, 100, testFrameSize, testSampleBytes)
	if !ok {
		t.Fatal("expected generator")
	}
	wantSlots := (size + testFrameSize - 1) / testFrameSize
	if got := gen.TotalSamples(); got != wantSlots {
		t.Fatalf("TotalSamples() = %d, want %d", got, wantSlots)
	}
	if samples := collectSamples(gen, 4); len(samples) != 4 {
		t.Fatalf("expected 4 samples, got %d", len(samples))
	}
}

func FuzzGeneratorFullCoverageNoRepeats(f *testing.F) {
	f.Add("same.bin", uint64(1))
	f.Add("tiny.bin", uint64(testSampleBytes))
	f.Add("frame.bin", uint64(testFrameSize))
	f.Add("multi-frame.bin", uint64(17*testFrameSize+123))
	f.Add("near-tib.bin", testMaxFileSize-1)

	f.Fuzz(func(t *testing.T, name string, rawSize uint64) {
		size := int64(rawSize%(testMaxFileSize-1)) + 1
		gen, ok := New("/remote", name, 7, size, 100, testFrameSize, testSampleBytes)
		if !ok {
			t.Fatalf("expected generator for size=%d name=%q", size, name)
		}

		frameSlots := int((size + testFrameSize - 1) / testFrameSize)
		if got := gen.TotalSamples(); got != int64(frameSlots) {
			t.Fatalf("TotalSamples() = %d, want %d", got, frameSlots)
		}

		seen := make([]bool, frameSlots)
		count := 0
		for {
			sample, ok := gen.Peek()
			if !ok {
				break
			}
			if sample.Offset < 0 || sample.Offset >= size {
				t.Fatalf("sample offset out of range: offset=%d size=%d", sample.Offset, size)
			}
			if sample.Size <= 0 || sample.Size > testSampleBytes {
				t.Fatalf("sample size out of range: got %d", sample.Size)
			}
			if sample.Offset+sample.Size > size {
				t.Fatalf("sample extends past file end: offset=%d size=%d fileSize=%d", sample.Offset, sample.Size, size)
			}

			slot := int(sample.Offset / testFrameSize)
			if slot < 0 || slot >= frameSlots {
				t.Fatalf("slot out of range: slot=%d frameSlots=%d", slot, frameSlots)
			}
			if seen[slot] {
				t.Fatalf("duplicate slot %d for size=%d name=%q", slot, size, name)
			}
			seen[slot] = true
			count++
			gen.Advance()
		}

		if count != frameSlots {
			t.Fatalf("emitted %d samples, want %d", count, frameSlots)
		}
		for slot, ok := range seen {
			if !ok {
				t.Fatalf("missing slot %d for size=%d name=%q", slot, size, name)
			}
		}
	})
}
