package sampler

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"testing"
)

const testFrameSize int64 = 4 * 1024 * 1024

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

// newSingle plans one file on its own at pct.
func newSingle(root, path string, fileID uint64, fileSize int64, pct int, frameSize int64) (Generator, bool) {
	return NewPlan(PlanOptions{Root: root, Pct: pct, FrameSize: frameSize, TotalSlots: SlotCount(fileSize, frameSize)}).Next(path, fileID, fileSize)
}

func sampleSlots(samples []Sample) []int64 {
	out := make([]int64, 0, len(samples))
	for _, sample := range samples {
		out = append(out, sample.Offset/testFrameSize)
	}
	return out
}

func TestGeneratorDeterministic(t *testing.T) {
	genA, ok := newSingle("/remote", "same.bin", 7, 64*testFrameSize, 25, testFrameSize)
	if !ok {
		t.Fatal("expected generator")
	}
	genB, ok := newSingle("/remote", "same.bin", 7, 64*testFrameSize, 25, testFrameSize)
	if !ok {
		t.Fatal("expected second generator")
	}
	samplesA := collectSamples(genA, 0)
	samplesB := collectSamples(genB, 0)
	if !slices.Equal(samplesA, samplesB) {
		t.Fatalf("expected deterministic samples, got %v vs %v", samplesA[:min(len(samplesA), 4)], samplesB[:min(len(samplesB), 4)])
	}

	genC, ok := newSingle("/remote", "other.bin", 7, 64*testFrameSize, 25, testFrameSize)
	if !ok {
		t.Fatal("expected different generator")
	}
	if slices.Equal(samplesA, collectSamples(genC, 0)) {
		t.Fatal("expected different file identity to change sample layout")
	}
}

func TestGeneratorHugeFile(t *testing.T) {
	size := int64(256) << 40
	gen, ok := newSingle("/remote", "huge.bin", 11, size, 100, testFrameSize)
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

// treePlan runs a plan over files of the given sizes and returns each file's
// samples, drained through Peek and Advance.
func treePlan(t testing.TB, sizes []int64, pct int, frameSize int64, seed uint64, sequential bool) [][]Sample {
	t.Helper()
	var slots int64
	for _, size := range sizes {
		slots += SlotCount(size, frameSize)
	}
	plan := NewPlan(PlanOptions{Root: "/remote", Pct: pct, FrameSize: frameSize, TotalSlots: slots, Seed: seed, Sequential: sequential})
	out := make([][]Sample, len(sizes))
	for i, size := range sizes {
		gen, ok := plan.Next(fmt.Sprintf("f%d.bin", i), uint64(i+1), size)
		if !ok {
			continue
		}
		for {
			sample, ok := gen.Peek()
			if !ok {
				break
			}
			out[i] = append(out[i], sample)
			gen.Advance()
		}
	}
	return out
}

// TestPlanRoundingIsUnbiased checks that systematic rounding gives every slot
// the same chance: 20 one-slot files at 5% draw exactly one sample, and each
// file is that sample under some seed. Always rounding down, or ignoring the
// seed's start, would pin the sample to one file.
func TestPlanRoundingIsUnbiased(t *testing.T) {
	sizes := make([]int64, 20)
	for i := range sizes {
		sizes[i] = 1000
	}
	picked := map[int]bool{}
	for seed := uint64(0); seed < 200; seed++ {
		total := 0
		for i, samples := range treePlan(t, sizes, 5, testFrameSize, seed, false) {
			total += len(samples)
			if len(samples) > 0 {
				picked[i] = true
			}
		}
		if total != 1 {
			t.Fatalf("seed %d: sampled %d of 20 one-slot files at 5%%, want 1", seed, total)
		}
	}
	if len(picked) != len(sizes) {
		t.Fatalf("only files %v were ever sampled, want all %d", picked, len(sizes))
	}
}

// TestPlanSamplesOneSlotOfTinyTree checks that a tree whose share rounds to
// zero still gets one slot, and that the seed spreads that slot over the tree.
func TestPlanSamplesOneSlotOfTinyTree(t *testing.T) {
	sizes := []int64{10, 20, 30}
	picked := map[int]bool{}
	for seed := uint64(0); seed < 200; seed++ {
		total := 0
		for i, samples := range treePlan(t, sizes, 1, testFrameSize, seed, false) {
			total += len(samples)
			if len(samples) > 0 {
				picked[i] = true
			}
		}
		if total != 1 {
			t.Fatalf("seed %d: sampled %d slots, want 1", seed, total)
		}
	}
	if len(picked) != 3 {
		t.Fatalf("the single slot always came from files %v, want a spread over all three", picked)
	}
	// Most seeds round to zero here, so the plan forces a slot. The seed
	// alone must spread those forced slots over the tree.
	forced := map[int64]bool{}
	for seed := uint64(0); seed < 200; seed++ {
		if f := NewPlan(PlanOptions{Root: "/remote", Pct: 1, FrameSize: testFrameSize, TotalSlots: 3, Seed: seed}).forced; f >= 0 {
			forced[f] = true
		}
	}
	if len(forced) != 3 {
		t.Fatalf("forced slots were %v across 200 seeds, want all of 0, 1 and 2", forced)
	}
}

// TestPlanSeedChangesSamples draws 30 of one file's 100 slots, a count that
// does not depend on rounding, so only the seed can change which ones.
func TestPlanSeedChangesSamples(t *testing.T) {
	sizes := []int64{100 * testFrameSize}
	draw := func(seed uint64) []int64 {
		samples := treePlan(t, sizes, 30, testFrameSize, seed, false)[0]
		if len(samples) != 30 {
			t.Fatalf("seed %d drew %d slots, want 30", seed, len(samples))
		}
		return sampleSlots(samples)
	}
	base := draw(1)
	if !slices.Equal(base, draw(1)) {
		t.Fatal("same seed gave a different plan")
	}
	distinct := 0
	for seed := uint64(2); seed < 12; seed++ {
		if !slices.Equal(base, draw(seed)) {
			distinct++
		}
	}
	if distinct < 9 {
		t.Fatalf("only %d of 10 other seeds changed the slots drawn", distinct)
	}
}

func FuzzGeneratorSlots(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint8(100), uint8(22))
	f.Add(uint64(2), uint64(0), uint8(5), uint8(22))
	f.Add(uint64(3), uint64(99), uint8(33), uint8(22))
	f.Add(uint64(4), uint64(7), uint8(100), uint8(4))
	f.Add(uint64(5), uint64(8), uint8(1), uint8(10))

	f.Fuzz(func(t *testing.T, shape uint64, seed uint64, rawPct uint8, frameShift uint8) {
		// Up to 4 MiB frames and 2^18 slots per file keep one input fast.
		frameSize := int64(1) << (frameShift % 23)
		rng := rand.New(rand.NewPCG(shape, seed))
		sizes := make([]int64, 1+rng.IntN(12))
		var totalSlots int64
		for i := range sizes {
			switch rng.IntN(4) {
			case 0:
				sizes[i] = 0
			case 1:
				sizes[i] = 1 + rng.Int64N(frameSize)
			default:
				sizes[i] = 1 + rng.Int64N(frameSize<<(1+rng.IntN(12)))
			}
			totalSlots += SlotCount(sizes[i], frameSize)
		}
		pct := int(rawPct%100) + 1
		plans := treePlan(t, sizes, pct, frameSize, seed, false)

		var total int64
		for i, samples := range plans {
			slots := SlotCount(sizes[i], frameSize)
			if int64(len(samples)) > slots {
				t.Fatalf("file %d drew %d samples from %d slots", i, len(samples), slots)
			}
			total += int64(len(samples))
			seen := make(map[int64]bool, len(samples))
			var covered int64
			for j, s := range samples {
				slot := s.Offset / frameSize
				if s.Offset%frameSize != 0 || slot >= slots || s.Size != min(frameSize, sizes[i]-s.Offset) {
					t.Fatalf("sample %+v is not a whole slot of a %d-byte file with %d-byte frames", s, sizes[i], frameSize)
				}
				if seen[slot] {
					t.Fatalf("file %d: duplicate slot %d", i, slot)
				}
				seen[slot] = true
				covered += s.Size
				if int64(len(samples)) < slots && j > 0 && samples[j-1].Offset >= s.Offset {
					t.Fatalf("partial samples out of order: %+v then %+v", samples[j-1], s)
				}
			}
			if pct == 100 && covered != sizes[i] {
				t.Fatalf("100%% covered %d of %d bytes of file %d", covered, sizes[i], i)
			}
		}

		// The total is floor or ceil of the exact share, and at least one
		// slot for any tree with data.
		floor, rem := totalSlots*int64(pct)/100, totalSlots*int64(pct)%100
		switch {
		case totalSlots == 0:
			if total != 0 {
				t.Fatalf("empty tree drew %d samples", total)
			}
		case floor == 0:
			if total != 1 {
				t.Fatalf("%d slots at %d%% drew %d samples, want 1", totalSlots, pct, total)
			}
		case total != floor && (rem == 0 || total != floor+1):
			t.Fatalf("%d slots at %d%% drew %d samples, want %d or %d", totalSlots, pct, total, floor, floor+1)
		}

		if !reflect.DeepEqual(plans, treePlan(t, sizes, pct, frameSize, seed, false)) {
			t.Fatal("same seed gave a different plan")
		}

		// Sequential changes only the order of a file's slots, never which
		// ones it draws, and always ascends.
		for i, samples := range treePlan(t, sizes, pct, frameSize, seed, true) {
			if !sort.SliceIsSorted(samples, func(a, b int) bool { return samples[a].Offset < samples[b].Offset }) {
				t.Fatalf("sequential plan for file %d is not ascending: %v", i, samples)
			}
			want := slices.Clone(plans[i])
			sort.Slice(want, func(a, b int) bool { return want[a].Offset < want[b].Offset })
			if len(want) != len(samples) || (len(want) > 0 && !slices.Equal(want, samples) && int64(len(want)) == SlotCount(sizes[i], frameSize)) {
				t.Fatalf("sequential plan for file %d drew %v, want the slots of %v", i, samples, want)
			}
		}
	})
}

// TestPlanSequentialFullIsAscending checks that a plan with no time budget
// reads every slot of every file in order, while the same plan without
// Sequential still permutes at least one file.
func TestPlanSequentialFullIsAscending(t *testing.T) {
	sizes := []int64{1, 40 * testFrameSize, 17*testFrameSize + 5, 64 * testFrameSize}
	var slots int64
	for _, size := range sizes {
		slots += SlotCount(size, testFrameSize)
	}
	permuted := false
	for seed := uint64(0); seed < 8; seed++ {
		for _, sequential := range []bool{true, false} {
			plan := NewPlan(PlanOptions{Root: "/r", Pct: 100, FrameSize: testFrameSize, TotalSlots: slots, Seed: seed, Sequential: sequential})
			for i, size := range sizes {
				gen, ok := plan.Next(fmt.Sprintf("f%d", i), uint64(i+1), size)
				if !ok {
					t.Fatalf("file %d drew no samples at 100%%", i)
				}
				got := sampleSlots(collectSamples(gen, 0))
				if int64(len(got)) != SlotCount(size, testFrameSize) {
					t.Fatalf("file %d drew %d slots, want %d", i, len(got), SlotCount(size, testFrameSize))
				}
				ascending := sort.SliceIsSorted(got, func(a, b int) bool { return got[a] < got[b] })
				if sequential {
					for j, slot := range got {
						if slot != int64(j) {
							t.Fatalf("seed %d file %d: sequential slots %v, want 0..n", seed, i, got)
						}
					}
				} else if !ascending {
					permuted = true
				}
			}
		}
	}
	if !permuted {
		t.Fatal("a plan without Sequential never permuted a full-coverage file")
	}
}
