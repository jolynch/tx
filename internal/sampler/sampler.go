// Package sampler provides a deterministic, low-memory sample generator for
// trees of large files.
//
// A Plan walks the tree's files in order and gives each a Generator. Each
// generator divides its file into fixed-size frame slots and emits selected
// slots whole, so sampling N% of slots reads N% of the bytes. Slot counts
// use systematic rounding over the tree's concatenated slots (see Plan), so
// the total is within one slot of N% and small files are not each rounded up
// to a whole slot. A Generator has two operating modes:
//
//   - Partial sampling (`sampleCount < frameSlots`): the slot domain is split
//     into `sampleCount` buckets and the generator picks exactly one slot from
//     each bucket using a deterministic hash-derived offset. This keeps output
//     ordered by bucket, which preserves mostly sequential I/O.
//
//   - Full coverage (`sampleCount == frameSlots`): every slot is visited
//     exactly once using an O(1)-state modular walk over `[0, frameSlots)`.
//     The walk starts from a deterministic seed-derived slot and advances by a
//     seed-derived step that is coprime with `frameSlots`, which guarantees the
//     traversal is a permutation rather than repeating early. With
//     PlanOptions.Sequential the walk starts at slot 0 with step 1, so slots
//     come in ascending order and reads stay sequential.
//
// The same seed and tree always produce the same samples, without allocating
// a permutation or sample list up front. Callers pick a new seed per run.
package sampler

import (
	"encoding/binary"
	"path/filepath"

	"github.com/zeebo/xxh3"
)

type Sample struct {
	Offset int64
	Size   int64
}

// Generator streams deterministic samples for one file without materializing
// the entire sample set in memory.
//
// Callers get a generator from Plan.Next, then repeatedly call Peek and
// Advance until Remaining reaches zero.
type Generator struct {
	fileSize     int64
	frameSize    int64
	frameSlots   int64
	sampleCount  int64
	nextIndex    int64
	seedLo       uint64
	seedHi       uint64
	fullCoverage bool
	permCurrent  int64
	permStep     int64
}

// PlanOptions configures a tree-wide sampling plan.
type PlanOptions struct {
	Root      string
	Pct       int   // percentage of slots to sample, 1 to 100
	FrameSize int64 // slot size in bytes
	// TotalSlots is the slot count of every file the plan will see, as
	// summed by SlotCount. The plan needs it to guarantee one slot when
	// the tree's share rounds to zero.
	TotalSlots int64
	// Seed selects the rounding offset and every file's slots. The same
	// seed and tree give the same plan.
	Seed uint64
	// Sequential visits a file's slots in ascending order when the plan
	// covers all of them. Leave it false when the run can stop early, so the
	// walk spreads partial coverage across each file.
	Sequential bool
}

// Plan hands out each file's Generator in order, choosing slot counts by
// systematic rounding over the tree's concatenated slots.
//
// With acc slots seen so far, a start u in [0,1) drawn from the seed, and a
// file of s slots, the file gets floor((acc+s)*N/100+u) - floor(acc*N/100+u)
// slots. Each file's expected count is N% of its slots, and the total is
// within one slot of N% of the tree. If that total would be zero for a tree
// with data, the file holding a seed-chosen slot gets one slot instead.
type Plan struct {
	opts  PlanOptions
	start int64 // u in hundredths
	acc   int64
	// forced is the tree slot index that gets the single slot when the
	// rounded total is zero, or -1.
	forced int64
}

// NewPlan returns a plan for a tree. Call Next for every file in the same
// order the slots were totalled.
func NewPlan(opts PlanOptions) *Plan {
	opts.Pct = max(min(opts.Pct, 100), 0)
	p := &Plan{opts: opts, forced: -1}
	p.start = int64(seedHash(opts.Seed, 'u') % 100)
	if opts.TotalSlots > 0 && opts.Pct > 0 && p.rounded(opts.TotalSlots) == 0 {
		p.forced = int64(seedHash(opts.Seed, 'f') % uint64(opts.TotalSlots))
	}
	return p
}

// SlotCount is the number of frame slots in a file of the given size.
func SlotCount(size, frameSize int64) int64 {
	if size <= 0 || frameSize <= 0 {
		return 0
	}
	return (size + frameSize - 1) / frameSize
}

// rounded is floor(slots*N/100+u) for slots seen so far.
func (p *Plan) rounded(slots int64) int64 {
	// Split so tiny frame sizes cannot overflow slots*pct.
	pct := int64(p.opts.Pct)
	return slots/100*pct + (slots%100*pct+p.start)/100
}

// Next returns the Generator for the next file. The bool is false when the
// file draws no slots. Files with no data do not advance the plan.
func (p *Plan) Next(path string, fileID uint64, fileSize int64) (Generator, bool) {
	slots := SlotCount(fileSize, p.opts.FrameSize)
	if slots == 0 || p.opts.Pct <= 0 {
		return Generator{}, false
	}
	count := p.rounded(p.acc+slots) - p.rounded(p.acc)
	if p.forced >= 0 {
		count = 0
		if p.forced >= p.acc && p.forced < p.acc+slots {
			count = 1
		}
	}
	p.acc += slots
	if count <= 0 {
		return Generator{}, false
	}
	seedLo, seedHi := buildSeed(p.opts.Root, path, fileID, fileSize, p.opts.Seed)
	gen := Generator{
		fileSize:     fileSize,
		frameSize:    p.opts.FrameSize,
		frameSlots:   slots,
		sampleCount:  count,
		seedLo:       seedLo,
		seedHi:       seedHi,
		fullCoverage: count == slots,
	}
	if gen.fullCoverage {
		gen.permStep = 1
		if !p.opts.Sequential {
			gen.permCurrent = int64(seedLo % uint64(slots))
			gen.permStep = coprimeStep(slots, seedHi)
		}
	}
	return gen, true
}

func seedHash(seed uint64, tag byte) uint64 {
	var buf [9]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	buf[8] = tag
	return xxh3.Hash(buf[:])
}

func (g Generator) TotalSamples() int64 {
	return g.sampleCount
}

// Remaining reports how many samples are left to emit.
func (g *Generator) Remaining() int64 {
	return g.sampleCount - g.nextIndex
}

// Peek returns the current sample without advancing the generator.
//
// In partial mode, Peek chooses one slot from the current bucket. In full
// coverage mode, Peek returns the current slot in the modular permutation.
func (g *Generator) Peek() (Sample, bool) {
	if g.nextIndex >= g.sampleCount {
		return Sample{}, false
	}
	var slotIndex int64
	if g.fullCoverage {
		slotIndex = g.permCurrent
	} else {
		slotStart := (g.nextIndex * g.frameSlots) / g.sampleCount
		slotEnd := ((g.nextIndex+1)*g.frameSlots)/g.sampleCount - 1
		slotIndex = slotStart
		if width := slotEnd - slotStart + 1; width > 1 {
			slotIndex += int64(g.hash64('b', uint64(g.nextIndex)) % uint64(width))
		}
	}
	return g.sampleForSlot(slotIndex), true
}

// Advance moves to the next sample.
//
// In full coverage mode this advances the modular permutation by the coprime
// step chosen during construction.
func (g *Generator) Advance() {
	if g.nextIndex >= g.sampleCount {
		return
	}
	g.nextIndex++
	if g.fullCoverage && g.nextIndex < g.sampleCount {
		g.permCurrent += g.permStep
		if g.permCurrent >= g.frameSlots {
			g.permCurrent %= g.frameSlots
		}
	}
}

func buildSeed(root string, path string, fileID uint64, fileSize int64, runSeed uint64) (uint64, uint64) {
	// One stack buffer holds root, a NUL, the path and three words; the
	// bytes match what a streaming hasher would see.
	var arr [256]byte
	buf := append(arr[:0], filepath.Clean(root)...)
	buf = append(buf, 0)
	buf = append(buf, filepath.ToSlash(path)...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(fileSize))
	buf = binary.LittleEndian.AppendUint64(buf, fileID)
	buf = binary.LittleEndian.AppendUint64(buf, runSeed)
	sum := xxh3.Hash128(buf)
	return sum.Lo, sum.Hi
}

// sampleForSlot returns the whole slot; the last one may be short.
func (g Generator) sampleForSlot(slotIndex int64) Sample {
	slotStart := slotIndex * g.frameSize
	return Sample{Offset: slotStart, Size: min(g.frameSize, g.fileSize-slotStart)}
}

func (g Generator) hash64(tag byte, value uint64) uint64 {
	var buf [25]byte
	binary.LittleEndian.PutUint64(buf[0:8], g.seedLo)
	binary.LittleEndian.PutUint64(buf[8:16], g.seedHi)
	binary.LittleEndian.PutUint64(buf[16:24], value)
	buf[24] = tag
	return xxh3.Hash(buf[:])
}

// coprimeStep chooses a deterministic step for the full-coverage modular walk.
//
// A step coprime with frameSlots guarantees that repeatedly adding the step
// modulo frameSlots visits every slot exactly once before repeating.
func coprimeStep(frameSlots int64, seed uint64) int64 {
	if frameSlots <= 1 {
		return 1
	}
	step := int64(seed%uint64(frameSlots-1)) + 1
	for gcd(step, frameSlots) != 1 {
		step++
		if step >= frameSlots {
			step = 1
		}
	}
	return step
}

func gcd(a int64, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	if a == 0 {
		return 1
	}
	return a
}
