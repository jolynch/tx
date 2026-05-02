// Package sampler provides a deterministic, low-memory sample generator for
// large files.
//
// The generator divides a file into fixed-size frame slots and emits one small
// sample per selected slot. It has two operating modes:
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
//     traversal is a permutation rather than repeating early.
//
// Within each chosen slot, the final byte offset is jittered deterministically
// so repeated runs on the same file produce the same sample layout without
// allocating a large permutation or sample list up front.
package sampler

import (
	"encoding/binary"
	"io"
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
// Callers typically create a generator with New, then repeatedly call Peek and
// Advance until Remaining reaches zero.
type Generator struct {
	fileSize     int64
	frameSize    int64
	sampleBytes  int64
	frameSlots   int64
	sampleCount  int64
	nextIndex    int64
	seedLo       uint64
	seedHi       uint64
	fullCoverage bool
	permCurrent  int64
	permStep     int64
}

// New constructs a deterministic generator for a single file.
//
// The seed is derived from the file identity (`root`, `path`, `fileID`,
// `fileSize`) so the same file produces the same sample sequence across runs.
// `pct` is interpreted as a percentage of frame slots, rounded up the same way
// the CLI verify path expects. The returned bool is false when sampling is not
// meaningful for the provided inputs.
func New(root string, path string, fileID uint64, fileSize int64, pct int, frameSize int64, sampleBytes int64) (Generator, bool) {
	if fileSize <= 0 || pct <= 0 || frameSize <= 0 || sampleBytes <= 0 {
		return Generator{}, false
	}
	frameSlots := (fileSize + frameSize - 1) / frameSize
	if frameSlots <= 0 {
		return Generator{}, false
	}
	sampleCount := (frameSlots*int64(pct) + 99) / 100
	if sampleCount <= 0 {
		sampleCount = 1
	}
	if sampleCount > frameSlots {
		sampleCount = frameSlots
	}
	seedLo, seedHi := buildSeed(root, path, fileID, fileSize)
	gen := Generator{
		fileSize:     fileSize,
		frameSize:    frameSize,
		sampleBytes:  sampleBytes,
		frameSlots:   frameSlots,
		sampleCount:  sampleCount,
		seedLo:       seedLo,
		seedHi:       seedHi,
		fullCoverage: sampleCount == frameSlots,
	}
	if gen.fullCoverage {
		gen.permCurrent = int64(seedLo % uint64(frameSlots))
		gen.permStep = coprimeStep(frameSlots, seedHi)
	}
	return gen, true
}

func (g Generator) TotalSamples() int64 {
	return g.sampleCount
}

// Remaining reports how many samples are left to emit.
func (g *Generator) Remaining() int64 {
	if g == nil {
		return 0
	}
	return g.sampleCount - g.nextIndex
}

// Peek returns the current sample without advancing the generator.
//
// In partial mode, Peek chooses one slot from the current bucket. In full
// coverage mode, Peek returns the current slot in the modular permutation.
func (g *Generator) Peek() (Sample, bool) {
	if g == nil || g.nextIndex >= g.sampleCount {
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
	if g == nil || g.nextIndex >= g.sampleCount {
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

func buildSeed(root string, path string, fileID uint64, fileSize int64) (uint64, uint64) {
	h := xxh3.New128()
	_, _ = io.WriteString(h, filepath.Clean(root))
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, filepath.ToSlash(path))
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(fileSize))
	binary.LittleEndian.PutUint64(buf[8:16], fileID)
	_, _ = h.Write(buf[:])
	sum := h.Sum128()
	return sum.Lo, sum.Hi
}

// sampleForSlot converts a slot index into a concrete byte sample within that
// slot. The intra-slot jitter is deterministic and bounded to the slot.
func (g Generator) sampleForSlot(slotIndex int64) Sample {
	slotStart := slotIndex * g.frameSize
	slotLen := minInt64(g.frameSize, g.fileSize-slotStart)
	size := minInt64(g.sampleBytes, slotLen)
	offset := slotStart
	if maxJitter := slotLen - size; maxJitter > 0 {
		offset += int64(g.hash64('j', uint64(slotIndex)) % uint64(maxJitter+1))
	}
	return Sample{Offset: offset, Size: size}
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

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
