package dataset

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"

	"github.com/zeebo/xxh3"
)

// PartPlan is the resolved layout of one mix part.
type PartPlan struct {
	Index  int
	Part   Part
	Budget int64
	Sizes  []int64
}

// Dir is the part's directory under the data root: "<index>-<source>".
func (p PartPlan) Dir() string {
	return strconv.Itoa(p.Index) + "-" + p.Part.Source.Kind
}

// IndexWidth is the zero-padded width of file indexes in this part: the
// digits of its file count, at least four.
func (p PartPlan) IndexWidth() int {
	return max(4, len(strconv.Itoa(len(p.Sizes))))
}

// FileStem is the "<index>" prefix of file i's name.
func (p PartPlan) FileStem(i int) string {
	return fmt.Sprintf("%0*d", p.IndexWidth(), i)
}

// FileName is the final name of file i given its hash token.
func (p PartPlan) FileName(i int, hashToken string) string {
	return p.FileStem(i) + "." + HexOf(hashToken) + ".bin"
}

// Plan splits total bytes across the mix and each part's budget into file
// sizes. The result is deterministic in (mix, total, seed) and its sizes sum
// to total exactly.
func Plan(mix Mix, total int64, seed int64) ([]PartPlan, error) {
	if total <= 0 {
		return nil, fmt.Errorf("dataset size must be > 0")
	}
	plans := make([]PartPlan, len(mix))
	var assigned int64
	for i, part := range mix {
		budget := total / 100 * int64(part.SharePct)
		budget += (total % 100) * int64(part.SharePct) / 100
		if i == len(mix)-1 {
			budget = total - assigned // the last part absorbs rounding
		}
		assigned += budget
		plans[i] = PartPlan{Index: i, Part: part, Budget: budget, Sizes: splitBudget(part, budget, seed, i)}
	}
	return plans, nil
}

func splitBudget(part Part, budget int64, seed int64, partIdx int) []int64 {
	if budget <= 0 {
		return nil
	}
	if part.Fixed() {
		n := budget / part.Lo
		sizes := make([]int64, 0, n+1)
		for range n {
			sizes = append(sizes, part.Lo)
		}
		if rem := budget % part.Lo; rem > 0 {
			sizes = append(sizes, rem)
		}
		return sizes
	}
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(partIdx)))
	logLo, logHi := math.Log(float64(part.Lo)), math.Log(float64(part.Hi))
	var sizes []int64
	for remaining := budget; remaining > 0; {
		s := int64(math.Round(math.Exp(logLo + rng.Float64()*(logHi-logLo))))
		s = min(max(s, part.Lo), part.Hi)
		s = min(s, remaining) // the last file is truncated to the remainder
		sizes = append(sizes, s)
		remaining -= s
	}
	return sizes
}

// fileKey is the per-file seed material xxh3(seed, part, index).
func fileKey(seed int64, part, index int) []byte {
	var b [24]byte
	binary.LittleEndian.PutUint64(b[0:], uint64(seed))
	binary.LittleEndian.PutUint64(b[8:], uint64(part))
	binary.LittleEndian.PutUint64(b[16:], uint64(index))
	return b[:]
}

// chachaSeed derives a ChaCha8 key from xxh3(seed, part, index): the 128-bit
// hash with seed 0 and with seed 1, concatenated.
func chachaSeed(seed int64, part, index int) [32]byte {
	key := fileKey(seed, part, index)
	var out [32]byte
	a := xxh3.Hash128Seed(key, 0).Bytes()
	b := xxh3.Hash128Seed(key, 1).Bytes()
	copy(out[:16], a[:])
	copy(out[16:], b[:])
	return out
}

// ringOffset picks where in the Silesia ring file (part, index) starts.
func ringOffset(seed int64, part, index int, ringLen int) int {
	return int(xxh3.Hash(fileKey(seed, part, index)) % uint64(ringLen))
}
