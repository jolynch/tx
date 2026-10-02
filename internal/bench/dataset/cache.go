package dataset

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/pagecache"
)

// WarmSpec is a parsed --cache-warm amount.
type WarmSpec struct {
	Raw    string
	Pct    float64 // percent of the dataset (N%) or of MemTotal (N%mem)
	OfMem  bool
	Skew   float64
	Block  int64
	Unset  bool
	DropMD bool // --cache-drop-meta
}

// DefaultWarmBlock is the default --cache-warm-block.
const DefaultWarmBlock = 1 << 20

// ParseWarm parses --cache-warm, --cache-warm-skew, and --cache-warm-block.
// Bare numbers and byte sizes are rejected so "30" is never ambiguous.
func ParseWarm(amount, skewRaw, blockRaw string, dropMeta bool) (WarmSpec, error) {
	w := WarmSpec{Raw: amount, Skew: 1, Block: DefaultWarmBlock, DropMD: dropMeta}
	amount = strings.TrimSpace(amount)
	if amount == "" {
		w.Unset = true
	} else {
		num := amount
		switch {
		case strings.HasSuffix(amount, "%mem"):
			w.OfMem = true
			num = strings.TrimSuffix(amount, "%mem")
		case strings.HasSuffix(amount, "%"):
			num = strings.TrimSuffix(amount, "%")
		default:
			return WarmSpec{}, fmt.Errorf("--cache-warm %q must be N%% of the dataset or N%%mem of RAM", amount)
		}
		pct, err := strconv.ParseFloat(num, 64)
		if err != nil || pct < 0 || pct > 100 || math.IsNaN(pct) {
			return WarmSpec{}, fmt.Errorf("--cache-warm %q must be 0%%..100%%", amount)
		}
		w.Pct = pct
	}
	if s := strings.TrimSpace(skewRaw); s != "" {
		skew, err := strconv.ParseFloat(s, 64)
		if err != nil || skew < 1 || math.IsNaN(skew) {
			return WarmSpec{}, fmt.Errorf("--cache-warm-skew %q must be a number >= 1", skewRaw)
		}
		w.Skew = skew
	}
	if s := strings.TrimSpace(blockRaw); s != "" {
		b, err := encoding.ParseByteSize(s)
		if err != nil || b <= 0 {
			return WarmSpec{}, fmt.Errorf("--cache-warm-block %q must be a positive size", blockRaw)
		}
		w.Block = b
	}
	if dropMeta && w.Unset {
		return WarmSpec{}, fmt.Errorf("--cache-drop-meta requires --cache-warm")
	}
	return w, nil
}

// TargetBytes resolves the warm amount against the dataset and MemTotal.
// clamped reports an N%mem larger than the dataset.
func (w WarmSpec) TargetBytes(datasetBytes, memTotal int64) (target int64, clamped bool, err error) {
	if w.OfMem {
		target = int64(float64(memTotal) * w.Pct / 100)
		if target > datasetBytes {
			return datasetBytes, true, nil
		}
		return target, false, nil
	}
	target = int64(float64(datasetBytes) * w.Pct / 100)
	if target > memTotal {
		return 0, false, fmt.Errorf("--cache-warm %s is %s, more than MemTotal %s", w.Raw,
			encoding.HumanBytes(target), encoding.HumanBytes(memTotal))
	}
	return target, false, nil
}

// Block is a contiguous byte range of one dataset file.
type Block struct {
	File   int // index into the file list
	Offset int64
	Len    int64
}

// SelectWarmBlocks chooses which byte ranges to warm. Files are ordered by
// a seeded shuffle and split into block-sized pieces (a file's tail is a
// short block). Block i at normalized midpoint x_i gets weight
// (1 - x_i)^(skew - 1), and blocks are drawn by weighted sampling without
// replacement (Efraimidis–Spirakis) until target bytes are covered; the last
// block is trimmed to the target. Selection is deterministic in its inputs.
func SelectWarmBlocks(sizes []int64, target int64, seed int64, skew float64, blockSize int64) []Block {
	var total int64
	for _, s := range sizes {
		total += s
	}
	if target <= 0 || total == 0 {
		return nil
	}
	order := make([]int, len(sizes))
	for i := range order {
		order[i] = i
	}
	rng := rand.New(rand.NewPCG(uint64(seed), 0x7761726d)) // "warm"
	rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })

	type keyed struct {
		Block
		key float64
		seq int
	}
	var blocks []keyed
	var pos int64
	for _, fi := range order {
		size := sizes[fi]
		for off := int64(0); off < size; off += blockSize {
			n := min(blockSize, size-off)
			x := (float64(pos) + float64(n)/2) / float64(total)
			// Minimize log(E) - log(w), E ~ Exp(1): the log form of
			// maximizing u^(1/w), which cannot underflow for large skew.
			e := rng.ExpFloat64()
			key := math.Log(e) - (skew-1)*math.Log1p(-x)
			blocks = append(blocks, keyed{Block: Block{File: fi, Offset: off, Len: n}, key: key, seq: len(blocks)})
			pos += n
		}
	}
	if target >= total {
		out := make([]Block, len(blocks))
		for i, b := range blocks {
			out[i] = b.Block
		}
		return out
	}
	slices.SortFunc(blocks, func(a, b keyed) int {
		if a.key != b.key {
			if a.key < b.key {
				return -1
			}
			return 1
		}
		return a.seq - b.seq
	})
	var out []Block
	var got int64
	for _, b := range blocks {
		if got >= target {
			break
		}
		if rem := target - got; b.Len > rem {
			b.Len = rem
		}
		out = append(out, b.Block)
		got += b.Len
	}
	return out
}

// CacheResult is what one cache step did and measured.
type CacheResult struct {
	Warm        string        `json:"warm"`
	Skew        float64       `json:"skew"`
	BlockBytes  int64         `json:"block_bytes"`
	TargetBytes int64         `json:"target_bytes"`
	Clamped     bool          `json:"clamped,omitempty"`
	Evicted     int64         `json:"evicted"`
	WarmedBytes int64         `json:"warmed"`
	HotPct      float64       `json:"hot_pct"`
	MetaCold    bool          `json:"meta_cold"`
	Duration    time.Duration `json:"-"`
	DurationMS  int64         `json:"dur_ms"`
	Warnings    []string      `json:"warnings,omitempty"`
}

// Residency measures the share of the dataset's pages resident in the page
// cache with mincore, as a percentage.
func Residency(paths []string, sizes []int64) float64 {
	page := int64(pagecache.PageSize())
	var resident, total int64
	for i, p := range paths {
		pages := (sizes[i] + page - 1) / page
		total += pages
		var e pagecache.CacheEntry
		if err := e.Load(p); err == nil {
			resident += int64(e.NumResidentPages())
		}
	}
	if total == 0 {
		return 0
	}
	return math.Round(float64(resident)*1000/float64(total)) / 10
}

// SetCache evicts every dataset page and warms the selected set. With an
// unset spec it only measures residency.
func SetCache(ctx context.Context, d *Dataset, w WarmSpec, seed int64, jobs int) (CacheResult, error) {
	start := time.Now()
	paths, sizes := d.Files()
	res := CacheResult{Warm: w.Raw, Skew: w.Skew, BlockBytes: w.Block}
	finish := func() (CacheResult, error) {
		res.HotPct = Residency(paths, sizes)
		if w.DropMD {
			if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("2\n"), 0); err != nil {
				return res, fmt.Errorf("--cache-drop-meta: write /proc/sys/vm/drop_caches (requires root): %w", err)
			}
			res.MetaCold = true
			res.Warnings = append(res.Warnings, "--cache-drop-meta dropped every cached dentry and inode on this host (drop_caches=2), not just the dataset's")
		}
		res.Duration = time.Since(start)
		res.DurationMS = res.Duration.Milliseconds()
		return res, nil
	}
	if w.Unset {
		return finish()
	}
	if fs, err := StatFS(d.DataRoot()); err == nil && fs.IgnoresFadvise() {
		res.Warnings = append(res.Warnings, fmt.Sprintf("%s is on %s, which ignores fadvise; --cache-warm cannot be honored", d.DataRoot(), fs.Type))
	}
	mem, err := ReadMemInfo()
	if err != nil {
		return res, err
	}
	var datasetBytes int64
	for _, s := range sizes {
		datasetBytes += s
	}
	target, clamped, err := w.TargetBytes(datasetBytes, mem.Total)
	if err != nil {
		return res, err
	}
	res.TargetBytes, res.Clamped = target, clamped

	ev, err := pagecache.EvictPaths(ctx, func(yield func(string) bool) {
		for _, p := range paths {
			if !yield(p) {
				return
			}
		}
	}, jobs)
	if err != nil {
		return res, err
	}
	res.Evicted = ev.Evicted
	if after, err := ReadMemInfo(); err == nil && target > after.Available {
		res.Warnings = append(res.Warnings, fmt.Sprintf("warm set %s exceeds MemAvailable %s; hot_pct will show the shortfall",
			encoding.HumanBytes(target), encoding.HumanBytes(after.Available)))
	}

	blocks := SelectWarmBlocks(sizes, target, seed, w.Skew, w.Block)
	res.WarmedBytes, err = warmBlocks(ctx, paths, sizes, blocks, jobs)
	if err != nil {
		return res, err
	}
	return finish()
}

func warmBlocks(ctx context.Context, paths []string, sizes []int64, blocks []Block, jobs int) (int64, error) {
	page := int64(pagecache.PageSize())
	bitsByFile := map[int][]byte{}
	var fileOrder []int
	var warmed int64
	for _, b := range blocks {
		bits, ok := bitsByFile[b.File]
		if !ok {
			pages := (sizes[b.File] + page - 1) / page
			bits = make([]byte, (pages+7)/8)
			bitsByFile[b.File] = bits
			fileOrder = append(fileOrder, b.File)
		}
		for p := b.Offset / page; p < (b.Offset+b.Len+page-1)/page; p++ {
			bits[p/8] |= 1 << (p % 8)
		}
		warmed += b.Len
	}
	entries := make([]pagecache.TouchEntry, 0, len(fileOrder))
	for _, fi := range fileOrder {
		pages := int((sizes[fi] + page - 1) / page)
		e := &pagecache.CacheEntry{}
		if err := e.SetPageBits(bitsByFile[fi], pages); err != nil {
			return 0, err
		}
		entries = append(entries, pagecache.TouchEntry{Path: paths[fi], Entry: e})
	}
	_, err := pagecache.TouchEntries(ctx, func(yield func(pagecache.TouchEntry) bool) {
		for _, e := range entries {
			if !yield(e) {
				return
			}
		}
	}, 0, jobs)
	return warmed, err
}
