package dataset

import (
	"context"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

func mkfifo(path string) error { return unix.Mkfifo(path, 0o644) }

// FuzzSelectWarmBlocks checks the warm set covers exactly the target, never
// overlaps itself, stays inside each file, and is deterministic.
func FuzzSelectWarmBlocks(f *testing.F) {
	f.Add(int64(1), uint16(30), uint8(1), uint8(5), int64(1<<20))
	f.Add(int64(9), uint16(99), uint8(200), uint8(40), int64(4096))
	f.Fuzz(func(t *testing.T, seed int64, pctRaw uint16, skewRaw uint8, nfiles uint8, block int64) {
		n := 1 + int(nfiles)%64
		sizes := make([]int64, n)
		var total int64
		for i := range sizes {
			sizes[i] = (int64(i)*7919+abs64(seed))%50000 + 1
			total += sizes[i]
		}
		// A floor of 64 bytes keeps an exec to at most ~50k blocks.
		block = 64 + abs64(block)%(64<<10)
		target := total * int64(pctRaw%101) / 100
		skew := 1 + float64(skewRaw)/4
		got := SelectWarmBlocks(sizes, target, seed, skew, block)
		again := SelectWarmBlocks(sizes, target, seed, skew, block)
		if !slices.Equal(got, again) {
			t.Fatal("selection is not deterministic")
		}
		var sum int64
		type span struct{ lo, hi int64 }
		perFile := map[int][]span{}
		for _, b := range got {
			if b.Len <= 0 || b.Offset < 0 || b.Offset+b.Len > sizes[b.File] {
				t.Fatalf("block %+v outside file of %d bytes", b, sizes[b.File])
			}
			for _, s := range perFile[b.File] {
				if b.Offset < s.hi && s.lo < b.Offset+b.Len {
					t.Fatalf("block %+v overlaps %+v", b, s)
				}
			}
			perFile[b.File] = append(perFile[b.File], span{b.Offset, b.Offset + b.Len})
			sum += b.Len
		}
		if sum != target {
			t.Fatalf("warm set %d bytes, target %d", sum, target)
		}
	})
}

// A large skew packs the warm set toward the front of the shuffled order,
// so it touches far fewer files than a uniform warm set of the same size.
func TestWarmSkewConcentrates(t *testing.T) {
	sizes := make([]int64, 1000)
	var total int64
	for i := range sizes {
		sizes[i] = 64 << 10
		total += sizes[i]
	}
	filesTouched := func(skew float64) int {
		seen := map[int]bool{}
		for _, b := range SelectWarmBlocks(sizes, total/10, 1, skew, 4096) {
			seen[b.File] = true
		}
		return len(seen)
	}
	uniform, skewed := filesTouched(1), filesTouched(500)
	if skewed*3 > uniform {
		t.Fatalf("skew 500 touched %d files, uniform %d; expected far fewer", skewed, uniform)
	}
}

func TestParseWarm(t *testing.T) {
	w, err := ParseWarm("30%mem", "8", "64KiB", false)
	if err != nil || !w.OfMem || w.Pct != 30 || w.Skew != 8 || w.Block != 64<<10 {
		t.Fatalf("ParseWarm = %+v %v", w, err)
	}
	for _, bad := range [][3]string{{"30", "", ""}, {"30GiB", "", ""}, {"101%", "", ""}, {"10%", "0.5", ""}, {"10%", "", "0"}} {
		if _, err := ParseWarm(bad[0], bad[1], bad[2], false); err == nil {
			t.Errorf("ParseWarm(%q) accepted", bad)
		}
	}
	if _, err := ParseWarm("", "", "", true); err == nil {
		t.Error("--cache-drop-meta without --cache-warm accepted")
	}
	got, clamped, err := (WarmSpec{Pct: 50, OfMem: true}).TargetBytes(100, 1000)
	if err != nil || !clamped || got != 100 {
		t.Fatalf("N%%mem over the dataset = %d %v %v", got, clamped, err)
	}
	if _, _, err := (WarmSpec{Raw: "100%", Pct: 100}).TargetBytes(2000, 1000); err == nil {
		t.Fatal("100% of a dataset bigger than RAM accepted")
	}
}

func TestSetCacheColdAndHot(t *testing.T) {
	benchDir := t.TempDir()
	generateSmall(t, benchDir, "rand=100%@256KiB", 4<<20, 1)
	d, err := Load(benchDir)
	if err != nil {
		t.Fatal(err)
	}
	if fs, err := StatFS(d.DataRoot()); err == nil && (fs.Type == "tmpfs" || fs.IgnoresFadvise()) {
		t.Skipf("%s cannot evict pages", fs.Type)
	}
	cold, _ := ParseWarm("0%", "", "", false)
	res, err := SetCache(context.Background(), d, cold, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if res.HotPct > 5 {
		t.Fatalf("0%% warm left %.1f%% hot", res.HotPct)
	}
	hot, _ := ParseWarm("100%", "", "", false)
	res, err = SetCache(context.Background(), d, hot, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if res.HotPct < 95 || res.WarmedBytes != 4<<20 {
		t.Fatalf("100%% warm: hot %.1f%% warmed %d", res.HotPct, res.WarmedBytes)
	}
	half, _ := ParseWarm("50%", "", "64KiB", false)
	res, err = SetCache(context.Background(), d, half, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if res.HotPct < 40 || res.HotPct > 60 {
		t.Fatalf("50%% warm measured %.1f%% hot", res.HotPct)
	}
}
