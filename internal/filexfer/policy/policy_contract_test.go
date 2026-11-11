package policy

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// scenario runs one policy through segments and returns the mode each frame
// ran in.
func scenario(t *testing.T, seed uint64, jitter float64, start CompressionMode, segs ...simSegment) []CompressionMode {
	t.Helper()
	return runScenario(t, newSim(seed, jitter), start, segs...)
}

// costScenario is scenario without a bottleneck reading, so the policy
// infers the bottleneck from frame costs alone.
func costScenario(t *testing.T, seed uint64, jitter float64, start CompressionMode, segs ...simSegment) []CompressionMode {
	t.Helper()
	return runScenario(t, newSim(seed, jitter).costsOnly(), start, segs...)
}

func runScenario(t *testing.T, s *sim, start CompressionMode, segs ...simSegment) []CompressionMode {
	t.Helper()
	p := NewCompressionPolicy()
	mode := start
	var modes []CompressionMode
	for _, sg := range segs {
		for i := 0; i < sg.n; i++ {
			modes = append(modes, mode)
			mode = s.step(t, p, sg.r, mode)
		}
	}
	return modes
}

// regime builds a regime from speeds in MiB/s: read, lz4, and zstd, and the
// lz4 and zstd ratios.
func regime(b simBottleneck, rateMiB, readMiB, lz4MiB, zstdMiB, lz4Ratio, zstdRatio float64) simRegime {
	read := simFrame / (readMiB * mib)
	return simRegime{
		b:       b,
		rate:    rateMiB * mib,
		ratio:   [3]float64{1, lz4Ratio, zstdRatio},
		prepare: [3]float64{read, read + simFrame/(lz4MiB*mib), read + simFrame/(zstdMiB*mib)},
	}
}

// countIn counts the frames from lo to hi that ran in mode.
func countIn(modes []CompressionMode, lo, hi int, want CompressionMode) int {
	n := 0
	for _, m := range modes[lo:hi] {
		if m == want {
			n++
		}
	}
	return n
}

// TestCompressionPolicyKeepsLz4AtHighRatio: lz4 shrinks the data 200-fold on
// a slow link, so preparing a frame takes several times longer than writing
// it, yet lz4 is far faster per logical byte than none or zstd. The CPU
// signal alone must not drop the codec.
func TestCompressionPolicyKeepsLz4AtHighRatio(t *testing.T) {
	r := regime(simNetwork, 32, 16<<10, 1<<10, 512, 200, 250)
	p, w, _ := r.nominal(1)
	if p < 4*w || !r.separated() || r.throughput(1) < r.throughput(2) {
		t.Fatalf("setup: want lz4 best by %.1fx with prepare well above write: %v", simSlow, r)
	}
	for _, start := range []CompressionMode{CompressionModeNone, CompressionModeLz4} {
		for seed := uint64(0); seed < 10; seed++ {
			const n = simSettle + 1024
			modes := scenario(t, seed, 0.3, start, simSegment{r, n})
			if got := countIn(modes, simSettle, n, CompressionModeLz4); got*20 < (n-simSettle)*19 {
				t.Fatalf("start %s seed %d: %d of %d settled frames in lz4, want 95%%\n  %v",
					modeName(start), seed, got, n-simSettle, r)
			}
		}
	}
}

// TestCompressionPolicyLeavesCompressionWhenCPUBound: the link is fast and the
// codecs slow, so none is several times faster. The policy must go to none
// and stay, both from a compressed start and after a stretch where
// compression was right.
func TestCompressionPolicyLeavesCompressionWhenCPUBound(t *testing.T) {
	cpu := regime(simCPU, 0, 4<<10, 512, 256, 2.4, 3)
	slowLink := regime(simNetwork, 32, 4<<10, 512, 256, 2.4, 3)
	if !cpu.separated() || cpu.throughput(0) < cpu.throughput(1) || cpu.good(CompressionModeZstdLevel1) {
		t.Fatalf("setup: want none best by %.1fx: %v", simSlow, cpu)
	}
	if !slowLink.good(CompressionModeZstdLevel1) || !slowLink.slow(CompressionModeNone) {
		t.Fatalf("setup: want compression right first: %v", slowLink)
	}
	cases := []struct {
		name  string
		start CompressionMode
		segs  []simSegment
	}{
		{"from zstd", CompressionModeZstdLevel1, []simSegment{{cpu, simSettle + 1024}}},
		{"from lz4", CompressionModeLz4, []simSegment{{cpu, simSettle + 1024}}},
		{"after compression was right", CompressionModeNone, []simSegment{{slowLink, 256}, {cpu, simSettle + 1024}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			change := 0
			for _, sg := range tc.segs[:len(tc.segs)-1] {
				change += sg.n
			}
			late := 0
			for seed := uint64(0); seed < 20; seed++ {
				modes := scenario(t, seed, 0.3, tc.start, tc.segs...)
				if firstIn(modes, change, CompressionModeNone) > change+2*simReact {
					late++
				}
				lo := change + simSettle
				if got := countIn(modes, lo, len(modes), CompressionModeNone); got*20 < (len(modes)-lo)*19 {
					t.Fatalf("seed %d: %d of %d settled frames in none, want 95%%\n  %v", seed, got, len(modes)-lo, cpu)
				}
			}
			if late > 1 {
				t.Fatalf("%d of 20 runs took more than %d frames to reach none\n  %v", late, 2*simReact, cpu)
			}
		})
	}
}

// firstIn returns the first frame at or after from that ran in want, or
// len(modes).
func firstIn(modes []CompressionMode, from int, want CompressionMode) int {
	for i := from; i < len(modes); i++ {
		if modes[i] == want {
			return i
		}
	}
	return len(modes)
}

// TestCompressionPolicyBurstCreditsRunOut: a burstable network runs fast, so
// none is best, until its credits run out mid-file and the link drops to
// 32 MiB/s, where compression is best. The policy must leave none within
// simReact frames (C3) and then run in good modes (C1).
func TestCompressionPolicyBurstCreditsRunOut(t *testing.T) {
	burst := regime(simNetwork, 2<<10, 16<<10, 1<<10, 512, 2.7, 3)
	drained := regime(simNetwork, 32, 16<<10, 1<<10, 512, 2.7, 3)
	if !burst.separated() || burst.throughput(0) < burst.throughput(1) || !drained.slow(CompressionModeNone) ||
		!drained.good(CompressionModeLz4) || !drained.good(CompressionModeZstdLevel1) {
		t.Fatalf("setup: want none best first, then %.1fx slower than either codec:\n  %v\n  %v", simSlow, burst, drained)
	}
	const before, after = 512, 512
	late := 0
	for seed := uint64(0); seed < 20; seed++ {
		modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{burst, before}, simSegment{drained, after})
		if modes[before] == CompressionModeNone {
			if left := firstNotIn(modes, before, CompressionModeNone); left > before+simReact {
				late++
			}
		}
		good := 0
		for _, m := range modes[before+simSettle:] {
			if drained.good(m) {
				good++
			}
		}
		if good*100 < (after-simSettle)*85 {
			t.Fatalf("seed %d: %d of %d settled frames within %.1fx of the best, want 85%%\n  %v",
				seed, good, after-simSettle, simGood, drained)
		}
	}
	if late > 1 {
		t.Fatalf("%d of 20 runs stayed in none more than %d frames after the link slowed\n  %v", late, simReact, drained)
	}
}

// firstNotIn returns the first frame at or after from that did not run in
// mode, or len(modes).
func firstNotIn(modes []CompressionMode, from int, mode CompressionMode) int {
	for i := from; i < len(modes); i++ {
		if modes[i] != mode {
			return i
		}
	}
	return len(modes)
}

// TestCompressionPolicyDrainsWhenReceiverDiskBinds: compression is right on
// a network-bound link until the receiver's disk becomes the bottleneck.
// Every codec then only adds CPU time, and both are at least simSlow times
// slower than none, so the policy must leave each rung within simReact
// frames: in none within 2*simReact frames of the change.
func TestCompressionPolicyDrainsWhenReceiverDiskBinds(t *testing.T) {
	network := regime(simNetwork, 256, 16<<10, 1<<10, 512, 18.4, 30)
	disk := regime(simDisk, 768, 16<<10, 1<<10, 512, 18.4, 30)
	if !network.good(CompressionModeLz4) || !network.slow(CompressionModeNone) {
		t.Fatalf("setup: want compression right first: %v", network)
	}
	if !disk.slow(CompressionModeLz4) || !disk.slow(CompressionModeZstdLevel1) ||
		moved(network, disk, CompressionModeLz4) < simVisible || moved(network, disk, CompressionModeZstdLevel1) < simVisible {
		t.Fatalf("setup: want both codecs %.1fx slower than none, visibly:\n  %v\n  %v", simSlow, network, disk)
	}
	const before, after = 256, 512
	late := 0
	for seed := uint64(0); seed < 20; seed++ {
		modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{network, before}, simSegment{disk, after})
		if firstIn(modes, before, CompressionModeNone) > before+2*simReact {
			late++
		}
		lo := before + simSettle
		if got := countIn(modes, lo, len(modes), CompressionModeNone); got*100 < (len(modes)-lo)*85 {
			t.Fatalf("seed %d: %d of %d settled frames in none, want 85%%\n  %v", seed, got, len(modes)-lo, disk)
		}
	}
	if late > 1 {
		t.Fatalf("%d of 20 runs took more than %d frames to drain to none\n  %v", late, 2*simReact, disk)
	}
}

// TestCompressionPolicyIncompressibleData checks C2 under every bottleneck,
// with the cheapest lz4 to tempt the policy: at most 5% of frames compressed,
// and the gaps between probes shrink at most once and grow at least twofold. On a
// network-bound link, where compression could pay if the data changed,
// probes continue into the second half of the run.
func TestCompressionPolicyIncompressibleData(t *testing.T) {
	const n = 4096
	for _, b := range []simBottleneck{simNetwork, simDisk, simCPU} {
		for _, ratio := range []float64{1.0, 1.05} {
			r := regime(b, 128, 4<<10, 4<<10, 512, ratio, ratio)
			for seed := uint64(0); seed < 5; seed++ {
				name := fmt.Sprintf("%s ratio %.2f seed %d", b, ratio, seed)
				modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{r, n})
				compressed := n - countIn(modes, 0, n, CompressionModeNone)
				if compressed*20 > n {
					t.Errorf("%s: %d of %d frames compressed, want at most 5%%\n  %v", name, compressed, n, r)
					continue
				}
				// Upward exploration stays off the wire: incompressible data
				// costs only bounded background samples, which
				// TestCompressionPolicyProbeGapsBounded spaces.
			}
		}
	}
}

// TestFileCompressionZeroValueStartsFresh checks C6: a zero-valued
// FileCompression, or one whose mode adapt does not use, decides exactly as a
// fresh policy that starts at none.
func TestFileCompressionZeroValueStartsFresh(t *testing.T) {
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 2.4, 3)
	for _, mode := range []CompressionMode{CompressionModeZstdDefault, CompressionMode(9), CompressionModeNone} {
		s := newSim(2, 0.3)
		got := FileCompression{Mode: mode}
		want := FileCompression{Mode: CompressionModeNone, Policy: *NewCompressionPolicy()}
		for i := 0; i < 200; i++ {
			m := s.sendMetrics(r, s.frame(r, want.Mode))
			dg := got.Policy.DecideSampled(got.Mode, m, sampleFor(&got.Policy, r, i))
			dw := want.Policy.DecideSampled(want.Mode, m, sampleFor(&want.Policy, r, i))
			if dg.Next != dw.Next {
				t.Fatalf("FileCompression{Mode: %s} at frame %d chose %s; a fresh policy at none chose %s",
					modeName(mode), i, modeName(dg.Next), modeName(dw.Next))
			}
			checkStep(t, want.Mode, dw.Next)
			got.Mode, want.Mode = dg.Next, dw.Next
		}
	}
}

// TestCompressionPolicyDegenerateMetrics: frames with zero sizes or
// latencies, or a very long write, still yield a mode adapt uses, at most one
// rung above the current one.
func TestCompressionPolicyDegenerateMetrics(t *testing.T) {
	frames := []CompressionMetrics{
		{},
		{LogicalSize: simFrame, WireSize: simFrame},
		{LogicalSize: simFrame, WireSize: 0, PrepareLatency: time.Millisecond, WriteLatency: time.Millisecond},
		{LogicalSize: simFrame, WireSize: 1, PrepareLatency: 0, WriteLatency: time.Hour},
		{LogicalSize: 0, WireSize: simFrame, PrepareLatency: time.Hour, WriteLatency: 0},
		{LogicalSize: simFrame, WireSize: 2 * simFrame, PrepareLatency: -time.Millisecond, WriteLatency: -time.Millisecond},
	}
	for _, start := range simModes {
		p := NewCompressionPolicy()
		mode := start
		for i := 0; i < 64; i++ {
			next := p.DecideSampled(mode, frames[i%len(frames)], nil).Next
			checkStep(t, mode, next)
			mode = next
		}
	}
}

// TestCompressionPolicyDeterministic checks C7: the same metrics give the
// same decisions, and a FileCompression copied mid-file, as the store copies
// it between windows, continues exactly as the original.
func TestCompressionPolicyDeterministic(t *testing.T) {
	segs := []simRegime{
		regime(simNetwork, 32, 4<<10, 1<<10, 512, 2.4, 3),
		regime(simDisk, 768, 16<<10, 1<<10, 512, 18.4, 30),
		regime(simNetwork, 128, 4<<10, 4<<10, 512, 1, 1),
	}
	s := newSim(3, 0.3)
	a := FileCompression{Mode: CompressionModeNone, Policy: *NewCompressionPolicy()}
	b := a
	var c FileCompression
	for i := 0; i < 900; i++ {
		if i == 450 {
			c = a
		}
		r := segs[i/300]
		m := s.sendMetrics(r, s.frame(r, a.Mode))
		da := a.Policy.DecideSampled(a.Mode, m, sampleFor(&a.Policy, r, i))
		db := b.Policy.DecideSampled(b.Mode, m, sampleFor(&b.Policy, r, i))
		if da != db {
			t.Fatalf("frame %d: identical policies decided %+v and %+v", i, da, db)
		}
		if i >= 450 {
			if dc := c.Policy.DecideSampled(c.Mode, m, sampleFor(&c.Policy, r, i)); dc != da {
				t.Fatalf("frame %d: the copy decided %+v, the original %+v", i, dc, da)
			}
			c.Mode = da.Next
		}
		a.Mode, b.Mode = da.Next, db.Next
	}
}

// TestCompressionPolicyDecideDoesNotAllocate checks C7 across modes and
// switches, with and without sniff ratios.
func TestCompressionPolicyDecideDoesNotAllocate(t *testing.T) {
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 2.4, 3)
	s := newSim(4, 0.3)
	var frames [64]CompressionMetrics
	for i := range frames {
		frames[i] = s.frame(r, simModes[i%3])
		if i%9 == 0 { // a none frame
			frames[i].SniffRatio = 2.2
		}
	}
	p := NewCompressionPolicy()
	mode := CompressionModeNone
	sample := samplesForRegime(r)
	i := 0
	n := testing.AllocsPerRun(1000, func() {
		m := frames[i%len(frames)]
		m.RecoveryAllowed, m.RecoveryReady = true, true
		var ready *encoding.CodecSamples
		if i%3 == 0 {
			ready = &sample
		}
		i++
		mode = p.DecideSampled(mode, m, ready).Next
	})
	if n != 0 {
		t.Fatalf("DecideSampled allocated %v times per call", n)
	}
}

// simMixedEfficiency is the efficiency the mixed-compressibility scenario
// requires. It stays at its previous value: the scenario is one fixed input,
// not a fuzzed one.
const simMixedEfficiency = 0.90

// frac returns a/b as a share, or 0 when b is 0.
func frac(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// TestCompressionPolicyMixedCompressibility: data alternates between regions
// with a ratio of about 2.5 and incompressible regions, 16 to 256 frames
// each, on a slow link. While in none, the policy sees the sniff ratio of
// the data, so it must not wait for a probe to find a compressible region: the
// time efficiency over the whole file, regions' first frames included, is at
// least simMixedEfficiency (0.90), summed over 8 runs.
func TestCompressionPolicyMixedCompressibility(t *testing.T) {
	compressible := regime(simNetwork, 32, 4<<10, 1<<10, 512, 2.2, 2.5)
	random := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 1)
	if !compressible.slow(CompressionModeNone) || random.throughput(0) < random.throughput(1) {
		t.Fatalf("setup: want compression right on one and none best on the other:\n  %v\n  %v", compressible, random)
	}
	var ty tally
	for seed := uint64(0); seed < 8; seed++ {
		rng := rand.New(rand.NewPCG(seed, 0x6d78))
		var segs []simSegment
		for total, i := 0, 0; total < 3072; i++ {
			n := 16 + rng.IntN(241)
			r := compressible
			if i%2 == 1 {
				r = random
			}
			segs = append(segs, simSegment{r, n})
			total += n
		}
		modes := scenario(t, seed, 0.3, CompressionModeNone, segs...)
		at := 0
		for _, sg := range segs {
			for i := 0; i < sg.n; i++ {
				ty.add(sg.r, modes[at+i])
			}
			at += sg.n
		}
	}
	if ty.efficiency() < simMixedEfficiency {
		t.Fatalf("efficiency %.3f over alternating regions, want at least %.2f\n  %v\n  %v",
			ty.efficiency(), simMixedEfficiency, compressible, random)
	}
}

// TestCompressionPolicyAdoptsFasterNeighbor: a neighbor mode 1.2 times
// faster than the current one is adopted within 256 frames in at least 18 of
// 20 runs and held for at least 70% of the following 512, going up from none
// to lz4 and down from lz4 to none.
func TestCompressionPolicyAdoptsFasterNeighbor(t *testing.T) {
	up := regime(simNetwork, 128, 16<<10, 800, 256, 1.5, 1.5)
	down := regime(simNetwork, 128, 16<<10, 380, 256, 1.15, 1.15)
	if r := up.throughput(1) / up.throughput(0); r < 1.15 || r > 1.3 || up.throughput(2) > up.throughput(0) {
		t.Fatalf("setup: want lz4 1.2x faster than none: %v", up)
	}
	if r := down.throughput(0) / down.throughput(1); r < 1.15 || r > 1.3 || down.throughput(2) > down.throughput(1) {
		t.Fatalf("setup: want none 1.2x faster than lz4: %v", down)
	}
	cases := []struct {
		name        string
		r           simRegime
		start, want CompressionMode
	}{
		{"none to lz4", up, CompressionModeNone, CompressionModeLz4},
		{"lz4 to none", down, CompressionModeLz4, CompressionModeNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			late, held, total := 0, 0, 0
			for seed := uint64(0); seed < 20; seed++ {
				modes := scenario(t, seed, 0.3, tc.start, simSegment{tc.r, 256 + 512})
				if firstIn(modes, 0, tc.want) >= 256 {
					late++
				}
				held += countIn(modes, 256, len(modes), tc.want)
				total += len(modes) - 256
			}
			if late > 2 {
				t.Errorf("%d of 20 runs did not reach %s within 256 frames\n  %v", late, modeName(tc.want), tc.r)
			}
			if frac(held, total) < 0.70 {
				t.Errorf("%.0f%% of frames 256-768 in %s, want at least 70%%\n  %v", 100*frac(held, total), modeName(tc.want), tc.r)
			}
		})
	}
}

// TestCompressionPolicyNearTieSettlesOnCheaperCodec: when the costlier mode
// is 1% faster, a difference far below the noise, the policy settles on the
// mode that costs less CPU for at least 60% of the settled frames, summed
// over 10 runs.
func TestCompressionPolicyNearTieSettlesOnCheaperCodec(t *testing.T) {
	cases := []struct {
		name          string
		r             simRegime
		cheap, costly CompressionMode
	}{
		// lz4's smaller wire size repays its CPU time almost exactly.
		{"none or lz4", regime(simNetwork, 128, 16<<10, 261, 64, 2, 2), CompressionModeNone, CompressionModeLz4},
		// zstd shrinks the wire size a little more than lz4 for more CPU.
		{"lz4 or zstd", regime(simNetwork, 32, 16<<10, 1<<10, 386, 3, 3.6), CompressionModeLz4, CompressionModeZstdLevel1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, k := tc.r.modeThroughput(tc.cheap), tc.r.modeThroughput(tc.costly)
			if k < c || k > 1.03*c {
				t.Fatalf("setup: want %s within 3%% of %s and no slower: %v", modeName(tc.costly), modeName(tc.cheap), tc.r)
			}
			held, total := 0, 0
			for seed := uint64(0); seed < 10; seed++ {
				modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{tc.r, 256 + 1024})
				held += countIn(modes, 256, len(modes), tc.cheap)
				total += len(modes) - 256
			}
			if frac(held, total) < 0.60 {
				t.Fatalf("%.0f%% of settled frames in %s, want at least 60%%\n  %v", 100*frac(held, total), modeName(tc.cheap), tc.r)
			}
		})
	}
}

// TestCompressionPolicyProbeGapsBounded: on incompressible data a network
// bound link keeps checking, with background samples, whether the data
// changed. Over 4,096 frames no stretch without a sample, from the start,
// between samples, or to the end, exceeds 300 frames, and samples continue
// in the second half.
func TestCompressionPolicyProbeGapsBounded(t *testing.T) {
	const n, maxGap = 4096, 300
	for _, ratio := range []float64{1.0, 1.05} {
		r := regime(simNetwork, 128, 4<<10, 4<<10, 512, ratio, ratio)
		for seed := uint64(0); seed < 5; seed++ {
			s := newSim(seed, 0.3)
			p := NewCompressionPolicy()
			mode := CompressionModeNone
			prev, widest, late := 0, 0, 0
			for i := range n {
				if sampleFor(p, r, s.frames) != nil {
					widest = max(widest, i-prev)
					prev = i
					if i >= n/2 {
						late++
					}
				}
				mode = s.step(t, p, r, mode)
			}
			widest = max(widest, n-prev)
			if widest > maxGap || late == 0 {
				t.Errorf("ratio %.2f seed %d: widest gap between samples %d (want at most %d), %d samples in the second half\n  %v",
					ratio, seed, widest, maxGap, late, r)
			}
		}
	}
}

// TestCompressionPolicyReturnsToNoneWhenDataTurnsIncompressible: while
// compressing, with lz4 best or with zstd best, the data stops compressing.
// The policy is back in none within 48 frames in at least 90% of the runs
// that were compressing at the change, and then stays in none for at least
// 90% of the next 448 frames.
func TestCompressionPolicyReturnsToNoneWhenDataTurnsIncompressible(t *testing.T) {
	cases := []struct {
		name         string
		before, then simRegime
	}{
		{"lz4 best", regime(simNetwork, 32, 4<<10, 1<<10, 128, 2.4, 3), regime(simNetwork, 32, 4<<10, 1<<10, 128, 1, 1)},
		{"zstd best", regime(simNetwork, 32, 4<<10, 1<<10, 1<<10, 1.5, 8), regime(simNetwork, 32, 4<<10, 1<<10, 1<<10, 1, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.before.slow(CompressionModeNone) || tc.then.throughput(0) < tc.then.throughput(1) {
				t.Fatalf("setup: want compression right, then none best:\n  %v\n  %v", tc.before, tc.then)
			}
			const before, after = 256, 512
			applicable, late, held := 0, 0, 0
			for seed := uint64(0); seed < 20; seed++ {
				modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{tc.before, before}, simSegment{tc.then, after})
				if modes[before] == CompressionModeNone {
					continue
				}
				applicable++
				if firstIn(modes, before, CompressionModeNone) >= before+48 {
					late++
				}
				held += countIn(modes, before+64, len(modes), CompressionModeNone)
			}
			if applicable < 15 {
				t.Fatalf("only %d of 20 runs were compressing at the change\n  %v", applicable, tc.before)
			}
			if late*10 > applicable || frac(held, applicable*(after-64)) < 0.90 {
				t.Errorf("%d of %d runs took 48 or more frames to reach none; %.0f%% of the later frames in none (want 90%%)\n  %v",
					late, applicable, 100*frac(held, applicable*(after-64)), tc.then)
			}
		})
	}
}

// TestCompressionPolicyDropsToNoneOnLowGain: while compressing under heavy
// noise (lognormal sigma 0.5 and 3% stalled frames), the data stops
// compressing. The compression ratio carries no timing noise, so the policy
// is in none within 8 decisions in at least 95% of the runs that were
// compressing at the change, whichever mode it compressed in. A dip of
// lowGainFrames-1 incompressible frames in compressible data never drops it
// to none.
func TestCompressionPolicyDropsToNoneOnLowGain(t *testing.T) {
	const noise = 0xf3
	cases := []struct {
		name         string
		before, then simRegime
	}{
		{"lz4 best, then incompressible", regime(simNetwork, 32, 4<<10, 1<<10, 128, 2.4, 3), regime(simNetwork, 32, 4<<10, 1<<10, 128, 1, 1)},
		{"zstd best, then CPU bound and incompressible", regime(simNetwork, 32, 4<<10, 1<<10, 1<<10, 1.5, 8), regime(simCPU, 0, 1<<10, 200, 128, 1, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applicable, late := 0, 0
			for seed := uint64(0); seed < 100; seed++ {
				s := newSim(seed, 0.3).noisy(noise)
				p := NewCompressionPolicy()
				mode := CompressionModeNone
				for range 256 {
					mode = s.step(t, p, tc.before, mode)
				}
				if p.home == CompressionModeNone {
					continue
				}
				applicable++
				reached := false
				for i := 0; i < 8 && !reached; i++ {
					mode = s.step(t, p, tc.then, mode)
					reached = mode == CompressionModeNone
				}
				if !reached {
					late++
				}
			}
			if applicable < 50 {
				t.Fatalf("only %d of 100 runs were compressing at the change\n  %v", applicable, tc.before)
			}
			t.Logf("%d of %d compressing runs took over 8 decisions to reach none", late, applicable)
			if late*20 > applicable {
				t.Errorf("%d of %d runs took over 8 decisions to reach none (want at most 5%%)\n  %v", late, applicable, tc.then)
			}
		})
	}
	t.Run("short dip", func(t *testing.T) {
		before, dip := cases[1].before, cases[1].then
		applicable := 0
		for seed := uint64(0); seed < 100; seed++ {
			s := newSim(seed, 0.3).noisy(noise)
			p := NewCompressionPolicy()
			mode := CompressionModeNone
			for range 256 {
				mode = s.step(t, p, before, mode)
			}
			if p.home == CompressionModeNone {
				continue
			}
			applicable++
			for i := 0; i < 16; i++ {
				r := before
				if i < lowGainFrames-1 {
					r = dip
				}
				d := s.decide(p, r, mode)
				checkStep(t, mode, d.Next)
				if d.Reason == "low-gain" && d.Next == CompressionModeNone {
					t.Fatalf("seed %d: a dip of %d incompressible frames dropped %s to none", seed, lowGainFrames-1, modeName(mode))
				}
				mode = d.Next
			}
		}
		if applicable < 50 {
			t.Fatalf("only %d of 100 runs were compressing before the dip\n  %v", applicable, before)
		}
	})
}

// TestCompressionPolicyStatePersistsAcrossWindows: a FileCompression carried
// across window boundaries, as the store carries it, backs off its probes on
// incompressible data, so it compresses far fewer frames than one whose Policy
// is recreated each window, even when the mode is kept. Windows are 32 frames.
func TestCompressionPolicyStatePersistsAcrossWindows(t *testing.T) {
	r := regime(simNetwork, 128, 4<<10, 4<<10, 512, 1, 1)
	const n, window = 4096, 32
	run := func(seed uint64, keepPolicy bool) int {
		s := newSim(seed, 0.3)
		fc := FileCompression{Mode: CompressionModeNone, Policy: *NewCompressionPolicy()}
		compressed := 0
		for i := 0; i < n; i++ {
			if i%window == 0 && !keepPolicy {
				fc.Policy = *NewCompressionPolicy()
			}
			if fc.Mode != CompressionModeNone {
				compressed++
			}
			next := s.decide(&fc.Policy, r, fc.Mode).Next
			checkStep(t, fc.Mode, next)
			fc.Mode = next
		}
		return compressed
	}
	carried, recreated := 0, 0
	for seed := uint64(0); seed < 5; seed++ {
		carried += run(seed, true)
		recreated += run(seed, false)
	}
	if carried*2 > recreated {
		t.Fatalf("a carried policy compressed %d frames, a recreated one %d; want the carried one at most half", carried, recreated)
	}
}
