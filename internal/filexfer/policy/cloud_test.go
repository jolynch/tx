package policy

import (
	"fmt"
	"testing"
)

// Cloud scenarios replay the transfers docs/arch/CLOUD.md models, per data
// stream: an i4i.xlarge pair with 16 streams and an i4i.4xlarge pair with 64,
// so each stream gets a quarter of a vCPU. The paths only slow: a network
// burst gives way to the receiver's disk or to baseline bandwidth.
// TestPolicyCloudScenarios is the policy's acceptance test on realistic
// transfers; the fuzz properties cover inputs no cloud produces.

// cloudData is a kind of file: its lz4 and zstd ratios.
type cloudData struct {
	name      string
	lz4, zstd float64
}

var cloudDatas = []cloudData{
	{"incompressible", 1, 1},
	{"db pages", 1.02, 2},
	{"silesia", 2.224, 2.86},
	{"logs", 4, 8},
}

// cloudPhase is one stretch of a path: its bottleneck and rate in MiB/s per
// stream, held for frames 4 MiB frames.
type cloudPhase struct {
	b      simBottleneck
	rate   float64
	frames int
}

type cloudPath struct {
	name   string
	phases []cloudPhase
	window bool // its network phases are receive-window clamps (simRegime.window)
}

// Per-stream rates: CLOUD.md's aggregate rates over the pair's streams.
var cloudPaths = []cloudPath{
	{"xlarge: burst, disk 33, network 14", []cloudPhase{{simCPU, 0, 768}, {simDisk, 525.0 / 16, 768}, {simNetwork, 224.0 / 16, 1536}}, false},
	{"xlarge: 30 s burst, network 14", []cloudPhase{{simCPU, 0, 256}, {simNetwork, 224.0 / 16, 2048}}, false},
	{"4xlarge: burst, network 17", []cloudPhase{{simCPU, 0, 768}, {simNetwork, 1118.0 / 64, 2048}}, false},
	{"EBS: disk 75, disk 37", []cloudPhase{{simDisk, 1192.0 / 16, 768}, {simDisk, 596.0 / 16, 1536}}, false},
	{"steady network 14", []cloudPhase{{simNetwork, 224.0 / 16, 2048}}, false},
	{"network 56 clamps to 14", []cloudPhase{{simNetwork, 56, 1024}, {simNetwork, 14, 1024}}, false},
	// A cross-region stream behind a receive window below its 70 ms
	// bandwidth-delay product: 8 MiB a round trip, over 16 streams.
	{"cross-region window 7", []cloudPhase{{simNetwork, 8 / 0.070 / 16, 2048}}, true},
}

// cloudRegime is a phase carrying data: a stream reads at 90 MiB/s (the
// pipeline's 360 MiB/s per vCPU over four streams), and encodes lz4 at 38
// and zstd at 29.5 MiB/s (153 and 118 per vCPU).
func cloudRegime(path cloudPath, ph cloudPhase, d cloudData) simRegime {
	r := regime(ph.b, ph.rate, 90, 38, 29.5, d.lz4, d.zstd)
	r.window = path.window && ph.b == simNetwork
	return r
}

// cloudResult sums a scenario's time over runs: the policy's, the oracle's
// (the best mode in each phase), and each fixed codec's.
type cloudResult struct {
	taken, oracle float64
	fixed         [3]float64
	slow          int // frames in a mode simSlow times slower than the best
	frames        int
}

// runCloud sends a path's phases as files of fileFrames frames each (0: one
// file), each a fresh policy seeded from the connection's link estimate, as
// SEND does.
func runCloud(path cloudPath, d cloudData, noise byte, fileFrames int, seed uint64, res *cloudResult) {
	s := newSim(seed, simJitters[noise%4]).noisy(noise)
	p := NewCompressionPolicy()
	mode := CompressionModeNone
	var link LinkPrior
	var linked bool
	frame := 0
	for _, ph := range path.phases {
		r := cloudRegime(path, ph, d)
		for range ph.frames {
			if fileFrames > 0 && frame > 0 && frame%fileFrames == 0 {
				if l, ok := p.Link(); ok {
					link, linked = l, true
				}
				p, mode = NewCompressionPolicy(), CompressionModeNone
				if linked {
					p.SeedLink(link)
				}
			}
			t := r.time(simRung(mode))
			res.taken += t
			res.oracle += r.bestTime()
			for rung := range 3 {
				res.fixed[rung] += r.time(rung)
			}
			if t > simSlow*r.bestTime() {
				res.slow++
			}
			res.frames++
			mode = s.decide(p, r, mode).Next
			frame++
		}
	}
}

// TestPolicyCloudScenarios logs, per path and data kind, the policy's time
// efficiency against the oracle and against the best fixed codec, for one
// 12 GiB file and for 512 MiB files, under uniform and lognormal noise, and
// asserts the efficiency against the oracle.
func TestPolicyCloudScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("cloud scenarios run hundreds of simulated transfers")
	}
	const runs = 8
	for _, files := range []int{0, 128} {
		for _, noise := range []byte{0x02, 0x92} {
			var worst float64 = 1
			var worstName string
			for _, path := range cloudPaths {
				for _, d := range cloudDatas {
					var res cloudResult
					for run := range uint64(runs) {
						runCloud(path, d, noise, files, run+1, &res)
					}
					eff := res.oracle / res.taken
					fixed := min(res.fixed[0], res.fixed[1], res.fixed[2])
					name := fmt.Sprintf("%-36s %-15s", path.name, d.name)
					t.Logf("files=%-3d noise=%#02x %s vs oracle %.3f, vs best fixed %.3f, slow frames %.1f%%",
						files, noise, name, eff, fixed/res.taken, 100*float64(res.slow)/float64(res.frames))
					if eff < worst {
						worst, worstName = eff, name
					}
					// Regression bounds, a little under what the policy reaches:
					// a 512 MiB file starts at none, so it spends its first
					// frames there whatever the data.
					want := 0.95
					if files > 0 {
						want = 0.90
					}
					if files > 0 && path.window {
						// Behind a small receive window each file starts in
						// none, as behind a disk, and a codec must deliver
						// half its predicted gain to validate.
						want = 0.85
					}
					if eff < want {
						t.Errorf("files=%d noise=%#02x %s: efficiency %.3f against the oracle, want at least %.2f", files, noise, name, eff, want)
					}
				}
			}
			t.Logf("files=%-3d noise=%#02x WORST %.3f: %s", files, noise, worst, worstName)
		}
	}
}
