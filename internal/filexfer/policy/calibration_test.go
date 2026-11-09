package policy

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// The sim's noise is independent from frame to frame, so the error rate of
// each statistical decision the policy makes can be measured directly: how
// often it acts on a regime that did not change, and how soon it reacts to
// one that did. TestPolicyNoiseCalibration reports both per noise class.

// calibrationNoise is one noise class, as sim.noisy decodes its byte, and the
// least stationary efficiency the policy keeps in it.
type calibrationNoise struct {
	name       string
	b          byte
	efficiency float64
}

var calibrationNoises = []calibrationNoise{
	{"uniform 0.2", 0x02, 0.993},
	{"lognormal 0.35", 0x92, 0.985},
	{"outliers 3%", 0x4a, 0.99},
	{"heavy", 0xe6, 0.98},
}

// stationaryRates counts decisions in a regime that never changes. Every
// detection or rollback in it is a false alarm; checks also run on the
// budget's schedule.
type stationaryRates struct {
	frames                  int
	detections, unconfirmed int
	checks, forced          int     // check episodes, and those that skipped the budget
	falseRollbacks          int     // a validated codec that really was faster rolled back
	falseAdoptions          int     // an adopted codec more than 10% slower
	falsePasses             int     // such a codec validated
	falseRestores           int     // a check that restored home from a codec 10% faster, or moved on from none
	falseWins               int     // a check won by a codec 10% slower than home
	checkDelay              []int   // frames from a detection to the check it started
	loss, taken             float64 // seconds recovery charged, and of all frames
	ty                      tally
	// lost is the time over the best mode's, by what the policy was doing:
	// checking, validating, or settled in a worse compressed mode or in none.
	lost [4]float64
	// settled is lostCompressed and lostNone by the mode taken and the best.
	settled [3][3]float64
	// byGap is settled loss by how much slower the mode is than the best:
	// under 1.1x, under 1.25x, under 1.5x, and more.
	byGap [4]float64
}

const (
	lostCheck = iota
	lostValidate
	lostCompressed
	lostNone
)

func runCalibrationStationary(r simRegime, noise byte, seed uint64, rates *stationaryRates) {
	s := newSim(seed, simJitters[noise%4]).noisy(noise)
	p := NewCompressionPolicy()
	mode := CompressionModeNone
	reason, detected := "", -1
	for i := range simSettle + 1024 {
		ready := s.budget.Ready()
		validating, checking := p.shadow.phase == phaseValidate, p.shadow.phase == phaseCheck && mode != p.home
		cause := lostCompressed
		switch {
		case p.shadow.phase == phaseCheck && mode != p.home:
			cause = lostCheck
		case p.shadow.phase == phaseValidate:
			cause = lostValidate
		case mode == CompressionModeNone:
			cause = lostNone
		}
		home := p.home
		d := s.decide(p, r, mode)
		if i >= simSettle {
			rates.lost[cause] += r.time(simRung(mode)) - r.bestTime()
			if cause >= lostCompressed {
				best := 0
				for rung := range 3 {
					if r.time(rung) < r.time(best) {
						best = rung
					}
				}
				rates.settled[simRung(mode)][best] += r.time(simRung(mode)) - r.bestTime()
				g, x := 0, r.time(simRung(mode))/r.bestTime()
				for _, b := range []float64{1.1, 1.25, 1.5} {
					if x >= b {
						g++
					}
				}
				rates.byGap[g] += r.time(simRung(mode)) - r.bestTime()
			}
			rates.frames++
			rates.ty.add(r, mode)
			rates.taken += r.time(simRung(mode))
			rates.loss += d.RecoveryLoss.Seconds()
			switch {
			case d.Reason == "regime":
				rates.detections++
				detected = i
			case d.Reason == "recovery-unconfirmed":
				rates.unconfirmed++
				detected = -1
			case d.Reason == "recovery-baseline" && reason != "recovery-baseline":
				rates.checks++
				if !ready {
					rates.forced++
				}
				if detected >= 0 {
					rates.checkDelay = append(rates.checkDelay, i-detected)
					detected = -1
				}
			case d.Reason == "sample-overestimated" && r.time(simRung(mode)) < r.time(simRung(d.Next)):
				rates.falseRollbacks++
			case d.Reason == "sample-won" && r.time(simRung(d.Next)) > 1.1*r.time(simRung(mode)):
				rates.falseAdoptions++
			case (d.Reason == "recovery-restore" || d.Reason == "recovery-check") && checking &&
				1.1*r.time(simRung(mode)) < r.time(simRung(home)):
				rates.falseRestores++
			case d.Reason == "recovery-won" && r.time(simRung(d.Next)) > 1.1*r.time(simRung(home)):
				rates.falseWins++
			}
			if validating && p.shadow.phase != phaseValidate && d.Next == mode &&
				r.time(simRung(mode)) > 1.1*r.time(simRung(p.shadow.previous)) {
				rates.falsePasses++
			}
		}
		reason, mode = d.Reason, d.Next
	}
}

// calibrationShift is one change between regimes: a path clamp, a
// bottleneck that moves down, or new data on the same path. prompt marks
// the clamps large enough that the policy must leave within simReact.
type calibrationShift struct {
	name   string
	prompt bool
	apply  func(r simRegime, data []byte) simRegime
}

var calibrationShifts = []calibrationShift{
	{"clamp 1.25x", false, clampBy(1.25)},
	{"clamp 1.5x", true, clampBy(1.5)},
	{"clamp 2x", true, clampBy(2)},
	{"clamp 4x", true, clampBy(4)},
	{"network to disk", false, func(r simRegime, _ []byte) simRegime {
		if r.b == simNetwork {
			r.b = simDisk
		}
		return r
	}},
	{"new data", false, func(r simRegime, data []byte) simRegime {
		n := regimeFromBytes(data)
		n.b, n.rate = r.b, r.rate
		return n
	}},
}

func clampBy(f float64) func(simRegime, []byte) simRegime {
	return func(r simRegime, _ []byte) simRegime {
		if r.b == simCPU {
			r.b, r.rate = simNetwork, simRates[3]
		}
		r.rate /= f
		return r
	}
}

// reactionRates records, per change that leaves the current mode simSlow
// times slower than the best and visibly changed, how many frames the policy
// took to leave it.
type reactionRates struct {
	applicable int
	left       []int // frames to leave, for the runs that left within 512
}

func runCalibrationReaction(before, after simRegime, noise byte, seed uint64, rates *reactionRates) {
	s := newSim(seed, simJitters[noise%4]).noisy(noise)
	p := NewCompressionPolicy()
	mode := CompressionModeNone
	for range 256 {
		mode = s.decide(p, before, mode).Next
	}
	if !after.slow(mode) || moved(before, after, mode) < simVisible {
		return
	}
	rates.applicable++
	from := mode
	for i := range simHeavyLimit {
		mode = s.decide(p, after, mode).Next
		if mode != from {
			rates.left = append(rates.left, i+1)
			return
		}
	}
}

func (r reactionRates) within(n int) float64 {
	k := 0
	for _, f := range r.left {
		if f <= n {
			k++
		}
	}
	return float64(k) / float64(max(1, r.applicable))
}

func percentile(v []int, q float64) int {
	if len(v) == 0 {
		return -1
	}
	v = slices.Clone(v)
	slices.Sort(v)
	return v[min(len(v)-1, int(q*float64(len(v))))]
}

// TestPolicyNoiseCalibration measures the policy's false alarms in
// stationary regimes and its reaction to realistic changes, per noise class.
// It logs a table; the bounds it asserts catch regressions, not targets:
// each class's stationary efficiency, reaction within simReact to clamps of
// 1.5x or more in 95% of runs, and to every change within simHeavyLimit.
func TestPolicyNoiseCalibration(t *testing.T) {
	if testing.Short() {
		t.Skip("calibration runs thousands of simulated transfers")
	}
	const regimes = 400
	for _, nc := range calibrationNoises {
		var st stationaryRates
		rng := rand.New(rand.NewPCG(31, uint64(nc.b)))
		for i := range regimes {
			r := regimeFromBytes(sweepBytes(rng, simRegimeBytes))
			for run := range uint64(2) {
				runCalibrationStationary(r, nc.b, uint64(i)<<8|run, &st)
			}
		}
		perK := func(n int) float64 { return 1000 * float64(n) / float64(st.frames) }
		lost := func(c int) float64 { return 100 * st.lost[c] / st.taken }
		t.Logf("%-14s stationary: efficiency %.4f, check loss %.3f%%; per 1,000 frames: %.2f detections, %.2f unconfirmed, %.2f checks (%.2f forced), %.2f false rollbacks, %.2f false adoptions; detection to check p50 %d p90 %d frames",
			nc.name, st.ty.efficiency(), 100*st.loss/st.taken, perK(st.detections), perK(st.unconfirmed),
			perK(st.checks), perK(st.forced), perK(st.falseRollbacks), perK(st.falseAdoptions),
			percentile(st.checkDelay, 0.5), percentile(st.checkDelay, 0.9))
		t.Logf("%-14s per 1,000 frames: %.2f worse codecs validated, %.2f checks that lost with a faster codec, %.2f won with a slower one",
			nc.name, perK(st.falsePasses), perK(st.falseRestores), perK(st.falseWins))
		t.Logf("%-14s time lost: %.2f%% checking, %.2f%% validating, %.2f%% in a worse compressed mode, %.2f%% in none",
			nc.name, lost(lostCheck), lost(lostValidate), lost(lostCompressed), lost(lostNone))
		t.Logf("%-14s settled loss by slowdown: <1.1x %.2f%%, <1.25x %.2f%%, <1.5x %.2f%%, more %.2f%%",
			nc.name, 100*st.byGap[0]/st.taken, 100*st.byGap[1]/st.taken, 100*st.byGap[2]/st.taken, 100*st.byGap[3]/st.taken)
		t.Logf("%-14s settled loss (taken→best): none→lz4 %.2f%%, none→zstd %.2f%%, lz4→none %.2f%%, lz4→zstd %.2f%%, zstd→none %.2f%%, zstd→lz4 %.2f%%",
			nc.name, 100*st.settled[0][1]/st.taken, 100*st.settled[0][2]/st.taken, 100*st.settled[1][0]/st.taken,
			100*st.settled[1][2]/st.taken, 100*st.settled[2][0]/st.taken, 100*st.settled[2][1]/st.taken)
		if eff := st.ty.efficiency(); eff < nc.efficiency {
			t.Errorf("%s: stationary efficiency %.4f, want at least %.3f", nc.name, eff, nc.efficiency)
		}
		for _, sh := range calibrationShifts {
			var rr reactionRates
			rng := rand.New(rand.NewPCG(37, uint64(nc.b)))
			for i := range regimes {
				before := regimeFromBytes(sweepBytes(rng, simRegimeBytes))
				after := sh.apply(before, sweepBytes(rng, simRegimeBytes))
				for run := range uint64(4) {
					runCalibrationReaction(before, after, nc.b, uint64(i)<<8|run, &rr)
				}
			}
			t.Logf("%-14s %-16s %4d applicable: left within 24 %.2f, 128 %.2f, 512 %.2f; p50 %d p90 %d frames",
				nc.name, sh.name, rr.applicable, rr.within(simReact), rr.within(128), rr.within(simHeavyLimit),
				percentile(rr.left, 0.5), percentile(rr.left, 0.9))
			if sh.prompt && rr.within(simReact) < 0.95 {
				t.Errorf("%s, %s: left within %d frames in %.2f of runs, want 0.95", nc.name, sh.name, simReact, rr.within(simReact))
			}
			if rr.applicable > 0 && rr.within(simHeavyLimit) < 1 {
				t.Errorf("%s, %s: %.2f of runs left within %d frames, want all", nc.name, sh.name, rr.within(simHeavyLimit), simHeavyLimit)
			}
		}
	}
}
