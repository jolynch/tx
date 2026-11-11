package policy

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
)

// These tests check the policy's behavioral contract through its public API
// alone: they drive Decide with frames from a model of a link and judge the
// modes it picks by the model's throughput. They do not depend on the policy's
// internals, so they hold across rewrites of it.

const (
	mib = float64(1 << 20)
	gib = float64(1 << 30)

	// simFrame is the logical size of one frame.
	simFrame = 4 << 20
	// simFastLink is the wire rate when the network is not the bottleneck.
	simFastLink = 16 * gib
	// simDiskLinkFactor is how much faster than the receiver's disk the
	// network runs when the disk is the bottleneck.
	simDiskLinkFactor = 4

	// simSettle is how many frames after a regime change the policy may take
	// to settle before its choices count.
	simSettle = 32
	// simGood is how close to the best mode's throughput a mode must be to
	// count as a good choice (the stationary test's good-mode count; the
	// regret and stability properties judge time efficiency instead).
	simGood = 1.1
	// The fuzz properties are directional. Sending uncompressed is the safe
	// default: a policy slow to start compressing loses at most what
	// compression could have saved, which TestPolicyCloudScenarios and the
	// quality sweep measure on seeded inputs. Compressing when it does not pay
	// can make a transfer slower than not compressing at all, so the fuzzers
	// bound that on every input.
	//
	// simOverNone is how much more time than sending uncompressed throughout
	// the policy may take, summed over an input's settled frames and runs
	// (simHeavyOverNone under heavy-tailed noise, of cappedOverNone). It
	// covers holding a codec slower than none, thrashing, and the cost of
	// checks.
	simOverNone      = 0.05
	simHeavyOverNone = 0.15
	// simOverShare is the share of counted frames that may run compressed
	// while none would be simSlow times faster (overCompressed): a policy
	// stuck spending CPU that the path does not repay. A codec that still
	// beats none, though a cheaper codec would beat it, is inefficiency the
	// quality sweep and the cloud scenarios measure instead. Under
	// heavy-tailed noise, where checks lose to stalls, simHeavyOverShare.
	simOverShare      = 0.10
	simHeavyOverShare = 0.25
	// simStuck is the share of reaction runs that may stay in a slow mode
	// longer than simReact decisions, per fuzz input, but never less than
	// one run.
	simStuck = 0.10
	// simHeavyReact is the decision budget of the reaction quality sweep
	// under high noise, where a stall can hide a slow mode for a while.
	simHeavyReact = 48
	// simHeavyLimit is the reaction fuzzers' disaster bound under high noise.
	// A searching fuzzer always finds batches whose noise hides a change
	// for longer, so they check only that no run sticks: the policy's worst
	// found inputs leave within about 190 frames.
	simHeavyLimit = 512
	// simHeavyChanges bounds codec changes per 1,000 settled frames of one
	// stationary fuzz input: a disaster bound on thrashing.
	simHeavyChanges = 60
	// simSniffEvery is how many none frames pass between sniffs.
	simSniffEvery = 8
	// simSlow is how much slower than the best a mode must be before the
	// policy has to leave it within simReact frames.
	simSlow = 1.5
	// simReact is the number of decisions within which the policy must leave
	// a mode that a regime change made simSlow times slower than the best,
	// when the change moved that mode's throughput by simPrompt or more.
	simReact = 24
	// simPrompt is the change in the current mode's throughput that the
	// policy detects within simReact decisions. A smaller visible change
	// takes the detector longer under 0.3 jitter (TestPolicyNoiseCalibration
	// asserts prompt reactions only from 1.5x clamps), so it must be left
	// within simSubtleReact frames of the recovery budget repaying the
	// debt outstanding at the change (debtFrames): the detector may miss it,
	// and then only a periodic check, which waits for that debt, leaves the
	// mode.
	simPrompt      = 1.5
	simSubtleReact = 128
	// simVisible is how far a regime change must move the throughput of the
	// mode the policy is in for the policy to be able to see it. A change
	// that moves it less, such as data turning compressible while the policy
	// sends uncompressed, shows only to a probe, and C2 lets probes back off
	// past any fixed reaction bound.
	simVisible = 1.25
	// simRuns is how many jittered runs the regret and stability properties
	// sum over. The contract bounds the policy's typical behavior; a fuzzer
	// free to pick one unlucky jitter seed would find a tail that any
	// policy has.
	simRuns = 8
)

// FuzzCompressionPolicyRegret checks C1 against over-compression: one
// policy, carried across regime changes and window boundaries the way a
// file's state is, takes at most simOverNone more time than sending
// uncompressed after each change's settling period, summed over simRuns
// jittered runs, and runs at most simOverShare of those frames compressed
// while none would be simSlow times faster. Compressing too little is
// allowed; TestPolicyCloudScenarios and the quality sweep measure
// it on seeded inputs. Frames
// in none report a sniff ratio every simSniffEvery frames. The
// FileCompression is saved and reloaded by value at window boundaries.
//
// Input: jitter and noise (see sim.noisy), window size, two seed bytes, then
// segments of a regime and a length byte. Each segment's path is clamped to
// the one before it (clampedPath).
func FuzzCompressionPolicyRegret(f *testing.F) {
	seg := func(regime []byte, n byte) []byte { return append(append([]byte{}, regime...), n) }
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	head := []byte{3, 1, 0, 0}
	f.Add(cat(head, seg([]byte{0, 1, 0, 2, 2, 1, 2}, 200)))                                        // network, incompressible, cheap lz4
	f.Add([]byte("70\aa02X0020000200000"))                                                         // a check's baseline must not take on two frames' jitter
	f.Add(cat(head, seg([]byte{0, 1, 1, 2, 2, 1, 2}, 200)))                                        // network, ratio 1.05
	f.Add(cat(head, seg([]byte{0, 1, 2, 1, 1, 1, 2}, 200)))                                        // network, ratio 2
	f.Add(cat(head, seg([]byte{0, 0, 3, 1, 1, 1, 2}, 200)))                                        // slow network, ratio 3
	f.Add(cat(head, seg([]byte{0, 0, 5, 1, 1, 1, 2}, 200)))                                        // slow network, ratio 250: prepare far exceeds write
	f.Add(cat(head, seg([]byte{1, 2, 2, 1, 1, 1, 2}, 200)))                                        // receiver disk, ratio 2
	f.Add(cat(head, seg([]byte{2, 0, 3, 1, 0, 0, 1}, 200)))                                        // CPU bound, slow codecs
	f.Add(cat(head, seg([]byte{0, 1, 2, 1, 1, 1, 2}, 200), seg([]byte{1, 1, 2, 1, 1, 1, 2}, 200))) // network then receiver disk, ratio 2
	f.Add(cat(head, seg([]byte{0, 1, 3, 1, 1, 1, 2}, 200), seg([]byte{1, 1, 2, 1, 1, 1, 2}, 200))) // network ratio 3, then receiver disk ratio 2
	f.Add(cat(head, seg([]byte{1, 0, 0, 1, 1, 1, 2}, 160), seg([]byte{0, 0, 3, 1, 1, 1, 2}, 160))) // receiver disk, then network
	f.Add(cat(head, seg([]byte{1, 2, 2, 1, 1, 1, 2}, 160), seg([]byte{0, 1, 2, 1, 1, 1, 2}, 200))) // receiver disk, then a slower network (CLOUD.md)
	f.Add(cat(head, seg([]byte{0, 1, 3, 1, 1, 1, 2}, 150), seg([]byte{0, 1, 0, 1, 1, 1, 2}, 200))) // compressible, then incompressible
	f.Add(cat(head, seg([]byte{0, 3, 3, 1, 1, 1, 2}, 200), seg([]byte{0, 0, 3, 1, 1, 1, 2}, 200))) // burst network credits run out
	// Heavy-tailed noise: outliers, lognormal jitter, and both.
	f.Add(cat([]byte{0x43, 1, 0, 0}, seg([]byte{0, 0, 3, 1, 1, 1, 2}, 200), seg([]byte{0, 0, 0, 1, 1, 1, 2}, 200)))
	f.Add(cat([]byte{0xB3, 1, 0, 0}, seg([]byte{0, 1, 2, 1, 1, 1, 2}, 200), seg([]byte{1, 1, 2, 1, 1, 1, 2}, 200)))
	// Inputs the fuzzer found against earlier versions of the policy.
	f.Add([]byte("21\x00(00211100"))
	f.Add([]byte("710800211100"))
	f.Add([]byte("7100002111000"))
	f.Add([]byte("710\x0011200100"))
	f.Add([]byte("11910791220\x00"))
	f.Add([]byte("010002221100"))
	f.Add([]byte("71A70192011\x020"))
	f.Add([]byte("711001211200"))
	f.Add([]byte("710200211100"))
	f.Add([]byte("000000A00210"))
	f.Add([]byte("01000190011\x02"))
	f.Add([]byte("\xeb00000A12220"))      // noise must not repeatedly reopen costly wire trials
	f.Add([]byte("00002000000000X20220")) // ratio changes must refresh the clipped baseline
	f.Add([]byte("\xeb00100A12220"))      // a noise burst must not erase expensive-trial backoff
	f.Add([]byte("701701910010"))
	f.Add([]byte("21C00120010\x020"))
	f.Add([]byte("20a20121220\x020"))
	f.Add([]byte("107107A0020 0"))
	f.Add([]byte("70170121210 "))
	f.Add([]byte("71700121210\x020"))
	// Found against the round 3 policy, when the bounds were per-input
	// quality: disasters only if they miss the bounds above.
	f.Add([]byte("70\x00\x0000A00200")) // uniform, efficiency 0.946
	f.Add([]byte("\xc802000A11120"))
	f.Add([]byte("C0770090000\x01"))
	f.Add([]byte("709B00A10100"))
	// High noise, found against the swarm-style bounds.
	f.Add([]byte("\xff0010020000\x020"))
	f.Add([]byte("\xf300100200100"))
	// Low noise, found by fuzz-long in CI.
	f.Add([]byte("70\aa02X001000020000a"))    // ratio 1.3: a none-lz4 coin flip
	f.Add([]byte("\xeb00002A02220"))          // a baseline restored after a failed validation is not settled: no restore/check cycle
	f.Add([]byte("\xee0\xf9\xf900A2222\x10")) // heavy noise: a check that costs a hundred frames needs more than noise to run
	f.Add([]byte("\xeb0c002A01220"))          // stalls that fail a much cheaper codec must not back it off for 256 frames
	f.Add([]byte("20001120010\x940290011C"))  // a network path that writes clearly faster than the disk before it
	f.Add([]byte("00001291111\x8e02A01100"))  // nor when the path changes at the same speed
	f.Add([]byte("\xeb00\x8401A00210"))       // heavy noise: three stalls fail a codec 12 times faster in one run
	// Inputs the directional properties found before the bottleneck class.
	f.Add([]byte("\xf70A212X0111X"))                          // heavy noise must stay near none's time
	f.Add([]byte("\xf808\x0620A0222\xad20A2222\xad20A00110")) // heavy noise: one stall must not fail a much cheaper codec's validation
	f.Add([]byte("\xf808\x0620A0222\xad20A2222\xad20A02020")) // heavy noise: a stalled validation waits out the budget, with lz4 23 times slower than none
	f.Add([]byte("\xeb0A0070000010700000\xac2092210\xc020X2010X0"))
	f.Fuzz(func(t *testing.T, in []byte) {
		rg, ok := runRegret(t, in, simRuns)
		if !ok {
			return
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "\n  jitter %.1f noise %#x window %d, %d runs", rg.jitter, in[0]>>4, rg.window, simRuns)
		for i, sg := range rg.segs {
			fmt.Fprintf(&sb, "\n  %d frames, efficiency %.3f, %.1f%% over none, %d over-compressed: %v",
				sg.n, rg.per[i].efficiency(), 100*rg.per[i].overNone(), rg.per[i].over, sg.r)
		}
		checkOverCompression(t, "C1", rg.all, heavy(in[0]), sb.String())
	})
}

// regretResult is one regret input's outcome.
type regretResult struct {
	segs   []simSegment
	per    []tally // per segment
	all    tally
	jitter float64
	window int
}

// runRegret decodes a regret input and runs it runs times. It reports false
// when the input is too short or counts no frames.
func runRegret(t *testing.T, in []byte, runs int) (regretResult, bool) {
	const segBytes = simRegimeBytes + 1
	var rg regretResult
	if len(in) < 4+segBytes {
		return rg, false
	}
	rg.jitter = simJitters[in[0]%4]
	rg.window = [4]int{0, 8, 32, 128}[in[1]%4]
	seed := uint64(in[2]) | uint64(in[3])<<8
	for i := 4; i+segBytes <= len(in) && len(rg.segs) < 8; i += segBytes {
		r := regimeFromBytes(in[i:])
		if len(rg.segs) > 0 {
			r = clampedPath(rg.segs[len(rg.segs)-1].r, r)
		}
		rg.segs = append(rg.segs, simSegment{r: r, n: 128 + int(in[i+simRegimeBytes])})
	}
	rg.per = make([]tally, len(rg.segs))
	for run := range runs {
		regretRun(t, newSim(seed<<8|uint64(run), rg.jitter).noisy(in[0]), rg.segs, rg.window, rg.per)
	}
	for _, ty := range rg.per {
		rg.all.merge(ty)
	}
	return rg, rg.all.counted > 0
}

// regretRun drives one FileCompression through segs and adds, per segment,
// the frames counted toward C1.
func regretRun(t *testing.T, s *sim, segs []simSegment, window int, per []tally) {
	t.Helper()
	fc := FileCompression{Mode: CompressionModeNone, Policy: *NewCompressionPolicy()}
	saved := fc
	frame := 0
	for si, sg := range segs {
		r := sg.r
		settleUntil := simSettle
		// A change the current mode cannot see counts only from the
		// policy's first move after it.
		blind := si > 0 && moved(segs[si-1].r, r, fc.Mode) < simVisible
		changeMode := fc.Mode
		for i := 0; i < sg.n; i++ {
			if window > 0 && frame%window == 0 {
				fc = saved
			}
			mode := fc.Mode
			if blind && mode != changeMode {
				blind = false
				settleUntil = i + simSettle
			}
			if !blind && i >= settleUntil {
				per[si].add(r, mode)
			}
			next := s.decide(&fc.Policy, r, mode).Next
			checkStep(t, mode, next)
			fc.Mode = next
			frame++
			if window > 0 && frame%window == 0 {
				saved = fc
			}
		}
	}
}

// FuzzCompressionPolicyStationary checks C4 and C2 in one regime held for
// 1,056 frames from a fresh policy, summed over simRuns jittered runs: after
// settling, at most simHeavyChanges adopted codec changes per 1,000 frames,
// and over-compression bounded as in FuzzCompressionPolicyRegret; on
// incompressible data, at most 5% of all frames compressed.
//
// Input: jitter and noise (see sim.noisy), two seed bytes, a regime.
func FuzzCompressionPolicyStationary(f *testing.F) {
	f.Add([]byte("\xff1700X0200"))                 // noise must preserve upward as well as downward backoff
	f.Add([]byte("1010722120"))                    // cheaper-codec credit must not overlap upward wins
	f.Add([]byte("A1101A0000"))                    // tiny write-cost changes must not reopen comparisons
	f.Add([]byte{3, 0, 0, 0, 1, 0, 2, 2, 1, 2})    // network, incompressible, cheap lz4
	f.Add([]byte{3, 0, 0, 0, 3, 1, 2, 2, 1, 2})    // fast network, ratio 1.05
	f.Add([]byte{3, 0, 0, 0, 1, 2, 1, 1, 1, 2})    // network, ratio 2
	f.Add([]byte{3, 0, 0, 0, 0, 5, 1, 1, 1, 2})    // slow network, ratio 250
	f.Add([]byte{3, 0, 0, 0, 1, 3, 0, 0, 1, 0})    // network, ratio 3, lz4 gains half, slow read
	f.Add([]byte{3, 0, 0, 1, 2, 2, 1, 1, 1, 2})    // receiver disk, ratio 2
	f.Add([]byte{3, 0, 0, 2, 0, 3, 1, 0, 0, 1})    // CPU bound, slow codecs
	f.Add([]byte{3, 0, 0, 2, 0, 0, 1, 2, 2, 0})    // CPU bound, incompressible, slow read
	f.Add([]byte{0, 0, 0, 0, 1, 4, 1, 1, 0, 2})    // no jitter, network, ratio 30, slow zstd
	f.Add([]byte{0x43, 0, 0, 0, 1, 2, 1, 1, 1, 2}) // outliers, network, ratio 2
	f.Add([]byte{0xB3, 0, 0, 0, 0, 0, 2, 2, 1, 2}) // lognormal jitter, network, incompressible
	f.Add([]byte{0xF3, 0, 0, 0, 1, 3, 1, 1, 1, 2}) // both, network, ratio 3
	// Inputs the fuzzer found against earlier versions of the policy.
	f.Add([]byte("7\x00C0221110"))
	f.Add([]byte("7'C00A1000"))
	f.Add([]byte("\xff\xff00120010"))    // found against the round 3 policy
	f.Add([]byte("\xeb7000A2222"))       // high ratios make repeated false alarms expensive
	f.Add([]byte("2101020020"))          // disk-bound tie: samples and checks trade codecs
	f.Add([]byte("1001120201"))          // a disk-bound regime with small gains
	f.Add([]byte("1001220212"))          // the same thrashing in another disk-bound regime
	f.Add([]byte("1\xb1\"1220212"))      // a validation that loses to a regime detected during it must keep its backoff
	f.Add([]byte("\xeb0012A1011"))       // a disk-bound path drops to none on the receiver class
	f.Add([]byte("\xeb0011A1000"))       // the same in another disk-bound regime
	f.Add([]byte("\xeb\x03\xe812X1111")) // heavy noise must stay near none's time
	f.Fuzz(func(t *testing.T, in []byte) {
		st, ok := runStationary(t, in, simRuns)
		if !ok {
			return
		}
		// Adopted codecs must not thrash.
		if st.changes*1000 > st.counted*simHeavyChanges {
			t.Errorf("C4: %d codec changes in %d settled frames, want at most %d per 1,000\n  %v", st.changes, st.counted, simHeavyChanges, st.r)
		}
		if st.r.incompressible() && st.compressed*20 > st.frames {
			t.Errorf("C2: %d of %d frames compressed on incompressible data, want at most 5%%\n  %v",
				st.compressed, st.frames, st.r)
		}
		checkOverCompression(t, "C4", st.ty, heavy(in[0]), fmt.Sprintf("\n  %v", st.r))
	})
}

// stationaryResult is one stationary input's outcome.
type stationaryResult struct {
	r                  simRegime
	ty                 tally   // settled frames
	changes, counted   int     // adopted codec changes and frames after settling
	loss               float64 // seconds of settled time recovery checks charged
	compressed, frames int     // frames in any compressing mode, and all frames
}

// runStationary decodes a stationary input and runs it runs times. It reports
// false when the input is too short.
func runStationary(t *testing.T, in []byte, runs int) (stationaryResult, bool) {
	var st stationaryResult
	if len(in) < 3+simRegimeBytes {
		return st, false
	}
	jitter := simJitters[in[0]%4]
	seed := uint64(in[1]) | uint64(in[2])<<8
	st.r = regimeFromBytes(in[3:])
	const settled = 1024
	st.frames = runs * (simSettle + settled)
	st.counted = runs * settled
	for run := 0; run < runs; run++ {
		s := newSim(seed<<8|uint64(run), jitter).noisy(in[0])
		p := NewCompressionPolicy()
		mode := CompressionModeNone
		for i := 0; i < simSettle+settled; i++ {
			if mode != CompressionModeNone {
				st.compressed++
			}
			if i >= simSettle {
				st.ty.add(st.r, mode)
			}
			home := p.home
			d := s.decide(p, st.r, mode)
			checkStep(t, mode, d.Next)
			if i >= simSettle {
				st.loss += d.RecoveryLoss.Seconds()
				if p.home != home {
					st.changes++
				}
			}
			mode = d.Next
		}
	}
	return st, true
}

// FuzzCompressionPolicyReaction checks C3 against over-compression: after
// any history, a regime change that leaves the current mode compressed while
// none would be simSlow times faster, and that mode can see, sends the
// policy out of it within simReact decisions (simHeavyLimit under
// heavy-tailed noise; simSubtleReact when the change moved the mode's
// throughput less than simPrompt, or landed on a home too young to detect
// it) in all but simStuck of 40 jittered runs.
// A mode compressing too little may stay; see simOverNone.
//
// Input: jitter and noise (see sim.noisy), a seed byte, up to three history
// segments of a regime and a length byte (32 + 4x frames), then the new regime.
// Each regime's path is clamped to the one before it (clampedPath).
func FuzzCompressionPolicyReaction(f *testing.F) {
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	head := []byte{3, 0}
	f.Add(cat(head, []byte{0, 3, 0, 1, 1, 1, 2, 255}, []byte{0, 0, 3, 1, 1, 1, 2})) // long incompressible history, then the link slows
	f.Add(cat(head, []byte{0, 2, 4, 1, 0, 1, 2, 100}, []byte{1, 2, 4, 1, 0, 1, 2})) // compression right, then receiver disk
	f.Add(cat(head, []byte{2, 0, 3, 1, 0, 0, 1, 200}, []byte{0, 0, 3, 1, 0, 0, 1})) // CPU bound, then a slow link
	f.Add(cat(head, []byte{0, 3, 2, 1, 1, 1, 2, 30}, []byte{0, 0, 2, 1, 1, 1, 2}))  // burst credits run out
	f.Add([]byte("70129102000722122"))                                              // a disk-bound path across a data change
	f.Add([]byte("201020010\x000192200"))                                           // a change during validation must still reach a cheaper codec
	f.Add([]byte("7010X0010\x0000A0101"))                                           // the same, when encode and write time move in opposite directions
	f.Add([]byte("7B1020000001000000010000000090002"))                              // a change under simPrompt gets simSubtleReact
	f.Add([]byte("000002100\x000\xe05A200"))                                        // a network becomes a small receive window: compress again, though it reads as the receiver
	f.Add([]byte("\xe300002100\x000\xe05A000"))                                     // the same under heavy noise
	f.Add([]byte("00019120000221100\x000220001"))
	f.Add([]byte("0001A10009019120000221100\x000220000")) // the same with 10 s of debt: the periodic check waits about 250 frames for it                                   // a change under simPrompt after two lost checks waits for the budget
	f.Add([]byte("7010200000010000000100000\x000090000"))
	f.Add([]byte("1010X0010\x000192200")) // a home whose samples missed must catch up before a change reads against it
	f.Add([]byte("7102X1011\x0212X1000")) // a disk path whose encode outgrew its write still reads as the receiver
	f.Add([]byte("701090000\x0000A1200")) // a slower codec must be left after a change
	f.Add([]byte("7010A0000\x0000A1200"))
	f.Add([]byte("7102X1010\x0202X0000"))    // a change five frames after a switch waits for a periodic check
	f.Add([]byte("\xde10121110\x000220001")) // heavy noise: a change right after a lucky check waits out its debt
	f.Fuzz(func(t *testing.T, in []byte) {
		react := 0
		if len(in) > 0 && heavy(in[0]) {
			react = simHeavyLimit // See simHeavyLimit.
		}
		rc, ok := runReaction(t, in, 40, react)
		if !ok {
			return
		}
		// An input often makes few runs applicable, and then one stuck run
		// alone would exceed simStuck: allow at least one.
		if float64(rc.stuck) > max(1, simStuck*float64(rc.applicable)) {
			var sb strings.Builder
			for _, sg := range rc.segs {
				fmt.Fprintf(&sb, "\n  history %d frames: %v", sg.n, sg.r)
			}
			t.Fatalf("C3: %d of %d runs stayed over %d decisions (%d after a change under %.1fx) compressed though none is %.1fx faster (%s), want at most %.0f%% or one run:%s\n  then: %v",
				rc.stuck, rc.applicable, rc.react, max(rc.react, simSubtleReact), simPrompt, simSlow, rc.failure, 100*simStuck, sb.String(), rc.final)
		}
		if float64(rc.pinned) > max(1, simStuck*float64(rc.clamped)) {
			t.Fatalf("C3: %d of %d runs stayed in none for %d frames behind a small receive window, though a codec is %.1fx faster (%s), want at most %.0f%% or one run\n  then: %v",
				rc.pinned, rc.clamped, simHeavyLimit, simSlow, rc.failure, 100*simStuck, rc.final)
		}
	})
}

// reactionResult is one reaction input's outcome.
type reactionResult struct {
	segs       []simSegment
	final      simRegime
	applicable int // runs whose mode the change made slow and visible
	stuck      int // applicable runs that stayed in that mode
	// clamped counts runs whose final regime is a small receive window that
	// leaves none simSlow times slower than the best, after a history that
	// never read as the receiver; pinned counts those that never compressed
	// again within simHeavyLimit frames.
	clamped, pinned int
	failure         string
	react           int // decision budget of this input's noise class
}

// runReaction decodes a reaction input and runs it runs times. It reports
// false when the input is too short or has no history.
// runReaction reports how many runs stayed in a slow mode for react frames
// after the change, or simReact and simHeavyReact when react is 0.
func runReaction(t *testing.T, in []byte, runs, react int) (reactionResult, bool) {
	const segBytes = simRegimeBytes + 1
	var rc reactionResult
	if len(in) < 2+segBytes+simRegimeBytes {
		return rc, false
	}
	jitter := simJitters[in[0]%4]
	rc.react = simReact
	if heavy(in[0]) {
		rc.react = simHeavyReact
	}
	if react > 0 {
		rc.react = react
	}
	hist := in[2 : len(in)-simRegimeBytes]
	for i := 0; i+segBytes <= len(hist) && len(rc.segs) < 3; i += segBytes {
		r := regimeFromBytes(hist[i:])
		if len(rc.segs) > 0 {
			r = clampedPath(rc.segs[len(rc.segs)-1].r, r)
		}
		rc.segs = append(rc.segs, simSegment{r: r, n: 32 + 4*int(hist[i+simRegimeBytes])})
	}
	if len(rc.segs) == 0 {
		return rc, false
	}
	rc.final = clampedPath(rc.segs[len(rc.segs)-1].r, regimeFromBytes(in[len(in)-simRegimeBytes:]))
	last := rc.segs[len(rc.segs)-1].r
	receiverHistory := false
	for _, sg := range rc.segs {
		receiverHistory = receiverHistory || sg.r.b == simDisk || sg.r.window
	}
	for run := 0; run < runs; run++ {
		s := newSim(uint64(in[1])<<8|uint64(run), jitter).noisy(in[0])
		p := NewCompressionPolicy()
		mode := CompressionModeNone
		for _, sg := range rc.segs {
			for i := 0; i < sg.n; i++ {
				mode = s.step(t, p, sg.r, mode)
			}
		}
		if rc.final.window && !receiverHistory && rc.final.time(0) >= simSlow*rc.final.bestTime() {
			// The class reads as the receiver, as behind a disk, so the
			// policy drops to none; upward samples must still find the
			// codec that shortens the writes.
			rc.clamped++
			wasNone, compressed := false, false
			for i := 0; i < simHeavyLimit && !compressed; i++ {
				mode = s.step(t, p, rc.final, mode)
				compressed = wasNone && mode != CompressionModeNone
				wasNone = wasNone || mode == CompressionModeNone
			}
			if !compressed && mode == CompressionModeNone {
				// A run that never dropped to none was not pinned there.
				rc.pinned++
				if rc.failure == "" {
					rc.failure = fmt.Sprintf("run %d stayed in none behind a small receive window", run)
				}
			}
			continue
		}
		visible := moved(last, rc.final, mode)
		if visible < simVisible || !rc.final.overCompressed(mode) {
			// Only over-compression must end promptly; see simOverNone.
			continue
		}
		rc.applicable++
		react := rc.react
		if visible < simPrompt || p.n < homeFrames {
			// A home younger than homeFrames has not armed its change
			// detector, so only a periodic check, paced by the recovery
			// budget, leaves it: within simSubtleReact frames of repaying
			// the debt earlier checks left.
			react = max(react, simSubtleReact) + debtFrames(&s.budget, rc.final, mode)
		}
		from, left := mode, false
		for i := 0; i < react && !left; i++ {
			mode = s.step(t, p, rc.final, mode)
			left = mode != from
		}
		if !left {
			rc.stuck++
			if rc.failure == "" {
				rc.failure = fmt.Sprintf("run %d stayed in %s", run, modeName(from))
			}
		}
	}
	return rc, true
}

// Quality sweep thresholds: averages over many seeded inputs, which the fuzz
// properties' disaster bounds leave to TestCompressionPolicyQualitySweep.
const (
	sweepEfficiency      = 0.97 // uniform noise
	sweepHeavyEfficiency = 0.95 // heavy-tailed noise
	sweepStuck           = 0.02
	sweepChanges         = 15.0 // per 1,000 settled frames, uniform noise
	sweepHeavyChanges    = 30.0 // heavy-tailed noise

	// How many inputs of each kind a sweep half draws.
	sweepRegret     = 120
	sweepStationary = 60
	sweepReaction   = 200
)

// sweepNoise draws the noise byte of a fuzz input: below 0x40 selects
// uniform jitter alone, from 0x40 up heavy-tailed noise (see sim.noisy).
func sweepNoise(rng *rand.Rand, heavyNoise bool) byte {
	if heavyNoise {
		return byte(0x40 + rng.IntN(0xC0))
	}
	return byte(rng.IntN(0x40))
}

func sweepBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.IntN(256))
	}
	return b
}

// TestCompressionPolicyQualitySweep judges the policy's average quality over
// a fixed, seeded set of random inputs drawn the way the fuzzers' inputs are
// decoded, split into a uniform-noise half and a heavy-tailed half. The
// fuzz properties bound disasters; this bounds the typical case, pooled over
// each half: time efficiency (sweepEfficiency, sweepHeavyEfficiency), the
// share of reaction runs stuck in a slow mode (sweepStuck), and mode changes
// per 1,000 settled frames in stationary regimes (sweepChanges,
// sweepHeavyChanges). It logs the 5th-percentile per-input efficiency.
func TestCompressionPolicyQualitySweep(t *testing.T) {
	for _, half := range []struct {
		name                 string
		heavy                bool
		wantEff, wantChanges float64
		seed                 uint64
	}{
		{"uniform", false, sweepEfficiency, sweepChanges, 1},
		{"heavy", true, sweepHeavyEfficiency, sweepHeavyChanges, 2},
	} {
		t.Run(half.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(half.seed, 0x7377))
			var eff tally
			var per []float64
			for i := 0; i < sweepRegret; i++ {
				in := sweepBytes(rng, 4+(simRegimeBytes+1)*(1+rng.IntN(3)))
				in[0] = sweepNoise(rng, half.heavy)
				if rg, ok := runRegret(t, in, 2); ok {
					eff.merge(rg.all)
					per = append(per, rg.all.efficiency())
				}
			}
			changes, counted := 0, 0
			for i := 0; i < sweepStationary; i++ {
				in := sweepBytes(rng, 3+simRegimeBytes)
				in[0] = sweepNoise(rng, half.heavy)
				if st, ok := runStationary(t, in, 2); ok {
					eff.merge(st.ty)
					per = append(per, st.ty.efficiency())
					changes += st.changes
					counted += st.counted
				}
			}
			applicable, stuck := 0, 0
			for i := 0; i < sweepReaction; i++ {
				in := sweepBytes(rng, 2+(simRegimeBytes+1)*(1+rng.IntN(3))+simRegimeBytes)
				in[0] = sweepNoise(rng, half.heavy)
				if rc, ok := runReaction(t, in, 10, 0); ok {
					applicable += rc.applicable
					stuck += rc.stuck
				}
			}
			sort.Float64s(per)
			p5 := per[len(per)/20]
			perK := 1000 * float64(changes) / float64(counted)
			stuckRate := float64(stuck) / float64(max(1, applicable))
			t.Logf("efficiency %.4f over %d inputs (5th percentile %.3f); stuck %d of %d runs (%.2f%%); %.1f mode changes per 1,000 settled frames",
				eff.efficiency(), len(per), p5, stuck, applicable, 100*stuckRate, perK)
			if eff.efficiency() < half.wantEff {
				t.Errorf("efficiency %.4f, want at least %.2f", eff.efficiency(), half.wantEff)
			}
			if stuckRate > sweepStuck {
				t.Errorf("stuck rate %.2f%% (%d of %d), want at most %.0f%%", 100*stuckRate, stuck, applicable, 100*sweepStuck)
			}
			if perK > half.wantChanges {
				t.Errorf("%.1f mode changes per 1,000 settled frames, want at most %.0f", perK, half.wantChanges)
			}
		})
	}
}
