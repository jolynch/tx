package policy

import (
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// Background results can be delayed or absent while the bottleneck changes.
// A background winner needs four independent results; rollback may
// restore an already established codec. Copying the
// per-file state at a window boundary must preserve every decision, and so
// must sampling only the codecs SampleNeeds names.
func FuzzBackgroundCompression(f *testing.F) {
	f.Add([]byte("00X00200000000000")) // candidate ratios must not shorten the live low-gain streak
	f.Add([]byte{1, 2, 4, 1, 1, 1, 2, 255})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0, 0, 4, 0, 2, 2, 2, 255, 1, 2, 4}) // seeded from a different link
	f.Add([]byte("00A0000000000000"))                // a settled lz4 home must still sample zstd's ratio
	f.Add([]byte("10201100"))                        // adoption need not change the check frame's codec
	f.Add([]byte("10X00101"))                        // a regime change during validation must not leave a due check unreported by SampleNeeds
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in) < 8 {
			return
		}
		p := NewCompressionPolicy()
		if len(in)%8 == 3 {
			// Start from the link estimate a file on another regime left on
			// the connection, which may be stale: seeding must not let a
			// switch skip this file's own evidence.
			prior := NewCompressionPolicy()
			r := regimeFromBytes(in[len(in)-8:])
			ps := newSim(6, .3)
			for range 8 {
				prior.DecideSampled(CompressionModeNone, ps.frame(r, CompressionModeNone), nil)
			}
			if link, ok := prior.Link(); ok {
				p.SeedLink(link)
			}
		}
		masked := *p
		// Odd-length inputs deliver each masked sample a frame after it was
		// requested, as a busy worker can.
		late := len(in)%2 == 1
		needs := masked.SampleNeeds()
		mode := CompressionModeNone
		var budget RecoveryBudget
		s := newSim(5, .3)
		evidence := 0
		established := [3]bool{true, false, false}
		var previous simRegime
		for seg := 0; seg+8 <= len(in) && seg < 32; seg += 8 {
			r := regimeFromBytes(in[seg:])
			if seg > 0 {
				r = clampedPath(previous, r)
			}
			previous = r
			sample := samplesForRegime(r)
			for frame := range 96 {
				m := s.frame(r, mode)
				m.RecoveryAllowed, m.RecoveryReady = frame < 91, budget.Ready()
				m.ReadLatency = min(m.PrepareLatency, time.Duration(r.prepare[0]*float64(time.Second)))
				var ready *encoding.CodecSamples
				if in[seg+7] != 0 && frame%8 <= int(in[seg+7]%8) {
					ready = &sample
					evidence++
				}
				clone := *p
				requested := masked.SampleNeeds()
				if !late {
					needs = requested
				}
				var partial *encoding.CodecSamples
				if ready != nil {
					partial = maskSample(*ready, needs)
				}
				d := p.DecideSampled(mode, m, ready)
				if other := clone.DecideSampled(mode, m, ready); d != other || *p != clone {
					t.Fatal("copying window state changed a decision")
				}
				if other := masked.DecideSampled(mode, m, partial); d != other || *p != masked {
					// The one allowed difference: the masked sample lacked a
					// codec the full one had, or was not taken, because it
					// was requested under an earlier state. On time, that
					// happens only for a comparison opened on its frame.
					lacked := ready != nil && (partial == nil || *partial != *ready)
					if !lacked || !late && !masked.BackgroundPending() {
						t.Fatalf("sampling only %b changed a decision: %+v, want %+v", needs, other, d)
					}
					masked = *p
				}
				needs = requested
				if simRung(d.Next) < 0 {
					t.Fatalf("invalid mode %v", d.Next)
				}
				if d.Next > mode {
					if d.Reason == "recovery-check" {
						if p.shadow.phase != phaseCheck || d.Next >= p.home {
							t.Fatal("recovery explored a stronger codec")
						}
					} else if d.Reason == "sample-overestimated" || d.Reason == "recovery-restore" {
						if !established[d.Next] {
							t.Fatal("rollback selected an unmeasured codec")
						}
					} else if evidence < BackgroundWinFrames {
						t.Fatalf("upgrade %v -> %v used only %d results", mode, d.Next, evidence)
					}
				}
				if d.Next != mode {
					if d.Reason == "sample-won" {
						if evidence < BackgroundWinFrames {
							t.Fatalf("winning %v -> %v at seg %d frame %d used %d results; low=%d validate=%d", mode, d.Next, seg, frame, evidence, p.shadow.low, p.shadow.validate)
						}
						established[d.Next] = true
					}
					evidence = 0
				}
				if d.Reason == "recovery-won" {
					established[d.Next] = true
				}
				mode = d.Next
				budget.Observe(d.RecoveryLoss, m.PrepareLatency+m.WriteLatency)
			}
		}
	})
}

// backgroundSim drives the production policy and connection budget with
// samples available at SEND's cadence. The caller controls codec evidence,
// while every frame, including a recovery check, pays its real modeled cost.
type backgroundSim struct {
	p      CompressionPolicy
	mode   CompressionMode
	budget *RecoveryBudget // nil uses own; windows of one server share one
	own    RecoveryBudget
	frame  int
	last   CompressionMetrics // the frame step last sent
}

func (b *backgroundSim) step(s *sim, r simRegime, remaining int) CompressionDecision {
	budget := b.budget
	if budget == nil {
		budget = &b.own
	}
	m := s.sendMetrics(r, s.frame(r, b.mode))
	m.RecoveryAllowed, m.RecoveryReady = remaining >= RecoveryFrames, budget.Ready()
	d := b.p.DecideSampled(b.mode, m, sampleFor(&b.p, r, b.frame))
	// SEND repays the budget with the frame's raw time, as here.
	budget.Observe(d.RecoveryLoss, observedLatency(m))
	b.mode, b.last = d.Next, m
	b.frame++
	return d
}

// The CI failures exercise a different input format from the background
// invariant fuzzer. Keep the reaction history and noise seeds identical.
// Under heavy-tailed noise only the disaster bound simHeavyLimit applies.
// Each regime's path is clamped to the one before it (clampedPath).
func FuzzBackgroundReaction(f *testing.F) {
	f.Add([]byte("\xe3\x800020000\x0000A0210")) // keep the baseline through a check at the transition
	f.Add([]byte("7\x000700000\xd80020010"))    // a weak lz4 estimate must not delay zstd
	f.Add([]byte{3, 0, 0, 3, 2, 1, 1, 1, 2, 30, 0, 0, 2, 1, 1, 1, 2})
	f.Fuzz(func(t *testing.T, in []byte) {
		const width = simRegimeBytes + 1
		if len(in) < 2+width+simRegimeBytes {
			return
		}
		history := in[2 : len(in)-simRegimeBytes]
		var segs []simSegment
		for i := 0; i+width <= len(history) && len(segs) < 3; i += width {
			r := regimeFromBytes(history[i:])
			if len(segs) > 0 {
				r = clampedPath(segs[len(segs)-1].r, r)
			}
			segs = append(segs, simSegment{r, 32 + 4*int(history[i+simRegimeBytes])})
		}
		if len(segs) == 0 {
			return
		}
		final := clampedPath(segs[len(segs)-1].r, regimeFromBytes(in[len(in)-simRegimeBytes:]))
		applicable, stuck := 0, 0
		react := simReact
		if heavy(in[0]) {
			react = simHeavyLimit
		}
		for run := range 40 {
			s := newSim(uint64(in[1])<<8|uint64(run), simJitters[in[0]%4]).noisy(in[0])
			b := backgroundSim{}
			for _, sg := range segs {
				for range sg.n {
					b.step(s, sg.r, 1024)
				}
			}
			if !final.slow(b.mode) || moved(segs[len(segs)-1].r, final, b.mode) < simVisible {
				continue
			}
			applicable++
			from := b.mode
			left := false
			for range react {
				used := b.mode
				b.step(s, final, 1024)
				// Match the trial-policy reaction contract: a cheaper mode
				// must actually be sent. Throughput is checked separately.
				if used != from {
					left = true
					break
				}
			}
			if !left {
				stuck++
			}
		}
		if float64(stuck) > max(1, simStuck*float64(applicable)) {
			t.Fatalf("background reaction: %d of %d remained slow after %d frames; final %v", stuck, applicable, react, final)
		}
	})
}

func FuzzBackgroundThroughput(f *testing.F) {
	f.Add([]byte("\xe6000A11220"))      // noise must not repeatedly advance expensive checks
	f.Add([]byte("\xb0000A02220"))      // seed the baseline before allowing another expedited check
	f.Add([]byte("\xb0000A012200"))     // noisy components must not trigger repeated expensive checks
	f.Add([]byte("\xe6000A00100"))      // a noisy regime signal must not replace the clipped baseline
	f.Add([]byte("\xffA00A12220"))      // an unconfirmed change must keep the check backoff
	f.Add([]byte("\xffA00A22220"))      // noise must not skip a costly loss gap
	f.Add([]byte("7012X0010000200100")) // rollback to none must reopen background exploration
	f.Add([]byte{3, 0, 1, 2, 4, 1, 0, 1, 2, 48, 0, 0, 4, 1, 0, 1, 2, 48})
	f.Add([]byte{0, 0, 2, 0, 3, 1, 0, 0, 1, 48, 0, 0, 3, 1, 0, 0, 1, 48})
	f.Add([]byte("\xffB02A01220")) // a baseline restored after a failed validation is not settled: no restore/check cycle
	f.Add([]byte("\xe7#02A02220")) // one stall must not fail the validation of a much cheaper codec for long (validateStall)
	f.Add([]byte("\xe6@01A12210")) // nor a stall on its first validation frame
	// Hold data and CPU constant while the bottleneck moves at one speed.
	for _, noise := range []byte{0, 3, 0xe3} {
		for _, pair := range [][2]byte{{0, 1}, {1, 0}, {2, 0}, {2, 1}} {
			seed := []byte{noise, 0, pair[0], 1, 4, 1, 0, 1, 2, 48, pair[1], 1, 4, 1, 0, 1, 2, 48}
			f.Add(seed)
		}
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in) < 10 {
			return
		}
		var total tally
		for run := range simRuns {
			s := newSim(uint64(in[1])<<8|uint64(run), simJitters[in[0]%4]).noisy(in[0])
			b := backgroundSim{}
			var previous simRegime
			for i := 2; i+8 <= len(in) && i < 34; i += 8 {
				r := regimeFromBytes(in[i:])
				if i > 2 {
					r = clampedPath(previous, r)
				}
				blind := i > 2 && moved(previous, r, b.mode) < simVisible
				from := b.mode
				for n := range 256 {
					if b.mode != from {
						blind = false
					}
					if n >= simSettle && !blind {
						total.add(r, b.mode)
					}
					b.step(s, r, 1024)
				}
				previous = r
			}
		}
		want := simEfficiency
		if heavy(in[0]) {
			want = simHeavyEfficiency
		}
		if got := total.efficiency(); got < want {
			t.Fatalf("background throughput efficiency %.3f, want %.2f", got, want)
		}
	})
}

// A socket send buffer lets a frame's write return before its bytes leave,
// so the first frames after a switch to a cheaper codec look faster than
// they are. Recovery checks and their validation must judge a codec by the
// wall time its frames really took: behind any buffer, allowing checks must
// cost at most one unpaid episode per regime plus the budget's share more
// than forbidding them. An episode sends at most 2*maxCheckFrames check
// frames and validateFrames validation frames. (Adopted codecs can still alternate in near-ties,
// where checks measure the overlap that background samples miss.) Time is raw frame time, which sums to wall time. (The policy
// prices a frame as prepare plus write, which a buffer overlaps; that model
// gap is in IDEAS.md and is held equal here by comparing the policy with
// itself.)
//
// Buffers hold at most two frames. A larger one can absorb a check's frames
// and enough validation frames to pass a codec about a quarter slower; see
// IDEAS.md. The regime is stationary: after a change, a run with checks
// can start from a codec that was right before it, and climbing back up
// behind a buffer is the background policy's weakness, not recovery's.
//
// Input: noise (see sim.noisy), seed, buffer in frames (0 to 2), then a
// regime.
func FuzzBackgroundSendBuffer(f *testing.F) {
	f.Add([]byte{0, 0, 4, 0, 0, 3, 1, 0, 1, 2})    // slow link, zstd best, 1-frame buffer
	f.Add([]byte{0, 0, 8, 0, 0, 3, 1, 0, 1, 2})    // a buffer of two frames
	f.Add([]byte{3, 1, 2, 0, 1, 2, 1, 0, 1, 1})    // jitter, lz4-size gains
	f.Add([]byte{0xe3, 0, 4, 0, 0, 4, 1, 0, 1, 2}) // heavy noise
	f.Add([]byte("0001020000"))                    // disk-bound: every codec ties, checks recur
	f.Add([]byte("0000021100"))                    // lz4 adopted while zstd is best
	f.Add([]byte("\xe3\x00100A0000"))              // absorbed check frames must not pass validation
	f.Add([]byte("00007212200"))                   // near-tie: checks find what samples miss
	f.Add([]byte("0020291120"))                    // a restored baseline must not check at once behind send buffers
	f.Add([]byte("00201X0101"))                    // the same restore/check cycle in another regime
	f.Add([]byte("00202X0021"))                    // the same, with another regime order
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in) < 3+simRegimeBytes {
			return
		}
		sndbuf := float64(in[2]%3) * simFrame
		regimes := []simRegime{regimeFromBytes(in[3:])}
		var taken [2]float64
		var episodes float64 // the time of one unpaid episode per regime, per run
		for _, r := range regimes {
			episodes += simRuns * (2*maxCheckFrames + validateFrames) * max(r.time(0), r.time(1), r.time(2))
		}
		for checks := range 2 {
			for run := range simRuns {
				s := newSim(uint64(in[1])<<8|uint64(run), simJitters[in[0]%4]).noisy(in[0])
				s.sndbuf = sndbuf
				b := backgroundSim{}
				for _, r := range regimes {
					for n := range 256 {
						left := 1024
						if checks == 0 {
							left = 0 // Too little left in the file to check.
						}
						b.step(s, r, left)
						if n < simSettle {
							continue
						}
						taken[checks] += observedLatency(b.last).Seconds()
					}
				}
			}
		}
		share := 0.015
		if heavy(in[0]) {
			// Heavy-tailed noise fires change detections that check early by
			// design, and stalls hide absorbed frames; as in the other
			// properties, only a disaster bound applies (simHeavyEfficiency).
			share = 1/simHeavyEfficiency - 1
		}
		if extra := taken[1] - taken[0]; extra > episodes+share*taken[0] {
			t.Fatalf("checks behind a %d-frame buffer cost %.2fs over %.2fs, want at most %.2fs",
				in[2]%3, extra, taken[0], episodes+share*taken[0])
		}
	})
}

// recoveryOverhead returns the extra wall time recovery checks cost on a
// slow link where zstd is clearly best: files of fileFrames frames, each
// sent as concurrent windows of window frames over conns connections that
// share one budget, against the same transfer with checks disallowed.
func recoveryOverhead(fileFrames, files, window, conns int, seed uint64) float64 {
	r := simRegime{b: simNetwork, rate: 32 * mib, ratio: [3]float64{1, 2, 3},
		prepare: [3]float64{simFrame / (4 * gib), simFrame/(4*gib) + simFrame/(1536*mib), simFrame/(4*gib) + simFrame/(512*mib)}}
	run := func(allowed bool) float64 {
		s := newSim(seed, .1)
		var budget RecoveryBudget
		var total float64
		for range files {
			var saved CompressionPolicy
			savedMode := CompressionModeNone
			sims := make([]backgroundSim, conns)
			for f := 0; f < fileFrames; f += window * conns {
				// A round's windows all start from the last saved state and
				// send their frames in turn, so connections can find the
				// budget ready together; the last window's save wins, as
				// concurrent SENDs' do. Frames run one at a time, so this
				// does not model connections reading the budget at the same
				// instant, which the start reservation does not exclude.
				for c := range sims {
					sims[c] = backgroundSim{p: saved, mode: savedMode, budget: &budget}
				}
				last := 0
				for i := range window {
					for c := range sims {
						pos := f + c*window + i
						if pos >= fileFrames {
							continue
						}
						left := fileFrames - pos
						if !allowed {
							left = 0
						}
						sims[c].step(s, r, left)
						total += observedLatency(sims[c].last).Seconds()
						last = c
					}
				}
				saved, savedMode = sims[last].p, sims[last].mode
			}
		}
		return total
	}
	return run(true)/run(false) - 1
}

// Every connection of a server shares one recovery budget, so neither many
// files, nor many connections, nor connections without keep-alive buy
// unpaid checks: recovery stays near its 1% budget where compression wins.
func TestRecoveryBudgetSharedAcrossConnections(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		fileFrames, files, window, conns int
	}{
		{"many files", 32, 64, 8, 1},
		{"one file, 16 connections", 1024, 1, 16, 16},
		{"many files, 16 connections", 64, 32, 4, 16},
	} {
		for seed := range uint64(4) {
			if got := recoveryOverhead(tc.fileFrames, tc.files, tc.window, tc.conns, seed); got > .015 {
				t.Errorf("%s, seed %d: recovery costs %.1f%%, want at most 1.5%%", tc.name, seed, 100*got)
			}
		}
	}
}

// A path whose rate changes every frame or few, as bursty links do, gives a
// background comparison a home cost that alternates with it. Judged against
// home on the frames it was predicted from, a codec that wins on average is
// adopted, whichever frames the comparison happened to see.
func TestBackgroundAlternatingPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  []byte // the slow regime; the fast one is the next rate up
		k    int    // frames per half period
	}{
		{"32/128 MiB/s every frame", []byte{0, 0, 2, 2, 1, 0, 0}, 1},
		{"128/512 MiB/s every frame", []byte{0, 1, 2, 2, 1, 1, 2}, 1},
		{"32/128 MiB/s every 3 frames", []byte{0, 0, 2, 2, 1, 0, 0}, 3},
	} {
		slow := regimeFromBytes(tc.cfg)
		fast := regimeFromBytes(append([]byte{tc.cfg[0], tc.cfg[1] + 1}, tc.cfg[2:]...))
		var ty tally
		for run := range uint64(8) {
			s := newSim(run, 0.1)
			p := NewCompressionPolicy()
			mode := CompressionModeNone
			for i := range 1100 {
				r := slow
				if i/tc.k%2 == 1 {
					r = fast
				}
				if i >= simSettle {
					ty.add(r, mode)
				}
				mode = s.step(t, p, r, mode)
			}
		}
		if ty.efficiency() < 0.95 {
			t.Errorf("%s: efficiency %.3f, want at least 0.95 (slow %v, fast %v)", tc.name, ty.efficiency(), slow, fast)
		}
	}
}
