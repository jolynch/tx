package policy

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
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
	// simEfficiency is the disaster bound on time efficiency, summed over
	// simRuns runs of one fuzz input: the time the best mode would take over
	// the time the policy's modes take. The policy is stochastic in effect, so
	// a fuzzer that searches inputs and seeds always finds a worst batch; the
	// fuzz properties bound disasters, and TestCompressionPolicyQualitySweep
	// bounds the average.
	simEfficiency = 0.85
	// simHeavyEfficiency is the bound under high noise, where only basic
	// properties are checked: a policy stuck in a mode twice as slow as the
	// best for a whole input would score 0.5. Per-input quality under
	// heavy-tailed noise is a long tail any searching fuzzer reaches;
	// TestCompressionPolicyQualitySweep bounds the average instead.
	simHeavyEfficiency = 0.50
	// simWithin is the share of counted frames that must take less than
	// simSlow times the best mode's time, which catches a policy that sticks
	// in a bad mode while an average hides it.
	simWithin = 0.90
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
	// simChanges and simHeavyChanges bound codec changes per 1,000 settled
	// frames of one stationary fuzz input, under uniform and heavy-tailed
	// noise.
	simChanges      = 30
	simHeavyChanges = 60
	// simSniffEvery is how many none frames pass between sniffs.
	simSniffEvery = 8
	// simSlow is how much slower than the best a mode must be before the
	// policy has to leave it within simReact frames.
	simSlow = 1.5
	// simReact is the number of decisions within which the policy must leave
	// a mode that a regime change made simSlow times slower than the best.
	simReact = 24
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

type simBottleneck uint8

const (
	simNetwork simBottleneck = iota // write time follows wire bytes
	simDisk                         // write time follows logical bytes: the receiver's disk pushes back
	simCPU                          // the link is fast, so writes are short
)

func (b simBottleneck) String() string {
	switch b {
	case simNetwork:
		return "network"
	case simDisk:
		return "disk"
	default:
		return "cpu"
	}
}

// simModes are the modes adapt uses, bottom rung first.
var simModes = [3]CompressionMode{CompressionModeNone, CompressionModeLz4, CompressionModeZstdLevel1}

// simRung returns the ladder position of a mode adapt uses, or -1.
func simRung(m CompressionMode) int {
	for i, sm := range simModes {
		if sm == m {
			return i
		}
	}
	return -1
}

func modeName(m CompressionMode) string {
	switch m {
	case CompressionModeNone:
		return "none"
	case CompressionModeLz4:
		return "lz4"
	case CompressionModeZstdLevel1:
		return "zstd1"
	case CompressionModeZstdDefault:
		return "zstd-default"
	default:
		return fmt.Sprintf("mode(%d)", m)
	}
}

// simRegime is one set of conditions: the bottleneck and its rate, and what
// each rung costs and gains on the data.
type simRegime struct {
	b       simBottleneck
	rate    float64    // bytes per second: wire bytes on a network, logical bytes on a disk
	ratio   [3]float64 // compression ratio per rung
	prepare [3]float64 // seconds to read and encode one frame, per rung
}

// nominal returns a rung's mean prepare and write seconds and wire bytes.
func (r simRegime) nominal(rung int) (prepare, write, wire float64) {
	wire = simFrame / r.ratio[rung]
	switch r.b {
	case simNetwork:
		write = wire / r.rate
	case simDisk:
		write = max(wire/(r.rate*simDiskLinkFactor), simFrame/r.rate)
	default:
		write = wire / simFastLink
	}
	return r.prepare[rung], write, wire
}

// throughput is a rung's true logical bytes per second: the objective the
// policy maximizes, logical size over prepare plus write time.
func (r simRegime) throughput(rung int) float64 {
	p, w, _ := r.nominal(rung)
	return simFrame / (p + w)
}

func (r simRegime) modeThroughput(m CompressionMode) float64 { return r.throughput(simRung(m)) }

// best returns the highest and second-highest rung throughputs.
func (r simRegime) best() (best, second float64) {
	for rung := range simModes {
		t := r.throughput(rung)
		switch {
		case t > best:
			best, second = t, best
		case t > second:
			second = t
		}
	}
	return best, second
}

// time returns a rung's mean seconds per frame.
func (r simRegime) time(rung int) float64 {
	p, w, _ := r.nominal(rung)
	return p + w
}

// bestTime returns the shortest rung time.
func (r simRegime) bestTime() float64 {
	return min(r.time(0), r.time(1), r.time(2))
}

// good reports whether m is within simGood of the best mode.
func (r simRegime) good(m CompressionMode) bool {
	b, _ := r.best()
	return r.modeThroughput(m)*simGood >= b
}

// slow reports whether m is at least simSlow times slower than the best mode.
func (r simRegime) slow(m CompressionMode) bool {
	b, _ := r.best()
	return r.modeThroughput(m)*simSlow <= b
}

// separated reports whether the best mode beats every other by more than
// simSlow.
func (r simRegime) separated() bool {
	b, s := r.best()
	return b > s*simSlow
}

func (r simRegime) incompressible() bool { return r.ratio[1] <= 1.05 && r.ratio[2] <= 1.05 }

func (r simRegime) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s@%.0fMiB/s", r.b, r.rate/mib)
	for rung, m := range simModes {
		p, w, _ := r.nominal(rung)
		fmt.Fprintf(&sb, " %s{ratio %.3g prep %.2fms write %.2fms = %.0fMiB/s}",
			modeName(m), r.ratio[rung], p*1e3, w*1e3, r.throughput(rung)/mib)
	}
	return sb.String()
}

// moved returns the factor by which a mode's throughput differs between two
// regimes.
func moved(a, b simRegime, m CompressionMode) float64 {
	ta, tb := a.modeThroughput(m), b.modeThroughput(m)
	return max(ta/tb, tb/ta)
}

var (
	simRates      = [4]float64{32 * mib, 128 * mib, 512 * mib, 2 * gib}
	simRatios     = [6]float64{1.0, 1.05, 2, 3, 30, 250}
	simLz4Shares  = [3]float64{0.3, 0.6, 0.9}
	simLz4Factors = [3]float64{1.5, 3, 6}
	simZstdSpeeds = [3]float64{128 * mib, 512 * mib, 2 * gib}
	simReadSpeeds = [3]float64{1 * gib, 4 * gib, 16 * gib}
	simJitters    = [4]float64{0, 0.1, 0.2, 0.3}
)

// simRegimeBytes is how many input bytes describe a regime.
const simRegimeBytes = 7

// regimeFromBytes decodes bottleneck, rate, data ratio (zstd's), lz4's share
// of zstd's gain, how many times faster than zstd lz4 encodes, zstd speed, and
// read speed. Every codec reads the frame first, so none's prepare time is the
// read alone. lz4 always encodes faster and compresses less than zstd, as the
// real codecs do.
func regimeFromBytes(in []byte) simRegime {
	ratio := simRatios[in[2]%6]
	zstd := simZstdSpeeds[in[5]%3]
	read := simFrame / simReadSpeeds[in[6]%3]
	return simRegime{
		b:     simBottleneck(in[0] % 3),
		rate:  simRates[in[1]%4],
		ratio: [3]float64{1, 1 + (ratio-1)*simLz4Shares[in[3]%3], ratio},
		prepare: [3]float64{
			read,
			read + simFrame/(zstd*simLz4Factors[in[4]%3]),
			read + simFrame/zstd,
		},
	}
}

// sim draws jittered frames. Each frame's prepare time, write time, and wire
// size vary by an independent multiplicative factor, so their means are the
// regime's nominal values. Heavy-tailed noise adds lognormal jitter to the
// prepare and write times and occasional outlier frames, whose write takes
// 100 ms longer or five times as long. While in none, every simSniffEvery-th
// frame also reports the lz4 ratio of the data, with mild jitter.
type sim struct {
	rng     *rand.Rand
	sniffer *rand.Rand // its own stream, so sniffing leaves frame jitter as it was
	jitter  float64
	sigma   float64 // lognormal sigma on prepare and write; 0 for none
	outlier float64 // probability a frame is an outlier
	none    int     // none frames drawn so far
	// sndbuf is a socket send buffer, in bytes, or 0 for none; queued is
	// what it holds. A buffered write returns before its bytes leave, so a
	// frame's raw time (FrameLatency) can be far below its rate-floored write.
	sndbuf, queued float64
	// frames counts decide's frames, for the sample cadence; budget is
	// the connection's recovery budget.
	frames int
	budget RecoveryBudget
}

func newSim(seed uint64, jitter float64) *sim {
	return &sim{
		rng:     rand.New(rand.NewPCG(seed, 0x7478)),
		sniffer: rand.New(rand.NewPCG(seed, 0x736e)),
		jitter:  jitter,
	}
}

// noisy returns a simulator with the noise a fuzz input byte selects. Each
// input is in one of two noise classes, for all its runs. Low noise is
// uniform jitter alone (up to +/-30%, bits 0-1 of the byte), selected by
// bits 6-7 being zero, as in the ASCII digits. High noise (heavy() reports
// it) is any other bits 6-7: a subset of {lognormal jitter, stalled frames},
// never empty, with uniform jitter on top. Bit 7 enables lognormal jitter,
// bit 6 enables stalls; bits 4-5 pick the lognormal sigma (0.2 to 0.5) and
// bits 2-3 the stall rate (1% to 3%). A stall adds 100 ms to a frame's
// write or multiplies it by five.
func (s *sim) noisy(b byte) *sim {
	if kind := b >> 6; kind&2 != 0 {
		s.sigma = [4]float64{0.2, 0.35, 0.5, 0.5}[b>>4&3]
	}
	if b>>6&1 != 0 {
		s.outlier = [4]float64{0.01, 0.02, 0.03, 0.02}[b>>2&3]
	}
	return s
}

// heavy reports whether a fuzz input byte selects the high-noise class.
func heavy(b byte) bool { return b>>6 != 0 }

func (s *sim) factor(spread float64) float64 {
	return 1 + spread*(2*s.rng.Float64()-1)
}

// lognormal returns a factor with mean 1 and the given sigma.
func (s *sim) lognormal() float64 {
	if s.sigma == 0 {
		return 1
	}
	return math.Exp(s.sigma*s.rng.NormFloat64() - s.sigma*s.sigma/2)
}

func (s *sim) frame(r simRegime, m CompressionMode) CompressionMetrics {
	p, w, wire := r.nominal(simRung(m))
	wireSize := max(1, int64(wire*s.factor(s.jitter/4)))
	prepare := p * s.factor(s.jitter) * s.lognormal()
	write := w * s.factor(s.jitter) * s.lognormal()
	if s.outlier > 0 && s.rng.Float64() < s.outlier {
		if s.rng.IntN(2) == 0 {
			write += 0.1
		} else {
			write *= 5
		}
	}
	fm := CompressionMetrics{
		LogicalSize:    simFrame,
		WireSize:       wireSize,
		PrepareLatency: time.Duration(prepare * float64(time.Second)),
		WriteLatency:   time.Duration(write * float64(time.Second)),
	}
	if s.sndbuf > 0 {
		// The path drains the buffer while the next frame prepares, and a
		// write waits only for the bytes that do not fit, so raw frame times
		// sum to wall time. WriteLatency stays the rate-floored write, as
		// SEND's writeTime reports it.
		drain := float64(wireSize) / write
		s.queued = max(0, s.queued-prepare*drain)
		wait := 0.0
		if free := s.sndbuf - s.queued; float64(wireSize) <= free {
			s.queued += float64(wireSize)
		} else {
			wait = (float64(wireSize) - free) / drain
			s.queued = s.sndbuf
		}
		fm.FrameLatency = time.Duration((prepare + wait) * float64(time.Second))
	}
	if m == CompressionModeNone {
		if s.none++; s.none%simSniffEvery == 0 {
			ratio := r.ratio[1] * (1 + 0.1*(s.sniffer.Float64()-0.5))
			fm.SniffRatio = float32(max(1, ratio))
		}
	}
	return fm
}

// checkStep fails unless next is a mode adapt uses (C6). Background
// evidence may take none straight to zstd, and checks may drop any rung.
func checkStep(t testing.TB, current, next CompressionMode) {
	t.Helper()
	if simRung(next) < 0 {
		t.Fatalf("DecideSampled(%s) chose %s, which adapt does not use", modeName(current), modeName(next))
	}
}

// sampleFor returns the codec sample SEND would hand p with its frame-th
// frame: at every simSniffEvery-th frame and while a comparison is pending,
// holding only the codecs p needs. Copies of one policy get equal samples.
func sampleFor(p *CompressionPolicy, r simRegime, frame int) *encoding.CodecSamples {
	if !p.BackgroundPending() && frame%simSniffEvery != 0 {
		return nil
	}
	return maskSample(samplesForRegime(r), p.SampleNeeds())
}

// sendMetrics sets the fields SEND adds to a frame's metrics: its read
// time, and recovery's room and budget. Its sniff ratio comes from samples.
func (s *sim) sendMetrics(r simRegime, m CompressionMetrics) CompressionMetrics {
	m.ReadLatency = min(m.PrepareLatency, time.Duration(r.prepare[0]*float64(time.Second)))
	m.SniffRatio = 0
	m.RecoveryAllowed, m.RecoveryReady = true, s.budget.Ready()
	return m
}

// decide sends one frame of r in mode through the production controller, as
// SEND does, and charges the connection's budget.
func (s *sim) decide(p *CompressionPolicy, r simRegime, mode CompressionMode) CompressionDecision {
	m := s.sendMetrics(r, s.frame(r, mode))
	d := p.DecideSampled(mode, m, sampleFor(p, r, s.frames))
	s.budget.Observe(d.RecoveryLoss, observedLatency(m))
	s.frames++
	return d
}

// step runs one frame of r in mode and returns the policy's next mode.
func (s *sim) step(t testing.TB, p *CompressionPolicy, r simRegime, mode CompressionMode) CompressionMode {
	t.Helper()
	next := s.decide(p, r, mode).Next
	checkStep(t, mode, next)
	return next
}

// simSegment is one regime held for n frames.
type simSegment struct {
	r simRegime
	n int
}

// tally sums, over the frames counted toward one property, the time the best
// mode would take and the time the modes taken take, and how many frames took
// less than simSlow times the best.
type tally struct {
	best, taken     float64
	counted, within int
}

func (ty *tally) add(r simRegime, m CompressionMode) {
	b, tk := r.bestTime(), r.time(simRung(m))
	ty.best += b
	ty.taken += tk
	ty.counted++
	if tk < simSlow*b {
		ty.within++
	}
}

func (ty *tally) merge(o tally) {
	ty.best += o.best
	ty.taken += o.taken
	ty.counted += o.counted
	ty.within += o.within
}

func (ty tally) efficiency() float64 {
	if ty.taken == 0 {
		return 1
	}
	return ty.best / ty.taken
}

// FuzzCompressionPolicyRegret checks C1 as a disaster bound: one policy,
// carried across regime changes and window boundaries the way a file's state
// is, runs with a time efficiency of at least simEfficiency after each
// change's settling period, summed over simRuns jittered runs. At least
// simWithin of the counted frames must also take less than simSlow times the
// best mode's time, which catches a policy that sticks in a bad mode while
// the average hides it. Under high noise only the basic bound applies
// (simHeavyEfficiency, no share check); the average quality is
// TestCompressionPolicyQualitySweep's. Frames
// in none report a sniff ratio every simSniffEvery frames. The
// FileCompression is saved and reloaded by value at window boundaries.
//
// Input: jitter and noise (see sim.noisy), window size, two seed bytes, then
// segments of a regime and a length byte.
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
	f.Add(cat(head, seg([]byte{0, 1, 3, 1, 1, 1, 2}, 150), seg([]byte{0, 1, 0, 1, 1, 1, 2}, 200))) // compressible, then incompressible
	f.Add(cat(head, seg([]byte{0, 3, 3, 1, 1, 1, 2}, 200), seg([]byte{0, 0, 3, 1, 1, 1, 2}, 200))) // burst network credits run out
	f.Add(cat(head, seg([]byte{0, 0, 4, 1, 0, 1, 2}, 200), seg([]byte{2, 0, 4, 1, 0, 1, 2}, 200))) // compression right, then CPU bound
	f.Add(cat([]byte{3, 2, 7, 1}, seg([]byte{0, 1, 0, 2, 2, 1, 2}, 255), seg([]byte{0, 0, 3, 1, 1, 1, 2}, 100),
		seg([]byte{1, 2, 4, 1, 1, 1, 2}, 100), seg([]byte{0, 0, 0, 1, 1, 1, 2}, 100))) // several shifts, short windows
	// Heavy-tailed noise: outliers, lognormal jitter, and both.
	f.Add(cat([]byte{0x43, 1, 0, 0}, seg([]byte{0, 0, 3, 1, 1, 1, 2}, 200), seg([]byte{0, 0, 0, 1, 1, 1, 2}, 200)))
	f.Add(cat([]byte{0xB3, 1, 0, 0}, seg([]byte{0, 1, 2, 1, 1, 1, 2}, 200), seg([]byte{1, 1, 2, 1, 1, 1, 2}, 200)))
	f.Add(cat([]byte{0xF3, 2, 0, 1}, seg([]byte{0, 0, 3, 1, 1, 1, 2}, 200), seg([]byte{2, 0, 3, 1, 0, 0, 1}, 200)))
	// Inputs the fuzzer found against earlier versions of the policy.
	f.Add([]byte("21\x00(00211100"))
	f.Add([]byte("710800211100"))
	f.Add([]byte("7100002111000"))
	f.Add([]byte("710\x0011200100"))
	f.Add([]byte("11910791220\x00"))
	f.Add([]byte("010002221100"))
	f.Add([]byte("71A70192011\x020"))
	f.Add([]byte("71880021110011200100"))
	f.Add([]byte("711001211200"))
	f.Add([]byte("710200211100"))
	f.Add([]byte("000000A00210"))
	f.Add([]byte("01000190011\x02"))
	f.Add([]byte("\xeb00000A12220"))      // noise must not repeatedly reopen costly wire trials
	f.Add([]byte("00002000000000X20220")) // ratio changes must refresh the clipped baseline
	f.Add([]byte("\xeb00100A12220"))      // a noise burst must not erase expensive-trial backoff
	f.Add([]byte("701701910010"))
	f.Add([]byte("21C00120010\x020"))
	f.Add([]byte("70100090011011200110"))
	f.Add([]byte("20a20121220\x020"))
	f.Add([]byte("710C02X1012\xc820X1011X"))
	f.Add([]byte("0200000000001120000\x0102201000"))
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
	f.Add([]byte("\xf30aB0090001020000000")) // zstd home, then CPU bound and incompressible
	// Low noise, found by fuzz-long in CI.
	f.Add([]byte("010000X0000C20X00100"))  // a path 4x faster must not judge zstd against a stale baseline
	f.Add([]byte("70\aa02X001000020000a")) // ratio 1.3: a none-lz4 coin flip must not set a disk floor
	f.Fuzz(func(t *testing.T, in []byte) {
		rg, ok := runRegret(t, in, simRuns)
		if !ok {
			return
		}
		within := float64(rg.all.within) / float64(rg.all.counted)
		wantEff, wantWithin := simEfficiency, simWithin
		if heavy(in[0]) {
			wantEff, wantWithin = simHeavyEfficiency, 0
		}
		if rg.all.efficiency() < wantEff || within < wantWithin {
			var sb strings.Builder
			for i, sg := range rg.segs {
				fmt.Fprintf(&sb, "\n  %d frames, efficiency %.3f, %d of %d counted within %.1fx: %v",
					sg.n, rg.per[i].efficiency(), rg.per[i].within, rg.per[i].counted, simSlow, sg.r)
			}
			t.Fatalf("C1: jitter %.1f noise %#x window %d, %d runs: efficiency %.3f (want %.2f), %.1f%% of counted frames within %.1fx of the best (want %.0f%%):%s",
				rg.jitter, in[0]>>4, rg.window, simRuns, rg.all.efficiency(), wantEff, 100*within, simSlow, 100*wantWithin, sb.String())
		}
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
		rg.segs = append(rg.segs, simSegment{r: regimeFromBytes(in[i:]), n: 128 + int(in[i+simRegimeBytes])})
	}
	rg.per = make([]tally, len(rg.segs))
	for run := 0; run < runs; run++ {
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

// FuzzCompressionPolicyStationary checks C4 and C2 as disaster bounds in one
// regime held for 1,056 frames from a fresh policy, summed over simRuns
// jittered runs: after settling, at most simChanges adopted codec changes
// per 1,000 frames (simHeavyChanges under heavy-tailed noise), checks that
// lose at most 2% of the time, and a time efficiency of
// at least simEfficiency (simHeavyEfficiency under heavy-tailed noise); on
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
	f.Add([]byte("\xff\xff00120010")) // found against the round 3 policy
	f.Add([]byte("\xeb7000A2222"))    // high ratios make repeated false alarms expensive
	f.Add([]byte("2101020020"))       // disk-bound tie: samples and checks trade codecs
	f.Fuzz(func(t *testing.T, in []byte) {
		st, ok := runStationary(t, in, simRuns)
		if !ok {
			return
		}
		wantChanges := simChanges
		if heavy(in[0]) {
			wantChanges = simHeavyChanges
		}
		// Adopted codecs must be stable. Checks of cheaper codecs send real
		// frames by design and stay within their budget of about 1%.
		if st.changes*1000 > st.counted*wantChanges {
			t.Errorf("C4: %d codec changes in %d settled frames, want at most %d per 1,000\n  %v", st.changes, st.counted, wantChanges, st.r)
		}
		wantLoss := 0.02
		if heavy(in[0]) {
			wantLoss = 1/simHeavyEfficiency - 1
		}
		if st.loss > wantLoss*st.ty.taken {
			t.Errorf("C4: checks lost %.2fs of %.2fs settled, want at most %.0f%%\n  %v", st.loss, st.ty.taken, 100*wantLoss, st.r)
		}
		wantEff := simEfficiency
		if heavy(in[0]) {
			wantEff = simHeavyEfficiency
		}
		if st.ty.efficiency() < wantEff {
			t.Errorf("C4: efficiency %.3f over %d settled frames, want at least %.2f\n  %v", st.ty.efficiency(), st.counted, wantEff, st.r)
		}
		if st.r.incompressible() && st.compressed*20 > st.frames {
			t.Errorf("C2: %d of %d frames compressed on incompressible data, want at most 5%%\n  %v",
				st.compressed, st.frames, st.r)
		}
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

// FuzzCompressionPolicyReaction checks C3 as a disaster bound: after any
// history, a regime change that makes the current mode simSlow times slower
// than the best, and that mode can see, sends the policy out of it within
// simReact decisions in all but simStuck of 40 jittered runs. Under
// heavy-tailed noise only the disaster bound simHeavyLimit applies;
// TestCompressionPolicyQualitySweep holds the typical reaction.
//
// Input: jitter and noise (see sim.noisy), a seed byte, up to three history
// segments of a regime and a length byte (32 + 4x frames), then the new regime.
func FuzzCompressionPolicyReaction(f *testing.F) {
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	head := []byte{3, 0}
	f.Add([]byte("\xff210X0011012A0010"))                                                                            // the receiver's disk speeds up while a mispredicted zstd validates
	f.Add([]byte("\xbb200X0000212X0001"))                                                                            // heavy noise hides a disk speedup for longer than the quality bound
	f.Add([]byte("70102102100191200"))                                                                               // a check straddling a path change must not hold back upward samples
	f.Add([]byte("10009010011120000"))                                                                               // none lost a check on the old path, lz4 won on the new one
	f.Add(cat(head, []byte{0, 3, 0, 1, 1, 1, 2, 255}, []byte{0, 0, 3, 1, 1, 1, 2}))                                  // long incompressible history, then the link slows
	f.Add(cat(head, []byte{0, 0, 4, 1, 0, 0, 2, 100}, []byte{2, 0, 4, 1, 0, 0, 2}))                                  // compression right, then CPU bound
	f.Add(cat(head, []byte{0, 2, 4, 1, 0, 1, 2, 100}, []byte{1, 2, 4, 1, 0, 1, 2}))                                  // compression right, then receiver disk
	f.Add(cat(head, []byte{0, 0, 3, 1, 1, 1, 2, 60}, []byte{0, 1, 0, 1, 1, 1, 2, 255}, []byte{0, 0, 4, 1, 1, 1, 2})) // kept, then failed probes, then a slow link
	f.Add(cat(head, []byte{2, 0, 3, 1, 0, 0, 1, 200}, []byte{0, 0, 3, 1, 0, 0, 1}))                                  // CPU bound, then a slow link
	f.Add(cat(head, []byte{0, 3, 2, 1, 1, 1, 2, 30}, []byte{0, 0, 2, 1, 1, 1, 2}))                                   // burst credits run out
	f.Add([]byte("20009001101120001"))                                                                               // found against the round 3 policy: uniform, 12 of 40 stuck
	f.Add([]byte("\xe3\xff00X0012012X0010"))                                                                         // found against the round 3 policy
	f.Add([]byte("\xe3200X0001012X0010"))                                                                            // high noise: found against the swarm-style bounds
	f.Add([]byte("\xff800X0011012X0010"))
	f.Add([]byte("7A00X1002X2020010"))    // found against round 2: a trial running when home's regime shifts
	f.Add([]byte("\xe3000X0011012X0010")) // PR #50 acceptance: noisy network-to-disk shift
	f.Add([]byte("\xe3200X0000012X0001")) // a long downward backoff hid the new bottleneck
	f.Add([]byte("\xeb000X0000012X0001")) // noisy cost shifts must still reopen downward checks
	f.Add([]byte("\xeb200X0000Y12X0001")) // the reference must not follow a noisy shift too quickly
	f.Add([]byte("\xbb200X0000Y12X0001")) // codec CPU cost must not hide persistent write drift
	f.Fuzz(func(t *testing.T, in []byte) {
		react := 0
		if len(in) > 0 && heavy(in[0]) {
			react = simHeavyLimit
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
			t.Fatalf("C3: %d of %d runs stayed over %d decisions in a mode %.1fx slower than the best (%s), want at most %.0f%% or one run:%s\n  then: %v",
				rc.stuck, rc.applicable, rc.react, simSlow, rc.failure, 100*simStuck, sb.String(), rc.final)
		}
	})
}

// reactionResult is one reaction input's outcome.
type reactionResult struct {
	segs       []simSegment
	final      simRegime
	applicable int // runs whose mode the change made slow and visible
	stuck      int // applicable runs that stayed in that mode
	failure    string
	react      int // decision budget of this input's noise class
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
	rc.final = regimeFromBytes(in[len(in)-simRegimeBytes:])
	hist := in[2 : len(in)-simRegimeBytes]
	for i := 0; i+segBytes <= len(hist) && len(rc.segs) < 3; i += segBytes {
		rc.segs = append(rc.segs, simSegment{r: regimeFromBytes(hist[i:]), n: 32 + 4*int(hist[i+simRegimeBytes])})
	}
	if len(rc.segs) == 0 {
		return rc, false
	}
	last := rc.segs[len(rc.segs)-1].r
	for run := 0; run < runs; run++ {
		s := newSim(uint64(in[1])<<8|uint64(run), jitter).noisy(in[0])
		p := NewCompressionPolicy()
		mode := CompressionModeNone
		for _, sg := range rc.segs {
			for i := 0; i < sg.n; i++ {
				mode = s.step(t, p, sg.r, mode)
			}
		}
		if !rc.final.slow(mode) || moved(last, rc.final, mode) < simVisible {
			continue
		}
		rc.applicable++
		from, left := mode, false
		for i := 0; i < rc.react && !left; i++ {
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
