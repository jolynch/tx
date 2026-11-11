package policy

// The simulation model the contract tests drive the policy with: regimes of
// a link and its data, the jittered frames they produce, the socket's
// bottleneck reading, and the tallies the properties judge.

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
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
	b simBottleneck
	// window marks a network clamp that a receive window below the path's
	// bandwidth-delay product sets: writes follow wire bytes, but TCP
	// reports the receiver's window as the limit.
	window  bool
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

// overCompressed reports whether m compresses while none would be simSlow
// times faster.
func (r simRegime) overCompressed(m CompressionMode) bool {
	return simRung(m) > 0 && r.time(simRung(m)) >= simSlow*r.time(0)
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
	name := r.b.String()
	if r.window {
		name = "window"
	}
	fmt.Fprintf(&sb, "%s@%.0fMiB/s", name, r.rate/mib)
	for rung, m := range simModes {
		p, w, _ := r.nominal(rung)
		fmt.Fprintf(&sb, " %s{ratio %.3g prep %.2fms write %.2fms = %.0fMiB/s}",
			modeName(m), r.ratio[rung], p*1e3, w*1e3, r.throughput(rung)/mib)
	}
	return sb.String()
}

// debtFrames is how many frames of r sent in m repay b's debt. A periodic
// check waits for the debt, so a change only such a check can see is left
// that many frames later.
func debtFrames(b *RecoveryBudget, r simRegime, m CompressionMode) int {
	debt := math.Float64frombits(b.debt.Load())
	return int(math.Ceil(debt / r.time(simRung(m))))
}

// moved returns the factor by which a mode's throughput differs between two
// regimes.
func moved(a, b simRegime, m CompressionMode) float64 {
	ta, tb := a.modeThroughput(m), b.modeThroughput(m)
	return max(ta/tb, tb/ta)
}

var (
	simRates      = [4]float64{32 * mib, 128 * mib, 512 * mib, 2 * gib}
	simRatios     = [6]float64{1.0, 1.05, 2, 3, 6, 10}
	simLz4Shares  = [3]float64{0.3, 0.6, 0.9}
	simLz4Factors = [3]float64{1.5, 3, 6}
	simZstdSpeeds = [3]float64{128 * mib, 512 * mib, 2 * gib}
	simReadSpeeds = [3]float64{1 * gib, 4 * gib, 16 * gib}
	simJitters    = [4]float64{0, 0.1, 0.2, 0.3}
)

// simRegimeBytes is how many input bytes describe a regime.
const simRegimeBytes = 7

// simMaxRise is the most a path speeds up from one segment to the next. It
// stays under simVisible: a minor rise is no visible change, so it cannot
// make a change that only probes see, such as newly compressible data, part
// of the reaction bound.
const simMaxRise = 1.2

// pathSpeed is the path's uncompressed rate.
func (r simRegime) pathSpeed() float64 {
	if r.b == simCPU {
		return simFastLink
	}
	return r.rate
}

// clampedPath keeps r's data but limits how its path may differ from prev's.
// Real paths slow abruptly and often, as a page cache fills or a network
// clamps, and speed up rarely and by little. So a path may slow by any
// factor and move its bottleneck either way, as a receiver disk gives way to
// a network whose credits ran out, but speeds up by at most simMaxRise. A
// fast link is never a minor rise, so only a CPU-bound path stays CPU-bound.
func clampedPath(prev, r simRegime) simRegime {
	limit := simMaxRise * prev.pathSpeed()
	if r.pathSpeed() <= limit {
		return r
	}
	if r.b == simCPU {
		r.b, r.rate, r.window = prev.b, prev.rate, prev.window
	}
	if r.b != simCPU {
		r.rate = min(r.rate, limit)
	}
	return r
}

func TestClampedPath(t *testing.T) {
	disk, network := simRegime{b: simDisk, rate: 512 * mib}, simRegime{b: simNetwork, rate: 128 * mib}
	cpu := simRegime{b: simCPU, rate: 2 * gib}
	for _, tc := range []struct {
		name          string
		prev, r, want simRegime
	}{
		{"a receiver disk gives way to a slower network", disk, network, network},
		{"a network speeds up by at most simMaxRise", network, disk, simRegime{b: simDisk, rate: simMaxRise * 128 * mib}},
		{"a network does not become a fast link", network, cpu, network},
		{"a fast link slows to a network", cpu, network, network},
	} {
		if got := clampedPath(tc.prev, tc.r); got.b != tc.want.b || got.rate != tc.want.rate {
			t.Errorf("%s: got %v at %.0f MiB/s, want %v at %.0f MiB/s", tc.name, got.b, got.rate/mib, tc.want.b, tc.want.rate/mib)
		}
	}
}

// regimeFromBytes decodes bottleneck, rate, data ratio (zstd's), lz4's share
// of zstd's gain, how many times faster than zstd lz4 encodes, zstd speed, and
// read speed. Every codec reads the frame first, so none's prepare time is the
// read alone. lz4 always encodes faster and compresses less than zstd, as the
// real codecs do.
func regimeFromBytes(in []byte) simRegime {
	ratio := simRatios[in[2]%6]
	zstd := simZstdSpeeds[in[5]%3]
	read := simFrame / simReadSpeeds[in[6]%3]
	b := simBottleneck(in[0] % 3)
	return simRegime{
		b: b,
		// A rate byte of 0xe0 or more, never in ASCII seeds, selects a
		// window clamp: one network regime in eight of random bytes.
		window: b == simNetwork && in[1] >= 0xe0,
		rate:   simRates[in[1]%4],
		ratio:  [3]float64{1, 1 + (ratio-1)*simLz4Shares[in[3]%3], ratio},
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
	// The socket's bottleneck reading, from its own stream so it leaves the
	// jitter as it was. The socket is read once every classEvery frames, as
	// SEND reads TCP_INFO on a byte schedule, a reading names a wrong class
	// with probability classErr, and as in SEND a class takes effect when
	// two readings in a row agree. A run starts with its path's class, as a
	// new connection starts from the process's, and the class lags a change
	// by up to two readings.
	// classOff turns the reading off (costsOnly).
	classer          *rand.Rand
	classOff         bool
	classEvery       int
	classErr         float64
	class, lastClass Bottleneck
	classFrames      int
}

func newSim(seed uint64, jitter float64) *sim {
	classer := rand.New(rand.NewPCG(seed, 0x636c))
	return &sim{
		rng:        rand.New(rand.NewPCG(seed, 0x7478)),
		sniffer:    rand.New(rand.NewPCG(seed, 0x736e)),
		jitter:     jitter,
		classer:    classer,
		classEvery: [4]int{1, 2, 4, 8}[classer.IntN(4)],
		classErr:   [3]float64{0, 0.01, 0.02}[classer.IntN(3)],
	}
}

// costsOnly turns off the bottleneck reading, as on a socket without
// TCP_INFO, for tests of how the policy infers the bottleneck from costs.
func (s *sim) costsOnly() *sim {
	s.classOff = true
	return s
}

// bottleneck returns the class TCP_INFO would report while rung sends: the
// receiver on a disk-bound path, whose disk paces every write, or behind a
// small receive window, whose window does; else the
// sender when the socket sits idle over half the frame, and the network
// otherwise.
func (r simRegime) bottleneck(rung int) Bottleneck {
	p, w, _ := r.nominal(rung)
	switch {
	case r.b == simDisk || r.window:
		return BottleneckReceiver
	case w < p:
		return BottleneckSender
	default:
		return BottleneckNetwork
	}
}

// readClass returns the bottleneck class the frame reports while m sends.
func (s *sim) readClass(r simRegime, m CompressionMode) Bottleneck {
	if s.classOff {
		return BottleneckUnknown
	}
	if s.classFrames == 0 {
		// A new connection starts from the class the process last reached,
		// taken here to be this path's.
		s.class = r.bottleneck(simRung(m))
		s.lastClass = s.class
	}
	if s.classFrames%s.classEvery == 0 {
		read := r.bottleneck(simRung(m))
		if s.classer.Float64() < s.classErr {
			// One of the two other classes.
			read = Bottleneck(1 + (int(read)-1+1+s.classer.IntN(2))%3)
		}
		if read == s.lastClass {
			s.class = read
		}
		s.lastClass = read
	}
	s.classFrames++
	return s.class
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
	if heavy(b) {
		// Noisy hosts misread the socket more often, too.
		s.classErr = [3]float64{0.02, 0.05, 0.05}[s.classer.IntN(3)]
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
		Bottleneck:     s.readClass(r, m),
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
//
// It also sums the time none would take, the time taken with each frame
// held to simSlow times none's (slowCapped), and counts the frames run
// over-compressed (overCompressed).
type tally struct {
	best, taken, none, capped float64
	counted, within, over     int
}

func (ty *tally) add(r simRegime, m CompressionMode) {
	b, tk := r.bestTime(), r.time(simRung(m))
	ty.best += b
	ty.taken += tk
	ty.none += r.time(0)
	ty.capped += min(tk, simSlow*r.time(0))
	ty.counted++
	if tk < simSlow*b {
		ty.within++
	} else if r.overCompressed(m) {
		ty.over++
	}
}

func (ty *tally) merge(o tally) {
	ty.best += o.best
	ty.taken += o.taken
	ty.none += o.none
	ty.capped += o.capped
	ty.counted += o.counted
	ty.within += o.within
	ty.over += o.over
}

// overNone is the time taken beyond sending uncompressed throughout, as a
// share of the latter: negative when compression paid.
func (ty tally) overNone() float64 {
	if ty.none == 0 {
		return 0
	}
	return ty.taken/ty.none - 1
}

// cappedOverNone is overNone with each frame held to simSlow times none's
// time. A frame slower than that is over-compressed, which overShare bounds
// by count: when a codec costs many times none, as encoding does on a fast
// path, one recovery-budget wait after a stall would otherwise exceed any
// time bound on its own.
func (ty tally) cappedOverNone() float64 {
	if ty.none == 0 {
		return 0
	}
	return ty.capped/ty.none - 1
}

// overShare is the share of counted frames run over-compressed.
func (ty tally) overShare() float64 {
	return float64(ty.over) / float64(max(1, ty.counted))
}

// checkOverCompression fails t when ty took simOverNone more time than
// sending uncompressed, or ran over simOverShare of its frames
// over-compressed. Under heavy noise, where a stall can fail any check, the
// bounds are simHeavyOverNone of capped time (cappedOverNone) and
// simHeavyOverShare.
func checkOverCompression(t *testing.T, name string, ty tally, heavyNoise bool, detail string) {
	t.Helper()
	want, share, over, capped := simOverNone, simOverShare, ty.overNone(), ""
	if heavyNoise {
		want, share, over, capped = simHeavyOverNone, simHeavyOverShare, ty.cappedOverNone(), fmt.Sprintf(" with frames capped at %.1fx", simSlow)
	}
	if over > want || ty.overShare() > share {
		t.Fatalf("%s: %.1f%% more time than sending uncompressed%s (want at most %.0f%%), %.1f%% of %d frames compressed though none is %.1fx faster (want at most %.0f%%):%s",
			name, 100*over, capped, 100*want, 100*ty.overShare(), ty.counted, simSlow, 100*share, detail)
	}
}

func (ty tally) efficiency() float64 {
	if ty.taken == 0 {
		return 1
	}
	return ty.best / ty.taken
}
