package policy

import (
	"math"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// CompressionMode is a frame codec. The adaptive ladder is none, lz4, and
// zstd level 1, in that order. None is the zero value, so a zero
// FileCompression is a fresh policy at none.
type CompressionMode uint8

const (
	CompressionModeNone CompressionMode = iota
	CompressionModeLz4
	CompressionModeZstdLevel1
	// CompressionModeZstdDefault is zstd at its default level. The adaptive
	// policy never selects it.
	CompressionModeZstdDefault
)

// Home estimates.
const (
	// seedFrames is how many samples home needs before a pending probe, and
	// settleFrames before a scheduled probe or the regime check.
	seedFrames   = 4
	settleFrames = 8
	// homeFrames bounds the home mean that trials are compared with, and
	// refFrames the long averages the regime check uses and the sample
	// variance: running means up to that many samples, then EMAs. Established
	// references move at most 1/homeFrames per frame.
	homeFrames = 16
	refFrames  = 64
	// homeClip caps a home sample, and its write time, at this multiple of
	// the recent average, so one stalled frame moves no estimate far while
	// a lasting shift still compounds through the cap within a few frames.
	homeClip = 2
	// While home seeds, a frame under 1/seedStall of the average restarts
	// the seed from that frame: the frames before it were stalls. A stall
	// takes several times as long as a frame, while under heavy noise about
	// one ordinary frame in eight falls under half the average; restarting
	// on those would seed home from its low tail and bias its cost low.
	seedStall = 4
	// minVar is the least relative variance assumed for any sample, so a
	// noiseless run still needs a few frames to decide.
	minVar = 0.03 * 0.03
)

// Regime detection.
const (
	// Home's cost has moved when a CUSUM of its frame costs over (or under)
	// its long average, each frame less an allowance of regimeK standard
	// deviations of the samples (at least regimeMinK), passes regimeH
	// standard deviations (at least regimeShift). The sums add up a lasting
	// shift that the noise hides frame by frame and drain back to zero
	// while the cost stays near its average. No frame adds more than
	// 1/regimeCap of the threshold, so one stalled frame cannot trip it.
	regimeK     = 0.5
	regimeMinK  = 0.12
	regimeH     = 6
	regimeShift = 0.2
	regimeCap   = 3
	// Either part of home's cost, its write or its prepare time, moving by
	// a factor of regimePart and by more than regimeFloor of home's cost,
	// for regimeFrames frames in a row, also marks a regime change: the
	// bottleneck moved, which changes the other modes' costs far more than
	// home's when the other part dominates home's cost.
	regimeFrames         = 3
	regimePart           = 2
	compressedRegimePart = 1.5
	regimeFloor          = 0.05
	// regimeRatio is the factor by which home's compression ratio may move
	// from its long average before the probes restart: data that compresses
	// differently changes what the other modes would do even when home's
	// own times stay put. In none, the ratio is the sniffed lz4 ratio.
	regimeRatio = 1.5
	driftMargin = 1.10
	driftFrames = 8
)

// Trials.
const (
	// A trial runs a neighboring mode and compares its mean cost with
	// home's after each frame, a sequential test on both modes' sample
	// variance. It wins once the trial is faster than home by upMargin
	// (going up) or no slower than downCredit (going down) with winZ
	// standard errors to spare, and loses once it misses that bound by
	// loseZ standard errors. At maxTrialFrames the means decide alone, with
	// twice the margins. The margins settle near-ties on the cheaper codec,
	// and the gap between them keeps a tie from flapping. An undecided
	// trial of lz4 from none that comes within legMargin of none goes on to
	// zstd for half as many frames, so a weak middle rung cannot hide a
	// strong one. A trial needs two frames to lose, except on low gain,
	// and its greatest sample counts at most homeClip times the mean of the
	// others, so one stall can neither decide a trial nor seed a new home.
	maxTrialFrames = 16
	winZ           = 2.5
	loseZ          = 1.5
	upMargin       = 1.03
	downCredit     = 1.03
	// minGain is the compression ratio an upward trial's first frame must
	// reach; below it the trial ends after that one frame. lowGain is the
	// ratio below which a later frame ends it.
	minGain   = 1.10
	lowGain   = 1.08
	legMargin = 1.06
	// lowGainFrames is how many compressed frames in a row must compress
	// less than lowGain before a compressing home, or its trial of lz4,
	// drops straight to none. The ratio carries no timing noise, so a few
	// frames suffice, and lz4 compresses less than zstd, so zstd gaining
	// little means lz4 gains less. Requiring several frames keeps a short
	// incompressible stretch of compressible data from dropping the codec.
	lowGainFrames = 3
)

// Probe schedule.
const (
	// Each pair of neighboring modes schedules its own probes. Each lost
	// probe doubles its pair's gap from initialGap, and makes it at least
	// lossGap frames per frame of time the probe cost (its frames times the
	// trial's cost over home's, less one; the cost is the larger of the
	// trial's mean and its least sample, so one stall cannot stretch the
	// gap). The gap stops at maxGap while the pair's probes cost less than
	// a frame each, and at maxLossGap once one costs more, and it shrinks
	// only on a win or a regime change. So probes into a slower mode grow
	// geometrically rarer while they keep losing.
	initialGap = 24
	maxGap     = 256
	lossGap    = 64
	maxLossGap = 4096
)

// Probe directions from home, also bits of CompressionPolicy.pending.
const (
	dirUp   uint8 = 1
	dirDown uint8 = 2
)

type CompressionMetrics struct {
	LogicalSize    int64
	WireSize       int64
	PrepareLatency time.Duration
	ReadLatency    time.Duration
	WriteLatency   time.Duration
	// FrameLatency is observed prepare+write time before any delivery-rate
	// floor. Zero uses PrepareLatency+WriteLatency for synthetic observations.
	FrameLatency time.Duration
	// SniffRatio is the lz4 compression ratio of a slice of a frame sent in
	// none, or 0 when the frame was not sniffed.
	SniffRatio float32
	// RecoveryAllowed reserves RecoveryFrames frames: an episode's baseline
	// and check frames and a useful frame afterward.
	RecoveryAllowed bool
	// RecoveryReady reports whether the connection has repaid past check losses.
	RecoveryReady bool
	// Bottleneck is what the connection's socket reports limited its
	// sending recently, or BottleneckUnknown without a reading.
	Bottleneck Bottleneck
}

// Bottleneck is what limits a connection's sending, as its TCP socket
// reports it.
type Bottleneck uint8

const (
	// BottleneckUnknown means no reading: the policy decides on frame
	// costs alone.
	BottleneckUnknown Bottleneck = iota
	// BottleneckSender means the socket sat idle: reading or encoding could
	// not keep the path busy. No rule acts on it; checks of cheaper codecs
	// on real frames find such a sender.
	BottleneckSender
	// BottleneckNetwork means the path itself, its congestion window or
	// pacing, held the bytes back, so compression can help.
	BottleneckNetwork
	// BottleneckReceiver means the receiver's window held the bytes back.
	// Usually the receiver drains slower than the path delivers, and
	// compression cannot help; but a receive window below the path's
	// bandwidth-delay product reads the same, and there it does.
	BottleneckReceiver
)

func (b Bottleneck) String() string {
	switch b {
	case BottleneckSender:
		return "sender"
	case BottleneckNetwork:
		return "network"
	case BottleneckReceiver:
		return "receiver"
	}
	return "unknown"
}

type CompressionDecision struct {
	Next          CompressionMode
	Reason        string
	Ratio         float64
	ReadOverWrite float64
	// RecoveryLoss is the time to charge the recovery budget: a check frame's
	// excess over the codec preceding the check, or one frame's time when a
	// check starts.
	RecoveryLoss time.Duration
}

// CompressionPolicy compares home costs with candidate codec costs to maximize
// logical throughput. DecideSampled, its only entry point, compares upward
// codecs with background estimates and checks cheaper ones on real frames.
// Wins need sustained evidence, and losses back off. Normal home frames detect
// changed conditions and validate the estimates after a switch.
//
// Costs are seconds per logical byte. The state is copyable, its zero value
// starts at none, and DecideSampled does not allocate.
type CompressionPolicy struct {
	shadow    shadowState
	mean      float32 // home's cost, over up to homeFrames frames
	sampleVar float32 // home's relative sample variance, over up to refFrames frames
	fast      float32 // home's recent cost, EMA α=1/4
	ref       float32 // home's cost over up to refFrames frames
	fastW     float32 // home's recent write seconds per logical byte, EMA α=1/4
	refW      float32 // the same over up to refFrames frames
	ratio     float32 // home's compression ratio, EMA α=1/4, or in none the last sniff; 0 until sampled
	refRatio  float32 // the same over up to refFrames frames (none: EMA α=1/4 of sniffs)
	meanW     float32 // home's clipped write cost over up to homeFrames frames
	up, down  float32 // CUSUMs of home's cost over and under ref, in relative units

	// Comparison slots, used by one activity at a time:
	//   - a background comparison (trial != home): its virtual frames' sums
	//     of clipped costs and their squares, write seconds per logical
	//     byte, greatest cost, and logical and wire bytes, and the sum of
	//     home's costs on the frames they were predicted from (homeSum);
	//   - a check (phaseCheck): its real frames' cost, write, logical, and
	//     wire sums, its penalty, and its greatest frame cost (tMax);
	//   - a detected change waiting to check (flagDue): the references it
	//     moved from (changeRefs) and the check gap it may skip (changeGap);
	//   - a sampled codec validating (phaseValidate): the write per wire
	//     byte it won at (predictedWrite).
	// Starting one activity clears the slots of the last.
	tSum, tSq, tMin, tMax, tWrite, tLogical, tWire float32

	gap  [2]uint16 // per rung pair (see edge), frames between scheduled probes; 0 means initialGap
	wait [2]uint16 // per rung pair, home frames until the next scheduled probe

	home    CompressionMode
	trial   CompressionMode // the mode on trial, or home when none is running
	leg     uint8           // 1 when the trial is two rungs from home
	tn      uint8           // trial frames so far
	n       uint8           // home samples since home's estimates were seeded, saturating
	out     uint8           // consecutive frames home's write, prepare, or ratio sat outside its band
	pending uint8           // directions to probe without waiting
	drift   uint8           // consecutive observations of sustained cost drift
	class   Bottleneck      // the bottleneck the last steady frame reported, and classBackoff
}

// classBackoff marks, in class, upward backoff that refuted adoptions under
// the receiver class earned, which leaving that class undoes.
const classBackoff Bottleneck = 1 << 7

// bottleneck returns the class the last steady frame reported.
func (p *CompressionPolicy) bottleneck() Bottleneck { return p.class &^ classBackoff }

func NewCompressionPolicy() *CompressionPolicy {
	return &CompressionPolicy{}
}

// homeFrame records a frame sent in home, at cost seconds per logical byte,
// write of them writing, with compression ratio ratio (0 when unknown), and
// decides whether to start a trial.
func (p *CompressionPolicy) homeFrame(cost, write, ratio float32, schedule bool) CompressionDecision {
	if p.n < 255 {
		p.n++
	}
	if p.n <= seedFrames && seedStall*cost < p.fast {
		// The seed so far was a stall: start it over from this frame.
		p.n = 1
	}
	raw := cost
	mean := &p.mean
	if p.n == 1 {
		*mean, p.fast, p.ref, p.sampleVar = cost, cost, cost, 0
		p.meanW, p.fastW, p.refW = write, write, write
	} else {
		cost = min(cost, homeClip*p.fast)
		write = min(write, homeClip*max(p.fastW, regimeFloor*p.fast))
		d := (cost - p.fast) / p.fast
		*mean += (cost - *mean) / float32(min(p.n, homeFrames))
		p.meanW += (write - p.meanW) / float32(min(p.n, homeFrames))
		p.sampleVar += (d*d - p.sampleVar) / float32(min(p.n, refFrames))
		p.fast += (cost - p.fast) / float32(min(p.n, 4))
		// Established references move at most 1/homeFrames per frame,
		// so a lasting shift is measured against the level before it.
		k := float32(min(p.n, refFrames))
		if p.n > seedFrames {
			k = max(k, homeFrames)
			if p.home != CompressionModeNone {
				// Seed from home's observations before slowing the reference;
				// a noisy check must not become a lasting change baseline.
				k = float32(p.n)
				if p.n > homeFrames {
					k = 2 * refFrames
				}
			}
		}
		// Hold the reference while a sustained drift is being confirmed;
		// otherwise it can follow the new rate before eight observations.
		if p.drift == 0 {
			p.ref += (cost - p.ref) / k
		}
		p.fastW += (write - p.fastW) / float32(min(p.n, 4))
		p.refW += (write - p.refW) / k
	}
	switch {
	case ratio <= 0:
	case p.ratio == 0:
		p.ratio, p.refRatio = ratio, ratio
	case p.home == CompressionModeNone:
		p.ratio = ratio
		p.refRatio += (ratio - p.refRatio) / 4
	default:
		p.ratio += (ratio - p.ratio) / 4
		p.refRatio += (ratio - p.refRatio) / float32(min(p.n, refFrames))
	}

	settled := settleFrames
	if p.home != CompressionModeNone {
		settled = homeFrames
	}
	if int(p.n) >= settled && !p.shadow.dueNow() {
		if costMoved, ratioMoved := p.regimeChanged(raw); costMoved || ratioMoved {
			p.out, p.drift, p.up, p.down = 0, 0, 0, 0
			if costMoved {
				// Re-measure home before comparing candidates. A ratio change
				// can arrive before clipped costs catch up to a changed path.
				p.forget()
			} else {
				p.refRatio = p.ratio
			}
			p.gap, p.wait = [2]uint16{}, [2]uint16{}
			p.pending = p.neighbors()
			// Throughput moved, so a futile verdict may be stale too.
			p.shadow.futileLeft, p.shadow.futileGap = 0, 0
			p.trial, p.leg, p.tn = p.home, 0, 0
			return p.decision(p.home, "regime")
		}
	}
	if !schedule {
		return p.decision(p.home, "sample-pending")
	}
	for i := range p.wait {
		if p.wait[i] > 0 {
			p.wait[i]--
		}
	}
	if p.shadow.futileLeft > 0 {
		// Probes from an uncompressed home need samples, closed for now.
		return p.decision(p.home, "hold")
	}
	dir := p.dueDirection()
	if dir == 0 {
		return p.decision(p.home, "hold")
	}
	p.pending &^= dir
	p.trial = step(p.home, dir)
	p.tn = 0
	p.tSum, p.tSq, p.tMin, p.tMax, p.tWrite, p.tLogical, p.tWire = 0, 0, 0, 0, 0, 0, 0
	if dir == dirUp {
		return p.decision(p.trial, "probe-up")
	}
	return p.decision(p.trial, "probe-down")
}

// A background comparison sums home's costs in tMin.
func (p *CompressionPolicy) homeSum() float32     { return p.tMin }
func (p *CompressionPolicy) setHomeSum(v float32) { p.tMin = v }

// trialFrame records a frame of the running trial and judges it. home is
// home's cost on the frame the trial's cost was predicted from: the trial
// is judged against home on the same frames, so a path that changes, or
// alternates, during the trial moves both sides alike, rather than against
// a mean that lags it.
func (p *CompressionPolicy) trialFrame(cost, write, ratio, logical, wire, home float32) CompressionDecision {
	p.setHomeSum(p.homeSum() + min(home, homeClip*p.fast))
	p.tMax = max(p.tMax, cost)
	p.tSum += cost
	p.tSq += cost * cost
	p.tWrite += write
	p.tLogical += logical
	p.tWire += wire
	if p.tn < 255 {
		p.tn++
	}
	dir := p.direction()
	if dir == dirUp && (p.tn == 1 && ratio < minGain || p.tn >= 2 && p.trialRatio() < lowGain) {
		return p.lose("low-gain")
	}

	n := float32(p.tn)
	mean, tv := p.trialStats()
	v := max(p.sampleVar, minVar, tv)
	r := mean / (p.homeSum() / n)
	se := r * sqrt32(v/n+max(p.sampleVar, minVar)/float32(min(p.n, homeFrames)))
	bound := 1 / float32(upMargin)
	if dir == dirDown {
		bound = downCredit
	}
	switch {
	case p.tn >= 2 && r-loseZ*se > bound:
		return p.lose("probe-lost")
	case p.tn >= p.winFrames() && r+winZ*se < bound:
		return p.win(r, true)
	case p.tn < maxTrialFrames>>p.leg:
		// A second rung gets half the frames: it is tried to find a
		// clearly faster mode past a weak first rung.
		return p.decision(p.trial, "probe")
	case dir == dirUp && r*upMargin*upMargin < 1, dir == dirDown && r < downCredit:
		// At the limit, upward checks need twice the gain. Keep the ordinary
		// cheaper-codec credit: doubling it would overlap upward wins and
		// make both modes win the same comparison in opposite directions.
		return p.win(r, false)
	case p.home == CompressionModeNone && p.leg == 0 && r < legMargin:
		// lz4 may be no slower than none, so zstd may still beat both:
		// try it before giving up.
		p.leg = 1
		p.trial++
		p.tn = 0
		p.tSum, p.tSq, p.tMin, p.tMax, p.tWrite, p.tLogical, p.tWire = 0, 0, 0, 0, 0, 0, 0
		return p.decision(p.trial, "probe-up")
	}
	return p.lose("probe-lost")
}

// win makes the trial mode home, seeded with the trial's samples. r is the
// trial's cost over home's, and clear reports a win before maxTrialFrames.
func (p *CompressionPolicy) win(r float32, clear bool) CompressionDecision {
	dir := p.direction()
	back := dirUp | dirDown ^ dir
	n := float32(p.tn)
	mean, tv := p.trialStats()
	v := max(p.sampleVar, tv)
	ratio := float32(0)
	if p.trial != CompressionModeNone {
		ratio = p.trialRatio()
	}
	p.home = p.trial
	p.leg = 0
	p.mean = mean
	p.fast, p.ref, p.sampleVar = mean, mean, v
	p.meanW, p.fastW, p.refW = p.tWrite/n, p.tWrite/n, p.tWrite/n
	p.ratio, p.refRatio = ratio, ratio
	p.n, p.out, p.drift, p.up, p.down = p.tn, 0, 0, 0, 0
	// Every win checks the way back first, at once: home's cost may have
	// gone stale while the trial ran, and a mode far slower than the new
	// home loses that check in a frame or two, which then spaces later
	// checks by what it cost. A clear win then probes on in the same
	// direction.
	p.pending = back
	if clear {
		p.gap[p.edge(back)] = 0
		if step(p.home, dir) != p.home {
			p.gap[p.edge(dir)], p.wait[p.edge(dir)] = 0, 0
		}
	}
	return p.decision(p.home, "probe-won")
}

// lose ends a trial and backs off its direction by what it cost.
func (p *CompressionPolicy) lose(reason string) CompressionDecision {
	if p.home == CompressionModeNone && p.trial == CompressionModeLz4 && p.leg == 0 {
		// Background estimates of zstd are independent of lz4's gain or
		// speed. No trial frame is sent, so even a losing lz4 cannot hide it.
		p.leg, p.trial, p.tn = 1, CompressionModeZstdLevel1, 0
		p.tSum, p.tSq, p.tMin, p.tMax, p.tWrite, p.tLogical, p.tWire = 0, 0, 0, 0, 0, 0, 0
		return p.decision(p.trial, "sample-zstd")
	}
	dir := p.direction()
	n := float32(p.tn)
	mean, _ := p.trialStats()
	lost := n * (mean/p.mean - 1)
	if p.leg == 1 {
		// The first rung came close to home, so the next probe of the
		// pair costs little whatever the second rung cost.
		lost = 0
	}
	p.backoff(dir, lost)
	return p.endTrial(reason)
}

// endTrial returns to home, through the first rung if the trial was on the
// second.
func (p *CompressionPolicy) endTrial(reason string) CompressionDecision {
	p.leg = 0
	p.trial = p.home
	return p.decision(p.home, reason)
}

// dropToNone makes none home, abandoning any trial, once lowGainFrames
// compressed frames in a row compressed less than lowGain, the last at ratio
// ratio. None's ratio, the sniffed lz4 ratio, starts
// there, so data turning compressible again moves it and starts a probe. No
// probe up is pending, and a scheduled one waits at least the pair's gap,
// since the data just showed it would lose.
func (p *CompressionPolicy) dropToNone(ratio float32) CompressionDecision {
	p.rehome(CompressionModeNone)
	p.ratio, p.refRatio = ratio, ratio
	p.pending = 0
	gap := p.gap[0]
	if gap == 0 {
		gap = initialGap
	}
	p.wait[0] = max(p.wait[0], gap)
	return p.decision(CompressionModeNone, "low-gain")
}

// rehome makes mode home with fresh estimates.
func (p *CompressionPolicy) rehome(mode CompressionMode) {
	p.home, p.trial = mode, mode
	p.leg = 0
	p.forget()
}

// forget drops home's estimates, to be measured afresh.
func (p *CompressionPolicy) forget() {
	p.mean = 0
	p.n, p.out, p.up, p.down = 0, 0, 0, 0
	p.drift = 0
	p.sampleVar, p.fast, p.ref, p.meanW, p.fastW, p.refW = 0, 0, 0, 0, 0, 0
	p.ratio, p.refRatio = 0, 0
}

// backoff spaces dir's probes after a loss: the gap doubles from initialGap,
// is at least lossGap times lost (the frames of time the probe cost), and
// never shrinks until a win or a regime change resets it. It stops at maxGap
// while the pair's probes are cheap; once one costs a frame or more, at
// maxLossGap, so the probes of a neighbor that stays slower grow rare.
func (p *CompressionPolicy) backoff(dir uint8, lost float32) {
	i := p.edge(dir)
	prev := p.gap[i]
	gap := float32(initialGap)
	if prev != 0 {
		gap = 2 * float32(prev)
	}
	limit := float32(maxGap)
	if lost >= 1 || prev > maxGap {
		limit = maxLossGap
	}
	p.gap[i] = max(uint16(min(max(gap, lossGap*lost), limit)), prev)
	p.wait[i] = p.gap[i]
}

// regimeChanged reports whether home's cost has moved from its long average,
// by a CUSUM of the frame costs, or whether its write time, prepare time, or
// compression ratio has sat outside its band for regimeFrames frames.
func (p *CompressionPolicy) regimeChanged(cost float32) (costMoved, ratioMoved bool) {
	sd := sqrt32(max(p.sampleVar, minVar))
	k := max(regimeMinK, regimeK*sd)
	h := max(regimeShift, regimeH*sd)
	// No frame adds more than h/regimeCap, so one stall cannot trip it.
	d := min(max(cost/p.ref-1, -k-h/regimeCap), k+h/regimeCap)
	p.up = max(0, p.up+d-k)
	p.down = max(0, p.down-d-k)
	costMoved = p.up > h || p.down > h
	// Seed the short mean before asking it to confirm sustained drift.
	if p.home != CompressionModeNone && p.n >= homeFrames && movedBy(p.mean, p.ref, driftMargin) {
		p.drift++
	} else {
		p.drift = 0
	}
	costMoved = costMoved || p.drift >= driftFrames
	partMoved := p.partMoved(p.fastW, p.refW) || p.partMoved(p.fast-p.fastW, p.ref-p.refW)
	ratioMoved = p.ratio > 0 && p.refRatio > 0 && movedBy(p.ratio, p.refRatio, regimeRatio)
	if !partMoved && !ratioMoved {
		p.out = 0
		return costMoved, false
	}
	p.out++
	if p.out < regimeFrames {
		return costMoved, false
	}
	return costMoved || partMoved, ratioMoved
}

// partMoved reports whether one part of home's cost, its write or its
// prepare time, moved by its mode's threshold and by more than regimeFloor
// of the cost.
func (p *CompressionPolicy) partMoved(fast, ref float32) bool {
	part := float32(regimePart)
	if p.home != CompressionModeNone {
		part = compressedRegimePart
	}
	return movedBy(fast, ref, part) && abs32(fast-ref) > regimeFloor*p.ref
}

// rememberChange saves the references and variance a detected change moved
// from, while ordinary home frames refresh its baseline.
func (p *CompressionPolicy) rememberChange(ref, writeRef, variance float32) {
	p.tMin, p.tSum, p.tSq = ref, writeRef, variance
}

// changeRefs returns what rememberChange saved; ref is 0 for none.
func (p *CompressionPolicy) changeRefs() (ref, writeRef, variance float32) {
	return p.tMin, p.tSum, p.tSq
}

// changeGap is the downward check gap a detected change may skip, which
// sets how much evidence it needs (checkConfidence).
func (p *CompressionPolicy) changeGap() float32     { return p.tLogical }
func (p *CompressionPolicy) setChangeGap(g float32) { p.tLogical = g }

func (p *CompressionPolicy) changePersists(z float32) bool {
	ref, writeRef, variance := p.changeRefs()
	if ref == 0 {
		return true
	}
	v := max(variance, p.sampleVar)
	// dueCheck asks again on each frame until homeFrames, so an earlier
	// answer needs more confidence, as in a group-sequential test: for the
	// cumulative cost mean, all the looks together pass noise about as
	// rarely as one look at homeFrames.
	n := float32(min(p.n, homeFrames))
	z *= sqrt32(homeFrames / n)
	margin := max(float32(driftMargin), 1+z*sqrt32(v/n))
	// Independent prepare and write jitter can average out in total cost.
	// Allow twice its relative variance for the shorter write EMA.
	writeSD := sqrt32(2 * v)
	writeMargin := max(float32(driftMargin), 1+z*writeSD/sqrt32(float32(min(p.n, 4))))
	return movedBy(p.mean, ref, margin) || movedBy(p.fastW, writeRef, writeMargin)
}

// Costly failed checks need more evidence before a change skips their
// backoff: z rises with the gap, to 4 at maxLossGap. Under heavy noise a
// regime detector fires often, and each check skipped past a loss gap can
// cost many frames.
func checkConfidence(gap float32) float32 {
	return max(1, sqrt32(gap/maxGap))
}

// dueDirection returns the direction to probe now, or 0: a pending one once
// home has seedFrames samples, or else one whose schedule is due once it has
// settleFrames. When both are, it looks up first.
func (p *CompressionPolicy) dueDirection() uint8 {
	settle := seedFrames
	valid := p.neighbors()
	want := p.pending & valid
	switch {
	case want != 0 && p.n < seedFrames:
		return 0
	case want == 0 && int(p.n) < settle:
		return 0
	case want == 0:
		for _, dir := range [2]uint8{dirUp, dirDown} {
			if valid&dir != 0 && p.wait[p.edge(dir)] == 0 {
				want |= dir
			}
		}
	}
	if want == dirUp|dirDown {
		return dirUp
	}
	return want
}

// edge returns the index of the rung pair between home and its neighbor in
// dir: 0 for none and lz4, 1 for lz4 and zstd. Probe schedules belong to
// pairs, since a pair's comparison is the same from either side.
func (p *CompressionPolicy) edge(dir uint8) int {
	if dir == dirDown {
		return int(p.home) - 1
	}
	return int(p.home)
}

// direction returns the direction of the running trial from home.
func (p *CompressionPolicy) direction() uint8 {
	if p.trial > p.home {
		return dirUp
	}
	return dirDown
}

// neighbors returns the directions with a rung from home.
func (p *CompressionPolicy) neighbors() uint8 {
	switch p.home {
	case CompressionModeNone:
		return dirUp
	case CompressionModeZstdLevel1:
		return dirDown
	default:
		return dirUp | dirDown
	}
}

// trialStats returns the running trial's mean cost and relative sample
// variance (0 below two samples), with its greatest sample counted at most
// homeClip times the mean of the others.
func (p *CompressionPolicy) trialStats() (mean, v float32) {
	n := float32(p.tn)
	if p.tn < 2 {
		return p.tSum / n, 0
	}
	sum, sq := p.tSum, p.tSq
	if c := homeClip * (sum - p.tMax) / (n - 1); p.tMax > c {
		sum += c - p.tMax
		sq += c*c - p.tMax*p.tMax
	}
	mean = sum / n
	return mean, (sq/n - mean*mean) / (mean * mean) * n / (n - 1)
}

func (p *CompressionPolicy) trialRatio() float32 {
	if p.tWire <= 0 {
		return 1
	}
	return p.tLogical / p.tWire
}

func (p *CompressionPolicy) decision(next CompressionMode, reason string) CompressionDecision {
	ratio := float64(p.ratio)
	if p.trial != p.home && p.tn > 0 {
		ratio = float64(p.trialRatio())
	}
	if ratio == 0 {
		ratio = 1
	}
	rw := float64(1)
	if p.meanW > 0 {
		rw = float64(max(0, p.mean-p.meanW) / p.meanW)
	} else if p.mean > 0 {
		rw = 9999
	}
	return CompressionDecision{Next: next, Reason: reason, Ratio: ratio, ReadOverWrite: rw}
}

// step returns the adaptive mode one rung from mode in dir, or mode at the
// end of the ladder.
func step(mode CompressionMode, dir uint8) CompressionMode {
	switch {
	case dir == dirUp && mode < CompressionModeZstdLevel1:
		return mode + 1
	case dir == dirDown && mode > CompressionModeNone:
		return mode - 1
	}
	return mode
}

// movedBy reports whether x differs from ref by more than factor, either way.
func movedBy(x, ref, factor float32) bool {
	return x > ref*factor || x*factor < ref
}

func sqrt32(x float32) float32 { return float32(math.Sqrt(float64(max(x, 0)))) }

func FrameCompTokenForMode(mode CompressionMode) string {
	switch mode {
	case CompressionModeLz4:
		return encoding.EncodingLz4
	case CompressionModeNone:
		return "none"
	default:
		return encoding.EncodingZstd
	}
}

// FileCompression is the adaptive compression state a file carries from one
// window to the next. Its zero value starts at none with a fresh policy.
type FileCompression struct {
	Mode   CompressionMode
	Policy CompressionPolicy
}

func abs32(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
