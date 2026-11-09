package policy

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

const recoveryBudgetScale = 100

// recoveryCheckFrames is how many frames a winning check candidate sends at
// least, and maxCheckFrames at most: a candidate whose mean is above the bar,
// but not by more than home's noise explains, keeps sending.
const (
	recoveryCheckFrames = 2
	maxCheckFrames      = 4
)

// checkZ is how many standard errors of the check frames' mean above the
// bar end a check before maxCheckFrames. A single noisy frame must not
// restore a codec the candidate beats: each restore backs off the next
// check, which leaves the policy in the slower codec.
const checkZ = 2

// recoveryBaselineFrames home frames start each check episode.
const recoveryBaselineFrames = 2

// floorRatio is the smallest compression ratio whose loss in a check can
// reveal a receiver-disk floor: a network path would move writes by that
// factor, well past the 1.25 a floor allows.
const floorRatio = 1.25 * 1.25

// RecoveryFrames is the frames a file must have left to allow a check: its
// baseline frames, check frames for none and lz4, and a useful frame
// afterward.
const RecoveryFrames = recoveryBaselineFrames + 2*maxCheckFrames + 1

// A policy is steady, checking cheaper codecs on real frames, or
// validating a codec it adopted.
const (
	phaseSteady uint8 = iota
	phaseCheck
	phaseValidate
)

// Flags qualify a phase: flagDue defers a requested check until home may
// start one; flagAfterCheck marks a validation that follows a winning check,
// whose frames are measured and charged to the recovery budget; flagRecheck
// requests a check if that validation keeps the winner (pathMoved);
// flagSmallGain marks an adopted codec that saves too few wire bytes over
// the one it replaced to reveal a receiver-disk floor (floorRatio);
// flagStalled marks a validation with a frame past validateStall.
const (
	flagDue uint8 = 1 << iota
	flagAfterCheck
	flagRecheck
	flagSmallGain
	flagStalled
)

func (s *shadowState) due() bool { return s.flags&flagDue != 0 }

// dueNow reports a requested check that home may start once it is steady.
// A check requested while a check's winner validates waits for it to end.
func (s *shadowState) dueNow() bool {
	return s.due() && s.flags&flagAfterCheck == 0 && s.phase != phaseCheck
}

// validatingCheck reports validation of a check's winner.
func (s *shadowState) validatingCheck() bool {
	return s.phase == phaseValidate && s.flags&flagAfterCheck != 0
}

// RecoveryBudget is shared by every connection of a server, not kept per
// file or connection: otherwise each new connection, and without keep-alive
// each file, would get an unpaid check. A lost second requires 100 seconds
// of ordinary frame time, summed across connections, before periodic checks
// resume. A detected change may check sooner without forgiving debt. It is
// safe for concurrent use and must not be copied.
type RecoveryBudget struct {
	debt atomic.Uint64 // float64 seconds, as bits; never negative
}

func (b *RecoveryBudget) Ready() bool { return math.Float64frombits(b.debt.Load()) <= 0 }

func (b *RecoveryBudget) Observe(loss, elapsed time.Duration) {
	for {
		old := b.debt.Load()
		if old == 0 && loss == 0 {
			return // Steady state reads the shared value and writes nothing.
		}
		debt := math.Float64frombits(old)
		if loss > 0 {
			debt += recoveryBudgetScale * loss.Seconds()
		} else {
			debt = max(0, debt-elapsed.Seconds())
		}
		// A lost update would forgive debt, so retry until this one lands.
		if b.debt.CompareAndSwap(old, math.Float64bits(debt)) {
			return
		}
	}
}

// RecoveryActive includes validation, whose final frame can still incur debt.
func (p *CompressionPolicy) RecoveryActive() bool {
	return p.shadow.phase == phaseCheck || p.shadow.validatingCheck()
}

func (p *CompressionPolicy) requestRecovery() {
	if p.home == CompressionModeNone {
		return
	}
	p.clearRecoverySamples() // including a stale change gap
	p.shadow.flags |= flagDue
}

// DecideSampled normally compares codecs off the wire. A compressed home
// also checks cheaper codecs on real frames: a background estimate cannot
// tell whether fewer wire bytes would reduce downstream backpressure.
func (p *CompressionPolicy) DecideSampled(current CompressionMode, m CompressionMetrics, sample *encoding.CodecSamples) CompressionDecision {
	if m.LogicalSize <= 0 || m.PrepareLatency < 0 || m.WriteLatency < 0 || m.PrepareLatency+m.WriteLatency <= 0 {
		return p.decision(current, "hold")
	}
	if current > CompressionModeZstdLevel1 {
		*p = CompressionPolicy{}
		current = CompressionModeNone
	}
	if p.shadow.phase == phaseCheck {
		if current == p.trial {
			return p.observeRecovery(m)
		}
		// A caller that did not send the requested codec starts afresh.
		p.shadow.phase = phaseSteady
	}
	var loss time.Duration
	if p.shadow.validatingCheck() && current == p.home {
		loss = p.recoveryLoss(m)
	}
	d := p.decideSampled(current, m, sample)
	d.RecoveryLoss += loss
	return d
}

// startRecovery begins an episode with recoveryBaselineFrames home frames,
// flushed as check frames are. Ordinary frames can leave their bytes in a
// response buffer, so home's average can omit time the candidates' flushed
// frames include; or ordinary frames can pay for their drains as they
// stream, so the average already includes it. The average, the steadier
// estimate, stays the bar unless the baseline frames show it missed time.
// A stall can slow one baseline frame but not make it faster, so they are
// judged by their fastest: a stalled frame must not raise the bar a slower
// codec has to beat, since the bar also decides what the budget is charged.
func (p *CompressionPolicy) startRecovery(write float32) CompressionDecision {
	s := &p.shadow
	s.previous, s.priorCost, s.priorWrite = p.home, s.cost, s.write
	s.phase, s.flags = phaseCheck, 0
	p.trial, p.leg, p.tn = p.home, 0, 0
	p.pending &^= dirDown
	p.clearRecoverySamples()
	// Keep the latest home's write cost: a moving average alone can hide a
	// faster path at check start.
	p.setCheckStartWrite(write)
	return p.decision(p.home, "recovery-baseline")
}

// observeBaseline records a baseline frame, and after the last one starts
// the check with none. The fastest baseline frame replaces home's average as
// the bar only when it exceeds the average by more than the noise of both
// explains, and only once the average has homeFrames frames behind it.
func (p *CompressionPolicy) observeBaseline(cost, write float32) CompressionDecision {
	s := &p.shadow
	if p.tn == 0 || cost < p.tMin {
		p.tMin, p.tWrite = cost, write
	}
	p.tn++
	if p.tn < recoveryBaselineFrames {
		return p.decision(p.home, "recovery-baseline")
	}
	v := max(p.sampleVar, minVar)
	se := sqrt32(v/float32(p.tn) + v/float32(min(p.n, homeFrames)))
	if p.n >= homeFrames && p.tMin > s.priorCost*(1+winZ*se) {
		s.priorCost, s.priorWrite = p.tMin, p.tWrite
	}
	p.trial, p.tn = CompressionModeNone, 0
	p.clearRecoverySamples()
	p.setCheckStartWrite(write)
	return p.decision(p.trial, "recovery-check")
}

// A candidate wins when the mean of at least recoveryCheckFrames frames
// beats the prior baseline, and loses once its mean is clearly above it or
// it has sent maxCheckFrames; a losing none check may be followed by lz4,
// but never a stronger codec.
func (p *CompressionPolicy) observeRecovery(m CompressionMetrics) CompressionDecision {
	cost := float32(observedLatency(m).Seconds()) / float32(m.LogicalSize)
	if p.trial == p.home {
		return p.observeBaseline(cost, float32(m.WriteLatency.Seconds())/float32(m.LogicalSize))
	}
	loss := p.recoveryLoss(m)
	penalty := p.penalty() + max(0, cost/p.shadow.priorCost-1)
	p.setPenalty(penalty)
	p.tn++
	p.tSum += cost
	p.tWrite += float32(m.WriteLatency.Seconds()) / float32(m.LogicalSize)
	p.tLogical += float32(m.LogicalSize)
	p.tWire += float32(m.WireSize)
	var d CompressionDecision
	bar := p.shadow.priorCost / upMargin
	mean := p.tSum / float32(p.tn)
	se := sqrt32(max(p.sampleVar, minVar) / float32(p.tn))
	if mean >= bar*(1+checkZ*se) || mean >= bar && p.tn >= maxCheckFrames {
		if p.home == CompressionModeZstdLevel1 && p.trial == CompressionModeNone {
			noneWrite := p.tWrite / float32(p.tn)
			p.trial, p.tn = CompressionModeLz4, 0
			p.clearRecoverySamples()
			p.setPenalty(penalty)
			p.setCheckNoneWrite(noneWrite)
			d = p.decision(p.trial, "recovery-check")
		} else {
			p.trial, p.tn = p.home, 0
			p.shadow.phase = phaseSteady
			p.backoff(dirDown, penalty)
			d = p.decision(p.home, "recovery-restore")
		}
	} else if p.tn < recoveryCheckFrames || mean >= bar {
		d = p.decision(p.trial, "recovery-confirm")
	} else {
		previous, previousRatio := p.home, p.ratio
		write := p.tWrite / float32(p.tn)
		ratio := p.trialRatio()
		p.rehome(p.trial)
		p.mean, p.fast, p.ref = mean, mean, mean
		p.meanW, p.fastW, p.refW = write, write, write
		p.ratio, p.refRatio = ratio, ratio
		if p.home == CompressionModeNone {
			// An uncompressed home's ratio comes from background samples,
			// not the check's identity encoding.
			p.ratio, p.refRatio = 0, 0
		}
		p.n = 2
		p.shadow.previous = previous
		// Validation averages from the check frames, so time a send buffer
		// absorbed from them counts against the codec.
		p.shadow.cost, p.shadow.write, p.shadow.predicted = mean, write, mean
		p.shadow.validate, p.shadow.low = validateFrames, 0
		p.shadow.phase, p.shadow.flags = phaseValidate, flagAfterCheck
		// An unchanged write cost despite more wire bytes is an observed
		// floor for later background comparisons, not another codec sample.
		// Only a codec that saved enough wire bytes can tell a disk floor
		// from noise: below floorRatio, a network path moves writes too
		// little to tell apart.
		if p.home == CompressionModeNone && previousRatio >= floorRatio &&
			!movedBy(write, p.shadow.priorWrite, 1.25) && !movedBy(write, p.checkStartWrite(), 1.25) {
			p.shadow.floor = min(write, p.shadow.priorWrite)
		}
		// Upward comparisons resume at once: a path change can straddle the
		// check, and a disk floor the check exposed is recorded above.
		p.pending = 0
		if p.pathMoved(previous, write, ratio) {
			// none lost on a path the check outlived; check it again if lz4
			// validates, rather than wait out the debt it incurred.
			p.shadow.flags |= flagRecheck
		}
		d = p.decision(p.home, "recovery-won")
	}
	d.RecoveryLoss = loss
	return d
}

// A check sums its frames in the comparison slots, keeps the penalty an
// earlier candidate lost in tSq, and home's write at its start in tMax.
func (p *CompressionPolicy) penalty() float32             { return p.tSq }
func (p *CompressionPolicy) setPenalty(v float32)         { p.tSq = v }
func (p *CompressionPolicy) checkStartWrite() float32     { return p.tMax }
func (p *CompressionPolicy) setCheckStartWrite(w float32) { p.tMax = w }

// After none loses a check from zstd, tMin keeps its write cost per logical
// byte for pathMoved.
func (p *CompressionPolicy) checkNoneWrite() float32     { return p.tMin }
func (p *CompressionPolicy) setCheckNoneWrite(w float32) { p.tMin = w }

// pathMoved reports whether lz4, winning a check from zstd after none lost,
// wrote at a cost no single path explains. On one path, lz4's write per
// logical byte lies between none's and none's over the ratio: a disk writes
// logical bytes, a network wire bytes. Outside that range by regimePart, the
// path changed between none's frame and lz4's.
func (p *CompressionPolicy) pathMoved(previous CompressionMode, write, ratio float32) bool {
	none := p.checkNoneWrite()
	if previous != CompressionModeZstdLevel1 || p.home != CompressionModeLz4 || none <= 0 {
		return false
	}
	return write*regimePart < none/max(1, ratio) || write > none*regimePart
}

func (p *CompressionPolicy) clearRecoverySamples() {
	p.tSum, p.tSq, p.tMin, p.tMax, p.tWrite, p.tLogical, p.tWire = 0, 0, 0, 0, 0, 0, 0
}

func (p *CompressionPolicy) recoveryLoss(m CompressionMetrics) time.Duration {
	baseline := float64(p.shadow.priorCost) * float64(m.LogicalSize) * float64(time.Second)
	elapsed := observedLatency(m)
	if baseline >= float64(elapsed) {
		return 0
	}
	return elapsed - time.Duration(max(0, baseline))
}

func observedLatency(m CompressionMetrics) time.Duration {
	if m.FrameLatency > 0 {
		return m.FrameLatency
	}
	return m.PrepareLatency + m.WriteLatency
}

// A restored home has already measured its baseline before the check.
func (p *CompressionPolicy) seedRecoveryBaseline(cost, write float32) {
	p.n = seedFrames
	p.mean, p.fast, p.ref = cost, cost, cost
	p.meanW, p.fastW, p.refW = min(write, cost), min(write, cost), min(write, cost)
}
