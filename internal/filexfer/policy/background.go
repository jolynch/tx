package policy

import (
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// BackgroundWinFrames is the least number of independent background samples
// that can choose a new codec. SEND also reserves a frame for the result to
// arrive, and the baseline samples a fresh file still needs.
const BackgroundWinFrames = 4

// shadowState keeps observed frame costs separate from the trial engine's
// estimates. It contains no worker or buffer, so file state remains copyable.
type shadowState struct {
	cost, write, priorCost, priorWrite, floor float32 // seconds per logical byte
	zstdRatio                                 float32
	// predicted is the adopted codec's cost per byte when it won: a
	// sample's estimate, or the checks' measurement.
	predicted                  float32
	previous                   CompressionMode
	validate, low, dataChanged uint8
	// Frames until sampling reopens, and the gap used for its backoff.
	futileLeft, futileGap uint8
	// phase is phaseSteady, phaseCheck, or phaseValidate, which validate
	// counts down; flags qualify it.
	phase, flags uint8
}

// Back off only on clear losses. Frame-cost shifts reopen sampling early;
// otherwise the bounded gap lets later data overturn a futile verdict.
// validateFrames is how many ordinary frames confirm an adopted codec.
const validateFrames = 8

const (
	futileMargin   = 2
	futileFirstGap = 8
	futileMaxGap   = 64
)

// futile requires both codecs to cost more CPU than they could save in
// writing, with a margin for measurement noise. A partial sample cannot
// rule out the missing codec.
func futile(sample encoding.CodecSamples, write float32) bool {
	if !sample.Has(encoding.EncodingLz4) || !sample.Has(encoding.EncodingZstd) {
		return false
	}
	for _, codec := range [...]string{encoding.EncodingLz4, encoding.EncodingZstd} {
		saved := write * (1 - 1/sample.Ratio(codec))
		encode := float32(sample.Cost(codec).Seconds()) / float32(sample.LogicalSize)
		if encode < futileMargin*saved {
			return false
		}
	}
	return true
}

func (p *CompressionPolicy) winFrames() uint8 { return BackgroundWinFrames }

// BackgroundPending reports whether a comparison needs another independent
// sample. Recovery checks and normal-frame validation need no codec worker.
func (p *CompressionPolicy) BackgroundPending() bool {
	return p.shadow.phase == phaseSteady && p.trial != p.home
}

// SampleNeeds names the codecs the next background sample must measure. A
// comparison needs both encode costs. A settled policy reads only ratios
// its own frames cannot show: none needs lz4's, as its sniff, and zstd's, to
// see changed data; lz4 needs zstd's; zstd needs none, since its frames show
// its ratio and SEND then takes no sample.
func (p *CompressionPolicy) SampleNeeds() encoding.SampleSet {
	switch {
	case p.shadow.flags != 0 || p.shadow.phase != phaseSteady:
		return 0
	case p.home == CompressionModeNone && p.shadow.futileLeft > 0:
		// No codec could pay at this link speed; see futile.
		return 0
	case p.BackgroundPending(), p.home == CompressionModeNone:
		return encoding.SampleBoth
	case p.home == CompressionModeLz4:
		return encoding.SampleZstd
	default:
		return 0
	}
}

// sampleCovers reports whether sample measured the codecs of home and
// trial, as a comparison needs. A sample requested before the comparison
// opened, or before home moved, can lack one; the comparison then waits for
// the next.
func sampleCovers(sample encoding.CodecSamples, home, trial CompressionMode) bool {
	has := func(mode CompressionMode) bool {
		return mode == CompressionModeNone || sample.Has(FrameCompTokenForMode(mode))
	}
	return sample.Valid() && has(home) && has(trial)
}

// BackgroundFramesNeeded counts the frames, from the one about to be sampled
// through the first one sent in a new codec, that a sample needs to change
// the codec. A pending comparison needs the rest of its baseline and winning
// samples. Without one, a sample only detects changed data: regimeFrames
// moved samples, sampleEvery frames apart and after one that sets the
// reference, then a whole comparison.
func (p *CompressionPolicy) BackgroundFramesNeeded(sampleEvery int) int {
	baseline := max(0, seedFrames-int(p.n))
	if p.BackgroundPending() {
		return baseline + max(1, BackgroundWinFrames-int(p.tn)) + 1
	}
	detect := (regimeFrames-1)*sampleEvery + 1
	if p.shadow.zstdRatio == 0 {
		detect += sampleEvery
	}
	return max(baseline, detect) + BackgroundWinFrames + 1
}

// decideSampled observes a normally sent frame and, when sample is non-nil,
// advances a background comparison. Only a winning comparison changes the
// wire codec. Missing or busy samples never select a trial frame or wait.
// The candidate's CPU and ratio come from sample; transport cost is estimated
// from the current frame. Normal frames validate a switch and restore the
// previous codec when the estimate overpromised.
func (p *CompressionPolicy) decideSampled(current CompressionMode, m CompressionMetrics, sample *encoding.CodecSamples) CompressionDecision {
	if current != p.home {
		p.rehome(current)
		p.shadow = shadowState{}
	}
	if current == CompressionModeNone && p.shadow.dueNow() {
		p.shadow.flags &^= flagDue
	}
	if sample != nil && p.SampleNeeds() == 0 {
		sample = nil // The policy reads nothing from a sample now.
	}
	f := p.observeCost(m)
	if s := &p.shadow; current != CompressionModeNone && m.WireSize > 0 && f.logical/float32(m.WireSize) < lowGain {
		s.low++
	} else {
		s.low = 0
	}
	if s := &p.shadow; s.low >= lowGainFrames {
		s.cost, s.write, s.validate, s.low = 0, 0, 0, 0
		s.phase, s.flags = phaseSteady, 0
		return p.dropToNone(f.logical / float32(m.WireSize))
	}
	if p.shadow.phase == phaseValidate {
		if d, restored := p.validateFrame(current, f); restored {
			return d
		}
	}
	sample = p.futileSample(current, sample)
	p.watchSampledRatios(current, sample, &m)
	reason := p.observeHome(current, m, f)
	if d, started := p.dueCheck(current, m, f); started {
		return d
	}
	if p.trial != p.home && sample != nil && sampleCovers(*sample, current, p.trial) {
		reason = p.compareSample(current, m, *sample)
	}
	if p.home != current {
		p.adopted(current)
		reason = "sample-won"
	}
	return p.decision(p.home, reason)
}

// frameCosts is one frame's cost per logical byte: with the delivery-rate
// floor (cost), as measured (observed), and its write alone.
type frameCosts struct {
	logical, cost, observed, write float32
}

// observeCost adds the frame to the shadow's home cost: the mean since a
// switch while validating, a fresh mean while re-measuring a changed home,
// and otherwise a clipped average.
func (p *CompressionPolicy) observeCost(m CompressionMetrics) frameCosts {
	s := &p.shadow
	logical := float32(m.LogicalSize)
	f := frameCosts{
		logical:  logical,
		cost:     float32((m.PrepareLatency + m.WriteLatency).Seconds()) / logical,
		observed: float32(observedLatency(m).Seconds()) / logical,
		write:    float32(m.WriteLatency.Seconds()) / logical,
	}
	observed, write := f.observed, f.write
	if s.cost == 0 || s.dueNow() && p.n == 0 {
		s.cost, s.write = observed, write
	} else if s.phase == phaseValidate {
		// A send buffer can absorb a new codec's first frames, so their raw
		// time understates its cost until the buffer fills. Validate the
		// mean of every frame since the switch, unclipped: absorbed time
		// comes back on later frames, where a clipped average would hide it.
		n := float32(validateFrames - s.validate + 1)
		if s.validatingCheck() {
			n += recoveryCheckFrames // Seeded with the check frames' mean.
		}
		s.cost += (observed - s.cost) / n
		s.write += (write - s.write) / n
	} else if s.dueNow() && p.n < seedFrames {
		// Re-measure the changed home before comparing cheaper frames.
		// Its old clipped average can still be far below the new cost.
		n := float32(p.n + 1)
		s.cost += (observed - s.cost) / n
		s.write += (write - s.write) / n
	} else {
		s.cost += (min(observed, 2*s.cost) - s.cost) / 4
		s.write += (write - s.write) / 4
	}
	if s.floor > write*2 {
		s.floor = 0 // The path became faster than the learned disk limit.
	}
	if s.futileLeft > 0 {
		s.futileLeft--
	}
	return f
}

// validateFrame counts one frame toward validating an adopted codec and
// restores the previous codec when the adopted one proves slower.
func (p *CompressionPolicy) validateFrame(current CompressionMode, f frameCosts) (CompressionDecision, bool) {
	s := &p.shadow
	write, cost := f.write, f.cost
	if s.validatingCheck() && !s.due() && current != CompressionModeNone {
		// Cheaper encoding normally sends more bytes. A sustained drop
		// in write cost reveals a change that straddled the check. Trial
		// storage is free while the new home validates.
		if write*compressedRegimePart < s.priorWrite && s.priorWrite-write > regimeFloor*cost {
			p.tn++
		} else {
			p.tn = 0
		}
		if p.tn >= regimeFrames {
			p.requestRecovery()
		}
	}
	// Later frames cannot take less than nothing, so once the time
	// since the switch exceeds what every validation frame may take,
	// the codec has already lost: restore it now.
	done := float32(validateFrames - s.validate + 1)
	if s.validatingCheck() {
		done += recoveryCheckFrames
	}
	if s.cost*done > s.priorCost*1.10*(done+float32(s.validate-1)) {
		s.validate = 1
	}
	s.validate--
	if s.validate == 0 && s.cost > s.priorCost*1.10 {
		recovering := s.validatingCheck()
		previous := s.previous
		pathMoved := s.write > 1.25*s.priorWrite
		if previous < current && !movedBy(s.write, s.priorWrite, 1.25) {
			// Wire savings did not reduce the real write cost: the
			// receiver's disk, rather than the network, can be binding.
			s.floor = min(s.write, s.priorWrite)
		}
		p.rehome(previous)
		p.seedRecoveryBaseline(s.priorCost, s.priorWrite)
		if previous < current {
			gap := uint16(maxGap)
			if pathMoved {
				gap = initialGap // Reconsider promptly under the new path.
			}
			p.gap[previous], p.wait[previous] = gap, gap
		}
		s.cost, s.write = s.priorCost, s.priorWrite
		s.phase, s.flags = phaseSteady, s.flags&flagDue
		if recovering {
			return p.decision(previous, "recovery-restore"), true
		}
		return p.decision(previous, "sample-overestimated"), true
	}
	if s.validate == 0 && s.predicted > 0 && movedBy(s.cost, s.predicted, regimePart) {
		// The codec costs far from what it won at: the path changed
		// while it validated, or the estimate missed a bottleneck, and
		// its baseline has absorbed either. A cheaper codec may now win.
		// Check without waiting on debt from before, once fresh home
		// timings confirm the cost still differs from the estimate.
		p.requestRecovery()
		p.rememberChange(s.predicted, p.fastW, p.sampleVar)
	}
	if s.validate == 0 {
		if s.flags&flagRecheck != 0 {
			p.requestRecovery()
		}
		s.phase, s.flags = phaseSteady, s.flags&flagDue
	}
	return CompressionDecision{}, false
}

// futileSample stops comparing and sampling an uncompressed home while no
// codec could repay its CPU, and returns the sample the frame may still use.
func (p *CompressionPolicy) futileSample(current CompressionMode, sample *encoding.CodecSamples) *encoding.CodecSamples {
	s := &p.shadow
	if current == CompressionModeNone && sample != nil && sample.Valid() {
		if futile(*sample, s.write) {
			// Stop comparing and sampling until home's cost shifts or the
			// gap ends.
			s.futileGap = min(max(futileFirstGap, 2*s.futileGap), futileMaxGap)
			s.futileLeft = s.futileGap
			if p.BackgroundPending() {
				p.trial, p.leg, p.tn = p.home, 0, 0
			}
			sample = nil
		} else {
			s.futileLeft, s.futileGap = 0, 0
		}
	}
	return sample
}

// watchSampledRatios takes the sniff ratio from a sample and watches zstd's
// sampled ratio below a zstd home, reopening exploration when it moves.
func (p *CompressionPolicy) watchSampledRatios(current CompressionMode, sample *encoding.CodecSamples, m *CompressionMetrics) {
	s := &p.shadow
	// A sample can arrive after the state that requested it changed, and
	// lack a codec the new state reads; that codec counts as unsampled for
	// this frame, as if no sample had arrived.
	if sample != nil && sample.Has(encoding.EncodingLz4) {
		m.SniffRatio = sample.Ratio(encoding.EncodingLz4)
	}
	// A zstd home watches its own frames' ratio (homeFrame), so only lower
	// rungs watch zstd's sampled ratio. Its reference starts over below a
	// zstd home, rather than resume from before it.
	if current >= CompressionModeZstdLevel1 {
		s.zstdRatio, s.dataChanged = 0, 0
	} else if sample != nil && sample.Has(encoding.EncodingZstd) {
		ratio := sample.Ratio(encoding.EncodingZstd)
		if s.zstdRatio == 0 {
			s.zstdRatio = ratio
		} else if movedBy(ratio, s.zstdRatio, regimeRatio) {
			s.dataChanged++
			if s.dataChanged >= regimeFrames {
				// lz4 can keep gaining nothing while zstd starts working.
				// A few independent samples reopen exploration in that case.
				p.rehome(current)
				p.requestRecovery()
				p.gap, p.wait = [2]uint16{}, [2]uint16{}
				p.pending = p.neighbors()
				s.zstdRatio, s.dataChanged = ratio, 0
			}
		} else {
			s.zstdRatio += (ratio - s.zstdRatio) / 4
			s.dataChanged = 0
		}
	}
}

// observeHome feeds the frame to home's baseline and change detection, and
// schedules nothing while a comparison, validation, or check is pending.
func (p *CompressionPolicy) observeHome(current CompressionMode, m CompressionMetrics, f frameCosts) string {
	s := &p.shadow
	cost, write, observed, logical := f.cost, f.write, f.observed, f.logical
	priorRef, priorWriteRef := p.ref, p.refW
	priorVariance := p.sampleVar
	priorCheckGap := uint16(0)
	if current != CompressionModeNone {
		priorCheckGap = p.gap[p.edge(dirDown)]
	}
	// While a comparison, validation, or check is pending, home frames
	// still refresh the baseline; only scheduling is suspended.
	scheduled := p.trial == p.home && s.phase == phaseSteady && !s.dueNow()
	ratio := logical / float32(max(1, m.WireSize))
	if current == CompressionModeNone {
		ratio = m.SniffRatio
	}
	reason := p.homeFrame(cost, write, ratio, scheduled).Reason
	if !scheduled && s.phase == phaseSteady && current != CompressionModeNone && p.wait[p.edge(dirDown)] > 0 {
		p.wait[p.edge(dirDown)]--
	}
	if reason == "regime" {
		s.floor = 0
		if current == CompressionModeNone && s.phase == phaseSteady {
			// A newly useful codec is validated against the changed raw
			// path, rather than an average still clipped near its old rate.
			s.cost, s.write = observed, write
		}
		p.requestRecovery()
		if current != CompressionModeNone && s.phase == phaseSteady && p.n == 0 {
			// Keep the references of a detected cost shift while ordinary
			// home frames refresh its baseline. Recovery owns these slots.
			p.rememberChange(priorRef, priorWriteRef, priorVariance)
			p.setChangeGap(float32(priorCheckGap))
		}
	}
	return reason
}

// dueCheck starts a check of cheaper codecs on real frames when one is due
// and allowed, or reports why a forced one waits.
func (p *CompressionPolicy) dueCheck(current CompressionMode, m CompressionMetrics, f frameCosts) (CompressionDecision, bool) {
	s := &p.shadow
	write := f.write
	if s.phase == phaseSteady && current != CompressionModeNone {
		forced := s.dueNow()
		periodic := p.n >= settleFrames && p.wait[p.edge(dirDown)] == 0
		if p.n >= seedFrames && (forced || periodic || p.trial < p.home) {
			if m.RecoveryAllowed && (forced || m.RecoveryReady) {
				if forced && !m.RecoveryReady && !p.changePersists(checkConfidence(p.changeGap())) {
					// Noise can move a clipped mean long enough to fire the
					// detector, then disappear during baseline collection.
					// Only a persistent change may advance another unpaid check.
					if p.n < homeFrames {
						return p.decision(p.home, "recovery-pending"), true
					}
					// The change did not persist: keep the backoff the detection
					// would have skipped, rather than restart it from scratch.
					s.flags &^= flagDue
					e := p.edge(dirDown)
					p.gap[e] = uint16(min(p.changeGap(), maxLossGap))
					p.wait[e] = max(p.gap[e], initialGap)
					return p.decision(p.home, "recovery-unconfirmed"), true
				}
				// Charge the budget one frame before the check's losses
				// arrive, so connections sharing it mostly wait for them. It
				// is not exclusive: another connection can read the budget
				// first. Winning checks keep the charge.
				d := p.startRecovery(write)
				d.RecoveryLoss = observedLatency(m)
				return d, true
			}
			// A blocked check must not freeze the baseline or consume samples.
			if p.trial < p.home {
				p.trial, p.leg, p.tn = p.home, 0, 0
			}
			p.wait[p.edge(dirDown)] = initialGap
		}
	}
	return CompressionDecision{}, false
}

// compareSample advances the background comparison with a virtual frame of
// the candidate, predicted from this frame and the sample.
func (p *CompressionPolicy) compareSample(current CompressionMode, m CompressionMetrics, sample encoding.CodecSamples) string {
	s := &p.shadow
	if current == CompressionModeNone && p.trial == CompressionModeLz4 && p.tn == 0 && sample.Has(encoding.EncodingZstd) {
		lz4 := predictedFrame(current, CompressionModeLz4, m, sample, s.floor)
		zstd := predictedFrame(current, CompressionModeZstdLevel1, m, sample, s.floor)
		if float64(zstd.PrepareLatency+zstd.WriteLatency)*upMargin < float64(lz4.PrepareLatency+lz4.WriteLatency) {
			p.trial, p.leg = CompressionModeZstdLevel1, 1
		}
	}
	v := predictedFrame(current, p.trial, m, sample, s.floor)
	vl := float32(v.LogicalSize)
	return p.trialFrame(float32((v.PrepareLatency+v.WriteLatency).Seconds())/vl,
		float32(v.WriteLatency.Seconds())/vl, vl/float32(v.WireSize), vl, float32(v.WireSize)).Reason
}

// adopted starts validating the codec a background comparison selected.
func (p *CompressionPolicy) adopted(current CompressionMode) {
	s := &p.shadow
	s.phase, s.flags = phaseValidate, 0
	s.previous, s.priorCost, s.priorWrite = current, s.cost, s.write
	s.cost, s.write, s.low, s.validate = 0, 0, 0, validateFrames
	s.predicted = p.mean // win set home's mean to the samples' estimate
}

// predictedFrame retains measured read/hash work, substitutes the candidate's
// sampled encode cost, and scales writes by its ratio. A receiver-disk floor
// learned from a failed switch is charged per logical byte, without compression.
func predictedFrame(current, candidate CompressionMode, m CompressionMetrics, sample encoding.CodecSamples, floor float32) CompressionMetrics {
	scale := float64(m.LogicalSize) / float64(sample.LogicalSize)
	prepare := max(m.ReadLatency, m.PrepareLatency-time.Duration(float64(sample.Cost(FrameCompTokenForMode(current)))*scale))
	prepare += time.Duration(float64(sample.Cost(FrameCompTokenForMode(candidate))) * scale)
	currentRatio := float64(m.LogicalSize) / float64(max(1, m.WireSize))
	ratio := float64(sample.Ratio(FrameCompTokenForMode(candidate)))
	write := time.Duration(float64(m.WriteLatency) * currentRatio / ratio)
	write = max(write, time.Duration(float64(floor)*float64(m.LogicalSize)*float64(time.Second)))
	return CompressionMetrics{
		LogicalSize: m.LogicalSize, WireSize: max(1, int64(float64(m.LogicalSize)/ratio)),
		PrepareLatency: prepare, ReadLatency: m.ReadLatency, WriteLatency: write,
	}
}

// LinkPrior is what a connection's earlier files measured about sending
// uncompressed: home's cost and write time per logical byte, their spread,
// and any receiver-disk floor. It describes the link and the sender's reads,
// not a file's data, so a new file on the connection can start from it
// instead of seeding its own baseline. Compression ratios and codec samples
// stay with each file.
type LinkPrior struct {
	mean, sampleVar, fast, ref, meanW, fastW, refW, floor float32
}

// Link returns the link estimate p holds: its home is none and its baseline
// is seeded.
func (p *CompressionPolicy) Link() (LinkPrior, bool) {
	if p.home != CompressionModeNone || p.n < seedFrames || p.mean <= 0 {
		return LinkPrior{}, false
	}
	return LinkPrior{p.mean, p.sampleVar, p.fast, p.ref, p.meanW, p.fastW, p.refW, p.shadow.floor}, true
}

// SeedLink starts a fresh policy from a connection's link estimate: home's
// baseline counts as seeded and the first comparison, upward from none,
// starts at once, so the file's own samples can decide from its first frame.
// A wrong estimate costs at most a switch that validation on the file's own
// frames undoes. SeedLink leaves a policy that has seen a frame, or an empty
// estimate, alone.
func (p *CompressionPolicy) SeedLink(l LinkPrior) {
	if l.mean <= 0 || l.fast <= 0 || l.ref <= 0 || *p != (CompressionPolicy{}) {
		return
	}
	p.mean, p.sampleVar, p.fast, p.ref, p.fastW, p.refW = l.mean, l.sampleVar, l.fast, l.ref, l.fastW, l.refW
	p.meanW = l.meanW
	p.n = seedFrames
	p.shadow.floor = l.floor
	p.trial = step(p.home, dirUp)
}
