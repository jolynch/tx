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
	previous                                  CompressionMode
	validate, low, dataChanged                uint8
	// Frames until sampling reopens, and the gap used for its backoff.
	futileLeft, futileGap uint8
	enabled               bool
}

// Back off only on clear losses. Frame-cost shifts reopen sampling early;
// otherwise the bounded gap lets later data overturn a futile verdict.
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

func (p *CompressionPolicy) winFrames() uint8 {
	if p.shadow.enabled {
		return BackgroundWinFrames
	}
	return minWinFrames
}

// BackgroundPending reports whether a comparison needs another independent
// sample. No actual frame uses the trial engine's candidate mode.
func (p *CompressionPolicy) BackgroundPending() bool { return p.trial != p.home }

// SampleNeeds names the codecs the next background sample must measure. A
// comparison needs both encode costs. A settled policy reads only ratios
// its own frames cannot show: none needs lz4's, as its sniff, and zstd's, to
// see changed data; lz4 needs zstd's; zstd needs none, since its frames show
// its ratio and SEND then takes no sample.
func (p *CompressionPolicy) SampleNeeds() encoding.SampleSet {
	switch {
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

// DecideSampled observes a normally sent frame and, when sample is non-nil,
// advances a background comparison. Only a winning comparison changes the
// wire codec. Missing or busy samples never select a trial frame or wait.
// The candidate's CPU and ratio come from sample; transport cost is estimated
// from the current frame. Normal frames validate a switch and restore the
// previous codec when the estimate overpromised.
func (p *CompressionPolicy) DecideSampled(current CompressionMode, m CompressionMetrics, sample *encoding.CodecSamples) CompressionDecision {
	if m.LogicalSize <= 0 || m.PrepareLatency < 0 || m.WriteLatency < 0 || m.PrepareLatency+m.WriteLatency <= 0 {
		return p.decision(current, "hold")
	}
	if current > CompressionModeZstdLevel1 {
		*p = CompressionPolicy{}
		current = CompressionModeNone
	}
	if current != p.home {
		p.rehome(current)
		p.shadow = shadowState{}
	}
	if sample != nil && p.SampleNeeds() == 0 {
		sample = nil // The policy reads nothing from a sample now.
	}
	s := &p.shadow
	s.enabled = true
	logical := float32(m.LogicalSize)
	cost := float32((m.PrepareLatency + m.WriteLatency).Seconds()) / logical
	write := float32(m.WriteLatency.Seconds()) / logical
	if s.cost == 0 {
		s.cost, s.write = cost, write
	} else {
		s.cost += (min(cost, 2*s.cost) - s.cost) / 4
		s.write += (write - s.write) / 4
	}
	if s.floor > write*2 {
		s.floor = 0 // The path became faster than the learned disk limit.
	}
	if s.futileLeft > 0 {
		s.futileLeft--
	}
	if current != CompressionModeNone && m.WireSize > 0 && logical/float32(m.WireSize) < lowGain {
		s.low++
	} else {
		s.low = 0
	}
	if s.low >= lowGainFrames {
		s.cost, s.write, s.validate, s.low = 0, 0, 0, 0
		return p.dropToNone(logical / float32(m.WireSize))
	}
	if s.validate > 0 {
		s.validate--
		if s.validate == 0 && s.cost > s.priorCost*1.10 {
			previous := s.previous
			pathMoved := s.write > 1.25*s.priorWrite
			if previous < current && !movedBy(s.write, s.priorWrite, 1.25) {
				// Wire savings did not reduce the real write cost: the
				// receiver's disk, rather than the network, can be binding.
				s.floor = min(s.write, s.priorWrite)
			}
			p.rehome(previous)
			if previous < current {
				gap := uint16(maxGap)
				if pathMoved {
					gap = initialGap // Reconsider promptly under the new path.
				}
				p.gap[previous], p.wait[previous] = gap, gap
			}
			s.cost, s.write = s.priorCost, s.priorWrite
			return p.decision(previous, "sample-overestimated")
		}
	}
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
				p.gap, p.wait = [2]uint16{}, [2]uint16{}
				p.pending = p.neighbors()
				s.zstdRatio, s.dataChanged = ratio, 0
			}
		} else {
			s.zstdRatio += (ratio - s.zstdRatio) / 4
			s.dataChanged = 0
		}
	}
	reason := "sample-pending"
	if p.trial == p.home {
		// Virtual candidates have their own ratios. Do not let a weak
		// candidate count toward the real home's incompressible streak.
		p.low = 0
		if s.low > 0 {
			p.low = s.low - 1
		}
		reason = p.Decide(current, m).Reason
	} else {
		// Candidate work must not freeze the real baseline while a worker
		// is busy or the path changes. Only scheduling is suspended.
		p.low = s.low
		ratio := logical / float32(max(1, m.WireSize))
		if current == CompressionModeNone {
			ratio = m.SniffRatio
		}
		reason = p.homeFrame(cost, write, ratio, false).Reason
	}
	if p.trial != p.home && sample != nil && sampleCovers(*sample, current, p.trial) {
		virtual := predictedFrame(current, p.trial, m, *sample, s.floor)
		reason = p.Decide(p.trial, virtual).Reason
	}
	if p.home != current {
		s.previous, s.priorCost, s.priorWrite = current, s.cost, s.write
		s.cost, s.write, s.low, s.validate = 0, 0, 0, 8
		reason = "sample-won"
	}
	return p.decision(p.home, reason)
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
	mean, sampleVar, fast, ref, fastW, refW, floor float32
}

// Link returns the link estimate p holds: its home is none and its baseline
// is seeded.
func (p *CompressionPolicy) Link() (LinkPrior, bool) {
	if p.home != CompressionModeNone || p.n < seedFrames || p.mean <= 0 {
		return LinkPrior{}, false
	}
	return LinkPrior{p.mean, p.sampleVar, p.fast, p.ref, p.fastW, p.refW, p.shadow.floor}, true
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
	p.n = seedFrames
	p.shadow.floor = l.floor
	p.shadow.enabled = true
	p.trial = step(p.home, dirUp)
}
