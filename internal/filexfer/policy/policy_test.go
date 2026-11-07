package policy

import (
	"math"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// The tests below check how this policy meets the contract, through its
// internals: they change when its design does.

// A policy with state, handed a mode adapt does not use, starts over and
// takes the frame as a fresh policy's first frame at none.
func TestCompressionPolicyInvalidModeResetsState(t *testing.T) {
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 2.4, 3)
	for _, invalid := range []CompressionMode{CompressionModeZstdDefault, 200} {
		s := newSim(5, 0.3)
		p := NewCompressionPolicy()
		mode := CompressionModeNone
		for range 50 {
			mode = s.step(t, p, r, mode)
		}
		m := s.frame(r, CompressionModeNone)
		fresh := NewCompressionPolicy()
		got, want := p.DecideSampled(invalid, m, nil), fresh.DecideSampled(CompressionModeNone, m, nil)
		if got != want || *p != *fresh {
			t.Fatalf("mode %s: decided %+v with state %+v; a fresh policy at none decided %+v with %+v",
				modeName(invalid), got, *p, want, *fresh)
		}
	}
}

// On incompressible data an upward probe ends on its first frame's ratio,
// whatever the timing.
func TestCompressionPolicyIncompressibleProbesLastOneFrame(t *testing.T) {
	r := regime(simNetwork, 128, 4<<10, 4<<10, 512, 1, 1)
	for seed := range uint64(5) {
		modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{r, 2048})
		for i := 1; i < len(modes); i++ {
			if modes[i] != CompressionModeNone && modes[i-1] != CompressionModeNone {
				t.Fatalf("seed %d: frame %d is the second compressed frame in a row", seed, i)
			}
		}
	}
}

// A regime change that home sees starts probes at once, however long the
// next scheduled probe is away.
func TestCompressionPolicyRegimeChangeResetsGap(t *testing.T) {
	network := regime(simNetwork, 256, 16<<10, 1<<10, 512, 18.4, 30)
	disk := regime(simDisk, 768, 16<<10, 1<<10, 512, 18.4, 30)
	for seed := range uint64(10) {
		s := newSim(seed, 0.3)
		p := NewCompressionPolicy()
		mode := CompressionModeNone
		// nextProbe is how many home frames until home's next scheduled
		// probe.
		nextProbe := func() int {
			next := math.MaxInt
			for _, dir := range [2]uint8{dirUp, dirDown} {
				if p.neighbors()&dir != 0 && p.pending&dir == 0 {
					next = min(next, int(p.wait[p.edge(dir)]))
				}
			}
			return next
		}
		for i := 0; i < 1024 || p.trial != p.home || nextProbe() < 2*simReact; i++ {
			mode = s.step(t, p, network, mode)
			if i > 4096 {
				t.Fatalf("seed %d: no stretch without a scheduled probe", seed)
			}
		}
		if mode == CompressionModeNone {
			t.Fatalf("seed %d: settled in none, want a codec\n  %v", seed, network)
		}
		from, sawRegime := mode, false
		for i := range simReact {
			d := s.decide(p, disk, mode)
			sawRegime = sawRegime || d.Reason == "regime"
			mode = d.Next
			if mode != from {
				break
			}
			if i == simReact-1 {
				t.Fatalf("seed %d: stayed in %s %d frames after the receiver's disk became the bottleneck", seed, modeName(from), simReact)
			}
		}
		if !sawRegime {
			t.Fatalf("seed %d: left %s without seeing the regime change", seed, modeName(from))
		}
	}
}

// Frames with no time or no bytes are not samples and leave the policy as it
// was.
func TestCompressionPolicyIgnoresEmptyFrames(t *testing.T) {
	p := NewCompressionPolicy()
	for range 20 {
		d := p.DecideSampled(CompressionModeLz4, CompressionMetrics{LogicalSize: 1000, WireSize: 900}, nil)
		if d.Next != CompressionModeLz4 {
			t.Fatalf("zero-latency frame switched to %s", modeName(d.Next))
		}
		p.DecideSampled(CompressionModeLz4, CompressionMetrics{PrepareLatency: time.Millisecond, WriteLatency: time.Millisecond}, nil)
	}
	if *p != (CompressionPolicy{}) {
		t.Fatal("empty frames changed the policy")
	}
}

// Tests of the policy's individual mechanisms. Each pins one mechanism to the
// failure it prevents, so deleting the mechanism fails here as well as in the
// contract property that first needed it.

// A probe into a mode far slower than home spaces the next probe of that
// pair by what it cost (lossGap), so it stays rare: here none is about 30
// times slower than lz4, and the policy runs at most a few frames in none
// after settling.
func TestCompressionPolicyCostlyProbesStayRare(t *testing.T) {
	r := regime(simNetwork, 32, 16<<10, 2<<10, 512, 150, 250)
	if r.throughput(1) < 20*r.throughput(0) {
		t.Fatalf("setup: want none far slower than lz4: %v", r)
	}
	for seed := range uint64(10) {
		modes := scenario(t, seed, 0.3, CompressionModeNone, simSegment{r, simSettle + 4096})
		if n := countIn(modes, simSettle, len(modes), CompressionModeNone); n > 6 {
			t.Fatalf("seed %d: %d settled frames in none, want at most 6\n  %v", seed, n, r)
		}
	}
}

// While in none, data that turns compressible is adopted promptly, though
// none's own cost does not move. Sampling may have paused for up to
// futileMaxGap frames because no codec could pay on the incompressible data;
// then the samples need a reaction's worth of frames.
func TestCompressionPolicySniffStartsProbe(t *testing.T) {
	random := regime(simNetwork, 128, 4<<10, 4<<10, 512, 1, 1)
	text := regime(simNetwork, 128, 4<<10, 4<<10, 512, 2.5, 3)
	const within = futileMaxGap + simReact
	for seed := range uint64(10) {
		s := newSim(seed, 0.3)
		p := NewCompressionPolicy()
		mode := CompressionModeNone
		for range 2048 {
			mode = s.step(t, p, random, mode)
		}
		for mode != CompressionModeNone || p.trial != p.home {
			mode = s.step(t, p, random, mode)
		}
		probed := false
		for i := 0; i < within && !probed; i++ {
			mode = s.step(t, p, text, mode)
			probed = mode != CompressionModeNone
		}
		if !probed {
			t.Fatalf("seed %d: no codec within %d frames of the data turning compressible", seed, within)
		}
	}
}

// One stalled frame in home, however long, is clipped: it neither marks a
// regime change nor starts a probe, nor leaves home blind to a real change
// that follows it.
func TestCompressionPolicyIgnoresOneStalledFrame(t *testing.T) {
	slow := regime(simNetwork, 32, 4<<10, 512, 256, 2.4, 3)
	cpu := regime(simCPU, 0, 4<<10, 512, 256, 2.4, 3)
	for _, stall := range []time.Duration{100 * time.Millisecond, time.Second, time.Minute} {
		for seed := range uint64(5) {
			s := newSim(seed, 0.1)
			p := NewCompressionPolicy()
			mode := CompressionModeNone
			for range 300 {
				mode = s.step(t, p, slow, mode)
			}
			for p.trial != p.home || p.wait[0] < 16 || p.wait[1] < 16 {
				mode = s.step(t, p, slow, mode)
			}
			home := p.home
			m := s.frame(slow, mode)
			m.WriteLatency += stall
			for i := range 8 {
				if d := p.DecideSampled(mode, s.sendMetrics(slow, m), nil); d.Next != home || d.Reason != "hold" {
					t.Fatalf("stall %v seed %d: frame %d after it decided %s (%s) in %s",
						stall, seed, i, modeName(d.Next), d.Reason, modeName(home))
				}
				m = s.frame(slow, mode)
			}
			left := false
			for i := 0; i < simReact && !left; i++ {
				mode = s.step(t, p, cpu, mode)
				left = mode != home
			}
			if !left {
				t.Fatalf("stall %v seed %d: stayed in %s %d frames after the link sped up", stall, seed, modeName(home), simReact)
			}
		}
	}
}

// TestFileCompressionFitsMapValue checks C7: the store keeps FileCompression
// in a map, and Go stores values over 128 bytes out of line, which allocates.
func TestFileCompressionFitsMapValue(t *testing.T) {
	size := unsafe.Sizeof(FileCompression{})
	if size > 128 {
		t.Fatalf("FileCompression is %d bytes, want at most 128", size)
	}
	t.Logf("FileCompression is %d bytes", size)
}

// Background decisions keep candidates off the wire. Lz4 gaining nothing
// must not prevent zstd from being measured and selected.
func TestBackgroundCompressionIndependentZstd(t *testing.T) {
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 3)
	p := NewCompressionPolicy()
	mode := CompressionModeNone
	samples := samplesForRegime(r)
	seen, switched := 0, false
	for frame := range 80 {
		metrics := newSim(uint64(frame), 0).frame(r, mode)
		var sample *encoding.CodecSamples
		if frame >= seedFrames && frame < 20 {
			sample = &samples
			seen++
		}
		before := mode
		mode = p.DecideSampled(mode, metrics, sample).Next
		if mode != before && mode != CompressionModeZstdLevel1 {
			t.Fatalf("frame %d: a losing lz4 probe reached the wire: %v", frame, mode)
		}
		if mode != CompressionModeNone && seen < BackgroundWinFrames {
			t.Fatalf("switched on only %d independent samples", seen)
		}
		switched = switched || mode == CompressionModeZstdLevel1
	}
	if !switched || mode != CompressionModeZstdLevel1 {
		t.Fatalf("zstd was not discovered despite lz4 ratio 1: mode %v", mode)
	}
}

// Busy workers provide no results. Even when a scheduled comparison is due,
// sending continues in the same mode instead of trying a codec on the wire.
func TestBackgroundCompressionWithoutSamplesNeverProbes(t *testing.T) {
	p := NewCompressionPolicy()
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 3)
	for range 256 {
		m := newSim(1, 0).frame(r, CompressionModeNone)
		if d := p.DecideSampled(CompressionModeNone, m, nil); d.Next != CompressionModeNone {
			t.Fatalf("no sample selected %v", d)
		}
	}
	if !p.BackgroundPending() {
		t.Fatal("setup: no comparison was scheduled")
	}
}

// Normal frames expose a prediction that mistook receiver disk pressure for
// a network limit. The policy must learn that floor and restore none.
func TestBackgroundCompressionValidatesReceiverDisk(t *testing.T) {
	p := NewCompressionPolicy()
	r := regime(simDisk, 128, 4<<10, 1<<10, 512, 2, 4)
	samples := samplesForRegime(r)
	mode := CompressionModeNone
	compressed := false
	for frame := range 256 {
		m := newSim(uint64(frame), 0).frame(r, mode)
		mode = p.DecideSampled(mode, m, &samples).Next
		compressed = compressed || mode != CompressionModeNone
	}
	if !compressed {
		t.Fatal("setup: the estimator never made the initially optimistic switch")
	}
	if mode != CompressionModeNone || p.shadow.floor == 0 {
		t.Fatalf("did not learn the receiver limit: mode %v, floor %g", mode, p.shadow.floor)
	}
}

// Zstd's gain can change while lz4's does not. Background zstd measurements
// reopen a comparison even after long failed lz4 exploration.
func TestBackgroundCompressionDetectsZstdOnlyChange(t *testing.T) {
	p := NewCompressionPolicy()
	before := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 1)
	after := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 4)
	mode := CompressionModeNone
	for i := range 1024 {
		sample := samplesForRegime(before)
		mode = p.DecideSampled(mode, newSim(uint64(i), 0).frame(before, mode), &sample).Next
	}
	if mode != CompressionModeNone {
		t.Fatalf("setup: incompressible data selected %v", mode)
	}
	for i := range 64 {
		sample := samplesForRegime(after)
		mode = p.DecideSampled(mode, newSim(uint64(i), 0).frame(after, mode), &sample).Next
	}
	if mode != CompressionModeZstdLevel1 {
		t.Fatalf("zstd-only change was missed: mode %v", mode)
	}
}

// The send path admits a sample only while the file has the frames a switch
// needs. Clear wins from every frame must switch within that count, or the
// gate refuses files that could still switch.
func TestBackgroundFramesNeededCoversASwitch(t *testing.T) {
	const every = 8
	p := NewCompressionPolicy()
	r := regime(simNetwork, 128, 4<<10, 1<<10, 512, 1, 4)
	samples := samplesForRegime(r)
	mode := CompressionModeNone
	start, need := -1, 0
	for frame := range 64 {
		if start < 0 && p.BackgroundPending() {
			start, need = frame, p.BackgroundFramesNeeded(every)
		}
		if mode != CompressionModeNone {
			if start < 0 || frame > start+need-1 {
				t.Fatalf("first frame in %v is %d; pending from frame %d, which needed %d frames", mode, frame, start, need)
			}
			t.Logf("pending from frame %d, needing %d frames; first %v frame is %d", start, need, mode, frame)
			return
		}
		mode = p.DecideSampled(mode, newSim(uint64(frame), 0).frame(r, mode), &samples).Next
	}
	t.Fatal("setup: clear wins never switched")
}

// A file seeded from its connection's link estimate compares codecs from its
// first frame, with only its own samples deciding: four clear wins choose
// the next codec. Seeding touches only a fresh policy, and
// only a seeded none home exports an estimate.
func TestSeedLinkSkipsTheBaseline(t *testing.T) {
	const every = 8
	r := regime(simNetwork, 128, 4<<10, 1<<10, 512, 1, 4)
	samples := samplesForRegime(r)
	var earlier CompressionPolicy
	if _, ok := earlier.Link(); ok {
		t.Fatal("a fresh policy exported a link estimate")
	}
	for frame := range seedFrames {
		earlier.DecideSampled(CompressionModeNone, newSim(uint64(frame), 0).frame(r, CompressionModeNone), nil)
	}
	link, ok := earlier.Link()
	if !ok {
		t.Fatal("a seeded none home exported no link estimate")
	}
	p := NewCompressionPolicy()
	p.SeedLink(link)
	if !p.BackgroundPending() {
		t.Fatal("a seeded policy did not start comparing")
	}
	if need := p.BackgroundFramesNeeded(every); need != BackgroundWinFrames+1 {
		t.Fatalf("a seeded policy needs %d frames, want %d", need, BackgroundWinFrames+1)
	}
	mode := CompressionModeNone
	for frame := range 64 {
		if mode != CompressionModeNone {
			if frame > BackgroundWinFrames {
				t.Fatalf("first frame in %v is %d, want by %d", mode, frame, BackgroundWinFrames)
			}
			break
		}
		if frame == 63 {
			t.Fatal("clear wins never switched a seeded file")
		}
		mode = p.DecideSampled(mode, newSim(uint64(100+frame), 0).frame(r, mode), &samples).Next
	}
	before := *p
	p.SeedLink(link)
	if *p != before {
		t.Fatal("SeedLink changed a policy that had seen frames")
	}
	if _, ok := p.Link(); ok {
		t.Fatal("a compressing home exported a link estimate")
	}
	empty := NewCompressionPolicy()
	if empty.SeedLink(LinkPrior{}); *empty != (CompressionPolicy{}) {
		t.Fatal("an empty estimate seeded a policy")
	}
}

func samplesForRegime(r simRegime) encoding.CodecSamples {
	logical := int64(encoding.CodecSampleBytes)
	scale := float64(logical) / simFrame
	measurement := func(mode int) encoding.CodecMeasurement {
		return encoding.CodecMeasurement{
			WireSize: int64(float64(logical) / r.ratio[mode]),
			Elapsed:  time.Duration((r.prepare[mode] - r.prepare[0]) * scale * float64(time.Second)),
		}
	}
	return encoding.CodecSamples{LogicalSize: logical, Lz4: measurement(1), Zstd: measurement(2)}
}

// A path change during validation must not become a permanent receiver-disk
// floor. If the old baseline rejects a switch, retry under the new path.
func TestBackgroundCompressionLinkClampsDuringValidation(t *testing.T) {
	p := NewCompressionPolicy()
	before := regime(simNetwork, 128, 4<<10, 1<<10, 512, 1, 4)
	after := regime(simNetwork, 16, 4<<10, 1<<10, 512, 1, 4)
	mode := CompressionModeNone
	for i := range 32 {
		s := samplesForRegime(before)
		mode = p.DecideSampled(mode, newSim(uint64(i), 0).frame(before, mode), &s).Next
		if mode == CompressionModeZstdLevel1 {
			break
		}
	}
	if mode != CompressionModeZstdLevel1 {
		t.Fatal("setup: zstd did not win")
	}
	for i := range 64 {
		s := samplesForRegime(after)
		mode = p.DecideSampled(mode, newSim(uint64(i), 0).frame(after, mode), &s).Next
	}
	if mode != CompressionModeZstdLevel1 || p.shadow.floor != 0 {
		t.Fatalf("clamp blocked a useful codec: mode %v floor %g", mode, p.shadow.floor)
	}
}

func TestBackgroundCompressionDoesNotAllocate(t *testing.T) {
	p := NewCompressionPolicy()
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 3)
	sample := samplesForRegime(r)
	mode := CompressionModeNone
	m := newSim(1, 0).frame(r, mode)
	if n := testing.AllocsPerRun(100, func() { mode = p.DecideSampled(mode, m, &sample).Next }); n != 0 {
		t.Fatalf("background decision allocates %v times", n)
	}
}

// A weak virtual lz4 comparison cannot shorten the actual zstd home's
// three-frame incompressible streak. Only real wire frames count there.
func TestBackgroundCandidateLowGainDoesNotChangeHomeStreak(t *testing.T) {
	p := NewCompressionPolicy()
	r := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 3)
	sample := samplesForRegime(r)
	mode := CompressionModeNone
	for i := range 40 {
		mode = p.DecideSampled(mode, newSim(uint64(i), 0).frame(r, mode), &sample).Next
	}
	if mode != CompressionModeZstdLevel1 {
		t.Fatal("setup: zstd was not established")
	}
	p.trial, p.leg, p.tn = CompressionModeLz4, 0, 0
	random := regime(simNetwork, 32, 4<<10, 1<<10, 512, 1, 1)
	for i := range lowGainFrames {
		d := p.DecideSampled(mode, newSim(uint64(i), 0).frame(random, mode), nil)
		if i+1 < lowGainFrames && d.Next != mode {
			t.Fatalf("only %d real frames dropped home: %v", i+1, d)
		}
		if i+1 == lowGainFrames && d.Next != CompressionModeNone {
			t.Fatalf("three real low-gain frames did not drop home: %v", d)
		}
	}
}

// maskSample keeps only the codecs set names, as a SEND sampling for that set
// would, or returns nil when set is empty and SEND samples nothing.
func maskSample(s encoding.CodecSamples, set encoding.SampleSet) *encoding.CodecSamples {
	if set == 0 {
		return nil
	}
	if set&encoding.SampleLz4 == 0 {
		s.Lz4 = encoding.CodecMeasurement{}
	}
	if set&encoding.SampleZstd == 0 {
		s.Zstd = encoding.CodecMeasurement{}
	}
	return &s
}

// A settled policy samples only the ratios its own frames cannot show; a
// comparison samples both codecs.
func TestSampleNeedsByState(t *testing.T) {
	for _, tc := range []struct {
		home, trial CompressionMode
		want        encoding.SampleSet
	}{
		{CompressionModeNone, CompressionModeNone, encoding.SampleBoth},
		{CompressionModeLz4, CompressionModeLz4, encoding.SampleZstd},
		{CompressionModeZstdLevel1, CompressionModeZstdLevel1, 0},
		{CompressionModeNone, CompressionModeLz4, encoding.SampleBoth},
		{CompressionModeLz4, CompressionModeZstdLevel1, encoding.SampleBoth},
		{CompressionModeLz4, CompressionModeNone, encoding.SampleBoth},
		{CompressionModeZstdLevel1, CompressionModeLz4, encoding.SampleBoth},
	} {
		p := NewCompressionPolicy()
		p.home, p.trial = tc.home, tc.trial
		if got := p.SampleNeeds(); got != tc.want {
			t.Errorf("home %v trial %v: need %b, want %b", tc.home, tc.trial, got, tc.want)
		}
	}
}

// A sample requested while settled can arrive after a comparison opened. It
// lacks a codec the comparison needs, so the comparison waits for the next
// sample instead of reading a missing measurement.
func TestComparisonWaitsForACoveringSample(t *testing.T) {
	r := regime(simNetwork, 128, 4<<10, 1<<10, 512, 2, 4)
	full := samplesForRegime(r)
	s := newSim(7, 0)
	p := NewCompressionPolicy()
	for range seedFrames + 1 {
		p.DecideSampled(CompressionModeLz4, s.frame(r, CompressionModeLz4), nil)
	}
	p.trial, p.tn = CompressionModeZstdLevel1, 0
	stale := maskSample(full, encoding.SampleZstd)
	if d := p.DecideSampled(CompressionModeLz4, s.frame(r, CompressionModeLz4), stale); d.Next != CompressionModeLz4 || p.tn != 0 {
		t.Fatalf("a zstd-only sample advanced an lz4 home's comparison: %+v tn=%d", d, p.tn)
	}
	if p.trial != CompressionModeZstdLevel1 {
		t.Fatalf("a stale sample ended the comparison: trial %v", p.trial)
	}
	if p.DecideSampled(CompressionModeLz4, s.frame(r, CompressionModeLz4), &full); p.tn == 0 && p.trial == CompressionModeZstdLevel1 {
		t.Fatal("a covering sample did not advance the comparison")
	}
}

// futileFixture is a 256 KiB sample of text whose codecs cost about 2.6 and
// 3.3 ns per byte, compressing 2:1 and 3:1, and a 4 MiB uncompressed frame
// whose write takes write.
func futileFixture(write time.Duration) (encoding.CodecSamples, CompressionMetrics) {
	const logical = encoding.CodecSampleBytes
	sample := encoding.CodecSamples{LogicalSize: logical,
		Lz4:  encoding.CodecMeasurement{WireSize: logical / 2, Elapsed: 680 * time.Microsecond},
		Zstd: encoding.CodecMeasurement{WireSize: logical / 3, Elapsed: 870 * time.Microsecond}}
	return sample, CompressionMetrics{LogicalSize: 4 << 20, PrepareLatency: time.Millisecond, WriteLatency: write}
}

// On a link that writes a frame faster than any codec could shrink it, one
// sample ends the comparison and sampling stops for a gap that doubles while
// samples stay futile. On a slow link the same sample keeps comparing.
func TestFutileSampleStopsSampling(t *testing.T) {
	sample, fast := futileFixture(time.Millisecond)
	for _, missing := range []string{encoding.EncodingLz4, encoding.EncodingZstd} {
		partial := sample
		if missing == encoding.EncodingLz4 {
			partial.Lz4 = encoding.CodecMeasurement{}
		} else {
			partial.Zstd = encoding.CodecMeasurement{}
		}
		if futile(partial, float32(fast.WriteLatency.Seconds())/float32(fast.LogicalSize)) {
			t.Fatalf("sample without %s ruled out both codecs", missing)
		}
	}
	p := NewCompressionPolicy()
	for range seedFrames {
		p.DecideSampled(CompressionModeNone, fast, nil)
	}
	if p.SampleNeeds() != encoding.SampleBoth {
		t.Fatalf("a settled uncompressed home needs %b", p.SampleNeeds())
	}
	for _, gap := range []int{futileFirstGap, 2 * futileFirstGap, 4 * futileFirstGap, futileMaxGap, futileMaxGap} {
		if d := p.DecideSampled(CompressionModeNone, fast, &sample); d.Next != CompressionModeNone {
			t.Fatalf("a futile sample switched to %v", d.Next)
		}
		if p.SampleNeeds() != 0 || p.BackgroundPending() {
			t.Fatalf("after a futile sample: needs %b, pending %v", p.SampleNeeds(), p.BackgroundPending())
		}
		for range gap - 1 {
			p.DecideSampled(CompressionModeNone, fast, &sample) // ignored while closed
		}
		if p.SampleNeeds() != 0 {
			t.Fatalf("sampling reopened before its %d-frame gap", gap)
		}
		p.DecideSampled(CompressionModeNone, fast, nil)
		if p.SampleNeeds() == 0 {
			t.Fatalf("sampling stayed closed past its %d-frame gap", gap)
		}
	}
	changed := sample
	changed.Lz4.Elapsed, changed.Zstd.Elapsed = time.Nanosecond, time.Nanosecond
	p.DecideSampled(CompressionModeNone, fast, &changed)
	if p.SampleNeeds() == 0 || p.shadow.futileGap != 0 {
		t.Fatal("a useful sample did not clear the backoff for changed data")
	}

	sample, slow := futileFixture(60 * time.Millisecond) // about 64 MiB/s
	q := NewCompressionPolicy()
	for range seedFrames {
		q.DecideSampled(CompressionModeNone, slow, nil)
	}
	q.DecideSampled(CompressionModeNone, slow, &sample)
	if q.SampleNeeds() == 0 {
		t.Fatal("a slow link stopped sampling")
	}
}

// A shift in home's frame cost, as when the link slows, reopens sampling
// before the gap ends: the verdict was made at the old cost.
func TestFutileVerdictReopensWhenThroughputShifts(t *testing.T) {
	sample, fast := futileFixture(time.Millisecond)
	p := NewCompressionPolicy()
	for range settleFrames {
		p.DecideSampled(CompressionModeNone, fast, nil)
	}
	p.DecideSampled(CompressionModeNone, fast, &sample)
	if p.SampleNeeds() != 0 {
		t.Fatal("a futile sample left sampling open")
	}
	_, slower := futileFixture(4 * time.Millisecond)
	for frame := range futileFirstGap - 1 {
		if p.DecideSampled(CompressionModeNone, slower, nil); p.SampleNeeds() != 0 {
			if frame < regimeFrames-1 {
				t.Fatalf("sampling reopened after %d slower frames, before the regime check", frame+1)
			}
			return
		}
	}
	t.Fatal("slower frames did not reopen sampling before the gap ended")
}

func recoveryFixture(mode CompressionMode) (CompressionPolicy, CompressionMetrics) {
	m := CompressionMetrics{LogicalSize: simFrame, WireSize: simFrame / 3,
		PrepareLatency: 5 * time.Millisecond, WriteLatency: 15 * time.Millisecond,
		RecoveryAllowed: true, RecoveryReady: true}
	cost := float32(.020 / simFrame)
	write := float32(.015 / simFrame)
	p := CompressionPolicy{home: mode, trial: mode, n: settleFrames,
		mean: cost, fast: cost, ref: cost, meanW: write, fastW: write, refW: write,
		pending: dirDown, ratio: 3, refRatio: 3,
		shadow: shadowState{cost: cost, write: write}}
	return p, m
}

// startCheck sends home frames like m until a check sends none: the frame
// that starts it, then its baseline frames.
func startCheck(t *testing.T, p *CompressionPolicy, m CompressionMetrics) CompressionDecision {
	t.Helper()
	d := p.DecideSampled(p.home, m, nil)
	for range recoveryBaselineFrames {
		if d.Reason != "recovery-baseline" {
			break
		}
		d = p.DecideSampled(p.home, m, nil)
	}
	return d
}

func TestRecoveryChecksMeasuredThroughput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		home  CompressionMode
		costs []time.Duration
		want  CompressionMode
	}{
		{"none wins", CompressionModeZstdLevel1, []time.Duration{12 * time.Millisecond, 12 * time.Millisecond}, CompressionModeNone},
		{"lz4 wins", CompressionModeZstdLevel1, []time.Duration{30 * time.Millisecond, 16 * time.Millisecond, 16 * time.Millisecond}, CompressionModeLz4},
		{"four frames lose", CompressionModeZstdLevel1, []time.Duration{12 * time.Millisecond, 25 * time.Millisecond, 12 * time.Millisecond, 25 * time.Millisecond}, CompressionModeZstdLevel1},
		{"lz4 restores", CompressionModeLz4, []time.Duration{30 * time.Millisecond}, CompressionModeLz4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, m := recoveryFixture(tc.home)
			d := startCheck(t, &p, m)
			if d.Next != CompressionModeNone || d.Reason != "recovery-check" {
				t.Fatalf("start: %+v", d)
			}
			for _, elapsed := range tc.costs {
				m.PrepareLatency, m.WriteLatency = 0, elapsed
				copy := p
				other := copy.DecideSampled(d.Next, m, nil)
				d = p.DecideSampled(d.Next, m, nil)
				if p != copy || d != other {
					t.Fatal("copying an active check changed its decision")
				}
				if want := max(0, elapsed-20*time.Millisecond); abs32(float32(d.RecoveryLoss-want)) > 100 {
					t.Fatalf("lost %v, want %v", d.RecoveryLoss, want)
				}
			}
			if d.Next != tc.want || p.home != tc.want || p.shadow.phase == phaseCheck {
				t.Fatalf("finished %+v; home=%v phase=%d", d, p.home, p.shadow.phase)
			}
		})
	}
}

// Ordinary fast frames can leave their bytes in a response buffer, so home's
// average cost can omit time its baseline frames, flushed like the check's,
// spend flushing. The check must add that time to home's average: here none
// is faster than that sum, though slower than the average alone.
func TestRecoveryComparesWithFlushedBaseline(t *testing.T) {
	p, m := recoveryFixture(CompressionModeZstdLevel1)
	if d := p.DecideSampled(p.home, m, nil); d.Reason != "recovery-baseline" {
		t.Fatalf("check did not start: %+v", d)
	}
	m.PrepareLatency, m.WriteLatency, m.FlushLatency = 5*time.Millisecond, 35*time.Millisecond, 20*time.Millisecond
	var d CompressionDecision
	for range recoveryBaselineFrames {
		d = p.DecideSampled(p.home, m, nil)
	}
	if d.Next != CompressionModeNone {
		t.Fatalf("baseline did not end in a none check: %+v", d)
	}
	m.PrepareLatency, m.WriteLatency, m.FlushLatency = 0, 30*time.Millisecond, 0
	for range recoveryCheckFrames {
		d = p.DecideSampled(d.Next, m, nil)
	}
	if d.Reason != "recovery-won" || p.home != CompressionModeNone {
		t.Fatalf("none lost to home's unflushed average: %+v", d)
	}
}

func TestRecoveryRequiresWorkAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		allowed, ready, forced, want bool
	}{
		{"short tail", false, true, false, false},
		{"unpaid loss", true, false, false, false},
		{"forced short tail", false, false, true, false},
		{"detected change", true, false, true, true},
		{"periodic", true, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, m := recoveryFixture(CompressionModeZstdLevel1)
			m.RecoveryAllowed, m.RecoveryReady = tc.allowed, tc.ready
			if tc.forced {
				p.shadow.flags = flagDue
			}
			d := p.DecideSampled(p.home, m, nil)
			if got := d.Reason == "recovery-baseline"; got != tc.want {
				t.Fatalf("decision %+v, want check=%v", d, tc.want)
			}
		})
	}
}

func TestRecoveryDoesNotInferFloorAcrossFasterPath(t *testing.T) {
	p, m := recoveryFixture(CompressionModeZstdLevel1)
	m.PrepareLatency, m.WriteLatency = 2*time.Millisecond, 8*time.Millisecond
	d := startCheck(t, &p, m)
	if d.Reason != "recovery-check" {
		t.Fatalf("check did not start: %+v", d)
	}
	// These frames beat the old average after the path becomes faster.
	// Their writes match that average, but not home's latest write cost.
	m.PrepareLatency, m.WriteLatency = 0, 12*time.Millisecond
	for range 2 {
		d = p.DecideSampled(d.Next, m, nil)
	}
	if d.Reason != "recovery-won" || p.shadow.floor != 0 {
		t.Fatalf("check inferred a floor from a changing path: %+v, floor=%g", d, p.shadow.floor)
	}
}

func TestRecoveryConfirmsChangeBeforeAdvancingDebt(t *testing.T) {
	for _, tc := range []struct {
		name           string
		prepare, write time.Duration
		want           string
	}{
		{"noise disappeared", 5 * time.Millisecond, 15 * time.Millisecond, "recovery-pending"},
		{"cost still moved", 20 * time.Millisecond, 8 * time.Millisecond, "recovery-baseline"},
		{"opposite shift", 2 * time.Millisecond, 8 * time.Millisecond, "recovery-baseline"},
		{"write still moved", 0, 18 * time.Millisecond, "recovery-baseline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, m := recoveryFixture(CompressionModeZstdLevel1)
			p.shadow.flags, p.n = flagDue, seedFrames-1
			p.tMin, p.tSum = p.shadow.cost, p.shadow.write
			m.RecoveryReady = false
			m.PrepareLatency, m.WriteLatency = tc.prepare, tc.write
			p.shadow.cost = float32((tc.prepare + tc.write).Seconds()) / simFrame
			p.shadow.write = float32(tc.write.Seconds()) / simFrame
			p.mean, p.fast, p.ref = p.shadow.cost, p.shadow.cost, p.shadow.cost
			p.meanW, p.fastW, p.refW = p.shadow.write, p.shadow.write, p.shadow.write
			copy := p
			d := p.DecideSampled(p.home, m, nil)
			if other := copy.DecideSampled(copy.home, m, nil); other != d || copy != p {
				t.Fatal("copying a pending check lost its change reference")
			}
			if d.Reason != tc.want {
				t.Fatalf("decision %+v, want %s", d, tc.want)
			}
			if tc.want == "recovery-pending" {
				for range homeFrames - seedFrames {
					d = p.DecideSampled(p.home, m, nil)
				}
				if d.Reason != "recovery-unconfirmed" {
					t.Fatalf("transient change did not expire: %+v", d)
				}
			}
		})
	}
}

func TestRecoveryValidatesAndRestores(t *testing.T) {
	p, m := recoveryFixture(CompressionModeZstdLevel1)
	d := startCheck(t, &p, m)
	m.PrepareLatency, m.WriteLatency = 0, 12*time.Millisecond
	for range 2 {
		d = p.DecideSampled(d.Next, m, nil)
	}
	if d.Next != CompressionModeNone || p.shadow.validate != 8 {
		t.Fatalf("check did not win: %+v", d)
	}
	m.WriteLatency = 30 * time.Millisecond
	// The mean since the switch, checks included, must stay within 10% of
	// the prior cost; validation ends as soon as it no longer can.
	limit := 1.10 * float64(p.shadow.priorCost) * float64(m.LogicalSize) * float64(time.Second) * (2 + 8)
	spent := 2 * float64(12*time.Millisecond)
	for i := range 8 {
		d = p.DecideSampled(d.Next, m, nil)
		spent += float64(30 * time.Millisecond)
		if d.RecoveryLoss < 9*time.Millisecond {
			t.Fatalf("validation frame %d did not charge its loss: %+v", i, d)
		}
		if d.Next != CompressionModeNone {
			if spent <= limit && i < 7 {
				t.Fatalf("validation frame %d restored before failing was certain", i)
			}
			break
		}
		if spent > limit {
			t.Fatalf("validation frame %d continued after failing was certain", i)
		}
	}
	if d.Next != CompressionModeZstdLevel1 || d.Reason != "recovery-restore" {
		t.Fatalf("failed validation: %+v", d)
	}
}

// A send buffer absorbs a check's frames and the first validation frame, so
// they look nearly free; the codec then runs twice as slow as the one it
// replaced. Validation judges every frame since the switch, so the absorbed
// frames cannot hide that, and it restores the faster codec.
func TestRecoveryValidationSeesThroughAbsorbedFrames(t *testing.T) {
	p, m := recoveryFixture(CompressionModeZstdLevel1)
	d := startCheck(t, &p, m)
	frame := func(raw time.Duration) {
		m.PrepareLatency, m.WriteLatency, m.FrameLatency = 0, raw, raw
		d = p.DecideSampled(d.Next, m, nil)
	}
	for range 3 {
		frame(2 * time.Millisecond)
	}
	if p.home != CompressionModeNone || !p.RecoveryActive() {
		t.Fatalf("absorbed check frames did not win: %+v", d)
	}
	for range 7 {
		frame(40 * time.Millisecond)
		if d.Next != CompressionModeNone {
			break
		}
	}
	if d.Next != CompressionModeZstdLevel1 || d.Reason != "recovery-restore" {
		t.Fatalf("a codec twice as slow passed validation: %+v", d)
	}
}

func TestRecoveryBudgetAmortizesLoss(t *testing.T) {
	var b RecoveryBudget
	if !b.Ready() {
		t.Fatal("fresh budget is blocked")
	}
	b.Observe(10*time.Millisecond, time.Second)
	if b.Ready() {
		t.Fatal("lost check has no debt")
	}
	b.Observe(0, 999*time.Millisecond)
	if b.Ready() {
		t.Fatal("loss repaid before 100x its duration")
	}
	// An expedited second episode adds debt rather than resetting it.
	b.Observe(10*time.Millisecond, time.Second)
	b.Observe(0, time.Second)
	if b.Ready() {
		t.Fatal("expedited check forgave debt")
	}
	b.Observe(0, time.Millisecond)
	if !b.Ready() {
		t.Fatal("ordinary progress did not repay loss")
	}
}

// Every connection of a server charges and repays one budget at once; no
// concurrent loss may be forgiven.
func TestRecoveryBudgetCountsConcurrentLosses(t *testing.T) {
	var b RecoveryBudget
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				b.Observe(time.Millisecond, 0)
				b.Observe(0, time.Microsecond)
			}
		})
	}
	wg.Wait()
	// 8000 losses of 0.1 s of debt each, less 8000 µs of progress.
	want := 8000*recoveryBudgetScale*time.Millisecond.Seconds() - 8000*time.Microsecond.Seconds()
	if got := math.Float64frombits(b.debt.Load()); math.Abs(got-want) > 1e-6*want {
		t.Fatalf("debt %.6f s, want %.6f s", got, want)
	}
	if got := testing.AllocsPerRun(100, func() { b.Observe(0, time.Millisecond) }); got != 0 {
		t.Fatalf("budget allocates %.1f times per frame", got)
	}
}

func TestRecoveryDoesNotAllocate(t *testing.T) {
	initial, m := recoveryFixture(CompressionModeZstdLevel1)
	if got := testing.AllocsPerRun(20, func() {
		p := initial
		d := p.DecideSampled(p.home, m, nil)
		for range 4 {
			d = p.DecideSampled(d.Next, m, nil)
		}
	}); got != 0 {
		t.Fatalf("recovery allocates %.1f times", got)
	}
}

func TestRecoveryUsesObservedFrameTime(t *testing.T) {
	p, m := recoveryFixture(CompressionModeZstdLevel1)
	d := p.DecideSampled(p.home, m, nil)
	// A stale delivery-rate floor predicts a slow write, but the two real
	// frames complete faster. Conversely, a slow observed frame must lose.
	m.PrepareLatency, m.WriteLatency = 0, time.Second
	m.FrameLatency = 12 * time.Millisecond
	for range 2 {
		d = p.DecideSampled(d.Next, m, nil)
	}
	if d.Next != CompressionModeNone || d.RecoveryLoss != 0 {
		t.Fatalf("used the estimate instead of the observation: %+v", d)
	}
}

func TestRecoveryDiskShiftWithoutNoise(t *testing.T) {
	in := []byte("\xe3000X0011012X0010")
	before, after := regimeFromBytes(in[2:]), regimeFromBytes(in[len(in)-simRegimeBytes:])
	s, b := newSim(1, 0), backgroundSim{}
	for range 224 {
		b.step(s, before, 1024)
	}
	if b.mode != CompressionModeZstdLevel1 {
		t.Fatal("setup did not establish zstd")
	}
	first := -1
	for n := range 128 {
		b.step(s, after, 1024)
		if b.p.home == CompressionModeNone && first < 0 {
			first = n
		}
		if first >= 0 && b.p.home != CompressionModeNone {
			t.Fatal("a prediction displaced the measured faster codec")
		}
	}
	if first < 0 || first > simReact {
		t.Fatalf("none adopted after %d frames, want within %d", first, simReact)
	}
}

func TestRecoverySteadyStateCostBudget(t *testing.T) {
	p, m := recoveryFixture(CompressionModeZstdLevel1)
	mode := p.home
	p.ratio, p.refRatio = 250, 250
	var budget RecoveryBudget
	var loss, elapsed time.Duration
	for range 20000 {
		m.PrepareLatency = 0
		switch mode {
		case CompressionModeNone:
			m.WireSize = simFrame
			m.WriteLatency = 200 * time.Millisecond
		case CompressionModeLz4:
			m.WireSize = simFrame / 30
			m.WriteLatency = 60 * time.Millisecond
		default:
			m.WireSize = simFrame / 250
			m.WriteLatency = 20 * time.Millisecond
		}
		m.RecoveryReady = budget.Ready()
		d := p.DecideSampled(mode, m, nil)
		if p.home != CompressionModeZstdLevel1 {
			t.Fatal("unsuccessful checks displaced useful compression")
		}
		budget.Observe(d.RecoveryLoss, m.WriteLatency)
		loss += d.RecoveryLoss
		elapsed += m.WriteLatency
		mode = d.Next
	}
	if loss == 0 || float64(loss)/float64(elapsed) > .0105 {
		t.Fatalf("steady recovery loss %v / %v = %.2f%%", loss, elapsed, 100*float64(loss)/float64(elapsed))
	}
}

func TestSustainedDriftNeedsEightObservations(t *testing.T) {
	p := CompressionPolicy{home: CompressionModeLz4, n: homeFrames,
		mean: 1.2, fast: 1.2, ref: 1, sampleVar: 1}
	for n := range driftFrames {
		moved, _ := p.regimeChanged(1.2)
		if moved != (n == driftFrames-1) {
			t.Fatalf("observation %d: moved=%v", n+1, moved)
		}
	}
	p = CompressionPolicy{home: CompressionModeNone, trial: CompressionModeLz4,
		tn: 3, tSum: 3, tSq: 3, tMin: 1, tMax: 1, tLogical: 6, tWire: 3, drift: driftFrames - 1}
	p.win(.5, true)
	if p.drift != 0 {
		t.Fatal("new codec inherited the previous codec's drift observations")
	}
}

func TestCompressionPolicyReferenceUsesHomeObservations(t *testing.T) {
	p := CompressionPolicy{home: CompressionModeLz4, n: 3,
		mean: 1, fast: 1, ref: 1, meanW: .2, fastW: .2, refW: .2, ratio: 2, refRatio: 2}
	for range homeFrames - 3 {
		p.homeFrame(1.1, .22, 2, false)
	}
	want := float32((3 + (homeFrames-3)*1.1) / homeFrames)
	if abs32(p.ref-want) > 1e-6 {
		t.Fatalf("reference %g, want %g from home's observations", p.ref, want)
	}
	p.drift = 1
	p.homeFrame(1.1, .22, 2, false)
	if abs32(p.ref-want) > 1e-6 {
		t.Fatal("reference followed a drift being confirmed")
	}
}
