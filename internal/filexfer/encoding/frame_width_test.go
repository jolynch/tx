package encoding

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompressesSlowly(t *testing.T) {
	const frame = 4 << 20
	// A 4 MiB frame is slow at or above 2 ms on one goroutine.
	threshold := time.Duration(float64(frame) / WideCompressMaxBytesPerCoreSec * float64(time.Second))
	for _, tc := range []struct {
		name    string
		logical int64
		wall    time.Duration
		want    bool
	}{
		{"no time measured", frame, 0, false},
		{"empty frame", 0, time.Millisecond, false},
		{"random data through lz4, about 20 GiB/s", frame, 200 * time.Microsecond, false},
		{"just under the threshold", frame, threshold - time.Microsecond, false},
		{"just over the threshold", frame, threshold + time.Microsecond, true},
		{"text through lz4, about 500 MiB/s", frame, 8 * time.Millisecond, true},
	} {
		if got := compressesSlowly(tc.logical, tc.wall); got != tc.want {
			t.Errorf("%s: compressesSlowly(%d, %v) = %v, want %v", tc.name, tc.logical, tc.wall, got, tc.want)
		}
	}
}

// sample describes a frame that compressed to half its size.
func sample(codec string, logical int64, concurrency int, wall time.Duration) FrameSample {
	return FrameSample{Codec: codec, Logical: logical, Wire: logical / 2, Concurrency: concurrency, Wall: wall}
}

// TestFrameWidth walks a window through the cases its policy distinguishes.
func TestFrameWidth(t *testing.T) {
	const frame = 4 << 20
	slow, fast := 8*time.Millisecond, 200*time.Microsecond
	w := FrameWidth{hints: &frameHints{}}
	if w.Wide(EncodingLz4) {
		t.Fatal("a window's first frame went wide")
	}
	w.Observe(sample(EncodingLz4, frame, 1, slow))
	if !w.Wide(EncodingLz4) {
		t.Fatal("the frame after a slow lz4 frame stayed narrow")
	}
	if w.Wide(EncodingZstd) {
		t.Fatal("a zstd frame went wide on lz4's evidence")
	}
	w.Observe(sample(EncodingLz4, frame, 4, slow/4)) // still slow once spread
	if !w.Wide(EncodingLz4) {
		t.Fatal("a wide frame that stayed slow narrowed the next")
	}
	w.Observe(sample(EncodingLz4, frame, 4, fast/4)) // the data turned cheap
	if w.Wide(EncodingLz4) {
		t.Fatal("the frame after a fast frame went wide")
	}
	stored := sample(EncodingZstd, frame, 4, slow)
	stored.Wire = frame + 1
	w.Observe(stored) // incompressible, slowed by a loaded host
	if w.Wide(EncodingZstd) {
		t.Fatal("the frame after a slow frame that did not shrink went wide")
	}
	w.Observe(sample(EncodingLz4, frame, 1, slow))
	w.Observe(sample("none", frame, 0, 0))
	if w.Wide(EncodingLz4) {
		t.Fatal("the frame after an uncompressed frame went wide")
	}
	gentle := FrameWidth{Gentle: true, hints: w.hints}
	gentle.Observe(sample(EncodingLz4, frame, 1, slow))
	if gentle.Wide(EncodingLz4) {
		t.Fatal("a gentle window went wide")
	}
}

// A window's first frame follows the process's last frame of its codec, so
// a file that is a single window can compress wide, and fast data does not.
func TestFrameWidthFirstFrameFollowsHints(t *testing.T) {
	const frame = 4 << 20
	slow, fast := 8*time.Millisecond, 200*time.Microsecond
	hints := &frameHints{}
	first := FrameWidth{hints: hints}
	first.Observe(sample(EncodingLz4, frame, 1, slow))
	if w := (FrameWidth{hints: hints}); !w.Wide(EncodingLz4) || w.Wide(EncodingZstd) {
		t.Fatal("after a slow lz4 frame, a new window's first frame should go wide for lz4 only")
	}
	first.Observe(sample("none", frame, 0, 0)) // an uncompressed frame leaves the hint
	if w := (FrameWidth{hints: hints}); !w.Wide(EncodingLz4) {
		t.Fatal("an uncompressed frame cleared the lz4 hint")
	}
	other := FrameWidth{hints: hints}
	other.Observe(sample(EncodingLz4, frame, 4, fast/4))
	if w := (FrameWidth{hints: hints}); w.Wide(EncodingLz4) {
		t.Fatal("after a fast lz4 frame, a new window's first frame went wide")
	}
	if w := (FrameWidth{Gentle: true, hints: hints}); w.Wide(EncodingLz4) {
		t.Fatal("a gentle window's first frame went wide")
	}
}

// A window of data that compresses faster than the threshold on one
// goroutine must narrow again after one frame misjudged as slow, such as the
// first frame on a cold encoder pool, or one preempted. Wide encoders scale
// sublinearly, so a wide frame's wall time times its goroutines overstates
// the CPU a narrow frame of the same data would take, and fast data then
// stays wide for the rest of the window.
//
// The cases use measured scaling on text (lz4 2.9x at 4, zstd 1.8x at 2 and
// 3.4x at 4) and, for cheap data that compresses, the wall times measured
// for random data on 4 goroutines: 0.27 ms through lz4 and 0.45 ms through
// zstd, against 0.17 ms and 0.43 ms on one. A loaded host may double the
// wide times. Random data itself does not shrink, so Observe counts it as
// fast whatever its wall time.
func TestFrameWidthNarrowsWhenWideFramesOfFastData(t *testing.T) {
	const frame = 4 << 20
	narrow := func(bytesPerSec float64) time.Duration {
		return time.Duration(frame / bytesPerSec * float64(time.Second))
	}
	const giB = 1 << 30
	for _, tc := range []struct {
		name        string
		codec       string
		concurrency int
		narrowWall  time.Duration
		wideWall    time.Duration
	}{
		{"lz4 at 2.5 GiB/s, 2.9x on 4", EncodingLz4, 4, narrow(2.5 * giB), narrow(2.5 * giB * 2.9)},
		{"zstd at 2.5 GiB/s, 1.8x on 2", EncodingZstd, 2, narrow(2.5 * giB), narrow(2.5 * giB * 1.8)},
		{"zstd at 2.5 GiB/s, 3.4x on 4", EncodingZstd, 4, narrow(2.5 * giB), narrow(2.5 * giB * 3.4)},
		{"cheap data through zstd on 2", EncodingZstd, 2, 430 * time.Microsecond, 470 * time.Microsecond},
		{"cheap data through zstd on 4", EncodingZstd, 4, 430 * time.Microsecond, 450 * time.Microsecond},
		{"cheap data through zstd on 4, loaded", EncodingZstd, 4, 430 * time.Microsecond, 900 * time.Microsecond},
		{"cheap data through lz4 on 4, loaded", EncodingLz4, 4, 170 * time.Microsecond, 540 * time.Microsecond},
	} {
		if compressesSlowly(frame, tc.narrowWall) {
			t.Fatalf("%s: the case's narrow frame is slow; it must model fast data", tc.name)
		}
		w := FrameWidth{hints: &frameHints{}}
		w.Observe(sample(tc.codec, frame, 1, 3*time.Millisecond)) // misjudged as slow
		if !w.Wide(tc.codec) {
			t.Fatalf("%s: the frame after a slow one stayed narrow", tc.name)
		}
		w.Observe(sample(tc.codec, frame, tc.concurrency, tc.wideWall))
		if w.Wide(tc.codec) {
			t.Errorf("%s: a %v frame on %d goroutines, whose data takes %v on one, kept the window wide", tc.name, tc.wideWall, tc.concurrency, tc.narrowWall)
		}
	}
}

// FuzzFrameWidth drives windows that share hints through random frames and
// checks Wide against a model. Within a window, a frame may go wide only
// after a frame of the same codec that shrank and whose estimated
// one-goroutine time (wall time times the encoder's speedup) put it below
// the threshold. A window's
// first frame follows the last compressed frame of its codec in any window.
// A gentle window never goes wide.
func FuzzFrameWidth(f *testing.F) {
	f.Add(false, []byte{0, 1, 200, 0, 1, 10})
	f.Add(true, []byte{0, 1, 255, 0, 1, 255})
	f.Add(false, []byte{1, 2, 128, 2, 0, 0, 1, 4, 30})
	// Slow zstd, then a slow wide zstd frame that did not shrink.
	f.Add(false, []byte{1, 0, 200, 1, 0x82, 200, 1, 2, 200})
	// Slow lz4, then zstd (narrow), slow lz4, wide and still slow, wide and
	// fast, then narrow.
	f.Add(false, []byte{0, 0, 200, 1, 0, 0, 0, 0, 200, 0, 2, 31, 0, 2, 1, 0, 0, 0})
	// Slow lz4, a new window, its first lz4 frame, then fast lz4 and a new
	// window again.
	f.Add(false, []byte{0, 0, 200, 3, 0, 0, 0, 0, 200, 0, 2, 1, 3, 0, 0, 0, 0, 10})
	f.Fuzz(func(t *testing.T, gentle bool, ops []byte) {
		codecs := []string{EncodingLz4, EncodingZstd, "none"}
		hints := &frameHints{}
		w := FrameWidth{Gentle: gentle, hints: hints}
		hint := map[string]bool{}
		var lastCodec string
		var lastSlow bool
		for len(ops) >= 3 {
			if ops[0]%4 == 3 { // a new window
				w = FrameWidth{Gentle: gentle, hints: hints}
				lastCodec, lastSlow = "", false
				ops = ops[3:]
				continue
			}
			codec := codecs[int(ops[0])%len(codecs)]
			concurrency := []int{1, 2, 4}[int(ops[1]&0x7f)%3]
			stored := ops[1]&0x80 != 0
			if codec == "none" {
				concurrency = 0
			}
			// Wall times from 0 to about 16 ms per 4 MiB frame straddle the
			// 2 ms threshold at every concurrency.
			wall := time.Duration(ops[2]) * 64 * time.Microsecond
			ops = ops[3:]

			want := !gentle && lastSlow && lastCodec == codec
			if lastCodec == "" {
				want = !gentle && hint[codec]
			}
			if got := w.Wide(codec); got != want {
				t.Fatalf("Wide(%s) = %v after a %q frame (slow %v, hint %v), gentle %v", codec, got, lastCodec, lastSlow, hint[codec], gentle)
			}
			s := sample(codec, 4<<20, concurrency, wall)
			if stored {
				s.Wire = s.Logical + int64(wall/time.Microsecond)%64
			}
			w.Observe(s)
			narrow := wall.Seconds() * frameSpeedup(codec, concurrency)
			lastCodec, lastSlow = codec, concurrency > 0 && !stored && narrow > 0 && float64(4<<20) < WideCompressMaxBytesPerCoreSec*narrow
			if concurrency > 0 {
				hint[codec] = lastSlow
			}
		}
	})
}

// Concurrent frames never hold more extra goroutines than the budget leaves
// beyond their own: each grant above 1 fits within the limit when it is made,
// so the extras outstanding at any moment stay below the limit.
func TestFrameCompressionBudgetConcurrentExtrasStayWithinLimit(t *testing.T) {
	const limit = 6
	b := coreBudget{limit: func() int64 { return limit }}
	var extras, maxExtras atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for i := range 2000 {
				c := b.reserve(4 >> (i % 3))
				now := extras.Add(int64(c - 1))
				for {
					m := maxExtras.Load()
					if now <= m || maxExtras.CompareAndSwap(m, now) {
						break
					}
				}
				extras.Add(-int64(c - 1))
				b.release(c)
			}
		})
	}
	wg.Wait()
	if got := maxExtras.Load(); got > limit-1 {
		t.Fatalf("%d extra goroutines outstanding at once, above the %d a %d-core budget allows", got, limit-1, limit)
	}
	if got := b.inUse.Load(); got != 0 {
		t.Fatalf("%d goroutines still reserved after every release", got)
	}
}

func TestFrameConcurrencyCap(t *testing.T) {
	for _, tc := range []struct{ n, codecMax, callerMax, want int }{
		{0, 4, 0, 1},
		{1, 4, 0, 1},
		{1 << 20, 4, 0, 1},
		{1<<20 + 1, 4, 0, 2},
		{4 << 20, 4, 0, 4},
		{16 << 20, 4, 0, 4},
		{4 << 20, 2, 0, 2},
		{4 << 20, 4, 1, 1},
		{4 << 20, 4, 3, 3},
		{3 << 20, 4, 0, 3},
	} {
		if got := frameConcurrencyCap(tc.n, tc.codecMax, tc.callerMax); got != tc.want {
			t.Errorf("frameConcurrencyCap(%d, %d, %d) = %d, want %d", tc.n, tc.codecMax, tc.callerMax, got, tc.want)
		}
	}
}

// FuzzFrameCompressionBudget drives the budget through a random sequence of
// reservations and releases against a model. Each grant is 1, 2, or 4 and
// within its cap; a grant above 1 keeps the total within the limit and is
// the largest that fits; and releasing everything returns the budget to 0.
func FuzzFrameCompressionBudget(f *testing.F) {
	f.Add(uint8(8), []byte{4, 4, 4, 0x80, 2})
	f.Add(uint8(1), []byte{4, 2, 1})
	f.Add(uint8(3), []byte{4, 0x80, 4, 4})
	f.Add(uint8(8), []byte{1})
	f.Fuzz(func(t *testing.T, limitRaw uint8, ops []byte) {
		limit := int64(limitRaw%64) + 1
		b := coreBudget{limit: func() int64 { return limit }}
		var held []int
		var inUse int64
		for _, op := range ops {
			if op&0x80 != 0 && len(held) > 0 {
				i := int(op&0x7f) % len(held)
				b.release(held[i])
				inUse -= int64(held[i])
				held = append(held[:i], held[i+1:]...)
				continue
			}
			maxC := int(op%5) + 1
			got := b.reserve(maxC)
			want := 1
			for _, c := range []int{4, 2} {
				if c <= maxC && inUse+int64(c) <= limit {
					want = c
					break
				}
			}
			if got != want {
				t.Fatalf("reserve(%d) with %d of %d in use = %d, want %d", maxC, inUse, limit, got, want)
			}
			inUse += int64(got)
			held = append(held, got)
			if b.inUse.Load() != inUse {
				t.Fatalf("budget holds %d, model %d", b.inUse.Load(), inUse)
			}
		}
		for _, c := range held {
			b.release(c)
		}
		if got := b.inUse.Load(); got != 0 {
			t.Fatalf("%d goroutines still reserved after every release", got)
		}
	})
}
