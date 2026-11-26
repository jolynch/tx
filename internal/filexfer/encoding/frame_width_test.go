package encoding

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompressesSlowly(t *testing.T) {
	const frame = 4 << 20
	// A 4 MiB frame is slow at or above 2 ms of encoder CPU.
	threshold := time.Duration(float64(frame) / WideCompressMaxBytesPerCoreSec * float64(time.Second))
	for _, tc := range []struct {
		name        string
		logical     int64
		concurrency int
		wall        time.Duration
		want        bool
	}{
		{"uncompressed frame", frame, 0, time.Millisecond, false},
		{"no time measured", frame, 1, 0, false},
		{"empty frame", 0, 1, time.Millisecond, false},
		{"random data through lz4, about 20 GiB/s", frame, 1, 200 * time.Microsecond, false},
		{"just under the threshold", frame, 1, threshold - time.Microsecond, false},
		{"just over the threshold", frame, 1, threshold + time.Microsecond, true},
		{"text through lz4, about 500 MiB/s", frame, 1, 8 * time.Millisecond, true},
		// A wide frame's wall time understates its CPU: 1 ms on 4 goroutines
		// is 4 ms of CPU, which is slow, so the next frame stays wide.
		{"wide text frame", frame, 4, time.Millisecond, true},
		{"wide random frame", frame, 4, 100 * time.Microsecond, false},
	} {
		if got := compressesSlowly(tc.logical, tc.concurrency, tc.wall); got != tc.want {
			t.Errorf("%s: compressesSlowly(%d, %d, %v) = %v, want %v", tc.name, tc.logical, tc.concurrency, tc.wall, got, tc.want)
		}
	}
}

// TestFrameWidth walks a window through the cases its policy distinguishes.
func TestFrameWidth(t *testing.T) {
	const frame = 4 << 20
	slow, fast := 8*time.Millisecond, 200*time.Microsecond
	var w FrameWidth
	if w.Wide(EncodingLz4) {
		t.Fatal("a window's first frame went wide")
	}
	w.Observe(EncodingLz4, frame, 1, slow)
	if !w.Wide(EncodingLz4) {
		t.Fatal("the frame after a slow lz4 frame stayed narrow")
	}
	if w.Wide(EncodingZstd) {
		t.Fatal("a zstd frame went wide on lz4's evidence")
	}
	w.Observe(EncodingLz4, frame, 4, slow/4) // still slow once spread
	if !w.Wide(EncodingLz4) {
		t.Fatal("a wide frame that stayed slow narrowed the next")
	}
	w.Observe(EncodingLz4, frame, 4, fast/4) // the data turned cheap
	if w.Wide(EncodingLz4) {
		t.Fatal("the frame after a fast frame went wide")
	}
	w.Observe(EncodingLz4, frame, 1, slow)
	w.Observe("none", frame, 0, 0)
	if w.Wide(EncodingLz4) {
		t.Fatal("the frame after an uncompressed frame went wide")
	}
	gentle := FrameWidth{Gentle: true}
	gentle.Observe(EncodingLz4, frame, 1, slow)
	if gentle.Wide(EncodingLz4) {
		t.Fatal("a gentle window went wide")
	}
}

// FuzzFrameWidth drives a window through random frames and checks Wide
// against a model: wide only after a frame of the same codec whose encoder
// CPU (wall time times goroutines) put it below the threshold, and never in
// a gentle window.
func FuzzFrameWidth(f *testing.F) {
	f.Add(false, []byte{0, 1, 200, 0, 1, 10})
	f.Add(true, []byte{0, 1, 255, 0, 1, 255})
	f.Add(false, []byte{1, 2, 128, 2, 0, 0, 1, 4, 30})
	f.Fuzz(func(t *testing.T, gentle bool, ops []byte) {
		codecs := []string{EncodingLz4, EncodingZstd, "none"}
		w := FrameWidth{Gentle: gentle}
		var lastCodec string
		var lastSlow bool
		for len(ops) >= 3 {
			codec := codecs[int(ops[0])%len(codecs)]
			concurrency := []int{1, 2, 4}[int(ops[1])%3]
			if codec == "none" {
				concurrency = 0
			}
			// Wall times from 0 to about 16 ms per 4 MiB frame straddle the
			// 2 ms threshold at every concurrency.
			wall := time.Duration(ops[2]) * 64 * time.Microsecond
			ops = ops[3:]

			want := !gentle && lastSlow && lastCodec == codec
			if got := w.Wide(codec); got != want {
				t.Fatalf("Wide(%s) = %v after a %s frame (slow %v), gentle %v", codec, got, lastCodec, lastSlow, gentle)
			}
			w.Observe(codec, 4<<20, concurrency, wall)
			cpu := wall.Seconds() * float64(concurrency)
			lastCodec, lastSlow = codec, cpu > 0 && float64(4<<20) < WideCompressMaxBytesPerCoreSec*cpu
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
		{4 << 20, 4, 3, 2},
		{3 << 20, 4, 0, 2},
	} {
		if got := frameConcurrencyCap(tc.n, tc.codecMax, tc.callerMax); got != tc.want {
			t.Errorf("frameConcurrencyCap(%d, %d, %d) = %d, want %d", tc.n, tc.codecMax, tc.callerMax, got, tc.want)
		}
	}
}

// TestFrameCompressionBudget checks the core budget's grants: one goroutine
// always, more only within the limit, and everything returned on release.
func TestFrameCompressionBudget(t *testing.T) {
	b := coreBudget{limit: func() int64 { return 8 }}
	first, second, third := b.reserve(4), b.reserve(4), b.reserve(4)
	if first != 4 || second != 4 || third != 1 {
		t.Fatalf("grants on an 8-core budget = %d, %d, %d; want 4, 4, 1", first, second, third)
	}
	b.release(first)
	if got := b.reserve(2); got != 2 {
		t.Fatalf("grant after a release = %d, want 2", got)
	}
	b.release(2)
	b.release(second)
	b.release(third)
	if got := b.inUse.Load(); got != 0 {
		t.Fatalf("%d goroutines still reserved after every release", got)
	}

	small := coreBudget{limit: func() int64 { return 3 }}
	if got := small.reserve(4); got != 2 {
		t.Fatalf("grant on a 3-core budget = %d, want 2", got)
	}

	// Concurrent callers never leave goroutines reserved.
	shared := coreBudget{limit: func() int64 { return 4 }}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 1000 {
				shared.release(shared.reserve(4))
			}
		})
	}
	wg.Wait()
	if got := shared.inUse.Load(); got != 0 {
		t.Fatalf("%d goroutines still reserved after concurrent callers finished", got)
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
