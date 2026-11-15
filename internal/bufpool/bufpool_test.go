package bufpool

import (
	"bytes"
	"io"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

func TestBucketSizeLadder(t *testing.T) {
	cases := []struct {
		n    int64
		want int
	}{
		{1, 4 * kiB},
		{4 * kiB, 4 * kiB},
		{4*kiB + 1, 16 * kiB},
		{64 * kiB, 64 * kiB},
		{64*kiB + 1, 128 * kiB},
		{128 * kiB, 128 * kiB},
		{128*kiB + 1, 256 * kiB},
		{4 * miB, 4 * miB},
		{4*miB + 1, 8 * miB},
		{64 * miB, 64 * miB},
		{64*miB + 1, MaxBucket}, // clamps rather than growing without bound
	}
	for _, tc := range cases {
		if got := BucketSize(tc.n); got != tc.want {
			t.Errorf("BucketSize(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestGetReturnsBucketLengthAndCapacity(t *testing.T) {
	p, err := Get(5000)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer Put(p)
	if len(*p) != 16*kiB {
		t.Errorf("len = %d, want the 16 KiB bucket", len(*p))
	}
	if cap(*p) != 16*kiB {
		t.Errorf("cap = %d, want the 16 KiB bucket", cap(*p))
	}
}

// Get and Put are the per-record path's pool: once warm they allocate
// nothing, and Get hands back a bucket-length buffer.
func TestGetPutDoNotAllocate(t *testing.T) {
	p, err := Get(100 * kiB)
	if err != nil || len(*p) != 128*kiB {
		t.Fatalf("Get(100KiB) = (%d bytes, %v), want the 128 KiB bucket", len(*p), err)
	}
	Put(p)
	if raceEnabled {
		t.Skip("the race detector makes sync.Pool drop buffers at random")
	}
	allocs := testing.AllocsPerRun(100, func() {
		p, _ := Get(128 * kiB)
		Put(p)
	})
	if allocs != 0 {
		t.Fatalf("Get/Put allocated %.1f times", allocs)
	}
}

// BucketSize clamps, so servicing a request above MaxBucket would slice past
// the clamped capacity. Get reports it instead: a size this large was not
// bounded upstream, and quietly satisfying it is the failure mode the pool
// exists to prevent.
func TestGetRejectsInvalidSizes(t *testing.T) {
	for _, n := range []int{0, -1, MaxBucket + 1} {
		if p, err := Get(n); err == nil {
			t.Errorf("Get(%d) succeeded with %d bytes, want an error", n, len(*p))
		}
	}
	p, err := Get(MaxBucket)
	if err != nil {
		t.Fatalf("Get at exactly MaxBucket failed: %v", err)
	}
	if len(*p) != MaxBucket {
		t.Fatalf("len = %d, want %d", len(*p), MaxBucket)
	}
	Put(p)
}

// A buffer Put back keeps its bucket length and capacity, and is the one the
// next Get on the same goroutine receives. This is also the control for
// TestPutDropsOversizedAndNonBucketBuffers: it shows that pointer identity
// detects a pooled buffer.
func TestBuffersRoundTripThroughThePool(t *testing.T) {
	p, err := Get(4 * kiB)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Put restores the full bucket length of a buffer that was resliced.
	*p = (*p)[:10]
	Put(p)

	// sync.Pool may discard buffers; only bucket length and capacity are
	// guaranteed.
	for i := 0; i < 100; i++ {
		q, err := Get(4 * kiB)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(*q) != 4*kiB || cap(*q) != 4*kiB {
			t.Fatalf("len, cap = %d, %d, want 4 KiB", len(*q), cap(*q))
		}
		Put(q)
	}

	if raceEnabled {
		t.Skip("the race detector makes sync.Pool drop buffers at random")
	}
	// Put and Get use the current P's private slot, which no other P can take.
	// If the goroutine moves to another P between them, Get misses the buffer,
	// so allow a few attempts.
	const attempts = 10
	for i := 0; i < attempts; i++ {
		p, err := Get(16 * kiB)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		(*p)[0] = 0xAB
		Put(p)
		q, err := Get(16 * kiB)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		Put(q)
		if q == p && (*q)[0] == 0xAB {
			return
		}
	}
	t.Fatalf("Get after Put returned a different buffer in all %d attempts, want the one put back", attempts)
}

// Buffers not sized to a bucket, or above the pooling limit, must not remain
// pooled.
func TestPutDropsOversizedAndNonBucketBuffers(t *testing.T) {
	odd := make([]byte, 1234)
	Put(&odd)
	if _, ok := pools.Load(1234); ok {
		t.Error("a non-bucket size got its own pool")
	}
	// Put judges the capacity, not the length.
	short := make([]byte, 4*kiB, 5000)
	Put(&short)
	if _, ok := pools.Load(5000); ok {
		t.Error("a non-bucket capacity got its own pool")
	}

	// A bucket above maxPooledBytes is left to the garbage collector. Take the
	// pool's next buffer straight after the Put: had the Put pooled it, that
	// would be the same pointer, as TestBuffersRoundTripThroughThePool shows.
	big := make([]byte, MaxBucket)
	Put(&big)
	if p, ok := pools.Load(MaxBucket); ok {
		if got, isBuf := p.(*sync.Pool).Get().(*[]byte); isBuf && got == &big {
			t.Errorf("a %d-byte buffer was pooled above the %d-byte limit", MaxBucket, maxPooledBytes)
		}
	}
	Put(nil)
}

// allocatedBytes reports heap bytes allocated while fn runs.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A large hint must not allocate a large buffer before data arrives.
func TestGrowingAllocatesForDataNotForTheHint(t *testing.T) {
	const hugeHint = 64 * miB
	const actual = 16

	// Measure steady-state allocations.
	for i := 0; i < 3; i++ {
		g, err := NewGrowing(hugeHint)
		if err != nil {
			t.Fatalf("NewGrowing: %v", err)
		}
		_, _ = g.ReadFrom(bytes.NewReader(make([]byte, actual)))
		g.Release()
	}

	if raceEnabled {
		t.Skip("allocation measurement is not meaningful under -race")
	}

	used := allocatedBytes(func() {
		for i := 0; i < 20; i++ {
			g, err := NewGrowing(hugeHint)
			if err != nil {
				t.Fatalf("NewGrowing: %v", err)
			}
			if _, err := g.ReadFrom(bytes.NewReader(make([]byte, actual))); err != nil {
				t.Fatalf("ReadFrom: %v", err)
			}
			if g.Len() != actual {
				t.Fatalf("Len = %d, want %d", g.Len(), actual)
			}
			g.Release()
		}
	})

	// Allow slack for test scaffolding, but not 20 allocations of 64 MiB.
	if used > 8*miB {
		t.Fatalf("allocated %d bytes for 20 x %d real bytes under a %d hint — the hint is sizing the allocation",
			used, actual, hugeHint)
	}
	t.Logf("allocated %d bytes total for 20 reads of %d bytes under a %d-byte hint", used, actual, hugeHint)
}

func TestGrowingStepsUpThroughBuckets(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 300*kiB)
	g, err := NewGrowing(0)
	if err != nil {
		t.Fatalf("NewGrowing: %v", err)
	}
	defer g.Release()

	n, err := g.ReadFrom(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("read %d bytes, want %d", n, len(payload))
	}
	if !bytes.Equal(g.Bytes(), payload) {
		t.Fatal("content did not survive the bucket transitions")
	}
	if cap(g.Bytes()) < len(payload) {
		t.Fatalf("cap %d is below the %d bytes held", cap(g.Bytes()), len(payload))
	}
	// Capacity should track data size.
	if cap(g.Bytes()) > 1*miB {
		t.Fatalf("cap %d overshot for %d bytes", cap(g.Bytes()), len(payload))
	}
}

func TestGrowingWriteAndLimitReader(t *testing.T) {
	g, err := NewGrowing(0)
	if err != nil {
		t.Fatalf("NewGrowing: %v", err)
	}
	defer g.Release()

	if _, err := g.Write([]byte("hello ")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// LimitReader is how callers bound an untrusted source.
	if _, err := g.ReadFrom(io.LimitReader(bytes.NewReader([]byte("world and then some")), 5)); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if got := string(g.Bytes()); got != "hello world" {
		t.Fatalf("got %q", got)
	}
}

// A second Release must not put the buffer back twice: the pool would then
// hand the same memory to two later Growing values.
func TestGrowingReleaseIsIdempotent(t *testing.T) {
	g, err := NewGrowing(0)
	if err != nil {
		t.Fatalf("NewGrowing: %v", err)
	}
	if _, err := g.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	g.Release()
	g.Release()

	// Both copies of a double Put sit at the front of the pool, behind at most
	// the one buffer already in its private slot, so four Gets reach them.
	var held [4]*Growing
	for i := range held {
		h, err := NewGrowing(0)
		if err != nil {
			t.Fatalf("NewGrowing: %v", err)
		}
		defer h.Release()
		if _, err := h.Write([]byte{byte(i)}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		held[i] = h
	}
	for i := range held {
		for j := i + 1; j < len(held); j++ {
			if &held[i].Bytes()[0] == &held[j].Bytes()[0] {
				t.Fatalf("Growing values %d and %d share a buffer after a double Release", i, j)
			}
		}
	}
}

// Growing moves between buckets with Get and Put alone: once the pool is warm,
// a NewGrowing, Writes that cross two bucket boundaries, and a Release
// allocate only the *Growing itself.
func TestGrowingDoesNotAllocatePerBucket(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector makes sync.Pool drop buffers at random")
	}
	chunk := make([]byte, 64*kiB)
	run := func() {
		g, err := NewGrowing(0) // 64 KiB bucket
		if err != nil {
			t.Fatalf("NewGrowing: %v", err)
		}
		_, _ = g.Write(chunk) // fills the 64 KiB bucket
		_, _ = g.Write(chunk) // steps to 128 KiB
		_, _ = g.Write(chunk) // steps to 256 KiB
		if g.Len() != 3*len(chunk) || cap(g.Bytes()) != 256*kiB {
			t.Fatalf("Len, cap = %d, %d, want %d bytes in the 256 KiB bucket", g.Len(), cap(g.Bytes()), 3*len(chunk))
		}
		g.Release()
	}
	run() // warm the 64, 128, and 256 KiB pools
	if allocs := testing.AllocsPerRun(100, run); allocs > 1 {
		t.Fatalf("NewGrowing, two bucket steps, and Release allocated %.1f times, want at most 1 (the *Growing)", allocs)
	}
}

// ReadFrom's one-byte probe at a full bucket must not allocate either: a warm
// NewGrowing, a ReadFrom that grows through two buckets, and a Release
// allocate only the *Growing itself.
func TestGrowingReadFromDoesNotAllocatePerBucket(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector makes sync.Pool drop buffers at random")
	}
	payload := make([]byte, 3*64*kiB)
	r := bytes.NewReader(nil)
	run := func() {
		r.Reset(payload)
		g, err := NewGrowing(0) // 64 KiB bucket
		if err != nil {
			t.Fatalf("NewGrowing: %v", err)
		}
		if _, err := g.ReadFrom(r); err != nil {
			t.Fatalf("ReadFrom: %v", err)
		}
		if g.Len() != len(payload) || cap(g.Bytes()) != 256*kiB {
			t.Fatalf("Len, cap = %d, %d, want %d bytes in the 256 KiB bucket", g.Len(), cap(g.Bytes()), len(payload))
		}
		g.Release()
	}
	run() // warm the 64, 128, and 256 KiB pools
	if allocs := testing.AllocsPerRun(100, run); allocs > 1 {
		t.Fatalf("NewGrowing, ReadFrom through two buckets, and Release allocated %.1f times, want at most 1 (the *Growing)", allocs)
	}
}

// zeroReader supplies endless zero bytes without materializing a source, so a
// test can drive a 64 MiB read without also allocating 64 MiB to read from.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// A read that exactly fills its bucket is the ordinary case — it is what a full
// frame does — and it must not grow. Growing first and discovering EOF
// afterwards copies the whole buffer for nothing.
func TestGrowingExactBucketFitDoesNotGrow(t *testing.T) {
	for _, bucket := range buckets {
		if bucket > 4*miB {
			continue // larger buckets are covered separately; keep this cheap
		}
		t.Run(strconv.Itoa(bucket), func(t *testing.T) {
			g, err := NewGrowing(int64(bucket))
			if err != nil {
				t.Fatalf("NewGrowing: %v", err)
			}
			defer g.Release()

			n, err := g.ReadFrom(io.LimitReader(zeroReader{}, int64(bucket)))
			if err != nil {
				t.Fatalf("exact-fit read failed: %v", err)
			}
			if n != int64(bucket) {
				t.Fatalf("read %d bytes, want %d", n, bucket)
			}
			if got := cap(g.Bytes()); got != bucket {
				t.Fatalf("buffer grew to %d for an exact %d-byte fit", got, bucket)
			}
		})
	}
}

// One byte past a bucket must land in exactly the next bucket, not skip ahead.
func TestGrowingOneBytePastBucketStepsUpOnce(t *testing.T) {
	for i, bucket := range buckets {
		if bucket > 1*miB || i+1 >= len(buckets) {
			continue
		}
		t.Run(strconv.Itoa(bucket), func(t *testing.T) {
			g, err := NewGrowing(int64(bucket))
			if err != nil {
				t.Fatalf("NewGrowing: %v", err)
			}
			defer g.Release()

			want := int64(bucket) + 1
			n, err := g.ReadFrom(io.LimitReader(zeroReader{}, want))
			if err != nil {
				t.Fatalf("read failed: %v", err)
			}
			if n != want {
				t.Fatalf("read %d bytes, want %d", n, want)
			}
			if got, next := cap(g.Bytes()), buckets[i+1]; got != next {
				t.Fatalf("buffer is %d after %d bytes, want the next bucket %d", got, want, next)
			}
		})
	}
}

// The correctness half: at the largest bucket there is no next bucket to grow
// into, so growing before checking for EOF turns a valid exactly-full read into
// an error.
func TestGrowingExactlyMaxBucketSucceeds(t *testing.T) {
	g, err := NewGrowing(MaxBucket)
	if err != nil {
		t.Fatalf("NewGrowing: %v", err)
	}
	defer g.Release()

	n, err := g.ReadFrom(io.LimitReader(zeroReader{}, MaxBucket))
	if err != nil {
		t.Fatalf("a legitimate exactly-MaxBucket read was rejected: %v", err)
	}
	if n != MaxBucket {
		t.Fatalf("read %d bytes, want %d", n, MaxBucket)
	}
}
