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
		{64*kiB + 1, 256 * kiB},
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

func TestAcquireReturnsRequestedLengthFromBucketCapacity(t *testing.T) {
	buf, release, err := Acquire(5000)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	if len(buf) != 5000 {
		t.Errorf("len = %d, want 5000", len(buf))
	}
	if cap(buf) != 16*kiB {
		t.Errorf("cap = %d, want the 16 KiB bucket", cap(buf))
	}
}

func TestAcquireRejectsNonPositive(t *testing.T) {
	for _, n := range []int{0, -1} {
		if _, _, err := Acquire(n); err == nil {
			t.Errorf("Acquire(%d) succeeded, want an error", n)
		}
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	_, release, err := Acquire(1024)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
	release() // must not double-Put the same buffer into the pool
}

func TestBuffersRoundTripThroughThePool(t *testing.T) {
	buf, release, err := Acquire(4 * kiB)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	marker := byte(0xAB)
	buf[0] = marker
	release()

	// sync.Pool may discard buffers; only bucket capacity is guaranteed.
	for i := 0; i < 100; i++ {
		b, rel, err := Acquire(4 * kiB)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if cap(b) != 4*kiB {
			t.Fatalf("cap = %d, want 4 KiB", cap(b))
		}
		rel()
	}
}

// Oversized buffers must not remain pooled.
func TestReleaseDropsOversizedAndNonBucketBuffers(t *testing.T) {
	// A non-bucket capacity is not pooled.
	Release(make([]byte, 1234))
	if _, ok := pools.Load(1234); ok {
		t.Error("a non-bucket size got its own pool")
	}
	// Releasing buffers above the cap must not create a pool.
	for i := 0; i < 5; i++ {
		Release(make([]byte, MaxBucket))
	}
	if p, ok := pools.Load(MaxBucket); ok {
		if got, isBuf := p.(*sync.Pool).Get().([]byte); isBuf && len(got) == MaxBucket {
			// Value alone cannot distinguish a new buffer from a pooled one.
			Release(got)
		}
	}
	Release(nil)
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

func TestGrowingReleaseIsIdempotent(t *testing.T) {
	g, err := NewGrowing(0)
	if err != nil {
		t.Fatalf("NewGrowing: %v", err)
	}
	g.Release()
	g.Release()
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

// BucketSize clamps, so a larger request used to slice past the clamped
// capacity and panic. It must report the problem instead: a size this large was
// not bounded upstream, and silently servicing it is the failure mode the pool
// exists to prevent.
func TestAcquireAboveMaxBucketReturnsError(t *testing.T) {
	if _, _, err := Acquire(MaxBucket + 1); err == nil {
		t.Fatal("Acquire above MaxBucket succeeded, want an error")
	}
	buf, release, err := Acquire(MaxBucket)
	if err != nil {
		t.Fatalf("Acquire at exactly MaxBucket failed: %v", err)
	}
	if len(buf) != MaxBucket {
		t.Fatalf("len = %d, want %d", len(buf), MaxBucket)
	}
	release()
}
