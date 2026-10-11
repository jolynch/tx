// Package bufpool provides reusable byte buffers in fixed size buckets.
package bufpool

import (
	"errors"
	"fmt"
	"sync"
)

const (
	kiB = 1024
	miB = 1024 * kiB
)

// buckets covers frame sizes up to 64 MiB.
var buckets = [...]int{
	4 * kiB,
	16 * kiB,
	64 * kiB,
	128 * kiB,
	256 * kiB,
	1 * miB,
	2 * miB,
	4 * miB,
	8 * miB,
	16 * miB,
	32 * miB,
	64 * miB,
}

// MaxBucket is the largest bucket size.
const MaxBucket = 64 * miB

// Buffers above this size are left to the garbage collector.
const maxPooledBytes = 32 * miB

var pools sync.Map // map[int]*sync.Pool

// BucketSize returns the bucket that fits n, or MaxBucket when n exceeds the
// ladder.
func BucketSize(n int64) int {
	for _, b := range buckets {
		if n <= int64(b) {
			return b
		}
	}
	return MaxBucket
}

// poolFor returns the pool of size-byte buffers. It holds *[]byte, so Put
// stores a pointer and allocates nothing.
func poolFor(size int) *sync.Pool {
	if existing, ok := pools.Load(size); ok {
		return existing.(*sync.Pool)
	}
	sz := size
	created := &sync.Pool{New: func() any { b := make([]byte, sz); return &b }}
	actual, _ := pools.LoadOrStore(size, created)
	return actual.(*sync.Pool)
}

// Get returns a pooled buffer holding at least size bytes; the slice it
// points to has its bucket's full length. Contents are not cleared. Get and
// Put allocate nothing once the pool is warm, so per-record and per-message
// paths use them; Put the same pointer back after the last use.
func Get(size int) (*[]byte, error) {
	if size <= 0 {
		return nil, errors.New("bufpool: size must be > 0")
	}
	if size > MaxBucket {
		return nil, fmt.Errorf("bufpool: size %d exceeds the largest bucket %d", size, MaxBucket)
	}
	bucket := BucketSize(int64(size))
	p, ok := poolFor(bucket).Get().(*[]byte)
	if !ok {
		return nil, errors.New("bufpool: pool returned an unexpected type")
	}
	if cap(*p) < bucket {
		// Defend against a short buffer returned by the pool.
		*p = make([]byte, bucket)
	}
	*p = (*p)[:bucket]
	return p, nil
}

// Put returns a buffer from Get for reuse. Buffers not sized to a bucket, or
// larger than maxPooledBytes, are dropped rather than pooled.
func Put(p *[]byte) {
	if p == nil {
		return
	}
	c := cap(*p)
	if c == 0 || c > maxPooledBytes || BucketSize(int64(c)) != c {
		return
	}
	*p = (*p)[:c]
	poolFor(c).Put(p)
}

// Acquire returns a size-byte buffer and a release function. Contents are not
// cleared; callers must overwrite them and release only after their last use.
func Acquire(size int) ([]byte, func(), error) {
	if size <= 0 {
		return nil, nil, errors.New("bufpool: size must be > 0")
	}
	if size > MaxBucket {
		// BucketSize clamps, so servicing this would slice past the clamped
		// capacity. Report it rather than allocating: a request this large was
		// not bounded upstream, and quietly satisfying it is the behavior this
		// pool exists to prevent. Callers with a legitimate need for a larger
		// buffer allocate it themselves.
		return nil, nil, fmt.Errorf("bufpool: size %d exceeds the largest bucket %d", size, MaxBucket)
	}
	p, err := Get(size)
	if err != nil {
		return nil, nil, err
	}
	buf := (*p)[:size]
	released := false
	return buf, func() {
		if released {
			return
		}
		released = true
		Put(p)
	}, nil
}

// Release returns a buffer for reuse. Buffers not sized to a bucket, or larger
// than maxPooledBytes, are dropped rather than pooled.
func Release(buf []byte) {
	c := cap(buf)
	if c == 0 || c > maxPooledBytes {
		return
	}
	if BucketSize(int64(c)) != c {
		return
	}
	b := buf[:c]
	poolFor(c).Put(&b)
}
