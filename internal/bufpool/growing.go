package bufpool

import (
	"errors"
	"io"
)

// The initial allocation never exceeds 64 KiB, regardless of hint.
const growingInitialBytes = 64 * kiB

// Growing holds bytes in a pooled buffer that expands as data arrives, without
// allocating from an untrusted size hint.
type Growing struct {
	buf []byte // len is the bytes written; cap is the current bucket
}

// NewGrowing uses hint up to the initial 64 KiB; pass 0 if unknown.
func NewGrowing(hint int64) (*Growing, error) {
	initial := int64(growingInitialBytes)
	if hint > 0 && hint < initial {
		initial = hint
	}
	buf, _, err := Acquire(int(initial))
	if err != nil {
		return nil, err
	}
	return &Growing{buf: buf[:0]}, nil
}

// grow makes room for n more bytes and releases the old bucket.
func (g *Growing) grow(n int) error {
	need := len(g.buf) + n
	if need <= cap(g.buf) {
		return nil
	}
	if int64(need) > int64(MaxBucket) {
		return errors.New("bufpool: growing buffer exceeds the largest bucket")
	}
	next, _, err := Acquire(need)
	if err != nil {
		return err
	}
	next = next[:len(g.buf)]
	copy(next, g.buf)
	Release(g.buf[:cap(g.buf)])
	g.buf = next
	return nil
}

// Write appends p, growing through buckets as needed.
func (g *Growing) Write(p []byte) (int, error) {
	if err := g.grow(len(p)); err != nil {
		return 0, err
	}
	g.buf = append(g.buf, p...)
	return len(p), nil
}

// ReadFrom consumes r to EOF, growing as bytes arrive. Bound r with
// io.LimitReader when the source is untrusted.
func (g *Growing) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	var probe [1]byte
	for {
		if len(g.buf) < cap(g.buf) {
			// Read straight into the spare capacity so the data is not copied
			// twice.
			n, err := r.Read(g.buf[len(g.buf):cap(g.buf)])
			if n > 0 {
				g.buf = g.buf[:len(g.buf)+n]
				total += int64(n)
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					return total, nil
				}
				return total, err
			}
			continue
		}

		// The buffer is exactly full. Probe for one more byte before paying to
		// move up a bucket: a read that exactly fills its bucket is the
		// ordinary case — it is what a full frame does — and growing first
		// would copy the whole buffer only to discover EOF. At the largest
		// bucket there is no next bucket, so growing first would also turn a
		// valid exactly-full read into an error.
		n, err := r.Read(probe[:])
		if n > 0 {
			if growErr := g.grow(1); growErr != nil {
				return total, growErr
			}
			g.buf = append(g.buf, probe[0])
			total++
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
	}
}

// Bytes returns the accumulated bytes. They remain valid until Release.
func (g *Growing) Bytes() []byte { return g.buf }

// Len returns how many bytes have been accumulated.
func (g *Growing) Len() int { return len(g.buf) }

// Release returns the buffer for reuse. The slice from Bytes must not be used
// afterwards.
func (g *Growing) Release() {
	if g.buf == nil {
		return
	}
	Release(g.buf[:cap(g.buf)])
	g.buf = nil
}
