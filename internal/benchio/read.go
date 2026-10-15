// Package benchio injects optional read latency into verification benchmarks.
package benchio

import (
	"context"
	"io"
	"sync/atomic"
)

// WithReadLatency wraps reads only when benchmark instrumentation requests it.
// Production returns the original reader without allocating a wrapper.
func WithReadLatency(ctx context.Context, side string, reader io.ReaderAt) io.ReaderAt {
	inst := readLatency.Load()
	if inst == nil || inst.wait == nil {
		return reader
	}
	return &latencyReader{ctx: ctx, side: side, reader: reader, wait: inst.wait}
}

type latencyReader struct {
	ctx    context.Context
	side   string
	reader io.ReaderAt
	wait   func(context.Context, string, int) error
}

func (r *latencyReader) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) > 0 {
		if err := r.wait(r.ctx, r.side, len(p)); err != nil {
			return 0, err
		}
	}
	return r.reader.ReadAt(p, offset)
}

// The hook is installed before benchmark work starts and restored after it ends.
type latencyHook struct {
	wait func(context.Context, string, int) error
}

var readLatency atomic.Pointer[latencyHook]

// SetReadLatency installs a benchmark hook and returns its restore function.
func SetReadLatency(wait func(context.Context, string, int) error) func() {
	old := readLatency.Swap(&latencyHook{wait: wait})
	return func() { readLatency.Store(old) }
}
