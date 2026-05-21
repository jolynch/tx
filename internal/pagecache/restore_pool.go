package pagecache

import (
	"context"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultRestoreWorkerQueueDepth is the default channel capacity for the
// cache-restore worker pool. Producers block when the channel is full;
// callers should run them in their own goroutine so SYNC handlers never
// stall on this.
const DefaultRestoreWorkerQueueDepth = 4096

// RestoreProducerTimeout is how long a single SendBatch goroutine will
// keep trying to drain its slice into the pool's channel before giving
// up. Anything still pending after this elapses is abandoned and
// counted in the batch's "dropped" tally. The pool's done signal also
// terminates the producer early on shutdown.
const RestoreProducerTimeout = 1 * time.Minute

// RestoreWorkerPool is a long-running background goroutine pool that
// applies page-cache snapshots from a bounded channel. Callers hand
// items in via SendBatch, which spawns a work-scoped producer
// goroutine; the SYNC handler returns immediately and the producer
// drains its slice under RestoreProducerTimeout, blocking on
// backpressure from the worker pool rather than dropping items
// mid-batch.
//
// Each worker per item: evicts all of the file's pages (FADV_DONTNEED),
// then re-warms the snapshot's resident pages via fadvise(WILLNEED) plus
// a synchronous mmap+read fault-in. Errors are logged with the path and
// otherwise dropped — the operation is advisory.
//
// The pool emits **exactly two** structured log lines per batch — one
// when the producer starts flushing and one when it finishes, with the
// elapsed flush duration on the end line. Workers themselves are quiet
// apart from per-item error messages.
type RestoreWorkerPool struct {
	in   chan TouchEntry
	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	accepted atomic.Int64
	applied  atomic.Int64
	dropped  atomic.Int64
	errored  atomic.Int64
}

// NewRestoreWorkerPool starts a pool. Pass 0 for workers to default to
// runtime.NumCPU(). Pass 0 for queueDepth to default to
// DefaultRestoreWorkerQueueDepth.
//
// On platforms where TouchSupported is false the pool still starts but
// per-item work is a no-op.
func NewRestoreWorkerPool(workers int, queueDepth int) *RestoreWorkerPool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if queueDepth <= 0 {
		queueDepth = DefaultRestoreWorkerQueueDepth
	}
	p := &RestoreWorkerPool{
		in:   make(chan TouchEntry, queueDepth),
		done: make(chan struct{}),
	}
	p.wg.Add(workers)
	for range workers {
		go p.run()
	}
	return p
}

// Send blocks until either the item is queued, the context is done, or
// the pool is closed. Returns true only on successful enqueue. Invalid
// items (empty path, nil/empty entry) are dropped immediately. Safe to
// call from any goroutine; no locks on the hot path.
func (p *RestoreWorkerPool) Send(ctx context.Context, item TouchEntry) bool {
	if p == nil || item.Path == "" || item.Entry == nil || item.Entry.Empty() {
		if p != nil {
			p.dropped.Add(1)
		}
		return false
	}
	// Fast pre-check: a select with three ready cases picks randomly, so
	// without this we'd occasionally let items slip into a closed pool's
	// buffer. After Close has returned, this guarantees rejection.
	select {
	case <-p.done:
		p.dropped.Add(1)
		return false
	default:
	}
	select {
	case p.in <- item:
		p.accepted.Add(1)
		return true
	case <-ctx.Done():
		p.dropped.Add(1)
		return false
	case <-p.done:
		p.dropped.Add(1)
		return false
	}
}

// SendBatch hands a slice of items to a work-scoped producer goroutine
// that flushes them into the pool's bounded channel, blocking on
// backpressure for up to RestoreProducerTimeout. Returns immediately;
// the caller does not wait for the producer.
//
// The producer logs exactly two lines per batch — one at start, one at
// finish with the elapsed flush duration and the sent / dropped counts.
// Items still in the slice when the timeout fires (or the pool closes)
// are abandoned and counted in dropped.
func (p *RestoreWorkerPool) SendBatch(txferID string, items []TouchEntry) {
	if p == nil || len(items) == 0 {
		return
	}
	p.wg.Add(1)
	go p.flushBatch(txferID, items)
}

func (p *RestoreWorkerPool) flushBatch(txferID string, items []TouchEntry) {
	defer p.wg.Done()
	start := time.Now()
	log.Printf("cache-restore: batch tid=%s items=%d begin", txferID, len(items))
	ctx, cancel := context.WithTimeout(context.Background(), RestoreProducerTimeout)
	defer cancel()
	sent := 0
	for i := range items {
		if !p.Send(ctx, items[i]) {
			// Send already accounted the first dropped item; add the
			// remainder so the end log's "dropped" matches reality.
			p.dropped.Add(int64(len(items) - i - 1))
			break
		}
		sent++
	}
	log.Printf("cache-restore: batch tid=%s items=%d sent=%d dropped=%d duration=%s",
		txferID, len(items), sent, len(items)-sent, time.Since(start).Round(time.Millisecond))
}

// Close signals all workers and any in-flight producers to stop and
// waits for them to exit. Workers exit immediately on shutdown (no
// per-item drain) so Close returns in milliseconds; any items still
// buffered in the channel at that moment are abandoned and added to
// the dropped counter with a single visibility log line. Safe to call
// multiple times.
func (p *RestoreWorkerPool) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		close(p.done)
	})
	p.wg.Wait()
	if leftover := int64(len(p.in)); leftover > 0 {
		p.dropped.Add(leftover)
		log.Printf("cache-restore: pool closed with %d buffered items abandoned", leftover)
	}
}

func (p *RestoreWorkerPool) run() {
	defer p.wg.Done()
	for {
		select {
		case item := <-p.in:
			p.apply(item)
			p.applied.Add(1)
		case <-p.done:
			// Exit immediately so Close() doesn't block on in-flight
			// per-item work. Items still buffered in p.in are abandoned
			// and counted by Close() as dropped.
			return
		}
	}
}

func (p *RestoreWorkerPool) apply(item TouchEntry) {
	if !TouchSupported() {
		return
	}
	if err := evictPagesFn(item.Path); err != nil {
		log.Printf("cache-restore: evict %s: %v", item.Path, err)
		p.errored.Add(1)
		// Continue to touch anyway — eviction is advisory.
	}
	if _, err := item.Entry.Touch(item.Path, true); err != nil {
		log.Printf("cache-restore: advise %s: %v", item.Path, err)
		p.errored.Add(1)
		return
	}
	if _, err := item.Entry.Touch(item.Path, false); err != nil {
		log.Printf("cache-restore: read-touch %s: %v", item.Path, err)
		p.errored.Add(1)
	}
}
