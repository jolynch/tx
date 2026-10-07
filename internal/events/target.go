package events

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// TargetBufferLimit bounds the encoded records an events target holds while
// it cannot be written.
const TargetBufferLimit = 1 << 20

// Target is one events progress target: a file or FIFO path, or "-" for
// Stdout.
type Target struct {
	Path   string
	Stdout io.Writer // used when Path is "-"; nil selects os.Stdout
}

type targetState struct {
	Target

	// Guarded by TargetWriter.mu: emitters append, the flusher takes.
	queue   [][]byte
	size    int
	dropped int64
	partial bool // queue[0] is the unwritten rest of a record already begun

	// Owned by the flusher, under TargetWriter.flushMu.
	wrote bool
	f     *os.File // held open once opened, so a FIFO reader sees one stream
	buf   []byte   // reused to write a whole flush in one call
}

// TargetWriter writes every event to its targets as JSON lines. Unlike
// snapshot progress targets it skips nothing: records wait in a bounded
// buffer until a later write succeeds, and when the buffer overflows the
// oldest records are replaced by a "dropped" record carrying their count.
type TargetWriter struct {
	// mu guards the queues and closed. Emitters hold it only to append, and
	// the flusher only to swap a queue out: every write happens under flushMu
	// alone, so a slow target never blocks an emitter.
	mu      sync.Mutex
	flushMu sync.Mutex
	header  Header
	targets []*targetState
	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	closed  bool
}

// StartTargetWriter subscribes to sink and writes buffered records to the
// targets every interval, or sooner when a buffer is half full. Stop performs
// the final write.
func StartTargetWriter(sink *Sink, h Header, interval time.Duration, targets []Target) *TargetWriter {
	if interval <= 0 {
		interval = time.Second
	}
	w := &TargetWriter{
		header: h,
		kick:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	for _, t := range targets {
		w.targets = append(w.targets, &targetState{Target: t})
	}
	sink.Subscribe(w.add)
	go func() {
		defer close(w.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				w.flush()
			case <-w.kick:
				if w.flush() {
					continue
				}
				// A target that cannot take its records yet (a FIFO with no
				// reader) keeps them; wait for the tick so kicks cannot spin.
				select {
				case <-w.stop:
					return
				case <-t.C:
					w.flush()
				}
			}
		}
	}()
	return w
}

func (w *TargetWriter) add(e Event) {
	rec := AppendJSON(nil, w.header, e)
	kick := false
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	for _, t := range w.targets {
		t.queue = append(t.queue, rec)
		t.size += len(rec)
		t.trimLocked()
		kick = kick || t.size >= TargetBufferLimit/2
	}
	w.mu.Unlock()
	if kick {
		select {
		case w.kick <- struct{}{}:
		default:
		}
	}
}

// trimLocked drops the oldest whole records while the queue is over the
// limit, counting them in dropped. A partly written head stays, since
// dropping it would leave half a record in the stream. The caller holds mu.
func (t *targetState) trimLocked() {
	keep := 1
	if t.partial {
		keep = 2
	}
	for t.size > TargetBufferLimit && len(t.queue) > keep {
		victim := keep - 1
		t.size -= len(t.queue[victim])
		t.queue[victim] = t.queue[0] // a no-op unless the head is partial
		t.queue[0] = nil
		t.queue = t.queue[1:]
		t.dropped++
	}
}

// flush writes what each target has buffered and reports whether every
// target took all of it.
func (w *TargetWriter) flush() bool {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	drained := true
	for _, t := range w.targets {
		w.mu.Lock()
		queue, dropped, partial := t.queue, t.dropped, t.partial
		t.queue, t.size, t.dropped, t.partial = nil, 0, 0, false
		w.mu.Unlock()
		if len(queue) == 0 && dropped == 0 {
			continue
		}
		rest, restDropped, restPartial := w.writeTarget(t, queue, dropped, partial)
		if len(rest) == 0 && restDropped == 0 {
			continue
		}
		drained = false
		// Put back what the target did not take, ahead of anything emitted
		// during the write.
		w.mu.Lock()
		for _, r := range rest {
			t.size += len(r)
		}
		t.queue = append(rest, t.queue...)
		t.dropped += restDropped
		t.partial = restPartial
		t.trimLocked()
		w.mu.Unlock()
	}
	return drained
}

// writeTarget writes a "dropped" record for dropped, then queue, in one Write.
// It returns what the target did not take: the records left, the dropped
// count when its record was not begun, and whether the first record left is
// partly written. The caller holds flushMu, not mu.
func (w *TargetWriter) writeTarget(t *targetState, queue [][]byte, dropped int64, partial bool) ([][]byte, int64, bool) {
	var out io.Writer
	if t.Path == "-" {
		out = t.Stdout
		if out == nil {
			out = os.Stdout
		}
	} else {
		if t.f == nil {
			flags := unix.O_WRONLY | unix.O_CREAT | unix.O_NONBLOCK | unix.O_CLOEXEC
			if t.wrote {
				flags |= unix.O_APPEND
			} else {
				flags |= unix.O_TRUNC
			}
			fd, err := unix.Open(t.Path, flags, 0o644)
			if err != nil {
				return queue, dropped, partial // a FIFO without a reader: keep the records for later
			}
			t.f = os.NewFile(uintptr(fd), t.Path)
		}
		out = t.f
	}
	t.wrote = true

	// A partly written head goes first: anything ahead of it, including the
	// "dropped" record, would land inside a record already begun.
	type segment struct {
		b      []byte
		marker bool // the "dropped" record
	}
	segs := make([]segment, 0, len(queue)+1)
	if partial && len(queue) > 0 {
		segs = append(segs, segment{b: queue[0]})
		queue = queue[1:]
	}
	if dropped > 0 {
		rec := AppendJSON(nil, w.header, Event{T: time.Now(), Name: "dropped", Fields: []Field{F("n", dropped)}})
		segs = append(segs, segment{b: rec, marker: true})
	}
	for _, r := range queue {
		segs = append(segs, segment{b: r})
	}
	t.buf = t.buf[:0]
	for _, seg := range segs {
		t.buf = append(t.buf, seg.b...)
	}
	n, err := out.Write(t.buf)
	// A write error other than a full pipe means the reader left: drop the
	// handle so the next attempt reopens and waits for a new reader.
	if err != nil && !errors.Is(err, syscall.EAGAIN) && t.f != nil {
		t.f.Close()
		t.f = nil
	}

	// Keep what the target did not take, splitting the record it stopped in.
	// An unbegun "dropped" record goes back as its count.
	var rest [][]byte
	var restDropped int64
	restPartial := false
	for i, seg := range segs {
		if n >= len(seg.b) {
			n -= len(seg.b)
			continue
		}
		switch {
		case n > 0:
			rest = append(rest, seg.b[n:])
			restPartial = len(rest) == 1
			n = 0
		case seg.marker:
			restDropped = dropped
		default:
			rest = append(rest, seg.b)
			restPartial = restPartial || (i == 0 && partial && len(rest) == 1)
		}
	}
	return rest, restDropped, restPartial
}

// Stop ends the periodic writes and makes a final one, so a file target is
// complete when the process ends. It is safe to call more than once.
func (w *TargetWriter) Stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	close(w.stop)
	<-w.done
	w.flush()
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	for _, t := range w.targets {
		if t.f != nil {
			t.f.Close()
			t.f = nil
		}
	}
}
