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
	queue   [][]byte
	size    int
	dropped int64
	wrote   bool
	f       *os.File // held open once opened, so a FIFO reader sees one stream
}

// TargetWriter writes every event to its targets as JSON lines. Unlike
// snapshot progress targets it skips nothing: records wait in a bounded
// buffer until a later write succeeds, and when the buffer overflows the
// oldest records are replaced by a "dropped" record carrying their count.
type TargetWriter struct {
	mu      sync.Mutex
	header  Header
	targets []*targetState
	stop    chan struct{}
	done    chan struct{}
	closed  bool
}

// StartTargetWriter subscribes to sink and writes buffered records to the
// targets every interval. Stop performs the final write.
func StartTargetWriter(sink *Sink, h Header, interval time.Duration, targets []Target) *TargetWriter {
	if interval <= 0 {
		interval = time.Second
	}
	w := &TargetWriter{header: h, stop: make(chan struct{}), done: make(chan struct{})}
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
			}
		}
	}()
	return w
}

func (w *TargetWriter) add(e Event) {
	rec := AppendJSON(nil, w.header, e)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	for _, t := range w.targets {
		t.queue = append(t.queue, rec)
		t.size += len(rec)
		for t.size > TargetBufferLimit && len(t.queue) > 1 {
			t.size -= len(t.queue[0])
			t.queue[0] = nil
			t.queue = t.queue[1:]
			t.dropped++
		}
	}
}

// flush writes what each target has buffered.
func (w *TargetWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, t := range w.targets {
		w.flushTarget(t)
	}
}

func (w *TargetWriter) flushTarget(t *targetState) {
	if len(t.queue) == 0 && t.dropped == 0 {
		return
	}
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
				return // a FIFO without a reader: keep the records for later
			}
			t.f = os.NewFile(uintptr(fd), t.Path)
		}
		out = t.f
	}
	t.wrote = true
	// A write error other than a full pipe means the reader left: drop the
	// handle so the next attempt reopens and waits for a new reader.
	failed := func(err error) bool {
		if err != nil && !errors.Is(err, syscall.EAGAIN) && t.f != nil {
			t.f.Close()
			t.f = nil
			return true
		}
		return false
	}
	if t.dropped > 0 {
		rec := AppendJSON(nil, w.header, Event{T: time.Now(), Name: "dropped", Fields: []Field{F("n", t.dropped)}})
		if _, err := out.Write(rec); err != nil {
			failed(err)
			return
		}
		t.dropped = 0
	}
	for len(t.queue) > 0 {
		n, err := out.Write(t.queue[0])
		if failed(err) && n == 0 {
			return
		}
		if n == len(t.queue[0]) {
			t.size -= n
			t.queue[0] = nil
			t.queue = t.queue[1:]
			continue
		}
		// A full FIFO (EAGAIN) or a failed write took at most part of a
		// record: keep the rest for the next attempt.
		t.queue[0] = t.queue[0][n:]
		t.size -= n
		return
	}
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
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, t := range w.targets {
		if t.f != nil {
			t.f.Close()
			t.f = nil
		}
	}
}
