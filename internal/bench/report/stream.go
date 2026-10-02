package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// StreamBufferLimit bounds the records a metrics stream holds while its
// target cannot be written (a FIFO with no reader).
const StreamBufferLimit = 1 << 20

// IsFIFO reports whether path names an existing named pipe.
func IsFIFO(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode()&os.ModeNamedPipe != 0
}

// CanStream reports whether a metrics path takes one record per run as it
// happens: JSON lines (.jsonl) or text rows. A single .json object cannot.
func CanStream(path string) bool {
	return strings.ToLower(filepath.Ext(path)) != ".json"
}

// MetricsStream writes a metrics file as the benchmark runs: a header
// record first, one record per run, and a summary record at the end. It
// works on regular files and FIFOs alike and never blocks the benchmark:
// records wait in a bounded buffer until the target can be opened and
// written, and when the buffer overflows the oldest records are replaced by
// a "dropped" record carrying their count. A regular file is truncated on
// the first write, so each invocation starts a fresh stream.
type MetricsStream struct {
	path    string
	asJSON  bool
	mu      sync.Mutex
	queue   [][]byte
	size    int
	dropped int64
	wrote   bool
	f       *os.File // held open once opened, so a FIFO reader sees one stream
	stop    chan struct{}
	done    chan struct{}
	closed  bool
}

// NewMetricsStream starts a stream with its header record and retries
// pending writes every second until Close.
func NewMetricsStream(path string, h Header) *MetricsStream {
	s := &MetricsStream{path: path, asJSON: IsJSONPath(path), stop: make(chan struct{}), done: make(chan struct{})}
	if s.asJSON {
		s.add(mustJSONLine(map[string]any{"header": h}))
	} else {
		var b bytes.Buffer
		hj, _ := json.Marshal(h)
		b.WriteString("# tx-bench metrics v1\n")
		b.WriteString(textHeaderPrefix)
		b.Write(hj)
		b.WriteByte('\n')
		for i, c := range textColumns {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(c.name)
		}
		b.WriteByte('\n')
		s.add(b.Bytes())
	}
	go func() {
		defer close(s.done)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				s.mu.Lock()
				s.flushLocked()
				s.mu.Unlock()
			}
		}
	}()
	return s
}

func mustJSONLine(v any) []byte {
	data, _ := json.Marshal(v)
	return append(data, '\n')
}

// Run appends one run's record and tries to write it out.
func (s *MetricsStream) Run(r Run) {
	if s.asJSON {
		s.add(mustJSONLine(map[string]any{"run": r}))
		return
	}
	var b strings.Builder
	for i, c := range textColumns {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(c.get(r))
	}
	b.WriteByte('\n')
	s.add([]byte(b.String()))
}

// Close appends the summary record, stops the retries, and makes a last
// write attempt. It reports whether everything was written; a FIFO nobody
// is reading loses what is still buffered.
func (s *MetricsStream) Close(sum Summary) bool {
	if s.asJSON {
		s.add(mustJSONLine(map[string]any{"summary": sum}))
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return true
	}
	s.closed = true
	s.mu.Unlock()
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushLocked()
	if s.f != nil {
		s.f.Close()
		s.f = nil
	}
	return len(s.queue) == 0 && s.dropped == 0
}

func (s *MetricsStream) add(rec []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, rec)
	s.size += len(rec)
	for s.size > StreamBufferLimit && len(s.queue) > 1 {
		s.size -= len(s.queue[0])
		s.queue[0] = nil
		s.queue = s.queue[1:]
		s.dropped++
	}
	s.flushLocked()
}

func (s *MetricsStream) droppedRecord() []byte {
	if s.asJSON {
		return mustJSONLine(map[string]any{"dropped": s.dropped})
	}
	return []byte(fmt.Sprintf("# dropped %d records\n", s.dropped))
}

func (s *MetricsStream) flushLocked() {
	if len(s.queue) == 0 && s.dropped == 0 {
		return
	}
	if s.f == nil {
		flags := unix.O_WRONLY | unix.O_CREAT | unix.O_NONBLOCK | unix.O_CLOEXEC
		if s.wrote {
			flags |= unix.O_APPEND
		} else {
			flags |= unix.O_TRUNC
		}
		fd, err := unix.Open(s.path, flags, 0o644)
		if err != nil {
			return // a FIFO without a reader: keep the records for later
		}
		s.f = os.NewFile(uintptr(fd), s.path)
		s.wrote = true
	}
	f := s.f
	// A write error other than a full pipe means the reader left: drop the
	// handle so the next attempt reopens and waits for a new reader.
	failed := func(err error) bool {
		if err != nil && !errors.Is(err, syscall.EAGAIN) {
			s.f.Close()
			s.f = nil
			return true
		}
		return false
	}
	if s.dropped > 0 {
		if _, err := f.Write(s.droppedRecord()); err != nil {
			failed(err)
			return
		}
		s.dropped = 0
	}
	for len(s.queue) > 0 {
		n, err := f.Write(s.queue[0])
		if failed(err) && n == 0 {
			return
		}
		if n == len(s.queue[0]) {
			s.size -= n
			s.queue[0] = nil
			s.queue = s.queue[1:]
			continue
		}
		// A full FIFO took at most part of a record: keep the rest.
		s.queue[0] = s.queue[0][n:]
		s.size -= n
		return
	}
}
