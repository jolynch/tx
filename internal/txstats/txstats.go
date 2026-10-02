// Package txstats writes tx's --stats output: JSON lines per transfer and
// per process for tx send tree, and one JSON object per run for tx recv copy
// and tx recv get. tx-bench reads both; their shapes are documented in
// docs/pub/CLI.md.
package txstats

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/metrics"
)

// Server record kinds.
const (
	RecStart   = "start"
	RecEnd     = "end"
	RecProcess = "process"
)

// ServerRecord is one line of tx send tree --stats. Fields not used by a
// record kind are omitted.
type ServerRecord struct {
	Rec  string `json:"rec"`
	T    int64  `json:"t"` // unix nanoseconds
	TID  string `json:"tid,omitempty"`
	Path string `json:"path,omitempty"`

	// end
	Complete     *bool            `json:"complete,omitempty"`
	Files        int64            `json:"files,omitempty"`
	Bytes        int64            `json:"bytes,omitempty"`
	LogicalBytes int64            `json:"logical_bytes,omitempty"`
	WireBytes    int64            `json:"wire_bytes,omitempty"`
	Windows      map[string]int64 `json:"windows,omitempty"`
	SendPath     map[string]int64 `json:"send_path,omitempty"`
	DurNS        int64            `json:"dur_ns,omitempty"`

	// process
	ConnsAccepted int64  `json:"conns_accepted,omitempty"`
	PeakConns     int64  `json:"peak_conns,omitempty"`
	Heartbeats    int64  `json:"heartbeats,omitempty"`
	Transfers     int64  `json:"transfers,omitempty"`
	Exit          string `json:"exit,omitempty"`
}

type transferAcc struct {
	path     string
	start    time.Time
	logical  int64
	wire     int64
	windows  map[string]int64
	sendPath map[string]int64
}

// ServerWriter turns server events into --stats records.
type ServerWriter struct {
	mu         sync.Mutex
	f          *os.File
	open       map[string]*transferAcc
	accepted   int64
	live       int64
	peak       int64
	heartbeats int64
	transfers  int64
	closed     bool
}

// NewServerWriter truncates path and subscribes to sink.
func NewServerWriter(path string, sink *events.Sink) (*ServerWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &ServerWriter{f: f, open: map[string]*transferAcc{}}
	sink.Subscribe(w.observe)
	return w, nil
}

func (w *ServerWriter) observe(e events.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	switch e.Name {
	case "transfer_start":
		w.transfers++
		acc := &transferAcc{path: e.Str("path"), start: e.T, windows: map[string]int64{}, sendPath: map[string]int64{}}
		w.open[e.TID] = acc
		w.writeLocked(ServerRecord{Rec: RecStart, T: e.T.UnixNano(), TID: e.TID, Path: acc.path})
	case "window":
		if acc := w.open[e.TID]; acc != nil {
			acc.logical += e.Int("len")
			acc.wire += e.Int("wire")
			acc.windows[e.Str("codec")]++
			acc.sendPath[e.Str("send_path")]++
		}
	case "transfer_done":
		acc := w.open[e.TID]
		if acc == nil {
			return
		}
		delete(w.open, e.TID)
		w.writeLocked(endRecord(e.T, e.TID, acc, true, e.Int("files"), e.Int("bytes")))
	case "accept":
		w.accepted++
		w.live++
		w.peak = max(w.peak, w.live)
	case "conn_close":
		w.live--
	case "heartbeat":
		w.heartbeats++
	}
}

func endRecord(t time.Time, tid string, acc *transferAcc, complete bool, files, bytes int64) ServerRecord {
	return ServerRecord{
		Rec: RecEnd, T: t.UnixNano(), TID: tid, Path: acc.path, Complete: &complete,
		Files: files, Bytes: bytes, LogicalBytes: acc.logical, WireBytes: acc.wire,
		Windows: acc.windows, SendPath: acc.sendPath, DurNS: int64(t.Sub(acc.start)),
	}
}

func (w *ServerWriter) writeLocked(r ServerRecord) {
	data, err := json.Marshal(r)
	if err != nil {
		return
	}
	_, _ = w.f.Write(append(data, '\n'))
}

// Close writes an incomplete end record for every transfer that never
// finished, then the process record, and closes the file. exit names why the
// process is stopping (for example "sigterm" or "exit-after").
func (w *ServerWriter) Close(exit string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	now := time.Now()
	tids := make([]string, 0, len(w.open))
	for tid := range w.open {
		tids = append(tids, tid)
	}
	sort.Strings(tids)
	for _, tid := range tids {
		w.writeLocked(endRecord(now, tid, w.open[tid], false, 0, 0))
	}
	w.writeLocked(ServerRecord{
		Rec: RecProcess, T: now.UnixNano(), ConnsAccepted: w.accepted, PeakConns: w.peak,
		Heartbeats: w.heartbeats, Transfers: w.transfers, Exit: exit,
	})
	w.closed = true
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// Phases are client phase timings in nanoseconds.
type Phases struct {
	Probe    int64 `json:"probe"`
	Manifest int64 `json:"manifest"`
	Data     int64 `json:"data"`
	Finalize int64 `json:"finalize"`
}

// Client is the one object tx recv copy and tx recv get write with --stats.
type Client struct {
	Command  string `json:"command"`
	TID      string `json:"tid,omitempty"`
	Status   string `json:"status"` // ok or error
	Error    string `json:"error,omitempty"`
	ExitCode int    `json:"exit_code"`
	Start    int64  `json:"start"` // unix nanoseconds
	End      int64  `json:"end"`
	WallNS   int64  `json:"wall_ns"`
	Phases   Phases `json:"phases_ns"`

	Files        int64            `json:"files"`
	Bytes        int64            `json:"bytes"`
	LogicalBytes int64            `json:"logical_bytes"`
	WireBytes    int64            `json:"wire_bytes"`
	Windows      map[string]int64 `json:"windows,omitempty"`

	Dials             int64 `json:"dials"`
	SyncFallbacks     int64 `json:"sync_fallbacks"`
	Reuses            int64 `json:"reuses"`
	Heartbeats        int64 `json:"heartbeats"`
	HeartbeatFailures int64 `json:"heartbeat_failures"`
	AckRetries        int64 `json:"ack_retries"`
	RequestErrors     int64 `json:"request_errors"`
}

// Recorder accumulates one client run. A nil *Recorder records nothing, so
// call sites need no --stats check.
type Recorder struct {
	mu      sync.Mutex
	path    string
	sink    *events.Sink
	stats   Client
	start   time.Time
	phase   string
	phaseAt time.Time
	failure string // the first explicit failure reason, preferred over stderr
	Metrics *metrics.ClientMetrics
}

// NewRecorder returns a recorder that writes the stats object to path (when
// set) and emits run_end into sink (when set), or nil when both are off.
func NewRecorder(path, command string, sink *events.Sink) *Recorder {
	if path == "" && !sink.Enabled() {
		return nil
	}
	now := time.Now()
	return &Recorder{path: path, sink: sink, start: now, stats: Client{Command: command}, Metrics: &metrics.ClientMetrics{}}
}

// Begin emits run_start for a run writing to dst.
func (r *Recorder) Begin(dst string) {
	if r == nil {
		return
	}
	r.sink.Emit("run_start", "", events.F("dst", dst), events.F("command", r.stats.Command))
}

// EventSink returns the run's event sink, nil when events are off.
func (r *Recorder) EventSink() *events.Sink {
	if r == nil {
		return nil
	}
	return r.sink
}

// ClientMetrics returns the counters every client in the run shares.
func (r *Recorder) ClientMetrics() *metrics.ClientMetrics {
	if r == nil {
		return nil
	}
	return r.Metrics
}

// SetTID records the run's transfer ID.
func (r *Recorder) SetTID(tid string) {
	if r == nil || tid == "" {
		return
	}
	r.mu.Lock()
	r.stats.TID = tid
	r.mu.Unlock()
}

// TID returns the recorded transfer ID.
func (r *Recorder) TID() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats.TID
}

// AddFiles counts files and bytes the run transferred.
func (r *Recorder) AddFiles(files, bytes int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.stats.Files += files
	r.stats.Bytes += bytes
	r.mu.Unlock()
}

// Fail records why the run failed. The first reason wins: later failures
// are usually consequences of it.
func (r *Recorder) Fail(reason string) {
	if r == nil || reason == "" {
		return
	}
	r.mu.Lock()
	if r.failure == "" {
		r.failure = reason
	}
	r.mu.Unlock()
}

// Phase ends the current phase and starts the named one: probe, manifest,
// data, or finalize. Time spent in a phase accumulates if it recurs.
func (r *Recorder) Phase(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closePhaseLocked(time.Now())
	r.phase = name
	r.phaseAt = time.Now()
}

func (r *Recorder) closePhaseLocked(now time.Time) {
	if r.phase == "" {
		return
	}
	d := int64(now.Sub(r.phaseAt))
	switch r.phase {
	case "probe":
		r.stats.Phases.Probe += d
	case "manifest":
		r.stats.Phases.Manifest += d
	case "data":
		r.stats.Phases.Data += d
	case "finalize":
		r.stats.Phases.Finalize += d
	}
	r.phase = ""
}

// Finish writes the stats object for an exit code. A failed run reports the
// reason given to Fail, else errMsg (the caller's best guess, such as the last
// line written to stderr).
func (r *Recorder) Finish(exitCode int, errMsg string) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.closePhaseLocked(now)
	s := r.stats
	s.ExitCode = exitCode
	s.Status = "ok"
	if exitCode != 0 {
		s.Status = "error"
		s.Error = r.failure
		if s.Error == "" {
			s.Error = errMsg
		}
		if s.Error == "" {
			s.Error = "exit code " + itoa(exitCode)
		}
	}
	s.Start, s.End, s.WallNS = r.start.UnixNano(), now.UnixNano(), int64(now.Sub(r.start))
	m := r.Metrics.Snapshot()
	s.LogicalBytes, s.WireBytes, s.Windows = m.LogicalBytes, m.WireBytes, m.WindowsByCodec
	s.Dials, s.SyncFallbacks, s.Reuses = m.DialCount, m.SyncConnectionCount, m.ConnectionReuseCount
	s.Heartbeats, s.HeartbeatFailures = m.HeartbeatCount, m.HeartbeatFailureCount
	s.AckRetries, s.RequestErrors = m.AckRetryCount, m.RequestErrorCount
	r.sink.Emit("run_end", s.TID, events.F("status", s.Status), events.F("bytes", s.Bytes),
		events.F("files", s.Files), events.F("err", s.Error), events.F("exit_code", exitCode))
	if r.path == "" {
		return nil
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(r.path, append(data, '\n'), 0o644)
}

func itoa(n int) string {
	data, _ := json.Marshal(n)
	return string(data)
}
