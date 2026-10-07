package events

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

// FuzzAppendJSON checks every encoded record is one valid JSON object whose
// fixed keys and string fields survive decoding.
func FuzzAppendJSON(f *testing.F) {
	f.Add("window", "ab12", "a/b\tc.bin", int64(42), "c", "")
	f.Add("", "", "\x00\xff <&>\"\\", int64(-1), "s", "w1")
	f.Fuzz(func(t *testing.T, name, tid, path string, n int64, side, run string) {
		e := Event{T: time.Unix(0, 1700000000123456789), Name: name, TID: tid,
			Fields: []Field{F("path", path), F("n", n), Dur("dur", time.Duration(n)), F("ok", n > 0), F("f", 1.5), F("m", map[string]int{"x": 1})}}
		line := AppendJSON(nil, Header{Side: side, Run: run}, e)
		if line[len(line)-1] != '\n' {
			t.Fatal("record not newline-terminated")
		}
		var got map[string]any
		if err := json.Unmarshal(line, &got); err != nil {
			t.Fatalf("invalid JSON %q: %v", line, err)
		}
		want := func(key, v string) {
			if utf8.ValidString(v) && got[key] != v {
				t.Fatalf("%s = %q, want %q (%s)", key, got[key], v, line)
			}
		}
		want("ev", name)
		want("path", path)
		want("side", side)
		if tid == "" {
			want("tid", "-")
		} else {
			want("tid", tid)
		}
		if _, ok := got["run"]; ok != (run != "") {
			t.Fatalf("run key presence wrong: %s", line)
		}
		if int64(got["t"].(float64)/1e9) != 1700000000 {
			t.Fatalf("t = %v", got["t"])
		}
	})
}

func TestNilSinkIsSafe(t *testing.T) {
	var s *Sink
	s.Emit("x", "")
	s.Subscribe(func(Event) { t.Fatal("called") })
	if s.Enabled() || s.NextConn() != 0 {
		t.Fatal("nil sink reports enabled")
	}
	if FromContext(WithScope(t.Context(), Scope{})).Sink != nil {
		t.Fatal("empty scope stored")
	}
}

func TestSinkDelivers(t *testing.T) {
	s := NewSink()
	var got []Event
	s.Subscribe(func(e Event) { got = append(got, e) })
	s.Emit("a", "t1", F("k", "v"), F("n", 3))
	if len(got) != 1 || got[0].Str("k") != "v" || got[0].Int("n") != 3 || got[0].T.IsZero() {
		t.Fatalf("got %+v", got)
	}
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestTargetWriterFileKeepsEveryEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ev.jsonl")
	if err := os.WriteFile(path, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewSink()
	var stdout bytes.Buffer
	w := StartTargetWriter(s, Header{Side: "c"}, time.Hour, []Target{{Path: path}, {Path: "-", Stdout: &stdout}})
	for i := range 100 {
		s.Emit("window", "t1", F("off", i))
	}
	w.Stop()
	s.Emit("late", "") // after Stop: not written
	w.Stop()
	got := readLines(t, path)
	if len(got) != 100 || got[0]["side"] != "c" || got[99]["off"].(float64) != 99 {
		t.Fatalf("file has %d records (first %v)", len(got), got[0])
	}
	if strings.Count(stdout.String(), "\n") != 100 {
		t.Fatalf("stdout has %d records", strings.Count(stdout.String(), "\n"))
	}
}

func TestTargetWriterFIFOWaitsThenDrops(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "ev.fifo")
	if err := unix.Mkfifo(fifo, 0o644); err != nil {
		t.Skip("mkfifo:", err)
	}
	s := NewSink()
	w := StartTargetWriter(s, Header{Side: "s"}, 5*time.Millisecond, []Target{{Path: fifo}})
	defer w.Stop()
	// No reader: every write attempt fails, so records stay buffered, and
	// overflowing the buffer drops the oldest.
	big := strings.Repeat("x", 1000)
	n := 3 * TargetBufferLimit / 1000
	for i := range n {
		s.Emit("window", "t", F("i", i), F("pad", big))
	}
	time.Sleep(30 * time.Millisecond)
	r, err := os.OpenFile(fifo, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	dec := json.NewDecoder(r)
	var first map[string]any
	if err := dec.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if first["ev"] != "dropped" || first["n"].(float64) <= 0 {
		t.Fatalf("first record after overflow = %v", first)
	}
	dropped := int(first["n"].(float64))
	var next map[string]any
	if err := dec.Decode(&next); err != nil {
		t.Fatal(err)
	}
	if int(next["i"].(float64)) != dropped {
		t.Fatalf("after dropping %d records the next is %v", dropped, next["i"])
	}
}

// lockCheckWriter records each Write and fails the test if the writer's
// emitter mutex is held during one: a slow target must never block emitters.
type lockCheckWriter struct {
	t      *testing.T
	w      *TargetWriter
	mu     sync.Mutex
	writes int
	buf    bytes.Buffer
}

func (c *lockCheckWriter) Write(p []byte) (int, error) {
	if !c.w.mu.TryLock() {
		c.t.Error("target written while holding the emitter mutex")
	} else {
		c.w.mu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	return c.buf.Write(p)
}

func (c *lockCheckWriter) snapshot() (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes, c.buf.String()
}

// Records are written outside the emitter mutex, one Write per flush, and a
// half-full buffer flushes without waiting for the interval.
func TestTargetWriterWritesOutsideEmitterLock(t *testing.T) {
	s := NewSink()
	out := &lockCheckWriter{t: t}
	out.w = StartTargetWriter(s, Header{Side: "c"}, time.Hour, []Target{{Path: "-", Stdout: out}})
	for i := range 100 {
		s.Emit("window", "t1", F("off", i))
	}
	out.w.Stop()
	writes, got := out.snapshot()
	if writes != 1 || strings.Count(got, "\n") != 100 {
		t.Fatalf("got %d records in %d writes, want 100 in 1", strings.Count(got, "\n"), writes)
	}

	s = NewSink()
	out = &lockCheckWriter{t: t}
	out.w = StartTargetWriter(s, Header{Side: "c"}, time.Hour, []Target{{Path: "-", Stdout: out}})
	defer out.w.Stop()
	pad := strings.Repeat("x", 1000)
	for i := range TargetBufferLimit / 2 / 1000 {
		s.Emit("window", "t1", F("i", i), F("pad", pad))
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if writes, _ := out.snapshot(); writes > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a half-full buffer was not flushed before the interval")
		}
		time.Sleep(time.Millisecond)
	}
}
