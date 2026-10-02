package txstats

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/events"
)

func readRecords(t *testing.T, path string) []ServerRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []ServerRecord
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var r ServerRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

func TestServerWriterRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.jsonl")
	sink := events.NewSink()
	w, err := NewServerWriter(path, sink)
	if err != nil {
		t.Fatal(err)
	}
	sink.Emit("accept", "", events.F("conn", uint64(1)))
	sink.Emit("accept", "", events.F("conn", uint64(2)))
	sink.Emit("conn_close", "", events.F("conn", uint64(1)))
	sink.Emit("accept", "", events.F("conn", uint64(3)))
	sink.Emit("heartbeat", "")
	sink.Emit("transfer_start", "t1", events.F("path", "/data"))
	sink.Emit("transfer_start", "t2", events.F("path", "/runs/prep"))
	for _, c := range []string{"zstd", "zstd", "none"} {
		sink.Emit("window", "t1", events.F("len", int64(100)), events.F("wire", int64(40)), events.F("codec", c), events.F("send_path", "buffered"))
	}
	sink.Emit("window", "unknown", events.F("len", int64(1)))
	sink.Emit("transfer_done", "t1", events.F("files", 3), events.F("bytes", int64(300)), events.Dur("dur", time.Second))
	if err := w.Close("sigterm"); err != nil {
		t.Fatal(err)
	}
	sink.Emit("transfer_start", "late", events.F("path", "/x")) // after Close: ignored

	recs := readRecords(t, path)
	if len(recs) != 5 {
		t.Fatalf("got %d records: %+v", len(recs), recs)
	}
	if recs[0].Rec != RecStart || recs[0].Path != "/data" || recs[1].Path != "/runs/prep" {
		t.Fatalf("start records %+v %+v", recs[0], recs[1])
	}
	end := recs[2]
	if end.Rec != RecEnd || end.TID != "t1" || !*end.Complete || end.Files != 3 || end.Bytes != 300 ||
		end.LogicalBytes != 300 || end.WireBytes != 120 || end.Windows["zstd"] != 2 || end.SendPath["buffered"] != 3 {
		t.Fatalf("end record %+v", end)
	}
	if open := recs[3]; open.Rec != RecEnd || open.TID != "t2" || *open.Complete {
		t.Fatalf("unfinished transfer record %+v", open)
	}
	p := recs[4]
	if p.Rec != RecProcess || p.ConnsAccepted != 3 || p.PeakConns != 2 || p.Heartbeats != 1 || p.Transfers != 2 || p.Exit != "sigterm" {
		t.Fatalf("process record %+v", p)
	}
}

func TestRecorderNilAndPhases(t *testing.T) {
	var nilRec *Recorder
	nilRec.Phase("data")
	nilRec.SetTID("x")
	if nilRec.ClientMetrics() != nil || nilRec.Finish(1, "x") != nil || NewRecorder("", "copy", nil) != nil {
		t.Fatal("nil recorder is not inert")
	}
	path := filepath.Join(t.TempDir(), "c.json")
	r := NewRecorder(path, "copy", nil)
	r.Phase("probe")
	time.Sleep(2 * time.Millisecond)
	r.Phase("data")
	r.SetTID("abc")
	r.ClientMetrics().ObserveWindow("lz4", 10, 5)
	r.ClientMetrics().IncAckRetry()
	if err := r.Finish(1, ""); err != nil {
		t.Fatal(err)
	}
	var c Client
	data, _ := os.ReadFile(path)
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	if c.Status != "error" || c.Error != "exit code 1" || c.TID != "abc" || c.Phases.Probe < int64(2*time.Millisecond) ||
		c.Phases.Data <= 0 || c.Windows["lz4"] != 1 || c.WireBytes != 5 || c.AckRetries != 1 || c.WallNS <= 0 {
		t.Fatalf("client stats %+v", c)
	}
}
