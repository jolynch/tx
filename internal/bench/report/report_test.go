package report

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

// FuzzTraceRecordText checks the text encoding of a trace record parses
// back to the same record, whatever its strings contain.
func FuzzTraceRecordText(f *testing.F) {
	f.Add(int64(1700000000000000000), "c", "w1", "ab12", "window", "path with space=x", int64(4096), "-", true)
	f.Add(int64(0), "", "", "", "", "", int64(-1), `"quoted"\`, false)
	f.Add(int64(5), "s", "-", "-", "accept", "12", int64(0), "true", false)
	f.Fuzz(func(t *testing.T, ts int64, side, run, tid, ev, path string, n int64, s string, b bool) {
		for _, v := range []string{side, run, tid, ev, path, s} {
			if strings.ContainsAny(v, "\n\r") {
				t.Skip()
			}
		}
		rec := TraceRecord{T: ts, Side: side, Run: run, TID: tid, Ev: ev, Fields: []KV{
			{"file", json.Number("7")}, {"off", json.Number(jsonInt(n))}, {"dur", json.Number("12")},
			{"path", path}, {"s", s}, {"ok", b}, {"n", json.Number(jsonInt(n))},
		}}
		line := rec.Text()
		got, err := ParseTextRecord(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		want := rec
		norm := func(v string) string {
			if v == "-" {
				return ""
			}
			return v
		}
		want.Side, want.Run, want.TID, want.Ev = norm(side), norm(run), norm(tid), norm(ev)
		// Fixed columns come back first, in column order.
		want.Fields = []KV{{"file", json.Number("7")}, {"off", json.Number(jsonInt(n))}, {"dur", json.Number("12")},
			{"path", path}, {"s", s}, {"ok", b}, {"n", json.Number(jsonInt(n))}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip of %q:\n got %+v\nwant %+v", line, got, want)
		}
		// The JSON encoding round-trips too, except that invalid UTF-8
		// becomes U+FFFD there (the text encoding keeps the bytes).
		back, err := ParseJSONRecord(rec.JSON())
		if err != nil {
			t.Fatalf("parse %s: %v", rec.JSON(), err)
		}
		if back.T != rec.T || (utf8.ValidString(path) && back.Str("path") != path) || back.Int("n") != n || back.Bool("ok") != b {
			t.Fatalf("JSON round trip %+v -> %+v", rec, back)
		}
	})
}

func jsonInt(n int64) string {
	data, _ := json.Marshal(n)
	return string(data)
}

func TestEstimateClockOffset(t *testing.T) {
	// The sender's clock is 5ms behind the client's, the one-way delay is
	// at least 200µs, and the RTT is 400µs.
	const offset = int64(5e6)
	var recs []TraceRecord
	for i := range 50 {
		sendMS := int64(1_000_000 + i*10)
		arrival := sendMS*1e6 + offset + 200_000 + int64(i%7)*100_000
		recs = append(recs, TraceRecord{T: arrival + 1_000_000, Ev: "window", Fields: []KV{
			{"server_ts_ms", json.Number(jsonInt(sendMS))}, {"dur", json.Number("1000000")},
		}})
	}
	recs = append(recs, TraceRecord{Ev: "probe", Fields: []KV{{"rtt", json.Number("400000")}}})
	got, errBound := EstimateClockOffset(recs)
	if math.Abs(float64(got-offset)) > float64(errBound) || errBound != 1_200_000 {
		t.Fatalf("offset %d ± %d, want %d", got, errBound, offset)
	}
	if o, e := EstimateClockOffset(nil); o != 0 || e != 0 {
		t.Fatal("no windows should give no offset")
	}
}

func TestSummarizeAndAggregate(t *testing.T) {
	s := Summarize([]float64{3, 1, 2, 4})
	if s.Min != 1 || s.Max != 4 || s.P50 != 2 || s.Mean != 2.5 || math.Abs(s.Stddev-1.29099) > 1e-4 || s.N != 4 {
		t.Fatalf("Summarize = %+v", s)
	}
	runs := []Run{
		{Label: "w1", Status: StatusOK, WallNS: 9e9, Bytes: 1 << 30},
		{Label: "1", Measured: true, Status: StatusOK, WallNS: 2e9, Bytes: 1 << 30, Sender: &Sender{Rusage: Rusage{MaxRSS: 10}}},
		{Label: "2", Measured: true, Status: StatusFailed, WallNS: 1},
		{Label: "3", Measured: true, Status: StatusOK, WallNS: 1e9, Bytes: 1 << 30},
	}
	a := Aggregate(runs)
	if a.Measured != 3 || a.Failed != 1 || a.SenderRuns != 1 {
		t.Fatalf("Aggregate = %+v", a)
	}
	if w := a.Stats["wall_s"]; w.Min != 1 || w.Max != 2 || w.N != 2 {
		t.Fatalf("wall stats exclude warmups and failures: %+v", w)
	}
	if r := a.Stats["sender_rss"]; r.N != 1 {
		t.Fatalf("sender stats only cover runs with a sender: %+v", r)
	}
}

func TestMetricsRoundTripEveryFormat(t *testing.T) {
	m := Metrics{
		Header: Header{Version: 1, Compress: "zstd", Encrypt: "none", Dataset: Dataset{Files: 3, Bytes: 300, Shape: "profile=small"},
			TxArgs: []string{"--compress", "zstd"}},
		Runs: []Run{
			{Label: "w1", Status: StatusOK, TID: "a", WallNS: 5, Bytes: 300, Files: 3, Verify: Verify{Mode: "full", Files: 3}},
			{Label: "1", Measured: true, Status: StatusOK, TID: "b", WallNS: 4, Bytes: 300, Files: 3, WireBytes: 200,
				Client: Rusage{UserNS: 1, MaxRSS: 2}, Sender: &Sender{Rusage: Rusage{MaxRSS: 3}, ConnsAccepted: 4},
				Cache: Cache{HotPct: 12.5, MetaCold: true}, Verify: Verify{Mode: "full", Files: 3}},
			{Label: "2", Measured: true, Status: StatusFailed, ExitCode: 1, Verify: Verify{Mode: "-"}},
		},
	}
	m.Summary = Aggregate(m.Runs)
	var want bytes.Buffer
	RenderText(&want, m)
	for _, ext := range []string{".json", ".jsonl", ".tsv"} {
		path := filepath.Join(t.TempDir(), "m"+ext)
		if err := WriteMetrics(path, m); err != nil {
			t.Fatal(err)
		}
		if kind, err := Sniff(path); err != nil || kind != KindMetrics {
			t.Fatalf("%s sniffed as %q %v", ext, kind, err)
		}
		got, err := ReadMetrics(path)
		if err != nil {
			t.Fatalf("%s: %v", ext, err)
		}
		if got.Summary.Measured != 2 || got.Summary.Failed != 1 || got.Runs[1].Sender.ConnsAccepted != 4 ||
			got.Runs[1].Cache.HotPct != 12.5 || !got.Runs[1].Cache.MetaCold || got.Runs[0].Sender != nil {
			t.Fatalf("%s round trip: %+v", ext, got)
		}
		var buf bytes.Buffer
		RenderText(&buf, got)
		if ext != ".tsv" && buf.String() != want.String() {
			t.Fatalf("%s renders differently:\n%s\nwant:\n%s", ext, buf.String(), want.String())
		}
	}
}

func TestTraceFilesSniffAndRead(t *testing.T) {
	recs := []TraceRecord{
		{T: 1, Side: "c", Run: "1", TID: "t", Ev: "window", Fields: []KV{{"file", json.Number("1")}, {"len", json.Number("10")}}},
	}
	for _, name := range []string{"c.jsonl", "c.txt"} {
		path := filepath.Join(t.TempDir(), name)
		h := TraceHeader{Side: "c", Host: "h", Start: "s", ClockOffsetNS: -3, ClockErrNS: 4}
		if err := WriteTrace(path, h, recs); err != nil {
			t.Fatal(err)
		}
		if kind, err := Sniff(path); err != nil || kind != KindTrace {
			t.Fatalf("%s sniffed as %q %v", name, kind, err)
		}
		gotH, got, err := ReadTrace(path)
		if err != nil || gotH != h || len(got) != 1 || got[0].Int("len") != 10 || got[0].Run != "1" {
			t.Fatalf("%s: %+v %+v %v", name, gotH, got, err)
		}
	}
	if SiblingPath("dir.x/c.jsonl", "server") != "dir.x/c.server.jsonl" || SiblingPath("dir.x/trace", "server") != "dir.x/trace.server" {
		t.Fatal("SiblingPath")
	}
}

func TestMetricsStreamFIFOKeepsThenDrops(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "m.jsonl")
	if err := unix.Mkfifo(fifo, 0o644); err != nil {
		t.Skip("mkfifo:", err)
	}
	s := NewMetricsStream(fifo, Header{Version: 1})
	pad := strings.Repeat("x", 1000)
	n := 2 * StreamBufferLimit / 1000
	for i := range n {
		s.Run(Run{Label: strconv.Itoa(i), Error: pad})
	}
	r, err := os.Open(fifo) // the stream retries every second
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	dec := json.NewDecoder(r)
	var first map[string]json.RawMessage
	if err := dec.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if _, ok := first["dropped"]; !ok {
		t.Fatalf("first record after overflow: %v", first)
	}
	var next struct{ Run Run }
	if err := dec.Decode(&next); err != nil {
		t.Fatal(err)
	}
	var dropped int
	_ = json.Unmarshal(first["dropped"], &dropped)
	// The header was the oldest record, so it went first.
	if got, _ := strconv.Atoi(next.Run.Label); got != dropped-1 {
		t.Fatalf("dropped %d, next run %s", dropped, next.Run.Label)
	}
	go func() { _, _ = io.Copy(io.Discard, r) }()
	if !s.Close(Summary{}) {
		t.Fatal("Close with a reader attached lost records")
	}
}

func TestMetricsStreamFileReadsBack(t *testing.T) {
	for _, name := range []string{"m.jsonl", "m.txt"} {
		path := filepath.Join(t.TempDir(), name)
		s := NewMetricsStream(path, Header{Version: 1, Compress: "zstd"})
		s.Run(Run{Label: "1", Measured: true, Status: StatusOK, WallNS: 1e9, Bytes: 10})
		s.Run(Run{Label: "2", Measured: true, Status: StatusFailed})
		if !s.Close(Aggregate(nil)) {
			t.Fatal("Close lost records")
		}
		m, err := ReadMetrics(path)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(m.Runs) != 2 || m.Summary.Measured != 2 || m.Summary.Failed != 1 || m.Header.Compress != "zstd" {
			t.Fatalf("%s read back %+v", name, m)
		}
	}
}
