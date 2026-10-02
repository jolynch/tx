package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
)

// sampleRSS appends an rss event for p to path every interval until the
// returned stop is called or p exits.
func sampleRSS(p *proc, every time.Duration, path, side string) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		defer f.Close()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			if rss, err := readRSS(p.pid()); err == nil {
				rec := report.TraceRecord{T: time.Now().UnixNano(), Side: side, Ev: "rss",
					Fields: []report.KV{{Key: "bytes", Val: json.Number(fmt.Sprint(rss))}}}
				_, _ = f.Write(rec.JSON())
			}
			select {
			case <-done:
				return
			case <-p.done:
				return
			case <-t.C:
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done); wg.Wait() }) }
}

// readEvents reads a tx events file and its rss samples, merged by time.
// Missing files read as empty.
func readEvents(paths ...string) ([]report.TraceRecord, error) {
	var all []report.TraceRecord
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		_, recs, err := report.ReadTrace(p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		all = append(all, recs...)
	}
	slices.SortStableFunc(all, func(a, b report.TraceRecord) int {
		switch {
		case a.T < b.T:
			return -1
		case a.T > b.T:
			return 1
		}
		return 0
	})
	return all, nil
}

func stagingPath(path string) string { return path + ".partial.jsonl" }

// initClientTrace starts the staging files that finishClientTrace turns
// into PATH and its .server sibling.
func initClientTrace(path string, _ report.Header) error {
	for _, p := range []string{stagingPath(path), stagingPath(report.SiblingPath(path, "server"))} {
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			return fmt.Errorf("--trace: %w", err)
		}
	}
	return nil
}

// appendClientTrace labels one run's client events and stages them.
func appendClientTrace(path, label, eventsPath string) error {
	recs, err := readEvents(eventsPath, eventsPath+".rss")
	if err != nil {
		return err
	}
	for i := range recs {
		recs[i].Run = label
		if recs[i].Side == "" {
			recs[i].Side = "c"
		}
	}
	return report.AppendTrace(stagingPath(path), recs)
}

// appendVerifyTrace stages the oracle's verify_start, verify_fail, and
// verify_end events for a run.
func appendVerifyTrace(path, label, tid string, start time.Time, res dataset.VerifyResult) error {
	rec := func(t time.Time, ev string, fields ...report.KV) report.TraceRecord {
		return report.TraceRecord{T: t.UnixNano(), Side: "c", Run: label, TID: tid, Ev: ev, Fields: fields}
	}
	end := start.Add(res.Duration)
	recs := []report.TraceRecord{rec(start, "verify_start")}
	for _, m := range append(slices.Clone(res.Mismatches), res.Excused...) {
		recs = append(recs, rec(end, "verify_fail", report.KV{Key: "path", Val: m.Path}, report.KV{Key: "field", Val: m.Field},
			report.KV{Key: "expected", Val: m.Expected}, report.KV{Key: "got", Val: m.Got}))
	}
	recs = append(recs, rec(end, "verify_end", report.KV{Key: "files", Val: num(res.Files)}, report.KV{Key: "bytes", Val: num(res.Bytes)},
		report.KV{Key: "dur", Val: num(int64(res.Duration))}, report.KV{Key: "mismatches", Val: num(res.MismatchCount + res.ExcusedCount)}))
	return report.AppendTrace(stagingPath(path), recs)
}

// fetchServerTrace fetches a run's sender events and stages them.
func (r *receiver) fetchServerTrace(ctx context.Context, run *report.Run) error {
	local := filepath.Join(r.tmp, "server-"+run.TID+".trace.jsonl")
	if _, err := r.get(ctx, serverTraceName(run.TID), local); err != nil {
		return err
	}
	_, recs, err := report.ReadTrace(local)
	if err != nil {
		return err
	}
	for i := range recs {
		recs[i].Run = "-"
	}
	return report.AppendTrace(stagingPath(report.SiblingPath(r.cfg.flags.trace, "server")), recs)
}

// finishClientTrace estimates the clock offset and writes the final trace
// files in the format their extension selects.
func finishClientTrace(path string, h *report.Header, _ []report.Run) error {
	_, client, err := report.ReadTrace(stagingPath(path))
	if err != nil {
		return err
	}
	serverPath := report.SiblingPath(path, "server")
	_, server, err := report.ReadTrace(stagingPath(serverPath))
	if err != nil {
		return err
	}
	offset, errBound := report.EstimateClockOffset(client)
	h.ClockOffset, h.ClockErr = offset, errBound
	start := h.Start
	if err := report.WriteTrace(path, report.TraceHeader{Side: "c", Host: h.Client.Name, Start: start,
		ClockOffsetNS: offset, ClockErrNS: errBound}, client); err != nil {
		return err
	}
	_ = os.Remove(stagingPath(path))
	if len(server) > 0 {
		if err := report.WriteTrace(serverPath, report.TraceHeader{Side: "s", Host: h.SenderHost.Name, Start: start,
			ClockOffsetNS: offset, ClockErrNS: errBound}, server); err != nil {
			return err
		}
	}
	_ = os.Remove(stagingPath(serverPath))
	return nil
}

// initSenderTrace writes the header of send-tree's --trace file.
func initSenderTrace(path, host string) error {
	return report.WriteTrace(path, report.TraceHeader{Side: "s", Host: host, Start: time.Now().UTC().Format(time.RFC3339Nano)}, nil)
}

// collectTrace reads the complete events of a stopped tx #k plus its rss
// samples, appends them to --trace, and returns them for the fetchable
// runs/server-<tid>.trace.jsonl of each run it served.
func (s *sender) collectTrace(sp *serverProc) ([]report.TraceRecord, error) {
	recs, err := readEvents(sp.eventPath, sp.eventPath+".rss")
	if err != nil {
		return nil, err
	}
	for i := range recs {
		recs[i].Run = "-"
		if recs[i].Side == "" {
			recs[i].Side = "s"
		}
	}
	return recs, report.AppendTrace(s.cfg.flags.trace, recs)
}

func (s *sender) writeServerTrace(sp *serverProc, tid string, recs []report.TraceRecord) error {
	h := report.TraceHeader{Side: "s", Host: hostInfo("").Name, Start: sp.p.started.UTC().Format(time.RFC3339Nano)}
	return report.WriteTrace(s.runPath(serverTraceName(tid)), h, recs)
}

// emitPrepEvent appends a tx-bench prep_* event to send-tree's --trace.
func (s *sender) emitPrepEvent(ev string, fields ...report.KV) {
	if s.cfg.flags.trace == "" {
		return
	}
	rec := report.TraceRecord{T: time.Now().UnixNano(), Side: "s", Run: "-", Ev: ev, Fields: fields}
	if err := report.AppendTrace(s.cfg.flags.trace, []report.TraceRecord{rec}); err != nil {
		fmt.Fprintf(s.cfg.out, "warning: --trace: %v\n", err)
	}
}

func num(v any) json.Number { return json.Number(fmt.Sprint(v)) }
