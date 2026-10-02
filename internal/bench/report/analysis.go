package report

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// File kinds report accepts.
const (
	KindMetrics = "metrics"
	KindTrace   = "trace"
)

// Sniff tells a metrics file from a trace file by its first line.
func Sniff(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "# tx-bench trace"), strings.HasPrefix(line, `{"trace":`):
			return KindTrace, nil
		case strings.HasPrefix(line, "# tx-bench metrics"), strings.HasPrefix(line, `{"header":`), line == "{":
			return KindMetrics, nil
		}
		break
	}
	return "", fmt.Errorf("%s is neither a tx-bench metrics file nor a trace", path)
}

// ParseOffset parses a --clock-offset: a Go duration or integer nanoseconds.
func ParseOffset(raw string) (int64, error) {
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	return int64(d), nil
}

// AnalysisOptions selects what AnalyzeTrace covers.
type AnalysisOptions struct {
	Run         string // one run label; empty for all measured runs
	Top         int
	ClockOffset *int64 // overrides the trace header's estimate
}

// FileBreakdown is one file's time on each side, in nanoseconds.
type FileBreakdown struct {
	Run       string `json:"run"`
	TID       string `json:"tid"`
	File      int64  `json:"file"`
	Path      string `json:"path"`
	Len       int64  `json:"len"`
	Total     int64  `json:"total_ns"`
	Windows   int    `json:"windows"`
	SendRead  int64  `json:"send_read_ns"`
	SendComp  int64  `json:"send_comp_ns"`
	SendWrite int64  `json:"send_write_ns"`
	InFlight  int64  `json:"in_flight_ns"`
	Recv      int64  `json:"recv_ns"`
	Write     int64  `json:"write_ns"`
	Fsync     int64  `json:"fsync_ns"`
	Ack       int64  `json:"ack_ns"`
	HasSender bool   `json:"has_sender"`
}

// Gap is a stretch where one side had no window in progress.
type Gap struct {
	Run   string `json:"run"`
	Side  string `json:"side"`
	Start int64  `json:"start_ns"` // since the run started
	Dur   int64  `json:"dur_ns"`
}

// Bucket is one second of throughput.
type Bucket struct {
	Run        string  `json:"run"`
	Second     int     `json:"second"`
	Goodput    int64   `json:"client_goodput_bytes"`
	SenderWire int64   `json:"sender_wire_bytes"`
	Files      float64 `json:"avg_files_in_flight"`
	Windows    float64 `json:"avg_windows_in_flight"`
}

// Anomaly is one notable event.
type Anomaly struct {
	Run    string `json:"run"`
	At     int64  `json:"at_ns"` // since the run started
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// RunAnalysis is the timeline analysis of one run.
type RunAnalysis struct {
	Run           string          `json:"run"`
	TID           string          `json:"tid"`
	Duration      int64           `json:"dur_ns"`
	Concurrency   int64           `json:"target_concurrency"`
	PeakFiles     int             `json:"peak_files_in_flight"`
	PeakWindows   int             `json:"peak_windows_in_flight"`
	Slowest       []FileBreakdown `json:"slowest_files"`
	Gaps          []Gap           `json:"gaps"`
	Throughput    []Bucket        `json:"throughput"`
	Anomalies     []Anomaly       `json:"anomalies"`
	SlowFirstByte []Anomaly       `json:"largest_first_byte"`
	Joined        int             `json:"windows_joined"`
	ClientWindows int             `json:"client_windows"`
}

// Analysis is the result for one trace file.
type Analysis struct {
	Path          string        `json:"path"`
	ServerPath    string        `json:"server_path,omitempty"`
	ClockOffsetNS int64         `json:"clock_offset_ns"`
	ClockErrNS    int64         `json:"clock_err_ns"`
	Runs          []RunAnalysis `json:"runs"`
}

// gapThreshold is the idle time that counts as a gap.
const gapThreshold = 10 * time.Millisecond

// AnalyzeTrace analyzes a client trace and, when present, its .server
// sibling.
func AnalyzeTrace(path string, opts AnalysisOptions) (Analysis, error) {
	if opts.Top <= 0 {
		opts.Top = 20
	}
	h, client, err := ReadTrace(path)
	if err != nil {
		return Analysis{}, err
	}
	a := Analysis{Path: path, ClockOffsetNS: h.ClockOffsetNS, ClockErrNS: h.ClockErrNS}
	if h.Side == "s" {
		return Analysis{}, fmt.Errorf("%s is a sender trace; pass the client trace (its .server sibling is read automatically)", path)
	}
	if opts.ClockOffset != nil {
		a.ClockOffsetNS = *opts.ClockOffset
	}
	var server []TraceRecord
	if sp := SiblingPath(path, "server"); sp != path {
		if _, err := os.Stat(sp); err == nil {
			if _, server, err = ReadTrace(sp); err != nil {
				return Analysis{}, err
			}
			a.ServerPath = sp
		}
	}
	for i := range server {
		server[i].T += a.ClockOffsetNS
	}
	serverByTID := map[string][]TraceRecord{}
	for _, r := range server {
		serverByTID[r.TID] = append(serverByTID[r.TID], r)
	}

	byRun := map[string][]TraceRecord{}
	var order []string
	for _, r := range client {
		if _, ok := byRun[r.Run]; !ok {
			order = append(order, r.Run)
		}
		byRun[r.Run] = append(byRun[r.Run], r)
	}
	for _, run := range order {
		if opts.Run != "" && run != opts.Run {
			continue
		}
		if opts.Run == "" && strings.HasPrefix(run, "w") {
			continue
		}
		a.Runs = append(a.Runs, analyzeRun(run, byRun[run], serverByTID, opts.Top))
	}
	if opts.Run != "" && len(a.Runs) == 0 {
		return Analysis{}, fmt.Errorf("no run %q in %s", opts.Run, path)
	}
	return a, nil
}

type winKey struct {
	tid  string
	file int64
	off  int64
}

type fileKey struct {
	tid  string
	file int64
}

type span struct{ lo, hi int64 }

func analyzeRun(run string, recs []TraceRecord, serverByTID map[string][]TraceRecord, top int) RunAnalysis {
	ra := RunAnalysis{Run: run}
	if len(recs) == 0 {
		return ra
	}
	start, end := recs[0].T, recs[len(recs)-1].T
	for _, r := range recs {
		start, end = min(start, r.T), max(end, r.T)
		if r.TID != "" && r.TID != "-" && ra.TID == "" {
			ra.TID = r.TID
		}
	}
	ra.Duration = end - start

	files := map[fileKey]*FileBreakdown{}
	file := func(tid string, id int64) *FileBreakdown {
		k := fileKey{tid, id}
		fb := files[k]
		if fb == nil {
			fb = &FileBreakdown{Run: run, TID: tid, File: id}
			files[k] = fb
		}
		return fb
	}
	var clientWins, fileSpans []span
	clientWin := map[winKey]TraceRecord{}
	var acks []TraceRecord
	type firstByte struct {
		Anomaly
		ns int64
	}
	var firstBytes []firstByte
	fileDoneAt := map[fileKey]int64{}
	for _, r := range recs {
		at := r.T - start
		switch r.Ev {
		case "probe":
			if c := r.Int("concurrency"); c > 0 {
				ra.Concurrency = c
			}
		case "file_start":
			fb := file(r.TID, r.Int("file"))
			fb.Path, fb.Len = r.Str("path"), r.Int("len")
		case "file_done":
			fb := file(r.TID, r.Int("file"))
			fb.Total = r.Int("dur")
			fileSpans = append(fileSpans, span{r.T - fb.Total, r.T})
			fileDoneAt[fileKey{r.TID, r.Int("file")}] = r.T
		case "window":
			fb := file(r.TID, r.Int("file"))
			fb.Windows++
			fb.Recv += r.Int("dur") - r.Int("write_dur")
			fb.Write += r.Int("write_dur")
			clientWins = append(clientWins, span{r.T - r.Int("dur"), r.T})
			clientWin[winKey{r.TID, r.Int("file"), r.Int("off")}] = r
			ra.ClientWindows++
			if fbNS := r.Int("first_byte"); fbNS > 0 {
				firstBytes = append(firstBytes, firstByte{Anomaly{Run: run, At: at, Kind: "first_byte",
					Detail: fmt.Sprintf("file %d off %d waited %s for its first byte", r.Int("file"), r.Int("off"), time.Duration(fbNS).Round(time.Microsecond))}, fbNS})
			}
		case "fsync":
			file(r.TID, r.Int("file")).Fsync += r.Int("dur")
		case "ack":
			acks = append(acks, r)
		case "retry":
			ra.Anomalies = append(ra.Anomalies, Anomaly{run, at, "retry", fmt.Sprintf("%s attempt %d: %s", r.Str("verb"), r.Int("attempt"), r.Str("err"))})
		case "heartbeat_fail":
			ra.Anomalies = append(ra.Anomalies, Anomaly{run, at, "heartbeat_fail", fmt.Sprintf("conn %d: %s", r.Int("conn"), r.Str("err"))})
		case "conn_dial":
			if r.Bool("sync") {
				ra.Anomalies = append(ra.Anomalies, Anomaly{run, at, "sync_dial", fmt.Sprintf("conn %d dialed synchronously in %s", r.Int("conn"), time.Duration(r.Int("dur")).Round(time.Microsecond))})
			}
		case "error":
			ra.Anomalies = append(ra.Anomalies, Anomaly{run, at, "error", fmt.Sprintf("%s file %d: %s", r.Str("verb"), r.Int("file"), r.Str("err"))})
		case "dropped":
			ra.Anomalies = append(ra.Anomalies, Anomaly{run, at, "dropped", fmt.Sprintf("%d client events lost", r.Int("n"))})
		case "req_end":
			if e := r.Str("err"); e != "" {
				ra.Anomalies = append(ra.Anomalies, Anomaly{run, at, "request_error", fmt.Sprintf("%s: %s", r.Str("verb"), e)})
			}
		}
	}
	// ACK latency: from a file's completion to the next ACK that ends after it.
	for k, done := range fileDoneAt {
		for _, ack := range acks {
			if ack.TID == k.tid && ack.T >= done {
				file(k.tid, k.file).Ack = ack.T - done
				break
			}
		}
	}

	// Join the sender's windows of this run's transfer.
	var senderWins []span
	sender := serverByTID[ra.TID]
	for _, r := range sender {
		switch r.Ev {
		case "window":
			read, comp, write := r.Int("read_dur"), r.Int("comp_dur"), r.Int("write_dur")
			sStart := r.T - read - comp - write
			senderWins = append(senderWins, span{sStart, r.T})
			fb := file(r.TID, r.Int("file"))
			fb.HasSender = true
			fb.SendRead += read
			fb.SendComp += comp
			fb.SendWrite += write
			if cw, ok := clientWin[winKey{r.TID, r.Int("file"), r.Int("off")}]; ok {
				ra.Joined++
				arrival := cw.T - cw.Int("dur")
				if inFlight := arrival - (r.T - write); inFlight > 0 {
					fb.InFlight += inFlight
				}
			}
		case "dropped":
			ra.Anomalies = append(ra.Anomalies, Anomaly{run, r.T - start, "dropped", fmt.Sprintf("%d sender events lost", r.Int("n"))})
		}
	}

	var list []FileBreakdown
	for _, fb := range files {
		if fb.Windows > 0 || fb.Total > 0 {
			list = append(list, *fb)
		}
	}
	slices.SortFunc(list, func(a, b FileBreakdown) int {
		if c := cmpDesc(a.Total, b.Total); c != 0 {
			return c
		}
		return -cmpDesc(a.File, b.File)
	})
	ra.Slowest = list[:min(top, len(list))]

	ra.Gaps = append(gaps(run, "c", clientWins, start, end), gaps(run, "s", senderWins, start, end)...)
	ra.PeakFiles = peak(fileSpans)
	ra.PeakWindows = peak(clientWins)
	ra.Throughput = throughput(run, recs, sender, start, end, fileSpans, clientWins)
	slices.SortStableFunc(firstBytes, func(a, b firstByte) int { return cmpDesc(a.ns, b.ns) })
	for _, fb := range firstBytes[:min(5, len(firstBytes))] {
		ra.SlowFirstByte = append(ra.SlowFirstByte, fb.Anomaly)
	}
	slices.SortStableFunc(ra.Anomalies, func(a, b Anomaly) int { return -cmpDesc(a.At, b.At) })
	return ra
}

func cmpDesc(a, b int64) int {
	switch {
	case a > b:
		return -1
	case a < b:
		return 1
	}
	return 0
}

// gaps finds idle stretches longer than gapThreshold between the first and
// last span on one side.
func gaps(run, side string, spans []span, runStart, runEnd int64) []Gap {
	if len(spans) == 0 {
		return nil
	}
	s := slices.Clone(spans)
	slices.SortFunc(s, func(a, b span) int { return -cmpDesc(a.lo, b.lo) })
	var out []Gap
	cover := s[0].hi
	for _, sp := range s[1:] {
		if idle := sp.lo - cover; idle > int64(gapThreshold) {
			out = append(out, Gap{Run: run, Side: side, Start: cover - runStart, Dur: idle})
		}
		cover = max(cover, sp.hi)
	}
	return out
}

// peak is the most spans open at once.
func peak(spans []span) int {
	type edge struct {
		t     int64
		delta int
	}
	var edges []edge
	for _, s := range spans {
		edges = append(edges, edge{s.lo, 1}, edge{s.hi, -1})
	}
	slices.SortFunc(edges, func(a, b edge) int {
		if a.t != b.t {
			return -cmpDesc(a.t, b.t)
		}
		return a.delta - b.delta // close before open at the same instant
	})
	cur, best := 0, 0
	for _, e := range edges {
		cur += e.delta
		best = max(best, cur)
	}
	return best
}

func throughput(run string, client, sender []TraceRecord, start, end int64, fileSpans, winSpans []span) []Bucket {
	n := int((end-start)/int64(time.Second)) + 1
	if n > 100000 {
		n = 100000
	}
	b := make([]Bucket, n)
	for i := range b {
		b[i] = Bucket{Run: run, Second: i}
	}
	idx := func(t int64) int {
		i := int((t - start) / int64(time.Second))
		return min(max(i, 0), n-1)
	}
	for _, r := range client {
		if r.Ev == "window" {
			b[idx(r.T)].Goodput += r.Int("len")
		}
	}
	for _, r := range sender {
		if r.Ev == "window" && r.T >= start && r.T <= end {
			b[idx(r.T)].SenderWire += r.Int("wire")
		}
	}
	// Average occupancy per bucket: overlap of each span with the second.
	occupy := func(spans []span, set func(*Bucket, float64)) {
		acc := make([]float64, n)
		for _, s := range spans {
			for i := idx(s.lo); i <= idx(s.hi); i++ {
				lo := max(s.lo, start+int64(i)*int64(time.Second))
				hi := min(s.hi, start+int64(i+1)*int64(time.Second))
				if hi > lo {
					acc[i] += float64(hi-lo) / float64(time.Second)
				}
			}
		}
		for i := range b {
			set(&b[i], acc[i])
		}
	}
	occupy(fileSpans, func(bk *Bucket, v float64) { bk.Files = v })
	occupy(winSpans, func(bk *Bucket, v float64) { bk.Windows = v })
	return b
}

// RenderAnalysis writes the timeline analysis as text.
func RenderAnalysis(w io.Writer, a Analysis) {
	fmt.Fprintf(w, "trace %s", a.Path)
	if a.ServerPath != "" {
		fmt.Fprintf(w, " + %s  clock offset %s ± %s", a.ServerPath, time.Duration(a.ClockOffsetNS).Round(time.Microsecond),
			time.Duration(a.ClockErrNS).Round(time.Microsecond))
	} else {
		fmt.Fprint(w, "  (client side only: no sender trace)")
	}
	fmt.Fprintln(w)
	for _, ra := range a.Runs {
		fmt.Fprintf(w, "\nrun %s  tid=%s  %s  windows %d (joined %d)  peak in flight: files %d, windows %d",
			ra.Run, ra.TID, time.Duration(ra.Duration).Round(time.Millisecond), ra.ClientWindows, ra.Joined, ra.PeakFiles, ra.PeakWindows)
		if ra.Concurrency > 0 {
			fmt.Fprintf(w, " (target %d)", ra.Concurrency)
		}
		fmt.Fprintln(w)
		if len(ra.Slowest) > 0 {
			fmt.Fprintln(w, "  slowest files")
			t := Table{W: w, Indent: "    ", Cols: []Col{
				{Name: "total", Width: 9}, {Name: "s.read", Width: 9}, {Name: "s.comp", Width: 9}, {Name: "s.write", Width: 9},
				{Name: "flight", Width: 9}, {Name: "c.recv", Width: 9}, {Name: "c.write", Width: 9}, {Name: "fsync", Width: 9},
				{Name: "ack", Width: 9}, {Name: "len", Width: 9},
			}}
			t.Row("total", "s.read", "s.comp", "s.write", "flight", "c.recv", "c.write", "fsync", "ack", "len", "path")
			for _, f := range ra.Slowest {
				sd := func(v int64) string {
					if !f.HasSender {
						return ""
					}
					return dur(v)
				}
				t.Row(dur(f.Total), sd(f.SendRead), sd(f.SendComp), sd(f.SendWrite), sd(f.InFlight),
					dur(f.Recv), dur(f.Write), dur(f.Fsync), dur(f.Ack), HumanBytes(f.Len), f.Path)
			}
		}
		if len(ra.Gaps) > 0 {
			fmt.Fprintf(w, "  idle gaps over %s\n", gapThreshold)
			t := Table{W: w, Indent: "    ", Cols: []Col{{Name: "side", Width: 6, Left: true}, {Name: "at", Width: 9}, {Name: "idle", Width: 9}}}
			t.Header()
			for i, g := range ra.Gaps {
				if i == 10 {
					fmt.Fprintf(w, "    ... %d more\n", len(ra.Gaps)-10)
					break
				}
				side := "client"
				if g.Side == "s" {
					side = "sender"
				}
				t.Row(side, "+"+dur(g.Start), dur(g.Dur))
			}
		}
		if len(ra.Throughput) > 0 {
			fmt.Fprintln(w, "  throughput, 1s buckets")
			t := Table{W: w, Indent: "    ", Cols: []Col{
				{Name: "sec", Width: 4}, {Name: "goodput/s", Width: 10}, {Name: "s.wire/s", Width: 10},
				{Name: "files", Width: 7}, {Name: "windows", Width: 7},
			}}
			t.Header()
			for _, bk := range ra.Throughput {
				t.Row(strconv.Itoa(bk.Second), HumanBytes(bk.Goodput), HumanBytes(bk.SenderWire),
					strconv.FormatFloat(bk.Files, 'f', 1, 64), strconv.FormatFloat(bk.Windows, 'f', 1, 64))
			}
		}
		if len(ra.Anomalies)+len(ra.SlowFirstByte) > 0 {
			fmt.Fprintln(w, "  anomalies")
			t := Table{W: w, Indent: "    ", Cols: []Col{{Name: "at", Width: 9}, {Name: "kind", Width: 14, Left: true}}}
			t.Row("at", "kind", "detail")
			for _, an := range append(append([]Anomaly(nil), ra.Anomalies...), ra.SlowFirstByte...) {
				t.Row("+"+dur(an.At), an.Kind, an.Detail)
			}
		}
	}
}

func dur(ns int64) string {
	d := time.Duration(ns)
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(100 * time.Microsecond).String()
	case d > 0:
		return d.Round(time.Microsecond).String()
	}
	return "0"
}
