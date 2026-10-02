package report

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// RenderText writes the summary recv-copy prints after its runs: a short
// labeled header, one fixed-width table of every metric (min, p50, and max in
// one unit per row), then failures and notes.
func RenderText(w io.Writer, m Metrics) {
	h, s := m.Header, m.Summary
	label := func(name, value string) { fmt.Fprintf(w, "  %-9s %s\n", name, value) }

	fmt.Fprintln(w, "tx-bench report")
	ds := fmt.Sprintf("%s  %s files  %s  fingerprint %s", HumanBytes(h.Dataset.Bytes), Commas(int64(h.Dataset.Files)), h.Dataset.Shape, h.Dataset.Fingerprint)
	label("dataset", ds)
	client := fmt.Sprintf("compress=%s  encrypt=%s  oracle=%s", h.Compress, h.Encrypt, h.Oracle)
	if h.Baseline != "" && h.Baseline != "none" {
		client = "baseline=" + h.Baseline + "  " + client
	}
	if len(h.TxArgs) > 0 {
		client += "  args: " + strings.Join(h.TxArgs, " ")
	}
	label("client", client)
	sender := "cache " + cacheState(m.Runs)
	if len(h.SenderArgs) > 0 {
		sender += "  args: " + strings.Join(h.SenderArgs, " ")
	}
	if note := senderPathNote(m.Runs); note != "" {
		sender += "  " + note
	}
	label("sender", sender)
	warmups := len(m.Runs) - s.Measured
	label("runs", fmt.Sprintf("%d measured, %d failed, %d warmup", s.Measured, s.Failed, warmups))
	label("verify", verifyLine(m))

	if len(s.Stats) > 0 {
		fmt.Fprintln(w)
		t := Table{W: w, Indent: "  ", Cols: []Col{
			{Name: "metric", Width: 18, Left: true},
			{Name: "min", Width: 10}, {Name: "p50", Width: 10}, {Name: "max", Width: 10},
			{Name: "unit", Width: 10, Left: true},
		}}
		t.Header()
		for i, group := range metricRows {
			if i > 0 {
				fmt.Fprintln(w)
			}
			for _, r := range group {
				st, ok := s.Stats[r.stat]
				if !ok {
					t.Row(r.label, "", "", "", "")
					continue
				}
				f, unit := scaleFor(r.kind, st.P50)
				t.Row(r.label, f(st.Min), f(st.P50), f(st.Max), unit)
			}
		}
	}
	renderFailures(w, m.Runs)
	if s.SenderRuns > 0 {
		fmt.Fprintln(w, "\n  note: sender rusage covers each run's whole tx send tree process, including startup and a few KiB of coordination fetches")
	}
}

type metricRow struct{ label, stat, kind string }

// Metric kinds, which pick how a row is scaled.
const (
	kindDur   = "dur" // seconds
	kindBytes = "bytes"
	kindRate  = "rate" // bytes per second
	kindCount = "count"
	kindRatio = "ratio"
	kindPct   = "pct"
	kindCPU   = "cpu" // core-seconds per GiB
)

var metricRows = [][]metricRow{
	{
		{"wall", "wall_s", kindDur},
		{"  probe", "probe_s", kindDur},
		{"  manifest", "manifest_s", kindDur},
		{"  data", "data_s", kindDur},
		{"  finalize", "finalize_s", kindDur},
		{"verify (oracle)", "verify_s", kindDur},
	},
	{
		{"rate", "rate_bps", kindRate},
		{"wire rate", "wire_bps", kindRate},
		{"files/s", "files_per_s", kindCount},
		{"compress ratio", "compress_ratio", kindRatio},
		{"sender hot", "hot_pct", kindPct},
	},
	{
		{"client rss", "client_rss", kindBytes},
		{"client cpu", "client_cpu_s_per_gib", kindCPU},
		{"client majflt", "client_majflt", kindCount},
		{"client minflt", "client_minflt", kindCount},
		{"client blk in", "client_inblock", kindCount},
		{"client blk out", "client_oublock", kindCount},
		{"client ctxsw", "client_ctxsw", kindCount},
	},
	{
		{"sender rss", "sender_rss", kindBytes},
		{"sender cpu", "sender_cpu_s_per_gib", kindCPU},
		{"sender conns", "sender_conns", kindCount},
		{"sender peak conns", "sender_peak_conns", kindCount},
	},
	{
		{"dials", "dials", kindCount},
		{"reuses", "reuses", kindCount},
		{"sync fallbacks", "sync_fallbacks", kindCount},
		{"heartbeats", "heartbeats", kindCount},
		{"heartbeat fails", "heartbeat_failures", kindCount},
		{"ack retries", "ack_retries", kindCount},
		{"request errors", "request_errors", kindCount},
	},
}

// scaleFor picks one unit for a row from its p50, so min, p50, and max are
// directly comparable.
func scaleFor(kind string, p50 float64) (func(float64) string, string) {
	fixed := func(div float64, decimals int) func(float64) string {
		return func(v float64) string { return strconv.FormatFloat(v/div, 'f', decimals, 64) }
	}
	switch kind {
	case kindDur:
		switch {
		case p50 >= 1:
			return fixed(1, 2), "s"
		case p50 >= 1e-3:
			return fixed(1e-3, 1), "ms"
		}
		return fixed(1e-6, 0), "µs"
	case kindBytes, kindRate:
		units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
		i, div := 0, 1.0
		for i < len(units)-1 && p50/div >= 1024 {
			div *= 1024
			i++
		}
		unit := units[i]
		if kind == kindRate {
			unit += "/s"
		}
		decimals := 1
		if i == 0 {
			decimals = 0
		}
		return fixed(div, decimals), unit
	case kindRatio:
		return fixed(1, 2), "x"
	case kindPct:
		return fixed(1, 1), "%"
	case kindCPU:
		return fixed(1, 2), "core-s/GiB"
	}
	return func(v float64) string { return Commas(int64(math.Round(v))) }, "count"
}

// cacheState describes the sender's page cache at the start of the
// measured runs.
func cacheState(runs []Run) string {
	var warm string
	meta := false
	for _, r := range runs {
		if r.Measured {
			warm, meta = r.Cache.Warm, r.Cache.MetaCold
		}
	}
	switch {
	case warm == "0%" && meta:
		return "cold"
	case warm == "0%":
		return "data-cold (dentries and inodes stay cached)"
	case warm != "":
		return "warmed to " + warm
	}
	return "left as found"
}

func senderPathNote(runs []Run) string {
	var sendfile, buffered int64
	overlap := false
	for _, r := range runs {
		if !r.Measured || r.Sender == nil {
			continue
		}
		sendfile += r.Sender.SendPath["sendfile"]
		buffered += r.Sender.SendPath["buffered"]
		overlap = overlap || r.Sender.Overlap
	}
	note := ""
	if sendfile+buffered > 0 {
		note = fmt.Sprintf("sendfile %.0f%% of windows", 100*float64(sendfile)/float64(sendfile+buffered))
	}
	if overlap {
		note += "  OVERLAPPING TRANSFERS"
	}
	return note
}

func verifyLine(m Metrics) string {
	mode := ""
	var files, mism int
	for _, r := range m.Runs {
		if r.Measured && r.OK() {
			mode, files, mism = r.Verify.Mode, r.Verify.Files, r.Verify.Mismatches
		}
	}
	switch {
	case mode == "skipped":
		return "skipped (--skip-write)"
	case mode == "":
		return "no successful runs"
	}
	label := "fingerprint ok"
	if mode == "names" {
		label += ", names only"
	}
	if mism > 0 {
		label = fmt.Sprintf("%d mismatches", mism)
	}
	return fmt.Sprintf("%s/%s files, %s", Commas(int64(files)), Commas(int64(m.Header.Dataset.Files)), label)
}

// renderFailures lists every run that did not succeed, one row each, with
// its error at the end of the row.
func renderFailures(w io.Writer, runs []Run) {
	var failed []Run
	for _, r := range runs {
		if !r.OK() {
			failed = append(failed, r)
		}
	}
	if len(failed) == 0 {
		return
	}
	fmt.Fprintln(w)
	t := Table{W: w, Indent: "  ", Cols: []Col{
		{Name: "run", Width: 4, Left: true}, {Name: "status", Width: 14, Left: true}, {Name: "exit", Width: 4},
	}}
	t.Header()
	for _, r := range failed {
		msg := r.Error
		if msg == "" {
			msg = "exit code " + strconv.Itoa(r.ExitCode)
		}
		t.Row(r.Label, r.Status, strconv.Itoa(r.ExitCode))
		fmt.Fprintf(w, "        error: %s\n", msg)
	}
}

// HumanBytes is a compact one-decimal size: 4.0GiB, 512B.
func HumanBytes(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	v := float64(n)
	i := 0
	for i < len(units)-1 && math.Abs(v) >= 1024 {
		v /= 1024
		i++
	}
	if i == 0 {
		return strconv.FormatInt(n, 10) + "B"
	}
	return strconv.FormatFloat(v, 'f', 1, 64) + units[i]
}

// Commas groups digits: 52436 is "52,436".
func Commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
