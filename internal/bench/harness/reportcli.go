package harness

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
)

// RunReportCLI is 'tx-bench report'.
func RunReportCLI(args []string, stdout, stderr io.Writer) int {
	cf := cliflags.New("report")
	cf.SetOutput(stderr)
	var opts report.AnalysisOptions
	var format, clockOffset string
	cf.StringVar(&opts.Run, "r", "run", "", "Trace analysis: only this run (e.g. 1, w1); empty for all measured runs")
	cf.IntVar(&opts.Top, "", "top", 20, "Trace analysis: slowest files to list")
	cf.StringVar(&format, "", "format", "text", "Report format: text|json")
	cf.StringVar(&clockOffset, "", "clock-offset", "", "Trace analysis: override the estimated sender clock offset (e.g. 0, -1.8ms) (default: from trace header)")
	cf.FlagSet().Usage = func() {
		fmt.Fprint(stderr, `usage: tx-bench report [options] FILE [FILE...]

Print the benchmark report from files written by 'tx-bench remote recv-copy'.

  FILE    a recv-copy --metrics file, a recv-copy --trace file, or both

A metrics file re-renders the summary recv-copy printed at the end of its runs.
A trace file adds timeline analysis: per-file breakdowns, concurrency gaps,
throughput over time, and anomalies. Sender events are read from the trace's
.server sibling when present.

`)
		cf.PrintDefaults(stderr)
	}
	if code, done := parseFlags(cf, args); done {
		return code
	}
	if format != "text" && format != "json" {
		return reportErr(stderr, usageErrorf("--format must be text or json"))
	}
	if clockOffset != "" {
		d, err := report.ParseOffset(clockOffset)
		if err != nil {
			return reportErr(stderr, usageErrorf("--clock-offset: %v", err))
		}
		opts.ClockOffset = &d
	}
	files := cf.Args()
	if len(files) == 0 {
		cf.FlagSet().Usage()
		return exitUsage
	}
	var out struct {
		Metrics  []report.Metrics  `json:"metrics,omitempty"`
		Analysis []report.Analysis `json:"analysis,omitempty"`
	}
	for _, path := range files {
		kind, err := report.Sniff(path)
		if err != nil {
			return reportErr(stderr, err)
		}
		switch kind {
		case report.KindMetrics:
			m, err := report.ReadMetrics(path)
			if err != nil {
				return reportErr(stderr, err)
			}
			if format == "text" {
				report.RenderText(stdout, m)
			}
			out.Metrics = append(out.Metrics, m)
		case report.KindTrace:
			a, err := report.AnalyzeTrace(path, opts)
			if err != nil {
				return reportErr(stderr, err)
			}
			if format == "text" {
				report.RenderAnalysis(stdout, a)
			}
			out.Analysis = append(out.Analysis, a)
		}
	}
	if format == "json" {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return reportErr(stderr, err)
		}
	}
	return exitOK
}
