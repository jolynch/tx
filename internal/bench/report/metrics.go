// Package report holds the tx-bench metrics schema, reads and writes metrics
// files, renders the summary recv-copy prints (and tx-bench report
// re-renders), and analyzes traces. See docs/bench/OVERVIEW.md#metrics.
package report

import (
	"math"
	"slices"
)

// Rusage is a process's exact resource usage from wait4.
type Rusage struct {
	UserNS  int64 `json:"user_ns"`
	SysNS   int64 `json:"sys_ns"`
	MaxRSS  int64 `json:"max_rss"` // bytes
	MinFlt  int64 `json:"minflt"`
	MajFlt  int64 `json:"majflt"`
	InBlock int64 `json:"inblock"`
	OuBlock int64 `json:"oublock"`
	NVCSw   int64 `json:"nvcsw"`
	NIVCSw  int64 `json:"nivcsw"`
	WallNS  int64 `json:"wall_ns"`
}

// CPUNS is user plus system time.
func (r Rusage) CPUNS() int64 { return r.UserNS + r.SysNS }

// Cache is the sender cache state a run started from.
type Cache struct {
	Prepped  bool     `json:"prepped"` // a prep was acknowledged; the rest is meaningful
	Warm     string   `json:"warm"`
	Skew     float64  `json:"skew"`
	HotPct   float64  `json:"hot_pct"`
	MetaCold bool     `json:"meta_cold"`
	PrepMS   int64    `json:"prep_ms"`
	Changed  []string `json:"changed,omitempty"` // --in paths changed since import
}

// Sender is the sender side of one run, from runs/server-<tid>.json.
type Sender struct {
	K             int              `json:"k"`
	PID           int              `json:"pid"`
	Rusage        Rusage           `json:"rusage"`
	ConnsAccepted int64            `json:"conns_accepted"`
	PeakConns     int64            `json:"peak_conns"`
	Heartbeats    int64            `json:"heartbeats"`
	WireBytes     int64            `json:"wire_bytes"`
	Windows       map[string]int64 `json:"windows,omitempty"`
	SendPath      map[string]int64 `json:"send_path,omitempty"`
	Overlap       bool             `json:"overlap,omitempty"`
}

// Verify is the oracle's result for one run.
type Verify struct {
	Mode       string   `json:"mode"` // full, names, or skipped
	Entries    int      `json:"entries"`
	Files      int      `json:"files"`
	Bytes      int64    `json:"bytes"`
	Mismatches int      `json:"mismatches"`
	Paths      []string `json:"paths,omitempty"`
	DurNS      int64    `json:"dur_ns"`
}

// Run statuses.
const (
	StatusOK            = "ok"
	StatusFailed        = "failed"
	StatusCorrupt       = "corrupt"
	StatusSourceChanged = "source_changed"
)

// Run is one warmup or measured copy.
type Run struct {
	Label    string `json:"run"`
	Measured bool   `json:"measured"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
	Stderr   string `json:"stderr,omitempty"`
	TID      string `json:"tid,omitempty"`
	ExitCode int    `json:"exit_code"`
	Start    int64  `json:"start"` // unix nanoseconds

	WallNS     int64 `json:"wall_ns"`
	ProbeNS    int64 `json:"probe_ns"`
	ManifestNS int64 `json:"manifest_ns"`
	DataNS     int64 `json:"data_ns"`
	FinalizeNS int64 `json:"finalize_ns"`

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

	Client Rusage  `json:"client"`
	Sender *Sender `json:"sender,omitempty"`
	Cache  Cache   `json:"cache"`
	Verify Verify  `json:"verify"`
}

// OK reports a run that counts toward the statistics.
func (r Run) OK() bool { return r.Status == StatusOK }

// RateBps is logical bytes per second of wall time.
func (r Run) RateBps() float64 {
	if r.WallNS <= 0 {
		return 0
	}
	return float64(r.Bytes) / (float64(r.WallNS) / 1e9)
}

// Dataset describes what was copied.
type Dataset struct {
	Fingerprint string  `json:"fingerprint"`
	Files       int     `json:"files"`
	Bytes       int64   `json:"bytes"`
	Shape       string  `json:"shape"` // profile=..., mix=..., or in=PATH
	ZstdRatio   float64 `json:"zstd_ratio_est,omitempty"`
}

// Host is one side's machine facts.
type Host struct {
	Name     string `json:"hostname"`
	Kernel   string `json:"kernel"`
	CPUs     int    `json:"cpus"`
	MemTotal int64  `json:"mem_total"`
	FS       string `json:"fs,omitempty"`
	Device   string `json:"device,omitempty"`
}

// Header is everything a report needs besides the runs.
type Header struct {
	Version     int      `json:"version"`
	TxBench     string   `json:"tx_bench"`
	Start       string   `json:"start"`
	Server      string   `json:"server"`
	TxPath      string   `json:"tx_path"`
	TxHash      string   `json:"tx_hash"`
	SenderTx    string   `json:"sender_tx_hash"`
	TxArgs      []string `json:"tx_args"`
	SenderArgs  []string `json:"sender_tx_args"`
	Baseline    string   `json:"baseline,omitempty"`
	Compress    string   `json:"compress"`
	Encrypt     string   `json:"encrypt"`
	Oracle      string   `json:"oracle"`
	Dataset     Dataset  `json:"dataset"`
	Client      Host     `json:"client_host"`
	SenderHost  Host     `json:"sender_host"`
	SenderTrace bool     `json:"sender_trace"`
	ClockOffset int64    `json:"clock_offset_ns,omitempty"`
	ClockErr    int64    `json:"clock_err_ns,omitempty"`
}

// Stat summarizes one metric over the measured runs.
type Stat struct {
	Min    float64 `json:"min"`
	P50    float64 `json:"p50"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Stddev float64 `json:"stddev"`
	N      int     `json:"n"`
}

// Summarize computes a Stat; p50 is the lower median.
func Summarize(vals []float64) Stat {
	if len(vals) == 0 {
		return Stat{}
	}
	s := slices.Clone(vals)
	slices.Sort(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	mean := sum / float64(len(s))
	var ss float64
	for _, v := range s {
		ss += (v - mean) * (v - mean)
	}
	sd := 0.0
	if len(s) > 1 {
		sd = math.Sqrt(ss / float64(len(s)-1))
	}
	return Stat{Min: s[0], P50: s[(len(s)-1)/2], Max: s[len(s)-1], Mean: mean, Stddev: sd, N: len(s)}
}

// Summary is the aggregate of the measured, successful runs.
type Summary struct {
	Measured   int             `json:"measured"`
	Failed     int             `json:"failed"`
	Stats      map[string]Stat `json:"stats"`
	SenderRuns int             `json:"sender_runs"`
}

// metricFns lists the per-run values summarized, by name.
var metricFns = []struct {
	name string
	fn   func(Run) (float64, bool)
}{
	{"wall_s", func(r Run) (float64, bool) { return float64(r.WallNS) / 1e9, true }},
	{"probe_s", func(r Run) (float64, bool) { return float64(r.ProbeNS) / 1e9, true }},
	{"manifest_s", func(r Run) (float64, bool) { return float64(r.ManifestNS) / 1e9, true }},
	{"data_s", func(r Run) (float64, bool) { return float64(r.DataNS) / 1e9, true }},
	{"finalize_s", func(r Run) (float64, bool) { return float64(r.FinalizeNS) / 1e9, true }},
	{"verify_s", func(r Run) (float64, bool) { return float64(r.Verify.DurNS) / 1e9, r.Verify.Mode != "skipped" }},
	{"rate_bps", func(r Run) (float64, bool) { return r.RateBps(), true }},
	{"files_per_s", func(r Run) (float64, bool) {
		return float64(r.Files) / math.Max(float64(r.WallNS)/1e9, 1e-9), true
	}},
	{"wire_bps", func(r Run) (float64, bool) {
		return float64(r.WireBytes) / math.Max(float64(r.WallNS)/1e9, 1e-9), true
	}},
	{"compress_ratio", func(r Run) (float64, bool) {
		if r.WireBytes == 0 {
			return 0, false
		}
		return float64(r.LogicalBytes) / float64(r.WireBytes), true
	}},
	{"client_rss", func(r Run) (float64, bool) { return float64(r.Client.MaxRSS), true }},
	{"client_cpu_s_per_gib", func(r Run) (float64, bool) { return cpuPerGiB(r.Client, r.Bytes) }},
	{"client_majflt", func(r Run) (float64, bool) { return float64(r.Client.MajFlt), true }},
	{"client_minflt", func(r Run) (float64, bool) { return float64(r.Client.MinFlt), true }},
	{"client_inblock", func(r Run) (float64, bool) { return float64(r.Client.InBlock), true }},
	{"client_oublock", func(r Run) (float64, bool) { return float64(r.Client.OuBlock), true }},
	{"client_ctxsw", func(r Run) (float64, bool) { return float64(r.Client.NVCSw + r.Client.NIVCSw), true }},
	{"sender_rss", func(r Run) (float64, bool) {
		if r.Sender == nil {
			return 0, false
		}
		return float64(r.Sender.Rusage.MaxRSS), true
	}},
	{"sender_cpu_s_per_gib", func(r Run) (float64, bool) {
		if r.Sender == nil {
			return 0, false
		}
		return cpuPerGiB(r.Sender.Rusage, r.Bytes)
	}},
	{"sender_conns", func(r Run) (float64, bool) {
		if r.Sender == nil {
			return 0, false
		}
		return float64(r.Sender.ConnsAccepted), true
	}},
	{"sender_peak_conns", func(r Run) (float64, bool) {
		if r.Sender == nil {
			return 0, false
		}
		return float64(r.Sender.PeakConns), true
	}},
	{"hot_pct", func(r Run) (float64, bool) { return r.Cache.HotPct, true }},
	{"dials", func(r Run) (float64, bool) { return float64(r.Dials), true }},
	{"reuses", func(r Run) (float64, bool) { return float64(r.Reuses), true }},
	{"sync_fallbacks", func(r Run) (float64, bool) { return float64(r.SyncFallbacks), true }},
	{"heartbeats", func(r Run) (float64, bool) { return float64(r.Heartbeats), true }},
	{"heartbeat_failures", func(r Run) (float64, bool) { return float64(r.HeartbeatFailures), true }},
	{"ack_retries", func(r Run) (float64, bool) { return float64(r.AckRetries), true }},
	{"request_errors", func(r Run) (float64, bool) { return float64(r.RequestErrors), true }},
}

func cpuPerGiB(ru Rusage, bytes int64) (float64, bool) {
	if bytes <= 0 {
		return 0, false
	}
	return float64(ru.CPUNS()) / 1e9 / (float64(bytes) / (1 << 30)), true
}

// Aggregate summarizes the measured runs that succeeded.
func Aggregate(runs []Run) Summary {
	s := Summary{Stats: map[string]Stat{}}
	var ok []Run
	for _, r := range runs {
		if !r.Measured {
			continue
		}
		s.Measured++
		if !r.OK() {
			s.Failed++
			continue
		}
		ok = append(ok, r)
		if r.Sender != nil {
			s.SenderRuns++
		}
	}
	for _, m := range metricFns {
		var vals []float64
		for _, r := range ok {
			if v, has := m.fn(r); has {
				vals = append(vals, v)
			}
		}
		if len(vals) > 0 {
			s.Stats[m.name] = Summarize(vals)
		}
	}
	return s
}

// Metrics is a whole metrics file.
type Metrics struct {
	Header  Header  `json:"header"`
	Runs    []Run   `json:"runs"`
	Summary Summary `json:"summary"`
}
