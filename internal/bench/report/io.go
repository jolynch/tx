package report

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// IsJSONPath reports whether an output path asks for JSON (.json or .jsonl)
// rather than space-separated text.
func IsJSONPath(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json", ".jsonl":
		return true
	}
	return false
}

// WriteMetrics writes m in the format path's extension selects: .json is
// one object, .jsonl one header line, one line per run, and one summary
// line, and anything else space-separated rows under a header comment.
func WriteMetrics(path string, m Metrics) error {
	var buf bytes.Buffer
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		data, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}
		buf.Write(data)
		buf.WriteByte('\n')
	case ".jsonl":
		enc := json.NewEncoder(&buf)
		if err := enc.Encode(map[string]any{"header": m.Header}); err != nil {
			return err
		}
		for _, r := range m.Runs {
			if err := enc.Encode(map[string]any{"run": r}); err != nil {
				return err
			}
		}
		if err := enc.Encode(map[string]any{"summary": m.Summary}); err != nil {
			return err
		}
	default:
		writeText(&buf, m)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadMetrics reads any format WriteMetrics writes and recomputes the
// summary from the runs.
func ReadMetrics(path string) (Metrics, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Metrics{}, err
	}
	var m Metrics
	trimmed := bytes.TrimSpace(data)
	switch {
	case strings.ToLower(filepath.Ext(path)) == ".jsonl":
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 1<<20), 64<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var rec struct {
				Header *Header `json:"header"`
				Run    *Run    `json:"run"`
			}
			if err := json.Unmarshal(line, &rec); err != nil {
				return Metrics{}, fmt.Errorf("%s: %w", path, err)
			}
			if rec.Header != nil {
				m.Header = *rec.Header
			}
			if rec.Run != nil {
				m.Runs = append(m.Runs, *rec.Run)
			}
		}
		if err := sc.Err(); err != nil {
			return Metrics{}, err
		}
	case len(trimmed) > 0 && trimmed[0] == '{':
		if err := json.Unmarshal(data, &m); err != nil {
			return Metrics{}, fmt.Errorf("%s: %w", path, err)
		}
	default:
		if m, err = readText(data); err != nil {
			return Metrics{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if m.Header.Version == 0 {
		return Metrics{}, fmt.Errorf("%s is not a tx-bench metrics file", path)
	}
	m.Summary = Aggregate(m.Runs)
	return m, nil
}

// textColumns are the space-separated columns, in order. Free text (errors,
// stderr) is kept out so every row splits cleanly; the header travels as
// JSON in a comment.
type column struct {
	name string
	get  func(Run) string
	set  func(*Run, string) error
}

var textColumns = []column{
	{"run", func(r Run) string { return r.Label }, func(r *Run, v string) error { r.Label = v; return nil }},
	{"measured", func(r Run) string { return strconv.FormatBool(r.Measured) }, setBool(func(r *Run) *bool { return &r.Measured })},
	{"status", func(r Run) string { return r.Status }, func(r *Run, v string) error { r.Status = v; return nil }},
	{"tid", func(r Run) string { return dash(r.TID) }, func(r *Run, v string) error { r.TID = undash(v); return nil }},
	{"exit", func(r Run) string { return strconv.Itoa(r.ExitCode) }, setInt(func(r *Run) *int { return &r.ExitCode })},
	i64("start_ns", func(r *Run) *int64 { return &r.Start }),
	i64("wall_ns", func(r *Run) *int64 { return &r.WallNS }),
	i64("probe_ns", func(r *Run) *int64 { return &r.ProbeNS }),
	i64("manifest_ns", func(r *Run) *int64 { return &r.ManifestNS }),
	i64("data_ns", func(r *Run) *int64 { return &r.DataNS }),
	i64("finalize_ns", func(r *Run) *int64 { return &r.FinalizeNS }),
	i64("verify_ns", func(r *Run) *int64 { return &r.Verify.DurNS }),
	i64("files", func(r *Run) *int64 { return &r.Files }),
	i64("bytes", func(r *Run) *int64 { return &r.Bytes }),
	i64("logical_bytes", func(r *Run) *int64 { return &r.LogicalBytes }),
	i64("wire_bytes", func(r *Run) *int64 { return &r.WireBytes }),
	i64("dials", func(r *Run) *int64 { return &r.Dials }),
	i64("sync_fallbacks", func(r *Run) *int64 { return &r.SyncFallbacks }),
	i64("reuses", func(r *Run) *int64 { return &r.Reuses }),
	i64("heartbeats", func(r *Run) *int64 { return &r.Heartbeats }),
	i64("heartbeat_failures", func(r *Run) *int64 { return &r.HeartbeatFailures }),
	i64("ack_retries", func(r *Run) *int64 { return &r.AckRetries }),
	i64("request_errors", func(r *Run) *int64 { return &r.RequestErrors }),
	i64("client_user_ns", func(r *Run) *int64 { return &r.Client.UserNS }),
	i64("client_sys_ns", func(r *Run) *int64 { return &r.Client.SysNS }),
	i64("client_rss", func(r *Run) *int64 { return &r.Client.MaxRSS }),
	i64("client_majflt", func(r *Run) *int64 { return &r.Client.MajFlt }),
	i64("client_minflt", func(r *Run) *int64 { return &r.Client.MinFlt }),
	i64("client_inblock", func(r *Run) *int64 { return &r.Client.InBlock }),
	i64("client_oublock", func(r *Run) *int64 { return &r.Client.OuBlock }),
	i64("client_nvcsw", func(r *Run) *int64 { return &r.Client.NVCSw }),
	i64("client_nivcsw", func(r *Run) *int64 { return &r.Client.NIVCSw }),
	senderI64("sender_user_ns", func(s *Sender) *int64 { return &s.Rusage.UserNS }),
	senderI64("sender_sys_ns", func(s *Sender) *int64 { return &s.Rusage.SysNS }),
	senderI64("sender_rss", func(s *Sender) *int64 { return &s.Rusage.MaxRSS }),
	senderI64("sender_conns", func(s *Sender) *int64 { return &s.ConnsAccepted }),
	senderI64("sender_peak_conns", func(s *Sender) *int64 { return &s.PeakConns }),
	{"hot_pct", func(r Run) string { return strconv.FormatFloat(r.Cache.HotPct, 'f', 1, 64) }, func(r *Run, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		r.Cache.HotPct = f
		return err
	}},
	{"meta_cold", func(r Run) string { return strconv.FormatBool(r.Cache.MetaCold) }, setBool(func(r *Run) *bool { return &r.Cache.MetaCold })},
	{"verify_mode", func(r Run) string { return dash(r.Verify.Mode) }, func(r *Run, v string) error { r.Verify.Mode = undash(v); return nil }},
	{"verify_files", func(r Run) string { return strconv.Itoa(r.Verify.Files) }, setInt(func(r *Run) *int { return &r.Verify.Files })},
	{"mismatches", func(r Run) string { return strconv.Itoa(r.Verify.Mismatches) }, setInt(func(r *Run) *int { return &r.Verify.Mismatches })},
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func undash(s string) string {
	if s == "-" {
		return ""
	}
	return s
}

func i64(name string, field func(*Run) *int64) column {
	return column{name,
		func(r Run) string { return strconv.FormatInt(*field(&r), 10) },
		func(r *Run, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			*field(r) = n
			return err
		}}
}

func senderI64(name string, field func(*Sender) *int64) column {
	return column{name, func(r Run) string {
		if r.Sender == nil {
			return "-"
		}
		return strconv.FormatInt(*field(r.Sender), 10)
	}, func(r *Run, v string) error {
		if v == "-" {
			return nil
		}
		if r.Sender == nil {
			r.Sender = &Sender{}
		}
		n, err := strconv.ParseInt(v, 10, 64)
		*field(r.Sender) = n
		return err
	}}
}

func setInt(field func(*Run) *int) func(*Run, string) error {
	return func(r *Run, v string) error {
		n, err := strconv.Atoi(v)
		*field(r) = n
		return err
	}
}

func setBool(field func(*Run) *bool) func(*Run, string) error {
	return func(r *Run, v string) error {
		b, err := strconv.ParseBool(v)
		*field(r) = b
		return err
	}
}

const textHeaderPrefix = "# header "

func writeText(buf *bytes.Buffer, m Metrics) {
	h, _ := json.Marshal(m.Header)
	buf.WriteString("# tx-bench metrics v1\n")
	buf.WriteString(textHeaderPrefix)
	buf.Write(h)
	buf.WriteByte('\n')
	for i, c := range textColumns {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(c.name)
	}
	buf.WriteByte('\n')
	for _, r := range m.Runs {
		for i, c := range textColumns {
			if i > 0 {
				buf.WriteByte(' ')
			}
			buf.WriteString(c.get(r))
		}
		buf.WriteByte('\n')
	}
}

func readText(data []byte) (Metrics, error) {
	var m Metrics
	var cols []string
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, textHeaderPrefix):
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, textHeaderPrefix)), &m.Header); err != nil {
				return Metrics{}, err
			}
		case strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "":
		case cols == nil:
			cols = strings.Fields(line)
		default:
			vals := strings.Fields(line)
			if len(vals) != len(cols) {
				return Metrics{}, fmt.Errorf("row has %d fields, header has %d", len(vals), len(cols))
			}
			var r Run
			for i, name := range cols {
				for _, c := range textColumns {
					if c.name == name {
						if err := c.set(&r, vals[i]); err != nil {
							return Metrics{}, fmt.Errorf("column %s: %w", name, err)
						}
					}
				}
			}
			m.Runs = append(m.Runs, r)
		}
	}
	return m, nil
}
