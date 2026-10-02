package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/txstats"
)

// Version is the tx-bench version recorded in every output.
const Version = "1"

// Coordination files, relative to BENCH_DIR. The receiver reads them, and
// requests preps, with ordinary tx recv get fetches.
const (
	serverJSONName = "server.json"
	runsDir        = "runs"
	prepRequest    = "runs/prep"
	flushRequest   = "runs/flush"
	stopRequest    = "runs/stop" // fetching it stops send-tree for good
	prepJSONName   = "runs/prep.json"
)

func serverRunName(tid string) string { return "runs/server-" + tid + ".json" }
func serverTraceName(tid string) string {
	return "runs/server-" + tid + ".trace.jsonl"
}

// ServerJSON is BENCH_DIR/server.json, rewritten at send-tree startup.
type ServerJSON struct {
	TxBench   string      `json:"tx_bench"`
	Tx        TxBinary    `json:"tx"`
	Host      report.Host `json:"host"`
	Chroot    string      `json:"chroot"`
	Auth      bool        `json:"auth"`
	CacheWarm string      `json:"cache_warm"`
	WarmSkew  float64     `json:"cache_warm_skew"`
	WarmBlock int64       `json:"cache_warm_block"`
	DropMeta  bool        `json:"cache_drop_meta"`
	Trace     bool        `json:"trace"`
	GoTrace   bool        `json:"go_trace"`
	Listen    string      `json:"listen"`
	TxArgs    []string    `json:"tx_args"`
	Started   string      `json:"started"`
	Warnings  []string    `json:"warnings,omitempty"`
}

// PrepJSON is runs/prep.json: the result of the latest prep or flush.
type PrepJSON struct {
	Seq       int                 `json:"seq"`
	ServedTID *string             `json:"served_tid"`
	Kind      string              `json:"kind"` // startup, prep, or flush
	K         int                 `json:"k"`    // the tx send tree now serving
	PID       int                 `json:"pid"`
	Cache     dataset.CacheResult `json:"cache"`
	Changed   []string            `json:"changed,omitempty"`
	T         string              `json:"t"`
}

// ServerRunJSON is runs/server-<tid>.json: the sender side of one run.
type ServerRunJSON struct {
	TID      string               `json:"tid"`
	K        int                  `json:"k"`
	PID      int                  `json:"pid"`
	ExitCode int                  `json:"exit_code"`
	Rusage   report.Rusage        `json:"rusage"`
	Process  txstats.ServerRecord `json:"process"`
	Transfer txstats.ServerRecord `json:"transfer"`
	Overlap  bool                 `json:"overlap,omitempty"`
	Trace    bool                 `json:"trace,omitempty"`
}

// sender converts the file to the report's view of it.
func (s ServerRunJSON) sender() *report.Sender {
	return &report.Sender{
		K: s.K, PID: s.PID, Rusage: s.Rusage,
		ConnsAccepted: s.Process.ConnsAccepted, PeakConns: s.Process.PeakConns, Heartbeats: s.Process.Heartbeats,
		WireBytes: s.Transfer.WireBytes, Windows: s.Transfer.Windows, SendPath: s.Transfer.SendPath,
		Overlap: s.Overlap,
	}
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("parse %s: %w", filepath.Base(path), err)
	}
	return nil
}

// hostInfo describes this machine and the filesystem holding dataRoot.
func hostInfo(dataRoot string) report.Host {
	h := report.Host{CPUs: runtime.NumCPU()}
	h.Name, _ = os.Hostname()
	var u unix.Utsname
	if unix.Uname(&u) == nil {
		h.Kernel = unix.ByteSliceToString(u.Release[:])
	}
	if mi, err := dataset.ReadMemInfo(); err == nil {
		h.MemTotal = mi.Total
	}
	if dataRoot != "" {
		if fs, err := dataset.StatFS(dataRoot); err == nil {
			h.FS, h.Device = fs.Type, fs.Device
		}
	}
	return h
}

// remoteJoin joins a remote directory and a relative coordination path.
func remoteJoin(dir, rel string) string {
	if dir == "/" || dir == "" {
		return "/" + rel
	}
	return strings.TrimRight(dir, "/") + "/" + rel
}
