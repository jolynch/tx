package dataset

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Well-known names under BENCH_DIR.
const (
	BenchJSONName  = "bench.json"
	FilesTSVName   = "files.tsv"
	DataDirName    = "data"
	InCacheName    = "in-cache.tsv"
	BenchMarker    = ".tx-bench"
	DstMarker      = ".tx-bench-dst"
	benchJSONVer   = 1
	markerContents = "tx-bench owns this directory and may delete or regenerate what it created here\n"
)

// PartJSON describes one generated part.
type PartJSON struct {
	Dir      string `json:"dir"`
	Source   string `json:"source"`
	SharePct int    `json:"share_pct"`
	Sizes    string `json:"sizes"`
	Files    int    `json:"files"`
	Bytes    int64  `json:"bytes"`
}

// InJSON describes an imported dataset.
type InJSON struct {
	Path          string         `json:"path"`
	SizeHistogram map[string]int `json:"size_histogram"`
	ZstdRatioEst  float64        `json:"zstd_ratio_est"`
}

// Remote holds paths relative to the tx send tree chroot.
type Remote struct {
	Bench string `json:"bench"`
	Data  string `json:"data"`
}

// BenchJSON is BENCH_DIR/bench.json. Generation fields are null for an
// imported dataset, and In is null for a generated one.
type BenchJSON struct {
	Version     int               `json:"version"`
	Seed        *int64            `json:"seed"`
	SizeSpec    *string           `json:"size_spec"`
	Bytes       int64             `json:"bytes"`
	Files       int               `json:"files"`
	Mix         *string           `json:"mix"`
	Parts       []PartJSON        `json:"parts"`
	Silesia     map[string]string `json:"silesia"`
	In          *InJSON           `json:"in"`
	Remote      Remote            `json:"remote"`
	Fingerprint string            `json:"fingerprint"`
	Created     string            `json:"created"`
}

// Generated reports whether the dataset was generated rather than imported.
func (b *BenchJSON) Generated() bool { return b.In == nil }

// ReadBenchJSON reads BENCH_DIR/bench.json; a missing file returns
// os.ErrNotExist.
func ReadBenchJSON(benchDir string) (*BenchJSON, error) {
	data, err := os.ReadFile(filepath.Join(benchDir, BenchJSONName))
	if err != nil {
		return nil, err
	}
	var b BenchJSON
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("parse bench.json: %w", err)
	}
	return &b, nil
}

// WriteJSONAtomic writes v as indented JSON via temp file + rename.
func WriteJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return WriteFileAtomic(path, append(data, '\n'))
}

// WriteFileAtomic writes data via temp file + fsync + rename.
func WriteFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// ErrUnmarked is returned when a directory tx-bench would manage exists, is
// not empty, and carries no tx-bench marker.
var ErrUnmarked = errors.New("exists, is not empty, and was not created by tx-bench")

// CheckIsDir rejects a path that exists but is not a directory, such as the
// tx-bench binary itself.
func CheckIsDir(dir string) error {
	if st, err := os.Stat(dir); err == nil && !st.IsDir() {
		return fmt.Errorf("%s exists and is not a directory; name another directory", dir)
	}
	return nil
}

// IsMarked reports whether dir carries the named marker.
func IsMarked(dir, marker string) bool {
	_, err := os.Stat(filepath.Join(dir, marker))
	return err == nil
}

// ClaimDir makes dir a tx-bench directory: it creates and marks dir when
// absent or empty, accepts it when already marked, and otherwise refuses with
// ErrUnmarked.
func ClaimDir(dir, marker string) error {
	if err := CheckIsDir(dir); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	case err != nil:
		return err
	case IsMarked(dir, marker):
		return nil
	case len(entries) > 0:
		return fmt.Errorf("%s %w", dir, ErrUnmarked)
	}
	return os.WriteFile(filepath.Join(dir, marker), []byte(markerContents), 0o644)
}
