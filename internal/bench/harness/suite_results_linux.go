//go:build linux

package harness

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

type suiteSample map[string]float64
type suiteSamples map[string][]suiteSample

type suiteCopyDetails struct {
	Fingerprint string           `json:"fingerprint"`
	Files       int              `json:"files"`
	Bytes       int64            `json:"bytes"`
	HotPercent  float64          `json:"hot_percent"`
	Windows     map[string]int64 `json:"windows,omitempty"`
}

// readSuiteBenchmarks reads the samples produced by a successful go test.
// A series with no samples is missing from the tested commit. Every other
// series must have all its repetitions and report its chart metrics.
func readSuiteBenchmarks(path string, c suiteCase) (suiteSamples, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	required := make(map[string]bool)
	for _, chart := range c.Charts {
		for metric := range chart.Metrics {
			required[metric] = true
		}
	}
	samples := make(suiteSamples, len(c.Series))
	for _, name := range c.Series {
		samples[name] = nil
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 || len(fields)%2 != 0 {
			return nil, nil, fmt.Errorf("malformed benchmark result %q", line)
		}
		iterations, err := strconv.Atoi(fields[1])
		if err != nil || iterations < 1 {
			return nil, nil, fmt.Errorf("invalid benchmark iteration count in %q", line)
		}
		name := fields[0]
		repeats, expected := samples[name]
		if !expected || len(repeats) == c.Repeats {
			return nil, nil, fmt.Errorf("unexpected benchmark result %s", name)
		}
		all, err := parseSuiteMetrics(fields[2:])
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", name, err)
		}
		sample := make(suiteSample, len(required))
		for metric := range required {
			value, ok := all[metric]
			if !ok {
				return nil, nil, fmt.Errorf("%s lacks the %s metric", name, metric)
			}
			sample[metric] = value
		}
		samples[name] = append(repeats, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	var missing []string
	for _, name := range c.Series {
		switch n := len(samples[name]); n {
		case 0:
			delete(samples, name)
			missing = append(missing, name)
		case c.Repeats:
		default:
			return nil, nil, fmt.Errorf("%s is missing %d of %d samples", name, c.Repeats-n, c.Repeats)
		}
	}
	return samples, missing, nil
}

func parseSuiteMetrics(fields []string) (suiteSample, error) {
	sample := make(suiteSample, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		value, err := strconv.ParseFloat(fields[i], 64)
		unit := fields[i+1]
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return nil, fmt.Errorf("invalid %s value %q", unit, fields[i])
		}
		if unit == "MB/s" {
			unit, value = "MiB/s", value*(1e6/suiteMiB)
		}
		if _, seen := sample[unit]; seen {
			return nil, fmt.Errorf("duplicate metric %s", unit)
		}
		sample[unit] = value
	}
	return sample, nil
}

// readSuiteGrid validates all configured rows.
func readSuiteGrid(path string, c suiteCase) (suiteSamples, bool, error) {
	var grid struct {
		CopyBytes int64 `json:"copy_bytes"`
		Pass      *bool `json:"pass"`
		Rows      []struct {
			Encrypt json.RawMessage `json:"encrypt"`
			CPUs    int             `json:"cpus"`
			Sizes   []struct {
				FileSize      int64    `json:"file_size"`
				MiBPerSec     *float64 `json:"mib_per_sec"`
				CoreSecPerGiB *float64 `json:"core_sec_per_gib"`
				CostRatio     *float64 `json:"cost_ratio"`
			} `json:"sizes"`
		} `json:"rows"`
	}
	if err := readJSON(path, &grid); err != nil {
		return nil, false, fmt.Errorf("read throughput grid: %w", err)
	}
	wantBytes, err := encoding.ParseByteSize(c.gridSize)
	if err != nil || grid.CopyBytes != wantBytes || grid.Pass == nil {
		return nil, false, fmt.Errorf("unexpected throughput grid size or missing result")
	}
	expected := make(map[string]bool)
	for _, mode := range strings.Split(c.gridModes, ",") {
		for _, cpus := range strings.Split(c.gridCPUs, ",") {
			for _, size := range []string{"4KiB", "16KiB", "64KiB", "1MiB", "16MiB"} {
				expected[mode+"/"+cpus+"cpu/"+size] = true
			}
		}
	}
	samples := make(suiteSamples)
	for _, row := range grid.Rows {
		var mode string
		if json.Unmarshal(row.Encrypt, &mode) != nil || mode == "" {
			return nil, false, fmt.Errorf("invalid encryption mode in throughput grid")
		}
		for _, sz := range row.Sizes {
			name := fmt.Sprintf("%s/%dcpu/%s", mode, row.CPUs, dataset.FormatSize(sz.FileSize))
			if !expected[name] || samples[name] != nil {
				return nil, false, fmt.Errorf("unexpected throughput grid sample %s", name)
			}
			if sz.MiBPerSec == nil || sz.CoreSecPerGiB == nil || sz.CostRatio == nil ||
				*sz.MiBPerSec < 0 || *sz.CoreSecPerGiB < 0 || *sz.CostRatio < 0 {
				return nil, false, fmt.Errorf("incomplete throughput grid sample %s", name)
			}
			sample := suiteSample{"MiB/s": *sz.MiBPerSec, "core-s/GiB": *sz.CoreSecPerGiB}
			if sz.FileSize == 4*1024 {
				sample["ratio"] = *sz.CostRatio
			}
			samples[name] = []suiteSample{sample}
		}
	}
	for name := range expected {
		if samples[name] == nil {
			return nil, false, fmt.Errorf("missing throughput grid sample %s", name)
		}
	}
	return samples, *grid.Pass, nil
}
