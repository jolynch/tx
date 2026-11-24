//go:build linux

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
)

func TestSuiteBenchmarkSamples(t *testing.T) {
	const name = "BenchmarkVerifyInMemory/4KiB/cpus=8/c=4-8"
	const other = "BenchmarkVerifyInMemory/1MiB/cpus=8/c=4-8"
	const sample = name + "  1  42 ns/op  1048.576 MB/s\n"
	const otherSample = other + "  1  23 ns/op  1048.576 MB/s\n"
	for _, tc := range []struct {
		label   string
		log     string
		missing []string
		err     string
	}{
		{"complete", "goos: linux\n" + strings.Repeat(sample, 3) + strings.Repeat(otherSample, 3) + "PASS\n", nil, ""},
		{"not in this commit", "PASS\n", []string{name, other}, ""},
		{"one series not in this commit", sample + sample + sample, []string{other}, ""},
		{"missing repetition", sample + sample, nil, "missing 1 of 3 samples"},
		{"wrong CPU", strings.ReplaceAll(sample, "-8", "-2"), nil, "unexpected benchmark result"},
		{"extra repetition", strings.Repeat(sample, 4), nil, "unexpected benchmark result"},
		{"missing metric", name + "  1  42 ns/op\n", nil, "lacks the MiB/s metric"},
		{"nonfinite metric", strings.Replace(sample, "42", "NaN", 1), nil, "invalid ns/op value"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "case.log")
			if err := os.WriteFile(log, []byte(tc.log), 0o600); err != nil {
				t.Fatal(err)
			}
			c := suiteCase{Series: []string{name, other}, Repeats: 3,
				Charts: []suiteChart{chart("verification", "Verification", "ns/op", "MiB/s")}}
			samples, missing, err := readSuiteBenchmarks(log, c)
			if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
				t.Fatalf("error = %v; want %q", err, tc.err)
			}
			if tc.err == "" && !slices.Equal(missing, tc.missing) {
				t.Fatalf("missing = %q; want %q", missing, tc.missing)
			}
			if err == nil && len(samples[name]) != 0 &&
				(len(samples[name]) != 3 || samples[name][0]["ns/op"] != 42 || samples[name][0]["MiB/s"] != 1000) {
				t.Fatalf("unexpected samples: %v", samples)
			}
		})
	}
}

func TestSuiteGridSamples(t *testing.T) {
	c := suiteCase{gridSize: "16MiB", gridCPUs: "2", gridModes: "none,aes"}
	var sizes []string
	for _, size := range []int64{4 << 10, 16 << 10, 64 << 10, 1 << 20, 16 << 20} {
		sizes = append(sizes, fmt.Sprintf(`{"file_size":%d,"mib_per_sec":12,"core_sec_per_gib":2,"cost_ratio":1}`, size))
	}
	for _, tc := range []struct {
		name    string
		modes   []string // empty omits the encrypt key
		samples int      // zero expects a validation error
	}{
		{"modern", []string{`"none"`, `"aes"`}, 10},
		{"no encrypt key", []string{""}, 0},
		{"missing encrypted row", []string{`"none"`}, 0},
		{"mixed raw formats", []string{"", `"aes"`}, 0},
		{"null encrypt", []string{"null"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rows []string
			for _, mode := range tc.modes {
				var encrypt string
				if mode != "" {
					encrypt = `,"encrypt":` + mode
				}
				rows = append(rows, fmt.Sprintf(`{"cpus":2%s,"sizes":[%s]}`, encrypt, strings.Join(sizes, ",")))
			}
			raw := fmt.Sprintf(`{"pass":true,"copy_bytes":16777216,"rows":[%s]}`, strings.Join(rows, ","))
			path := filepath.Join(t.TempDir(), "throughput.json")
			if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			samples, pass, err := readSuiteGrid(path, c)
			if tc.samples == 0 && err == nil || tc.samples > 0 && (err != nil || !pass || len(samples) != tc.samples) {
				t.Fatalf("got %d samples (pass=%v, error=%v), want %d samples or a validation error", len(samples), pass, err, tc.samples)
			}
		})
	}
}

func TestSuiteWorkspaceOwnership(t *testing.T) {
	out := t.TempDir()
	t.Run("unmarked workspace", func(t *testing.T) {
		root := t.TempDir()
		backup := filepath.Join(root, "smoke", "sources", ".prep-backup")
		if err := os.MkdirAll(backup, 0o700); err != nil {
			t.Fatal(err)
		}
		if w, err := openSmokeWorkspace(root, out); !errors.Is(err, dataset.ErrUnmarked) {
			if w != nil {
				w.close()
			}
			t.Fatalf("error = %v; want ErrUnmarked", err)
		}
		if _, err := os.Stat(backup); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("lock and stale directories", func(t *testing.T) {
		root := t.TempDir()
		first, err := openSmokeWorkspace(root, out)
		if err != nil {
			t.Fatal(err)
		}
		defer first.close()
		staging, err := os.MkdirTemp(first.sources, suitePrepPrefix+"ssd-small-")
		if err != nil {
			t.Fatal(err)
		}
		if w, err := openSmokeWorkspace(root, out); err == nil {
			w.close()
			t.Fatal("second suite acquired the lock")
		}
		if w, err := openSmokeWorkspace(first.root, out); err == nil {
			w.close()
			t.Fatal("second suite acquired the nested workspace lock")
		}
		if _, err := os.Stat(staging); err != nil {
			t.Fatal(err)
		}
		first.close()
		next, err := openSmokeWorkspace(root, out)
		if err != nil {
			t.Fatal(err)
		}
		defer next.close()
		if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale staging directory: stat error = %v", err)
		}
	})
	t.Run("symlinked cache", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "smoke"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(root, "smoke", "sources")); err != nil {
			t.Fatal(err)
		}
		if w, err := openSmokeWorkspace(root, out); err == nil {
			w.close()
			t.Fatal("suite accepted a symlinked cache directory")
		}
	})
}

func openSmokeWorkspace(root, out string) (*suiteWorkspace, error) {
	return openSuiteWorkspace(suiteWorkspaceOptions{Root: root, Out: out, Profile: "smoke"})
}

func TestSuiteResultsFormat(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "bench", "suite-results-format"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != strconv.Itoa(suiteResultsFormat) {
		t.Fatalf("bench/suite-results-format is %q, want %d", got, suiteResultsFormat)
	}
}

func TestSuiteChartDirections(t *testing.T) {
	for _, profile := range []string{"smoke", "full"} {
		for _, c := range suiteSettings(profile).cases() {
			if len(c.Charts) == 0 {
				t.Errorf("%s %s has no charts", profile, c.ID)
			}
			for _, chart := range c.Charts {
				for metric, better := range chart.Metrics {
					if better != "inc" && better != "dec" {
						t.Errorf("%s %s chart %s: metric %s has direction %q, want inc or dec",
							profile, c.ID, chart.ID, metric, better)
					}
				}
			}
		}
	}
}

func TestSuiteCaseIdentity(t *testing.T) {
	for _, profile := range []string{"smoke", "full"} {
		for _, c := range suiteSettings(profile).cases() {
			if len(c.Identity) == 0 || c.Revision < 1 {
				t.Fatalf("%s %s lacks an identity or revision", profile, c.ID)
			}
			// A case's identity must not change when a series is added, so it
			// cannot name any series or the -bench filter that selects them.
			raw, err := json.Marshal(c.Identity)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range c.Series {
				if strings.Contains(string(raw), strings.TrimSuffix(name, "-"+strconv.Itoa(suiteSettings(profile).CPUs))) {
					t.Errorf("%s %s identity names series %s", profile, c.ID, name)
				}
			}
			if strings.Contains(string(raw), "-bench") {
				t.Errorf("%s %s identity includes the -bench filter", profile, c.ID)
			}
		}
	}
}

func TestSuiteSSDWorkloads(t *testing.T) {
	for _, profile := range []string{"smoke", "full"} {
		cases := suiteSettings(profile).SSD
		if len(cases) != 4 {
			t.Fatalf("%s defines %d SSD workloads, want four", profile, len(cases))
		}
		if err := checkSuiteIDs(cases); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, c := range cases {
			if c.Repeats != 3 {
				t.Fatalf("%s %s has %d repeats", profile, c.ID, c.Repeats)
			}
			total += c.Bytes * int64(c.Repeats)
			mix, err := dataset.ParseMix(c.Recipe.Mix)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := dataset.Plan(mix, c.Bytes, c.Recipe.Seed)
			if err != nil {
				t.Fatal(err)
			}
			var compressible int64
			sizes := make(map[int64]bool)
			for _, p := range plan {
				if p.Part.Source.Kind != dataset.SourceSilesia {
					continue
				}
				compressible += p.Budget
				for _, size := range p.Sizes {
					if size <= 100*suiteMiB || size > suiteGiB {
						t.Errorf("%s %s has a Silesia file outside the target range: %d bytes", profile, c.ID, size)
					}
					sizes[size] = true
				}
			}
			want := map[string]int64{"ssd-mixed": c.Bytes / 2, "ssd-compressible": c.Bytes}[c.ID]
			if compressible != want || want > 0 && len(sizes) < 2 {
				t.Errorf("%s %s has %d compressible bytes and %d distinct compressible sizes, want %d bytes",
					profile, c.ID, compressible, len(sizes), want)
			}
		}
		if profile == "full" && total != 258*suiteGiB {
			t.Errorf("full suite copies %d bytes, want 258 GiB", total)
		}
	}
}

func TestSuiteCopyResidency(t *testing.T) {
	c := suiteSettings("smoke").SSD[0]
	reference := &dataset.BenchJSON{Fingerprint: "f", Files: 1024, Bytes: c.Bytes}
	for _, tc := range []struct {
		name           string
		hot            float64
		missingWindows bool
		err            string
		setup          bool
	}{
		{"cold", 0, false, "", false},
		{"maximum residency", suiteMaxHotPct, false, "", false},
		{"hot", 37.5, false, "source was 37.5% cached before copy 3", true},
		{"fully resident", 100, false, "source was 100.0% cached before copy 3", true},
		{"missing codec stats", 0, true, "copy 3 lacks a complete measured run", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m report.Metrics
			m.Header.Compress, m.Header.Encrypt = "adapt", "auto"
			m.Header.Dataset = report.Dataset{Fingerprint: reference.Fingerprint, Files: reference.Files, Bytes: c.Bytes}
			for i := range c.Repeats {
				run := report.Run{Label: strconv.Itoa(i + 1), Measured: true, Status: report.StatusOK, WallNS: 1,
					Bytes: c.Bytes, Files: int64(reference.Files), Windows: map[string]int64{"none": int64(reference.Files)}}
				run.Cache = report.Cache{Prepped: true, Warm: "0%"}
				run.Verify = report.Verify{Mode: "full", Bytes: c.Bytes, Files: reference.Files, DurNS: 1}
				m.Runs = append(m.Runs, run)
			}
			m.Runs[c.Repeats-1].Cache.HotPct = tc.hot
			if tc.missingWindows {
				m.Runs[c.Repeats-1].Windows = nil
			}
			path := filepath.Join(t.TempDir(), "m.json")
			if err := dataset.WriteJSONAtomic(path, m); err != nil {
				t.Fatal(err)
			}
			_, err := suiteCopyMetrics(path, c, reference)
			var setup suiteSetupError
			if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) ||
				errors.As(err, &setup) != tc.setup {
				t.Errorf("error = %v, want %q (retryable=%v)", err, tc.err, tc.setup)
			}
		})
	}
}

// suiteProcessAlive reports whether pid is running and not a zombie.
func suiteProcessAlive(pid int) bool {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	_, rest, _ := strings.Cut(string(stat), ") ")
	return !strings.HasPrefix(rest, "Z")
}

func waitForFile(t *testing.T, path string) []byte {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 && bytes.HasSuffix(data, []byte("\n")) {
			return data
		}
	}
	t.Fatalf("%s never appeared", path)
	return nil
}

func TestSuiteCommandStops(t *testing.T) {
	for _, tc := range []struct {
		label    string
		harness  bool
		grace    time.Duration
		escalate bool
		want     error
	}{
		{"non-harness killed at once", false, time.Minute, false, context.DeadlineExceeded},
		{"harness killed after grace", true, 100 * time.Millisecond, false, context.DeadlineExceeded},
		{"harness killed by second interrupt", true, time.Minute, true, errSuiteKilled},
	} {
		t.Run(tc.label, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "pid")
			escalate := make(chan struct{})
			w := &suiteWorkspace{grace: tc.grace, escalate: escalate}
			log, err := os.Create(filepath.Join(dir, "log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			// The shell ignores SIGTERM, as a hung harness would, and leaves
			// a grandchild in its process group.
			script := `trap "" TERM; sleep 60 & echo $! > "$1"; wait`
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if tc.escalate {
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
				go func() {
					time.Sleep(200 * time.Millisecond)
					cancel()
					time.Sleep(100 * time.Millisecond)
					close(escalate)
				}()
			}
			start := time.Now()
			err = w.run(ctx, log, suiteCommand{args: []string{"sh", "-c", script, "sh", pidFile}, harness: tc.harness})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v; want %v", err, tc.want)
			}
			if elapsed := time.Since(start); elapsed > 10*time.Second {
				t.Fatalf("run took %s", elapsed)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(waitForFile(t, pidFile))))
			if err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(5 * time.Second); suiteProcessAlive(pid); time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("grandchild %d outlived its process group", pid)
				}
			}
		})
	}
}

func TestSuiteChildHoldsLock(t *testing.T) {
	root := t.TempDir()
	lock, err := lockSuiteRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	w := &suiteWorkspace{locks: []*os.File{lock}}
	log, err := os.Create(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	// The child leaves a process behind that outlives the suite.
	pidFile := filepath.Join(t.TempDir(), "pid")
	script := `sleep 60 & echo $! > "$1"`
	if err := w.run(context.Background(), log, suiteCommand{args: []string{"sh", "-c", script, "sh", pidFile}, workspace: true}); err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(waitForFile(t, pidFile))))
	if err != nil {
		t.Fatal(err)
	}
	w.close()
	if f, err := lockSuiteRoot(root); err == nil {
		f.Close()
		t.Fatal("a new suite locked the root while a workspace child still ran")
	}
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		f, err := lockSuiteRoot(root)
		if err == nil {
			f.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lock after the child exited: %v", err)
		}
	}
}

func TestSuiteCacheRepair(t *testing.T) {
	cases := suiteSettings("smoke").SSD
	var notes strings.Builder
	open := func(root string) (*suiteWorkspace, error) {
		return openSuiteWorkspace(suiteWorkspaceOptions{
			Root: root, Out: t.TempDir(), Profile: "smoke", Cases: cases, Log: &notes,
		})
	}
	t.Run("invalid entry is provisioned again", func(t *testing.T) {
		root := t.TempDir()
		w, err := open(root)
		if err != nil {
			t.Fatal(err)
		}
		entry := w.cacheEntry(cases[0])
		w.close()
		if err := os.MkdirAll(filepath.Join(entry, "source"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(entry, "reference.json"), []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
		w, err = open(root)
		if err != nil {
			t.Fatal(err)
		}
		defer w.close()
		if _, err := os.Stat(entry); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid entry kept: stat error = %v", err)
		}
		if w.references[cases[0].ID] != nil || !strings.Contains(notes.String(), "provisioning it again") {
			t.Fatalf("entry not treated as missing; notes: %s", notes.String())
		}
	})
	t.Run("stale entries go only when space is short", func(t *testing.T) {
		root := t.TempDir()
		w, err := open(root)
		if err != nil {
			t.Fatal(err)
		}
		stale := filepath.Join(w.sources, cases[0].ID+"-00000000000000000000")
		retired := filepath.Join(w.sources, "ssd-retired-00000000000000000000")
		other := filepath.Join(w.sources, "unrelated-00000000000000000000")
		w.close()
		for _, dir := range []string{stale, retired} {
			if err := dataset.ClaimDir(filepath.Join(dir, "source"), dataset.BenchMarker); err != nil {
				t.Fatal(err)
			}
			if err := dataset.WriteJSONAtomic(filepath.Join(dir, "reference.json"), "0123456789abcdef"); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Mkdir(other, 0o755); err != nil {
			t.Fatal(err)
		}
		defer func(orig func(string) (dataset.FSInfo, error)) { suiteStatFS = orig }(suiteStatFS)
		available := int64(1 << 50)
		suiteStatFS = func(string) (dataset.FSInfo, error) { return dataset.FSInfo{Available: available}, nil }
		w, err = open(root)
		if err != nil {
			t.Fatal(err)
		}
		w.close()
		for _, entry := range []string{stale, retired} {
			if _, err := os.Stat(entry); err != nil {
				t.Fatalf("stale entry removed with space to spare: %v", err)
			}
		}
		available = 0
		if w, err := open(root); err == nil {
			w.close()
			t.Fatal("opened a workspace without space")
		}
		for _, entry := range []string{stale, retired} {
			if _, err := os.Stat(entry); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stale entry kept under space pressure: stat error = %v", err)
			}
		}
		if _, err := os.Stat(other); err != nil {
			t.Fatalf("removed an unowned entry: %v", err)
		}
	})
	t.Run("unused entries go after keep-unused", func(t *testing.T) {
		root := t.TempDir()
		current := ssdCase("ssd-tiny", "Tiny files", "64KiB", "rand=100%@16KiB", 64<<10)
		w, err := open(root)
		if err != nil {
			t.Fatal(err)
		}
		provisionTestReference(t, w, current, w.cacheEntry(current))
		retired := filepath.Join(w.sources, "ssd-tiny-00000000000000000000")
		recent := filepath.Join(w.sources, "ssd-gone-00000000000000000000")
		other := filepath.Join(w.sources, "unrelated-00000000000000000000")
		w.close()
		for _, dir := range []string{retired, recent} {
			if err := dataset.ClaimDir(filepath.Join(dir, "source"), dataset.BenchMarker); err != nil {
				t.Fatal(err)
			}
			if err := dataset.WriteJSONAtomic(filepath.Join(dir, "reference.json"), "0123456789abcdef"); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Mkdir(other, 0o755); err != nil {
			t.Fatal(err)
		}
		long, lately := time.Now().Add(-30*24*time.Hour), time.Now().Add(-24*time.Hour)
		for dir, stamp := range map[string]time.Time{w.cacheEntry(current): long, retired: long, recent: lately} {
			if err := os.Chtimes(filepath.Join(dir, "source"), stamp, stamp); err != nil {
				t.Fatal(err)
			}
		}
		defer func(orig func(string) (dataset.FSInfo, error)) { suiteStatFS = orig }(suiteStatFS)
		suiteStatFS = func(string) (dataset.FSInfo, error) { return dataset.FSInfo{Available: 1 << 50}, nil }
		var log strings.Builder
		w, err = openSuiteWorkspace(suiteWorkspaceOptions{
			Root: root, Out: t.TempDir(), Profile: "smoke", Cases: []suiteCase{current},
			Log: &log, KeepUnused: 7 * 24 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer w.close()
		if w.references[current.ID] == nil {
			t.Fatalf("current reference not reused; log: %s", log.String())
		}
		used, err := os.Stat(filepath.Join(w.cacheEntry(current), "source"))
		if err != nil || time.Since(used.ModTime()) > time.Hour {
			t.Fatalf("current reference not marked used: %v", err)
		}
		if _, err := os.Stat(retired); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("long-unused reference kept: stat error = %v", err)
		}
		for _, keep := range []string{recent, other} {
			if _, err := os.Stat(keep); err != nil {
				t.Fatalf("removed %s: %v", keep, err)
			}
		}
		if got := log.String(); !strings.Contains(got, "Removing stale reference "+filepath.Base(retired)) ||
			strings.Contains(got, filepath.Base(recent)) {
			t.Fatalf("unexpected removals; log: %s", got)
		}
	})
	t.Run("output inside the root", func(t *testing.T) {
		root := t.TempDir()
		_, err := openSuiteWorkspace(suiteWorkspaceOptions{Root: root, Out: filepath.Join(root, "results"), Profile: "smoke"})
		if err == nil || !strings.Contains(err.Error(), "outside the suite root") {
			t.Fatalf("error = %v; want output rejected", err)
		}
	})
}

func TestSuiteEvictionCheck(t *testing.T) {
	root := t.TempDir()
	fs, err := dataset.StatFS(root)
	if err != nil {
		t.Fatal(err)
	}
	w, err := openSuiteWorkspace(suiteWorkspaceOptions{Root: root, Out: t.TempDir(), Profile: "smoke", Evictable: true})
	if err == nil {
		w.close()
	} else if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Fatalf("refused root %s was written to: %v", root, entries)
	}
	if cannot := fs.Type == "tmpfs" || fs.Type == "ramfs" || fs.IgnoresFadvise(); cannot != (err != nil) {
		t.Fatalf("%s on %s: error = %v", root, fs.Type, err)
	}
}

func TestSuiteMismatchBlamesDamagedReference(t *testing.T) {
	// A stand-in tx-bench: local records an oracle mismatch and fails; prep
	// --check exits with PREP_EXIT.
	bench := filepath.Join(t.TempDir(), "tx-bench")
	script := `#!/bin/sh
case "$1" in
local)
	while [ $# -gt 0 ]; do
		if [ "$1" = --metrics ]; then shift; echo '{"runs":[{"verify":{"mismatches":1}}]}' > "$1"; fi
		shift
	done
	exit 1 ;;
prep) exit "$PREP_EXIT" ;;
esac
exit 2
`
	if err := os.WriteFile(bench, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		prepExit string
		setup    bool
	}{{"0", false}, {"1", true}} {
		t.Run("prep exit "+tc.prepExit, func(t *testing.T) {
			t.Setenv("PREP_EXIT", tc.prepExit)
			w, err := openSmokeWorkspace(t.TempDir(), t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer w.close()
			c := suiteSettings("smoke").SSD[0]
			entry := w.cacheEntry(c)
			if err := os.MkdirAll(filepath.Join(entry, "source"), 0o755); err != nil {
				t.Fatal(err)
			}
			w.references[c.ID] = &dataset.BenchJSON{Files: 1, Bytes: c.Bytes, Fingerprint: "f"}
			out := t.TempDir()
			log, err := os.Create(filepath.Join(out, "case.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			err = w.copyCase(context.Background(), &c, bench, "tx", out, log, io.Discard)
			var setup suiteSetupError
			if err == nil || errors.As(err, &setup) != tc.setup {
				t.Fatalf("error = %v; want setup error %v", err, tc.setup)
			}
			if _, err := os.Stat(entry); errors.Is(err, os.ErrNotExist) != tc.setup {
				t.Fatalf("reference removed = %v; want %v", errors.Is(err, os.ErrNotExist), tc.setup)
			}
		})
	}
}

// provisionTestReference generates c's recipe into entry as reference()
// does, without a tx-bench binary.
func provisionTestReference(t *testing.T, w *suiteWorkspace, c suiteCase, entry string) {
	t.Helper()
	source := filepath.Join(entry, "source")
	var out strings.Builder
	if code := Main(c.prepArgs(source), &out, &out); code != exitOK {
		t.Fatalf("prep exited %d: %s", code, out.String())
	}
	d, err := w.loadReference(source, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := dataset.WriteJSONAtomic(filepath.Join(entry, "reference.json"), d.Fingerprint); err != nil {
		t.Fatal(err)
	}
}
