//go:build linux

package harness

import (
	"bytes"
	"context"
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
	const sample = name + "  1  42 ns/op\n"
	const otherSample = other + "  1  42 ns/op\n"
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
	} {
		t.Run(tc.label, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "case.log")
			if err := os.WriteFile(log, []byte(tc.log), 0o600); err != nil {
				t.Fatal(err)
			}
			missing, err := checkSuiteBenchmarks(log, []string{name, other}, 3)
			if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
				t.Fatalf("error = %v; want %q", err, tc.err)
			}
			if tc.err == "" && !slices.Equal(missing, tc.missing) && !(len(tc.missing) == 0 && len(missing) == 0) {
				t.Fatalf("missing = %q; want %q", missing, tc.missing)
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

func TestSuiteCopyResidency(t *testing.T) {
	c := ssdCase("ssd-small", "16MiB", "rand=100%@16KiB", 16*suiteMiB)
	c.Files = 1024
	for _, tc := range []struct {
		hot float64
		ok  bool
	}{{0, true}, {suiteMaxHotPct, true}, {37.5, false}, {100, false}} {
		var m report.Metrics
		m.Header.Compress, m.Header.Encrypt = "none", "none"
		m.Header.Dataset = report.Dataset{Fingerprint: "f", Files: c.Files, Bytes: c.Bytes}
		run := report.Run{Measured: true, Status: report.StatusOK, WallNS: 1, Bytes: c.Bytes, Files: int64(c.Files)}
		run.Cache = report.Cache{Prepped: true, Warm: "0%", HotPct: tc.hot}
		run.Verify = report.Verify{Mode: "full", Bytes: c.Bytes, Files: c.Files}
		m.Runs = []report.Run{run}
		path := filepath.Join(t.TempDir(), "m.json")
		if err := dataset.WriteJSONAtomic(path, m); err != nil {
			t.Fatal(err)
		}
		_, err := suiteCopyMetrics(path, c, "f")
		var setup suiteSetupError
		if (err == nil) != tc.ok || err != nil && !errors.As(err, &setup) {
			t.Errorf("hot %.1f%%: error = %v; want ok=%v, or a retryable setup error", tc.hot, err, tc.ok)
		}
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
		other := filepath.Join(w.sources, "unrelated-00000000000000000000")
		w.close()
		for _, dir := range []string{stale, other} {
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		defer func(orig func(string) (dataset.FSInfo, error)) { suiteStatFS = orig }(suiteStatFS)
		available := int64(1 << 50)
		suiteStatFS = func(string) (dataset.FSInfo, error) { return dataset.FSInfo{Available: available}, nil }
		w, err = open(root)
		if err != nil {
			t.Fatal(err)
		}
		w.close()
		if _, err := os.Stat(stale); err != nil {
			t.Fatalf("stale entry removed with space to spare: %v", err)
		}
		available = 0
		if w, err := open(root); err == nil {
			w.close()
			t.Fatal("opened a workspace without space")
		}
		if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale entry kept under space pressure: stat error = %v", err)
		}
		if _, err := os.Stat(other); err != nil {
			t.Fatalf("removed an entry for another case: %v", err)
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
