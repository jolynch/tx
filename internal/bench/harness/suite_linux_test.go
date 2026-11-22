//go:build linux

package harness

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jolynch/tx/internal/bench/dataset"
)

func TestSuiteBenchmarkSamples(t *testing.T) {
	const name = "BenchmarkVerifyInMemory/4KiB/cpus=8/c=4-8"
	const sample = name + "  1  42 ns/op\n"
	for _, tc := range []struct {
		label string
		log   string
		err   string
	}{
		{"complete", "goos: linux\n" + sample + sample + sample + "PASS\n", ""},
		{"no benchmark matches", "PASS\n", "missing 3 of 3 samples"},
		{"missing repetition", sample + sample, "missing 1 of 3 samples"},
		{"wrong CPU", strings.ReplaceAll(sample, "-8", "-2"), "unexpected benchmark result"},
		{"extra repetition", strings.Repeat(sample, 4), "unexpected benchmark result"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "case.log")
			if err := os.WriteFile(log, []byte(tc.log), 0o600); err != nil {
				t.Fatal(err)
			}
			err := checkSuiteBenchmarks(log, []string{name}, 3)
			if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
				t.Fatalf("error = %v; want %q", err, tc.err)
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
		if w, err := openSuiteWorkspace(root, out, "smoke", nil); !errors.Is(err, dataset.ErrUnmarked) {
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
		first, err := openSuiteWorkspace(root, out, "smoke", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer first.close()
		staging, err := os.MkdirTemp(first.sources, suitePrepPrefix+"ssd-small-")
		if err != nil {
			t.Fatal(err)
		}
		if w, err := openSuiteWorkspace(root, out, "smoke", nil); err == nil {
			w.close()
			t.Fatal("second suite acquired the lock")
		}
		if w, err := openSuiteWorkspace(first.root, out, "smoke", nil); err == nil {
			w.close()
			t.Fatal("second suite acquired the nested workspace lock")
		}
		if _, err := os.Stat(staging); err != nil {
			t.Fatal(err)
		}
		first.close()
		next, err := openSuiteWorkspace(root, out, "smoke", nil)
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
		if w, err := openSuiteWorkspace(root, out, "smoke", nil); err == nil {
			w.close()
			t.Fatal("suite accepted a symlinked cache directory")
		}
	})
}
