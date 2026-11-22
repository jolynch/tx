//go:build linux

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

const (
	suiteBuildTimeout = 10 * time.Minute
	suiteGoTimeout    = 5 * time.Minute
	suiteGridTimeout  = 11 * time.Minute
	suiteSSDTimeout   = 30 * time.Minute
)

func runSuiteCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cf := cliflags.New("suite")
	cf.SetOutput(stderr)
	var profile, root, out string
	var describe bool
	cf.StringVar(&profile, "", "profile", "smoke", "Workloads: smoke|full")
	cf.StringVar(&root, "", "data-root", "", "Existing directory for reference trees and copy scratch, required for full; smoke uses a temporary one under bench/data/")
	cf.StringVar(&out, "", "out", "bench/results/suite", "Empty result directory outside the data root")
	cf.BoolVar(&describe, "", "describe", false, "Print commands without running the workloads")
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx-bench suite [options]\n\nRun from the tx checkout root on Linux.")
		cf.PrintDefaults(stderr)
	}
	if code, done := parseFlags(cf, args); done {
		return code
	}
	if cf.NArg() != 0 || (profile != "smoke" && profile != "full") {
		return reportErr(stderr, usageErrorf("suite takes no positional arguments and requires --profile smoke or full"))
	}
	settings := suiteSettings(profile)
	spec := suiteManifest{Version: 1, Profile: profile, Cases: settings.cases()}
	if describe {
		describeSuite(stdout, spec.Cases)
		return exitOK
	}
	if err := runSuite(ctx, out, root, &spec, settings, stdout); err != nil {
		return reportErr(stderr, err)
	}
	return exitOK
}

func runSuite(ctx context.Context, out, root string, spec *suiteManifest, settings suiteProfile, stdout io.Writer) error {
	minCPUs := 2
	if spec.Profile == "full" {
		minCPUs = 16
		if root == "" {
			return usageErrorf("full suite requires an explicit --data-root")
		}
	}
	if cpus := min(runtime.NumCPU(), runtime.GOMAXPROCS(0)); cpus < minCPUs {
		return usageErrorf("%s suite needs at least %d available CPUs, found %d", spec.Profile, minCPUs, cpus)
	}
	if _, err := os.Stat("cmd/tx/main.go"); err != nil {
		return usageErrorf("run suite from the tx checkout root: %v", err)
	}
	bench, err := os.Executable()
	if err != nil {
		return err
	}
	if out == "" {
		out = "bench/results/suite"
	}
	out, err = emptySuiteOutput(out)
	if err != nil {
		return err
	}
	escalate, stopEscalation := suiteEscalation()
	defer stopEscalation()
	w, err := openSuiteWorkspace(suiteWorkspaceOptions{
		Root: root, Out: out, Profile: spec.Profile, Cases: settings.SSD, Log: stdout, Evictable: true,
	})
	if err != nil {
		return err
	}
	defer w.close()
	w.escalate = escalate
	bin, err := os.MkdirTemp("", "tx-bench-suite-bin-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(bin)
	if err := ctx.Err(); err != nil {
		return err
	}
	build, err := os.OpenFile(filepath.Join(out, "build.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	tx := filepath.Join(bin, "tx")
	buildCtx, cancelBuild := context.WithTimeout(ctx, suiteBuildTimeout)
	buildErr := w.run(buildCtx, build, suiteCommand{args: []string{"go", "build", "-o", tx, "./cmd/tx"}})
	cancelBuild()
	buildErr = errors.Join(buildErr, build.Close())
	if err := ctx.Err(); err != nil {
		return err
	}

	status := make(map[string]string, len(spec.Cases))
	var failures []string
	for i := range spec.Cases {
		c := &spec.Cases[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if buildErr != nil {
			status[c.ID] = "build_failed"
			continue
		}
		fmt.Fprintf(stdout, "Running %s\n", c.ID)
		outcome, err := w.runCase(ctx, c, settings.Repeats, bench, tx, out, stdout)
		var setup suiteSetupError
		switch {
		case errors.As(err, &setup):
			fmt.Fprintf(stdout, "%s setup failed: %v\n", c.ID, err)
			status[c.ID] = suiteSetupFailed
			failures = append(failures, c.ID)
		case err != nil:
			fmt.Fprintf(stdout, "%s failed: %v\n", c.ID, err)
			status[c.ID] = "failed"
			failures = append(failures, c.ID)
		case outcome != suiteOK:
			fmt.Fprintf(stdout, "%s: %s in this commit\n", c.ID, outcome)
			status[c.ID] = outcome
		default:
			status[c.ID] = suiteOK
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := dataset.WriteJSONAtomic(filepath.Join(out, "suite.json"), spec); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := dataset.WriteJSONAtomic(filepath.Join(out, "status.json"), status); err != nil {
		return err
	}
	if buildErr != nil {
		return fmt.Errorf("build tx: %w (see %s)", buildErr, filepath.Join(out, "build.log"))
	}
	if len(failures) != 0 {
		return fmt.Errorf("failed cases: %s (see %s)", strings.Join(failures, ", "), out)
	}
	fmt.Fprintf(stdout, "Suite results: %s\n", out)
	return nil
}

func emptySuiteOutput(out string) (string, error) {
	out, err := filepath.Abs(out)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return "", err
	}
	out, err = filepath.EvalSymlinks(out)
	if err != nil {
		return "", err
	}
	files, err := os.ReadDir(out)
	if err != nil {
		return "", err
	}
	if len(files) != 0 {
		return "", usageErrorf("pick an empty result directory: %s", out)
	}
	return out, nil
}

// Case outcomes other than failure. A benchmark that the tested commit does not
// define is missing, not failed, so older commits can be measured unchanged.
const (
	suiteOK      = "ok"
	suitePartial = "partial" // some expected Go series are not in this commit
	suiteMissing = "missing" // the whole workload is not in this commit
)

// suiteSetupFailed marks a case whose reference could not be provisioned or
// validated. It measured nothing, so the scheduler retries the commit.
const suiteSetupFailed = "setup_failed"

func (w *suiteWorkspace) runCase(ctx context.Context, c *suiteCase, repeats int, bench, tx, out string, stdout io.Writer) (outcome string, result error) {
	log, err := os.OpenFile(filepath.Join(out, c.ID+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	defer func() {
		if result != nil {
			fmt.Fprintf(log, "\n%s failed: %v\n", c.ID, result)
		}
		result = errors.Join(result, log.Close())
	}()
	switch c.Kind {
	case "ssd":
		return suiteOK, w.copyCase(ctx, c, bench, tx, out, log, stdout)
	case "grid":
		if !suiteGridExists() {
			_, err := fmt.Fprintln(log, "This commit has no bench-throughput target or TestTransferCostRatio.")
			return suiteMissing, err
		}
		ctx, cancel := context.WithTimeout(ctx, suiteGridTimeout)
		defer cancel()
		env := maps.Clone(*c.Env)
		env["THROUGHPUT_OUT"] = out
		if err := w.run(ctx, log, suiteCommand{args: c.Argv, env: env}); err != nil {
			return "", err
		}
		var grid struct {
			Pass bool `json:"pass"`
		}
		if err := readJSON(filepath.Join(out, "throughput.json"), &grid); err != nil {
			return "", fmt.Errorf("read throughput grid: %w", err)
		}
		if !grid.Pass {
			return "", fmt.Errorf("throughput grid did not pass its CPU cost ratio gate")
		}
		return suiteOK, nil
	case "go":
		if pkg := c.Argv[len(c.Argv)-1]; !suiteGoPackageExists(pkg) {
			_, err := fmt.Fprintf(log, "This commit has no tests in %s.\n", pkg)
			return suiteMissing, err
		}
		ctx, cancel := context.WithTimeout(ctx, suiteGoTimeout)
		defer cancel()
		if err := w.run(ctx, log, suiteCommand{args: c.Argv, env: *c.Env}); err != nil {
			return "", err
		}
		missing, err := checkSuiteBenchmarks(log.Name(), c.Series, repeats)
		switch {
		case err != nil:
			return "", err
		case len(missing) == len(c.Series):
			_, err = fmt.Fprintln(log, "\nNone of the expected benchmarks are in this commit.")
			return suiteMissing, err
		case len(missing) != 0:
			_, err = fmt.Fprintf(log, "\nNot in this commit: %s\n", strings.Join(missing, ", "))
			return suitePartial, err
		}
		return suiteOK, nil
	}
	return "", fmt.Errorf("unknown case kind %q", c.Kind)
}

func (w *suiteWorkspace) copyCase(ctx context.Context, c *suiteCase, bench, tx, out string, log *os.File, stdout io.Writer) error {
	prepCtx, cancelPrep := context.WithTimeout(ctx, suiteSSDTimeout)
	source, reference, err := w.reference(prepCtx, *c, bench, log)
	cancelPrep()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return suiteSetupError{err}
	}
	c.Files = reference.Files
	c.MeanFileBytes = float64(reference.Bytes) / float64(reference.Files)
	mean := encoding.HumanBytes(int64(c.MeanFileBytes))
	fmt.Fprintf(log, "Source layout: %d files, mean size %s\n", c.Files, mean)
	dest, err := os.MkdirTemp(w.scratch, suiteCopyPrefix+c.ID+"-")
	if err != nil {
		return suiteSetupError{err}
	}
	defer os.RemoveAll(dest)
	metrics := filepath.Join(out, c.ID+".json")
	copyCtx, cancelCopy := context.WithTimeout(ctx, suiteSSDTimeout)
	defer cancelCopy()
	args := append([]string{bench}, c.copyArgs(tx, reference.Fingerprint, metrics, source, dest)...)
	err = w.run(copyCtx, log, suiteCommand{args: args, harness: true, workspace: true})
	var run report.Run
	if err == nil {
		run, err = suiteCopyMetrics(metrics, *c, reference.Fingerprint)
	}
	if err != nil {
		if ctx.Err() == nil && suiteCopyMismatched(metrics) && w.recheckReference(ctx, *c, bench, source, log) {
			return suiteSetupError{fmt.Errorf("reference failed its rehash after the copy mismatched it: %w", err)}
		}
		return err
	}
	fmt.Fprintf(io.MultiWriter(stdout, log), "%s: %.2f MiB/s, %d files, mean size %s\n",
		c.ID, run.RateBps()/suiteMiB, c.Files, mean)
	return nil
}

// suiteSetupError marks a case that failed before measuring anything: its
// reference could not be provisioned or its copy destination created.
type suiteSetupError struct{ err error }

func (e suiteSetupError) Error() string { return e.err.Error() }
func (e suiteSetupError) Unwrap() error { return e.err }

// suiteMaxHotPct is the most measured source residency a cold copy accepts.
const suiteMaxHotPct = 1.0

func suiteCopyMetrics(path string, c suiteCase, fingerprint string) (report.Run, error) {
	var m report.Metrics
	if err := readJSON(path, &m); err != nil {
		return report.Run{}, fmt.Errorf("read copy metrics: %w", err)
	}
	if len(m.Runs) != 1 {
		return report.Run{}, fmt.Errorf("expected one measured copy, got %d runs", len(m.Runs))
	}
	run, ds := m.Runs[0], m.Header.Dataset
	if ds.Fingerprint != fingerprint || ds.Files != c.Files || ds.Bytes != c.Bytes ||
		m.Header.Compress != "none" || m.Header.Encrypt != "none" ||
		!run.Measured || !run.OK() || run.WallNS <= 0 || run.Bytes != c.Bytes || run.Files != int64(c.Files) ||
		!run.Cache.Prepped || run.Cache.Warm != "0%" || run.Verify.Mode != "full" || run.Verify.Mismatches != 0 ||
		run.Verify.Bytes != c.Bytes || run.Verify.Files != c.Files {
		return report.Run{}, fmt.Errorf("copy metrics lack a complete measured run with full verification")
	}
	if !(run.Cache.HotPct <= suiteMaxHotPct) {
		// The host, not tx, failed to make the source cold: retry it.
		return report.Run{}, suiteSetupError{fmt.Errorf("source was %.1f%% cached before a cold copy; the filesystem did not evict it", run.Cache.HotPct)}
	}
	return run, nil
}

// suiteCopyMismatched reports whether a copy's metrics record destination
// bytes that differ from the reference.
func suiteCopyMismatched(path string) bool {
	var m report.Metrics
	if readJSON(path, &m) != nil {
		return false
	}
	return slices.ContainsFunc(m.Runs, func(r report.Run) bool { return r.Verify.Mismatches > 0 })
}
