//go:build linux

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"golang.org/x/sys/unix"
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
	cf.StringVar(&root, "", "ssd-root", "", "Existing dedicated SSD directory, required for full")
	cf.StringVar(&out, "", "out", "bench/results/suite", "Empty result directory")
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
			return usageErrorf("full suite requires an explicit --ssd-root")
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
	w, err := openSuiteWorkspace(root, out, spec.Profile, settings.SSD)
	if err != nil {
		return err
	}
	defer w.close()
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
	buildErr := runSuiteCommand(buildCtx, build, []string{"go", "build", "-o", tx, "./cmd/tx"}, nil, false)
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
		if err := w.runCase(ctx, c, settings.Repeats, bench, tx, out, stdout); err != nil {
			fmt.Fprintf(stdout, "%s failed: %v\n", c.ID, err)
			status[c.ID] = "failed"
			failures = append(failures, c.ID)
		} else {
			status[c.ID] = "ok"
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

func (w *suiteWorkspace) runCase(ctx context.Context, c *suiteCase, repeats int, bench, tx, out string, stdout io.Writer) (result error) {
	log, err := os.OpenFile(filepath.Join(out, c.ID+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if result != nil {
			fmt.Fprintf(log, "\n%s failed: %v\n", c.ID, result)
		}
		result = errors.Join(result, log.Close())
	}()
	switch c.Kind {
	case "ssd":
		return w.copyCase(ctx, c, bench, tx, out, log, stdout)
	case "grid":
		ctx, cancel := context.WithTimeout(ctx, suiteGridTimeout)
		defer cancel()
		env := maps.Clone(*c.Env)
		env["THROUGHPUT_OUT"] = out
		if err := runSuiteCommand(ctx, log, c.Argv, env, false); err != nil {
			return err
		}
		var grid struct {
			Pass bool `json:"pass"`
		}
		if err := readJSON(filepath.Join(out, "throughput.json"), &grid); err != nil {
			return fmt.Errorf("read throughput grid: %w", err)
		}
		if !grid.Pass {
			return fmt.Errorf("throughput grid did not pass its CPU cost ratio gate")
		}
		return nil
	case "go":
		ctx, cancel := context.WithTimeout(ctx, suiteGoTimeout)
		defer cancel()
		if err := runSuiteCommand(ctx, log, c.Argv, *c.Env, false); err != nil {
			return err
		}
		return checkSuiteBenchmarks(log.Name(), c.Series, repeats)
	}
	return fmt.Errorf("unknown case kind %q", c.Kind)
}

func (w *suiteWorkspace) copyCase(ctx context.Context, c *suiteCase, bench, tx, out string, log *os.File, stdout io.Writer) error {
	prepCtx, cancelPrep := context.WithTimeout(ctx, suiteSSDTimeout)
	source, reference, err := w.reference(prepCtx, *c, bench, log)
	cancelPrep()
	if err != nil {
		return err
	}
	c.Files = reference.Files
	c.MeanFileBytes = float64(reference.Bytes) / float64(reference.Files)
	mean := encoding.HumanBytes(int64(c.MeanFileBytes))
	fmt.Fprintf(log, "Source layout: %d files, mean size %s\n", c.Files, mean)
	dest, err := os.MkdirTemp(w.scratch, suiteCopyPrefix+c.ID+"-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dest)
	metrics := filepath.Join(out, c.ID+".json")
	copyCtx, cancelCopy := context.WithTimeout(ctx, suiteSSDTimeout)
	defer cancelCopy()
	args := append([]string{bench}, c.copyArgs(tx, reference.Fingerprint, metrics, source, dest)...)
	if err := runSuiteCommand(copyCtx, log, args, nil, true); err != nil {
		return err
	}
	run, err := suiteCopyMetrics(metrics, *c, reference.Fingerprint)
	if err != nil {
		return err
	}
	fmt.Fprintf(io.MultiWriter(stdout, log), "%s: %.2f MiB/s, %d files, mean size %s\n",
		c.ID, run.RateBps()/suiteMiB, c.Files, mean)
	return nil
}

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
	return run, nil
}

// The harness handles SIGTERM through its context and waits for both tx
// processes. Other commands may fork compilers or test binaries, so kill
// their process group on cancellation.
func runSuiteCommand(ctx context.Context, log *os.File, args []string, env map[string]string, harness bool) error {
	if _, err := fmt.Fprintf(log, "\n$ %s\n", suiteShellCommand(args, env)); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = suiteCommandEnv(env)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if harness {
			return cmd.Process.Signal(unix.SIGTERM)
		}
		err := unix.Kill(-cmd.Process.Pid, unix.SIGKILL)
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	err := cmd.Run()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func suiteCommandEnv(overrides map[string]string) []string {
	env := os.Environ()
	env = slices.DeleteFunc(env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "GOFLAGS", "GOWORK", "MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "MAKELEVEL", "GNUMAKEFLAGS",
			"TX_AUTH_TOKEN", txBinEnv:
			return true
		}
		return strings.HasPrefix(key, "TX_VERIFY_") || strings.HasPrefix(key, "TX_BENCH_") ||
			strings.HasPrefix(key, "TX_FAULTS_")
	})
	env = append(env, "GOFLAGS=", "GOWORK=off")
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}
