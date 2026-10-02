package harness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/txstats"
)

// recvFlags are recv-copy's options.
type recvFlags struct {
	warmup      int
	iterations  int
	expect      string
	oracle      string
	jobs        int
	metrics     string
	trace       string
	rssInterval string
	goTrace     string
	serverWait  string
	failFast    bool
	keep        bool
	forever     bool
	stopSender  bool
	tx          string

	metricsGiven bool
}

func (r *recvFlags) register(cf *cliflags.Flags) {
	cf.IntVar(&r.warmup, "w", "warmup", 1, "Unmeasured runs before measuring")
	cf.IntVar(&r.iterations, "n", "iterations", 3, "Measured runs")
	cf.StringVar(&r.expect, "e", "expect", "", "Dataset fingerprint printed by send-tree; checked against the fetched files.tsv")
	cf.StringVar(&r.oracle, "", "oracle", "full", "Check of DST/data against files.tsv after each run: full|names")
	cf.IntVar(&r.jobs, "j", "jobs", 0, "Verify workers (0=CPU count)")
	cf.StringVar(&r.metrics, "o", "metrics", "", "Metrics file; .json/.jsonl for JSON, else space-separated rows; empty disables (default: ./tx-bench-<UTC timestamp>.json)")
	cf.StringVar(&r.trace, "", "trace", "", "Write the client event timeline (tx -f events) to this file; fetched sender events go to its .server sibling")
	cf.StringVar(&r.rssInterval, "", "trace-rss-interval", "100ms", "Interval for sampling the tx process's RSS into the trace")
	cf.StringVar(&r.goTrace, "", "go-trace", "", "Write runtime/trace output of each tx recv copy (tx --trace) to PATH.<run>")
	cf.StringVar(&r.serverWait, "", "server-wait", "2m", "How long to wait for a prep acknowledgement or runs/server-<tid>.json")
	cf.BoolVar(&r.failFast, "", "fail-fast", false, "Stop at the first failed run")
	cf.BoolVar(&r.keep, "", "keep", false, "Keep DST after the last run")
	cf.BoolVar(&r.forever, "", "forever", false, "After the warmups, copy and verify until interrupted instead of --iterations times; metrics default to .jsonl")
	cf.BoolVar(&r.stopSender, "", "stop-sender", false, "After the last run, fetch runs/stop so send-tree records it and exits")
	cf.StringVar(&r.tx, "", "tx", "", "tx binary to fork (default: $TX_BIN, then tx next to tx-bench, then $PATH)")
}

func (r *recvFlags) noteGiven(cf *cliflags.Flags) {
	cf.Visit(func(f *flag.Flag) {
		if f.Name == "o" || f.Name == "metrics" {
			r.metricsGiven = true
		}
	})
}

// recvConfig is a fully parsed recv-copy invocation.
type recvConfig struct {
	flags       recvFlags
	server      string // host:port
	benchRemote string
	dst         string
	txArgs      []string
	senderArgs  []string // local mode: what the sender forks, for the header
	out         io.Writer
	baseline    string // local mode only: none or rsync
	localSrc    string // local mode: the data root, for --baseline rsync
}

// RunRecvCopyCLI is 'tx-bench remote recv-copy'.
func RunRecvCopyCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	own, txArgs := splitPassThrough(args)
	cf := cliflags.New("recv-copy")
	cf.SetOutput(stderr)
	cfg := recvConfig{txArgs: txArgs, out: stdout}
	cfg.flags.register(cf)
	cf.FlagSet().Usage = func() {
		fmt.Fprint(stderr, `usage: tx-bench remote recv-copy [options] [SERVER] [DST] [-- TX_ARGS...]

Pull a served benchmark dataset repeatedly with forked 'tx recv copy'
processes, verify every run, and report.

  SERVER    sender host[:port][/bench/path] (default 127.0.0.1:3453/)
  DST       work directory, marked by tx-bench; each run copies into DST/data
            after deleting DST/data and DST/.tx (default ./tx-bench-dst)
  TX_ARGS   passed to tx recv copy as given (see 'tx recv copy --help')

Behavior:
  - Runs --warmup unmeasured copies, then --iterations measured copies
  - Before each copy, cleans DST, then requests a sender prep and waits for
    the sender to acknowledge that exact request
  - Refuses an existing, non-empty DST that tx-bench did not mark
  - Each copy is one tx recv copy --verify none TX_ARGS... process, so its
    rusage is exact per run; --encrypt, -k, and -t also apply to the small
    tx recv get fetches of sender state
  - After each copy, DST/data is checked against the sender's files.tsv;
    --skip-write in TX_ARGS turns that check off
  - --forever keeps copying until interrupted; an interrupt always stops the
    copy in flight, flushes, and reports
  - --metrics streams: .jsonl and text get one record per run as it completes
    (a FIFO works, for exporting); .json is rewritten after each run
  - Exit codes: 0 ok, 1 failed run(s), 2 usage or --expect mismatch,
    3 corruption (stops immediately and keeps DST)

`)
		cf.PrintDefaults(stderr)
	}
	if code, done := parseFlags(cf, own); done {
		return code
	}
	cfg.flags.noteGiven(cf)
	pos := cf.Args()
	if len(pos) > 2 {
		return reportErr(stderr, usageErrorf("at most two positional arguments: SERVER DST (got %d)", len(pos)))
	}
	serverArg := "127.0.0.1:" + defaultPort + "/"
	if len(pos) > 0 {
		serverArg = pos[0]
	}
	var err error
	if cfg.server, cfg.benchRemote, err = parseServerArg(serverArg); err != nil {
		return reportErr(stderr, err)
	}
	dst := defaultDstDir
	if len(pos) > 1 {
		dst = pos[1]
	}
	if cfg.dst, err = filepath.Abs(dst); err != nil {
		return reportErr(stderr, err)
	}
	return reportCode(stderr, runRecvCopy(ctx, cfg))
}

func reportCode(stderr io.Writer, err error) int {
	if err == nil {
		return exitOK
	}
	return reportErr(stderr, err)
}

// parseServerArg splits host[:port][/bench/path].
func parseServerArg(raw string) (hostPort, benchRemote string, err error) {
	raw = strings.TrimPrefix(raw, "tx://")
	hostPart, path, hasPath := strings.Cut(raw, "/")
	benchRemote = "/"
	if hasPath && strings.Trim(path, "/") != "" {
		benchRemote = "/" + strings.Trim(path, "/")
	}
	if hostPart == "" {
		hostPart = "127.0.0.1"
	}
	if _, _, splitErr := net.SplitHostPort(hostPart); splitErr != nil {
		hostPart = net.JoinHostPort(strings.Trim(hostPart, "[]"), defaultPort)
	}
	if _, _, err := net.SplitHostPort(hostPart); err != nil {
		return "", "", usageErrorf("SERVER %q: %v", raw, err)
	}
	return hostPart, benchRemote, nil
}

// receiver is the state of one recv-copy.
type receiver struct {
	cfg        recvConfig
	tx         TxBinary
	pt         passThrough
	tmp        string
	bench      *dataset.BenchJSON
	server     ServerJSON
	entries    []dataset.Entry
	fp         string
	serverWait time.Duration
	rssEvery   time.Duration
	skipWrite  bool
	getSeq     int
}

func runRecvCopy(ctx context.Context, cfg recvConfig) error {
	f := cfg.flags
	if f.warmup < 0 || f.iterations < 1 {
		return usageErrorf("--warmup must be >= 0 and --iterations >= 1")
	}
	if f.oracle != "full" && f.oracle != "names" {
		return usageErrorf("--oracle must be full or names")
	}
	serverWait, err := time.ParseDuration(f.serverWait)
	if err != nil || serverWait <= 0 {
		return usageErrorf("--server-wait %q must be a positive duration", f.serverWait)
	}
	rssEvery, err := time.ParseDuration(f.rssInterval)
	if err != nil || rssEvery <= 0 {
		return usageErrorf("--trace-rss-interval %q must be a positive duration", f.rssInterval)
	}
	pt := parsePassThrough(cfg.txArgs)
	if err := pt.checkManaged(recvManaged(f.trace != "")); err != nil {
		return err
	}
	tx, err := resolveTx(f.tx)
	if err != nil {
		return err
	}
	if err := dataset.ClaimDir(cfg.dst, dataset.DstMarker); err != nil {
		return usageErrorf("DST %v; pick an empty or tx-bench-created directory", err)
	}
	metricsPath := f.metrics
	if !f.metricsGiven {
		ext := ".json"
		if f.forever {
			ext = ".jsonl" // a forever run's metrics must stream
		}
		metricsPath = "./tx-bench-" + time.Now().UTC().Format("20060102T150405Z") + ext
	}
	if metricsPath != "" && !report.CanStream(metricsPath) && (report.IsFIFO(metricsPath) || f.forever) {
		return usageErrorf("--metrics %s: a .json file is one object rewritten after each run, so it cannot be a FIFO or stream a --forever run; use .jsonl or a text name", metricsPath)
	}
	tmp, err := os.MkdirTemp(runtimeDir(), "tx-bench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	r := &receiver{cfg: cfg, tx: tx, pt: pt, tmp: tmp, serverWait: serverWait, rssEvery: rssEvery,
		skipWrite: pt.boolSet("skip-write")}
	out := cfg.out

	// 1. Check the dataset before transferring anything.
	if err := r.fetchState(ctx); err != nil {
		return err
	}
	want := f.expect
	if want == "" {
		want = r.bench.Fingerprint
	}
	if r.fp != want {
		return exitError{code: exitUsage, err: fmt.Errorf("files.tsv fingerprint %s does not match %s: wrong dataset or wrong host", r.fp, want)}
	}
	if r.server.Tx.Hash == tx.Hash {
		fmt.Fprintf(out, "tx      %s (%s)  matches sender\n", displayPath(tx.Path), tx.Short())
	} else {
		fmt.Fprintf(out, "warning: tx %s (%s) differs from the sender's %s (%s)\n", displayPath(tx.Path), tx.Short(), r.server.Tx.Path, r.server.Tx.Short())
	}
	if !r.server.Trace && f.trace != "" {
		fmt.Fprintln(out, "note: send-tree runs without --trace; the sender timeline is unavailable")
	}

	header := r.header()
	if f.trace != "" {
		if err := initClientTrace(f.trace, header); err != nil {
			return err
		}
	}
	var stream *report.MetricsStream
	if metricsPath != "" && report.CanStream(metricsPath) {
		stream = report.NewMetricsStream(metricsPath, header)
	}
	var runs []report.Run
	streamed := 0
	// A run's sender metrics arrive with the next run's prep (or the final
	// flush), so a run is streamed once the run after it has started.
	streamUpTo := func(n int) {
		for ; streamed < n; streamed++ {
			if stream != nil {
				stream.Run(runs[streamed])
			} else if metricsPath != "" {
				m := report.Metrics{Header: header, Runs: runs[:streamed+1], Summary: report.Aggregate(runs[:streamed+1])}
				if err := report.WriteMetrics(metricsPath, m); err != nil {
					fmt.Fprintf(out, "warning: --metrics: %v\n", err)
				}
			}
		}
	}
	var prevTID string
	var prevRun *report.Run
	exitCode := exitOK
	var fatal error
	r.table().Header()
	for i := 0; ; i++ {
		label, more := runLabel(i, f.warmup, f.iterations, f.forever)
		if !more || ctx.Err() != nil {
			break
		}
		measured := !strings.HasPrefix(label, "w")
		run, err := r.oneRun(ctx, label, measured, i == 0, prevTID, prevRun)
		if ctx.Err() != nil {
			// Interrupted: the copy in flight was stopped and does not count.
			if run != nil {
				fmt.Fprintf(out, "%-4s  interrupted\n", label)
			}
			break
		}
		if run != nil {
			runs = append(runs, *run)
			prevRun = &runs[len(runs)-1]
			prevTID = run.TID
			r.printRun(run)
			streamUpTo(len(runs) - 1)
		}
		if err != nil {
			fatal = err
			break
		}
		if !run.OK() {
			exitCode = exitFailedRuns
			if f.failFast {
				break
			}
		}
	}

	// The context may be canceled by now; the wrap-up gets its own.
	wrapCtx, cancelWrap := context.WithTimeout(context.Background(), r.serverWait+30*time.Second)
	defer cancelWrap()
	if fatal == nil || errors.As(fatal, new(corruptionError)) {
		// Record the last run's sender side, even after corruption.
		if prevRun != nil && prevTID != "" {
			if _, err := r.requestPrep(wrapCtx, flushRequest); err != nil {
				fmt.Fprintf(out, "warning: final flush: %v\n", err)
			} else {
				r.attachSender(wrapCtx, prevRun)
			}
		}
	}
	if f.stopSender {
		if _, err := r.get(wrapCtx, stopRequest, filepath.Join(r.tmp, "request")); err != nil {
			fmt.Fprintf(out, "warning: --stop-sender: %v\n", err)
		}
	}

	m := report.Metrics{Header: header, Runs: runs, Summary: report.Aggregate(runs)}
	if f.trace != "" {
		if err := finishClientTrace(f.trace, &m.Header, runs); err != nil {
			fmt.Fprintf(out, "warning: trace: %v\n", err)
		}
	}
	streamUpTo(len(runs))
	if stream != nil {
		if !stream.Close(m.Summary) {
			fmt.Fprintf(out, "warning: --metrics %s: no reader took every record\n", metricsPath)
		}
	}
	if len(runs) > 0 {
		fmt.Fprintln(out)
		report.RenderText(out, m)
		if metricsPath != "" {
			fmt.Fprintf(out, "metrics: %s\n", displayPath(mustAbs(metricsPath)))
		}
	}
	if fatal != nil {
		return fatal
	}
	if !f.keep {
		if err := r.cleanDst(); err != nil {
			return fmt.Errorf("final destination cleanup: %w", err)
		}
	}
	if exitCode != exitOK {
		return exitError{code: exitCode, err: fmt.Errorf("%d of %d measured runs failed", m.Summary.Failed, m.Summary.Measured)}
	}
	return nil
}

func mustAbs(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return a
}

// runLabel names the i-th run (0-based): warmups w1..wW, then measured runs
// 1..N, or 1, 2, ... without end when forever. more is false past the last.
func runLabel(i, warmup, iterations int, forever bool) (label string, more bool) {
	if i < warmup {
		return "w" + strconv.Itoa(i+1), true
	}
	n := i - warmup + 1
	return strconv.Itoa(n), forever || n <= iterations
}

// runtimeDir is a tmpfs for coordination fetches, so their syncfs never
// touches the filesystem DST is on.
func runtimeDir() string {
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		if st, err := os.Stat(d); err == nil && st.IsDir() {
			return d
		}
	}
	if st, err := os.Stat("/dev/shm"); err == nil && st.IsDir() {
		return "/dev/shm"
	}
	return os.TempDir()
}

// corruptionError stops the benchmark with exit code 3.
type corruptionError struct{ msg string }

func (e corruptionError) Error() string { return e.msg }

// fetchState fetches bench.json, server.json, files.tsv, and prep.json.
func (r *receiver) fetchState(ctx context.Context) error {
	var bench dataset.BenchJSON
	if err := r.fetchJSON(ctx, dataset.BenchJSONName, &bench); err != nil {
		return exitError{code: exitUsage, err: fmt.Errorf("fetch %s from %s: %w", dataset.BenchJSONName, r.cfg.server, err)}
	}
	r.bench = &bench
	if err := r.fetchJSON(ctx, serverJSONName, &r.server); err != nil {
		return exitError{code: exitUsage, err: err}
	}
	local := filepath.Join(r.tmp, dataset.FilesTSVName)
	if _, err := r.get(ctx, dataset.FilesTSVName, local); err != nil {
		return exitError{code: exitUsage, err: err}
	}
	entries, fp, err := dataset.ReadFilesTSVFile(local)
	if err != nil {
		return exitError{code: exitUsage, err: err}
	}
	r.entries, r.fp = entries, fp
	var pj PrepJSON
	if err := r.fetchJSON(ctx, prepJSONName, &pj); err != nil {
		return exitError{code: exitUsage, err: err}
	}
	return nil
}

func (r *receiver) fetchJSON(ctx context.Context, rel string, v any) error {
	local := filepath.Join(r.tmp, strings.ReplaceAll(rel, "/", "_"))
	if _, err := r.get(ctx, rel, local); err != nil {
		return err
	}
	return readJSON(local, v)
}

// getError is a failed coordination fetch.
type getError struct {
	rel     string
	msg     string
	refused bool
}

func (e *getError) Error() string { return "tx recv get " + e.rel + ": " + e.msg }

// get fetches BENCH_DIR/rel with tx recv get and returns the tid it saw.
func (r *receiver) get(ctx context.Context, rel, local string) (string, error) {
	r.getSeq++
	statsPath := filepath.Join(r.tmp, fmt.Sprintf("get.%d.stats.json", r.getSeq))
	args := []string{"recv", "get", "--skip-fsync", "--progress=false", "--stats", statsPath}
	args = append(args, r.pt.fetchArgs()...)
	args = append(args, "tx://"+r.cfg.server+remoteJoin(r.cfg.benchRemote, rel), local)
	cmd := exec.CommandContext(ctx, r.tx.Path, args...)
	out, runErr := cmd.CombinedOutput()
	var st txstats.Client
	statsErr := readJSON(statsPath, &st)
	_ = os.Remove(statsPath)
	if runErr == nil && statsErr == nil && st.Status == "ok" {
		return st.TID, nil
	}
	msg := st.Error
	if msg == "" {
		msg = strings.TrimSpace(lastLine(string(out)))
	}
	if msg == "" && runErr != nil {
		msg = runErr.Error()
	}
	return "", &getError{rel: rel, msg: msg, refused: strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset")}
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

// requestPrep fetches a request file and waits until prep.json acknowledges
// that exact fetch's tid.
func (r *receiver) requestPrep(ctx context.Context, request string) (*PrepJSON, error) {
	tid, err := r.get(ctx, request, filepath.Join(r.tmp, "request"))
	if err != nil {
		return nil, err
	}
	if tid == "" {
		return nil, fmt.Errorf("%s fetch reported no tid", request)
	}
	start := time.Now()
	deadline := start.Add(r.serverWait)
	nextNote := start.Add(10 * time.Second)
	backoff := 50 * time.Millisecond
	for {
		var pj PrepJSON
		err := r.fetchJSON(ctx, prepJSONName, &pj)
		if err == nil && pj.ServedTID != nil && *pj.ServedTID == tid {
			return &pj, nil
		}
		if now := time.Now(); now.After(nextNote) {
			nextNote = now.Add(10 * time.Second)
			why := "sender has not acknowledged it yet"
			if err != nil {
				why = err.Error()
			}
			fmt.Fprintf(r.cfg.out, "waiting %s/%s for the sender to acknowledge %s %s: %s\n",
				roundDur(now.Sub(start)), r.serverWait, filepath.Base(request), tid, why)
		}
		// Any failure, including connection refused while the sender
		// restarts, means "not yet" until the deadline.
		if time.Now().After(deadline) {
			if err == nil {
				err = fmt.Errorf("prep.json acknowledges tid %v", ptrStr(pj.ServedTID))
			}
			return nil, fmt.Errorf("prep request %s not acknowledged within %s: %w", tid, r.serverWait, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Second)
	}
}

// attachSender fetches runs/server-<tid>.json for a finished run.
func (r *receiver) attachSender(ctx context.Context, run *report.Run) {
	if run.TID == "" {
		return
	}
	start := time.Now()
	deadline := start.Add(r.serverWait)
	nextNote := start.Add(10 * time.Second)
	for {
		var sr ServerRunJSON
		err := r.fetchJSON(ctx, serverRunName(run.TID), &sr)
		if err == nil {
			run.Sender = sr.sender()
			if r.cfg.flags.trace != "" && sr.Trace {
				if err := r.fetchServerTrace(ctx, run); err != nil {
					fmt.Fprintf(r.cfg.out, "warning: sender trace for run %s: %v\n", run.Label, err)
				}
			}
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			fmt.Fprintf(r.cfg.out, "warning: %s not available within %s: %v\n", serverRunName(run.TID), r.serverWait, err)
			return
		}
		if now := time.Now(); now.After(nextNote) {
			nextNote = now.Add(10 * time.Second)
			fmt.Fprintf(r.cfg.out, "waiting %s/%s for %s: %v\n", roundDur(now.Sub(start)), r.serverWait, serverRunName(run.TID), err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// cleanDst removes DST/data and DST/.tx, then syncfs's DST's filesystem so
// writeback from the previous run does not leak into the next.
func (r *receiver) cleanDst() error {
	for _, name := range []string{"data", ".tx"} {
		path := filepath.Join(r.cfg.dst, name)
		if err := removeBenchTree(path); err != nil {
			return fmt.Errorf("remove %s: %w", path, err)
		}
	}
	f, err := os.Open(r.cfg.dst)
	if err != nil {
		return fmt.Errorf("open %s for syncfs: %w", r.cfg.dst, err)
	}
	defer f.Close()
	if err := unix.Syncfs(int(f.Fd())); err != nil {
		return fmt.Errorf("syncfs %s: %w", r.cfg.dst, err)
	}
	return nil
}

// removeBenchTree can remove copied directories whose source mode denies
// writes. WalkDir does not follow symlinks outside the marked destination.
func removeBenchTree(path string) error {
	if err := os.RemoveAll(path); err == nil {
		return nil
	}
	if err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.Chmod(p, info.Mode().Perm()|0o700)
	}); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

// oneRun cleans, requests a prep, fetches the previous run's sender metrics,
// copies, and verifies.
func (r *receiver) oneRun(ctx context.Context, label string, measured, first bool, prevTID string, prevRun *report.Run) (*report.Run, error) {
	f := r.cfg.flags
	if err := r.cleanDst(); err != nil {
		return nil, fmt.Errorf("prepare run %s: %w", label, err)
	}
	run := &report.Run{Label: label, Measured: measured}
	pj, err := r.requestPrep(ctx, prepRequest)
	if err != nil {
		// The sender writes the previous run's server-<tid>.json only while
		// handling a prep or flush, so without an acknowledged prep there is
		// nothing to wait for; the next prep or the final flush records it.
		run.Status, run.Error = report.StatusFailed, "unknown cache state: "+err.Error()
		return run, nil
	}
	run.Cache = report.Cache{Prepped: true, Warm: pj.Cache.Warm, Skew: pj.Cache.Skew, HotPct: pj.Cache.HotPct, MetaCold: pj.Cache.MetaCold,
		PrepMS: pj.Cache.DurationMS, Changed: pj.Changed}
	if prevRun != nil {
		r.attachSender(ctx, prevRun)
	}

	dstData := filepath.Join(r.cfg.dst, "data")
	statsPath := filepath.Join(r.tmp, "copy."+label+".stats.json")
	logPath := filepath.Join(r.tmp, "copy."+label+".log")
	var eventsPath string
	var p *proc
	start := time.Now()
	run.Start = start.UnixNano()
	if r.cfg.baseline == "rsync" {
		p, err = startProc("rsync", []string{"-a", "--delete", r.cfg.localSrc + "/", dstData + "/"}, logPath, false)
	} else {
		args := []string{"recv", "copy", "--verify", "none"}
		args = append(args, r.cfg.txArgs...)
		args = append(args, "--stats", statsPath)
		if f.trace != "" {
			eventsPath = filepath.Join(r.tmp, "copy."+label+".events.jsonl")
			args = append(args, "-p", eventsPath, "-f", "events")
		}
		if f.goTrace != "" {
			args = append(args, "--trace", insertSuffix(f.goTrace, label))
		}
		args = append(args, "tx://"+r.cfg.server+r.bench.Remote.Data, dstData)
		p, err = startProc(r.tx.Path, args, logPath, false)
	}
	if err != nil {
		return nil, err
	}
	var stopRSS func()
	if eventsPath != "" {
		stopRSS = sampleRSS(p, r.rssEvery, eventsPath+".rss", "c")
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		p.stop(10 * time.Second)
		if stopRSS != nil {
			stopRSS()
		}
		return run, nil
	}
	if stopRSS != nil {
		stopRSS()
	}
	run.Client = p.rusage()
	run.WallNS = int64(time.Since(start))
	run.ExitCode = p.exitCode()

	var st txstats.Client
	haveStats := readJSON(statsPath, &st) == nil
	if haveStats {
		run.TID = st.TID
		run.WallNS = st.WallNS
		run.ProbeNS, run.ManifestNS, run.DataNS, run.FinalizeNS = st.Phases.Probe, st.Phases.Manifest, st.Phases.Data, st.Phases.Finalize
		run.Files, run.Bytes, run.LogicalBytes, run.WireBytes, run.Windows = st.Files, st.Bytes, st.LogicalBytes, st.WireBytes, st.Windows
		run.Dials, run.SyncFallbacks, run.Reuses = st.Dials, st.SyncFallbacks, st.Reuses
		run.Heartbeats, run.HeartbeatFailures = st.Heartbeats, st.HeartbeatFailures
		run.AckRetries, run.RequestErrors = st.AckRetries, st.RequestErrors
	} else if r.cfg.baseline == "rsync" {
		run.DataNS = run.WallNS
		run.Files, run.Bytes = int64(r.bench.Files), r.bench.Bytes
		run.LogicalBytes, run.WireBytes = run.Bytes, run.Bytes
	}
	if eventsPath != "" {
		if err := appendClientTrace(f.trace, label, eventsPath); err != nil {
			fmt.Fprintf(r.cfg.out, "warning: trace for run %s: %v\n", label, err)
		}
	}
	if run.ExitCode != 0 {
		run.Status = report.StatusFailed
		run.Stderr = logExcerpt(logPath)
		run.Error = st.Error
		if !haveStats {
			// tx wrote no stats: it failed parsing its arguments, so its
			// first line says why.
			run.Error = firstLine(run.Stderr)
		}
		// A tx that fails before reporting a tid on the first run is a bad
		// flag, an unreachable host, or an auth failure: stop now.
		if first && run.TID == "" {
			return run, exitError{code: exitUsage, err: fmt.Errorf("tx recv copy failed before starting a transfer:\n%s", run.Stderr)}
		}
		return run, nil
	}

	if r.skipWrite {
		run.Verify.Mode = "skipped"
		run.Status = report.StatusOK
		return run, nil
	}
	changed := map[string]bool{}
	for _, c := range pj.Changed {
		changed[c] = true
	}
	verifyStart := time.Now()
	res, err := dataset.Verify(ctx, dstData, r.entries, dataset.VerifyOptions{
		Content: f.oracle == "full", Jobs: f.jobs,
		Excused: func(p string) bool { return changed[p] },
	})
	if err != nil {
		run.Status, run.Error = report.StatusFailed, "verify: "+err.Error()
		return run, nil
	}
	if f.trace != "" {
		if err := appendVerifyTrace(f.trace, label, run.TID, verifyStart, res); err != nil {
			fmt.Fprintf(r.cfg.out, "warning: trace: %v\n", err)
		}
	}
	run.Verify = report.Verify{Mode: f.oracle, Entries: res.Entries, Files: res.Files, Bytes: res.Bytes,
		Mismatches: res.MismatchCount + res.ExcusedCount, DurNS: res.DurationMillis * int64(time.Millisecond)}
	for _, m := range append(slices.Clone(res.Mismatches), res.Excused...) {
		run.Verify.Paths = append(run.Verify.Paths, m.String())
	}
	switch {
	case res.MismatchCount > 0:
		run.Status = report.StatusCorrupt
		run.Error = fmt.Sprintf("%d entries differ from files.tsv", res.MismatchCount)
		var b strings.Builder
		for _, m := range res.Mismatches {
			fmt.Fprintf(&b, "\n  %s", m)
		}
		return run, exitError{code: exitCorruption, err: corruptionError{msg: fmt.Sprintf(
			"CORRUPTION in run %s: %d entries under %s differ from files.tsv (DST kept for inspection):%s",
			label, res.MismatchCount, dstData, b.String())}}
	case res.ExcusedCount > 0:
		run.Status = report.StatusSourceChanged
		run.Error = fmt.Sprintf("source changed: %d paths changed under %s since import (first: %s)",
			res.ExcusedCount, r.bench.In.Path, res.Excused[0].Path)
	default:
		run.Status = report.StatusOK
	}
	return run, nil
}

// table is the receiver's run log: one fixed-width row per run, warmups
// (w1, w2, ...) first. A failed run's error follows its row.
func (r *receiver) table() report.Table {
	return report.Table{W: r.cfg.out, Cols: []report.Col{
		{Name: "run", Width: 4, Left: true}, {Name: "tid", Width: 8, Left: true}, {Name: "files", Width: 11},
		{Name: "bytes", Width: 9}, {Name: "wall", Width: 8}, {Name: "rate", Width: 11}, {Name: "hot", Width: 6},
		{Name: "status", Width: 14, Left: true},
	}}
}

func (r *receiver) printRun(run *report.Run) {
	status := run.Status
	if run.OK() {
		status = "ok"
		if run.Verify.Mode == "skipped" {
			status = "ok (unverified)"
		}
	}
	var files, bytes, wall, rate string
	if run.Bytes > 0 {
		files, bytes = commas(run.Files), humanBytes(run.Bytes)
	}
	if run.WallNS > 0 {
		wall = roundDur(time.Duration(run.WallNS)).String()
		if run.OK() && run.Bytes > 0 {
			rate = humanBytes(int64(run.RateBps())) + "/s"
		}
	}
	var hot string
	if run.Cache.Prepped {
		hot = fmt.Sprintf("%.1f%%", run.Cache.HotPct)
	}
	r.table().Row(run.Label, run.TID, files, bytes, wall, rate, hot, status)
	if run.Error != "" {
		fmt.Fprintf(r.cfg.out, "      error: %s\n", explainError(run.Error))
	}
}

// explainError adds the likely cause to errors whose message hides it.
func explainError(msg string) string {
	if strings.Contains(msg, "cannot assign requested address") {
		return msg + " (this host ran out of ephemeral ports; see dials and sync fallbacks, and net.ipv4.ip_local_port_range)"
	}
	return msg
}

func (r *receiver) header() report.Header {
	b := r.bench
	shape := ""
	switch {
	case b.In != nil:
		shape = "in=" + b.In.Path
	case b.Mix != nil:
		shape = "mix=" + *b.Mix
		for name, mix := range dataset.Profiles {
			if m, err := dataset.ParseMix(mix); err == nil && m.String() == *b.Mix {
				shape = "profile=" + name
			}
		}
	}
	h := report.Header{
		Version: 1, TxBench: Version, Start: time.Now().UTC().Format(time.RFC3339), Server: r.cfg.server + r.cfg.benchRemote,
		TxPath: r.tx.Path, TxHash: r.tx.Hash, SenderTx: r.server.Tx.Hash,
		TxArgs: r.cfg.txArgs, SenderArgs: r.server.TxArgs, Baseline: r.cfg.baseline,
		Compress: r.pt.setting("compress", "adapt"), Encrypt: r.pt.setting("encrypt", "none"), Oracle: r.cfg.flags.oracle,
		Dataset:    report.Dataset{Fingerprint: r.fp, Files: b.Files, Bytes: b.Bytes, Shape: shape},
		Client:     hostInfo(r.cfg.dst),
		SenderHost: r.server.Host, SenderTrace: r.server.Trace,
	}
	if b.In != nil {
		h.Dataset.ZstdRatio = b.In.ZstdRatioEst
	}
	if len(r.cfg.senderArgs) > 0 {
		h.SenderArgs = r.cfg.senderArgs
	}
	return h
}
