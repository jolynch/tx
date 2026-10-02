package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/txstats"
)

// sendFlags are send-tree's own options beyond the prep flags.
type sendFlags struct {
	tx          string
	listen      string
	advertise   string
	auth        string
	metrics     string
	trace       string
	rssInterval string
	goTrace     string
}

func (s *sendFlags) register(cf *cliflags.Flags, listenDefault string) {
	cf.StringVar(&s.tx, "", "tx", "", "tx binary to fork (default: $TX_BIN, then tx next to tx-bench, then $PATH)")
	cf.StringVar(&s.listen, "l", "listen", listenDefault, "Listen address (host:port)")
	cf.StringVar(&s.advertise, "", "advertise", "", "Host printed in the recv-copy command (default: first non-loopback address)")
	cf.StringVar(&s.auth, "", "auth", "auto", "Require a generated auth token: auto|on|off; auto is on for --in and off for generated data. On forces encryption, and the printed recv-copy command carries the token")
	cf.StringVar(&s.metrics, "o", "metrics", "", "Append one record per run to this file; .json/.jsonl for JSON lines, else space-separated rows")
	cf.StringVar(&s.trace, "", "trace", "", "Write the sender event timeline (tx -f events) to this file; .json/.jsonl for JSON lines, else space-separated")
	cf.StringVar(&s.rssInterval, "", "trace-rss-interval", "100ms", "Interval for sampling the tx process's RSS into the trace")
	cf.StringVar(&s.goTrace, "", "go-trace", "", "Write runtime/trace output of each tx send tree process (tx --trace) to PATH.<k>")
}

// sendReady is what a running send-tree tells a recv-copy about itself.
type sendReady struct {
	addr        string // host:port as dialed
	benchRemote string
	fingerprint string
	token       string // empty when auth is off
	dataRoot    string
	dataBytes   int64
}

// sendTreeConfig is a fully parsed send-tree invocation.
type sendTreeConfig struct {
	prep     PrepFlags
	warm     dataset.WarmSpec
	flags    sendFlags
	benchDir string
	txArgs   []string
	out      io.Writer
	// local mode: no recv-copy command is printed, and onReady receives
	// the address once tx #1 listens.
	onReady func(sendReady)
	quiet   bool
}

// RunSendTreeCLI is 'tx-bench remote send-tree'.
func RunSendTreeCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	own, txArgs := splitPassThrough(args)
	cf := cliflags.New("send-tree")
	cf.SetOutput(stderr)
	cfg := sendTreeConfig{txArgs: txArgs, out: stdout}
	cfg.prep.register(cf)
	cfg.flags.register(cf, "0.0.0.0:"+defaultPort)
	cf.FlagSet().Usage = func() {
		fmt.Fprint(stderr, `usage: tx-bench remote send-tree [options] [BENCH_DIR] [-- TX_ARGS...]

Prepare a benchmark dataset, serve it with a forked 'tx send tree', and print
the matching 'tx-bench remote recv-copy' command.

  BENCH_DIR    bench root (default ./tx-bench-src)
  TX_ARGS      passed to tx send tree as given (see 'tx send tree --help')

Behavior:
  - Runs every enabled prep step once (see 'tx-bench prep --help'), then
    starts tx send tree --exit-after never TX_ARGS...
  - Each prep request from recv-copy stops that tx send tree, writes
    runs/server-<tid>.json with its exact rusage, re-runs the cache step,
    and starts a fresh tx send tree for the next run
  - BENCH_DIR must be absent, empty, or already marked by tx-bench
  - Serves until interrupted, or exits if tx send tree exits unexpectedly

`)
		cf.PrintDefaults(stderr)
	}
	if code, done := parseFlags(cf, own); done {
		return code
	}
	cfg.prep.noteGiven(cf)
	var err error
	if cfg.benchDir, err = positionalDir(cf.Args(), defaultBenchDir, "BENCH_DIR"); err != nil {
		return reportErr(stderr, err)
	}
	if err := runSendTree(ctx, cfg); err != nil {
		return reportErr(stderr, err)
	}
	return exitOK
}

// sender is the state of one send-tree.
type sender struct {
	cfg         sendTreeConfig
	tx          TxBinary
	ds          *dataset.Dataset
	chroot      string
	benchRemote string
	dataRemote  string
	prepRemote  string
	flushRemote string
	stopRemote  string
	auth        bool
	token       string
	txArgs      []string
	seq         int
	k           int
	cur         *serverProc
	rssEvery    time.Duration
}

// serverProc is one tx send tree #k and what its --stats said.
type serverProc struct {
	k         int
	p         *proc
	statsPath string
	eventPath string
	tail      *lineTailer
	dataTIDs  []string
	ends      map[string]txstats.ServerRecord
	starts    map[string]txstats.ServerRecord
	process   *txstats.ServerRecord
	stopRSS   func()
}

func runSendTree(ctx context.Context, cfg sendTreeConfig) error {
	out := cfg.out
	warm, err := cfg.prep.validate()
	if err != nil {
		return err
	}
	cfg.warm = warm
	pt := parsePassThrough(cfg.txArgs)
	if err := pt.checkManaged(sendManaged(cfg.flags.trace != "")); err != nil {
		return err
	}
	tx, err := resolveTx(cfg.flags.tx)
	if err != nil {
		return err
	}
	rssEvery, err := time.ParseDuration(cfg.flags.rssInterval)
	if err != nil || rssEvery <= 0 {
		return usageErrorf("--trace-rss-interval %q must be a positive duration", cfg.flags.rssInterval)
	}
	switch cfg.flags.auth {
	case "auto", "on", "off":
	default:
		return usageErrorf("--auth must be auto, on, or off")
	}
	if _, _, err := net.SplitHostPort(cfg.flags.listen); err != nil {
		return usageErrorf("--listen %q: %v", cfg.flags.listen, err)
	}

	res, err := runPrep(ctx, &cfg.prep, warm, cfg.benchDir, out)
	if err != nil {
		return err
	}
	s := &sender{cfg: cfg, tx: tx, ds: res.Dataset, rssEvery: rssEvery}
	s.chroot = cfg.benchDir
	if s.ds.Bench.In != nil {
		s.chroot = dataset.Chroot(cfg.benchDir, s.ds.Bench.In.Path)
	}
	s.benchRemote = dataset.RemotePath(s.chroot, cfg.benchDir)
	s.dataRemote = dataset.RemotePath(s.chroot, s.ds.DataRoot())
	s.prepRemote = remoteJoin(s.benchRemote, prepRequest)
	s.flushRemote = remoteJoin(s.benchRemote, flushRequest)
	s.stopRemote = remoteJoin(s.benchRemote, stopRequest)
	if s.ds.Bench.Remote.Bench != s.benchRemote || s.ds.Bench.Remote.Data != s.dataRemote {
		s.ds.Bench.Remote = dataset.Remote{Bench: s.benchRemote, Data: s.dataRemote}
		if err := dataset.WriteJSONAtomic(filepath.Join(cfg.benchDir, dataset.BenchJSONName), s.ds.Bench); err != nil {
			return err
		}
	}

	var warnings []string
	userTokens := pt.values("require-auth-token")
	s.auth = cfg.flags.auth == "on" || (cfg.flags.auth == "auto" && s.ds.Bench.In != nil)
	if len(userTokens) > 0 {
		s.auth = false // the user manages auth; their token is not echoed
	}
	if s.auth {
		if s.token, err = aead.NewAuthToken(); err != nil {
			return err
		}
	}
	host, _, _ := net.SplitHostPort(cfg.flags.listen)
	if !s.auth && len(userTokens) == 0 && !isLoopback(host) {
		warnings = append(warnings, fmt.Sprintf("auth is off and %s listens on %s: %s is readable by anyone who can reach the port",
			"tx send tree", cfg.flags.listen, s.chroot))
	}
	if s.ds.Bench.In != nil {
		if extra := chrootExtras(s.chroot, cfg.benchDir, s.ds.Bench.In.Path); len(extra) > 0 {
			warnings = append(warnings, fmt.Sprintf("the served root %s also exposes %s", s.chroot, strings.Join(extra, ", ")))
		}
	}

	s.txArgs = []string{"send", "tree", "--exit-after", "never"}
	s.txArgs = append(s.txArgs, cfg.txArgs...)
	if s.auth {
		s.txArgs = append(s.txArgs, "--require-auth-token", s.token)
	}

	if err := os.MkdirAll(filepath.Join(cfg.benchDir, runsDir), 0o755); err != nil {
		return err
	}
	for _, f := range []string{prepRequest, flushRequest, stopRequest} {
		if err := dataset.WriteFileAtomic(filepath.Join(cfg.benchDir, f), []byte("\n")); err != nil {
			return err
		}
	}
	sj := ServerJSON{
		TxBench: Version, Tx: tx, Host: hostInfo(s.ds.DataRoot()), Chroot: s.chroot, Auth: s.auth,
		CacheWarm: warm.Raw, WarmSkew: warm.Skew, WarmBlock: warm.Block, DropMeta: warm.DropMD,
		Trace: cfg.flags.trace != "", GoTrace: cfg.flags.goTrace != "", Listen: cfg.flags.listen,
		TxArgs: cfg.txArgs, Started: time.Now().UTC().Format(time.RFC3339), Warnings: warnings,
	}
	if err := dataset.WriteJSONAtomic(filepath.Join(cfg.benchDir, serverJSONName), sj); err != nil {
		return err
	}
	if err := s.writePrepJSON("startup", nil, res.Cache, nil); err != nil {
		return err
	}
	if cfg.flags.trace != "" {
		if err := initSenderTrace(cfg.flags.trace, sj.Host.Name); err != nil {
			return fmt.Errorf("--trace: %w", err)
		}
	}

	fmt.Fprintf(out, "cache-warm %s (hot %.1f%%)  auth %s  trace %s\n", warmLabel(warm), res.Cache.HotPct, authLabel(s), onOff(cfg.flags.trace != ""))
	for _, w := range warnings {
		fmt.Fprintf(out, "warning: %s\n", w)
	}
	if err := s.startNext(ctx); err != nil {
		return exitError{code: exitUsage, err: err}
	}
	addr := s.dialAddr()
	fmt.Fprintf(out, "tx #%d   %s (%s)  pid %d  serving tx://%s (root %s)\n", s.k, displayPath(tx.Path), tx.Short(), s.cur.p.pid(), addr, displayPath(s.chroot))
	if cfg.onReady != nil {
		cfg.onReady(sendReady{addr: addr, benchRemote: s.benchRemote, fingerprint: s.ds.Fingerprint, token: s.token,
			dataRoot: s.ds.DataRoot(), dataBytes: s.ds.Bench.Bytes})
	} else {
		fmt.Fprintf(out, "\nRun the bench client with:\n  %s\n\n", s.recvCommand(addr))
	}
	s.table().Header()
	return s.serve(ctx)
}

// table is the sender's event log: one fixed-width row per prep, flush, or
// run. tid is the requesting fetch for prep and flush rows and the data
// transfer for run rows; tx is the process that row started or measured.
func (s *sender) table() report.Table {
	return report.Table{W: s.cfg.out, Cols: []report.Col{
		{Name: "event", Width: 5, Left: true}, {Name: "seq", Width: 4}, {Name: "tx", Width: 4},
		{Name: "tid", Width: 8, Left: true}, {Name: "files", Width: 11}, {Name: "bytes", Width: 9},
		{Name: "dur", Width: 8}, {Name: "hot", Width: 6}, {Name: "rss", Width: 9}, {Name: "cpu", Width: 8},
	}}
}

func authLabel(s *sender) string {
	switch {
	case s.auth:
		return "on (token in the recv-copy command)"
	case s.ds.Bench.In == nil:
		return "off (generated data)"
	}
	return "off"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// chrootExtras lists top-level entries of chroot that lead to neither tree.
func chrootExtras(chroot, a, b string) []string {
	entries, err := os.ReadDir(chroot)
	if err != nil {
		return nil
	}
	first := func(p string) string {
		rel, err := filepath.Rel(chroot, p)
		if err != nil {
			return ""
		}
		return strings.Split(rel, string(filepath.Separator))[0]
	}
	keep := map[string]bool{first(a): true, first(b): true}
	var extra []string
	for _, e := range entries {
		if !keep[e.Name()] {
			extra = append(extra, e.Name())
		}
	}
	return extra
}

// dialAddr is the address a receiver should use.
func (s *sender) dialAddr() string {
	host, port, _ := net.SplitHostPort(s.cfg.flags.listen)
	if s.cfg.flags.advertise != "" {
		host = s.cfg.flags.advertise
	} else if host == "" || host == "0.0.0.0" || host == "::" {
		host = firstNonLoopback()
	}
	return net.JoinHostPort(host, port)
}

func firstNonLoopback() string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil && !ipn.IP.IsLinkLocalUnicast() {
				return ipn.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

func (s *sender) recvCommand(addr string) string {
	server := addr
	if s.benchRemote != "/" {
		server += s.benchRemote
	}
	cmd := fmt.Sprintf("%s remote recv-copy %s --expect %s", selfName(), shellQuote(server), s.ds.Fingerprint)
	if s.auth {
		cmd += " -- --encrypt auto -t " + shellQuote(s.token)
	}
	return cmd
}

func (s *sender) runPath(name string) string { return filepath.Join(s.cfg.benchDir, name) }

// startNext starts tx send tree #k+1 and waits until it listens.
func (s *sender) startNext(ctx context.Context) error {
	s.k++
	sp := &serverProc{
		k:         s.k,
		statsPath: s.runPath(fmt.Sprintf("runs/tx-send.%d.stats.jsonl", s.k)),
		ends:      map[string]txstats.ServerRecord{},
		starts:    map[string]txstats.ServerRecord{},
	}
	sp.tail = &lineTailer{path: sp.statsPath}
	args := slices.Clone(s.txArgs)
	// --exit-with stdin ties tx to tx-bench: if tx-bench dies, even by
	// SIGKILL, the kernel closes the lifeline pipe and tx shuts down cleanly.
	args = append(args, "--listen", s.cfg.flags.listen, "--stats", sp.statsPath, "--exit-with", "stdin")
	if s.cfg.flags.trace != "" {
		sp.eventPath = s.runPath(fmt.Sprintf("runs/tx-send.%d.events.jsonl", s.k))
		args = append(args, "-p", sp.eventPath, "-f", "events")
	}
	if s.cfg.flags.goTrace != "" {
		args = append(args, "--trace", insertSuffix(s.cfg.flags.goTrace, strconv.Itoa(s.k)))
	}
	args = append(args, s.chroot)
	_ = os.Remove(sp.statsPath)
	p, err := startProc(s.tx.Path, args, s.runPath(fmt.Sprintf("runs/tx-send.%d.log", s.k)), true)
	if err != nil {
		return err
	}
	sp.p = p
	s.cur = sp
	if err := waitListening(ctx, localDialAddr(s.cfg.flags.listen), p, 30*time.Second); err != nil {
		p.stop(5 * time.Second)
		return fmt.Errorf("tx send tree #%d: %w\n%s", s.k, err, logExcerpt(p.logPath))
	}
	if s.cfg.flags.trace != "" {
		sp.stopRSS = sampleRSS(p, s.rssEvery, sp.eventPath+".rss", "s")
	}
	return nil
}

// localDialAddr turns a wildcard listen address into one this host can dial.
func localDialAddr(listen string) string {
	host, port, _ := net.SplitHostPort(listen)
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	} else if host == "::" {
		host = "::1"
	}
	return net.JoinHostPort(host, port)
}

// insertSuffix puts .suffix before path's extension: trace.out -> trace.3.out.
func insertSuffix(path, suffix string) string {
	ext := filepath.Ext(path)
	return strings.TrimSuffix(path, ext) + "." + suffix + ext
}

// serve tails tx #k's --stats and handles prep and flush requests until ctx
// ends or tx exits on its own.
func (s *sender) serve(ctx context.Context) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var pending *txstats.ServerRecord // a prep/flush/stop start awaiting its end
	for {
		select {
		case <-ctx.Done():
			s.finish()
			return nil
		case <-s.cur.p.done:
			s.drainStats()
			return fmt.Errorf("tx send tree #%d exited unexpectedly (code %d):\n%s", s.k, s.cur.p.exitCode(), logExcerpt(s.cur.p.logPath))
		case <-tick.C:
		}
		recs, err := s.readStats()
		if err != nil {
			return err
		}
		for _, r := range recs {
			switch {
			case r.Rec == txstats.RecStart && (r.Path == s.prepRemote || r.Path == s.flushRemote || r.Path == s.stopRemote):
				if pending == nil {
					rec := r
					pending = &rec
				}
			case r.Rec == txstats.RecEnd && pending != nil && r.TID == pending.TID:
				kind := "prep"
				switch pending.Path {
				case s.flushRemote:
					kind = "flush"
				case s.stopRemote:
					// The receiver is done: record the last run and exit.
					tid := pending.TID
					if err := s.stopAndRecord(); err != nil {
						return err
					}
					s.table().Row("stop", "", "#"+strconv.Itoa(s.k), tid, "", "", "", "", "", "", "requested by the receiver")
					return nil
				}
				tid := pending.TID
				pending = nil
				if err := s.handlePrep(ctx, kind, tid); err != nil {
					return err
				}
			}
		}
	}
}

// readStats consumes new --stats records of the current process, tracking
// its data transfers.
func (s *sender) readStats() ([]txstats.ServerRecord, error) {
	lines, err := s.cur.tail.next()
	if err != nil {
		return nil, err
	}
	var out []txstats.ServerRecord
	for _, line := range lines {
		var r txstats.ServerRecord
		if json.Unmarshal([]byte(line), &r) != nil {
			continue
		}
		switch r.Rec {
		case txstats.RecStart:
			s.cur.starts[r.TID] = r
			if r.Path == s.dataRemote {
				s.cur.dataTIDs = append(s.cur.dataTIDs, r.TID)
				if len(s.cur.dataTIDs) == 2 {
					fmt.Fprintf(s.cfg.out, "warning: tx #%d is serving overlapping data transfers (%s); is a second receiver running?\n",
						s.cur.k, strings.Join(s.cur.dataTIDs, ", "))
				}
			}
		case txstats.RecEnd:
			s.cur.ends[r.TID] = r
		case txstats.RecProcess:
			rec := r
			s.cur.process = &rec
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *sender) drainStats() {
	_, _ = s.readStats()
}

// handlePrep restarts the server for the next run: stop tx #k, record its
// run, run the cache step (prep only), start tx #k+1, and acknowledge tid.
func (s *sender) handlePrep(ctx context.Context, kind, tid string) error {
	start := time.Now()
	s.emitPrepEvent("prep_start", report.KV{Key: "seq", Val: num(s.seq + 1)}, report.KV{Key: "kind", Val: kind},
		report.KV{Key: "warm", Val: s.cfg.warm.Raw}, report.KV{Key: "skew", Val: num(s.cfg.warm.Skew)})
	if err := s.stopAndRecord(); err != nil {
		return err
	}
	cache := dataset.CacheResult{Warm: s.cfg.warm.Raw, Skew: s.cfg.warm.Skew, BlockBytes: s.cfg.warm.Block}
	var changed []string
	if kind == "prep" {
		var err error
		if s.ds.Bench.In != nil {
			if changed, err = dataset.SourceChanges(s.cfg.benchDir, s.ds.Bench.In.Path); err != nil {
				return err
			}
			if len(changed) > 0 {
				fmt.Fprintf(s.cfg.out, "warning: %d paths under %s changed since import; runs touching them fail as \"source changed\"\n",
					len(changed), s.ds.Bench.In.Path)
			}
		}
		if cache, err = dataset.SetCache(ctx, s.ds, s.cfg.warm, seedOf(s.ds), s.cfg.prep.jobs()); err != nil {
			return err
		}
		for _, w := range cache.Warnings {
			fmt.Fprintf(s.cfg.out, "warning: %s\n", w)
		}
	} else {
		paths, sizes := s.ds.Files()
		cache.HotPct = dataset.Residency(paths, sizes)
	}
	if err := s.startNext(ctx); err != nil {
		return err
	}
	cache.Duration = time.Since(start)
	cache.DurationMS = cache.Duration.Milliseconds()
	if err := s.writePrepJSON(kind, &tid, cache, changed); err != nil {
		return err
	}
	s.emitPrepEvent("prep_end", report.KV{Key: "seq", Val: num(s.seq)}, report.KV{Key: "kind", Val: kind},
		report.KV{Key: "evicted", Val: num(cache.Evicted)}, report.KV{Key: "warmed", Val: num(cache.WarmedBytes)},
		report.KV{Key: "hot_pct", Val: num(cache.HotPct)}, report.KV{Key: "dur", Val: num(int64(cache.Duration))})
	s.table().Row(kind, strconv.Itoa(s.seq), "#"+strconv.Itoa(s.k), tid, "", "",
		roundDur(cache.Duration).String(), fmt.Sprintf("%.1f%%", cache.HotPct), "", "",
		fmt.Sprintf("pid %d", s.cur.p.pid()))
	return nil
}

// stopAndRecord stops the current tx with SIGTERM and writes
// runs/server-<tid>.json for each data transfer it served.
func (s *sender) stopAndRecord() error {
	sp := s.cur
	sp.p.stop(30 * time.Second)
	if sp.stopRSS != nil {
		sp.stopRSS()
	}
	s.drainStats()
	if sp.process == nil {
		fmt.Fprintf(s.cfg.out, "warning: tx #%d exited without a process record (code %d)\n", sp.k, sp.p.exitCode())
	}
	ru := sp.p.rusage()
	var traceRecs []report.TraceRecord
	if s.cfg.flags.trace != "" {
		var err error
		if traceRecs, err = s.collectTrace(sp); err != nil {
			fmt.Fprintf(s.cfg.out, "warning: trace of tx #%d: %v\n", sp.k, err)
		}
	}
	for _, tid := range sp.dataTIDs {
		run := ServerRunJSON{
			TID: tid, K: sp.k, PID: sp.p.pid(), ExitCode: sp.p.exitCode(), Rusage: ru,
			Transfer: sp.ends[tid], Overlap: len(sp.dataTIDs) > 1, Trace: s.cfg.flags.trace != "",
		}
		if sp.process != nil {
			run.Process = *sp.process
		}
		if s.cfg.flags.trace != "" {
			if err := s.writeServerTrace(sp, tid, traceRecs); err != nil {
				fmt.Fprintf(s.cfg.out, "warning: trace for tid %s: %v\n", tid, err)
			}
		}
		if err := dataset.WriteJSONAtomic(s.runPath(serverRunName(tid)), run); err != nil {
			return err
		}
		if s.cfg.flags.metrics != "" {
			if err := appendSenderMetrics(s.cfg.flags.metrics, run); err != nil {
				fmt.Fprintf(s.cfg.out, "warning: --metrics: %v\n", err)
			}
		}
		end := sp.ends[tid]
		var notes []string
		if end.Complete == nil || !*end.Complete {
			notes = append(notes, "incomplete")
		}
		if run.Overlap {
			notes = append(notes, "overlap")
		}
		if code := sp.p.exitCode(); code != 0 {
			notes = append(notes, fmt.Sprintf("exit %d", code))
		}
		s.table().Row("run", "", "#"+strconv.Itoa(sp.k), tid, commas(end.Files), humanBytes(end.Bytes),
			roundDur(time.Duration(end.DurNS)).String(), "", humanBytes(ru.MaxRSS), roundDur(time.Duration(ru.CPUNS())).String(),
			strings.Join(notes, " "))
	}
	return nil
}

func (s *sender) writePrepJSON(kind string, tid *string, cache dataset.CacheResult, changed []string) error {
	if tid != nil {
		s.seq++
	}
	pj := PrepJSON{Seq: s.seq, ServedTID: tid, Kind: kind, K: s.k, Cache: cache, Changed: changed,
		T: time.Now().UTC().Format(time.RFC3339Nano)}
	if s.cur != nil {
		pj.PID = s.cur.p.pid()
	}
	return dataset.WriteJSONAtomic(s.runPath(prepJSONName), pj)
}

// finish stops the last tx and records its run, for an interrupted
// send-tree.
func (s *sender) finish() {
	if s.cur == nil || s.cur.p.exited() {
		return
	}
	if err := s.stopAndRecord(); err != nil {
		fmt.Fprintf(s.cfg.out, "warning: %v\n", err)
	}
}

func appendSenderMetrics(path string, run ServerRunJSON) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if report.IsJSONPath(path) {
		data, err := json.Marshal(run)
		if err != nil {
			return err
		}
		_, err = f.Write(append(data, '\n'))
		return err
	}
	if st, err := f.Stat(); err == nil && st.Size() == 0 {
		fmt.Fprintln(f, "tid k pid exit user_ns sys_ns max_rss minflt majflt inblock oublock conns_accepted peak_conns heartbeats files bytes wire_bytes dur_ns overlap")
	}
	ru, t := run.Rusage, run.Transfer
	_, err = fmt.Fprintf(f, "%s %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %t\n",
		run.TID, run.K, run.PID, run.ExitCode, ru.UserNS, ru.SysNS, ru.MaxRSS, ru.MinFlt, ru.MajFlt, ru.InBlock, ru.OuBlock,
		run.Process.ConnsAccepted, run.Process.PeakConns, run.Process.Heartbeats, t.Files, t.Bytes, t.WireBytes, t.DurNS, run.Overlap)
	return err
}
