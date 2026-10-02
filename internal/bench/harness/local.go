package harness

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/cliflags"
)

// RunLocalCLI is 'tx-bench local': send-tree on 127.0.0.1 with an ephemeral
// port and recv-copy against it, both forking real tx processes.
func RunLocalCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	own, txArgs := splitPassThrough(args)
	cf := cliflags.New("local")
	cf.SetOutput(stderr)
	var (
		send        sendTreeConfig
		recv        recvConfig
		sendArgs    []string
		baseline    string
		sendMetrics string
		sendTrace   string
		sendGoTrace string
	)
	send.prep.register(cf)
	cf.StringVar(&send.flags.tx, "", "tx", "", "tx binary to fork on both sides (default: $TX_BIN, then tx next to tx-bench, then $PATH)")
	cf.StringVar(&send.flags.auth, "", "auth", "auto", "Require a generated auth token: auto|on|off; auto is on for --in and off for generated data")
	recv.flags.registerLocal(cf)
	cf.StringVar(&send.flags.rssInterval, "", "trace-rss-interval", "100ms", "Interval for sampling each tx process's RSS into the traces")
	cf.StringSliceVar(&sendArgs, "", "send-arg", "Argument passed through to tx send tree; repeatable, in order")
	cf.StringVar(&baseline, "", "baseline", "none", "Reference copy instead of tx recv copy: none|rsync")
	cf.StringVar(&sendMetrics, "", "send-metrics", "", "Sender --metrics file")
	cf.StringVar(&sendTrace, "", "send-trace", "", "Sender --trace file")
	cf.StringVar(&sendGoTrace, "", "send-go-trace", "", "Sender --go-trace file")
	cf.FlagSet().Usage = func() {
		fmt.Fprint(stderr, `usage: tx-bench local [options] [BENCH_DIR] [DST] [-- TX_ARGS...]

Run 'tx-bench remote send-tree' on 127.0.0.1 with an ephemeral port and
'tx-bench remote recv-copy' against it, both forking real tx processes.
Accepts every send-tree and recv-copy option; -j and --tx apply to both
sides, and the sender's output files take the --send- options below. TX_ARGS
go to tx recv copy; use --send-arg for tx send tree. Both sides share one page
cache, so --cache-warm warns unless the dataset and DST fit in MemAvailable.
Exits with recv-copy's exit code.

`)
		// The shared options are documented by send-tree and recv-copy;
		// list only local's own.
		help := cliflags.New("local")
		var s string
		var ss []string
		help.StringSliceVar(&ss, "", "send-arg", "Argument passed through to tx send tree; repeatable, in order")
		help.StringVar(&s, "", "baseline", "none", "Reference copy instead of tx recv copy: none|rsync")
		help.StringVar(&s, "", "send-metrics", "", "Sender --metrics file")
		help.StringVar(&s, "", "send-trace", "", "Sender --trace file")
		help.StringVar(&s, "", "send-go-trace", "", "Sender --go-trace file")
		help.PrintDefaults(stderr)
	}
	if code, done := parseFlags(cf, own); done {
		return code
	}
	send.prep.noteGiven(cf)
	recv.flags.noteGiven(cf)
	pos := cf.Args()
	if len(pos) > 2 {
		return reportErr(stderr, usageErrorf("at most two positional arguments: BENCH_DIR DST (got %d)", len(pos)))
	}
	benchDir, dst := defaultBenchDir, defaultDstDir
	if len(pos) > 0 {
		benchDir = pos[0]
	}
	if len(pos) > 1 {
		dst = pos[1]
	}
	var err error
	if send.benchDir, err = filepath.Abs(benchDir); err != nil {
		return reportErr(stderr, err)
	}
	if recv.dst, err = filepath.Abs(dst); err != nil {
		return reportErr(stderr, err)
	}
	switch baseline {
	case "none":
	case "rsync":
		if _, err := exec.LookPath("rsync"); err != nil {
			return reportErr(stderr, usageErrorf("--baseline rsync: rsync not found on $PATH"))
		}
	default:
		return reportErr(stderr, usageErrorf("--baseline must be none or rsync"))
	}

	// Refuse an unusable DST before anything starts serving.
	if err := dataset.ClaimDir(recv.dst, dataset.DstMarker); err != nil {
		return reportErr(stderr, usageErrorf("DST %v; pick an empty or tx-bench-created directory", err))
	}
	port, err := freePort()
	if err != nil {
		return reportErr(stderr, err)
	}
	send.flags.listen = "127.0.0.1:" + strconv.Itoa(port)
	send.flags.metrics, send.flags.trace, send.flags.goTrace = sendMetrics, sendTrace, sendGoTrace
	send.txArgs = sendArgs
	send.out = stderr
	recv.flags.tx = send.flags.tx
	recv.flags.jobs = send.prep.Jobs
	recv.flags.rssInterval = send.flags.rssInterval
	recv.txArgs, recv.senderArgs, recv.out, recv.baseline = txArgs, sendArgs, stdout, baseline

	if !send.prep.Generating() && send.prep.In == "" {
		// Nothing to generate: still refuse early when there is no dataset.
		if _, err := dataset.Load(send.benchDir); err != nil {
			return reportErr(stderr, fmt.Errorf("%w; create one with --size 256MiB or --in DIR", err))
		}
	}

	// The sender outlives an interrupt until the receiver has flushed the
	// last run, so it is stopped explicitly rather than by ctx.
	sendCtx, stopSend := context.WithCancel(context.Background())
	defer stopSend()
	ready := make(chan sendReady, 1)
	send.onReady = func(r sendReady) { ready <- r }
	sendErr := make(chan error, 1)
	go func() { sendErr <- runSendTree(sendCtx, send) }()

	var info sendReady
	select {
	case info = <-ready:
	case err := <-sendErr:
		if err == nil {
			err = fmt.Errorf("send-tree exited before serving")
		}
		return reportErr(stderr, err)
	case <-ctx.Done():
		return reportErr(stderr, ctx.Err())
	}
	recv.server, recv.benchRemote = info.addr, info.benchRemote
	recv.localSrc = info.dataRoot
	if recv.flags.expect == "" {
		recv.flags.expect = info.fingerprint
	}
	if info.token != "" {
		recv.txArgs = append([]string{"--encrypt", "auto", "-t", info.token}, recv.txArgs...)
	}
	if send.prep.CacheWarm != "" {
		if mem, err := dataset.ReadMemInfo(); err == nil && info.dataBytes*2 > mem.Available {
			fmt.Fprintf(stderr, "warning: both sides share this host's page cache, and the dataset plus DST (%s) exceed MemAvailable (%s); --cache-warm is approximate\n",
				humanBytes(info.dataBytes*2), humanBytes(mem.Available))
		}
	}

	recvErr := runRecvCopy(ctx, recv)
	stopSend()
	if err := <-sendErr; err != nil {
		fmt.Fprintf(stderr, "tx-bench: send-tree: %v\n", err)
	}
	return reportCode(stderr, recvErr)
}

// registerLocal registers recv-copy's options that local does not share
// with send-tree (-j and --tx are shared and registered by the caller).
func (r *recvFlags) registerLocal(cf *cliflags.Flags) {
	cf.IntVar(&r.warmup, "w", "warmup", 1, "Unmeasured runs before measuring")
	cf.IntVar(&r.iterations, "n", "iterations", 3, "Measured runs")
	cf.StringVar(&r.expect, "e", "expect", "", "Dataset fingerprint; checked against the served files.tsv (default: the one send-tree prepared)")
	cf.StringVar(&r.oracle, "", "oracle", "full", "Check of DST/data against files.tsv after each run: full|names")
	cf.StringVar(&r.metrics, "o", "metrics", "", "Metrics file; .json/.jsonl for JSON, else space-separated rows; empty disables (default: ./tx-bench-<UTC timestamp>.json)")
	cf.StringVar(&r.trace, "", "trace", "", "Write the client event timeline (tx -f events) to this file; fetched sender events go to its .server sibling")
	cf.StringVar(&r.goTrace, "", "go-trace", "", "Write runtime/trace output of each tx recv copy (tx --trace) to PATH.<run>")
	cf.StringVar(&r.serverWait, "", "server-wait", "2m", "How long to wait for a prep acknowledgement or runs/server-<tid>.json")
	cf.BoolVar(&r.failFast, "", "fail-fast", false, "Stop at the first failed run")
	cf.BoolVar(&r.keep, "", "keep", false, "Keep DST after the last run")
	cf.BoolVar(&r.forever, "", "forever", false, "After the warmups, copy and verify until interrupted instead of --iterations times; metrics default to .jsonl")
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}
