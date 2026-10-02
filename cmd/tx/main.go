package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"filippo.io/age"
	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/cmd/filexfercli"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/txstats"
	"github.com/jolynch/tx/internal/utils"
)

const defaultFileListener = "127.0.0.1:3453"

// sendTreeStdin is what --exit-with stdin watches; tests replace it.
var sendTreeStdin = os.Stdin

// --exit-with modes. "none" keeps tx's lifetime its own; every other mode
// ties it to something outside the process. New modes (pid:N, fd:N) can be
// added without changing the flag.
const (
	exitWithNone  = "none"
	exitWithStdin = "stdin"
)

// parseExitWith validates an --exit-with mode.
func parseExitWith(raw string) (string, error) {
	switch mode := strings.ToLower(strings.TrimSpace(raw)); mode {
	case exitWithNone, exitWithStdin:
		return mode, nil
	}
	return "", fmt.Errorf("unsupported --exit-with %q (supported: none, stdin)", raw)
}

// checkExitWithStdin requires stdin to be a pipe or socket: anything else
// (/dev/null, a terminal, a file) would make tx exit at once or never.
func checkExitWithStdin(f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("--exit-with stdin: stat stdin: %w", err)
	}
	if st.Mode()&(os.ModeNamedPipe|os.ModeSocket) == 0 {
		return fmt.Errorf("--exit-with stdin needs stdin to be a pipe or socket held open by the process tx should exit with; it is %v", st.Mode().Type())
	}
	return nil
}

var (
	keysDir     = "/var/lib/tx/keys"
	serverKey   *age.X25519Identity
	fsFileRate  = ""
	fsFileBurst = "1MiB"
)

func parsePercentFlag(raw string, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasSuffix(raw, "%") {
		return 0, fmt.Errorf("%s must be an integer percent like 25%%", name)
	}
	value, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(raw, "%")))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer percent like 25%%", name)
	}
	if value < 1 || value > 100 {
		return 0, fmt.Errorf("%s must be between 1%% and 100%%", name)
	}
	return value, nil
}

func loadOrGenerateServerKey(dir string, isDefault bool) (*age.X25519Identity, error) {
	identity, ephemeral, err := aead.LoadOrGenerateAgeIdentity(dir, isDefault)
	if err != nil {
		return nil, err
	}
	if ephemeral {
		log.Printf("Keys directory not found, using ephemeral in-memory key")
	}
	return identity, nil
}

func printUsage() {
	fmt.Fprint(os.Stderr, `usage: tx <command> [options]

Commands:
  send       File transfer server
  recv       File transfer client

Run 'tx <command> --help' for command-specific options.
`)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "recv":
		os.Exit(filexfercli.RunCLI(os.Args[2:], os.Stdout, os.Stderr))
	case "send":
		os.Exit(runSendCLI(os.Args[2:], os.Stdout, os.Stderr))
	case "--help", "-h", "help":
		printUsage()
		os.Exit(0)
	default:
		printUsage()
		os.Exit(2)
	}
}

func printSendUsage(w io.Writer) {
	fmt.Fprint(w, `usage: tx send <command> [options]

Commands:
  tree       Offer a tree (directory) via a FTCP server

Run 'tx send <command> --help' for command-specific options.
`)
}

func runSendCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		printSendUsage(stderr)
		return 2
	}

	if args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		printSendUsage(stderr)
		return 0
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "tree":
		return runSendTreeCLI(cmdArgs, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", cmd)
		printSendUsage(stderr)
		return 2
	}
}

func runSendTreeCLI(args []string, _ io.Writer, stderr io.Writer) int {
	return runSendTree(args, stderr, ftcp.Serve)
}

func runSendTree(args []string, stderr io.Writer, serve func(net.Listener, ftcp.ServerOptions) error) int {
	cf := cliflags.New("tree")
	cf.SetOutput(stderr)

	gentleCPUDefault := fmt.Sprintf("%d%%", limit.DefaultGentleCPUPct)
	gentleBWDefault := fmt.Sprintf("%d%%", limit.DefaultGentleBWPct)

	var (
		listenAddr        string
		rate              string
		burst             string
		gentleCPURaw      string
		gentleBWRaw       string
		keysDirFlag       string
		requireAuth       bool
		authTokenVals     []string
		targetIODepth     int
		disableZeroCopy   bool
		traceFile         string
		exitAfterRaw      string
		idleTimeoutRaw    string
		progressPaths     []string
		progressFormats   []string
		progressIntervalR string
		statsPath         string
		exitWithRaw       string
	)

	cf.StringVar(&listenAddr, "", "listen", defaultFileListener, "Listen address (host:port)")
	cf.StringVar(&rate, "b", "bwlimit", "", "Response rate limit for gentle transfers only; fast transfers do not respect it (e.g. 100MiB, 1000mbps)")
	cf.StringVar(&burst, "", "bwlimit-burst", fsFileBurst, "Rate limit burst size")
	cf.StringVar(&gentleCPURaw, "", "gentle-cpu", gentleCPUDefault, "Percent of server CPUs advertised for gentle concurrency")
	cf.StringVar(&gentleBWRaw, "", "gentle-bw", gentleBWDefault, "Percent of observed link bandwidth used for gentle limiting")
	cf.StringVar(&keysDirFlag, "k", "keys", keysDir, "Age keys directory")
	cf.BoolVar(&requireAuth, "", "require-auth", false, "Require AUTH before commands")
	cf.StringSliceVar(&authTokenVals, "", "require-auth-token", "Allowlisted auth token (opaque string >8 bytes, repeatable); implies --require-auth")
	cf.IntVar(&targetIODepth, "", "target-io-depth", 4, "Target IO depth per CPU advertised in PROBE")
	cf.BoolVar(&disableZeroCopy, "", "disable-zero-copy", false, "Force buffered send path (for benchmarking)")
	cf.StringVar(&exitAfterRaw, "", "exit-after", "60s", "Exit duration after transfer completes; 'never' to run forever (e.g. 5s, 1m)")
	cf.StringVar(&idleTimeoutRaw, "", "idle-timeout", "60s", "Close kept-alive connections idle for this duration; 0 disables keep-alive (e.g. 10s, 1m)")
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	cf.StringSliceVar(&progressPaths, "p", "progress-path", "Progress output target; repeatable, use - for stdout")
	cf.StringSliceVar(&progressFormats, "f", "progress-format", "Progress format: json|int|events; 1 applies to all targets, or one per target (default json)")
	cf.StringVar(&progressIntervalR, "", "progress-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	cf.StringVar(&statsPath, "", "stats", "", "Write JSON-lines transfer and process statistics to this file")
	cf.StringVar(&exitWithRaw, "", "exit-with", exitWithNone, "Shut down cleanly, as on SIGTERM, when this ends: none|stdin. stdin exits when stdin (a pipe or socket) closes, tying tx to whoever holds it")

	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx send tree [--listen <addr>] [options] [CHROOT]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Offer a tree (directory) via a FTCP server.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "  CHROOT    server root directory (default: current working directory); requests cannot")
		fmt.Fprintln(stderr, "            leave it, symlinks inside it are followed")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}

	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	if err := utils.ValidateHostPort(listenAddr); err != nil {
		fmt.Fprintf(stderr, "invalid --listen: %v\n", err)
		return 2
	}
	exitWith, err := parseExitWith(exitWithRaw)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if exitWith == exitWithStdin {
		if err := checkExitWithStdin(sendTreeStdin); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}

	chroot := ""
	switch positional := cf.Args(); len(positional) {
	case 0:
	case 1:
		chroot = positional[0]
	default:
		fmt.Fprintf(stderr, "tree accepts at most one positional argument: CHROOT (got %d)\n", len(positional))
		cf.FlagSet().Usage()
		return 2
	}
	if chroot == "" {
		cwd, cwdErr := os.Getwd()
		if cwdErr != nil {
			log.Fatalf("Failed to determine current working directory for default chroot: %v", cwdErr)
		}
		chroot = cwd
	}

	var exitAfter time.Duration
	if exitAfterRaw != "never" {
		var parseErr error
		exitAfter, parseErr = time.ParseDuration(exitAfterRaw)
		if parseErr != nil {
			log.Fatalf("Invalid --exit-after: %v", parseErr)
		}
	}

	var idleTimeout time.Duration
	if idleTimeoutRaw != "0" {
		var parseErr error
		idleTimeout, parseErr = time.ParseDuration(idleTimeoutRaw)
		if parseErr != nil || idleTimeout < 0 {
			log.Fatalf("Invalid --idle-timeout: %v", parseErr)
		}
	}

	progressInterval, err := time.ParseDuration(progressIntervalR)
	if err != nil {
		log.Fatalf("Invalid --progress-interval: %v", err)
	}

	for _, tok := range authTokenVals {
		if vErr := aead.ValidateAuthToken(tok); vErr != nil {
			log.Fatalf("Invalid --require-auth-token: %v", vErr)
		}
	}
	if len(authTokenVals) > 0 && !requireAuth {
		requireAuth = true
	}
	if requireAuth && len(authTokenVals) == 0 {
		gen, tokErr := aead.NewAuthToken()
		if tokErr != nil {
			log.Fatalf("Generate auth token: %v", tokErr)
		}
		log.Printf("generated auth token: %s", gen)
		authTokenVals = append(authTokenVals, gen)
	}
	if len(authTokenVals) > 0 {
		log.Printf("auth required (%d identities/tokens allowlisted)", len(authTokenVals))
	}
	progressTargets, err := cliflags.ResolveProgressTargets(progressPaths, progressFormats)
	if err != nil {
		log.Fatalf("Invalid --progress-path/--progress-format: %v", err)
	}
	gentleCPUPct, err := parsePercentFlag(gentleCPURaw, "--gentle-cpu")
	if err != nil {
		log.Fatalf("Invalid --gentle-cpu: %v", err)
	}
	gentleBWPct, err := parsePercentFlag(gentleBWRaw, "--gentle-bw")
	if err != nil {
		log.Fatalf("Invalid --gentle-bw: %v", err)
	}

	if traceFile != "" {
		tf, err := os.Create(traceFile)
		if err != nil {
			log.Fatalf("Failed to create trace file %s: %v", traceFile, err)
		}
		defer tf.Close()
		if err := trace.Start(tf); err != nil {
			log.Fatalf("Failed to start trace: %v", err)
		}
		defer trace.Stop()
	}

	keysDirIsDefault := keysDirFlag == keysDir
	serverKey, err = loadOrGenerateServerKey(keysDirFlag, keysDirIsDefault)
	if err != nil {
		log.Fatalf("Key setup failed: %v", err)
	}
	log.Printf("Public key %s", serverKey.Recipient().String())

	fsFileRate = rate
	fsFileBurst = burst
	fileStreamLimiter, limiterErr := limit.NewLimiter(limit.Config{
		Rate:  fsFileRate,
		Burst: fsFileBurst,
	})
	if limiterErr != nil {
		log.Fatalf("Invalid rate limiter configuration: %v", limiterErr)
	}

	socketWriteBufBytes := utils.MaxSocketWriteBufferBytes()
	log.Printf("Detected ideal socket write buffer of size %d", socketWriteBufBytes)

	var sink *events.Sink
	var stats *txstats.ServerWriter
	eventTargets := cliflags.EventTargets(progressTargets)
	if statsPath != "" || len(eventTargets) > 0 {
		sink = events.NewSink()
	}
	if statsPath != "" {
		stats, err = txstats.NewServerWriter(statsPath, sink)
		if err != nil {
			log.Fatalf("Invalid --stats: %v", err)
		}
	}
	if len(eventTargets) > 0 {
		eventWriter := events.StartTargetWriter(sink, events.Header{Side: "s"}, progressInterval, eventTargets)
		defer eventWriter.Stop()
	}

	fileLn, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to bind file listener at %s: %v", listenAddr, err)
	}
	defer fileLn.Close()

	// SIGTERM, SIGINT, and the --exit-with target ending all close
	// the listener, so Serve returns and flushes its progress targets, and
	// the deferred stats close writes the final records before the process
	// exits.
	exitReason := "exit-after"
	var exitMu sync.Mutex
	var shutdownOnce sync.Once
	shutdown := func(reason string) {
		shutdownOnce.Do(func() {
			exitMu.Lock()
			exitReason = reason
			exitMu.Unlock()
			log.Printf("%s, shutting down", reason)
			fileLn.Close()
		})
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)
	go func() {
		sig, ok := <-sigCh
		if !ok {
			return
		}
		shutdown(strings.ToLower(unix.SignalName(sig.(syscall.Signal))))
	}()
	if exitWith == exitWithStdin {
		go func() {
			// Whatever arrives is discarded; only EOF (or a read error, as
			// when the writer dies) matters.
			_, _ = io.Copy(io.Discard, sendTreeStdin)
			shutdown("exit-with:stdin")
		}()
	}
	if stats != nil {
		defer func() {
			exitMu.Lock()
			reason := exitReason
			exitMu.Unlock()
			if err := stats.Close(reason); err != nil {
				log.Printf("write --stats: %v", err)
			}
		}()
	}

	log.Printf("File transfer listener at %s (root=%s)", listenAddr, chroot)
	if serveErr := serve(fileLn, ftcp.ServerOptions{
		RequireAuth:            requireAuth,
		AllowedAuthTokens:      authTokenVals,
		ServerIdentity:         serverKey,
		Limiter:                fileStreamLimiter,
		GentleCPUPct:           gentleCPUPct,
		GentleBWPct:            gentleBWPct,
		SocketWriteBufferBytes: socketWriteBufBytes,
		RootDir:                chroot,
		ProgressTargets:        progressTargets,
		ProgressInterval:       progressInterval,
		DisableZeroCopy:        disableZeroCopy,
		TargetIODepth:          targetIODepth,
		ExitAfter:              exitAfter,
		KeepAliveTimeout:       idleTimeout,
		Events:                 sink,
	}); serveErr != nil {
		log.Fatalf("File transfer listener stopped: %v", serveErr)
	}
	return 0
}
