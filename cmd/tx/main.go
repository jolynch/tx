package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"filippo.io/age"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/cmd/filexfercli"
	"github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/utils"
)

const defaultFileListener = "127.0.0.1:3453"

var (
	keysDir     = "/var/lib/pinch/keys"
	serverKey   *age.X25519Identity
	fsFileRate  = ""
	fsFileBurst = "1MiB"
)

var errMissingSendCommand = errors.New("missing send command")

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
  recv       File transfer CLI client

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

func isSendSubcommand(s string) bool {
	switch s {
	case "serve":
		return true
	}
	return false
}

func printSendUsage(w io.Writer) {
	fmt.Fprintf(w, `usage: tx send [<addr>] <command> [options]

Commands:
  serve      Start the file transfer TCP server

Default listen address: %s
Run 'tx send <command> --help' for command-specific options.
`, defaultFileListener)
}

func splitSendCommand(args []string) (addr, cmd string, rest []string, err error) {
	if len(args) == 0 {
		return "", "", nil, errMissingSendCommand
	}
	if isSendSubcommand(args[0]) {
		return defaultFileListener, args[0], args[1:], nil
	}
	if err := utils.ValidateHostPort(args[0]); err != nil {
		return "", "", nil, fmt.Errorf("invalid server-addr: %w", err)
	}
	if len(args) == 1 {
		return "", "", nil, errMissingSendCommand
	}
	return args[0], args[1], args[2:], nil
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

	addr, cmd, cmdArgs, err := splitSendCommand(args)
	if err != nil {
		if !errors.Is(err, errMissingSendCommand) {
			fmt.Fprintln(stderr, err)
		}
		printSendUsage(stderr)
		return 2
	}

	switch cmd {
	case "serve":
		return runSendServeCLI(addr, cmdArgs, stdout, stderr)
	case "--help", "-h", "help":
		printSendUsage(stderr)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", cmd)
		printSendUsage(stderr)
		return 2
	}
}

func runSendServeCLI(addr string, args []string, _ io.Writer, stderr io.Writer) int {
	cf := cliflags.New("serve")
	cf.SetOutput(stderr)

	gentleCPUDefault := fmt.Sprintf("%d%%", limit.DefaultGentleCPUPct)
	gentleBWDefault := fmt.Sprintf("%d%%", limit.DefaultGentleBWPct)

	var (
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
		progressPaths     []string
		progressFormats   []string
		progressIntervalR string
	)

	cf.StringVar(&rate, "b", "bwlimit", "", "Response rate limit for gentle transfers only; fast transfers do not respect it (e.g. 100MiB, 1000mbps)")
	cf.StringVar(&burst, "", "bwlimit-burst", fsFileBurst, "Rate limit burst size")
	cf.StringVar(&gentleCPURaw, "", "gentle-cpu", gentleCPUDefault, "Percent of server CPUs advertised for gentle concurrency")
	cf.StringVar(&gentleBWRaw, "", "gentle-bw", gentleBWDefault, "Percent of observed link bandwidth used for gentle limiting")
	cf.StringVar(&keysDirFlag, "k", "keys", keysDir, "Age keys directory")
	cf.BoolVar(&requireAuth, "", "require-auth", false, "Require AUTH before commands")
	cf.StringSliceVar(&authTokenVals, "", "require-auth-token", "Allowlisted auth token (opaque string >8 bytes, repeatable); implies --require-auth")
	cf.IntVar(&targetIODepth, "", "target-io-depth", 4, "Target IO depth per CPU advertised in PROBE")
	cf.BoolVar(&disableZeroCopy, "", "disable-zero-copy", false, "Force buffered send path (for benchmarking)")
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	cf.StringSliceVar(&progressPaths, "p", "progress-path", "Progress output target; repeatable, use - for stdout")
	cf.StringSliceVar(&progressFormats, "f", "progress-format", "Progress format: json|int; 1 applies to all targets, or one per target (default json)")
	cf.StringVar(&progressIntervalR, "", "progress-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")

	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx send [addr] serve [options] [CHROOT]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Start the file transfer TCP server.")
		fmt.Fprintln(stderr)
		fmt.Fprintf(stderr, "  addr      listen address (default %q)\n", defaultFileListener)
		fmt.Fprintln(stderr, "  CHROOT    server root directory (default: current working directory)")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}

	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	chroot := ""
	switch positional := cf.Args(); len(positional) {
	case 0:
	case 1:
		chroot = positional[0]
	default:
		fmt.Fprintf(stderr, "serve accepts at most one positional argument: CHROOT (got %d)\n", len(positional))
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

	fileLn, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to bind file listener at %s: %v", addr, err)
	}
	defer fileLn.Close()

	log.Printf("File transfer listener at %s (root=%s)", addr, chroot)
	if serveErr := ftcp.Serve(fileLn, ftcp.ServerOptions{
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
	}); serveErr != nil {
		log.Fatalf("File transfer listener stopped: %v", serveErr)
	}
	return 0
}
