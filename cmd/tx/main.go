package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"filippo.io/age"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/cmd/filexfercli"
	filexfer "github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/utils"
)

var (
	fileListener = "127.0.0.1:3453"
	keysDir      = "/var/lib/pinch/keys"
	serverKey    *age.X25519Identity
	fsFileRate   = ""
	fsFileBurst  = "1MiB"
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
  send       Start the file transfer TCP server
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
		os.Exit(runFileSrv(os.Args[2:]))
	case "--help", "-h", "help":
		printUsage()
		os.Exit(0)
	default:
		printUsage()
		os.Exit(2)
	}
}

func runFileSrv(args []string) int {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `usage: tx send [options]

Start the file transfer TCP server.

Options:
  -l, --listen string         listen address (default "127.0.0.1:3453")
  -b, --bwlimit string        response rate limit for gentle transfers only; fast transfers do not respect it (e.g. 100MiB, 1000mbps)
      --bwlimit-burst string  rate limit burst size (default "1MiB")
      --gentle-cpu string     percent of server CPUs advertised for gentle concurrency (default "25%")
      --gentle-bw string      percent of observed link bandwidth used for gentle limiting (default "25%")
  -c, --chroot string         server root directory (default "/")
  -k, --keys string           age keys directory (default "/var/lib/pinch/keys")
      --require-auth          require AUTH before commands
      --require-auth-token string  allowlisted auth token (opaque string >8 bytes, repeatable); implies --require-auth
      --target-io-depth int   target IO depth per CPU advertised in PROBE (default 4)
      --trace string          write runtime/trace to this file
  -p, --progress-path string    progress output target; repeatable, use - for stdout
  -f, --progress-format string  progress format: json|int; 1 applies to all, or one per target (default json)
      --progress-interval string  progress write interval (default "1s")
      --disable-zero-copy              force buffered send path (for benchmarking)
`)
	}
	fs.StringVar(&fileListener, "listen", fileListener, "")
	fs.StringVar(&fileListener, "l", fileListener, "")
	fs.StringVar(&fsFileRate, "bwlimit", fsFileRate, "")
	fs.StringVar(&fsFileRate, "b", fsFileRate, "")
	fs.StringVar(&fsFileBurst, "bwlimit-burst", fsFileBurst, "")
	gentleCPURaw := fmt.Sprintf("%d%%", limit.DefaultGentleCPUPct)
	gentleBWRaw := fmt.Sprintf("%d%%", limit.DefaultGentleBWPct)
	fs.StringVar(&gentleCPURaw, "gentle-cpu", gentleCPURaw, "")
	fs.StringVar(&gentleBWRaw, "gentle-bw", gentleBWRaw, "")
	var chroot string
	fs.StringVar(&chroot, "chroot", "/", "")
	fs.StringVar(&chroot, "c", "/", "")
	defaultKeysDir := keysDir
	fs.StringVar(&keysDir, "keys", keysDir, "")
	fs.StringVar(&keysDir, "k", keysDir, "")
	requireAuth := fs.Bool("require-auth", false, "")
	var authTokenVals []string
	authTokens := filexfer.StringSliceFlag{Values: &authTokenVals}
	fs.Var(&authTokens, "require-auth-token", "")
	targetIODepth := fs.Int("target-io-depth", 4, "")
	disableZeroCopy := fs.Bool("disable-zero-copy", false, "")
	traceFile := fs.String("trace", "", "")
	var progressPathVals, progressFormatVals []string
	progressPaths := filexfer.StringSliceFlag{Values: &progressPathVals}
	progressFormats := filexfer.StringSliceFlag{Values: &progressFormatVals}
	var progressIntervalRaw string
	fs.Var(&progressPaths, "progress-path", "")
	fs.Var(&progressPaths, "p", "")
	fs.Var(&progressFormats, "progress-format", "")
	fs.Var(&progressFormats, "f", "")
	fs.StringVar(&progressIntervalRaw, "progress-interval", "1s", "")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	progressInterval, err := time.ParseDuration(progressIntervalRaw)
	if err != nil {
		log.Fatalf("Invalid --progress-interval: %v", err)
	}

	for _, tok := range authTokenVals {
		if vErr := aead.ValidateAuthToken(tok); vErr != nil {
			log.Fatalf("Invalid --require-auth-token: %v", vErr)
		}
	}
	if len(authTokenVals) > 0 && !*requireAuth {
		*requireAuth = true
	}
	if *requireAuth && len(authTokenVals) == 0 {
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
	progressTargets, err := filexfer.ResolveProgressTargets(progressPathVals, progressFormatVals)
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

	if *traceFile != "" {
		tf, err := os.Create(*traceFile)
		if err != nil {
			log.Fatalf("Failed to create trace file %s: %v", *traceFile, err)
		}
		defer tf.Close()
		if err := trace.Start(tf); err != nil {
			log.Fatalf("Failed to start trace: %v", err)
		}
		defer trace.Stop()
	}

	serverKey, err = loadOrGenerateServerKey(keysDir, keysDir == defaultKeysDir)
	if err != nil {
		log.Fatalf("Key setup failed: %v", err)
	}
	log.Printf("Public key %s", serverKey.Recipient().String())

	fileStreamLimiter, limiterErr := limit.NewLimiter(limit.Config{
		Rate:  fsFileRate,
		Burst: fsFileBurst,
	})
	if limiterErr != nil {
		log.Fatalf("Invalid rate limiter configuration: %v", limiterErr)
	}

	socketWriteBufBytes := utils.MaxSocketWriteBufferBytes()
	log.Printf("Detected ideal socket write buffer of size %d", socketWriteBufBytes)

	fileLn, err := net.Listen("tcp", fileListener)
	if err != nil {
		log.Fatalf("Failed to bind file listener at %s: %v", fileListener, err)
	}
	defer fileLn.Close()

	log.Printf("File transfer listener at %s (root=%s)", fileListener, chroot)
	if serveErr := ftcp.Serve(fileLn, ftcp.ServerOptions{
		RequireAuth:            *requireAuth,
		AllowedAuthTokens:      authTokenVals,
		ServerIdentity:         serverKey,
		Limiter:                fileStreamLimiter,
		GentleCPUPct:           gentleCPUPct,
		GentleBWPct:            gentleBWPct,
		SocketWriteBufferBytes: socketWriteBufBytes,
		RootDir:                chroot,
		ProgressTargets:        progressTargets,
		ProgressInterval:       progressInterval,
		DisableZeroCopy:        *disableZeroCopy,
		TargetIODepth:          *targetIODepth,
	}); serveErr != nil {
		log.Fatalf("File transfer listener stopped: %v", serveErr)
	}
	return 0
}
