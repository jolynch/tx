package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"filippo.io/age"

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

// loadServerAgeIdentity loads an age identity from the key file in dir.
// It returns the identity if found, or an error if the file exists but is unreadable/invalid.
// If the key file does not exist, it returns (nil, nil).
func loadServerAgeIdentity(dir string) (*age.X25519Identity, error) {
	keyPath := path.Join(dir, "key")
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read key file %s: %w", keyPath, err)
	}
	lines := strings.Split(string(raw), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		identity, parseErr := age.ParseX25519Identity(line)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid existing key file %s: %w", keyPath, parseErr)
		}
		return identity, nil
	}
	return nil, fmt.Errorf("existing key file %s has no identity", keyPath)
}

// loadOrGenerateServerKey attempts to load a persistent key from dir.
// If the directory exists and contains a valid key, it returns that key.
// If the directory exists but the key is unreadable, it returns an error.
// If the directory does not exist and isDefault is true, it generates an ephemeral in-memory key.
// If the directory does not exist and isDefault is false (explicitly provided), it returns an error.
func loadOrGenerateServerKey(dir string, isDefault bool) (*age.X25519Identity, error) {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if !isDefault {
				return nil, fmt.Errorf("keys directory %s does not exist", dir)
			}
			// Default dir doesn't exist — generate ephemeral key.
			identity, genErr := age.GenerateX25519Identity()
			if genErr != nil {
				return nil, fmt.Errorf("generate ephemeral identity: %w", genErr)
			}
			log.Printf("Keys directory not found, using ephemeral in-memory key")
			return identity, nil
		}
		return nil, fmt.Errorf("stat keys directory %s: %w", dir, err)
	}

	// Directory exists — try to load.
	identity, err := loadServerAgeIdentity(dir)
	if err != nil {
		return nil, err
	}
	if identity != nil {
		return identity, nil
	}

	// Directory exists but no key file — generate and persist.
	identity, err = age.GenerateX25519Identity()
	if err != nil {
		return nil, fmt.Errorf("generate age identity: %w", err)
	}
	keyPath := path.Join(dir, "key")
	out, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open key file %s: %w", keyPath, err)
	}
	defer out.Close()
	fmt.Fprintf(out, "# created: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(out, "# public key: %s\n", identity.Recipient())
	fmt.Fprintf(out, "%s\n", identity)
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
