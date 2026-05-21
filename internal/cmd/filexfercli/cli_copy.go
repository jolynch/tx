package filexfercli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/jolynch/tx/internal/sampler"
	"github.com/zeebo/xxh3"
)

// parseVerifyFlag parses the --verify flag value into (verifyMeta, dataSamplePct, budget, error).
//
//	none    → false, 0,   0
//	meta    → true,  0,   0
//	N%data  → true,  N,   0     (1-100)
//	full    → true,  100, 0
//	<dur>   → true,  100, dur   (e.g. 30s, 5m — meta + 100% data with time budget)
func parseVerifyFlag(raw string) (bool, int, time.Duration, error) {
	s := strings.TrimSpace(raw)
	switch s {
	case "none":
		return false, 0, 0, nil
	case "meta":
		return true, 0, 0, nil
	case "full":
		return true, 100, 0, nil
	}
	if strings.HasSuffix(s, "%data") {
		numStr := strings.TrimSuffix(s, "%data")
		n, err := strconv.Atoi(numStr)
		if err != nil || n < 1 || n > 100 {
			return false, 0, 0, fmt.Errorf("invalid data sample percent %q; must be 1-100", numStr)
		}
		return true, n, 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return false, 0, 0, fmt.Errorf("--verify duration must be > 0")
		}
		return true, 100, d, nil
	}
	return false, 0, 0, fmt.Errorf("unsupported --verify value %q (supported: none, meta, N%%data, full, <duration>)", s)
}

func parseCacheLoadFlag(raw string) (bool, time.Duration, error) {
	s := strings.TrimSpace(raw)
	switch s {
	case "none":
		return false, 0, nil
	case "full":
		return true, 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return false, 0, fmt.Errorf("--cache-load duration must be > 0")
		}
		return true, d, nil
	}
	return false, 0, fmt.Errorf("unsupported --cache-load value %q (supported: none, full, <duration>)", s)
}

type copyCLIConfig struct {
	remoteSrc           string
	localDst            string
	encryptMode         string
	keysDir             string
	authTokens          []string
	compressRaw         string
	modeRaw             string
	concurrency         int
	ackEveryRaw         string
	probeSizeRaw        string
	deadlineRaw         string
	traceFile           string
	progressFilePaths   []string
	progressFormats     []string
	progressIntervalRaw string
	clean               bool
	skipFetch           bool
	skipWrite           bool
	skipFsync           bool
	fsyncIntervalRaw    string
	verifyRaw           string
	verifyMeta          bool
	verifyDataSamplePct int
	verifyBudget        time.Duration
	cacheLoadRaw        string
	cacheLoadEnabled    bool
	cacheLoadBudget     time.Duration
	verbose             bool
	progress            bool
	yes                 bool
}

// cacheMapValue maps the boolean "did the caller enable --cache-load?"
// onto the TXFER cache-map wire value. Centralized so the CLI keeps a
// single source of truth.
func cacheMapValue(cacheLoad bool) string {
	if cacheLoad {
		return "send"
	}
	return "none"
}

func cleanupCopyState(targetDir string, stderr io.Writer) int {
	ps, err := newPinchState(targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 1
	}
	if err := os.RemoveAll(ps.StateDir); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "remove state directory failed: %v\n", err)
		return 1
	}
	_ = os.Remove(filepath.Dir(ps.StateDir))
	return 0
}

func runCopyCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := cliflags.New("copy")
	cf.SetOutput(stderr)
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx recv [addr] copy [flags] REMOTE_SRC LOCAL_DST")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Copy REMOTE_SRC from the remote to LOCAL_DST on the local machine.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Behavior:")
		fmt.Fprintln(stderr, "  - If LOCAL_DST does not exist and no prior .tx/<dst>/ state: full transfer")
		fmt.Fprintln(stderr, "  - If LOCAL_DST does not exist but .tx/<dst>/ exists: resume via SYNC")
		fmt.Fprintln(stderr, "  - If LOCAL_DST exists: diff remote and send deltas")
		fmt.Fprintln(stderr, "  - --clean removes LOCAL_DST and .tx/<dst>/, then forces a clean transfer")
		fmt.Fprintln(stderr, "  - --skip-fetch fetches and writes manifest state only; no start/sync")
		fmt.Fprintln(stderr, "  - --skip-write fetches bodies to a discard sink and never mutates LOCAL_DST")
		fmt.Fprintf(stderr, "  - --verify=MODE post-copy verification: none|meta|N%%data|full|<dur>\n")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}
	cfg := copyCLIConfig{
		ackEveryRaw:         encoding.HumanBytes(defaultCLIAckEveryBytes),
		probeSizeRaw:        encoding.HumanBytes(defaultCLIProbeBytes),
		progressIntervalRaw: "1s",
		fsyncIntervalRaw:    "512MiB",
		cacheLoadRaw:        "none",
		progress:            true,
	}
	cf.BoolVar(&cfg.clean, "", "clean", false, "Remove LOCAL_DST first, then force a clean transfer")
	cf.BoolVar(&cfg.skipFetch, "", "skip-fetch", false, "Fetch and persist remote manifest state only; do not start or sync files")
	cf.BoolVar(&cfg.skipWrite, "", "skip-write", false, "Do not mutate LOCAL_DST; fetch file bodies to discard instead of writing them")
	cf.BoolVar(&cfg.skipFsync, "", "skip-fsync", false, "Acknowledge writes without fdatasync")
	cf.StringVar(&cfg.fsyncIntervalRaw, "", "fsync-interval", cfg.fsyncIntervalRaw, "Background fsync batch threshold; 0=inline fdatasync, -1=syncfs-only at exit")
	cf.StringVar(&cfg.verifyRaw, "", "verify", "meta", "Integrity checks after copy: none|meta|N%data|full|<duration> (default meta)")
	cf.StringVar(&cfg.cacheLoadRaw, "", "cache-load", cfg.cacheLoadRaw, "Mirror sender page cache after success: none|full|<duration> (default none)")
	cf.StringVar(&cfg.modeRaw, "", "mode", tx.LoadStrategyFast, "Server read strategy: fast|gentle")
	cf.StringVar(&cfg.encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&cfg.keysDir, "k", "keys", "", "Persistent age keys directory (default: ephemeral)")
	cf.StringSliceVar(&cfg.authTokens, "t", "auth-token", "Client auth token presented in encrypted AUTH blob; repeatable")
	cf.StringVar(&cfg.compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.IntVar(&cfg.concurrency, "", "concurrency", 0, "Parallel download / verification workers (0=adapt from server)")
	cf.BoolVar(&cfg.progress, "", "progress", true, "Show transfer progress every 2s")
	cf.BoolVar(&cfg.verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringSliceVar(&cfg.progressFilePaths, "p", "progress-path", "Progress output target; repeatable, use - for stdout")
	cf.StringSliceVar(&cfg.progressFormats, "f", "progress-format", "Progress format: json|int; 1 applies to all targets, or one per target (default json)")
	cf.StringVar(&cfg.progressIntervalRaw, "", "progress-interval", cfg.progressIntervalRaw, "Progress write interval (e.g. 500ms, 10s)")
	cf.BoolVar(&cfg.yes, "y", "yes", false, "Skip confirmation prompt on sync paths")
	cf.StringVar(&cfg.ackEveryRaw, "a", "ack-every", cfg.ackEveryRaw, "Bytes between progress acks; e.g. 1B, 4KiB, 8MiB")
	cf.StringVar(&cfg.probeSizeRaw, "", "probe-size", cfg.probeSizeRaw, "Probe payload size; e.g. 1B, 4KiB, 8MiB")
	cf.StringVar(&cfg.deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	cf.StringVar(&cfg.traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() != 2 {
		fmt.Fprintln(stderr, "copy requires exactly two positional arguments: REMOTE_SRC LOCAL_DST")
		return 2
	}
	cfg.remoteSrc = cf.Arg(0)
	cfg.localDst = cf.Arg(1)
	if !filepath.IsAbs(cfg.remoteSrc) {
		fmt.Fprintln(stderr, "copy requires REMOTE_SRC to be an absolute server path")
		return 2
	}
	{
		meta, dataPct, budget, err := parseVerifyFlag(cfg.verifyRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --verify: %v\n", err)
			return 2
		}
		cfg.verifyMeta = meta
		cfg.verifyDataSamplePct = dataPct
		cfg.verifyBudget = budget
	}
	{
		enabled, budget, err := parseCacheLoadFlag(cfg.cacheLoadRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --cache-load: %v\n", err)
			return 2
		}
		cfg.cacheLoadEnabled = enabled
		cfg.cacheLoadBudget = budget
	}
	// If --verify=<dur> was passed but --cache-load lacks an explicit
	// duration, inherit the verify deadline so the touch pass and the
	// post-touch cache-verify pass share the same overall budget.
	if cfg.cacheLoadEnabled && cfg.cacheLoadBudget == 0 && cfg.verifyBudget > 0 {
		cfg.cacheLoadBudget = cfg.verifyBudget
	}
	if cfg.verifyDataSamplePct > 0 && (cfg.skipFetch || cfg.skipWrite) {
		fmt.Fprintf(stderr, "--verify N%%data/full cannot be used with --skip-fetch or --skip-write\n")
		return 2
	}
	if cfg.verifyMeta && cfg.skipWrite {
		fmt.Fprintln(stderr, "--verify meta cannot be used with --skip-write")
		return 2
	}
	agePublicKey, ageIdentity, encMode, err := resolveEncryptionOptionsWithKeys(cfg.encryptMode, cfg.keysDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	if err := validateAuthTokens(cfg.authTokens, encMode); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	compress, err := resolveCompress(cfg.compressRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --compress: %v\n", err)
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(cfg.modeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --mode: %v\n", err)
		return 2
	}
	probeBytes, err := encoding.ParseByteSize(cfg.probeSizeRaw)
	if err != nil || probeBytes <= 0 {
		fmt.Fprintf(stderr, "invalid --probe-size: %v\n", err)
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(cfg.ackEveryRaw)
	if err != nil || ackEvery <= 0 {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	var fsyncInterval int64
	if cfg.fsyncIntervalRaw == "-1" {
		fsyncInterval = -1
	} else {
		fsyncInterval, err = encoding.ParseByteSize(cfg.fsyncIntervalRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --fsync-interval: %v\n", err)
			return 2
		}
		if fsyncInterval < 0 {
			fmt.Fprintln(stderr, "--fsync-interval must be >= 0 or -1")
			return 2
		}
	}
	progressInterval, err := time.ParseDuration(cfg.progressIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-interval: %v\n", err)
		return 2
	}
	progressTargets, err := cliflags.ResolveProgressTargets(cfg.progressFilePaths, cfg.progressFormats)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-path/--progress-format: %v\n", err)
		return 2
	}
	var deadlineMS int64
	if cfg.deadlineRaw != "" {
		d, err := time.ParseDuration(cfg.deadlineRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --deadline: %v\n", err)
			return 2
		}
		if d <= 0 {
			fmt.Fprintln(stderr, "--deadline must be > 0")
			return 2
		}
		deadlineMS = d.Milliseconds()
	}

	localExists := pathExists(cfg.localDst)
	if cfg.verifyMeta && cfg.skipFetch && !localExists {
		fmt.Fprintln(stderr, "--verify with --skip-fetch requires an existing LOCAL_DST")
		return 2
	}
	ps, err := newPinchState(cfg.localDst)
	if err != nil {
		fmt.Fprintf(stderr, "invalid local destination: %v\n", err)
		return 2
	}
	if cfg.clean && !cfg.skipFetch {
		if err := os.RemoveAll(cfg.localDst); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "remove local destination failed: %v\n", err)
			return 1
		}
		if err := os.RemoveAll(ps.StateDir); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "remove state directory failed: %v\n", err)
			return 1
		}
		_ = os.Remove(filepath.Dir(ps.StateDir))
		localExists = false
	}
	hasPriorState := pathExists(ps.ServerManifestPath)
	metadataFailures := &metadataFailureCollector{}
	copyVerbosity := verbosityFromFlags(cfg.progress, cfg.verbose)

	transferCfg := transferArgs{
		sourceDir:    cfg.remoteSrc,
		targetDir:    cfg.localDst,
		agePublicKey: agePublicKey,
		ageIdentity:  ageIdentity,
		encMode:      encMode,
		authTokens:   cfg.authTokens,
		loadStrategy: loadStrategy,
		probeBytes:   probeBytes,
		verbosity:    0,
		deadlineMS:   deadlineMS,
		cacheLoad:    cfg.cacheLoadEnabled,
	}
	// Resume path: prior .tx/<dst>/manifest.server exists from an interrupted
	// run, and the user has not re-created LOCAL_DST. Refresh via SYNC and
	// carry progress forward instead of re-fetching the whole manifest.
	useResume := hasPriorState && !localExists && !cfg.skipFetch
	if useResume {
		if code := runResumeRefresh(serverURL, transferCfg, stderr); code != 0 {
			return code
		}
	} else {
		if code := runTransfer(serverURL, transferCfg, stdout, stderr); code != 0 {
			return code
		}
	}

	if cfg.skipFetch {
		if cfg.verifyMeta {
			return verifyCopy(serverURL, cfg, stdout, stderr)
		}
		return 0
	}

	if localExists {
		syncCfg := syncArgs{
			sourceDir:           cfg.remoteSrc,
			targetDir:           cfg.localDst,
			agePublicKey:        agePublicKey,
			ageIdentity:         ageIdentity,
			encMode:             encMode,
			authTokens:          cfg.authTokens,
			concurrency:         cfg.concurrency,
			concurrencyExplicit: cfg.concurrency > 0,
			ackEvery:            ackEvery,
			compress:            compress,
			noSync:              cfg.skipFsync,
			fsyncInterval:       fsyncInterval,
			skipWrite:           cfg.skipWrite,
			verbosity:           verbosityFromFlags(false, cfg.verbose),
			yes:                 cfg.yes,
			probeBytes:          probeBytes,
			traceFile:           cfg.traceFile,
			progressTargets:     progressTargets,
			progressInterval:    progressInterval,
			metadataFailures:    metadataFailures,
		}
		if code := runSync(serverURL, syncCfg, stdout, stderr); code != 0 {
			printMetadataMirrorWarnings(stderr, "copy", metadataFailures.snapshot(), copyVerbosity)
			return code
		}
	} else {
		startCfg := startArgs{
			targetDir:           cfg.localDst,
			agePublicKey:        agePublicKey,
			ageIdentity:         ageIdentity,
			encMode:             encMode,
			authTokens:          cfg.authTokens,
			verbosity:           copyVerbosity,
			concurrency:         cfg.concurrency,
			concurrencyExplicit: cfg.concurrency > 0,
			ackEvery:            ackEvery,
			compress:            compress,
			noSync:              cfg.skipFsync,
			fsyncInterval:       fsyncInterval,
			discard:             cfg.skipWrite,
			deadlineMS:          deadlineMS,
			traceFile:           cfg.traceFile,
			progressTargets:     progressTargets,
			progressInterval:    progressInterval,
			metadataFailures:    metadataFailures,
		}
		if code := runStart(serverURL, startCfg, stdout, stderr); code != 0 {
			printMetadataMirrorWarnings(stderr, "copy", metadataFailures.snapshot(), copyVerbosity)
			return code
		}
	}

	// When --verify or --cache-load is requested, run an end-of-copy SYNC
	// to detect drift the server tree may have accumulated mid-transfer.
	// The same loop also carries the cache-map=recv snapshot back when the
	// caller asked for cache-load. Up to maxSyncRounds attempts; copy
	// fails if the remote still has differences after the cap.
	needConverge := cfg.cacheLoadEnabled ||
		cfg.verifyMeta || cfg.verifyDataSamplePct > 0 || cfg.verifyBudget > 0
	if needConverge {
		if code := runEndOfCopyConvergence(
			serverURL, cfg,
			agePublicKey, ageIdentity, encMode,
			ackEvery, compress, fsyncInterval, probeBytes,
			progressTargets, progressInterval,
			metadataFailures,
			stdout, stderr,
		); code != 0 {
			return code
		}
	}

	exitCode := 0
	if cfg.verifyMeta {
		if code := verifyCopy(serverURL, cfg, stdout, stderr); code != 0 {
			exitCode = code
		}
	}
	metadataWarnings := metadataFailures.snapshot()
	if len(metadataWarnings) > 0 {
		printMetadataMirrorWarnings(stderr, "copy", metadataWarnings, copyVerbosity)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	if exitCode != 0 {
		return exitCode
	}

	// Only touch and cleanup if success
	if cfg.cacheLoadEnabled {
		touchCopyCache(cfg, progressInterval, stderr)
	}
	if code := cleanupCopyState(cfg.localDst, stderr); code != 0 {
		return code
	}
	return 0
}

// cacheProgressState holds the counters and budgets formatCacheProgressLine
// renders. Budgets <= 0 indicate "unknown / unlimited" and cause their
// denominator slot to render as `n/a`.
type cacheProgressState struct {
	filesTouched int64
	totalFiles   int64
	bytesTouched int64
	totalBytes   int64
	pagesTouched int64
	pageBudget   int64
	elapsed      time.Duration
	timeBudget   time.Duration
}

const (
	cacheProgressCountWidth    = 6
	cacheProgressBytesWidth    = 10
	cacheProgressDurationWidth = 5
)

// formatCacheProgressLine renders one progress / summary line in the same
// bracketed style as fsync-progress, ending with a `budget[time/time][mem/mem]`
// block. The status (`[ok]` / `[partial-ok]`) is baked into tag for the final
// summary; the periodic line passes tag="cache-progress:".
func formatCacheProgressLine(tag string, s cacheProgressState) string {
	filesTouched := max(s.filesTouched, int64(0))
	totalFiles := max(s.totalFiles, int64(0))
	bytesTouched := max(s.bytesTouched, int64(0))
	totalBytes := max(s.totalBytes, int64(0))

	var pctFiles, pctBytes float64
	if totalFiles > 0 {
		pctFiles = clampCachePercent(float64(filesTouched) * 100 / float64(totalFiles))
	}
	if totalBytes > 0 {
		pctBytes = clampCachePercent(float64(bytesTouched) * 100 / float64(totalBytes))
	}

	elapsedField := fixedWidthCacheDuration(s.elapsed)
	timeBudgetField := fixedWidthCacheDurationNA()
	if s.timeBudget > 0 {
		timeBudgetField = fixedWidthCacheDuration(s.timeBudget)
	}

	pageSize := int64(pagecache.PageSize())
	memUsedField := encoding.HumanBytesFixedWidth(max(s.pagesTouched, int64(0))*pageSize, cacheProgressBytesWidth)
	memBudgetField := fixedWidthCacheBytesNA()
	if s.pageBudget > 0 {
		memBudgetField = encoding.HumanBytesFixedWidth(s.pageBudget*pageSize, cacheProgressBytesWidth)
	}

	return fmt.Sprintf(
		"%s[%s/%s](%5.1f%%) [%s/%s](%5.1f%%) budget[%s/%s][%s/%s]",
		tag,
		encoding.HumanCount(uint64(filesTouched), cacheProgressCountWidth),
		encoding.HumanCount(uint64(totalFiles), cacheProgressCountWidth),
		pctFiles,
		encoding.HumanBytesFixedWidth(bytesTouched, cacheProgressBytesWidth),
		encoding.HumanBytesFixedWidth(totalBytes, cacheProgressBytesWidth),
		pctBytes,
		elapsedField,
		timeBudgetField,
		memUsedField,
		memBudgetField,
	)
}

func fixedWidthCacheDuration(d time.Duration) string {
	return fmt.Sprintf("%*s", cacheProgressDurationWidth, compactETA(d))
}

func fixedWidthCacheDurationNA() string {
	return fmt.Sprintf("%*s", cacheProgressDurationWidth, "n/a")
}

func fixedWidthCacheBytesNA() string {
	return fmt.Sprintf("%*s", cacheProgressBytesWidth, "n/a")
}

func clampCachePercent(pct float64) float64 {
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// runEndOfCopyConvergence drives a closing SYNC loop against the server,
// repairing any drift the response reveals. When --cache-load is on, the
// SYNC carries cache-map=recv so the server also restores its page cache
// from the saved snapshot. Returns 0 on convergence (within
// maxSyncRounds) and non-zero with a "remote has not converged" log line
// otherwise.
func runEndOfCopyConvergence(
	serverURL string,
	cfg copyCLIConfig,
	agePublicKey, ageIdentity, encMode string,
	ackEvery int64,
	compress string,
	fsyncInterval int64,
	probeBytes int64,
	progressTargets []filexfer.ProgressTarget,
	progressInterval time.Duration,
	metadataFailures *metadataFailureCollector,
	stdout io.Writer,
	stderr io.Writer,
) int {
	ps, err := newPinchState(cfg.localDst)
	if err != nil {
		fmt.Fprintf(stderr, "copy-converge: skip (%v)\n", err)
		return 0
	}
	saved, err := tx.LoadManifest(ps.ServerManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "copy-converge: skip (load manifest: %v)\n", err)
		return 0
	}
	cacheMap := "none"
	if cfg.cacheLoadEnabled {
		cacheMap = "recv"
	}
	syncCfg := syncArgs{
		sourceDir:           cfg.remoteSrc,
		targetDir:           cfg.localDst,
		agePublicKey:        agePublicKey,
		ageIdentity:         ageIdentity,
		encMode:             encMode,
		authTokens:          cfg.authTokens,
		concurrency:         cfg.concurrency,
		concurrencyExplicit: cfg.concurrency > 0,
		ackEvery:            ackEvery,
		compress:            compress,
		noSync:              cfg.skipFsync,
		fsyncInterval:       fsyncInterval,
		skipWrite:           cfg.skipWrite,
		verbosity:           verbosityFromFlags(false, cfg.verbose),
		yes:                 true, // never prompt — convergence is automatic
		probeBytes:          probeBytes,
		traceFile:           cfg.traceFile,
		progressTargets:     progressTargets,
		progressInterval:    progressInterval,
		metadataFailures:    metadataFailures,
		initialOldManifest:  saved,
		cacheMap:            cacheMap,
		logPrefix:           "copy-converge",
	}
	return runSync(serverURL, syncCfg, stdout, stderr)
}

// touchCopyCache decodes per-file page-cache hints from the just-fetched
// manifest and issues posix_fadvise(WILLNEED) for each file, oldest-mtime
// first, up to a system-RAM-derived page budget. No-op on platforms where
// (*pagecache.CacheEntry).Touch isn't backed by real syscalls.
//
// Decoding happens lazily inside the TouchEntries yield closure so that the
// decompressed bitmaps for un-touched candidates never accumulate in memory,
// and so we never allocate a parallel slice of joined path strings — those
// are constructed only at the moment of yielding.
func touchCopyCache(cfg copyCLIConfig, progressInterval time.Duration, stderr io.Writer) {
	if !pagecache.TouchSupported() {
		return
	}
	ps, err := newPinchState(cfg.localDst)
	if err != nil {
		fmt.Fprintf(stderr, "cache-touch: skip (%v)\n", err)
		return
	}
	manifest, err := tx.LoadManifest(ps.ServerManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "cache-touch: skip (load manifest: %v)\n", err)
		return
	}

	var candidateIndexes []int
	var totalBytes int64
	for i := range manifest.Entries {
		e := &manifest.Entries[i]
		if e.Type != encoding.EntryTypeFile || len(e.PageCache) == 0 {
			continue
		}
		candidateIndexes = append(candidateIndexes, i)
		totalBytes += e.Size
	}
	if len(candidateIndexes) == 0 {
		return
	}
	sort.Slice(candidateIndexes, func(i, j int) bool {
		return manifest.Entries[candidateIndexes[i]].Mtime < manifest.Entries[candidateIndexes[j]].Mtime
	})
	totalFiles := int64(len(candidateIndexes))

	budget := pagecache.SystemPageBudget(pagecache.TouchCacheReserveBytes)
	ctx := context.Background()
	cancel := func() {}
	if cfg.cacheLoadBudget > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.cacheLoadBudget)
	}
	defer cancel()

	start := time.Now()
	var filesTouched, bytesTouched, pagesTouched int64
	lastProgress := start
	snapshot := func() cacheProgressState {
		return cacheProgressState{
			filesTouched: filesTouched,
			totalFiles:   totalFiles,
			bytesTouched: bytesTouched,
			totalBytes:   totalBytes,
			pagesTouched: pagesTouched,
			pageBudget:   budget,
			elapsed:      time.Since(start),
			timeBudget:   cfg.cacheLoadBudget,
		}
	}
	// Sample size scales with the total warmable bytes: ~10% of the chunks
	// the manifest covers. Big files contribute more chunks than small ones
	// so the retained sample is weighted by file size rather than file count.
	chunkSizeBytes := int64(pagecache.DefaultChunkSamplePages) * int64(pagecache.PageSize())
	totalChunks := (totalBytes + chunkSizeBytes - 1) / chunkSizeBytes
	sampleCap := int(float64(totalChunks) * pagecache.DefaultChunkSampleRate)
	if sampleCap < pagecache.MinChunkSampleCap {
		sampleCap = pagecache.MinChunkSampleCap
	}
	sampler := pagecache.NewChunkSampler(sampleCap, pagecache.DefaultChunkSamplePages)

	// Wipe the slate first: evict whatever pages the writer left in the page
	// cache for every transferred regular file. Without this step, the
	// receiver's cache state is the writer's "I just wrote these" plus our
	// WILLNEED additions — not the sender's snapshot we're trying to
	// reproduce. Use the same ctx so a tight --cache-load budget covers
	// evict + touch together.
	evict, _ := pagecache.EvictPaths(ctx, func(yield func(string) bool) {
		for i := range manifest.Entries {
			e := &manifest.Entries[i]
			if e.Type != encoding.EntryTypeFile {
				continue
			}
			if !yield(filepath.Join(ps.TargetDir, e.Path)) {
				return
			}
		}
	}, 0)
	if evict.Evicted+evict.Errors > 0 {
		fmt.Fprintf(stderr, "cache-evict: evicted=%d errors=%d\n", evict.Evicted, evict.Errors)
	}

	touchSummary, touchErr := pagecache.TouchEntries(ctx, func(yield func(pagecache.TouchEntry) bool) {
		for _, idx := range candidateIndexes {
			e := &manifest.Entries[idx]
			ce := &pagecache.CacheEntry{}
			if err := encoding.DecodePageCacheEntry(e.PageCache, ce); err != nil || ce.Empty() {
				continue
			}
			pages := int64(ce.NumResidentPages())
			te := pagecache.TouchEntry{
				Path:  filepath.Join(ps.TargetDir, e.Path),
				Entry: ce,
			}
			if !yield(te) {
				return
			}
			sampler.Observe(te)
			filesTouched++
			bytesTouched += e.Size
			pagesTouched += pages

			now := time.Now()
			if progressInterval > 0 && now.Sub(lastProgress) >= progressInterval {
				fmt.Fprintln(stderr, formatCacheProgressLine("cache-progress:", snapshot()))
				lastProgress = now
			}
		}
	}, budget, 0)

	status := "[ok]"
	if errors.Is(touchErr, context.DeadlineExceeded) {
		status = "[partial-ok]"
	}
	tag := "cache-touch:" + status
	if touchSummary.OpenErrors+touchSummary.AdviseErrors+touchSummary.ReadErrors > 0 {
		tag += fmt.Sprintf("[errs=open=%d/advise=%d/read=%d]",
			touchSummary.OpenErrors, touchSummary.AdviseErrors, touchSummary.ReadErrors)
	}
	fmt.Fprintln(stderr, formatCacheProgressLine(tag, snapshot()))

	// Reprobe runs against whatever fraction of the cache-load budget is
	// still on the clock. If the budget is already gone there's no time to
	// verify; skip entirely. The user already saw cache-touch:[partial-ok].
	reprobeCtx := context.Background()
	if cfg.cacheLoadBudget > 0 {
		remaining := cfg.cacheLoadBudget - time.Since(start)
		if remaining <= 0 {
			return
		}
		var reprobeCancel context.CancelFunc
		reprobeCtx, reprobeCancel = context.WithTimeout(reprobeCtx, remaining)
		defer reprobeCancel()
	}
	probe := pagecache.ReprobeChunks(reprobeCtx, sampler.Samples())
	if probe.SampledChunks > 0 || probe.Partial {
		fmt.Fprintln(stderr, formatCacheVerifyLine(probe))
	}
}

// formatCacheVerifyLine renders the post-touch verification line in the same
// bracketed style as cache-progress / cache-touch. Status is [ok] or
// [partial-ok] (the latter when the remaining cache-load budget expired
// mid-reprobe). The first bracket is SampledChunks/PlannedChunks; the
// percentage in parens is the kernel honor rate; the second bracket is
// honored/expected pages expressed as bytes.
func formatCacheVerifyLine(probe pagecache.ChunkProbeResult) string {
	status := "[ok]"
	if probe.Partial {
		status = "[partial-ok]"
	}
	var honorPct float64
	if probe.ExpectedPages > 0 {
		honorPct = clampCachePercent(float64(probe.HonoredPages) * 100 / float64(probe.ExpectedPages))
	}
	pageSize := int64(pagecache.PageSize())
	return fmt.Sprintf(
		"cache-verify:%s[%s/%s chunks](%5.1f%% honored) [%s/%s]",
		status,
		encoding.HumanCount(uint64(max(probe.SampledChunks, 0)), cacheProgressCountWidth),
		encoding.HumanCount(uint64(max(probe.PlannedChunks, 0)), cacheProgressCountWidth),
		honorPct,
		encoding.HumanBytesFixedWidth(probe.HonoredPages*pageSize, cacheProgressBytesWidth),
		encoding.HumanBytesFixedWidth(probe.ExpectedPages*pageSize, cacheProgressBytesWidth),
	)
}

func countManifestEntryTypes(entries []tx.ManifestEntry) (files, hardlinks, symlinks, dirs int) {
	for _, entry := range entries {
		switch entry.Type {
		case encoding.EntryTypeHard:
			hardlinks++
		case encoding.EntryTypeSymlink:
			symlinks++
		case encoding.EntryTypeDir:
			dirs++
		default:
			files++
		}
	}
	return
}

func verifyCopy(serverURL string, cfg copyCLIConfig, stdout io.Writer, stderr io.Writer) int {
	ps, err := newPinchState(cfg.localDst)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	serverManifest, err := tx.LoadManifest(ps.ServerManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load server manifest failed: %v\n", err)
		return 1
	}
	localManifest, err := scanLocalDir(ps.TargetDir, serverManifest)
	if err != nil {
		fmt.Fprintf(stderr, "scan local directory failed: %v\n", err)
		return 1
	}
	metaFailed := false
	delta := compareManifestEntries(localManifest, serverManifest)
	if len(delta.newFiles) > 0 || len(delta.staleFiles) > 0 || len(delta.removedPaths) > 0 {
		fmt.Fprintf(
			stderr,
			"copy-verify-meta: [fail] mismatch new=%d (%s) stale=%d (%s) rm=%d\n",
			len(delta.newFiles),
			encoding.HumanBytes(delta.newBytes),
			len(delta.staleFiles),
			encoding.HumanBytes(delta.staleBytes),
			len(delta.removedPaths),
		)
		metaFailed = true
	} else if err := verifyCopyRemoteMetadata(serverURL, cfg, serverManifest, ps.TargetDir); err != nil {
		fmt.Fprintf(stderr, "copy-verify-meta: [fail] %v\n", err)
		metaFailed = true
	} else {
		files, hardlinks, symlinks, dirs := countManifestEntryTypes(serverManifest.Entries)
		fmt.Fprintf(
			stderr,
			"copy-verify-meta: [ok] total=%d files=%d hardlinks=%d symlinks=%d dirs=%d\n",
			len(serverManifest.Entries),
			files,
			hardlinks,
			symlinks,
			dirs,
		)
	}
	if cfg.verifyDataSamplePct <= 0 {
		if metaFailed {
			return 1
		}
		return 0
	}
	sampledFiles, sampledRanges, elapsed, partial, err := verifyCopyDataSamples(serverURL, cfg, serverManifest, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "copy-verify-data: [fail] %v\n", err)
		return 1
	}
	fmt.Fprintln(stderr, formatVerifyDataSummaryLine(sampledFiles, sampledRanges, cfg.verifyDataSamplePct, cfg.verifyBudget, elapsed, partial))
	if metaFailed {
		return 1
	}
	return 0
}

func verifyCopyRemoteMetadata(serverURL string, cfg copyCLIConfig, manifest *tx.Manifest, targetDir string) error {
	if manifest == nil {
		return errors.New("missing manifest")
	}
	agePublicKey, ageIdentity, encMode, err := resolveEncryptionOptionsWithKeys(cfg.encryptMode, cfg.keysDir)
	if err != nil {
		return fmt.Errorf("invalid --encrypt: %w", err)
	}
	client := tx.NewClient(serverURL, tx.WithClientAgePublicKey(agePublicKey), tx.WithClientAgeIdentity(ageIdentity), tx.WithEncryptMode(encMode), tx.WithClientAuthTokens(cfg.authTokens...))
	defer client.Close()

	var dirs []tx.ManifestEntry
	for _, entry := range manifest.Entries {
		if entry.Type == encoding.EntryTypeDir {
			dirs = append(dirs, entry)
		}
	}
	labels := make(map[uint64]string, len(manifest.Entries)+1)
	localPaths := make(map[uint64]string, len(manifest.Entries)+1)
	labels[tx.RootFileID] = "."
	localPaths[tx.RootFileID] = targetDir
	for _, entry := range dirs {
		labels[entry.ID] = entry.Path
		localPaths[entry.ID] = filepath.Join(targetDir, filepath.FromSlash(entry.Path))
	}
	metaByID, err := client.GetEntryMetadata(context.Background(), manifest.TransferID, directoryMetadataPaths(manifest, dirs))
	if err != nil {
		return fmt.Errorf("remote metadata fetch failed: %w", err)
	}
	for fileID, meta := range metaByID {
		label := labels[fileID]
		if err := verifyLocalPathMetadata(label, localPaths[fileID], meta); err != nil {
			return err
		}
	}
	return nil
}

func verifyLocalPathMetadata(label string, path string, remote *tx.FileTrailerMetadata) error {
	if remote == nil {
		return fmt.Errorf("%s: missing remote metadata", label)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s: stat local path: %w", label, err)
	}
	local := encoding.CollectFileFrameMetadata(path, info)
	if remote.Mode != "" && remote.Mode != local.Mode {
		return fmt.Errorf("%s: mode mismatch local=%s remote=%s", label, local.Mode, remote.Mode)
	}
	if remote.MtimeNS > 0 && remote.MtimeNS != local.MtimeNS {
		return fmt.Errorf("%s: mtime mismatch local=%d remote=%d", label, local.MtimeNS, remote.MtimeNS)
	}
	if remote.UID != "" && remote.UID != local.UID {
		return fmt.Errorf("%s: uid mismatch local=%s remote=%s", label, local.UID, remote.UID)
	}
	if remote.GID != "" && remote.GID != local.GID {
		return fmt.Errorf("%s: gid mismatch local=%s remote=%s", label, local.GID, remote.GID)
	}
	return nil
}

func formatVerifyDataSummaryLine(files int, samples int, pct int, budget time.Duration, elapsed time.Duration, partial bool) string {
	elapsed = elapsed.Round(time.Millisecond)
	status := "[ok]"
	if partial {
		status = "[partial-ok]"
	}
	if budget > 0 {
		return fmt.Sprintf("copy-verify-data: %s files=%d samples=%d budget=%s elapsed=%s", status, files, samples, budget, elapsed)
	}
	return fmt.Sprintf("copy-verify-data: %s files=%d samples=%d pct=%d elapsed=%s", status, files, samples, pct, elapsed)
}

type verifySampleTask struct {
	entry      tx.ManifestEntry
	serverPath string
	localPath  string
	sampleGen  sampler.Generator
}

func verifyCopyDataSamples(serverURL string, cfg copyCLIConfig, manifest *tx.Manifest, stderr io.Writer) (int, int, time.Duration, bool, error) {
	start := time.Now()
	if manifest == nil {
		return 0, 0, 0, false, errors.New("missing manifest")
	}
	agePublicKey, ageIdentity, encMode, err := resolveEncryptionOptionsWithKeys(cfg.encryptMode, cfg.keysDir)
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("invalid --encrypt: %w", err)
	}
	tasks := make([]verifySampleTask, 0, len(manifest.Entries))
	var totalRanges int64
	for _, entry := range manifest.Entries {
		sampleGen, ok := sampler.New(manifest.Root, entry.Path, entry.ID, entry.Size, cfg.verifyDataSamplePct, defaultVerifySampleFrameSize, verifySampleBytes)
		if !ok {
			continue
		}
		tasks = append(tasks, verifySampleTask{
			entry:      entry,
			serverPath: filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path))),
			localPath:  filepath.Join(cfg.localDst, filepath.FromSlash(entry.Path)),
			sampleGen:  sampleGen,
		})
		totalRanges += sampleGen.TotalSamples()
	}
	if len(tasks) == 0 {
		return 0, 0, time.Since(start), false, nil
	}
	workers := cfg.concurrency
	if workers <= 0 {
		workers = manifest.Concurrency
	}
	if workers <= 0 {
		workers = 1
	}
	taskCh := make(chan verifySampleTask)
	errCh := make(chan error, 1)

	// budgetCtx controls when we stop dispatching new verification tasks.
	var budgetCtx context.Context
	var budgetCancel context.CancelFunc
	if cfg.verifyBudget > 0 {
		budgetCtx, budgetCancel = context.WithTimeout(context.Background(), cfg.verifyBudget)
	} else {
		budgetCtx, budgetCancel = context.WithCancel(context.Background())
	}
	defer budgetCancel()

	// workerCtx controls in-flight checksum requests. It is kept separate
	// so that requests mid-flight when the budget expires can finish
	// cleanly instead of being killed mid-TCP-connection.
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	budgetExpired := func() bool {
		return cfg.verifyBudget > 0 && budgetCtx.Err() == context.DeadlineExceeded
	}
	var wg sync.WaitGroup
	var completed atomic.Int64
	var completedRanges atomic.Int64
	var progressDone chan struct{}
	if stderr != nil {
		progressDone = make(chan struct{})
		go func() {
			defer close(progressDone)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					done := completed.Load()
					fmt.Fprintf(stderr, "copy-verify-data: progress files=%d/%d samples=%d pct=%d\n", done, len(tasks), totalRanges, cfg.verifyDataSamplePct)
				}
			}
		}()
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := tx.NewClient(serverURL, tx.WithClientAgePublicKey(agePublicKey), tx.WithClientAgeIdentity(ageIdentity), tx.WithEncryptMode(encMode), tx.WithClientAuthTokens(cfg.authTokens...))
			defer client.Close()
			for task := range taskCh {
				if err := verifySampleTaskData(workerCtx, client, manifest.TransferID, task, func(done int64) {
					completedRanges.Add(done)
				}); err != nil {
					if workerCtx.Err() != nil {
						return
					}
					select {
					case errCh <- err:
					default:
					}
					workerCancel()
					return
				}
				completed.Add(1)
			}
		}()
	}
	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// waitWorkersWithGrace gives in-flight workers up to
	// verifyBudgetGracePeriod to finish, then cancels workerCtx.
	waitWorkersWithGrace := func() {
		timer := time.NewTimer(verifyBudgetGracePeriod)
		defer timer.Stop()
		select {
		case <-workersDone:
		case <-timer.C:
			workerCancel()
			<-workersDone
		}
		workerCancel()
		if progressDone != nil {
			<-progressDone
		}
	}

	for _, task := range tasks {
		select {
		case <-budgetCtx.Done():
			close(taskCh)
			waitWorkersWithGrace()
			select {
			case err := <-errCh:
				return 0, 0, time.Since(start), false, err
			default:
				if budgetExpired() {
					if stderr != nil {
						fmt.Fprintf(stderr, "copy-verify-data: budget expired, verified %d/%d files %d samples in %s\n",
							completed.Load(), int64(len(tasks)), completedRanges.Load(), time.Since(start).Round(time.Millisecond))
					}
					return int(completed.Load()), int(completedRanges.Load()), time.Since(start), true, nil
				}
				return 0, 0, time.Since(start), false, context.Canceled
			}
		case err := <-errCh:
			workerCancel()
			close(taskCh)
			<-workersDone
			if progressDone != nil {
				<-progressDone
			}
			return 0, 0, time.Since(start), false, err
		case taskCh <- task:
		}
	}
	close(taskCh)
	select {
	case err := <-errCh:
		workerCancel()
		<-workersDone
		if progressDone != nil {
			<-progressDone
		}
		return 0, 0, time.Since(start), false, err
	case <-workersDone:
		workerCancel()
		if progressDone != nil {
			<-progressDone
		}
		select {
		case err := <-errCh:
			return 0, 0, time.Since(start), false, err
		default:
		}
		return len(tasks), int(totalRanges), time.Since(start), false, nil
	case <-budgetCtx.Done():
		waitWorkersWithGrace()
		select {
		case err := <-errCh:
			return 0, 0, time.Since(start), false, err
		default:
			if budgetExpired() {
				if stderr != nil {
					fmt.Fprintf(stderr, "copy-verify-data: budget expired, verified %d/%d files %d samples in %s\n",
						completed.Load(), int64(len(tasks)), completedRanges.Load(), time.Since(start).Round(time.Millisecond))
				}
				return int(completed.Load()), int(completedRanges.Load()), time.Since(start), true, nil
			}
			return 0, 0, time.Since(start), false, context.Canceled
		}
	}
}

func estimateChecksumRequestCommandBytes(transferID string, targets []tx.ChecksumTarget) int {
	total := len("CXSUM ") + len(transferID)
	for _, target := range targets {
		total += estimateChecksumTargetCommandBytes(target)
	}
	return total
}

func estimateChecksumTargetCommandBytes(target tx.ChecksumTarget) int {
	total := len(" fd=") + digitsBase10Uint64(target.FileID) + 1 + len(strconv.Itoa(len(target.FullPath))) + 1 + len(target.FullPath)
	if target.Offset > 0 {
		total += len(" offset=") + digitsBase10Int64(target.Offset)
	}
	if target.Size > 0 {
		total += len(" size=") + digitsBase10Int64(target.Size)
	}
	if algo := strings.TrimSpace(target.Algo); algo != "" {
		total += len(" algo=") + len(algo)
	}
	return total
}

func digitsBase10Uint64(v uint64) int {
	if v == 0 {
		return 1
	}
	return len(strconv.FormatUint(v, 10))
}

func digitsBase10Int64(v int64) int {
	if v == 0 {
		return 1
	}
	if v < 0 {
		return digitsBase10Uint64(uint64(-v)) + 1
	}
	return digitsBase10Uint64(uint64(v))
}

func verifySampleTaskData(ctx context.Context, client *tx.Client, transferID string, task verifySampleTask, onBatchVerified func(int64)) error {
	fd, err := os.Open(task.localPath)
	if err != nil {
		return fmt.Errorf("open local sample path %s: %w", task.localPath, err)
	}
	defer fd.Close()

	buf := make([]byte, verifySampleBytes)
	for task.sampleGen.Remaining() > 0 {
		batchCap := minInt64(task.sampleGen.Remaining(), 1024)
		targets := make([]tx.ChecksumTarget, 0, int(batchCap))
		samples := make([]sampler.Sample, 0, int(batchCap))
		wantHashes := make([]string, 0, int(batchCap))
		cmdBytes := len("CXSUM ") + len(transferID)
		for task.sampleGen.Remaining() > 0 {
			sample, ok := task.sampleGen.Peek()
			if !ok {
				break
			}
			target := tx.ChecksumTarget{
				FileID:   task.entry.ID,
				FullPath: task.serverPath,
				Offset:   sample.Offset,
				Size:     sample.Size,
				Algo:     "xxh128",
			}
			targetBytes := estimateChecksumTargetCommandBytes(target)
			if len(targets) > 0 && cmdBytes+targetBytes > verifyChecksumCommandBudgetBytes {
				break
			}
			want, err := computeLocalSampleHash(fd, sample.Offset, sample.Size, buf)
			if err != nil {
				return fmt.Errorf("hash local sample %s@%d: %w", task.localPath, sample.Offset, err)
			}
			targets = append(targets, target)
			samples = append(samples, sample)
			wantHashes = append(wantHashes, want)
			cmdBytes += targetBytes
			task.sampleGen.Advance()
		}
		if len(targets) == 0 {
			return fmt.Errorf("checksum batching failed for %s", task.serverPath)
		}

		verifyCtx, cancel := context.WithTimeout(ctx, verifyChecksumRequestTimeout)
		resp, err := client.GetChecksum(verifyCtx, tx.GetChecksumRequest{
			TransferID: transferID,
			Targets:    targets,
		})
		if err != nil {
			cancel()
			return fmt.Errorf("checksum request failed for %s: %w", task.serverPath, err)
		}
		results, err := readChecksumResults(resp.Reader)
		_ = resp.Reader.Close()
		cancel()
		if err != nil {
			return fmt.Errorf("read checksum response for %s: %w", task.serverPath, err)
		}
		if len(results) != len(samples) {
			return fmt.Errorf("checksum response count mismatch for %s: got %d want %d", task.serverPath, len(results), len(samples))
		}
		for i, result := range results {
			sample := samples[i]
			if result.FileID != task.entry.ID {
				return fmt.Errorf("checksum file id mismatch for %s: got %d want %d", task.serverPath, result.FileID, task.entry.ID)
			}
			if result.Offset != sample.Offset || result.Size != sample.Size {
				return fmt.Errorf("checksum range mismatch for %s: got offset=%d size=%d want offset=%d size=%d", task.serverPath, result.Offset, result.Size, sample.Offset, sample.Size)
			}
			if !strings.EqualFold(result.FileHashToken, wantHashes[i]) {
				return fmt.Errorf("checksum mismatch for %s at offset=%d size=%d", task.localPath, sample.Offset, sample.Size)
			}
		}
		if onBatchVerified != nil {
			onBatchVerified(int64(len(samples)))
		}
	}
	return nil
}

func computeLocalSampleHash(fd *os.File, offset int64, size int64, scratch []byte) (string, error) {
	if size < 0 {
		return "", errors.New("negative sample size")
	}
	if size == 0 {
		return encoding.FormatXXH128HashToken(xxh3.Hash128(nil)), nil
	}
	buf := scratch
	if int64(len(buf)) < size {
		buf = make([]byte, size)
	}
	n, err := fd.ReadAt(buf[:size], offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if int64(n) != size {
		return "", io.ErrUnexpectedEOF
	}
	return encoding.FormatXXH128HashToken(xxh3.Hash128(buf[:size])), nil
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
