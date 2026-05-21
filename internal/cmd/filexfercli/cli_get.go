package filexfercli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/fsync"
	"github.com/jolynch/tx/internal/pagecache"
)

func runGetCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := cliflags.New("get")
	cf.SetOutput(stderr)
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx recv [addr] get [flags] REMOTE_PATH")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Download a single remote file. REMOTE_PATH must be an absolute path to a file")
		fmt.Fprintln(stderr, "on the server. Output defaults to the file's basename in the current directory.")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}
	var outFile string
	var encryptMode string
	var keysDir string
	var authTokens []string
	var compressRaw string
	var ackEveryRaw string
	var skipWrite bool
	var skipFsync bool
	var fsyncIntervalRaw string
	var concurrency int
	var verbose bool
	var progress bool
	var deadlineRaw string
	var traceFile string
	var progressFilePaths []string
	var progressFormats []string
	var progressIntervalRaw string
	var cacheLoadRaw string
	var cacheLoadEnabled bool
	var cacheLoadBudget time.Duration
	cf.StringVar(&outFile, "o", "", "", "Output file path, or '-' for stdout")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&keysDir, "k", "keys", "", "Persistent age keys directory (default: ephemeral)")
	cf.StringSliceVar(&authTokens, "t", "auth-token", "Client auth token presented in encrypted AUTH blob; repeatable")
	cf.StringVar(&compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.IntVar(&concurrency, "", "concurrency", 0, "Parallel download workers (0=auto)")
	cf.BoolVar(&skipWrite, "", "skip-write", false, "Do not write the file; fetch to discard instead")
	cf.BoolVar(&skipFsync, "", "skip-fsync", false, "Acknowledge writes without fdatasync")
	fsyncIntervalRaw = "512MiB"
	cf.StringVar(&fsyncIntervalRaw, "", "fsync-interval", fsyncIntervalRaw, "Background fsync batch threshold; 0=inline fdatasync, -1=syncfs-only at exit")
	cf.BoolVar(&progress, "", "progress", true, "Show transfer progress every 2s")
	cf.BoolVar(&verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringSliceVar(&progressFilePaths, "p", "progress-path", "Progress output target; repeatable, use - for stdout")
	cf.StringSliceVar(&progressFormats, "f", "progress-format", "Progress format: json|int; 1 applies to all targets, or one per target (default json)")
	cf.StringVar(&progressIntervalRaw, "", "progress-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	cacheLoadRaw = "none"
	cf.StringVar(&cacheLoadRaw, "", "cache-load", cacheLoadRaw, "Load downloaded file into page cache after success: none|full|<duration> (default none)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	cf.StringVar(&ackEveryRaw, "a", "ack-every", ackEveryRaw, "Bytes between progress acks; e.g. 1B, 4KiB, 8MiB")
	cf.StringVar(&deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	stopTracing := startTracing(traceFile, stderr)
	defer stopTracing()
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "get requires exactly one positional argument: REMOTE_PATH")
		return 2
	}
	remotePath := cf.Arg(0)
	if !filepath.IsAbs(remotePath) {
		fmt.Fprintln(stderr, "get requires REMOTE_PATH to be an absolute server path")
		return 2
	}
	{
		enabled, budget, err := parseCacheLoadFlag(cacheLoadRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --cache-load: %v\n", err)
			return 2
		}
		cacheLoadEnabled = enabled
		cacheLoadBudget = budget
	}
	progressInterval, err := time.ParseDuration(progressIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-interval: %v\n", err)
		return 2
	}
	progressTargets, err := cliflags.ResolveProgressTargets(progressFilePaths, progressFormats)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-path/--progress-format: %v\n", err)
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(ackEveryRaw)
	if err != nil || ackEvery <= 0 {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	var fsyncInterval int64
	if fsyncIntervalRaw == "-1" {
		fsyncInterval = -1
	} else {
		fsyncInterval, err = encoding.ParseByteSize(fsyncIntervalRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --fsync-interval: %v\n", err)
			return 2
		}
		if fsyncInterval < 0 {
			fmt.Fprintln(stderr, "--fsync-interval must be >= 0 or -1")
			return 2
		}
	}
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptionsWithKeys(encryptMode, keysDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	if err := validateAuthTokens(authTokens, resolvedEncMode); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	compress, err := resolveCompress(compressRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --compress: %v\n", err)
		return 2
	}
	var deadlineMS int64
	if deadlineRaw != "" {
		d, dErr := time.ParseDuration(deadlineRaw)
		if dErr != nil || d <= 0 {
			fmt.Fprintf(stderr, "invalid --deadline: %v\n", dErr)
			return 2
		}
		deadlineMS = d.Milliseconds()
	}

	// Resolve output path: -o overrides, default is basename in cwd.
	outputPath := strings.TrimSpace(outFile)
	if outputPath == "" {
		outputPath = filepath.Base(remotePath)
	}
	if skipWrite {
		outputPath = os.DevNull
	}

	effectiveConcurrency := tx.DefaultClientConcurrency()
	if concurrency > 0 {
		effectiveConcurrency = concurrency
	}

	client := tx.NewClient(serverURL, tx.WithLoadStrategy(tx.LoadStrategyFast), tx.WithComp(compress), tx.WithClientAgePublicKey(agePublicKey), tx.WithClientAgeIdentity(ageIdentity), tx.WithEncryptMode(resolvedEncMode), tx.WithClientAuthTokens(authTokens...))
	defer client.Close()

	// Fetch manifest for the single file (skip full probe).
	fmt.Fprintf(stderr, "get(addr=[%s], path=[%s])\n", serverURL, remotePath)
	manifestResp, err := client.GetManifest(context.Background(), tx.GetManifestRequest{
		Directory:     remotePath,
		Mode:          tx.LoadStrategyFast,
		LinkMbps:      0,
		Concurrency:   effectiveConcurrency,
		DeadlineMS:    deadlineMS,
		CacheMap:    cacheMapValue(cacheLoadEnabled),
	})
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	manifest := manifestResp.Manifest
	if len(manifest.Entries) != 1 {
		fmt.Fprintf(stderr, "get failed: expected single file manifest, got %d entries\n", len(manifest.Entries))
		return 1
	}
	entry := manifest.Entries[0]
	fmt.Fprintf(stderr, "get-manifest: tid=%s file=%s size=%s\n", manifest.TransferID, entry.Path, encoding.HumanBytes(entry.Size))

	// Mini-probe to detect server send buffer and compute batch size.
	var miniProbe tx.ProbeResponse
	if probe, probeErr := client.ProbeLink(context.Background(), tx.ProbeRequest{ProbeBytes: 1}); probeErr == nil {
		miniProbe = probe
	}
	batchPlan := tx.ExplainBatchMaxBytes(
		miniProbe.SuggestedConcurrency,
		client.WindowConcurrency,
		client.FileRequestWindowBytes,
		miniProbe.ServerSendBufBytes,
		miniProbe.LinkMbps,
	)
	batchSize := batchPlan.BatchMaxBytes
	transferCtx, cancelTransfer := context.WithCancel(context.Background())
	defer cancelTransfer()
	probeInfo := startTransferProbeReporter(transferCtx, client, manifest.TransferID, tx.LoadStrategyFast, defaultCLIProbeBytes, miniProbe.LinkMbps)
	defer probeInfo.stop()

	start := time.Now()
	progressUpdates := make(chan tx.DownloadProgressUpdate, 128)
	var onProgressUpdate func(tx.DownloadProgressUpdate)
	if verbose {
		progressReporter := newVerboseProgressReporter(stderr)
		onProgressUpdate = progressReporter.ReportUpdate
	}
	forwardProgress := func(update tx.DownloadProgressUpdate) {
		if onProgressUpdate != nil {
			onProgressUpdate(update)
		}
	}
	// Use a no-op progress writer (no .tx state for single-file get).
	go func() {
		for update := range progressUpdates {
			forwardProgress(update)
		}
	}()

	syncWorker, stopSync := fsync.StartSyncWorker(fsyncInterval, skipFsync, progressInterval, stderr)
	defer func() {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), defaultFsyncTimeout)
		stopSync(stopCtx)
		cancelStop()
		if !skipFsync && outputPath != "-" && outputPath != os.DevNull {
			syncCtx, cancel := context.WithTimeout(context.Background(), defaultSyncfsTimeout)
			defer cancel()
			fsync.SyncfsDir(syncCtx, filepath.Dir(outputPath), progressInterval, stderr)
		}
	}()

	var totalCopied atomic.Int64
	var stopStatusPolling func()
	if verbosityFromFlags(progress, verbose) >= 1 {
		stopStatusPolling = startVerboseStatusPolling(manifest.TransferID, client, &totalCopied, entry.Size, nil, 1, probeInfo, stderr)
		defer stopStatusPolling()
	}
	outputWriter := func(me tx.ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
		w, syncFn, wErr := openDownloadOutput(me, offset, outputPath, stdout, syncWorker)
		if wErr != nil {
			return nil, nil, wErr
		}
		return &countingWriter{Writer: w, total: &totalCopied}, syncFn, nil
	}
	if len(progressTargets) > 0 {
		totalBytes := entry.Size
		stopProgressFile := filexfer.StartProgressFileWriter(context.Background(), progressTargets, progressInterval, func() filexfer.ProgressStatus {
			copied := totalCopied.Load()
			if totalBytes > 0 && copied > totalBytes {
				copied = totalBytes
			}
			doneFiles := uint64(0)
			if totalBytes <= 0 || copied >= totalBytes {
				doneFiles = 1
			}
			return filexfer.ProgressStatus{
				Source:     "client",
				DoneFiles:  doneFiles,
				TotalFiles: 1,
				DoneBytes:  copied,
				TotalBytes: totalBytes,
			}
		})
		defer func() { stopProgressFile(err == nil) }()
	}

	downloadBatchResp, err := client.GetFiles(transferCtx, tx.GetFilesRequest{
		Manifest:           manifest,
		FileIDs:            []uint64{entry.ID},
		BatchMaxBytes:      batchSize,
		SplitWindowWorkers: batchPlan.SplitWindowWorkers,
		OutputWriter:       outputWriter,
		ProgressUpdates:    progressUpdates,
	})
	elapsed := time.Since(start)
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	if len(downloadBatchResp.Files) != 1 {
		fmt.Fprintf(stderr, "get failed: expected one downloaded file, got %d\n", len(downloadBatchResp.Files))
		return 1
	}
	downloadResp := downloadBatchResp.Files[0]
	if err := applyDownloadedTrailerMetadata(outputPath, downloadResp.Meta.TrailerMetadata); err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	if cacheLoadEnabled {
		touchGetCache(outputPath, entry, cacheLoadBudget, stderr)
	}
	printFileMetrics(stdout, manifest.TransferID, entry.ID, outputPath, downloadResp.Meta, downloadResp.LocalFileHash, elapsed)
	return 0
}

func touchGetCache(outputPath string, entry tx.ManifestEntry, cacheLoadBudget time.Duration, stderr io.Writer) {
	if !pagecache.TouchSupported() || outputPath == "-" || outputPath == os.DevNull || len(entry.PageCache) == 0 {
		return
	}
	ce := &pagecache.CacheEntry{}
	if err := encoding.DecodePageCacheEntry(entry.PageCache, ce); err != nil || ce.Empty() {
		return
	}
	budget := pagecache.SystemPageBudget(pagecache.TouchCacheReserveBytes)
	ctx := context.Background()
	cancel := func() {}
	if cacheLoadBudget > 0 {
		ctx, cancel = context.WithTimeout(ctx, cacheLoadBudget)
	}
	defer cancel()
	start := time.Now()
	summary, touchErr := pagecache.TouchEntries(ctx, func(yield func(pagecache.TouchEntry) bool) {
		yield(pagecache.TouchEntry{Path: outputPath, Entry: ce})
	}, budget, 1)
	status := "[ok]"
	budgetPart := ""
	if errors.Is(touchErr, context.DeadlineExceeded) {
		status = "[partial-ok]"
		budgetPart = fmt.Sprintf(" budget=%s", cacheLoadBudget)
	}
	errPart := ""
	if summary.OpenErrors+summary.AdviseErrors+summary.ReadErrors > 0 {
		errPart = fmt.Sprintf(" errs=open=%d/advise=%d/read=%d",
			summary.OpenErrors, summary.AdviseErrors, summary.ReadErrors)
	}
	fmt.Fprintf(stderr,
		"cache-touch: %s warmed=%d/1 budget-pages=%d total-pages=%d%s elapsed=%s%s\n",
		status, summary.Touched, budget, ce.NumResidentPages(), budgetPart, time.Since(start).Round(time.Millisecond), errPart,
	)
}
