package filexfercli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/fsync"
)

type startArgs struct {
	targetDir           string
	agePublicKey        string
	ageIdentity         string
	encMode             string
	authTokens          []string
	verbosity           int
	concurrency         int
	concurrencyExplicit bool
	ackEvery            int64
	compress            string
	noSync              bool
	fsyncInterval       int64
	discard             bool
	deadlineMS          int64
	traceFile           string
	progressTargets     []filexfer.ProgressTarget
	progressInterval    time.Duration
}

func formatStartBatchCause(plan tx.BatchSizePlan) string {
	baseCause := "window"
	bwActive := plan.BwCeilBytes > 0 && plan.BwCeilBytes < plan.ConcBatchBytes
	baseBatch := plan.ConcBatchBytes
	if bwActive {
		baseCause = "bw-probe"
		baseBatch = plan.BwCeilBytes
	}
	if plan.BatchMaxBytes == plan.FloorBytes && plan.FloorBytes > baseBatch {
		if baseCause == "bw-probe" {
			return "bw-probe, raised to socket size"
		}
		return "floor"
	}
	return baseCause
}

func formatStartBatchWindowLine(windowBytes int64, plan tx.BatchSizePlan) string {
	return fmt.Sprintf(
		"    window: %s / %d per-file-workers = %s",
		encoding.HumanBytes(windowBytes),
		plan.PerFileWorkers,
		encoding.HumanBytes(plan.ConcBatchBytes),
	)
}

func formatStartBatchProbeLine(linkMiBPerSec int64, suggestedConcurrency int, plan tx.BatchSizePlan) string {
	if plan.BwCeilBytes <= 0 || plan.BwCeilBytes >= plan.ConcBatchBytes {
		return ""
	}
	return fmt.Sprintf(
		"    bw-probe: %d MiB/s / %d conc / 2 = %s",
		linkMiBPerSec,
		suggestedConcurrency,
		encoding.HumanBytes(plan.BwCeilBytes),
	)
}

func runStartCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := cliflags.New("start")
	cf.SetOutput(stderr)
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx recv start [flags] <target-dir>")
		cf.PrintDefaults(stderr)
	}
	var encryptMode string
	var keysDir string
	var authTokens []string
	var concurrency int
	var ackEveryRaw string
	var compressRaw string
	var noSync bool
	var fsyncIntervalRaw string
	var verbose bool
	var progress bool
	var discard bool
	var deadlineRaw string
	var progressFilePaths []string
	var progressFormats []string
	var progressIntervalRaw string
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&keysDir, "k", "keys", "", "Persistent age keys directory (default: ephemeral)")
	cf.StringSliceVar(&authTokens, "t", "auth-token", "Client auth token presented in encrypted AUTH blob; repeatable")
	cf.BoolVar(&progress, "", "progress", true, "Show transfer progress every 2s")
	cf.BoolVar(&verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringSliceVar(&progressFilePaths, "p", "progress-path", "Progress output target; repeatable, use - for stdout")
	cf.StringSliceVar(&progressFormats, "f", "progress-format", "Progress format: json|int; 1 applies to all targets, or one per target (default json)")
	cf.StringVar(&progressIntervalRaw, "", "progress-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	cf.BoolVar(&discard, "", "skip-write", false, "Discard downloaded file contents instead of writing to the target directory")
	cf.BoolVar(&discard, "", "discard", false, "Discard downloaded file contents instead of writing to the target directory")
	cf.IntVar(&concurrency, "", "concurrency", 0, "Parallel download workers (0=manifest default)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	cf.StringVar(&ackEveryRaw, "a", "ack-every", ackEveryRaw, "Bytes between progress acks; 1B, 4KiB, 8MiB")
	cf.StringVar(&compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.BoolVar(&noSync, "", "skip-fsync", false, "Ack without fdatasync")
	cf.BoolVar(&noSync, "", "no-sync", false, "Ack without fdatasync")
	fsyncIntervalRaw = "512MiB"
	cf.StringVar(&fsyncIntervalRaw, "", "fsync-interval", fsyncIntervalRaw, "Background fsync batch threshold; 0=inline fdatasync, -1=syncfs-only at exit")
	cf.StringVar(&deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	var traceFile string
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "start requires exactly one positional argument: <target-dir>")
		return 2
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
	verbosity := verbosityFromFlags(progress, verbose)
	var deadlineMS int64
	if deadlineRaw != "" {
		d, err := time.ParseDuration(deadlineRaw)
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
	concurrencyExplicit := false
	cf.Visit(func(f *flag.Flag) {
		if f.Name == "concurrency" {
			concurrencyExplicit = true
		}
	})
	if concurrencyExplicit && concurrency <= 0 {
		fmt.Fprintln(stderr, "--concurrency must be > 0")
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(ackEveryRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	if ackEvery <= 0 {
		fmt.Fprintln(stderr, "--ack-every must be > 0")
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
	return runStart(serverURL, startArgs{
		targetDir:           cf.Arg(0),
		agePublicKey:        agePublicKey,
		ageIdentity:         ageIdentity,
		encMode:             resolvedEncMode,
		authTokens:          authTokens,
		verbosity:           verbosity,
		concurrency:         concurrency,
		concurrencyExplicit: concurrencyExplicit,
		ackEvery:            ackEvery,
		compress:            compress,
		noSync:              noSync,
		fsyncInterval:       fsyncInterval,
		discard:             discard,
		deadlineMS:          deadlineMS,
		traceFile:           traceFile,
		progressTargets:     progressTargets,
		progressInterval:    progressInterval,
	}, stdout, stderr)
}

func runStart(serverURL string, cfg startArgs, stdout io.Writer, stderr io.Writer) int {
	outputMu := &sync.Mutex{}
	stdout = &synchronizedWriter{mu: outputMu, w: stdout}
	stderr = &synchronizedWriter{mu: outputMu, w: stderr}

	stopTracing := startTracing(cfg.traceFile, stderr)
	defer stopTracing()
	ps, err := newPinchState(cfg.targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	fmt.Fprintf(stderr, "start-state: <(%s) > %s\n", ps.ServerManifestPath, ps.TargetDir)
	manifest, err := tx.LoadManifest(ps.ServerManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
		return 1
	}
	loadStrategy, err := resolveLoadStrategy(manifest.Mode)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: invalid manifest mode %q\n", manifest.Mode)
		return 1
	}
	manifestConcurrency := manifest.Concurrency
	if manifestConcurrency <= 0 {
		fmt.Fprintf(stderr, "load manifest failed: invalid manifest concurrency %d\n", manifestConcurrency)
		return 1
	}
	effectiveConcurrency := manifestConcurrency
	if cfg.concurrencyExplicit {
		effectiveConcurrency = cfg.concurrency
	}
	txferID := manifest.TransferID
	outRoot := ps.StagingDir
	if cfg.discard {
		outRoot = os.DevNull
	} else {
		if err := ps.ensureStagingDir(); err != nil {
			fmt.Fprintf(stderr, "create staging directory failed: %v\n", err)
			return 1
		}
	}
	manifestFingerprint := tx.ManifestFingerprint(manifest)
	progressFingerprint, fpErr := loadProgressFingerprint(ps.ProgressPath)
	if fpErr != nil {
		fmt.Fprintf(stderr, "load progress fingerprint failed: %v\n", fpErr)
		return 1
	}
	progressState, err := loadProgressState(ps.ProgressPath)
	if err != nil {
		fmt.Fprintf(stderr, "load progress failed: %v\n", err)
		return 1
	}
	if len(progressState) > 0 && progressFingerprint != manifestFingerprint {
		if progressFingerprint == "" {
			fmt.Fprintf(stderr, "copy-resume: progress file has no manifest fingerprint; discarding %d entries\n", len(progressState))
		} else {
			fmt.Fprintf(stderr, "copy-resume: manifest fingerprint mismatch (manifest=%s progress=%s); discarding %d progress entries\n", manifestFingerprint, progressFingerprint, len(progressState))
		}
		progressState = make(map[uint64]tx.ManifestProgress)
		_ = os.Remove(ps.ProgressPath)
	}
	applyProgressStateToManifest(manifest, progressState)
	printResumeProgress(stderr, "copy-resume", manifest.Entries)
	if cfg.deadlineMS > 0 {
		manifest.DeadlineMS = cfg.deadlineMS
	}
	progressUpdates := make(chan tx.DownloadProgressUpdate, 1024)
	entryByID := manifestEntriesByID(manifest)
	var onStartProgressUpdate func(tx.DownloadProgressUpdate)
	if cfg.verbosity >= 2 {
		progressReporter := newVerboseProgressReporter(stderr)
		onStartProgressUpdate = progressReporter.ReportUpdate
	}
	forwardProgress := func(update tx.DownloadProgressUpdate) {
		if onStartProgressUpdate != nil {
			onStartProgressUpdate(update)
		}
	}
	stopProgress, persistProgressAck, markMetadataDonePersisted := startProgressWriter(ps.ProgressPath, manifestFingerprint, progressState, progressUpdates, forwardProgress, stderr)
	persistFileDone := func(fileID uint64, ackBytes int64) {
		persistProgressAck(fileID, ackBytes)
	}
	markMetadataDone := func(fileID uint64) {
		markMetadataDonePersisted(fileID)
	}
	progressStopped := false
	defer func() {
		if !progressStopped {
			stopProgress()
		}
	}()
	client := tx.NewClient(serverURL, tx.WithLoadStrategy(loadStrategy), tx.WithComp(cfg.compress), tx.WithClientAgePublicKey(cfg.agePublicKey), tx.WithClientAgeIdentity(cfg.ageIdentity), tx.WithEncryptMode(cfg.encMode), tx.WithClientAuthTokens(cfg.authTokens...))
	defer client.Close()
	startAll := time.Now()
	var completed int64
	var totalTransferred int64
	var failures []error
	var failuresMu sync.Mutex
	recordFailure := func(err error) {
		if err == nil {
			return
		}
		failuresMu.Lock()
		failures = append(failures, err)
		failuresMu.Unlock()
	}
	markCompleted := func(entry tx.ManifestEntry) {
		persistFileDone(entry.ID, entry.Size)
		markMetadataDone(entry.ID)
	}
	pendingWork, completedNow := collectPendingManifestWork(
		manifest.Entries,
		cfg.discard,
		markCompleted,
		func(entry tx.ManifestEntry) error {
			return refreshCompletedFileMetadata(context.Background(), client, manifest, entry.ID, outRoot, "")
		},
		recordFailure,
	)
	completed += completedNow
	syncWorker, stopSync := fsync.StartSyncWorker(cfg.fsyncInterval, cfg.noSync, stderr)
	defer func() {
		stopSync()
		if !cfg.noSync && !cfg.discard {
			syncCtx, cancel := context.WithTimeout(context.Background(), defaultSyncfsTimeout)
			defer cancel()
			fsync.SyncfsDir(syncCtx, filepath.Dir(ps.TargetDir), stderr)
		}
	}()

	totalAllBytes, totalAllFiles, priorBytes, priorFiles := progressTotals(manifest.Entries)
	if err := validateResumeProgressTotals(manifest.Entries, pendingWork, totalAllBytes, totalAllFiles, priorBytes, priorFiles); err != nil {
		fmt.Fprintf(stderr, "copy-resume: progress/manifest mismatch: %v\n", err)
		return 1
	}
	var totalCopied atomic.Int64
	totalCopied.Store(priorBytes)
	var doneFiles atomic.Uint64
	doneFiles.Store(priorFiles)

	if len(pendingWork.files) > 0 {
		var miniProbe tx.ProbeResponse
		if probe, probeErr := client.ProbeLink(context.Background(), tx.ProbeRequest{ProbeBytes: 1}); probeErr == nil {
			miniProbe = probe
		}
		rawLinkMbps := max(manifest.LinkMbps, miniProbe.LinkMbps)
		gentleCPUPct := tx.NormalizeGentleCPUPct(miniProbe.GentleCPUPct)
		gentleBWPct := tx.NormalizeGentleBWPct(miniProbe.GentleBWPct)
		effectiveLinkMbps := effectiveModeLinkMbps(loadStrategy, rawLinkMbps, gentleBWPct)
		batchSize := tx.SuggestBatchMaxBytes(
			miniProbe.SuggestedConcurrency,
			client.WindowConcurrency,
			client.FileRequestWindowBytes,
			miniProbe.ServerSendBufBytes,
			effectiveLinkMbps,
		)
		batchPlan := tx.ExplainBatchMaxBytes(
			miniProbe.SuggestedConcurrency,
			client.WindowConcurrency,
			client.FileRequestWindowBytes,
			miniProbe.ServerSendBufBytes,
			effectiveLinkMbps,
		)
		linkMiBPerSec := rawLinkMbps * 1_000_000 / 8 / (1 << 20)
		effectiveLinkMiBPerSec := effectiveLinkMbps * 1_000_000 / 8 / (1 << 20)
		fmt.Fprintf(stderr, "start-plan:\n")
		manifestMem, manifestDisk := manifest.Size()
		fmt.Fprintf(stderr, "  manifest: %d files indexed in [mem=%s, serialized=%s]\n",
			len(manifest.Entries),
			encoding.HumanBytesFixedWidth(manifestMem, 4),
			encoding.HumanBytesFixedWidth(manifestDisk, 4))
		if loadStrategy == tx.LoadStrategyGentle {
			fmt.Fprintf(stderr, "  server: %d cpu, %d io-depth, %d Mbps (%d MiB/s), %d%% gentle-cpu, %d%% gentle-bw\n",
				miniProbe.ServerCPU, miniProbe.ServerIODepth, rawLinkMbps, linkMiBPerSec, gentleCPUPct, gentleBWPct)
			fmt.Fprintf(stderr, "  mode: [%s] → concurrency = %d cpu * %d%% = %d, bw-limit = %d MiB/s * %d%% = %d MiB/s\n",
				loadStrategy, miniProbe.ServerCPU, gentleCPUPct, miniProbe.SuggestedConcurrency, linkMiBPerSec, gentleBWPct, effectiveLinkMiBPerSec)
		} else {
			fmt.Fprintf(stderr, "  server: %d cpu, %d io-depth, %d Mbps (%d MiB/s)\n",
				miniProbe.ServerCPU, miniProbe.ServerIODepth, rawLinkMbps, linkMiBPerSec)
			fmt.Fprintf(stderr, "  mode: [%s] → concurrency = %d cpu * %d io-depth = %d, bw-limit = none\n",
				loadStrategy, miniProbe.ServerCPU, miniProbe.ServerIODepth, miniProbe.SuggestedConcurrency)
		}
		if cfg.concurrencyExplicit {
			fmt.Fprintf(stderr, "  concurrency: %d (override from --concurrency, server suggested %d)\n",
				effectiveConcurrency, miniProbe.SuggestedConcurrency)
		} else {
			fmt.Fprintf(stderr, "  concurrency: %d\n", effectiveConcurrency)
		}
		fmt.Fprintf(stderr, "  conn-pool: %d\n", miniProbe.WarmConnectionPoolSize)
		fmt.Fprintf(stderr, "    window: %d\n", batchPlan.EffectiveWinConc)
		fmt.Fprintf(stderr, "    batch-per-window: %d\n", batchPlan.PerFileWorkers)
		fmt.Fprintf(stderr, "  batch: %s (from %s)\n",
			encoding.HumanBytes(batchPlan.BatchMaxBytes),
			formatStartBatchCause(batchPlan))
		fmt.Fprintln(stderr, formatStartBatchWindowLine(client.FileRequestWindowBytes, batchPlan))
		if bwProbeLine := formatStartBatchProbeLine(effectiveLinkMiBPerSec, miniProbe.SuggestedConcurrency, batchPlan); bwProbeLine != "" {
			fmt.Fprintln(stderr, bwProbeLine)
		}
		outputWriter := func(entry tx.ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			destPath := resolveDownloadDestinationPath(entry, outRoot, "")
			w, syncFn, err := openDownloadOutput(entry, offset, destPath, nil, syncWorker)
			if err != nil {
				return nil, nil, err
			}
			return &countingWriter{Writer: w, total: &totalCopied}, syncFn, nil
		}
		startResp, err := downloadManifestFiles(manifestDownloadConfig{
			Client:             client,
			Manifest:           manifest,
			Entries:            pendingWork.files,
			Concurrency:        effectiveConcurrency,
			BatchMaxBytes:      batchSize,
			SplitWindowWorkers: batchPlan.SplitWindowWorkers,
			ProgressUpdates:    progressUpdates,
			OutputWriter:       outputWriter,
			OnFileDone: func(evt tx.StartFileDoneEvent) {
				entry, ok := entryByID[evt.File.Meta.FileID]
				if !ok {
					recordFailure(fmt.Errorf("id=%d metadata apply failed: file id not in manifest", evt.File.Meta.FileID))
					return
				}
				destPath := resolveDownloadDestinationPath(entry, outRoot, "")
				if err := applyDownloadedTrailerMetadata(destPath, evt.File.Meta.TrailerMetadata); err != nil {
					recordFailure(fmt.Errorf("id=%d metadata apply failed: %w", evt.File.Meta.FileID, err))
					return
				}
				persistFileDone(evt.File.Meta.FileID, entry.Size)
				markMetadataDone(evt.File.Meta.FileID)
				if cfg.verbosity >= 2 {
					printStartFileSummary(stdout, evt.File.Meta.FileID, destPath, evt.File.Meta, evt.File.LocalFileHash, evt.File.WindowChecksumPassed, evt.File.WindowChecksumTotal, evt.Elapsed)
				}
			},
			TotalCopied:      &totalCopied,
			DoneFiles:        &doneFiles,
			ProgressTargets:  cfg.progressTargets,
			ProgressInterval: cfg.progressInterval,
			Stderr:           stderr,
			Verbosity:        cfg.verbosity,
			TransferID:       txferID,
			TransferMode:     loadStrategy,
			ProbeBytes:       defaultCLIProbeBytes,
			ObservedLinkMbps: manifest.LinkMbps,
			StatusTotalBytes: totalAllBytes,
			StatusTotalFiles: totalAllFiles,
			StatusPolling:    true,
		})
		if err != nil {
			stopProgress()
			progressStopped = true
			fmt.Fprintf(stderr, "start failed: %v\n", err)
			return 1
		}
		completed += int64(startResp.Downloaded)
		totalTransferred += startResp.TransferredBytes
		for _, startErr := range startResp.Errors {
			recordFailure(startErr)
		}
	}
	stopProgress()
	progressStopped = true
	applyProgressStateToManifest(manifest, progressState)
	if err := saveProgressState(ps.ProgressPath, manifestFingerprint, progressState); err != nil {
		fmt.Fprintf(stderr, "save progress state failed: %v\n", err)
		return 1
	}
	// Apply non-file entries after all file data has been downloaded.
	if !cfg.discard {
		for _, nfErr := range applyNonFileEntries(manifest.Entries, pendingWork.hardlinks, pendingWork.symlinks, pendingWork.dirs, outRoot) {
			recordFailure(nfErr)
		}
	}
	failuresMu.Lock()
	finalFailures := append([]error(nil), failures...)
	failuresMu.Unlock()
	printTransferErrors(stderr, "start", finalFailures, cfg.verbosity)

	elapsedAll := time.Since(startAll)
	overallSpeed := 0.0
	if elapsedAll > 0 {
		overallSpeed = float64(totalTransferred) / elapsedAll.Seconds()
	}
	fmt.Fprintf(
		stdout,
		"start-complete: tid=%s requested=%d downloaded=%d failed=%d transferred=%s speed=%s elapsed=%s sync-conn-fallbacks=%d\n",
		txferID,
		len(manifest.Entries),
		completed,
		len(finalFailures),
		encoding.HumanBytes(totalTransferred),
		encoding.HumanRate(overallSpeed),
		elapsedAll.Round(time.Millisecond),
		client.MetricSnapshot().SyncConnectionCount,
	)
	if len(finalFailures) > 0 {
		return 1
	}
	if cfg.discard {
		if err := os.Remove(ps.ProgressPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "remove progress state failed: %v\n", err)
			return 1
		}
		return 0
	}

	if err := os.RemoveAll(ps.TargetDir); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "remove old target directory failed: %v\n", err)
		return 1
	}
	if err := os.Rename(ps.StagingDir, ps.TargetDir); err != nil {
		fmt.Fprintf(stderr, "rename staging to target failed: %v\n", err)
		return 1
	}
	if err := tx.SaveManifest(ps.ManifestPath, manifest); err != nil {
		fmt.Fprintf(stderr, "save local manifest failed: %v\n", err)
		return 1
	}
	return 0
}

func printStartFileSummary(stdout io.Writer, fileID uint64, path string, meta tx.FileFrameMeta, localFileHash string, windowChecksumPassed, windowChecksumTotal int, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 0.000001
	}
	speed := float64(meta.Size) / seconds
	compSummary := formatCompSummary(meta)
	var checksum string
	switch {
	case windowChecksumTotal > 0:
		checksum = fmt.Sprintf("wxsum=[%d/%d]", windowChecksumPassed, windowChecksumTotal)
	case meta.FileHashToken != "" && localFileHash != "" && strings.EqualFold(meta.FileHashToken, localFileHash):
		checksum = "checksum=[ok]"
	case meta.FileHashToken != "" && localFileHash != "":
		checksum = "checksum=[x]"
	default:
		checksum = "checksum=[-]"
	}
	// Build the full line before writing to avoid multiple Write calls (and lock
	// acquisitions) on the synchronized stdout writer.
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString("start-file: fd=")
	sb.WriteString(strconv.FormatUint(fileID, 10))
	sb.WriteString(" path=")
	sb.WriteString(path)
	sb.WriteByte(' ')
	sb.WriteString(checksum)
	sb.WriteString(" comp=")
	sb.WriteString(compSummary)
	sb.WriteString(" rate=")
	sb.WriteString(encoding.HumanRate(speed))
	sb.WriteByte('\n')
	io.WriteString(stdout, sb.String())
}

func startVerboseStatusPolling(txferID string, client *tx.Client, localCopied *atomic.Int64, localTotalBytes int64, localDoneFiles *atomic.Uint64, localTotalFiles uint64, probe *probeReporter, stderr io.Writer) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(defaultVerboseProgressInterval)
		defer ticker.Stop()
		// Seed prevCopied from the current counter so the first tick's rate
		// delta is 0 even when localCopied was pre-seeded with bytes already
		// acked in a prior session (resume).
		prevCopied := localCopied.Load()
		prevTime := time.Now()
		for {
			statusResp, statusErr := client.GetStatus(ctx, tx.GetStatusRequest{
				TransferID: txferID,
			})
			if statusErr != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Fprintf(stderr, "status refresh failed: %v\n", statusErr)
			} else {
				s := statusResp.Status
				// Use local byte counter for rate/ETA — it updates on every
				// frame write, not just on ACK, so rate is never stuck at 0
				// while data is actively streaming.
				copied := localCopied.Load()
				totalBytes := localTotalBytes
				if totalBytes <= 0 {
					totalBytes = s.TotalSize
				}
				doneFiles := uint64(s.Done)
				if localDoneFiles != nil {
					doneFiles = localDoneFiles.Load()
				}
				totalFiles := localTotalFiles
				if totalFiles == 0 {
					totalFiles = uint64(s.NumFiles)
				}
				now := time.Now()
				dt := now.Sub(prevTime).Seconds()
				var rateBps float64
				if dt > 0 {
					rateBps = float64(copied-prevCopied) / dt
				}
				prevCopied = copied
				prevTime = now

				var pctBytes float64
				if totalBytes > 0 {
					pctBytes = float64(copied) * 100 / float64(totalBytes)
				}
				var pctFiles float64
				if totalFiles > 0 {
					pctFiles = float64(doneFiles) * 100 / float64(totalFiles)
				}
				etaDisplay := fixedWidthETANA()
				if rateBps > 0 && totalBytes > copied {
					remaining := float64(totalBytes - copied)
					etaSec := remaining / rateBps
					etaDisplay = fixedWidthETA(time.Duration(etaSec * float64(time.Second)))
				}
				probePart := formatProbeRateSuffix(now, rateBps, probe)
				fmt.Fprintf(
					stderr,
					"txfer-progress:[%6s/%6s](%5.1f%%) [%s/%s](%5.1f%%) [eta:%s]@[%s]%s\n",
					encoding.HumanCount(doneFiles, 6), encoding.HumanCount(totalFiles, 6),
					pctFiles,
					encoding.HumanBytesFixedWidth(copied, fixedWidthProgressBytesWidth),
					encoding.HumanBytesFixedWidth(totalBytes, fixedWidthProgressBytesWidth),
					pctBytes,
					etaDisplay, encoding.HumanRateFixedWidth(rateBps, fixedWidthProgressRateWidth), probePart,
				)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
