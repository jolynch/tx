package filexfercli

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

type syncArgs struct {
	sourceDir           string
	targetDir           string
	agePublicKey        string
	ageIdentity         string
	encMode             string
	authTokens          []string
	concurrency         int
	concurrencyExplicit bool
	ackEvery            int64
	compress            string
	noSync              bool
	fsyncInterval       int64
	skipWrite           bool
	verbosity           int
	yes                 bool
	probeBytes          int64
	traceFile           string
	progressTargets     []filexfer.ProgressTarget
	progressInterval    time.Duration
}

func confirmSyncProceed(stderr io.Writer, newCount int, staleCount int, rmCount int) bool {
	if !syncPromptIsTerminal() {
		return true
	}

	defaultYes := rmCount == 0 && (newCount > 0 || staleCount > 0)
	prompt := "[y/N]"
	if defaultYes {
		prompt = "[Y/n]"
	}
	fmt.Fprintf(stderr, "proceed? %s: ", prompt)

	scanner := bufio.NewScanner(syncPromptInput)
	if !scanner.Scan() {
		fmt.Fprintln(stderr, "aborted")
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if answer == "" {
		if defaultYes {
			return true
		}
		fmt.Fprintln(stderr, "aborted")
		return false
	}
	if strings.HasPrefix(answer, "y") {
		return true
	}
	fmt.Fprintln(stderr, "aborted")
	return false
}

func runSyncCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := cliflags.New("sync")
	cf.SetOutput(stderr)
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx recv sync [-s <dir>] [flags] <target-dir>")
		cf.PrintDefaults(stderr)
	}
	var sourceDir string
	var encryptMode string
	var keysDir string
	var authTokens []string
	var concurrency int
	var ackEveryRaw string
	var compressRaw string
	var noSync bool
	var fsyncIntervalRaw string
	var skipWrite bool
	var verbose bool
	var yes bool
	var probeSizeRaw string
	var traceFile string
	var progressFilePaths []string
	var progressFormats []string
	var progressIntervalRaw string
	cf.StringVar(&sourceDir, "s", "source-directory", "", "Absolute source directory on server (default: manifest root)")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&keysDir, "k", "keys", "", "Persistent age keys directory (default: ephemeral)")
	cf.StringSliceVar(&authTokens, "t", "auth-token", "Client auth token presented in encrypted AUTH blob; repeatable")
	cf.StringVar(&compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.IntVar(&concurrency, "", "concurrency", 0, "Parallel download workers (0=manifest default)")
	cf.BoolVar(&yes, "y", "yes", false, "Skip confirmation prompt")
	cf.BoolVar(&verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringSliceVar(&progressFilePaths, "p", "progress-path", "Progress output target; repeatable, use - for stdout")
	cf.StringSliceVar(&progressFormats, "f", "progress-format", "Progress format: json|int; 1 applies to all targets, or one per target (default json)")
	cf.StringVar(&progressIntervalRaw, "", "progress-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	cf.StringVar(&ackEveryRaw, "a", "ack-every", ackEveryRaw, "Bytes between progress acks; 1B, 4KiB, 8MiB")
	cf.BoolVar(&skipWrite, "", "skip-write", false, "Do not mutate the target directory; fetch bodies to discard instead of writing them")
	cf.BoolVar(&noSync, "", "skip-fsync", false, "Ack without fdatasync")
	cf.BoolVar(&noSync, "", "no-sync", false, "Ack without fdatasync")
	fsyncIntervalRaw = "512MiB"
	cf.StringVar(&fsyncIntervalRaw, "", "fsync-interval", fsyncIntervalRaw, "Background fsync batch threshold; 0=inline fdatasync, -1=syncfs-only at exit")
	probeSizeRaw = encoding.HumanBytes(defaultCLIProbeBytes)
	cf.StringVar(&probeSizeRaw, "", "probe-size", probeSizeRaw, "Probe payload size; 1B, 4KiB, 8MiB")
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "sync requires exactly one positional argument: <target-dir>")
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
	probeBytes, err := encoding.ParseByteSize(probeSizeRaw)
	if err != nil || probeBytes <= 0 {
		fmt.Fprintf(stderr, "invalid --probe-size: %v\n", err)
		return 2
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
	verbosity := verbosityFromFlags(false, verbose)
	return runSync(serverURL, syncArgs{
		sourceDir:           sourceDir,
		targetDir:           cf.Arg(0),
		agePublicKey:        agePublicKey,
		ageIdentity:         ageIdentity,
		encMode:             resolvedEncMode,
		authTokens:          authTokens,
		concurrency:         concurrency,
		concurrencyExplicit: concurrencyExplicit,
		ackEvery:            ackEvery,
		compress:            compress,
		noSync:              noSync,
		fsyncInterval:       fsyncInterval,
		skipWrite:           skipWrite,
		verbosity:           verbosity,
		yes:                 yes,
		probeBytes:          probeBytes,
		traceFile:           traceFile,
		progressTargets:     progressTargets,
		progressInterval:    progressInterval,
	}, stdout, stderr)
}

func runSync(serverURL string, cfg syncArgs, stdout io.Writer, stderr io.Writer) int {
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
	fmt.Fprintf(stderr, "sync-state: >(%s) <(%s)\n", ps.ManifestPath, ps.TargetDir)

	outRoot := ps.TargetDir

	syncWorker, stopSync := fsync.StartSyncWorker(cfg.fsyncInterval, cfg.noSync, stderr)
	defer func() {
		stopSync()
		if !cfg.noSync {
			syncCtx, cancel := context.WithTimeout(context.Background(), defaultSyncfsTimeout)
			defer cancel()
			fsync.SyncfsDir(syncCtx, filepath.Dir(ps.TargetDir), stderr)
		}
	}()

	// Load server manifest once for metadata (Root, Mode, Concurrency, etc.).
	serverManifest, serverManifestErr := tx.LoadManifest(ps.ServerManifestPath)
	if serverManifestErr != nil {
		if cfg.sourceDir == "" {
			fmt.Fprintf(stderr, "load server manifest failed (run transfer first, or provide -s): %v\n", serverManifestErr)
			return 1
		}
		// No server manifest yet; use minimal defaults and rely on -s.
		serverManifest = &tx.Manifest{Mode: tx.LoadStrategyFast, Concurrency: 48, LinkMbps: 1000}
	}

	syncSourceDir := cfg.sourceDir
	if syncSourceDir == "" {
		syncSourceDir = serverManifest.Root
	}
	if syncSourceDir == "" {
		fmt.Fprintln(stderr, "sync requires --source-directory (or -s) when manifest.server has no root")
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(serverManifest.Mode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid manifest mode: %v\n", err)
		return 1
	}

	for round := 0; round < maxSyncRounds; round++ {
		// Build local manifest by scanning the target directory (what client has on disk).
		oldManifest, err := scanLocalDir(ps.TargetDir, serverManifest)
		if err != nil {
			fmt.Fprintf(stderr, "scan local directory failed: %v\n", err)
			return 1
		}

		// Probe link bandwidth.
		client := tx.NewClient(serverURL, tx.WithLoadStrategy(loadStrategy), tx.WithComp(cfg.compress), tx.WithClientAgePublicKey(cfg.agePublicKey), tx.WithClientAgeIdentity(cfg.ageIdentity), tx.WithEncryptMode(cfg.encMode), tx.WithClientAuthTokens(cfg.authTokens...))
		defer client.Close()
		probeResult, err := client.ProbeLink(context.Background(), tx.ProbeRequest{
			ProbeBytes:   cfg.probeBytes,
			LoadStrategy: loadStrategy,
		})
		if err != nil {
			fmt.Fprintf(stderr, "probe failed: %v\n", err)
			return 1
		}
		batchPlan := tx.ExplainBatchMaxBytes(
			probeResult.SuggestedConcurrency,
			client.WindowConcurrency,
			client.FileRequestWindowBytes,
			probeResult.ServerSendBufBytes,
			effectiveModeLinkMbps(loadStrategy, probeResult.LinkMbps, probeResult.GentleBWPct),
		)
		batchSize := batchPlan.BatchMaxBytes

		// SYNC: send old manifest, receive new manifest + removed paths.
		syncResp, err := client.SyncManifest(context.Background(), tx.SyncManifestRequest{
			Directory:   syncSourceDir,
			OldManifest: oldManifest,
			Mode:        loadStrategy,
			LinkMbps:    probeResult.LinkMbps,
			Concurrency: probeResult.SuggestedConcurrency,
		})
		if err != nil {
			fmt.Fprintf(stderr, "sync failed: %v\n", err)
			return 1
		}
		newManifest := syncResp.Manifest

		// Compute delta: new, stale, unchanged, removed.
		oldByPath := make(map[string]tx.ManifestEntry, len(oldManifest.Entries))
		for _, e := range oldManifest.Entries {
			oldByPath[e.Path] = e
		}
		var newFiles, staleFiles, unchangedFiles []tx.ManifestEntry
		var newBytes, staleBytes int64
		for _, entry := range newManifest.Entries {
			if old, ok := oldByPath[entry.Path]; ok {
				if manifestEntryMatches(old, entry) {
					entry.Progress = tx.ManifestProgress{AckBytes: entry.Size, MetadataDone: true}
					unchangedFiles = append(unchangedFiles, entry)
				} else {
					staleFiles = append(staleFiles, entry)
					staleBytes += entry.Size
				}
			} else {
				newFiles = append(newFiles, entry)
				newBytes += entry.Size
			}
		}
		rmPaths := syncResp.RemovedPaths
		newCount := encoding.HumanCount(uint64(len(newFiles)), 6)
		staleCount := encoding.HumanCount(uint64(len(staleFiles)), 6)
		unchangedCount := encoding.HumanCount(uint64(len(unchangedFiles)), 6)
		rmCount := encoding.HumanCount(uint64(len(rmPaths)), 6)

		oldMem, _ := oldManifest.Size()
		newMem, newDisk := newManifest.Size()
		fmt.Fprintf(stderr,
			"sync-manifests[%d]: local %d files [mem=%s], remote %d files [mem=%s, disk=%s]\n",
			round,
			len(oldManifest.Entries), encoding.HumanBytes(oldMem),
			len(newManifest.Entries), encoding.HumanBytes(newMem),
			encoding.HumanBytes(newDisk),
		)
		fmt.Fprintf(stderr,
			"sync-delta[%d]: new[%s (%s)] stale[%s (%s)] same[%s] rm[%s] link=%s srv-conc=(%d cpu * %d io = %d) batch=%s\n",
			round,
			newCount, encoding.HumanBytes(newBytes),
			staleCount, encoding.HumanBytes(staleBytes),
			unchangedCount,
			rmCount,
			formatProbeLinkSummary(probeResult),
			probeResult.ServerCPU, probeResult.ServerIODepth, probeResult.SuggestedConcurrency,
			encoding.HumanBytes(batchSize),
		)

		if len(newFiles) == 0 && len(staleFiles) == 0 && len(rmPaths) == 0 {
			fmt.Fprintln(stderr, "sync: remote and local converged, nothing to do")
			return 0
		}

		// Prompt for confirmation on the first round only.
		if round == 0 && !cfg.yes && !cfg.skipWrite {
			if !confirmSyncProceed(stderr, len(newFiles), len(staleFiles), len(rmPaths)) {
				return 0
			}
		}

		// Build merged manifest with new entries, carry progress for unchanged.
		mergedManifest := &tx.Manifest{
			TransferID:  newManifest.TransferID,
			Root:        newManifest.Root,
			Mode:        newManifest.Mode,
			LinkMbps:    newManifest.LinkMbps,
			Concurrency: newManifest.Concurrency,
			Entries:     newManifest.Entries,
		}
		for _, uf := range unchangedFiles {
			for i := range mergedManifest.Entries {
				if mergedManifest.Entries[i].ID == uf.ID {
					mergedManifest.Entries[i].Progress = uf.Progress
					break
				}
			}
		}

		// Update server manifest with the latest server state for next round / future commands.
		if err := tx.SaveManifest(ps.ServerManifestPath, mergedManifest); err != nil {
			fmt.Fprintf(stderr, "save server manifest failed: %v\n", err)
			return 1
		}
		serverManifest = mergedManifest

		// Always delete removed files to converge.
		if !cfg.skipWrite {
			for _, rmPath := range rmPaths {
				localPath := filepath.Join(outRoot, filepath.FromSlash(rmPath))
				if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
					fmt.Fprintf(stderr, "sync: rm %s: %v\n", localPath, err)
				}
			}
			// Truncate local files for stale entries.
			for _, entry := range staleFiles {
				localPath := filepath.Join(outRoot, filepath.FromSlash(entry.Path))
				if err := os.Truncate(localPath, 0); err != nil && !os.IsNotExist(err) {
					fmt.Fprintf(stderr, "sync: truncate %s: %v\n", localPath, err)
				}
			}
		}

		// Build progress state for merged manifest.
		mergedProgress := make(map[uint64]tx.ManifestProgress, len(mergedManifest.Entries))
		for _, e := range mergedManifest.Entries {
			if e.Progress.AckBytes > 0 || e.Progress.MetadataDone {
				mergedProgress[e.ID] = e.Progress
			}
		}
		applyProgressStateToManifest(mergedManifest, mergedProgress)
		printResumeProgress(stderr, "copy-resume", mergedManifest.Entries)

		markCompleted := func(entry tx.ManifestEntry) {
			mergedProgress[entry.ID] = tx.ManifestProgress{
				AckBytes:     entry.Size,
				MetadataDone: true,
			}
		}
		pendingWork, _ := collectPendingManifestWork(mergedManifest.Entries, cfg.skipWrite, markCompleted, nil, nil)

		if !pendingWork.hasAny() {
			if cfg.skipWrite {
				fmt.Fprintln(stderr, "sync: skip-write, no downloads needed")
				return 0
			}
			fmt.Fprintln(stderr, "sync: no downloads needed")
			continue
		}

		effectiveConcurrency := mergedManifest.Concurrency
		if cfg.concurrencyExplicit {
			effectiveConcurrency = cfg.concurrency
		}

		progressUpdates := make(chan tx.DownloadProgressUpdate, 1024)
		entryByID := manifestEntriesByID(mergedManifest)
		var onProgressUpdate func(tx.DownloadProgressUpdate)
		if cfg.verbosity >= 2 {
			progressReporter := newVerboseProgressReporter(stderr)
			onProgressUpdate = progressReporter.ReportUpdate
		}
		forwardProgress := func(update tx.DownloadProgressUpdate) {
			if onProgressUpdate != nil {
				onProgressUpdate(update)
			}
		}
		mergedFingerprint := tx.ManifestFingerprint(mergedManifest)
		stopProgress, persistProgressAck, markMetadataDonePersisted := startProgressWriter(ps.ProgressPath, mergedFingerprint, mergedProgress, progressUpdates, forwardProgress, stderr)
		persistFileDone := func(fileID uint64, ackBytes int64) {
			persistProgressAck(fileID, ackBytes)
		}
		markMetadataDone := func(fileID uint64) {
			markMetadataDonePersisted(fileID)
		}

		totalAllBytes, totalAllFiles, priorBytes, priorFiles := progressTotals(mergedManifest.Entries)
		var totalCopied atomic.Int64
		totalCopied.Store(priorBytes)
		var doneFiles atomic.Uint64
		doneFiles.Store(priorFiles)
		outputWriter := func(entry tx.ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			destPath := resolveDownloadDestinationPath(entry, outRoot, "")
			if cfg.skipWrite {
				destPath = os.DevNull
			}
			w, syncFn, err := openDownloadOutput(entry, offset, destPath, nil, syncWorker)
			if err != nil {
				return nil, nil, err
			}
			return &countingWriter{Writer: w, total: &totalCopied}, syncFn, nil
		}

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
		startResp, err := downloadManifestFiles(manifestDownloadConfig{
			Client:             client,
			Manifest:           mergedManifest,
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
				if cfg.skipWrite {
					destPath = os.DevNull
				}
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
			TransferID:       mergedManifest.TransferID,
			TransferMode:     mergedManifest.Mode,
			ProbeBytes:       cfg.probeBytes,
			ObservedLinkMbps: mergedManifest.LinkMbps,
			StatusTotalBytes: totalAllBytes,
			StatusTotalFiles: totalAllFiles,
			StatusPolling:    false,
		})
		if err != nil {
			stopProgress()
			applyProgressStateToManifest(mergedManifest, mergedProgress)
			fmt.Fprintf(stderr, "sync download failed: %v\n", err)
			return 1
		}
		if len(pendingWork.files) > 0 {
			completed += int64(startResp.Downloaded)
			totalTransferred += startResp.TransferredBytes
			for _, startErr := range startResp.Errors {
				recordFailure(startErr)
			}
		}
		stopProgress()
		applyProgressStateToManifest(mergedManifest, mergedProgress)
		// Apply non-file entries after all file data has been downloaded.
		if !cfg.skipWrite {
			for _, nfErr := range applyNonFileEntries(mergedManifest.Entries, pendingWork.hardlinks, pendingWork.symlinks, pendingWork.dirs, outRoot) {
				recordFailure(nfErr)
			}
		}

		failuresMu.Lock()
		finalFailures := append([]error(nil), failures...)
		failuresMu.Unlock()
		printTransferErrors(stderr, "sync", finalFailures, cfg.verbosity)

		elapsedAll := time.Since(startAll)
		overallSpeed := 0.0
		if elapsedAll > 0 {
			overallSpeed = float64(totalTransferred) / elapsedAll.Seconds()
		}
		fmt.Fprintf(stderr,
			"sync complete[%d]: tid=%s downloaded=%d failed=%d transferred=%s speed=%s elapsed=%s sync-conn-fallbacks=%d\n",
			round,
			mergedManifest.TransferID,
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
		if cfg.skipWrite || len(pendingWork.files) == 0 {
			return 0
		}
	}

	fmt.Fprintf(stderr, "sync: failed to converge after %d rounds\n", maxSyncRounds)
	return 1
}
