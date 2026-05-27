package filexfercli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

type transferArgs struct {
	sourceDir    string
	targetDir    string
	agePublicKey string
	ageIdentity  string
	encMode      string
	authTokens   []string
	loadStrategy string
	probeBytes   int64
	verbosity    int
	deadlineMS   int64
	cacheLoad    bool
}

func runTransferCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := cliflags.New("transfer")
	cf.SetOutput(stderr)
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx recv transfer -s <dir> [flags] <target-dir>")
		cf.PrintDefaults(stderr)
	}
	var sourceDir string
	var encryptMode string
	var loadStrategyRaw string
	var probeSizeRaw string
	var verbose bool
	var deadlineRaw string
	cf.StringVar(&sourceDir, "s", "source-directory", "", "Absolute source directory to transfer")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&loadStrategyRaw, "", "load-strategy", tx.LoadStrategyFast, "Server load strategy (fast|gentle)")
	probeSizeRaw = encoding.HumanBytes(defaultCLIProbeBytes)
	cf.StringVar(&probeSizeRaw, "", "probe-size", probeSizeRaw, "Probe payload size for transfer metadata; 1B, 4KiB, 8MiB")
	cf.BoolVar(&verbose, "v", "verbose", false, "Disable front-coding")
	cf.StringVar(&deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if sourceDir == "" {
		fmt.Fprintln(stderr, "transfer requires --source-directory (or -s)")
		return 2
	}
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "transfer requires exactly one positional argument: <target-dir>")
		return 2
	}
	probeBytes, err := encoding.ParseByteSize(probeSizeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --probe-size: %v\n", err)
		return 2
	}
	if probeBytes <= 0 {
		fmt.Fprintln(stderr, "--probe-size must be > 0")
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(loadStrategyRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --load-strategy: %v\n", err)
		return 2
	}
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
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
	return runTransfer(serverURL, transferArgs{
		sourceDir:    sourceDir,
		targetDir:    cf.Arg(0),
		agePublicKey: agePublicKey,
		ageIdentity:  ageIdentity,
		encMode:      resolvedEncMode,
		loadStrategy: loadStrategy,
		probeBytes:   probeBytes,
		verbosity:    verbosityFromFlags(false, verbose),
		deadlineMS:   deadlineMS,
	}, stdout, stderr)
}

func runTransfer(serverURL string, cfg transferArgs, stdout io.Writer, stderr io.Writer) int {
	ps, err := newTxState(cfg.targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}

	fmt.Fprintf(stderr, "transfer(addr=[%s], source=[%s])\n", serverURL, cfg.sourceDir)

	client := tx.NewClient(serverURL, tx.WithLoadStrategy(cfg.loadStrategy), tx.WithClientAgePublicKey(cfg.agePublicKey), tx.WithClientAgeIdentity(cfg.ageIdentity), tx.WithEncryptMode(cfg.encMode), tx.WithClientAuthTokens(cfg.authTokens...))
	defer client.Close()
	start := time.Now()
	probeResult, err := client.ProbeLink(context.Background(), tx.ProbeRequest{
		ProbeBytes:   cfg.probeBytes,
		LoadStrategy: cfg.loadStrategy,
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe failed: %v\n", err)
		return 1
	}
	cipherDisplay := "none"
	if probeResult.SuggestedCipher != "" {
		cipherDisplay = probeResult.SuggestedCipher
	}
	fmt.Fprintf(
		stderr,
		"txfer-probe   : strategy=%s cipher=%s rtt_ms=%d srv-conc=(%d cpu * %d io = %d) conn-pool=%d\n",
		cfg.loadStrategy,
		cipherDisplay,
		probeResult.AvgLatencyMS,
		probeResult.ServerCPU, probeResult.ServerIODepth, probeResult.SuggestedConcurrency,
		probeResult.WarmConnectionPoolSize,
	)
	fmt.Fprintln(stderr, formatProbeLinkLine(probeResult, cfg.probeBytes))
	if err := ps.ensureStateDir(); err != nil {
		fmt.Fprintf(stderr, "create state directory failed: %v\n", err)
		return 1
	}
	tmpManifestPath := ps.ServerManifestPath + ".tmp"
	tmpFile, err := os.Create(tmpManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "create manifest tmp failed: %v\n", err)
		return 1
	}
	var manifestWireBytes atomic.Int64
	rawSink := &countingWriter{Writer: tmpFile, total: &manifestWireBytes}
	progressCh := make(chan tx.ManifestProgressUpdate, 64)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for upd := range progressCh {
			fmt.Fprintf(stderr,
				"txfer-manifest: [%d][%s → %s] total=[%s → %s]\n",
				upd.FrameIndex,
				encoding.HumanBytes(upd.WireBytes), encoding.HumanBytes(upd.LogicalBytes),
				encoding.HumanBytes(upd.TotalWire), encoding.HumanBytes(upd.TotalLogical),
			)
		}
	}()
	manifestResp, err := client.GetManifest(context.Background(), tx.GetManifestRequest{
		Directory:        cfg.sourceDir,
		Verbose:          cfg.verbosity >= 2,
		Mode:             cfg.loadStrategy,
		LinkMbps:         probeResult.LinkMbps,
		Concurrency:      probeResult.SuggestedConcurrency,
		DeadlineMS:       cfg.deadlineMS,
		CacheMap:         cacheMapValue(cfg.cacheLoad),
		RawSink:          rawSink,
		ManifestProgress: progressCh,
	})
	close(progressCh)
	<-progressDone
	closeErr := tmpFile.Close()
	if err != nil {
		_ = os.Remove(tmpManifestPath)
		fmt.Fprintf(stderr, "transfer failed: %v\n", err)
		return 1
	}
	if closeErr != nil {
		_ = os.Remove(tmpManifestPath)
		fmt.Fprintf(stderr, "close manifest tmp: %v\n", closeErr)
		return 1
	}
	manifest := manifestResp.Manifest

	var total int64
	for _, e := range manifest.Entries {
		total += e.Size
	}
	if manifestWireBytes.Load() > 0 {
		if err := os.Rename(tmpManifestPath, ps.ServerManifestPath); err != nil {
			_ = os.Remove(tmpManifestPath)
			fmt.Fprintf(stderr, "rename manifest: %v\n", err)
			return 1
		}
	} else {
		_ = os.Remove(tmpManifestPath)
		if err := tx.SaveManifest(ps.ServerManifestPath, manifest); err != nil {
			fmt.Fprintf(stderr, "save manifest failed: %v\n", err)
			return 1
		}
	}
	fmt.Fprintf(
		stderr,
		"txfer-loaded  : tid[%s] %d files (%s) from [%s] elapsed=%s\n",
		manifest.TransferID,
		len(manifest.Entries),
		encoding.HumanBytes(total),
		manifest.Root,
		time.Since(start).Round(time.Millisecond),
	)
	fmt.Fprintf(stderr, "txfer-state   : >(%s)\n", ps.ServerManifestPath)

	return 0
}

// stableEntryKey is the identity used to carry progress across a SYNC
// refresh that reassigns file IDs. Two entries with the same key are
// treated as the same file's bytes.
type stableEntryKey struct {
	Path     string
	Type     byte
	Size     int64
	Mtime    int64
	Mode     os.FileMode
	LinkPath string
}

func makeStableEntryKey(e tx.ManifestEntry) stableEntryKey {
	t := e.Type
	if t == 0 {
		t = encoding.EntryTypeFile
	}
	return stableEntryKey{
		Path:     e.Path,
		Type:     t,
		Size:     e.Size,
		Mtime:    e.Mtime,
		Mode:     e.Mode,
		LinkPath: e.LinkPath,
	}
}

// runResumeRefresh refreshes manifest.server via SYNC (sending the prior
// server manifest to the server), remaps any saved progress to the new
// manifest's file IDs by stable identity, drops progress for changed or
// removed files, and persists the new manifest + remapped progress (with
// fingerprint header) atomically. Used by `copy` when a prior .tx/<dst>/
// state exists but LOCAL_DST is not yet in place.
func runResumeRefresh(serverURL string, cfg transferArgs, stderr io.Writer) int {
	ps, err := newTxState(cfg.targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	priorManifest, err := tx.LoadManifest(ps.ServerManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load prior manifest failed: %v\n", err)
		return 1
	}
	priorFingerprint := tx.ManifestFingerprint(priorManifest)

	fmt.Fprintf(stderr, "resume(addr=[%s], source=[%s])\n", serverURL, cfg.sourceDir)

	client := tx.NewClient(serverURL,
		tx.WithLoadStrategy(cfg.loadStrategy),
		tx.WithClientAgePublicKey(cfg.agePublicKey),
		tx.WithClientAgeIdentity(cfg.ageIdentity),
		tx.WithEncryptMode(cfg.encMode),
		tx.WithClientAuthTokens(cfg.authTokens...),
	)
	defer client.Close()

	start := time.Now()
	probeResult, err := client.ProbeLink(context.Background(), tx.ProbeRequest{
		ProbeBytes:   cfg.probeBytes,
		LoadStrategy: cfg.loadStrategy,
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe failed: %v\n", err)
		return 1
	}
	cipherDisplay := "none"
	if probeResult.SuggestedCipher != "" {
		cipherDisplay = probeResult.SuggestedCipher
	}
	fmt.Fprintf(
		stderr,
		"resume-probe  : strategy=%s cipher=%s rtt_ms=%d srv-conc=(%d cpu * %d io = %d) conn-pool=%d\n",
		cfg.loadStrategy,
		cipherDisplay,
		probeResult.AvgLatencyMS,
		probeResult.ServerCPU, probeResult.ServerIODepth, probeResult.SuggestedConcurrency,
		probeResult.WarmConnectionPoolSize,
	)
	fmt.Fprintln(stderr, formatProbeLinkLine(probeResult, cfg.probeBytes))
	batchPlan := tx.ExplainBatchMaxBytes(
		probeResult.SuggestedConcurrency,
		client.WindowConcurrency,
		client.FileRequestWindowBytes,
		probeResult.ServerSendBufBytes,
		effectiveModeLinkMbps(cfg.loadStrategy, probeResult.LinkMbps, probeResult.GentleBWPct),
	)

	syncResp, err := client.SyncManifest(context.Background(), tx.SyncManifestRequest{
		Directory:   cfg.sourceDir,
		OldManifest: priorManifest,
		Mode:        cfg.loadStrategy,
		LinkMbps:    probeResult.LinkMbps,
		Concurrency: probeResult.SuggestedConcurrency,
	})
	if err != nil {
		fmt.Fprintf(stderr, "resume sync failed: %v\n", err)
		return 1
	}
	newManifest := syncResp.Manifest
	if cfg.deadlineMS > 0 {
		newManifest.DeadlineMS = cfg.deadlineMS
	}
	newFingerprint := tx.ManifestFingerprint(newManifest)
	delta := compareManifestEntries(priorManifest, newManifest)
	delta.removedPaths = syncResp.RemovedPaths

	// Validate the on-disk progress fingerprint against the *prior* manifest:
	// if they don't match, the progress file is stale (came from a different
	// manifest version) and we drop it wholesale.
	progressFingerprint, fpErr := loadProgressFingerprint(ps.ProgressPath)
	if fpErr != nil {
		fmt.Fprintf(stderr, "load progress fingerprint failed: %v\n", fpErr)
		return 1
	}
	priorProgress, err := loadProgressState(ps.ProgressPath)
	if err != nil {
		fmt.Fprintf(stderr, "load progress failed: %v\n", err)
		return 1
	}
	dropped := 0
	if len(priorProgress) > 0 && progressFingerprint != priorFingerprint {
		if progressFingerprint == "" {
			fmt.Fprintf(stderr, "copy-resume: progress file has no manifest fingerprint; discarding %d entries\n", len(priorProgress))
		} else {
			fmt.Fprintf(stderr, "copy-resume: progress fingerprint mismatch (prior-manifest=%s progress=%s); discarding %d entries\n", priorFingerprint, progressFingerprint, len(priorProgress))
		}
		dropped = len(priorProgress)
		priorProgress = make(map[uint64]tx.ManifestProgress)
	}

	// Build oldID -> stableEntryKey from the prior manifest, and
	// path -> entry for the new manifest, then remap by stable identity.
	oldKeyByID := make(map[uint64]stableEntryKey, len(priorManifest.Entries))
	for _, e := range priorManifest.Entries {
		oldKeyByID[e.ID] = makeStableEntryKey(e)
	}
	newByPath := make(map[string]tx.ManifestEntry, len(newManifest.Entries))
	for _, e := range newManifest.Entries {
		newByPath[e.Path] = e
	}
	remappedProgress := make(map[uint64]tx.ManifestProgress, len(priorProgress))
	carriedFiles := 0
	carriedBytes := int64(0)
	for oldID, prog := range priorProgress {
		oldKey, ok := oldKeyByID[oldID]
		if !ok {
			continue
		}
		newEntry, ok := newByPath[oldKey.Path]
		if !ok {
			// Server no longer has this path — handled by RemovedPaths cleanup below.
			continue
		}
		if makeStableEntryKey(newEntry) != oldKey {
			continue
		}
		ack := prog.AckBytes
		if ack > newEntry.Size {
			ack = newEntry.Size
		}
		remappedProgress[newEntry.ID] = tx.ManifestProgress{
			AckBytes:     ack,
			MetadataDone: prog.MetadataDone,
		}
		carriedFiles++
		carriedBytes += ack
	}

	// Drop any partial staging bytes for paths the server says are gone.
	removedPaths := syncResp.RemovedPaths
	for _, rmPath := range removedPaths {
		stagingPath := filepath.Join(ps.StagingDir, filepath.FromSlash(rmPath))
		if err := os.Remove(stagingPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "copy-resume: rm staging %s: %v\n", stagingPath, err)
		}
	}

	if err := ps.ensureStateDir(); err != nil {
		fmt.Fprintf(stderr, "create state directory failed: %v\n", err)
		return 1
	}
	if err := tx.SaveManifest(ps.ServerManifestPath, newManifest); err != nil {
		fmt.Fprintf(stderr, "save manifest failed: %v\n", err)
		return 1
	}
	if err := saveProgressState(ps.ProgressPath, newFingerprint, remappedProgress); err != nil {
		fmt.Fprintf(stderr, "save progress failed: %v\n", err)
		return 1
	}

	var totalBytes int64
	for _, e := range newManifest.Entries {
		totalBytes += e.Size
	}
	fmt.Fprintf(
		stderr,
		"resume-loaded : tid[%s] %d files (%s) from [%s] elapsed=%s\n",
		newManifest.TransferID,
		len(newManifest.Entries),
		encoding.HumanBytes(totalBytes),
		newManifest.Root,
		time.Since(start).Round(time.Millisecond),
	)
	fmt.Fprintf(
		stderr,
		"copy-resume-delta: new[%s (%s)] stale[%s (%s)] same[%s] rm[%s] link=%s srv-conc=(%d cpu * %d io = %d) batch=%s\n",
		encoding.HumanCount(uint64(len(delta.newFiles)), 6),
		encoding.HumanBytes(delta.newBytes),
		encoding.HumanCount(uint64(len(delta.staleFiles)), 6),
		encoding.HumanBytes(delta.staleBytes),
		encoding.HumanCount(uint64(len(delta.unchangedFiles)), 6),
		encoding.HumanCount(uint64(len(delta.removedPaths)), 6),
		formatProbeLinkSummary(probeResult),
		probeResult.ServerCPU, probeResult.ServerIODepth, probeResult.SuggestedConcurrency,
		encoding.HumanBytes(batchPlan.BatchMaxBytes),
	)
	fmt.Fprintf(
		stderr,
		"copy-resume-state: prior=%s carried=%d (%s) dropped=%d\n",
		ps.ProgressPath, carriedFiles, encoding.HumanBytes(carriedBytes), dropped,
	)
	return 0
}
