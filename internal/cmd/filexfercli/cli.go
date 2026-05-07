package filexfercli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"runtime/trace"

	"sync/atomic"

	"filippo.io/age"
	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/fsync"
	"github.com/jolynch/tx/internal/utils"
)

func startTracing(path string, stderr io.Writer) (stop func()) {
	if path == "" {
		return func() {}
	}
	tf, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(stderr, "trace: failed to create file %s: %v\n", path, err)
		return func() {}
	}
	if err := trace.Start(tf); err != nil {
		fmt.Fprintf(stderr, "trace: failed to start: %v\n", err)
		_ = tf.Close()
		return func() {}
	}
	return func() {
		trace.Stop()
		_ = tf.Close()
	}
}

const defaultVerboseStatusInterval = 10 * time.Second
const defaultCLIAckEveryBytes int64 = 128 * 1024 * 1024
const defaultVerboseProgressInterval = 2 * time.Second
const defaultCLIProbeBytes int64 = 1 * 1024 * 1024
const defaultVerifySampleFrameSize int64 = 4 * 1024 * 1024
const verifySampleBytes int64 = 8
const verifyChecksumCommandBudgetBytes = 3 * 1024 * 1024
const verifyChecksumRequestTimeout = 30 * time.Second
const defaultTransferProbeRefreshInterval = 10 * time.Second
const defaultFsyncTimeout = 60 * time.Second
const defaultSyncfsTimeout = 10 * time.Second
const fixedWidthProgressBytesWidth = 10
const fixedWidthProgressRateWidth = 13
const maxTransferErrorLines = 5

var transferProbeRefreshInterval = defaultTransferProbeRefreshInterval
var verifyBudgetGracePeriod = 10 * time.Second

var syncPromptInput io.Reader = os.Stdin

var syncPromptIsTerminal = func() bool {
	stat, err := os.Stdin.Stat()
	return err == nil && stat != nil && (stat.Mode()&os.ModeCharDevice) != 0
}

// formatProbeLinkSummary is a one-liner summary of the probe link estimate
// used inside existing status lines (sync-delta, start-plan). Fast mode expands
// to "<agg>Mbps (<N>x<per-conn>)"; gentle/single collapses to "<link>Mbps".
func formatProbeLinkSummary(probe tx.ProbeResponse) string {
	poolSuffix := ""
	if probe.WarmConnectionPoolSize > 0 {
		poolSuffix = fmt.Sprintf(" pool=%d", probe.WarmConnectionPoolSize)
	}
	if probe.ParallelConns > 1 {
		return fmt.Sprintf("%dMbps (%dx%dMbps)%s", probe.AggregateMbps, probe.ParallelConns, probe.PerConnMbps, poolSuffix)
	}
	return fmt.Sprintf("%dMbps%s", probe.LinkMbps, poolSuffix)
}

// formatProbeLinkLine renders the throughput phase of a probe. In fast mode a
// fanout across N connections is shown with per-conn median + aggregate; in
// gentle/single-conn modes we print just the averaged per-conn link.
func formatProbeLinkLine(probe tx.ProbeResponse, probeBytes int64) string {
	linkMiBPerSec := probe.LinkMbps * 1_000_000 / 8 / (1 << 20)
	if probe.ParallelConns > 1 {
		return fmt.Sprintf(
			"txfer-link    : %d×%s parallel -> agg=%dMbps (%d MiB/s) per-conn-median=%dMbps pool=%d",
			probe.ParallelConns,
			encoding.HumanBytes(probeBytes),
			probe.AggregateMbps,
			linkMiBPerSec,
			probe.PerConnMbps,
			probe.WarmConnectionPoolSize,
		)
	}
	return fmt.Sprintf(
		"txfer-link    : 1x%s linear -> link=%dMbps (%d MiB/s) pool=%d",
		encoding.HumanBytes(probeBytes),
		probe.LinkMbps,
		linkMiBPerSec,
		probe.WarmConnectionPoolSize,
	)
}

type synchronizedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (sw *synchronizedWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

type countingWriter struct {
	io.Writer
	total *atomic.Int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.Writer.Write(p)
	if n > 0 {
		cw.total.Add(int64(n))
	}
	return n, err
}

func (cw *countingWriter) Close() error {
	if c, ok := cw.Writer.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type probeReporter struct {
	stop           func()
	limiterBps     atomic.Int64 // server's current rate limiter (bytes/sec)
	linkMbps       atomic.Int64 // latest probe-derived link bandwidth (megabits/sec)
	lastProbeUnixS atomic.Int64 // unix seconds of most recent successful probe
}

func startTransferProbeReporter(ctx context.Context, client *tx.Client, transferID string, strategy string, probeBytes int64, initialObservedLinkMbps int64) *probeReporter {
	pr := &probeReporter{stop: func() {}}
	if client == nil || strings.TrimSpace(transferID) == "" {
		return pr
	}
	if probeBytes <= 0 {
		probeBytes = defaultCLIProbeBytes
	}
	if strings.TrimSpace(strategy) == "" {
		strategy = tx.LoadStrategyFast
	}
	probeCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var stopOnce sync.Once
	pr.stop = func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}
	var observed atomic.Int64
	if initialObservedLinkMbps > 0 {
		observed.Store(initialObservedLinkMbps)
		pr.linkMbps.Store(initialObservedLinkMbps)
		pr.lastProbeUnixS.Store(time.Now().Unix())
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(transferProbeRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-probeCtx.Done():
				return
			case <-ticker.C:
				resp, err := client.ProbeLink(probeCtx, tx.ProbeRequest{
					ProbeBytes:       probeBytes,
					Parallelism:      1,
					LoadStrategy:     strategy,
					TransferID:       transferID,
					ObservedLinkMbps: observed.Load(),
				})
				if err != nil {
					continue
				}
				if resp.LinkMbps > 0 {
					observed.Store(resp.LinkMbps)
					pr.linkMbps.Store(resp.LinkMbps)
				}
				if resp.ServerLimiterBps > 0 {
					pr.limiterBps.Store(resp.ServerLimiterBps)
				} else {
					pr.limiterBps.Store(0)
				}
				pr.lastProbeUnixS.Store(time.Now().Unix())
			}
		}
	}()
	return pr
}

const defaultFileListener = "127.0.0.1:3453"
const maxSyncRounds = 3

// pinchState computes all state file paths from a target output directory.
// Given targetDir="/var/lib/pinch/dst", state lives in a per-target subdir
// of the parent's .tx directory so sibling transfers don't collide:
//
//	/var/lib/pinch/.tx/dst/manifest         ← client state: what's on disk (written by start/sync)
//	/var/lib/pinch/.tx/dst/manifest.server  ← server state: written by transfer, read by start/get
//	/var/lib/pinch/.tx/dst/manifest.progress
//	/var/lib/pinch/.tx/dst/remote/          (staging for start)
type pinchState struct {
	TargetDir          string // the user-facing output directory
	StateDir           string // parent/.tx/<basename>
	ManifestPath       string // StateDir/manifest        (client state: what's on disk)
	ServerManifestPath string // StateDir/manifest.server (server state: from transfer)
	ProgressPath       string // StateDir/manifest.progress
	StagingDir         string // StateDir/remote
}

func newPinchState(targetDir string) (*pinchState, error) {
	targetDir = filepath.Clean(targetDir)
	parent := filepath.Dir(targetDir)
	if parent == targetDir {
		return nil, fmt.Errorf("target directory %q has no distinct parent", targetDir)
	}
	stateDir := filepath.Join(parent, ".tx", filepath.Base(targetDir))
	return &pinchState{
		TargetDir:          targetDir,
		StateDir:           stateDir,
		ManifestPath:       filepath.Join(stateDir, "manifest"),
		ServerManifestPath: filepath.Join(stateDir, "manifest.server"),
		ProgressPath:       filepath.Join(stateDir, "manifest.progress"),
		StagingDir:         filepath.Join(stateDir, "remote"),
	}, nil
}

func (ps *pinchState) ensureStateDir() error   { return os.MkdirAll(ps.StateDir, 0o755) }
func (ps *pinchState) ensureStagingDir() error { return os.MkdirAll(ps.StagingDir, 0o755) }

// scanLocalDir walks targetDir and returns a tx.Manifest representing the entries
// currently on disk, using meta for header fields (Root, Mode, etc.).
// If targetDir does not exist the returned manifest has no entries.
func scanLocalDir(targetDir string, meta *tx.Manifest) (*tx.Manifest, error) {
	out := &tx.Manifest{
		TransferID:  meta.TransferID,
		Root:        meta.Root,
		Mode:        meta.Mode,
		LinkMbps:    meta.LinkMbps,
		Concurrency: meta.Concurrency,
	}
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		return out, nil
	}
	err := encoding.WalkManifestEntries(targetDir, func(result encoding.WalkResult) error {
		entry := result.Entry
		out.Entries = append(out.Entries, tx.ManifestEntry{
			Type:       entry.Type,
			ID:         entry.ID,
			Size:       entry.Size,
			Mtime:      entry.Mtime,
			Mode:       entry.Mode,
			Path:       entry.Path,
			LinkTarget: entry.LinkTarget,
			LinkPath:   entry.LinkPath,
		})
		return nil
	})
	out.Size() // warm cache
	return out, err
}

func isKnownCommand(s string) bool {
	switch s {
	case "copy", "transfer", "start", "status", "get", "sync":
		return true
	}
	return false
}

func RunCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) < 1 {
		printCLIUsage(stderr)
		return 2
	}

	// Handle top-level help.
	if args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		printCLIUsage(stderr)
		return 0
	}

	// If first arg is a known command, default the server address.
	serverURL := args[0]
	cmdStart := 1
	if isKnownCommand(args[0]) {
		serverURL = defaultFileListener
		cmdStart = 0
	} else {
		if err := utils.ValidateHostPort(serverURL); err != nil {
			fmt.Fprintf(stderr, "invalid server-url: %v\n", err)
			printCLIUsage(stderr)
			return 2
		}
	}

	if cmdStart >= len(args) {
		printCLIUsage(stderr)
		return 2
	}

	cmd := args[cmdStart]
	cmdArgs := args[cmdStart+1:]

	switch cmd {
	case "copy":
		return runCopyCLI(serverURL, cmdArgs, stdout, stderr)
	case "status":
		return runStatusCLI(serverURL, cmdArgs, stdout, stderr)
	case "get":
		return runGetCLI(serverURL, cmdArgs, stdout, stderr)
	case "--help", "-h", "help":
		printCLIUsage(stderr)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", cmd)
		printCLIUsage(stderr)
		return 2
	}
}

func printCLIUsage(w io.Writer) {
	fmt.Fprintf(w, `usage: tx recv [<addr>] <command> [options]

Commands:
  copy       Copy REMOTE_SRC to LOCAL_DST
  status     Query and monitor transfer progress
  get        Download a single remote file

State is stored in <local-dst>/../.tx/ (manifest, progress, staging).
Default server address: %s
Run 'tx recv <command> --help' for command-specific options.
`, defaultFileListener)
}

func resolveEncryptionOptions(mode string) (pubKey string, identity string, encMode string, err error) {
	return resolveEncryptionOptionsWithKeys(mode, "")
}

// resolveEncryptionOptionsWithKeys resolves the client age identity. If
// keysDir is empty, an ephemeral identity is generated per call (legacy
// behavior). If keysDir is set, a persistent identity is loaded from
// keysDir/key (generated and persisted if absent), so pubkey-based server
// allowlists can match a stable client identity.
func resolveEncryptionOptionsWithKeys(mode, keysDir string) (pubKey string, identity string, encMode string, err error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", "none":
		return "", "", "", nil
	case "auto", "aes", "chacha20":
	default:
		return "", "", "", fmt.Errorf("unsupported --encrypt value %q (supported: none, auto, aes, chacha20)", mode)
	}
	if keysDir == "" {
		id, genErr := age.GenerateX25519Identity()
		if genErr != nil {
			return "", "", "", fmt.Errorf("generate age identity: %w", genErr)
		}
		return id.Recipient().String(), id.String(), mode, nil
	}
	id, _, err := aead.LoadOrGenerateAgeIdentity(keysDir, false)
	if err != nil {
		return "", "", "", fmt.Errorf("load keys from %s: %w", keysDir, err)
	}
	return id.Recipient().String(), id.String(), mode, nil
}

// validateAuthTokens enforces: every token is a valid opaque string, and the
// combination requires encryption.
func validateAuthTokens(tokens []string, encMode string) error {
	if len(tokens) == 0 {
		return nil
	}
	if encMode == "" || encMode == "none" {
		return fmt.Errorf("--auth-token requires --encrypt (auth tokens only travel inside the encrypted AUTH blob)")
	}
	for _, t := range tokens {
		if vErr := aead.ValidateAuthToken(t); vErr != nil {
			return fmt.Errorf("--auth-token %q: %w", t, vErr)
		}
	}
	return nil
}

func resolveLoadStrategy(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", tx.LoadStrategyFast:
		return tx.LoadStrategyFast, nil
	case tx.LoadStrategyGentle:
		return tx.LoadStrategyGentle, nil
	default:
		return "", fmt.Errorf("unsupported --load-strategy value %q (supported: fast, gentle)", raw)
	}
}

func resolveCompress(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "adapt", "none", "lz4", "zstd":
		return strings.ToLower(strings.TrimSpace(raw)), nil
	default:
		return "", fmt.Errorf("unsupported --compress value %q (supported: adapt, none, lz4, zstd)", raw)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func verbosityFromFlags(progress bool, verbose bool) int {
	if verbose {
		return 2
	}
	if progress {
		return 1
	}
	return 0
}

func printTransferErrors(stderr io.Writer, phase string, errs []error, verbosity int) {
	if stderr == nil || len(errs) == 0 {
		return
	}
	printed := len(errs)
	if verbosity < 2 && printed > maxTransferErrorLines {
		printed = maxTransferErrorLines
	}
	for _, err := range errs[:printed] {
		fmt.Fprintf(stderr, "%s error: %v\n", phase, err)
	}
	if verbosity < 2 && len(errs) > printed {
		fmt.Fprintf(stderr, "%s failed with %d errors\n", phase, len(errs))
	}
}

type manifestDelta struct {
	newFiles       []tx.ManifestEntry
	staleFiles     []tx.ManifestEntry
	unchangedFiles []tx.ManifestEntry
	removedPaths   []string
	newBytes       int64
	staleBytes     int64
}

func compareManifestEntries(localManifest *tx.Manifest, serverManifest *tx.Manifest) manifestDelta {
	delta := manifestDelta{}
	localByPath := make(map[string]tx.ManifestEntry, len(localManifest.Entries))
	for _, entry := range localManifest.Entries {
		localByPath[entry.Path] = entry
	}
	serverByPath := make(map[string]tx.ManifestEntry, len(serverManifest.Entries))
	for _, entry := range serverManifest.Entries {
		serverByPath[entry.Path] = entry
		local, ok := localByPath[entry.Path]
		if !ok {
			delta.newFiles = append(delta.newFiles, entry)
			delta.newBytes += entry.Size
			continue
		}
		if manifestEntryMatches(local, entry) {
			delta.unchangedFiles = append(delta.unchangedFiles, entry)
			continue
		}
		delta.staleFiles = append(delta.staleFiles, entry)
		delta.staleBytes += entry.Size
	}
	for _, entry := range localManifest.Entries {
		if _, ok := serverByPath[entry.Path]; !ok {
			delta.removedPaths = append(delta.removedPaths, entry.Path)
		}
	}
	sort.Strings(delta.removedPaths)
	return delta
}

func manifestEntryMatches(local tx.ManifestEntry, remote tx.ManifestEntry) bool {
	localType := local.Type
	if localType == 0 {
		localType = encoding.EntryTypeFile
	}
	remoteType := remote.Type
	if remoteType == 0 {
		remoteType = encoding.EntryTypeFile
	}
	if localType != remoteType {
		return false
	}

	switch remoteType {
	case encoding.EntryTypeHard:
		return local.Mode == remote.Mode && local.LinkTarget == remote.LinkTarget
	case encoding.EntryTypeSymlink:
		return local.Mode == remote.Mode && local.LinkPath == remote.LinkPath
	default:
		return local.Size == remote.Size && local.Mtime == remote.Mtime && local.Mode == remote.Mode
	}
}

type noOpWriteCloser struct {
	io.Writer
}

func (n noOpWriteCloser) Close() error {
	return nil
}

func isDiscardDestination(destPath string) bool {
	if destPath == "-" {
		return true
	}
	return filepath.Clean(destPath) == filepath.Clean(os.DevNull)
}

type pendingManifestWork struct {
	files     []tx.ManifestEntry
	hardlinks []tx.ManifestEntry
	symlinks  []tx.ManifestEntry
	dirs      []tx.ManifestEntry
}

func (w pendingManifestWork) hasAny() bool {
	return len(w.files) > 0 || len(w.hardlinks) > 0 || len(w.symlinks) > 0 || len(w.dirs) > 0
}

type manifestDownloadConfig struct {
	Client             *tx.Client
	Manifest           *tx.Manifest
	Entries            []tx.ManifestEntry
	Concurrency        int
	BatchMaxBytes      int64
	SplitWindowWorkers int
	ProgressUpdates    chan<- tx.DownloadProgressUpdate
	OutputWriter       func(tx.ManifestEntry, int64) (io.WriteCloser, func() error, error)
	OnFileDone         func(tx.StartFileDoneEvent)
	TotalCopied        *atomic.Int64
	DoneFiles          *atomic.Uint64
	ProgressTargets    []filexfer.ProgressTarget
	ProgressInterval   time.Duration
	Stderr             io.Writer
	Verbosity          int
	TransferID         string
	TransferMode       string
	ProbeBytes         int64
	ObservedLinkMbps   int64
	StatusTotalBytes   int64
	StatusTotalFiles   uint64
	StatusPolling      bool
}

func totalEntrySize(entries []tx.ManifestEntry) int64 {
	var total int64
	for _, entry := range entries {
		total += entry.Size
	}
	return total
}

// progressTotals summarizes the current ack state across all file entries:
// total bytes/files in the transfer, and the bytes/files already complete or
// copied from prior runs before this session's downloads start. The returned
// priorBytes/priorFiles drive resume-aware progress reporting so a partial
// download's progress bar starts at the prior percent instead of zero.
func progressTotals(entries []tx.ManifestEntry) (totalBytes int64, totalFiles uint64, priorBytes int64, priorFiles uint64) {
	for _, entry := range entries {
		if entry.Type != 0 && entry.Type != encoding.EntryTypeFile {
			continue
		}
		totalFiles++
		size := entry.Size
		if size < 0 {
			size = 0
		}
		totalBytes += size
		ack := entry.Progress.AckBytes
		if ack < 0 {
			ack = 0
		}
		if ack > size {
			ack = size
		}
		priorBytes += ack
		if ack >= size && size > 0 {
			priorFiles++
		}
	}
	return
}

func remainingProgressBytes(entries []tx.ManifestEntry) int64 {
	var remaining int64
	for _, entry := range entries {
		if entry.Type != 0 && entry.Type != encoding.EntryTypeFile {
			continue
		}
		size := entry.Size
		if size < 0 {
			size = 0
		}
		ack := entry.Progress.AckBytes
		if ack < 0 {
			ack = 0
		}
		if ack > size {
			ack = size
		}
		remaining += size - ack
	}
	return remaining
}

func validateResumeProgressTotals(entries []tx.ManifestEntry, pending pendingManifestWork, totalBytes int64, totalFiles uint64, priorBytes int64, priorFiles uint64) error {
	remainingBytes := remainingProgressBytes(entries)
	if priorBytes+remainingBytes != totalBytes {
		return fmt.Errorf("bytes prior=%s remaining=%s total=%s",
			encoding.HumanBytes(priorBytes),
			encoding.HumanBytes(remainingBytes),
			encoding.HumanBytes(totalBytes))
	}
	if priorFiles+uint64(len(pending.files)) != totalFiles {
		return fmt.Errorf("files prior=%d pending=%d total=%d", priorFiles, len(pending.files), totalFiles)
	}
	return nil
}

func collectPendingManifestWork(
	entries []tx.ManifestEntry,
	noWrite bool,
	markCompleted func(tx.ManifestEntry),
	refreshMetadata func(tx.ManifestEntry) error,
	recordFailure func(error),
) (pendingManifestWork, int64) {
	pendingEntries := make([]tx.ManifestEntry, 0, len(entries))
	var completed int64
	for _, entry := range entries {
		if entry.Type != 0 && entry.Type != encoding.EntryTypeFile {
			if !noWrite {
				pendingEntries = append(pendingEntries, entry)
			}
			continue
		}
		if entry.Progress.AckBytes >= entry.Size {
			if entry.Progress.MetadataDone || noWrite {
				if markCompleted != nil {
					markCompleted(entry)
				}
				completed++
				continue
			}
			if refreshMetadata != nil {
				if err := refreshMetadata(entry); err != nil {
					if recordFailure != nil {
						recordFailure(fmt.Errorf("id=%d metadata refresh failed: %w", entry.ID, err))
					}
					continue
				}
				if markCompleted != nil {
					markCompleted(entry)
				}
				completed++
				continue
			}
		}
		pendingEntries = append(pendingEntries, entry)
	}
	files, hardlinks, symlinks, dirs := separateEntriesByType(pendingEntries)
	// Push older files to the front so we work on the most likely to be retained
	// files.
	sort.Slice(files, func(i, j int) bool { return files[i].Mtime < files[j].Mtime })
	return pendingManifestWork{
		files:     files,
		hardlinks: hardlinks,
		symlinks:  symlinks,
		dirs:      dirs,
	}, completed
}

func downloadManifestFiles(cfg manifestDownloadConfig) (tx.StartFromManifestResponse, error) {
	if len(cfg.Entries) == 0 {
		return tx.StartFromManifestResponse{}, nil
	}

	totalCopied := cfg.TotalCopied
	if totalCopied == nil {
		totalCopied = &atomic.Int64{}
	}
	doneFiles := cfg.DoneFiles
	if doneFiles == nil {
		doneFiles = &atomic.Uint64{}
	}

	transferCtx, cancelTransfer := context.WithCancel(context.Background())
	defer cancelTransfer()
	probeInfo := startTransferProbeReporter(transferCtx, cfg.Client, cfg.TransferID, cfg.TransferMode, cfg.ProbeBytes, cfg.ObservedLinkMbps)
	defer probeInfo.stop()

	if cfg.StatusPolling && cfg.Verbosity >= 1 {
		stopStatusPolling := startVerboseStatusPolling(cfg.TransferID, cfg.Client, totalCopied, cfg.StatusTotalBytes, doneFiles, cfg.StatusTotalFiles, probeInfo, cfg.Stderr)
		defer stopStatusPolling()
	}

	success := false
	if len(cfg.ProgressTargets) > 0 {
		totalBytes := cfg.StatusTotalBytes
		if totalBytes <= 0 {
			totalBytes = totalEntrySize(cfg.Entries)
		}
		totalFiles := cfg.StatusTotalFiles
		if totalFiles == 0 {
			totalFiles = uint64(len(cfg.Entries))
		}
		stopProgressFile := filexfer.StartProgressFileWriter(context.Background(), cfg.ProgressTargets, cfg.ProgressInterval, func() filexfer.ProgressStatus {
			copied := totalCopied.Load()
			if totalBytes > 0 && copied > totalBytes {
				copied = totalBytes
			}
			done := doneFiles.Load()
			if totalFiles > 0 && done > totalFiles {
				done = totalFiles
			}
			return filexfer.ProgressStatus{
				Source:     "client",
				DoneFiles:  done,
				TotalFiles: totalFiles,
				DoneBytes:  copied,
				TotalBytes: totalBytes,
			}
		})
		defer func() { stopProgressFile(success) }()
	}

	onFileDone := cfg.OnFileDone
	startResp, err := cfg.Client.StartFromManifest(transferCtx, tx.StartFromManifestRequest{
		Manifest:           cfg.Manifest,
		Entries:            cfg.Entries,
		OutputWriter:       cfg.OutputWriter,
		Concurrency:        cfg.Concurrency,
		BatchMaxBytes:      cfg.BatchMaxBytes,
		SplitWindowWorkers: cfg.SplitWindowWorkers,
		ProgressUpdates:    cfg.ProgressUpdates,
		OnFileDone: func(evt tx.StartFileDoneEvent) {
			doneFiles.Add(1)
			if onFileDone != nil {
				onFileDone(evt)
			}
		},
	})
	if err != nil {
		return tx.StartFromManifestResponse{}, err
	}
	success = true
	return startResp, nil
}

// separateEntriesByType splits manifest entries into categories for processing.
// File entries are returned for download. Non-file entries (H, S, D) are returned
// separately for post-download processing.
func separateEntriesByType(entries []tx.ManifestEntry) (files, hardlinks, symlinks, dirs []tx.ManifestEntry) {
	for _, e := range entries {
		switch e.Type {
		case encoding.EntryTypeHard:
			hardlinks = append(hardlinks, e)
		case encoding.EntryTypeSymlink:
			symlinks = append(symlinks, e)
		case encoding.EntryTypeDir:
			dirs = append(dirs, e)
		default: // 'F' or 0
			files = append(files, e)
		}
	}
	return
}

// applyNonFileEntries creates hardlinks, symlinks, and applies directory metadata
// after all file data has been downloaded. Returns any errors encountered.
func applyNonFileEntries(allEntries []tx.ManifestEntry, hardlinks, symlinks, dirs []tx.ManifestEntry, outRoot string) []error {
	var errs []error

	// Build ID → entry index for hardlink resolution.
	byID := make(map[uint64]tx.ManifestEntry, len(allEntries))
	for _, e := range allEntries {
		byID[e.ID] = e
	}

	// 1. Hardlinks — target file must already exist on disk.
	for _, le := range hardlinks {
		target, ok := byID[uint64(le.LinkTarget)]
		if !ok {
			errs = append(errs, fmt.Errorf("hardlink %s: target id %d not found", le.Path, le.LinkTarget))
			continue
		}
		srcPath := filepath.Join(outRoot, filepath.FromSlash(target.Path))
		dstPath := filepath.Join(outRoot, filepath.FromSlash(le.Path))
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			errs = append(errs, fmt.Errorf("hardlink %s: mkdir: %w", le.Path, err))
			continue
		}
		os.Remove(dstPath) // remove stale from prior run
		if err := os.Link(srcPath, dstPath); err != nil {
			errs = append(errs, fmt.Errorf("hardlink %s -> %s: %w", le.Path, target.Path, err))
		}
	}

	// 2. Symlinks — create with stored target path.
	for _, se := range symlinks {
		dstPath := filepath.Join(outRoot, filepath.FromSlash(se.Path))
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			errs = append(errs, fmt.Errorf("symlink %s: mkdir: %w", se.Path, err))
			continue
		}
		os.Remove(dstPath) // remove stale from prior run
		if err := os.Symlink(se.LinkPath, dstPath); err != nil {
			errs = append(errs, fmt.Errorf("symlink %s -> %s: %w", se.Path, se.LinkPath, err))
		}
	}

	// 3. Directories — apply permissions and mtime last, since writing files
	// changes directory mtime. Process in reverse depth order (deepest first)
	// so parent mtime isn't overwritten by child dir metadata application.
	sort.Slice(dirs, func(i, j int) bool {
		return len(dirs[i].Path) > len(dirs[j].Path)
	})
	for _, de := range dirs {
		dstPath := filepath.Join(outRoot, filepath.FromSlash(de.Path))
		if err := os.MkdirAll(dstPath, 0o755); err != nil {
			errs = append(errs, fmt.Errorf("dir %s: mkdir: %w", de.Path, err))
			continue
		}
		os.Chmod(dstPath, de.Mode.Perm())
		if de.Mtime > 0 {
			mt := time.Unix(0, de.Mtime)
			os.Chtimes(dstPath, mt, mt)
		}
	}

	return errs
}

func resolveDownloadDestinationPath(entry tx.ManifestEntry, outRoot string, outFile string) string {
	outFile = strings.TrimSpace(outFile)
	if outFile != "" {
		return outFile
	}
	if outRoot == "" {
		outRoot = "."
	}
	if filepath.Clean(outRoot) == filepath.Clean(os.DevNull) {
		return os.DevNull
	}
	return filepath.Clean(filepath.Join(outRoot, filepath.FromSlash(entry.Path)))
}

func openDownloadOutput(entry tx.ManifestEntry, offset int64, destPath string, stdout io.Writer, syncWorker *fsync.SyncWorker) (io.WriteCloser, func() error, error) {
	if destPath == "-" {
		if offset > 0 {
			return nil, nil, errors.New("cannot resume when output is stdout")
		}
		if stdout == nil {
			stdout = os.Stdout
		}
		return noOpWriteCloser{Writer: stdout}, func() error { return nil }, nil
	}
	if filepath.Clean(destPath) == filepath.Clean(os.DevNull) {
		return noOpWriteCloser{Writer: io.Discard}, func() error { return nil }, nil
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create output parent directory: %w", err)
	}
	resumeBase := entry.Progress.AckBytes
	if resumeBase < 0 {
		resumeBase = 0
	}
	var (
		fd  *os.File
		err error
	)
	if resumeBase > 0 {
		fd, err = os.OpenFile(destPath, os.O_RDWR, 0)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil, fmt.Errorf("resume requested at offset %d but output file is missing", resumeBase)
			}
			return nil, nil, fmt.Errorf("open output file for resume: %w", err)
		}
		stat, statErr := fd.Stat()
		if statErr != nil {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("stat output file for resume: %w", statErr)
		}
		if stat.Size() < resumeBase {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("resume requested at offset %d but output file has only %d bytes", resumeBase, stat.Size())
		}
	} else if offset > 0 {
		fd, err = os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open output file for sparse write: %w", err)
		}
	} else {
		fd, err = os.Create(destPath)
		if err != nil {
			return nil, nil, fmt.Errorf("create output file: %w", err)
		}
	}
	if offset > 0 {
		if _, err := fd.Seek(offset, io.SeekStart); err != nil {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("seek output file for resume: %w", err)
		}
	}
	syncOutput := syncWorker.SyncOutput(fd, offset)
	return fd, syncOutput, nil
}

func applyDownloadedTrailerMetadata(destPath string, meta *tx.FileTrailerMetadata) error {
	if meta == nil || isDiscardDestination(destPath) {
		return nil
	}
	if err := applyTrailerMetadataToPath(destPath, meta); err != nil {
		return fmt.Errorf("apply trailer metadata to %s: %w", destPath, err)
	}
	return nil
}

func applyProgressStateToManifest(manifest *tx.Manifest, state map[uint64]tx.ManifestProgress) {
	if manifest == nil || len(manifest.Entries) == 0 || len(state) == 0 {
		return
	}
	for i := range manifest.Entries {
		if progress, ok := state[manifest.Entries[i].ID]; ok {
			manifest.Entries[i].Progress = progress
		}
	}
}

func printResumeProgress(stderr io.Writer, prefix string, entries []tx.ManifestEntry) {
	if stderr == nil {
		return
	}
	type resumeEntry struct {
		entry tx.ManifestEntry
		ack   int64
	}
	resumed := make([]resumeEntry, 0)
	var resumedBytes int64
	var resumedTotal int64
	var skippedFiles int
	var skippedBytes int64
	for _, entry := range entries {
		if entry.Type != 0 && entry.Type != encoding.EntryTypeFile {
			continue
		}
		ack := entry.Progress.AckBytes
		if ack <= 0 || entry.Size <= 0 {
			continue
		}
		if ack >= entry.Size {
			skippedFiles++
			skippedBytes += entry.Size
			continue
		}
		resumed = append(resumed, resumeEntry{entry: entry, ack: ack})
		resumedBytes += ack
		resumedTotal += entry.Size
	}
	if skippedFiles == 0 && len(resumed) == 0 {
		return
	}
	if skippedFiles > 0 {
		fmt.Fprintf(
			stderr,
			"%s: skipping %d file(s), %s already copied\n",
			prefix,
			skippedFiles,
			encoding.HumanBytes(skippedBytes),
		)
	}
	if len(resumed) == 0 {
		return
	}
	fmt.Fprintf(
		stderr,
		"%s: resuming %d file(s), %s/%s already copied\n",
		prefix,
		len(resumed),
		encoding.HumanBytes(resumedBytes),
		encoding.HumanBytes(resumedTotal),
	)
	for _, item := range resumed {
		pct := 100 * float64(item.ack) / float64(item.entry.Size)
		fmt.Fprintf(
			stderr,
			"%s:   id=%d done=%s/%s (%.1f%%)\n",
			prefix,
			item.entry.ID,
			encoding.HumanBytes(item.ack),
			encoding.HumanBytes(item.entry.Size),
			pct,
		)
	}
}

func manifestEntriesByID(manifest *tx.Manifest) map[uint64]tx.ManifestEntry {
	if manifest == nil || len(manifest.Entries) == 0 {
		return nil
	}
	entries := make(map[uint64]tx.ManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		entries[entry.ID] = entry
	}
	return entries
}

const progressFingerprintHeaderPrefix = "# manifest-fingerprint xxh128:"

// loadProgressFingerprint returns the fingerprint recorded as a comment
// header in the progress file, or "" if the file is missing or the
// header is absent. Errors only when the file exists but cannot be read.
func loadProgressFingerprint(progressPath string) (string, error) {
	fd, err := os.Open(progressPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	defer fd.Close()
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, progressFingerprintHeaderPrefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, progressFingerprintHeaderPrefix)), nil
		}
		// Stop at first non-comment, non-blank line.
		if !strings.HasPrefix(line, "#") {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", nil
}

func loadProgressState(progressPath string) (map[uint64]tx.ManifestProgress, error) {
	state := make(map[uint64]tx.ManifestProgress)
	fd, err := os.Open(progressPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return state, nil
		}
		return nil, err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 && len(parts) != 3 {
			return nil, fmt.Errorf("invalid progress line: %q", line)
		}
		fileID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid progress file id %q: %w", parts[0], err)
		}
		ack, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid progress ack %q: %w", parts[1], err)
		}
		metadataDone := false
		if len(parts) == 3 {
			switch parts[2] {
			case "0":
				metadataDone = false
			case "1":
				metadataDone = true
			default:
				return nil, fmt.Errorf("invalid progress metadata flag %q", parts[2])
			}
		}
		prev, ok := state[fileID]
		if !ok || ack > prev.AckBytes || (ack == prev.AckBytes && metadataDone && !prev.MetadataDone) {
			state[fileID] = tx.ManifestProgress{
				AckBytes:     ack,
				MetadataDone: metadataDone,
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return state, nil
}

func saveProgressState(progressPath string, fingerprint string, state map[uint64]tx.ManifestProgress) error {
	if len(state) == 0 {
		if err := os.Remove(progressPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	dir := filepath.Dir(progressPath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmpPath := progressPath + ".tmp"
	fd, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if fingerprint != "" {
		if _, err := fmt.Fprintf(fd, "%s%s\n", progressFingerprintHeaderPrefix, fingerprint); err != nil {
			_ = fd.Close()
			return err
		}
	}
	ids := make([]uint64, 0, len(state))
	for fileID := range state {
		ids = append(ids, fileID)
	}
	slices.Sort(ids)
	for _, fileID := range ids {
		entry := state[fileID]
		metaDone := 0
		if entry.MetadataDone {
			metaDone = 1
		}
		if _, err := fmt.Fprintf(fd, "%d %d %d\n", fileID, entry.AckBytes, metaDone); err != nil {
			_ = fd.Close()
			return err
		}
	}
	if err := fd.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, progressPath)
}

type metadataProgressUpdate struct {
	FileID uint64
}

type persistedProgressUpdate struct {
	FileID   uint64
	AckBytes int64
}

func startProgressWriter(progressPath string, fingerprint string, initial map[uint64]tx.ManifestProgress, updates <-chan tx.DownloadProgressUpdate, onUpdate func(tx.DownloadProgressUpdate), stderr io.Writer) (func(), func(uint64, int64), func(uint64)) {
	state := initial
	if state == nil {
		state = make(map[uint64]tx.ManifestProgress)
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	persistedProgressCh := make(chan persistedProgressUpdate, 1024)
	metadataDoneCh := make(chan metadataProgressUpdate, 1024)

	writeSnapshot := func() error {
		return saveProgressState(progressPath, fingerprint, state)
	}

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		dirty := false
		hasPersistedState := func() bool {
			return len(state) > 0
		}
		flushSnapshot := func(force bool) {
			if !force && !dirty {
				return
			}
			if !hasPersistedState() {
				return
			}
			if err := writeSnapshot(); err != nil {
				fmt.Fprintf(stderr, "progress flush failed: %v\n", err)
				return
			}
			dirty = false
		}
		applyProgress := func(update tx.DownloadProgressUpdate) {
			if onUpdate != nil {
				onUpdate(update)
			}
			prev := state[update.FileID]
			if update.AckBytes > prev.AckBytes {
				prev.AckBytes = update.AckBytes
				state[update.FileID] = prev
				dirty = true
			}
		}
		applyPersistedProgress := func(update persistedProgressUpdate) {
			prev := state[update.FileID]
			if update.AckBytes > prev.AckBytes {
				prev.AckBytes = update.AckBytes
				state[update.FileID] = prev
				dirty = true
			}
		}
		applyMetadataDone := func(update metadataProgressUpdate) {
			prev := state[update.FileID]
			if !prev.MetadataDone {
				prev.MetadataDone = true
				state[update.FileID] = prev
				dirty = true
			}
		}
		drainPending := func() {
			for {
				select {
				case update, ok := <-updates:
					if !ok {
						updates = nil
						continue
					}
					applyProgress(update)
				case update := <-persistedProgressCh:
					applyPersistedProgress(update)
				case update := <-metadataDoneCh:
					applyMetadataDone(update)
				default:
					return
				}
			}
		}
		for {
			select {
			case <-stopCh:
				drainPending()
				flushSnapshot(true)
				return
			case update, ok := <-updates:
				if !ok {
					flushSnapshot(hasPersistedState())
					return
				}
				applyProgress(update)
			case update := <-persistedProgressCh:
				applyPersistedProgress(update)
			case update := <-metadataDoneCh:
				applyMetadataDone(update)
			case <-ticker.C:
				flushSnapshot(false)
			}
		}
	}()

	stop := func() {
		close(stopCh)
		<-doneCh
	}
	persistProgressAck := func(fileID uint64, ackBytes int64) {
		update := persistedProgressUpdate{FileID: fileID, AckBytes: ackBytes}
		select {
		case <-doneCh:
			return
		case persistedProgressCh <- update:
		}
	}
	markMetadataDone := func(fileID uint64) {
		update := metadataProgressUpdate{FileID: fileID}
		select {
		case <-doneCh:
			return
		case metadataDoneCh <- update:
		}
	}
	return stop, persistProgressAck, markMetadataDone
}

func refreshCompletedFileMetadata(ctx context.Context, client *tx.Client, manifest *tx.Manifest, fileID uint64, outRoot string, outFile string) error {
	if manifest == nil {
		return errors.New("nil manifest")
	}
	entry, ok := manifest.EntryByID(fileID)
	if !ok {
		return fmt.Errorf("file id %d not in manifest", fileID)
	}
	destPath := outFile
	if destPath == "" {
		destPath = resolveDownloadDestinationPath(entry, outRoot, "")
	}
	if destPath == "-" {
		return nil
	}
	if isDiscardDestination(destPath) {
		return nil
	}
	serverPath := filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path)))
	if !filepath.IsAbs(serverPath) {
		return fmt.Errorf("resolved file path is not absolute: %s", serverPath)
	}
	meta, err := fetchTerminalTrailerMetadataFromChecksum(ctx, client, manifest.TransferID, fileID, serverPath, entry.Size)
	if err != nil {
		return err
	}
	if meta == nil {
		return errors.New("checksum response missing terminal trailer metadata")
	}
	return applyTrailerMetadataToPath(destPath, meta)
}

func fetchTerminalTrailerMetadataFromChecksum(ctx context.Context, client *tx.Client, transferID string, fileID uint64, serverPath string, fileSize int64) (*tx.FileTrailerMetadata, error) {
	resp, err := client.GetChecksum(ctx, tx.GetChecksumRequest{
		TransferID: transferID,
		Targets: []tx.ChecksumTarget{{
			FileID:   fileID,
			FullPath: serverPath,
			Offset:   0,
			Size:     fileSize,
			Algo:     "xxh128",
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("checksum request failed: %w", err)
	}
	defer resp.Reader.Close()
	results, err := readChecksumResults(resp.Reader)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if result.FileID == fileID && result.Metadata != nil {
			return result.Metadata, nil
		}
	}
	return nil, nil
}

type checksumFrameHeader struct {
	FileID   uint64
	Offset   int64
	Size     int64
	WireSize int64
}

type checksumFrameTrailer struct {
	FileID        uint64
	FileHashToken string
	Next          int64
	Metadata      *tx.FileTrailerMetadata
}

type checksumResult struct {
	FileID        uint64
	Offset        int64
	Size          int64
	FileHashToken string
	Metadata      *tx.FileTrailerMetadata
}

func readChecksumResults(reader io.Reader) ([]checksumResult, error) {
	br := bufio.NewReader(reader)
	results := make([]checksumResult, 0, 8)
	for {
		headerLine, err := br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && headerLine == "" {
				return results, nil
			}
			return nil, fmt.Errorf("read checksum frame header: %w", err)
		}
		trimmedHeader := strings.TrimRight(headerLine, "\r\n")
		if trimmedHeader == "" {
			continue
		}
		if isChecksumOKLine(trimmedHeader) {
			return results, nil
		}
		if strings.HasPrefix(trimmedHeader, "ERR ") {
			return nil, errors.New(trimmedHeader)
		}
		header, err := parseChecksumFrameHeader(trimmedHeader)
		if err != nil {
			return nil, err
		}
		if header.WireSize > 0 {
			if _, err := io.CopyN(io.Discard, br, header.WireSize); err != nil {
				return nil, fmt.Errorf("discard checksum frame payload: %w", err)
			}
		}
		trailerLine, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read checksum frame trailer: %w", err)
		}
		trailer, err := parseChecksumFrameTrailer(strings.TrimRight(trailerLine, "\r\n"))
		if err != nil {
			return nil, err
		}
		if trailer.FileID != header.FileID {
			return nil, errors.New("checksum frame trailer file id mismatch")
		}
		results = append(results, checksumResult{
			FileID:        header.FileID,
			Offset:        header.Offset,
			Size:          header.Size,
			FileHashToken: trailer.FileHashToken,
			Metadata:      trailer.Metadata,
		})
	}
}

func isChecksumOKLine(line string) bool {
	line = strings.TrimSpace(line)
	return line == "OK" || strings.HasPrefix(line, "OK ")
}

func parseChecksumFrameHeader(line string) (checksumFrameHeader, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FX/1" {
		return checksumFrameHeader{}, errors.New("invalid checksum frame header")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return checksumFrameHeader{}, errors.New("invalid checksum frame file id")
	}
	props := make(map[string]string, len(fields)-2)
	for _, token := range fields[2:] {
		key, val, ok := strings.Cut(token, "=")
		if ok {
			props[key] = val
		}
	}
	offset, err := strconv.ParseInt(props["offset"], 10, 64)
	if err != nil || offset < 0 {
		return checksumFrameHeader{}, errors.New("invalid checksum frame offset")
	}
	size, err := strconv.ParseInt(props["size"], 10, 64)
	if err != nil || size < 0 {
		return checksumFrameHeader{}, errors.New("invalid checksum frame size")
	}
	wsize, err := strconv.ParseInt(props["wsize"], 10, 64)
	if err != nil || wsize < 0 {
		return checksumFrameHeader{}, errors.New("invalid checksum frame wsize")
	}
	return checksumFrameHeader{
		FileID:   fileID,
		Offset:   offset,
		Size:     size,
		WireSize: wsize,
	}, nil
}

func parseChecksumFrameTrailer(line string) (checksumFrameTrailer, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return checksumFrameTrailer{}, errors.New("invalid checksum frame trailer")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return checksumFrameTrailer{}, errors.New("invalid checksum frame trailer file id")
	}
	next := int64(-1)
	meta := &tx.FileTrailerMetadata{}
	hasMeta := false
	fileHashToken := ""
	for _, token := range fields[2:] {
		if strings.HasPrefix(token, "next=") {
			nextRaw := strings.TrimPrefix(token, "next=")
			next, err = strconv.ParseInt(nextRaw, 10, 64)
			if err != nil || next < 0 {
				return checksumFrameTrailer{}, errors.New("invalid checksum frame trailer next offset")
			}
			continue
		}
		if strings.HasPrefix(token, "file-hash=") {
			fileHashToken = strings.TrimPrefix(token, "file-hash=")
			continue
		}
		if strings.HasPrefix(token, "meta:") {
			parts := strings.SplitN(token, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimPrefix(parts[0], "meta:")
			val := parts[1]
			switch key {
			case "mode":
				meta.Mode = val
				hasMeta = true
			case "uid":
				meta.UID = val
				hasMeta = true
			case "gid":
				meta.GID = val
				hasMeta = true
			case "user":
				meta.User = val
			case "group":
				meta.Group = val
			case "size":
				meta.Size, _ = strconv.ParseInt(val, 10, 64)
			case "mtime_ns":
				meta.MtimeNS, _ = strconv.ParseInt(val, 10, 64)
			}
		}
	}
	if next != 0 {
		return checksumFrameTrailer{}, errors.New("checksum frame trailer next offset must be 0")
	}
	if !hasMeta {
		meta = nil
	}
	return checksumFrameTrailer{
		FileID:        fileID,
		FileHashToken: fileHashToken,
		Next:          next,
		Metadata:      meta,
	}, nil
}

func applyTrailerMetadataToPath(path string, meta *tx.FileTrailerMetadata) error {
	if meta == nil {
		return nil
	}
	fd, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open destination for metadata apply: %w", err)
	}
	defer fd.Close()

	modeRaw := strings.TrimSpace(meta.Mode)
	if modeRaw != "" {
		modeBits, err := strconv.ParseUint(modeRaw, 8, 32)
		if err != nil || modeBits > 0o7777 {
			return fmt.Errorf("invalid trailer mode %q", modeRaw)
		}
		if err := fd.Chmod(os.FileMode(modeBits)); err != nil {
			return fmt.Errorf("chmod destination to %s: %w", modeRaw, err)
		}
	}
	if meta.MtimeNS > 0 {
		mtime := time.Unix(0, meta.MtimeNS)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			return fmt.Errorf("set destination mtime to %d: %w", meta.MtimeNS, err)
		}
	}
	uidRaw := strings.TrimSpace(meta.UID)
	gidRaw := strings.TrimSpace(meta.GID)
	if uidRaw == "" && gidRaw == "" {
		return nil
	}
	if uidRaw == "" || gidRaw == "" {
		return errors.New("trailer uid/gid must both be set")
	}
	uid, err := strconv.Atoi(uidRaw)
	if err != nil {
		return fmt.Errorf("invalid trailer uid %q: %w", uidRaw, err)
	}
	gid, err := strconv.Atoi(gidRaw)
	if err != nil {
		return fmt.Errorf("invalid trailer gid %q: %w", gidRaw, err)
	}
	if err := fd.Chown(uid, gid); err != nil {
		return fmt.Errorf("chown destination uid=%d gid=%d: %w", uid, gid, err)
	}
	return nil
}

type verboseProgressReporter struct {
	mu     sync.Mutex
	stderr io.Writer
	now    func() time.Time
	state  map[uint64]*verboseProgressState
}

type verboseProgressState struct {
	targetBytes     int64
	copiedBytes     int64
	ackedBytes      int64
	nextPct         int64
	startedAt       time.Time
	lastEmitAt      time.Time
	lastEmitBytes   int64
	completeEmitted bool
}

func newVerboseProgressReporter(stderr io.Writer) *verboseProgressReporter {
	return &verboseProgressReporter{
		stderr: stderr,
		now:    time.Now,
		state:  make(map[uint64]*verboseProgressState),
	}
}

func (r *verboseProgressReporter) ReportUpdate(update tx.DownloadProgressUpdate) {
	if r == nil || r.stderr == nil || update.TargetBytes <= 0 {
		return
	}
	now := update.UpdateTime
	if now.IsZero() {
		now = r.now()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.ensureStateLocked(update.FileID, update.TargetBytes, now)
	if st.targetBytes <= 0 {
		return
	}
	copied := clampInt64(update.CopiedBytes, 0, st.targetBytes)
	if copied > st.copiedBytes {
		st.copiedBytes = copied
	}
	acked := clampInt64(update.AckBytes, 0, st.targetBytes)
	if acked > st.ackedBytes {
		st.ackedBytes = acked
	}

	if st.completeEmitted {
		return
	}

	shouldEmit := false
	progressPct := (st.copiedBytes * 100) / st.targetBytes
	for st.nextPct <= 100 && progressPct >= st.nextPct {
		shouldEmit = true
		st.nextPct += 20
	}

	lastActivity := st.lastEmitAt
	if lastActivity.IsZero() {
		lastActivity = st.startedAt
	}
	if !shouldEmit && now.Sub(lastActivity) >= defaultVerboseProgressInterval && st.copiedBytes > st.lastEmitBytes {
		shouldEmit = true
	}
	if st.copiedBytes >= st.targetBytes {
		shouldEmit = true
	}
	if shouldEmit {
		r.emitLocked(update.FileID, st, now)
	}
}

func (r *verboseProgressReporter) ensureStateLocked(fileID uint64, targetBytes int64, now time.Time) *verboseProgressState {
	st := r.state[fileID]
	if st == nil {
		st = &verboseProgressState{
			targetBytes: targetBytes,
			nextPct:     20,
			startedAt:   now,
		}
		r.state[fileID] = st
	}
	if st.startedAt.IsZero() {
		st.startedAt = now
	}
	if targetBytes > 0 {
		st.targetBytes = targetBytes
	}
	if st.nextPct <= 0 {
		st.nextPct = 20
	}
	return st
}

func (r *verboseProgressReporter) emitLocked(fileID uint64, st *verboseProgressState, now time.Time) {
	if st == nil || st.targetBytes <= 0 {
		return
	}

	copied := clampInt64(st.copiedBytes, 0, st.targetBytes)
	acked := clampInt64(st.ackedBytes, 0, st.targetBytes)
	pct := (copied * 100) / st.targetBytes
	if pct > 100 {
		pct = 100
	}

	rateBps := 0.0
	if !st.lastEmitAt.IsZero() && now.After(st.lastEmitAt) && copied > st.lastEmitBytes {
		rateBps = float64(copied-st.lastEmitBytes) / now.Sub(st.lastEmitAt).Seconds()
	}
	if rateBps <= 0 && !st.startedAt.IsZero() && now.After(st.startedAt) && copied > 0 {
		rateBps = float64(copied) / now.Sub(st.startedAt).Seconds()
	}

	eta := "n/a"
	if rateBps > 0 && copied < st.targetBytes {
		remaining := st.targetBytes - copied
		eta = fixedWidthETA(time.Duration(float64(remaining) / rateBps * float64(time.Second)))
	}

	fmt.Fprintf(
		r.stderr,
		"file progress[%d]: %d%% bytes=%s/%s [%s] rate=%s eta=%s\n",
		fileID,
		pct,
		encoding.HumanBytesFixedWidth(copied, fixedWidthProgressBytesWidth),
		encoding.HumanBytesFixedWidth(st.targetBytes, fixedWidthProgressBytesWidth),
		encoding.HumanBytesFixedWidth(acked, fixedWidthProgressBytesWidth),
		encoding.HumanRateFixedWidth(rateBps, fixedWidthProgressRateWidth),
		eta,
	)

	st.lastEmitAt = now
	st.lastEmitBytes = copied
	if copied >= st.targetBytes {
		st.completeEmitted = true
	}
}

func clampInt64(value int64, min int64, max int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func humanETA(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.Round(time.Second).String()
}

func fixedWidthETA(d time.Duration) string {
	return fmt.Sprintf("%5s", compactETA(d))
}

func fixedWidthETANA() string {
	return fmt.Sprintf("%5s", "n/a")
}

func fixedWidthHumanDuration(d time.Duration) string {
	return fmt.Sprintf("%4s", humanETA(d))
}

func compactETA(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	seconds := d.Round(time.Second).Seconds()
	switch {
	case seconds < 60:
		return fmt.Sprintf("%.0fs", seconds)
	case seconds < 60*60:
		return fmt.Sprintf("%.1fm", seconds/60)
	case seconds < 24*60*60:
		return fmt.Sprintf("%.1fh", seconds/3600)
	case seconds < 7*24*60*60:
		return fmt.Sprintf("%.1fd", seconds/(24*3600))
	default:
		return fmt.Sprintf("%.1fw", seconds/(7*24*3600))
	}
}

func effectiveModeLinkMbps(strategy string, linkMbps int64, gentleBWPct int) int64 {
	if !strings.EqualFold(strings.TrimSpace(strategy), tx.LoadStrategyGentle) || linkMbps <= 0 {
		return linkMbps
	}
	gentleBWPct = tx.NormalizeGentleBWPct(gentleBWPct)
	scaled := (linkMbps * int64(gentleBWPct)) / 100
	if (linkMbps*int64(gentleBWPct))%100 != 0 {
		scaled++
	}
	return max(int64(1), scaled)
}

func effectiveProbeLimitBps(probe *probeReporter) (limitBps int64, basis string, hardLimit bool) {
	if probe == nil {
		return 0, "", false
	}
	if limiterBps := probe.limiterBps.Load(); limiterBps > 0 {
		return limiterBps, "limit", true
	}
	if linkMbps := probe.linkMbps.Load(); linkMbps > 0 {
		return (linkMbps * 1_000_000) / 8, "link", false
	}
	return 0, "", false
}

func formatProbeRateSuffix(now time.Time, rateBps float64, probe *probeReporter) string {
	if probe == nil {
		return ""
	}
	limitBps, basis, hardLimit := effectiveProbeLimitBps(probe)
	lastProbeUnixS := probe.lastProbeUnixS.Load()
	if limitBps <= 0 || basis == "" || lastProbeUnixS <= 0 {
		return ""
	}
	pctOfLimit := int(math.Round((rateBps * 100) / float64(limitBps)))
	if pctOfLimit < 0 {
		pctOfLimit = 0
	}
	if !hardLimit && pctOfLimit > 100 {
		pctOfLimit = 100
	}
	age := now.Sub(time.Unix(lastProbeUnixS, 0))
	if age < 0 {
		age = 0
	}
	return fmt.Sprintf(" (%d%% of %s=%s @%s)", pctOfLimit, basis, encoding.HumanRate(float64(limitBps)), fixedWidthHumanDuration(age))
}

func printFileMetrics(stdout io.Writer, txferID string, fileID uint64, path string, meta tx.FileFrameMeta, localFileHash string, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 0.000001
	}
	speed := float64(meta.Size) / seconds
	var ratio float64
	if meta.WireSize > 0 {
		ratio = float64(meta.Size) / float64(meta.WireSize)
	}
	serverFrameMS := meta.TrailerTS - meta.HeaderTS
	serverLogicalBps := 0.0
	serverWireBps := 0.0
	if serverFrameMS > 0 {
		serverSeconds := float64(serverFrameMS) / 1000.0
		serverLogicalBps = float64(meta.Size) / serverSeconds
		serverWireBps = float64(meta.WireSize) / serverSeconds
	}
	serverFileHash := meta.FileHashToken
	if serverFileHash == "" {
		serverFileHash = "n/a"
	}
	if localFileHash == "" {
		localFileHash = "n/a"
	}
	serverFileHashDisplay := encoding.AbbrevHashToken(serverFileHash)
	localFileHashDisplay := encoding.AbbrevHashToken(localFileHash)
	compSummary := formatCompSummary(meta)
	fmt.Fprintf(
		stdout,
		"file: tid=%s fd=%d\n  path: %s\n  transfer: comp=%s logical=%d wire=%d speed=%s ratio=%.3f\n  checksum: server=%s client=%s\n  timing: elapsed=%s ts0=%d ts1=%d server_frame_ms=%d server_logical=%s server_wire=%s\n\n",
		txferID,
		fileID,
		path,
		compSummary,
		meta.Size,
		meta.WireSize,
		encoding.HumanRate(speed),
		ratio,
		serverFileHashDisplay,
		localFileHashDisplay,
		elapsed.Round(time.Millisecond),
		meta.HeaderTS,
		meta.TrailerTS,
		serverFrameMS,
		encoding.HumanRate(serverLogicalBps),
		encoding.HumanRate(serverWireBps),
	)
}

func formatCompSummary(meta tx.FileFrameMeta) string {
	if len(meta.CompCounts) == 0 {
		return meta.Comp
	}
	parts := make([]string, 0, len(meta.CompCounts))
	preferred := []string{"none", "lz4", "zstd"}
	used := make(map[string]bool, len(preferred))
	for _, key := range preferred {
		if count, ok := meta.CompCounts[key]; ok && count > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", key, count))
			used[key] = true
		}
	}
	other := make([]string, 0, len(meta.CompCounts))
	for key, count := range meta.CompCounts {
		if count <= 0 || used[key] {
			continue
		}
		other = append(other, fmt.Sprintf("%s=%d", key, count))
	}
	sort.Strings(other)
	parts = append(parts, other...)
	return "[" + strings.Join(parts, ", ") + "]"
}
