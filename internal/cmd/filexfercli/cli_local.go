package filexfercli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/fsync"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/jolynch/tx/internal/sampler"
)

const localCopyBufferBytes = 1 << 20

// localEntry pairs a walked manifest entry with its absolute source path.
type localEntry struct {
	entry   tx.ManifestEntry
	srcPath string
}

// enumerateLocalSource walks root in deterministic walk order (parents before
// children, hardlink primaries before their links) and returns the entries plus
// the total regular-file byte count and regular-file count for progress.
func enumerateLocalSource(root string) (entries []localEntry, totalBytes int64, totalFiles int64, err error) {
	walkErr := encoding.WalkManifestEntries(root, func(r encoding.WalkResult) error {
		// WalkResult.Entry is an encoding.ManifestEntry; convert to the public
		// tx.ManifestEntry (same field set) as scanLocalDir does.
		entries = append(entries, localEntry{
			entry: tx.ManifestEntry{
				Type:       r.Entry.Type,
				ID:         r.Entry.ID,
				Size:       r.Entry.Size,
				Mtime:      r.Entry.Mtime,
				Mode:       r.Entry.Mode,
				Path:       r.Entry.Path,
				LinkTarget: r.Entry.LinkTarget,
				LinkPath:   r.Entry.LinkPath,
			},
			srcPath: r.FullPath,
		})
		if isRegularFileEntry(r.Entry.Type) {
			totalBytes += r.Entry.Size
			totalFiles++
		}
		return nil
	})
	return entries, totalBytes, totalFiles, walkErr
}

func isRegularFileEntry(t byte) bool {
	return t == 0 || t == encoding.EntryTypeFile
}

func localDestPath(base, relPath string) string {
	return filepath.Join(base, filepath.FromSlash(relPath))
}

// runLocalCopyCLI copies a local directory tree to dstRoot using copy_file_range,
// reusing the copy command's parsed config. It bypasses FTCP entirely: there is
// no network, no probe, and only fast mode is supported.
func runLocalCopyCLI(srcRoot, dstRoot string, cfg copyCLIConfig, stdout, stderr io.Writer) int {
	stopTracing := startTracing(cfg.traceFile, stderr)
	defer stopTracing()

	// Normalize the destination: a trailing slash would make filepath.Dir below
	// return dstRoot itself, placing the staging dir *inside* the destination and
	// colliding on the final rename (and re-creating a just---clean'd directory).
	dstRoot = filepath.Clean(dstRoot)

	loadStrategy, err := resolveLoadStrategy(cfg.modeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --mode: %v\n", err)
		return 2
	}
	if loadStrategy == tx.LoadStrategyGentle {
		fmt.Fprintln(stderr, "gentle mode is not supported for local copies (use --mode fast)")
		return 2
	}
	if cfg.skipFetch {
		fmt.Fprintln(stderr, "--skip-fetch is not supported for local copies")
		return 2
	}
	warnIgnoredLocalFlags(cfg.encryptMode, cfg.authTokens, stderr)

	progressInterval, err := time.ParseDuration(cfg.progressIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-interval: %v\n", err)
		return 2
	}
	progressTargets, err := resolveLocalProgressTargets(cfg.progressFilePaths, cfg.progressFormats, stderr)
	if err != nil {
		return 2
	}

	srcInfo, err := os.Lstat(srcRoot)
	if err != nil {
		fmt.Fprintf(stderr, "local source: %v\n", err)
		return 1
	}
	if !srcInfo.IsDir() {
		fmt.Fprintf(stderr, "copy source %s is not a directory; use 'tx recv get' for a single file\n", srcRoot)
		return 2
	}

	dstExists := pathExists(dstRoot)
	if cfg.clean {
		if err := os.RemoveAll(dstRoot); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "remove local destination failed: %v\n", err)
			return 1
		}
		dstExists = false
	}
	concurrency := cfg.concurrency
	if concurrency <= 0 {
		concurrency = tx.LocalSuggestedConcurrency(runtime.NumCPU(), tx.DefaultTargetIODepth)
	}
	preserveOwner := os.Geteuid() == 0

	// An existing destination is brought in line with the source via an in-place
	// delta (the local analogue of the remote SYNC), rather than a fresh staged
	// copy. --clean cleared dstExists above to force a clean rebuild instead.
	if dstExists {
		return runLocalSync(srcRoot, dstRoot, srcInfo, cfg, concurrency, preserveOwner, progressTargets, progressInterval, stdout, stderr)
	}

	entries, totalBytes, totalFiles, err := enumerateLocalSource(srcRoot)
	if err != nil {
		fmt.Fprintf(stderr, "scan local source failed: %v\n", err)
		return 1
	}

	var doneBytes, doneFiles atomic.Int64
	onBytes := func(n int64) { doneBytes.Add(n) }
	onFile := func() { doneFiles.Add(1) }

	// File-target progress runs for the whole operation; the stderr ticker runs
	// only during the copy itself (like the remote transfer phase) so the
	// syncfs/rename phase isn't spammed with stale 100% lines.
	copyDone := false
	stopFileProgress := startLocalFileProgress(progressTargets, progressInterval, &doneBytes, &doneFiles, totalBytes, totalFiles, func() bool { return copyDone })
	defer stopFileProgress()
	stopTicker := func() {}
	if verbosityFromFlags(cfg.progress, cfg.verbose) >= 1 && stderr != nil {
		stopTicker = startLocalProgressTicker(&doneBytes, &doneFiles, totalBytes, totalFiles, stderr)
	}
	defer stopTicker()

	ctx := context.Background()

	// --skip-write: read every file through copy_file_range into /dev/null and
	// never touch the destination.
	if cfg.skipWrite {
		err := localCopyDiscard(ctx, entries, concurrency, onBytes, onFile)
		stopTicker()
		if err != nil {
			fmt.Fprintf(stderr, "local copy (discard) failed: %v\n", err)
			return 1
		}
		copyDone = true
		fmt.Fprintf(stdout, "local-copy: [ok] (discard) files=%d bytes=%s\n", totalFiles, encoding.HumanBytes(doneBytes.Load()))
		return 0
	}

	// Stage into a sibling temp dir (same filesystem so the final rename is
	// atomic and never EXDEV), then rename into place.
	parent := filepath.Dir(dstRoot)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		fmt.Fprintf(stderr, "create destination parent failed: %v\n", err)
		return 1
	}
	staging, err := os.MkdirTemp(parent, ".tx-local-"+filepath.Base(dstRoot)+"-")
	if err != nil {
		fmt.Fprintf(stderr, "create staging directory failed: %v\n", err)
		return 1
	}
	cleanupStaging := func() { _ = os.RemoveAll(staging) }

	cache := newLocalCacheCollector(cfg.cacheLoadEnabled)
	metaFailures := &metadataFailureCollector{}
	start := time.Now()
	if err := localCopyTree(ctx, staging, srcRoot, srcInfo, entries, concurrency, onBytes, onFile, preserveOwner, cache, metaFailures); err != nil {
		cleanupStaging()
		fmt.Fprintf(stderr, "local copy failed: %v\n", err)
		return 1
	}
	stopTicker() // copy finished — stop stderr progress before the syncfs/rename phase
	copyElapsed := time.Since(start)

	// Replicate the source page-cache residency onto the new files (mirror
	// old->new). We are the server, so there is no closing send-back.
	if cfg.cacheLoadEnabled {
		touchLocalCache(cache.snapshotEntries(), cfg.cacheLoadBudget, concurrency, stderr)
	}

	if !cfg.skipFsync {
		syncCtx, cancel := context.WithTimeout(context.Background(), defaultSyncfsTimeout)
		fsync.SyncfsDir(syncCtx, staging, progressInterval, stderr)
		cancel()
	}

	if err := os.Rename(staging, dstRoot); err != nil {
		cleanupStaging()
		fmt.Fprintf(stderr, "rename staging to destination failed: %v\n", err)
		return 1
	}
	copyDone = true

	copied := doneBytes.Load()
	speed := "n/a"
	if secs := copyElapsed.Seconds(); secs > 0 {
		speed = encoding.HumanBytes(int64(float64(copied)/secs)) + "/s"
	}
	fmt.Fprintf(stdout, "local-copy: [ok] files=%d transferred=%s speed=%s elapsed=%s\n",
		totalFiles, encoding.HumanBytes(copied), speed, copyElapsed.Round(time.Millisecond))

	printMetadataMirrorWarnings(stderr, "local-copy", metaFailures.snapshot(), verbosityFromFlags(cfg.progress, cfg.verbose))

	exitCode := 0
	if cfg.verifyMeta {
		if code := verifyLocalCopy(srcRoot, dstRoot, entries, cfg, concurrency, stderr); code != 0 {
			exitCode = code
		}
	}
	return exitCode
}

// runLocalSync brings an existing destination in line with the source via an
// in-place delta — the local analogue of the remote SYNC. Pure additions proceed
// automatically; a destructive delta (overwrites or removals) prompts for
// confirmation unless -y is set or the session is non-interactive.
func runLocalSync(srcRoot, dstRoot string, srcInfo os.FileInfo, cfg copyCLIConfig, concurrency int, preserveOwner bool, progressTargets []filexfer.ProgressTarget, progressInterval time.Duration, stdout, stderr io.Writer) int {
	srcManifest, err := scanLocalDir(srcRoot, &tx.Manifest{})
	if err != nil {
		fmt.Fprintf(stderr, "scan local source failed: %v\n", err)
		return 1
	}
	dstManifest, err := scanLocalDir(dstRoot, &tx.Manifest{})
	if err != nil {
		fmt.Fprintf(stderr, "scan local destination failed: %v\n", err)
		return 1
	}
	delta := compareManifestEntries(dstManifest, srcManifest)

	fmt.Fprintf(stderr,
		"local-sync-delta: new[%s (%s)] stale[%s (%s)] same[%s] rm[%s]\n",
		encoding.HumanCount(uint64(len(delta.newFiles)), 6), encoding.HumanBytes(delta.newBytes),
		encoding.HumanCount(uint64(len(delta.staleFiles)), 6), encoding.HumanBytes(delta.staleBytes),
		encoding.HumanCount(uint64(len(delta.unchangedFiles)), 6),
		encoding.HumanCount(uint64(len(delta.removedPaths)), 6),
	)

	converged := len(delta.newFiles) == 0 && len(delta.staleFiles) == 0 && len(delta.removedPaths) == 0
	if converged {
		fmt.Fprintln(stderr, "local-sync: source and destination converged, nothing to do")
	}
	if cfg.skipWrite {
		if !converged {
			fmt.Fprintln(stderr, "local-sync: skip-write, no changes applied")
		}
		return 0
	}

	cache := newLocalCacheCollector(cfg.cacheLoadEnabled)
	metaFailures := &metadataFailureCollector{}

	// Apply the delta in place (skipped when already converged). --verify and
	// --cache-load below still run on a converged tree, matching the remote copy.
	if !converged {
		// Only a destructive delta (overwrites or removals) needs confirmation;
		// a delta of only new files proceeds automatically.
		if (len(delta.staleFiles) > 0 || len(delta.removedPaths) > 0) && !cfg.yes {
			if !confirmSyncProceed(stderr, len(delta.newFiles), len(delta.staleFiles), len(delta.removedPaths)) {
				return 0
			}
		}

		// Progress totals cover only the bytes/files we will actually write.
		totalBytes := delta.newBytes + delta.staleBytes
		var totalFiles int64
		for _, e := range delta.newFiles {
			if isRegularFileEntry(e.Type) {
				totalFiles++
			}
		}
		for _, e := range delta.staleFiles {
			if isRegularFileEntry(e.Type) {
				totalFiles++
			}
		}

		var doneBytes, doneFiles atomic.Int64
		onBytes := func(n int64) { doneBytes.Add(n) }
		onFile := func() { doneFiles.Add(1) }

		copyDone := false
		stopFileProgress := startLocalFileProgress(progressTargets, progressInterval, &doneBytes, &doneFiles, totalBytes, totalFiles, func() bool { return copyDone })
		stopTicker := func() {}
		if verbosityFromFlags(cfg.progress, cfg.verbose) >= 1 && stderr != nil {
			stopTicker = startLocalProgressTicker(&doneBytes, &doneFiles, totalBytes, totalFiles, stderr)
		}

		start := time.Now()
		if err := localApplyDelta(srcRoot, dstRoot, srcInfo, srcManifest, delta, concurrency, onBytes, onFile, preserveOwner, cache, metaFailures); err != nil {
			stopTicker()
			stopFileProgress()
			fmt.Fprintf(stderr, "local sync failed: %v\n", err)
			return 1
		}
		stopTicker()
		copyElapsed := time.Since(start)
		copyDone = true
		stopFileProgress()

		copied := doneBytes.Load()
		speed := "n/a"
		if secs := copyElapsed.Seconds(); secs > 0 {
			speed = encoding.HumanBytes(int64(float64(copied)/secs)) + "/s"
		}
		fmt.Fprintf(stdout, "local-sync: [ok] new=%d stale=%d rm=%d transferred=%s speed=%s elapsed=%s\n",
			len(delta.newFiles), len(delta.staleFiles), len(delta.removedPaths),
			encoding.HumanBytes(copied), speed, copyElapsed.Round(time.Millisecond))
		printMetadataMirrorWarnings(stderr, "local-sync", metaFailures.snapshot(), verbosityFromFlags(cfg.progress, cfg.verbose))

		if !cfg.skipFsync {
			syncCtx, cancel := context.WithTimeout(context.Background(), defaultSyncfsTimeout)
			fsync.SyncfsDir(syncCtx, dstRoot, progressInterval, stderr)
			cancel()
		}
	}

	// Replicate the source page-cache residency onto the destination for ALL
	// files (changed files were snapshotted pre-copy by the collector; unchanged
	// files are snapshotted here), matching the remote --cache-load behavior.
	if cfg.cacheLoadEnabled {
		touchList := cache.snapshotEntries()
		for _, e := range delta.unchangedFiles {
			if !isRegularFileEntry(e.Type) {
				continue
			}
			ce := &pagecache.CacheEntry{}
			if err := ce.Load(filepath.Join(srcRoot, filepath.FromSlash(e.Path))); err == nil && !ce.Empty() {
				touchList = append(touchList, pagecache.TouchEntry{Path: filepath.Join(dstRoot, filepath.FromSlash(e.Path)), Entry: ce})
			}
		}
		touchLocalCache(touchList, cfg.cacheLoadBudget, concurrency, stderr)
	}

	exitCode := 0
	if cfg.verifyMeta {
		entries := make([]localEntry, 0, len(srcManifest.Entries))
		for _, e := range srcManifest.Entries {
			entries = append(entries, localEntry{entry: e, srcPath: filepath.Join(srcRoot, filepath.FromSlash(e.Path))})
		}
		if code := verifyLocalCopy(srcRoot, dstRoot, entries, cfg, concurrency, stderr); code != 0 {
			exitCode = code
		}
	}
	return exitCode
}

// localApplyDelta applies a source->destination delta in place: it copies new and
// changed files (creating the directories/symlinks/hardlinks they need), removes
// paths no longer present in the source, then restores directory mtimes so
// metadata verification stays consistent.
func localApplyDelta(srcRoot, dstRoot string, srcInfo os.FileInfo, srcManifest *tx.Manifest, delta manifestDelta, concurrency int, onBytes func(int64), onFile func(), preserveOwner bool, cache *localCacheCollector, mf *metadataFailureCollector) error {
	idToPath := make(map[uint64]string, len(srcManifest.Entries))
	for _, e := range srcManifest.Entries {
		idToPath[e.ID] = e.Path
	}
	changed := make([]tx.ManifestEntry, 0, len(delta.newFiles)+len(delta.staleFiles))
	changed = append(changed, delta.newFiles...)
	changed = append(changed, delta.staleFiles...)

	// Phase A: create changed directories, and ensure parent dirs for files.
	for _, e := range changed {
		if e.Type != encoding.EntryTypeDir {
			continue
		}
		dst := localDestPath(dstRoot, e.Path)
		if err := os.MkdirAll(dst, 0o700); err != nil {
			return fmt.Errorf("mkdir %s: %w", e.Path, err)
		}
		if err := os.Chmod(dst, e.Mode); err != nil {
			mf.add("chmod", e.Type, e.Path, err)
		}
	}
	for _, e := range changed {
		if e.Type == encoding.EntryTypeDir {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(localDestPath(dstRoot, e.Path)), 0o755); err != nil {
			return fmt.Errorf("mkdir parent of %s: %w", e.Path, err)
		}
	}

	// Phase B: copy changed regular files + create symlinks, in parallel.
	fileWork := make([]localEntry, 0, len(changed))
	for _, e := range changed {
		if e.Type == encoding.EntryTypeDir || e.Type == encoding.EntryTypeHard {
			continue
		}
		fileWork = append(fileWork, localEntry{entry: e, srcPath: filepath.Join(srcRoot, filepath.FromSlash(e.Path))})
	}
	if err := runLocalCopyWorkers(fileWork, concurrency, func(le localEntry) error {
		dst := localDestPath(dstRoot, le.entry.Path)
		if le.entry.Type == encoding.EntryTypeSymlink {
			return createLocalSymlink(dst, le, preserveOwner, mf)
		}
		if err := copyLocalRegularFile(dst, le, onBytes, preserveOwner, cache, mf); err != nil {
			return err
		}
		if onFile != nil {
			onFile()
		}
		return nil
	}); err != nil {
		return err
	}

	// Phase C: hardlinks (serial — every target now exists, changed or unchanged).
	for _, e := range changed {
		if e.Type != encoding.EntryTypeHard {
			continue
		}
		targetRel, ok := idToPath[uint64(e.LinkTarget)]
		if !ok {
			return fmt.Errorf("hardlink %s: target file id %d not found in source", e.Path, e.LinkTarget)
		}
		dst := localDestPath(dstRoot, e.Path)
		_ = os.Remove(dst)
		if err := os.Link(localDestPath(dstRoot, targetRel), dst); err != nil {
			return fmt.Errorf("hardlink %s: %w", e.Path, err)
		}
	}

	// Phase D: remove paths present in the destination but not the source.
	for _, rel := range delta.removedPaths {
		if err := os.RemoveAll(localDestPath(dstRoot, rel)); err != nil && !os.IsNotExist(err) {
			mf.add("remove", encoding.EntryTypeFile, rel, err)
		}
	}

	// Phase E: re-apply directory mtimes/modes (child add/remove bumped them),
	// including the destination root, so verify-meta stays consistent.
	for _, e := range srcManifest.Entries {
		if e.Type != encoding.EntryTypeDir {
			continue
		}
		dst := localDestPath(dstRoot, e.Path)
		if err := os.Chmod(dst, e.Mode); err != nil {
			mf.add("chmod", e.Type, e.Path, err)
		}
		mt := time.Unix(0, e.Mtime)
		if err := os.Chtimes(dst, mt, mt); err != nil {
			mf.add("chtimes", e.Type, e.Path, err)
		}
		if preserveOwner {
			applyLocalOwner(dst, filepath.Join(srcRoot, filepath.FromSlash(e.Path)), e.Type, e.Path, mf)
		}
	}
	rootMt := srcInfo.ModTime()
	_ = os.Chmod(dstRoot, srcInfo.Mode().Perm())
	_ = os.Chtimes(dstRoot, rootMt, rootMt)
	if preserveOwner {
		applyLocalOwner(dstRoot, srcRoot, encoding.EntryTypeDir, ".", mf)
	}
	return nil
}

// runLocalCopyWorkers runs fn over items across a bounded worker pool, stopping
// dispatch on the first error and returning it.
func runLocalCopyWorkers(items []localEntry, concurrency int, fn func(localEntry) error) error {
	if len(items) == 0 {
		return nil
	}
	if concurrency < 1 {
		concurrency = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		errMu    sync.Mutex
		firstErr error
	)
	setErr := func(e error) {
		if e == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		errMu.Unlock()
	}
	jobs := make(chan localEntry)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for le := range jobs {
				if ctx.Err() != nil {
					continue
				}
				setErr(fn(le))
			}
		}()
	}
	for _, le := range items {
		if ctx.Err() != nil {
			break
		}
		jobs <- le
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// localCopyTree performs the copy in three ordered phases so directories exist
// before their children and hardlink targets exist before their links:
//
//	A) directories          (serial: mkdir + chmod)
//	B) regular files+links  (parallel: copy_file_range / symlink)
//	C) hardlinks            (serial: os.Link to the already-created primary)
//	D) directory mtimes     (serial: re-applied after children were written)
func localCopyTree(ctx context.Context, dstBase, srcRoot string, srcRootInfo os.FileInfo, entries []localEntry, concurrency int, onBytes func(int64), onFile func(), preserveOwner bool, cache *localCacheCollector, mf *metadataFailureCollector) error {
	idToDst := make(map[uint64]string, len(entries))
	var idMu sync.Mutex

	// Phase A: directories.
	for _, le := range entries {
		if le.entry.Type != encoding.EntryTypeDir {
			continue
		}
		dst := localDestPath(dstBase, le.entry.Path)
		if err := os.MkdirAll(dst, 0o700); err != nil {
			return fmt.Errorf("mkdir %s: %w", le.entry.Path, err)
		}
		if err := os.Chmod(dst, le.entry.Mode); err != nil {
			mf.add("chmod", le.entry.Type, le.entry.Path, err)
		}
		idToDst[le.entry.ID] = dst
	}

	// Phase B: regular files and symlinks, in parallel.
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		errMu    sync.Mutex
		firstErr error
	)
	setErr := func(e error) {
		if e == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		errMu.Unlock()
	}
	jobs := make(chan localEntry)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for le := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				dst := localDestPath(dstBase, le.entry.Path)
				var err error
				if le.entry.Type == encoding.EntryTypeSymlink {
					err = createLocalSymlink(dst, le, preserveOwner, mf)
				} else {
					err = copyLocalRegularFile(dst, le, onBytes, preserveOwner, cache, mf)
				}
				if err != nil {
					setErr(err)
					continue
				}
				idMu.Lock()
				idToDst[le.entry.ID] = dst
				idMu.Unlock()
				if onFile != nil && isRegularFileEntry(le.entry.Type) {
					onFile()
				}
			}
		}()
	}
	for _, le := range entries {
		if le.entry.Type == encoding.EntryTypeDir || le.entry.Type == encoding.EntryTypeHard {
			continue
		}
		if workCtx.Err() != nil {
			break
		}
		jobs <- le
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	// Phase C: hardlinks (serial — every primary now exists).
	for _, le := range entries {
		if le.entry.Type != encoding.EntryTypeHard {
			continue
		}
		target, ok := idToDst[uint64(le.entry.LinkTarget)]
		if !ok {
			return fmt.Errorf("hardlink %s: target file id %d was not created", le.entry.Path, le.entry.LinkTarget)
		}
		dst := localDestPath(dstBase, le.entry.Path)
		_ = os.Remove(dst)
		if err := os.Link(target, dst); err != nil {
			return fmt.Errorf("hardlink %s: %w", le.entry.Path, err)
		}
	}

	// Phase D: re-apply directory mtimes/ownership now that child writes (which
	// bump a directory's mtime) are done. verify-meta compares dir mtimes.
	for _, le := range entries {
		if le.entry.Type != encoding.EntryTypeDir {
			continue
		}
		dst := localDestPath(dstBase, le.entry.Path)
		mt := time.Unix(0, le.entry.Mtime)
		if err := os.Chtimes(dst, mt, mt); err != nil {
			mf.add("chtimes", le.entry.Type, le.entry.Path, err)
		}
		if preserveOwner {
			applyLocalOwner(dst, le.srcPath, le.entry.Type, le.entry.Path, mf)
		}
	}
	// Apply the source root's own metadata to the staging root (it becomes the
	// destination root after rename).
	rootMt := srcRootInfo.ModTime()
	_ = os.Chmod(dstBase, srcRootInfo.Mode().Perm())
	_ = os.Chtimes(dstBase, rootMt, rootMt)
	if preserveOwner {
		applyLocalOwner(dstBase, srcRoot, encoding.EntryTypeDir, ".", mf)
	}
	return nil
}

// copyLocalRegularFile copies one regular file with copy_file_range, snapshotting
// the source's page-cache residency first when cache-load is enabled.
func copyLocalRegularFile(dst string, le localEntry, onBytes func(int64), preserveOwner bool, cache *localCacheCollector, mf *metadataFailureCollector) error {
	cache.snapshot(le.srcPath, dst)

	src, err := os.Open(le.srcPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", le.entry.Path, err)
	}
	defer src.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", le.entry.Path, err)
	}
	if le.entry.Size > 0 {
		if _, err := localCopyFile(out, src, le.entry.Size, onBytes); err != nil {
			_ = out.Close()
			return fmt.Errorf("copy %s: %w", le.entry.Path, err)
		}
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", le.entry.Path, err)
	}
	applyLocalFileMetadata(dst, le, preserveOwner, mf)
	return nil
}

func createLocalSymlink(dst string, le localEntry, preserveOwner bool, mf *metadataFailureCollector) error {
	_ = os.Remove(dst)
	if err := os.Symlink(le.entry.LinkPath, dst); err != nil {
		return fmt.Errorf("symlink %s: %w", le.entry.Path, err)
	}
	if preserveOwner {
		applyLocalOwner(dst, le.srcPath, le.entry.Type, le.entry.Path, mf)
	}
	return nil
}

func applyLocalFileMetadata(path string, le localEntry, preserveOwner bool, mf *metadataFailureCollector) {
	if err := os.Chmod(path, le.entry.Mode); err != nil {
		mf.add("chmod", le.entry.Type, le.entry.Path, err)
	}
	mt := time.Unix(0, le.entry.Mtime)
	if err := os.Chtimes(path, mt, mt); err != nil {
		mf.add("chtimes", le.entry.Type, le.entry.Path, err)
	}
	if preserveOwner {
		applyLocalOwner(path, le.srcPath, le.entry.Type, le.entry.Path, mf)
	}
}

// applyLocalOwner lstats the source and best-effort lchowns the destination to
// match. Only meaningful for root; EPERM is collected, never fatal.
func applyLocalOwner(dst, srcPath string, entryType byte, relPath string, mf *metadataFailureCollector) {
	info, err := os.Lstat(srcPath)
	if err != nil {
		mf.add("lstat", entryType, relPath, err)
		return
	}
	uid, gid, ok := localFileOwner(info)
	if !ok {
		return
	}
	if err := os.Lchown(dst, uid, gid); err != nil {
		mf.add("lchown", entryType, relPath, err)
	}
}

// localCopyDiscard reads every regular file through copy_file_range into
// /dev/null without writing a destination (for --skip-write).
func localCopyDiscard(ctx context.Context, entries []localEntry, concurrency int, onBytes func(int64), onFile func()) error {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		errMu    sync.Mutex
		firstErr error
	)
	setErr := func(e error) {
		if e == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		errMu.Unlock()
	}
	jobs := make(chan localEntry)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for le := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				if err := discardLocalFile(le, onBytes); err != nil {
					setErr(err)
					continue
				}
				if onFile != nil {
					onFile()
				}
			}
		}()
	}
	for _, le := range entries {
		if !isRegularFileEntry(le.entry.Type) {
			continue
		}
		if workCtx.Err() != nil {
			break
		}
		jobs <- le
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func discardLocalFile(le localEntry, onBytes func(int64)) error {
	if le.entry.Size == 0 {
		return nil
	}
	src, err := os.Open(le.srcPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", le.entry.Path, err)
	}
	defer src.Close()
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open /dev/null: %w", err)
	}
	defer devnull.Close()
	if _, err := localCopyFile(devnull, src, le.entry.Size, onBytes); err != nil {
		return fmt.Errorf("read %s: %w", le.entry.Path, err)
	}
	return nil
}

// localCopyFileBuffered is the userspace fallback when copy_file_range is not
// supported for a file pair (old kernel, cross-device, pseudo-filesystem). It
// assumes both fds are positioned at offset 0 (true when invoked with nothing
// copied yet).
func localCopyFileBuffered(dst, src *os.File, onBytes func(int64)) (int64, error) {
	buf := make([]byte, localCopyBufferBytes)
	var copied int64
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return copied, werr
			}
			copied += int64(n)
			if onBytes != nil {
				onBytes(int64(n))
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return copied, nil
			}
			return copied, rerr
		}
	}
}

// --- page-cache replication (cache-load) ---

type localCacheCollector struct {
	enabled bool
	mu      sync.Mutex
	entries []pagecache.TouchEntry
}

func newLocalCacheCollector(enabled bool) *localCacheCollector {
	if !enabled || !pagecache.TouchSupported() {
		return nil
	}
	return &localCacheCollector{enabled: true}
}

// snapshot records the source file's resident pages before it is read so the
// same residency can be replayed onto the destination after the copy.
func (c *localCacheCollector) snapshot(srcPath, dstPath string) {
	if c == nil {
		return
	}
	ce := &pagecache.CacheEntry{}
	if err := ce.Load(srcPath); err != nil || ce.Empty() {
		return
	}
	c.mu.Lock()
	c.entries = append(c.entries, pagecache.TouchEntry{Path: dstPath, Entry: ce})
	c.mu.Unlock()
}

func (c *localCacheCollector) snapshotEntries() []pagecache.TouchEntry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries
}

func touchLocalCache(entries []pagecache.TouchEntry, budget time.Duration, workers int, stderr io.Writer) {
	if len(entries) == 0 || !pagecache.TouchSupported() {
		return
	}
	pageBudget := pagecache.SystemPageBudget(pagecache.TouchCacheReserveBytes)
	ctx := context.Background()
	cancel := func() {}
	if budget > 0 {
		ctx, cancel = context.WithTimeout(ctx, budget)
	}
	defer cancel()
	start := time.Now()
	summary, err := pagecache.TouchEntries(ctx, func(yield func(pagecache.TouchEntry) bool) {
		for _, te := range entries {
			if !yield(te) {
				return
			}
		}
	}, pageBudget, workers)
	status := "[ok]"
	if errors.Is(err, context.DeadlineExceeded) {
		status = "[partial-ok]"
	}
	errPart := ""
	if summary.OpenErrors+summary.AdviseErrors+summary.ReadErrors > 0 {
		errPart = fmt.Sprintf(" errs=open=%d/advise=%d/read=%d", summary.OpenErrors, summary.AdviseErrors, summary.ReadErrors)
	}
	fmt.Fprintf(stderr, "cache-load: %s warmed=%d/%d budget-pages=%d elapsed=%s%s\n",
		status, summary.Touched, len(entries), pageBudget, time.Since(start).Round(time.Millisecond), errPart)
}

// --- verification ---

func verifyLocalCopy(srcRoot, dstRoot string, entries []localEntry, cfg copyCLIConfig, concurrency int, stderr io.Writer) int {
	srcManifest, err := scanLocalDir(srcRoot, &tx.Manifest{})
	if err != nil {
		fmt.Fprintf(stderr, "local-verify-meta: [fail] scan source: %v\n", err)
		return 1
	}
	dstManifest, err := scanLocalDir(dstRoot, &tx.Manifest{})
	if err != nil {
		fmt.Fprintf(stderr, "local-verify-meta: [fail] scan destination: %v\n", err)
		return 1
	}
	metaFailed := false
	delta := compareManifestEntries(dstManifest, srcManifest)
	if len(delta.newFiles) > 0 || len(delta.staleFiles) > 0 || len(delta.removedPaths) > 0 {
		fmt.Fprintf(stderr, "local-verify-meta: [fail] mismatch new=%d (%s) stale=%d (%s) rm=%d\n",
			len(delta.newFiles), encoding.HumanBytes(delta.newBytes),
			len(delta.staleFiles), encoding.HumanBytes(delta.staleBytes),
			len(delta.removedPaths))
		metaFailed = true
	} else {
		files, hardlinks, symlinks, dirs := countManifestEntryTypes(srcManifest.Entries)
		fmt.Fprintf(stderr, "local-verify-meta: [ok] total=%d files=%d hardlinks=%d symlinks=%d dirs=%d\n",
			len(srcManifest.Entries), files, hardlinks, symlinks, dirs)
	}
	if cfg.verifyDataSamplePct <= 0 {
		if metaFailed {
			return 1
		}
		return 0
	}
	files, samples, elapsed, partial, err := verifyLocalCopyData(srcRoot, dstRoot, entries, cfg.verifyDataSamplePct, cfg.verifyBudget, concurrency)
	if err != nil {
		fmt.Fprintf(stderr, "local-verify-data: [fail] %v\n", err)
		return 1
	}
	status := "[ok]"
	if partial {
		status = "[partial-ok]"
	}
	fmt.Fprintf(stderr, "local-verify-data: %s files=%d samples=%d pct=%d elapsed=%s\n",
		status, files, samples, cfg.verifyDataSamplePct, elapsed.Round(time.Millisecond))
	if metaFailed {
		return 1
	}
	return 0
}

func verifyLocalCopyData(srcRoot, dstRoot string, entries []localEntry, pct int, budget time.Duration, concurrency int) (files int, samples int, elapsed time.Duration, partial bool, err error) {
	start := time.Now()
	type task struct {
		rel string
		gen sampler.Generator
	}
	var tasks []task
	for _, le := range entries {
		if !isRegularFileEntry(le.entry.Type) {
			continue
		}
		gen, ok := sampler.New(srcRoot, le.entry.Path, le.entry.ID, le.entry.Size, pct, defaultVerifySampleFrameSize, verifySampleBytes)
		if !ok {
			continue
		}
		tasks = append(tasks, task{rel: le.entry.Path, gen: gen})
	}
	if len(tasks) == 0 {
		return 0, 0, time.Since(start), false, nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if budget > 0 {
		var budgetCancel context.CancelFunc
		ctx, budgetCancel = context.WithTimeout(ctx, budget)
		defer budgetCancel()
	}
	var (
		mu         sync.Mutex
		firstErr   error
		doneFiles  int
		doneRanges int
	)
	jobs := make(chan task)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scratch := make([]byte, verifySampleBytes)
			for t := range jobs {
				if ctx.Err() != nil {
					continue
				}
				total := int(t.gen.TotalSamples())
				e := verifyLocalSampleFile(srcRoot, dstRoot, t.rel, t.gen, scratch)
				mu.Lock()
				if e != nil {
					if firstErr == nil {
						firstErr = e
						cancel()
					}
				} else {
					doneFiles++
					doneRanges += total
				}
				mu.Unlock()
			}
		}()
	}
	dispatched := true
	for _, t := range tasks {
		if ctx.Err() != nil {
			dispatched = false
			break
		}
		select {
		case <-ctx.Done():
			dispatched = false
		case jobs <- t:
			continue
		}
		break
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return 0, 0, time.Since(start), false, firstErr
	}
	partial = !dispatched && budget > 0
	return doneFiles, doneRanges, time.Since(start), partial, nil
}

func verifyLocalSampleFile(srcRoot, dstRoot, rel string, gen sampler.Generator, scratch []byte) error {
	srcPath := filepath.Join(srcRoot, filepath.FromSlash(rel))
	dstPath := filepath.Join(dstRoot, filepath.FromSlash(rel))
	srcFd, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open src %s: %w", rel, err)
	}
	defer srcFd.Close()
	dstFd, err := os.Open(dstPath)
	if err != nil {
		return fmt.Errorf("open dst %s: %w", rel, err)
	}
	defer dstFd.Close()
	for gen.Remaining() > 0 {
		s, ok := gen.Peek()
		if !ok {
			break
		}
		gen.Advance()
		srcHash, err := computeLocalSampleHash(srcFd, s.Offset, s.Size, scratch)
		if err != nil {
			return fmt.Errorf("hash src %s@%d: %w", rel, s.Offset, err)
		}
		dstHash, err := computeLocalSampleHash(dstFd, s.Offset, s.Size, scratch)
		if err != nil {
			return fmt.Errorf("hash dst %s@%d: %w", rel, s.Offset, err)
		}
		if !strings.EqualFold(srcHash, dstHash) {
			return fmt.Errorf("data mismatch %s at offset=%d size=%d", rel, s.Offset, s.Size)
		}
	}
	return nil
}

// --- progress ---

func resolveLocalProgressTargets(paths, formats []string, stderr io.Writer) ([]filexfer.ProgressTarget, error) {
	targets, err := cliflags.ResolveProgressTargets(paths, formats)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-path/--progress-format: %v\n", err)
		return nil, err
	}
	return targets, nil
}

func startLocalFileProgress(targets []filexfer.ProgressTarget, interval time.Duration, doneBytes, doneFiles *atomic.Int64, totalBytes, totalFiles int64, success func() bool) func() {
	if len(targets) == 0 {
		return func() {}
	}
	stop := filexfer.StartProgressFileWriter(context.Background(), targets, interval, func() filexfer.ProgressStatus {
		return filexfer.ProgressStatus{
			Source:     "client",
			DoneFiles:  uint64(maxInt64(doneFiles.Load(), 0)),
			TotalFiles: uint64(maxInt64(totalFiles, 0)),
			DoneBytes:  doneBytes.Load(),
			TotalBytes: totalBytes,
		}
	})
	return func() { stop(success()) }
}

// startLocalProgressTicker prints the shared "txfer-progress:" line to stderr on
// an interval, matching the remote transfer phase. The rate is instantaneous
// per-interval throughput; there is no link suffix (a local copy has no link).
func startLocalProgressTicker(doneBytes, doneFiles *atomic.Int64, totalBytes, totalFiles int64, stderr io.Writer) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(defaultVerboseProgressInterval)
		defer ticker.Stop()
		prevCopied := doneBytes.Load()
		prevTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				copied := doneBytes.Load()
				now := time.Now()
				dt := now.Sub(prevTime).Seconds()
				var rateBps float64
				if dt > 0 {
					rateBps = float64(copied-prevCopied) / dt
				}
				prevCopied = copied
				prevTime = now
				etaDisplay := fixedWidthETANA()
				if rateBps > 0 && totalBytes > copied {
					etaDisplay = fixedWidthETA(time.Duration(float64(totalBytes-copied) / rateBps * float64(time.Second)))
				}
				fmt.Fprintln(stderr, formatTxferProgressLine(
					uint64(maxInt64(doneFiles.Load(), 0)), uint64(maxInt64(totalFiles, 0)),
					copied, totalBytes, rateBps, etaDisplay, ""))
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func warnIgnoredLocalFlags(encryptMode string, authTokens []string, stderr io.Writer) {
	if enc := strings.ToLower(strings.TrimSpace(encryptMode)); enc != "" && enc != "none" {
		fmt.Fprintln(stderr, "note: --encrypt has no effect on local copies")
	}
	if len(authTokens) > 0 {
		fmt.Fprintln(stderr, "note: --auth-token has no effect on local copies")
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// --- get ---

type localGetArgs struct {
	srcPath          string
	outputPath       string
	skipFsync        bool
	cacheLoadEnabled bool
	cacheLoadBudget  time.Duration
	progressInterval time.Duration
}

// runLocalGetCLI copies a single local file to outputPath with copy_file_range.
func runLocalGetCLI(a localGetArgs, stdout, stderr io.Writer) int {
	info, err := os.Lstat(a.srcPath)
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	if info.IsDir() {
		fmt.Fprintf(stderr, "get source %s is a directory; use 'tx recv copy'\n", a.srcPath)
		return 2
	}

	toFile := a.outputPath != "-" && a.outputPath != os.DevNull

	var srcCE *pagecache.CacheEntry
	if a.cacheLoadEnabled && toFile && pagecache.TouchSupported() {
		ce := &pagecache.CacheEntry{}
		if err := ce.Load(a.srcPath); err == nil && !ce.Empty() {
			srcCE = ce
		}
	}

	src, err := os.Open(a.srcPath) // follows symlinks
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	defer src.Close()
	srcInfo, err := src.Stat()
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	size := srcInfo.Size()

	start := time.Now()
	if a.outputPath == "-" {
		buf := make([]byte, localCopyBufferBytes)
		if _, err := io.CopyBuffer(stdout, src, buf); err != nil {
			fmt.Fprintf(stderr, "get failed: %v\n", err)
			return 1
		}
		return 0
	}

	out, err := os.OpenFile(a.outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	if size > 0 {
		if _, err := localCopyFile(out, src, size, nil); err != nil {
			_ = out.Close()
			fmt.Fprintf(stderr, "get failed: %v\n", err)
			return 1
		}
	}
	if err := out.Close(); err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}

	if toFile {
		_ = os.Chmod(a.outputPath, srcInfo.Mode())
		mt := srcInfo.ModTime()
		_ = os.Chtimes(a.outputPath, mt, mt)
		if os.Geteuid() == 0 {
			if uid, gid, ok := localFileOwner(srcInfo); ok {
				_ = os.Lchown(a.outputPath, uid, gid)
			}
		}
		if !a.skipFsync {
			syncCtx, cancel := context.WithTimeout(context.Background(), defaultSyncfsTimeout)
			fsync.SyncfsDir(syncCtx, filepath.Dir(a.outputPath), a.progressInterval, stderr)
			cancel()
		}
		if srcCE != nil {
			touchLocalCache([]pagecache.TouchEntry{{Path: a.outputPath, Entry: srcCE}}, a.cacheLoadBudget, 1, stderr)
		}
	}

	fmt.Fprintf(stdout, "get: wrote %s (%s) in %s\n", a.outputPath, encoding.HumanBytes(size), time.Since(start).Round(time.Millisecond))
	return 0
}
