package ftcp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/zeebo/xxh3"
)

type txferRequest struct {
	Directory   string
	Verbose     bool
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64
	PageCache   bool
	Comp        string
	CacheMap    string // "none" or "send"
}

func parseTXFERRequest(req Request) (txferRequest, error) {
	if req.Verb != VerbTXFER {
		return txferRequest{}, protocolErr{code: "BAD_COMMAND", message: "not TXFER"}
	}
	if len(req.Params) != 1 {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid TXFER arguments"}
	}
	p := req.Params[0]
	for key := range p {
		switch key {
		case "directory", "verbose", "mode", "link-mbps", "concurrency", "deadline-ms", "cache-map", "comp":
		default:
			return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "unknown TXFER option"}
		}
	}
	directory := p["directory"]
	if strings.TrimSpace(directory) == "" {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing TXFER directory"}
	}
	verbose := false
	if raw := p["verbose"]; raw != "" {
		if raw == "1" || strings.EqualFold(raw, "true") {
			verbose = true
		}
	}
	cacheMap := "none"
	if raw := strings.ToLower(strings.TrimSpace(p["cache-map"])); raw != "" {
		switch raw {
		case "none":
			cacheMap = "none"
		case "send":
			cacheMap = "send"
		case "recv":
			return txferRequest{}, protocolErr{code: "UNSUPPORTED_CACHE_MAP", message: "cache-map=recv not supported on TXFER"}
		default:
			return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "cache-map must be none or send"}
		}
	}
	pageCache := cacheMap == "send"
	mode := strings.ToLower(strings.TrimSpace(p["mode"]))
	if mode != "fast" && mode != "gentle" {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "mode must be fast or gentle"}
	}
	linkMbps, err := strconv.ParseInt(strings.TrimSpace(p["link-mbps"]), 10, 64)
	if err != nil || linkMbps < 0 {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "link-mbps must be >= 0"}
	}
	concurrency, err := strconv.Atoi(strings.TrimSpace(p["concurrency"]))
	if err != nil || concurrency <= 0 {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "concurrency must be > 0"}
	}
	var deadlineMS int64
	if raw := strings.TrimSpace(p["deadline-ms"]); raw != "" {
		deadlineMS, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || deadlineMS < 0 {
			return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "deadline-ms must be >= 0"}
		}
	}
	comp := strings.ToLower(strings.TrimSpace(p["comp"]))
	if comp == "" {
		comp = encoding.EncodingZstd
	}
	switch comp {
	case "none", encoding.EncodingZstd:
	default:
		return txferRequest{}, protocolErr{code: "UNSUPPORTED_COMP", message: "supported comp values: none, zstd"}
	}
	return txferRequest{
		Directory:   directory,
		Verbose:     verbose,
		Mode:        mode,
		LinkMbps:    linkMbps,
		Concurrency: concurrency,
		DeadlineMS:  deadlineMS,
		PageCache:   pageCache,
		CacheMap:    cacheMap,
		Comp:        comp,
	}, nil
}

func handleTXFERWithCallback(ctx context.Context, req Request, out io.Writer, deps Deps, onCreated func(string)) error {
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		return err
	}
	parsed.Directory = filepath.Join(deps.Root(), parsed.Directory)
	isDir, err := validatePath(parsed.Directory)
	if err != nil {
		return protocolErr{code: "UNPROCESSABLE", message: err.Error()}
	}

	root := filepath.Clean(parsed.Directory)
	var singleFileName string
	if !isDir {
		singleFileName = filepath.Base(root)
		root = filepath.Dir(root)
	}
	transfer, err := deps.NewTransfer(root, 0, 0)
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to initialize transfer"}
	}
	if onCreated != nil {
		onCreated(transfer.ID)
	}
	if ok := deps.SetTransferHints(transfer.ID, parsed.Mode, parsed.LinkMbps, parsed.Concurrency); !ok {
		return protocolErr{code: "INTERNAL", message: "failed to persist transfer hints"}
	}
	if parsed.DeadlineMS > 0 {
		deps.SetTransferDeadline(transfer.ID, parsed.DeadlineMS)
	}
	manifestMode := parsed.Mode
	manifestLinkMbps := parsed.LinkMbps
	manifestConcurrency := parsed.Concurrency
	if stored, ok := deps.GetTransfer(transfer.ID); ok {
		if strings.TrimSpace(stored.Mode) != "" {
			manifestMode = strings.ToLower(strings.TrimSpace(stored.Mode))
		}
		if stored.LinkMbps >= 0 {
			manifestLinkMbps = stored.LinkMbps
		}
		if stored.Concurrency > 0 {
			manifestConcurrency = stored.Concurrency
		}
	}
	cleanupTransfer := true
	defer func() {
		if cleanupTransfer {
			deps.DeleteTransfer(transfer.ID)
		}
	}()

	cw := encoding.NewChunkedManifestWriter(out, parsed.Comp, encoding.DefaultManifestChunkSize, encoding.DefaultManifestFlushInterval)
	var encodeErr error
	if singleFileName != "" {
		encodeErr = encodeSingleFileManifest(cw, transfer.ID, root, singleFileName, manifestMode, manifestLinkMbps, manifestConcurrency, parsed.DeadlineMS, parsed.PageCache, deps)
	} else {
		encodeErr = encodeManifest(cw, transfer.ID, root, manifestMode, manifestLinkMbps, manifestConcurrency, parsed.DeadlineMS, parsed.Verbose, parsed.PageCache, deps)
	}
	if encodeErr != nil {
		if isBrokenPipe(encodeErr) {
			return nil
		}
		return protocolErr{code: "BAD_REQUEST", message: encodeErr.Error()}
	}
	if err := cw.Close(); err != nil {
		if isBrokenPipe(err) {
			return nil
		}
		return protocolErr{code: "INTERNAL", message: "flush manifest frames: " + err.Error()}
	}
	cleanupTransfer = false
	return nil
}

func handleTXFER(ctx context.Context, req Request, out io.Writer, deps Deps) error {
	return handleTXFERWithCallback(ctx, req, out, deps, nil)
}

func validatePath(path string) (isDir bool, err error) {
	if !filepath.IsAbs(path) {
		return false, errors.New("path must be an absolute path")
	}
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, errors.New("path does not exist")
		}
		return false, errors.New("path is not usable")
	}
	if stat.IsDir() {
		if _, err := os.ReadDir(path); err != nil {
			return false, errors.New("directory is not readable")
		}
		return true, nil
	}
	fd, err := os.Open(path)
	if err != nil {
		return false, errors.New("file is not readable")
	}
	fd.Close()
	return false, nil
}

func encodeSingleFileManifest(
	w io.Writer,
	transferID string,
	root string,
	filename string,
	mode string,
	linkMbps int64,
	concurrency int,
	deadlineMS int64,
	pageCache bool,
	deps Deps,
) error {
	fullPath := filepath.Join(root, filename)
	info, err := os.Stat(fullPath)
	if err != nil {
		return err
	}

	header := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID:  transferID,
		Mode:        mode,
		LinkMbps:    linkMbps,
		Concurrency: concurrency,
		DeadlineMS:  deadlineMS,
	})
	header += "\n"

	if _, err := io.WriteString(w, header); err != nil {
		return err
	}

	rootInfo, err := os.Stat(root)
	if err != nil {
		return err
	}
	rootEntry := encoding.ManifestEntry{
		Type:       encoding.EntryTypeDir,
		ID:         encoding.RootFileID,
		Size:       0,
		Mtime:      rootInfo.ModTime().UnixNano(),
		Mode:       encoding.NormalizeManifestMode(rootInfo.Mode()),
		Path:       filepath.Clean(root),
		LinkTarget: -1,
	}
	rootLine, prevPath, prevMtime, err := encoding.MarshalManifestEntry(rootEntry, "", "")
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, rootLine+"\n"); err != nil {
		return err
	}

	entry := encoding.ManifestEntry{
		Type:       encoding.EntryTypeFile,
		ID:         1,
		Size:       info.Size(),
		Mtime:      info.ModTime().UnixNano(),
		Mode:       encoding.NormalizeManifestMode(info.Mode()),
		Path:       filename,
		LinkTarget: -1,
	}
	if pageCache && info.Size() > 0 {
		var ce pagecache.CacheEntry
		if loadErr := ce.Load(fullPath); loadErr == nil && !ce.Empty() {
			if blob, mErr := encoding.EncodePageCacheEntry(&ce); mErr == nil && len(blob) > 0 {
				entry.PageCache = blob
				deps.SetTransferPageCache(transferID, 1, blob)
			}
		}
	}
	line, _, _, err := encoding.MarshalManifestEntry(entry, prevPath, prevMtime)
	if err != nil {
		return err
	}
	line += "\n"

	updatesCh := make(chan TransferFileStateUpdate, 2)
	done := deps.RegisterTransferFileState(transferID, updatesCh, TransferStateStarted)
	closedUpdates := false
	closeUpdates := func() {
		if closedUpdates {
			return
		}
		close(updatesCh)
		<-done
		closedUpdates = true
	}
	defer func() {
		closeUpdates()
	}()

	updatesCh <- TransferFileStateUpdate{
		FileID:    encoding.RootFileID,
		EntryType: encoding.EntryTypeDir,
		PathHash:  xxh3.Hash128([]byte(filepath.Clean(root))),
		FileSize:  0,
	}
	updatesCh <- TransferFileStateUpdate{
		FileID:    entry.ID,
		EntryType: encoding.EntryTypeFile,
		PathHash:  xxh3.Hash128([]byte(filepath.Clean(fullPath))),
		FileSize:  info.Size(),
	}

	if _, err := io.WriteString(w, line); err != nil {
		return err
	}
	closeUpdates()
	deps.ClipTransfer(transferID)
	return nil
}

func encodeManifest(
	w io.Writer,
	transferID string,
	root string,
	mode string,
	linkMbps int64,
	concurrency int,
	deadlineMS int64,
	verbose bool,
	pageCache bool,
	deps Deps,
) error {
	header := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID:  transferID,
		Mode:        mode,
		LinkMbps:    linkMbps,
		Concurrency: concurrency,
		DeadlineMS:  deadlineMS,
	})
	header += "\n"

	prevPath := ""
	prevMtime := ""
	updatesCh := make(chan TransferFileStateUpdate, 1000)
	done := deps.RegisterTransferFileState(transferID, updatesCh, TransferStateStarted)
	closedUpdates := false
	closeUpdates := func() {
		if closedUpdates {
			return
		}
		close(updatesCh)
		<-done
		closedUpdates = true
	}
	defer func() {
		closeUpdates()
	}()
	fileID := uint64(1)
	rootInfo, err := os.Stat(root)
	if err != nil {
		return err
	}

	if _, err := io.WriteString(w, header); err != nil {
		return err
	}

	rootEntry := encoding.ManifestEntry{
		Type:       encoding.EntryTypeDir,
		ID:         encoding.RootFileID,
		Size:       0,
		Mtime:      rootInfo.ModTime().UnixNano(),
		Mode:       encoding.NormalizeManifestMode(rootInfo.Mode()),
		Path:       filepath.Clean(root),
		LinkTarget: -1,
	}
	rootLine, nextPath, nextMtime, err := encoding.MarshalManifestEntry(rootEntry, prevPath, prevMtime)
	if err != nil {
		return err
	}
	rootLine += "\n"
	updatesCh <- TransferFileStateUpdate{
		FileID:    encoding.RootFileID,
		EntryType: encoding.EntryTypeDir,
		PathHash:  xxh3.Hash128([]byte(filepath.Clean(root))),
		FileSize:  0,
	}
	if _, err := io.WriteString(w, rootLine); err != nil {
		return err
	}
	prevPath = nextPath
	prevMtime = nextMtime

	emitEntry := func(entry encoding.ManifestEntry) error {
		entry.ID = fileID
		prevP := prevPath
		prevM := prevMtime
		if verbose {
			prevP = ""
			prevM = ""
		}
		line, _, mtimeRaw, err := encoding.MarshalManifestEntry(entry, prevP, prevM)
		if err != nil {
			return err
		}
		line += "\n"

		fullPath := filepath.Clean(filepath.Join(root, entry.Path))
		updatesCh <- TransferFileStateUpdate{
			FileID:    entry.ID,
			EntryType: entry.Type,
			PathHash:  xxh3.Hash128([]byte(fullPath)),
			FileSize:  entry.Size,
		}
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
		prevPath = entry.Path
		prevMtime = mtimeRaw
		fileID++
		return nil
	}

	var loadedFilePages map[string]*pagecache.CacheEntry
	if pageCache {
		// LoadDirectory keys its result map by absolute paths (it calls
		// filepath.Abs internally) while WalkManifestEntries below yields
		// FullPath in whatever form `root` was passed. Normalize once so the
		// two lookups agree even when the server was launched with a relative
		// chroot.
		if absRoot, absErr := filepath.Abs(root); absErr == nil {
			root = absRoot
		}
		loadedFilePages, _ = pagecache.LoadDirectory(root, 0)
	}

	err = encoding.WalkManifestEntries(root, func(result encoding.WalkResult) error {
		entry := result.Entry
		if loadedFilePages != nil && entry.Type == encoding.EntryTypeFile && entry.Size > 0 {
			if ce, ok := loadedFilePages[result.FullPath]; ok && !ce.Empty() {
				if blob, mErr := encoding.EncodePageCacheEntry(ce); mErr == nil && len(blob) > 0 {
					entry.PageCache = blob
					deps.SetTransferPageCache(transferID, uint64(fileID), blob)
				}
			}
		}
		return emitEntry(entry)
	})
	if err != nil {
		return err
	}
	closeUpdates()
	deps.ClipTransfer(transferID)
	return nil
}

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET)
}
