package ftcp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/jolynch/tx/internal/sysio"
	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
)

// manifestHints are the transfer hints TXFER and SYNC share. The manifest
// header echoes all but Comp, which selects the response body's codec.
type manifestHints struct {
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64
	Comp        string
}

type txferRequest struct {
	manifestHints
	Directory string
	Verbose   bool
	PageCache bool
	CacheMap  string // "none" or "send"
}

var txferOptions = []string{"directory", "verbose", "mode", "link-mbps", "concurrency", "deadline-ms", "cache-map", "comp"}

var errGentleCacheMap error = protocolErr{code: "BAD_REQUEST", message: "cache-map is not supported with gentle mode"}

func parseTXFERRequest(req Request) (txferRequest, error) {
	p, err := manifestParams(req, VerbTXFER, txferOptions)
	if err != nil {
		return txferRequest{}, err
	}
	r := txferRequest{Directory: p["directory"], CacheMap: "none"}
	r.Verbose = p["verbose"] == "1" || strings.EqualFold(p["verbose"], "true")
	switch strings.ToLower(strings.TrimSpace(p["cache-map"])) {
	case "", "none":
	case "send":
		r.CacheMap, r.PageCache = "send", true
	case "recv":
		return txferRequest{}, protocolErr{code: "UNSUPPORTED_CACHE_MAP", message: "cache-map=recv not supported on TXFER"}
	default:
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "cache-map must be none or send"}
	}
	if err := r.parseMode(p); err != nil {
		return txferRequest{}, err
	}
	if r.Mode == "gentle" && r.CacheMap != "none" {
		return txferRequest{}, errGentleCacheMap
	}
	if err := r.parseLimits(p); err != nil {
		return txferRequest{}, err
	}
	return r, nil
}

// manifestParams returns the options of a TXFER or SYNC request after
// checking its verb, its option names, and that it names a directory.
func manifestParams(req Request, verb Verb, options []string) (map[string]string, error) {
	name := verb.String()
	if req.Verb != verb {
		return nil, protocolErr{code: "BAD_COMMAND", message: "not " + name}
	}
	if len(req.Params) != 1 {
		return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + name + " arguments"}
	}
	p := req.Params[0]
	for key := range p {
		if !slices.Contains(options, key) {
			return nil, protocolErr{code: "BAD_REQUEST", message: "unknown " + name + " option"}
		}
	}
	if strings.TrimSpace(p["directory"]) == "" {
		return nil, protocolErr{code: "BAD_REQUEST", message: "missing " + name + " directory"}
	}
	return p, nil
}

// parseMode and parseLimits are separate because TXFER checks its cache map
// between them and SYNC checks it after both, and each verb keeps its order.
func (h *manifestHints) parseMode(p map[string]string) error {
	h.Mode = strings.ToLower(strings.TrimSpace(p["mode"]))
	if h.Mode != "fast" && h.Mode != "gentle" {
		return protocolErr{code: "BAD_REQUEST", message: "mode must be fast or gentle"}
	}
	return nil
}

func (h *manifestHints) parseLimits(p map[string]string) error {
	var err error
	if h.LinkMbps, err = strconv.ParseInt(strings.TrimSpace(p["link-mbps"]), 10, 64); err != nil || h.LinkMbps < 0 {
		return protocolErr{code: "BAD_REQUEST", message: "link-mbps must be >= 0"}
	}
	if h.Concurrency, err = strconv.Atoi(strings.TrimSpace(p["concurrency"])); err != nil || h.Concurrency <= 0 {
		return protocolErr{code: "BAD_REQUEST", message: "concurrency must be > 0"}
	}
	if raw := strings.TrimSpace(p["deadline-ms"]); raw != "" {
		if h.DeadlineMS, err = strconv.ParseInt(raw, 10, 64); err != nil || h.DeadlineMS < 0 {
			return protocolErr{code: "BAD_REQUEST", message: "deadline-ms must be >= 0"}
		}
	}
	h.Comp = strings.ToLower(strings.TrimSpace(p["comp"]))
	switch h.Comp {
	case "":
		h.Comp = encoding.EncodingZstd
	case "none", encoding.EncodingZstd:
	default:
		return protocolErr{code: "UNSUPPORTED_COMP", message: "supported comp values: none, zstd"}
	}
	return nil
}

func handleTXFERWithCallback(ctx context.Context, req Request, out io.Writer, deps Deps, onCreated func(string)) error {
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		return err
	}
	root, err := resolveUnderRoot(deps.Root(), parsed.Directory)
	if err != nil {
		return err
	}
	isDir, err := validatePath(root)
	if err != nil {
		return protocolErr{code: "UNPROCESSABLE", message: err.Error()}
	}
	var file string
	updates := 1000
	if !isDir {
		root, file = filepath.Dir(root), filepath.Base(root)
		updates = 2 // the root and the file
	}
	transferID, err := newManifestTransfer(deps, root, parsed.Directory, parsed.manifestHints, onCreated)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			deps.DeleteTransfer(transferID)
		}
	}()

	cw := encoding.NewFramedBodyWriter(out, parsed.Comp, encoding.DefaultBodyChunkSize, encoding.DefaultBodyFlushInterval)
	e := newManifestEmitter(cw, deps, transferID, root, updates)
	defer e.close()
	if err := e.start(parsed.manifestHints); err != nil {
		return manifestErr(err)
	}
	if file != "" {
		err = encodeSingleFileManifest(e, file, parsed.PageCache)
	} else {
		err = encodeManifest(e, parsed.Verbose, parsed.PageCache)
	}
	if err != nil {
		return manifestErr(err)
	}
	e.close()
	deps.ClipTransfer(transferID)
	if err := cw.Close(); err != nil {
		if isBrokenPipe(err) {
			return nil
		}
		return protocolErr{code: "INTERNAL", message: "flush manifest frames: " + err.Error()}
	}
	keep = true
	return nil
}

// manifestErr maps an error from writing a manifest: a closed connection ends
// the request quietly, and anything else is BAD_REQUEST.
func manifestErr(err error) error {
	if isBrokenPipe(err) {
		return nil
	}
	return protocolErr{code: "BAD_REQUEST", message: err.Error()}
}

func handleTXFER(ctx context.Context, req Request, out io.Writer, deps Deps) error {
	return handleTXFERWithCallback(ctx, req, out, deps, nil)
}

// newManifestTransfer creates the transfer a TXFER or SYNC manifest describes
// and stores its hints.
func newManifestTransfer(deps Deps, root, requested string, h manifestHints, onCreated func(string)) (string, error) {
	transfer, err := deps.NewTransfer(root, 0, 0, store.WithRequestPath(cleanRequestPath(requested)))
	if err != nil {
		return "", protocolErr{code: "INTERNAL", message: "failed to initialize transfer"}
	}
	if onCreated != nil {
		onCreated(transfer.ID)
	}
	if !deps.SetTransferHints(transfer.ID, h.Mode, h.LinkMbps, h.Concurrency) {
		return "", protocolErr{code: "INTERNAL", message: "failed to persist transfer hints"}
	}
	if h.DeadlineMS > 0 {
		deps.SetTransferDeadline(transfer.ID, h.DeadlineMS)
	}
	return transfer.ID, nil
}

// manifestEmitter writes a manifest's entries, front-coding each path and
// mtime against the previous entry's, and registers each entry's file state
// with the transfer.
type manifestEmitter struct {
	w          io.Writer
	deps       Deps
	transferID string
	root       string
	verbose    bool // write every path and mtime in full
	nextID     uint64
	prevPath   string
	prevMtime  string
	updates    chan TransferFileStateUpdate
	done       <-chan struct{}
}

func newManifestEmitter(w io.Writer, deps Deps, transferID, root string, capacity int) *manifestEmitter {
	updates := make(chan TransferFileStateUpdate, capacity)
	return &manifestEmitter{
		w:          w,
		deps:       deps,
		transferID: transferID,
		root:       root,
		nextID:     1,
		updates:    updates,
		done:       deps.RegisterTransferFileState(transferID, updates, TransferStateStarted),
	}
}

// start writes the header and the root directory's entry.
func (e *manifestEmitter) start(h manifestHints) error {
	header := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID:  e.transferID,
		Mode:        h.Mode,
		LinkMbps:    h.LinkMbps,
		Concurrency: h.Concurrency,
		DeadlineMS:  h.DeadlineMS,
	})
	if _, err := io.WriteString(e.w, header+"\n"); err != nil {
		return err
	}
	info, err := os.Stat(e.root)
	if err != nil {
		return err
	}
	root := filepath.Clean(e.root)
	return e.write(encoding.ManifestEntry{
		Type:       encoding.EntryTypeDir,
		ID:         encoding.RootFileID,
		Mtime:      info.ModTime().UnixNano(),
		Mode:       encoding.NormalizeManifestMode(info.Mode()),
		Path:       root,
		LinkTarget: -1,
	}, root)
}

// emit writes entry, which is relative to the root, with the next file ID.
func (e *manifestEmitter) emit(entry encoding.ManifestEntry) error {
	entry.ID = e.nextID
	e.nextID++
	return e.write(entry, filepath.Join(e.root, entry.Path))
}

func (e *manifestEmitter) write(entry encoding.ManifestEntry, fullPath string) error {
	prevPath, prevMtime := e.prevPath, e.prevMtime
	if e.verbose {
		prevPath, prevMtime = "", ""
	}
	line, _, mtime, err := encoding.MarshalManifestEntry(entry, prevPath, prevMtime)
	if err != nil {
		return err
	}
	e.updates <- TransferFileStateUpdate{
		FileID:    entry.ID,
		EntryType: entry.Type,
		PathHash:  xxh3.Hash128([]byte(fullPath)),
		FileSize:  entry.Size,
	}
	if _, err := io.WriteString(e.w, line+"\n"); err != nil {
		return err
	}
	e.prevPath, e.prevMtime = entry.Path, mtime
	return nil
}

// attachPageCache records blob as the page-cache map of the entry emit
// writes next.
func (e *manifestEmitter) attachPageCache(entry *encoding.ManifestEntry, blob []byte) {
	entry.PageCache = blob
	e.deps.SetTransferPageCache(e.transferID, e.nextID, blob)
}

// close ends file-state registration and waits for it to finish. Calls after
// the first do nothing.
func (e *manifestEmitter) close() {
	if e.updates == nil {
		return
	}
	close(e.updates)
	<-e.done
	e.updates = nil
}

// encodeSingleFileManifest emits the one file a single-file TXFER names.
func encodeSingleFileManifest(e *manifestEmitter, filename string, pageCache bool) error {
	fullPath := filepath.Join(e.root, filename)
	info, err := os.Stat(fullPath)
	if err != nil {
		return err
	}
	entry := encoding.ManifestEntry{
		Type:       encoding.EntryTypeFile,
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
				e.attachPageCache(&entry, blob)
			}
		}
	}
	return e.emit(entry)
}

// encodeManifest walks the root and emits every entry under it.
func encodeManifest(e *manifestEmitter, verbose bool, pageCache bool) error {
	e.verbose = verbose
	var pages map[string]*pagecache.CacheEntry
	if pageCache {
		// LoadDirectory keys its result map by absolute paths (it calls
		// filepath.Abs internally) while WalkManifestEntries below yields
		// FullPath in whatever form the root was passed. Normalize once so the
		// two lookups agree even when the server was launched with a relative
		// chroot.
		if absRoot, absErr := filepath.Abs(e.root); absErr == nil {
			e.root = absRoot
		}
		pages, _ = pagecache.LoadDirectory(e.root, 0)
	}
	return encoding.WalkManifestEntries(e.root, func(result encoding.WalkResult) error {
		entry := result.Entry
		if pages != nil && entry.Type == encoding.EntryTypeFile && entry.Size > 0 {
			if ce, ok := pages[result.FullPath]; ok && !ce.Empty() {
				if blob, mErr := encoding.EncodePageCacheEntry(ce); mErr == nil && len(blob) > 0 {
					e.attachPageCache(&entry, blob)
				}
			}
		}
		return e.emit(entry)
	})
}

// cleanRequestPath is the requested path as the client sees it under the
// server root, recorded on the transfer for stats and events.
func cleanRequestPath(requested string) string {
	return filepath.Clean("/" + requested)
}

// resolveUnderRoot joins a requested path onto the server root and rejects
// any result that lexically leaves it, before anything is stat-ed. Symlinks
// are deliberately not resolved: links inside the root, which the operator
// created, are still followed.
func resolveUnderRoot(root string, requested string) (string, error) {
	joined := filepath.Join(root, requested)
	if !utils.PathWithinRoot(filepath.Clean(root), joined) {
		return "", protocolErr{code: "UNPROCESSABLE", message: "path must be within server root"}
	}
	return joined, nil
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
	fd, err := os.Open(path) //nolint:forbidigo // Manifest readability probe; SEND and CXSUM gate data opens with OpenErr.
	if err != nil {
		return false, errors.New("file is not readable")
	}
	fd.Close()
	return false, nil
}

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, sysio.EPIPE) ||
		errors.Is(err, sysio.ECONNRESET)
}
