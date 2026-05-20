package ftcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

type syncRequest struct {
	Directory   string
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64
	Comp        string
}

type oldEntry struct {
	size   int64
	mtime  int64
	mode   os.FileMode
	fileID uint64
	seen   bool
}

func parseSYNCRequest(req Request) (syncRequest, error) {
	if req.Verb != VerbSYNC {
		return syncRequest{}, protocolErr{code: "BAD_COMMAND", message: "not SYNC"}
	}
	if len(req.Params) != 1 {
		return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SYNC arguments"}
	}
	p := req.Params[0]
	for key := range p {
		switch key {
		case "directory", "mode", "link-mbps", "concurrency", "deadline-ms", "comp":
		default:
			return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "unknown SYNC option"}
		}
	}
	directory := p["directory"]
	if strings.TrimSpace(directory) == "" {
		return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing SYNC directory"}
	}
	mode := strings.ToLower(strings.TrimSpace(p["mode"]))
	if mode != "fast" && mode != "gentle" {
		return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "mode must be fast or gentle"}
	}
	linkMbps, err := strconv.ParseInt(strings.TrimSpace(p["link-mbps"]), 10, 64)
	if err != nil || linkMbps < 0 {
		return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "link-mbps must be >= 0"}
	}
	concurrency, err := strconv.Atoi(strings.TrimSpace(p["concurrency"]))
	if err != nil || concurrency <= 0 {
		return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "concurrency must be > 0"}
	}
	var deadlineMS int64
	if raw := strings.TrimSpace(p["deadline-ms"]); raw != "" {
		deadlineMS, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || deadlineMS < 0 {
			return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "deadline-ms must be >= 0"}
		}
	}
	comp := strings.ToLower(strings.TrimSpace(p["comp"]))
	if comp == "" {
		comp = encoding.EncodingZstd
	}
	switch comp {
	case "none", encoding.EncodingZstd:
	default:
		return syncRequest{}, protocolErr{code: "UNSUPPORTED_COMP", message: "supported comp values: none, zstd"}
	}
	return syncRequest{
		Directory:   directory,
		Mode:        mode,
		LinkMbps:    linkMbps,
		Concurrency: concurrency,
		DeadlineMS:  deadlineMS,
		Comp:        comp,
	}, nil
}

// handleSYNCCommand is the placeholder in the handlers map. The real work
// is done by handleSYNCWithInput which needs the io.Reader for manifest body.
func handleSYNCCommand(_ context.Context, _ Request, _ io.Writer, _ Deps) error {
	return protocolErr{code: "INTERNAL", message: "SYNC requires input reader, use handleSYNCWithInput"}
}

func handleSYNCWithInput(ctx context.Context, req Request, in io.Reader, out io.Writer, deps Deps, onTransferCreated func(string)) error {
	parsed, err := parseSYNCRequest(req)
	if err != nil {
		return err
	}
	parsed.Directory = filepath.Join(deps.Root(), parsed.Directory)
	isDir, err := validatePath(parsed.Directory)
	if err != nil {
		return protocolErr{code: "UNPROCESSABLE", message: err.Error()}
	}
	if !isDir {
		return protocolErr{code: "UNPROCESSABLE", message: "sync requires a directory, not a file"}
	}

	root := filepath.Clean(parsed.Directory)

	// Read the framed old-manifest body from the input stream and parse it.
	oldManifestReader := encoding.NewChunkedManifestReader(in, encoding.ChunkedManifestReaderOpts{})
	pathIndex, parseErr := readOldManifest(oldManifestReader)
	if parseErr != nil {
		return protocolErr{code: "BAD_REQUEST", message: fmt.Sprintf("invalid old manifest: %s", parseErr)}
	}

	// Create transfer and set hints (same pattern as TXFER).
	transfer, err := deps.NewTransfer(root, 0, 0)
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to initialize transfer"}
	}
	if onTransferCreated != nil {
		onTransferCreated(transfer.ID)
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

	// Wrap the output writer with a chunked manifest writer so FM/1 and RM
	// lines flow through FX/1 + FXT/1 frames with per-frame compression,
	// matching TXFER's response framing.
	cw := encoding.NewChunkedManifestWriter(out, parsed.Comp, encoding.DefaultManifestChunkSize, encoding.DefaultManifestFlushInterval)

	// Write FM/1 header.
	hdr := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID:  transfer.ID,
		Mode:        manifestMode,
		LinkMbps:    manifestLinkMbps,
		Concurrency: manifestConcurrency,
		DeadlineMS:  parsed.DeadlineMS,
	})
	if _, err := io.WriteString(cw, hdr+"\n"); err != nil {
		if isBrokenPipe(err) {
			return nil
		}
		return err
	}

	// Walk filesystem and emit manifest entries.
	updatesCh := make(chan TransferFileStateUpdate, 1000)
	done := deps.RegisterTransferFileState(transfer.ID, updatesCh, TransferStateStarted)
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
	prevPath := ""
	prevMtime := ""
	rootInfo, err := os.Stat(root)
	if err != nil {
		return protocolErr{code: "BAD_REQUEST", message: err.Error()}
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
		return protocolErr{code: "BAD_REQUEST", message: err.Error()}
	}
	updatesCh <- TransferFileStateUpdate{
		FileID:    encoding.RootFileID,
		EntryType: encoding.EntryTypeDir,
		PathHash:  xxh3.Hash128([]byte(filepath.Clean(root))),
		FileSize:  0,
	}
	if _, err := io.WriteString(cw, rootLine+"\n"); err != nil {
		if isBrokenPipe(err) {
			return nil
		}
		return err
	}
	prevPath = nextPath
	prevMtime = nextMtime

	emitSyncEntry := func(entry encoding.ManifestEntry) error {
		entry.ID = fileID
		// Mark in pathIndex that this path still exists on disk.
		pathHash := xxh3.Hash128([]byte(entry.Path))
		if old, ok := pathIndex[pathHash]; ok {
			old.seen = true
		}

		line, _, mtimeRaw, err := encoding.MarshalManifestEntry(entry, prevPath, prevMtime)
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
		if _, err := io.WriteString(cw, line); err != nil {
			return err
		}
		prevPath = entry.Path
		prevMtime = mtimeRaw
		fileID++
		return nil
	}

	walkErr := encoding.WalkManifestEntries(root, func(result encoding.WalkResult) error {
		return emitSyncEntry(result.Entry)
	})
	if walkErr != nil {
		if isBrokenPipe(walkErr) {
			return nil
		}
		return protocolErr{code: "BAD_REQUEST", message: walkErr.Error()}
	}

	// Emit RM lines for old paths no longer on disk.
	// Use the old manifest's fileID — client resolves IDs to paths locally.
	for _, old := range pathIndex {
		if old.seen {
			continue
		}
		rmLine := fmt.Sprintf("RM %d\n", old.fileID)
		if _, err := io.WriteString(cw, rmLine); err != nil {
			if isBrokenPipe(err) {
				return nil
			}
			return err
		}
	}

	if err := cw.Close(); err != nil {
		if isBrokenPipe(err) {
			return nil
		}
		return protocolErr{code: "INTERNAL", message: "flush manifest frames: " + err.Error()}
	}
	closeUpdates()
	deps.ClipTransfer(transfer.ID)
	cleanupTransfer = false
	return nil
}

// readOldManifest reads FM/1 manifest lines from in until a blank line.
// Returns a map keyed by xxh3-128 path hash — no path strings are retained.
func readOldManifest(in io.Reader) (map[xxh3.Uint128]*oldEntry, error) {
	br := bufio.NewReader(in)
	pathIndex := make(map[xxh3.Uint128]*oldEntry)
	prevPath := ""
	prevMtime := ""
	seenHeader := false

	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		trimmed := strings.TrimSpace(line)

		// Blank line terminates the old manifest body.
		if trimmed == "" {
			break
		}

		if strings.HasPrefix(trimmed, "FM/1 ") {
			// Parse header but we only need to validate it; we don't use its fields.
			if _, headerErr := encoding.ParseManifestHeader(trimmed); headerErr != nil {
				return nil, headerErr
			}
			seenHeader = true
			prevPath = ""
			prevMtime = ""
			continue
		}

		if !seenHeader {
			return nil, fmt.Errorf("manifest entry before header")
		}

		entry, nextPath, nextMtime, parseErr := encoding.ParseManifestEntry(trimmed, prevPath, prevMtime)
		if parseErr != nil {
			return nil, parseErr
		}
		if entry.ID == encoding.RootFileID && entry.Type == encoding.EntryTypeDir && filepath.IsAbs(entry.Path) {
			prevPath = nextPath
			prevMtime = nextMtime
			if err == io.EOF {
				break
			}
			continue
		}
		// Hash path immediately and discard the string — no path strings retained.
		pathHash := xxh3.Hash128([]byte(entry.Path))
		pathIndex[pathHash] = &oldEntry{
			size:   entry.Size,
			mtime:  entry.Mtime,
			mode:   entry.Mode,
			fileID: entry.ID,
		}
		prevPath = nextPath
		prevMtime = nextMtime

		if err == io.EOF {
			break
		}
	}

	return pathIndex, nil
}
