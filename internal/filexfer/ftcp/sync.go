package ftcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/zeebo/xxh3"
)

type syncRequest struct {
	manifestHints
	Directory string
	CacheMap  string // "none" (default), "send", or "recv"
}

type oldEntry struct {
	size   int64
	mtime  int64
	mode   os.FileMode
	fileID uint64
	pcBlob []byte
	seen   bool
}

var syncOptions = []string{"directory", "mode", "link-mbps", "concurrency", "deadline-ms", "comp", "cache-map"}

func parseSYNCRequest(req Request) (syncRequest, error) {
	p, err := manifestParams(req, VerbSYNC, syncOptions)
	if err != nil {
		return syncRequest{}, err
	}
	r := syncRequest{Directory: p["directory"]}
	if err := r.parseMode(p); err != nil {
		return syncRequest{}, err
	}
	if err := r.parseLimits(p); err != nil {
		return syncRequest{}, err
	}
	r.CacheMap = strings.ToLower(strings.TrimSpace(p["cache-map"]))
	switch r.CacheMap {
	case "":
		r.CacheMap = "none"
	case "none", "send", "recv":
	default:
		return syncRequest{}, protocolErr{code: "BAD_REQUEST", message: "cache-map must be none, send, or recv"}
	}
	if r.Mode == "gentle" && r.CacheMap != "none" {
		return syncRequest{}, errGentleCacheMap
	}
	return r, nil
}

// handleSYNCCommand is the placeholder in the handlers map. The real work
// is done by handleSYNCWithInput which needs the io.Reader for manifest body.
func handleSYNCCommand(_ context.Context, _ Request, _ io.Writer, _ Deps) error {
	return protocolErr{code: "INTERNAL", message: "SYNC requires input reader, use handleSYNCWithInput"}
}

func handleSYNCWithInput(ctx context.Context, req Request, in io.Reader, out io.Writer, deps Deps, onTransferCreated func(string), maxBodyBytes int64) error {
	parsed, err := parseSYNCRequest(req)
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
	if !isDir {
		return protocolErr{code: "UNPROCESSABLE", message: "sync requires a directory, not a file"}
	}

	// Read the framed old-manifest body from the input stream and parse it.
	// The body is capped because the index built from it has to be held whole:
	// the walk below consults it for every path on disk.
	oldManifestReader := encoding.NewFramedBodyReader(requestBodySource(in), encoding.FramedBodyReaderOpts{
		MaxLogicalBytes: maxBodyBytes,
	})
	// One decoded buffer serves the parser and the end check, so bytes after
	// the manifest's terminator cannot hide in a second read-ahead buffer.
	manifest := bufio.NewReader(oldManifestReader)
	pathIndex, parseErr := readOldManifest(manifest)
	if parseErr == nil {
		parseErr = expectDecodedEnd(manifest)
	}
	if parseErr != nil {
		return protocolErr{code: "BAD_REQUEST", message: fmt.Sprintf("invalid old manifest: %s", parseErr)}
	}
	if err := completeRequest(in); err != nil {
		return err
	}

	transferID, err := newManifestTransfer(deps, root, parsed.Directory, parsed.manifestHints, onTransferCreated)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			deps.DeleteTransfer(transferID)
		}
	}()

	// FM/1 and RM lines flow through FX/1 + FXT/1 frames with per-frame
	// compression, matching TXFER's response framing.
	cw := encoding.NewFramedBodyWriter(out, parsed.Comp, encoding.DefaultBodyChunkSize, encoding.DefaultBodyFlushInterval)
	e := newManifestEmitter(cw, deps, transferID, root, 1000)
	defer e.close()

	// restoreBatch accumulates {path, decoded snapshot} pairs from the
	// walk when cache-map=recv is set. After the walk completes the
	// whole slice is handed to a server-side producer goroutine so the
	// SYNC handler never blocks on the bounded restore-pool channel.
	var restoreBatch []pagecache.TouchEntry

	// acks holds the F entries the client's old manifest agrees with and
	// will therefore not re-fetch. After the walk they are acknowledged so
	// the transfer's Done count tracks the deltas the client will ACK.
	var acks []AckEntry

	if err := e.start(parsed.manifestHints); err != nil {
		return manifestErr(err)
	}
	err = encoding.WalkManifestEntries(root, func(result encoding.WalkResult) error {
		entry := result.Entry
		// Mark in pathIndex that this path still exists on disk.
		matched, isMatch := pathIndex[xxh3.Hash128([]byte(entry.Path))]
		if isMatch {
			matched.seen = true
		}

		// This mirrors the client's manifestEntryMatches check for regular
		// files: identical size, mtime, and mode mean the client already
		// has the file and will skip it.
		if entry.Type == encoding.EntryTypeFile && isMatch &&
			matched.size == entry.Size && matched.mtime == entry.Mtime && matched.mode == entry.Mode {
			acks = append(acks, AckEntry{TxferID: transferID, FileID: e.nextID, AckBytes: entry.Size})
		}

		// Attach pc:<hex> per the echo-vs-probe rule when a cache map was
		// requested. Only F entries carry residency.
		if entry.Type == encoding.EntryTypeFile && parsed.CacheMap != "none" {
			fullPath := result.FullPath
			if fullPath == "" {
				fullPath = filepath.Clean(filepath.Join(root, entry.Path))
			}
			switch parsed.CacheMap {
			case "send":
				if blob := probeAndEncodePageCache(fullPath); len(blob) > 0 {
					entry.PageCache = blob
				}
			case "recv":
				if isMatch && matched.size == entry.Size && len(matched.pcBlob) > 0 {
					// Echo client's snapshot on the response and remember
					// the {path, decoded entry} pair so a producer
					// goroutine can drain the batch into the worker pool
					// after the walk finishes.
					entry.PageCache = matched.pcBlob
					ce := &pagecache.CacheEntry{}
					if err := encoding.DecodePageCacheEntry(matched.pcBlob, ce); err == nil && !ce.Empty() {
						restoreBatch = append(restoreBatch, pagecache.TouchEntry{Path: fullPath, Entry: ce})
					}
				} else if blob := probeAndEncodePageCache(fullPath); len(blob) > 0 {
					// New-on-disk file (or size drift); attach the server's
					// current view so the client can replay it.
					entry.PageCache = blob
				}
			}
		}
		return e.emit(entry)
	})
	if err != nil {
		return manifestErr(err)
	}

	// Emit RM lines for old paths no longer on disk.
	// Use the old manifest's fileID — client resolves IDs to paths locally.
	for _, old := range pathIndex {
		if old.seen {
			continue
		}
		if _, err := fmt.Fprintf(cw, "RM %d\n", old.fileID); err != nil {
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
	if len(restoreBatch) > 0 {
		deps.EnqueueCacheRestoreBatch(transferID, restoreBatch)
	}
	// Drain the file-state goroutine before auto-acking so the slots
	// the acks index into have been populated with their FileSize.
	e.close()
	if len(acks) > 0 {
		deps.AcknowledgeTransferFiles(acks)
	}
	deps.ClipTransfer(transferID)
	// Zero-delta SYNC: Done == NumFiles right now, so this fires the
	// transfer-complete log and the exit-after wrapper restarts the
	// shutdown timer. For a delta SYNC this is a no-op; the trailing
	// SEND+ACK cycle will hit the same path from ack.go.
	deps.MaybeLogTransferComplete(transferID)
	keep = true
	return nil
}

// probeAndEncodePageCache loads the kernel residency for path and encodes
// it as a pc:<hex> blob. Returns nil on unsupported platforms, on probe
// errors, or when the file is empty / has nothing resident.
func probeAndEncodePageCache(fullPath string) []byte {
	if !pagecache.TouchSupported() {
		return nil
	}
	var ce pagecache.CacheEntry
	if err := ce.Load(fullPath); err != nil || ce.Empty() {
		return nil
	}
	blob, err := encoding.EncodePageCacheEntry(&ce)
	if err != nil {
		return nil
	}
	return blob
}

// expectDecodedEnd checks that nothing follows the manifest's terminator and
// that the framed body reached its verified terminal frame.
func expectDecodedEnd(br *bufio.Reader) error {
	if br.Buffered() > 0 {
		return errors.New("bytes after the manifest terminator")
	}
	if _, err := br.ReadByte(); err != io.EOF {
		if err == nil {
			return errors.New("bytes after the manifest terminator")
		}
		return err
	}
	return nil
}

// readOldManifest reads FM/1 manifest lines from br until a blank line.
// Returns a map keyed by xxh3-128 path hash — no path strings are retained.
func readOldManifest(br *bufio.Reader) (map[xxh3.Uint128]*oldEntry, error) {
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
		// Only the line terminator is stripped from what the parsers see:
		// path tokens are length-prefixed, so trimming spaces would eat bytes
		// the prefix already counted and break names with trailing whitespace.
		// The space-trimmed copy is used solely to classify the line.
		// Leading whitespace is still tolerated; only the trailing side was
		// destructive.
		trimmed := strings.TrimSpace(line)
		parseLine := strings.TrimLeft(line, " \t")

		// Blank line terminates the old manifest body.
		if trimmed == "" {
			break
		}

		if strings.HasPrefix(trimmed, "FM/1 ") {
			// Parse header but we only need to validate it; we don't use its fields.
			if _, headerErr := encoding.ParseManifestHeader(parseLine); headerErr != nil {
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

		entry, nextPath, nextMtime, parseErr := encoding.ParseManifestEntry(parseLine, prevPath, prevMtime)
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
			pcBlob: entry.PageCache,
		}
		prevPath = nextPath
		prevMtime = nextMtime

		if err == io.EOF {
			break
		}
	}

	return pathIndex, nil
}
