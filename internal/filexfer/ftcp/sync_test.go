package ftcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/pagecache"
)

func TestParseSYNCRequest(t *testing.T) {
	good := fmt.Sprintf(`SYNC %q mode=fast link-mbps=900 concurrency=12`, "/tmp/test")
	req, err := ParseRequest([]byte(good))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseSYNCRequest(req)
	if err != nil {
		t.Fatalf("parseSYNCRequest failed: %v", err)
	}
	if parsed.Directory != "/tmp/test" || parsed.Mode != "fast" || parsed.LinkMbps != 900 || parsed.Concurrency != 12 {
		t.Fatalf("unexpected parsed request: %+v", parsed)
	}

	bad := []string{
		`SYNC "/tmp" link-mbps=900 concurrency=12`,
		`SYNC "/tmp" mode=fast concurrency=12`,
		`SYNC "/tmp" mode=fast link-mbps=900`,
		`SYNC "/tmp" mode=slow link-mbps=900 concurrency=12`,
		`SYNC "/tmp" mode=fast link-mbps=-1 concurrency=12`,
		`SYNC "/tmp" mode=fast link-mbps=900 concurrency=0`,
	}
	for _, raw := range bad {
		t.Run(raw, func(t *testing.T) {
			req, err := ParseRequest([]byte(raw))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			if _, err := parseSYNCRequest(req); err == nil {
				t.Fatalf("expected parseSYNCRequest to fail for %q", raw)
			}
		})
	}
}

func TestParseSYNCRequestRejectsGentleCacheMap(t *testing.T) {
	for _, cacheMap := range []string{"send", "recv"} {
		t.Run(cacheMap, func(t *testing.T) {
			req, err := ParseRequest([]byte(`SYNC "/tmp" mode=gentle link-mbps=900 concurrency=12 cache-map=` + cacheMap))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			if _, err := parseSYNCRequest(req); err == nil {
				t.Fatalf("expected parseSYNCRequest to reject gentle cache-map=%s", cacheMap)
			} else if !strings.Contains(err.Error(), "cache-map is not supported with gentle mode") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestHandleSYNCEmptyOldManifest(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "b.txt", "world")

	newManifest, rmPaths := runSYNCTest(t, root, "")
	if len(rmPaths) != 0 {
		t.Fatalf("expected no removals, got %v", rmPaths)
	}
	if len(newManifest) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(newManifest))
	}
	paths := manifestPaths(newManifest)
	if paths[0] != "a.txt" || paths[1] != "b.txt" {
		t.Fatalf("unexpected paths: %v", paths)
	}
}

func TestHandleSYNCNoChanges(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "sub/b.txt", "world")

	// Get initial manifest via TXFER.
	initialManifest := runTXFERTest(t, root)

	// SYNC with that manifest — nothing should be removed.
	newManifest, rmPaths := runSYNCTest(t, root, initialManifest)
	if len(rmPaths) != 0 {
		t.Fatalf("expected no removals, got %v", rmPaths)
	}
	if len(newManifest) != 3 { // sub/ (D) + a.txt (F) + sub/b.txt (F)
		t.Fatalf("expected 3 entries, got %d", len(newManifest))
	}
}

func TestHandleSYNCAllRemoved(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "b.txt", "world")

	initialManifest := runTXFERTest(t, root)

	// Remove all files.
	os.Remove(filepath.Join(root, "a.txt"))
	os.Remove(filepath.Join(root, "b.txt"))

	newManifest, rmPaths := runSYNCTest(t, root, initialManifest)
	if len(newManifest) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(newManifest))
	}
	sort.Strings(rmPaths)
	if len(rmPaths) != 2 || rmPaths[0] != "a.txt" || rmPaths[1] != "b.txt" {
		t.Fatalf("unexpected removals: %v", rmPaths)
	}
}

func TestHandleSYNCZstdRoundTrip(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "sub/b.txt", "world")

	initialManifest := runTXFERTest(t, root)
	newManifest, rmPaths := runSYNCTestWithComp(t, root, initialManifest, encoding.EncodingZstd)
	if len(rmPaths) != 0 {
		t.Fatalf("expected no removals, got %v", rmPaths)
	}
	if len(newManifest) != 3 { // sub/ + a.txt + sub/b.txt
		t.Fatalf("expected 3 entries, got %d", len(newManifest))
	}
}

func TestHandleSYNCCacheMapRecvEnqueuesAndEchoes(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "stale.txt", "ignore")

	// Build an old-manifest body whose F-entry for a.txt carries a
	// synthetic pc:<hex> blob. We expect the server to (a) enqueue a
	// restore for that {path, decoded entry} pair and (b) echo the
	// blob verbatim on the response entry for a.txt.
	info, err := os.Stat(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}
	ce := pagecache.CacheEntry{}
	// One resident page is enough — the bitmap travels as data.
	if err := ce.SetPageBits([]byte{0x01}, 1); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	pcBlob, err := encoding.EncodePageCacheEntry(&ce)
	if err != nil {
		t.Fatalf("EncodePageCacheEntry: %v", err)
	}
	oldEntry := encoding.ManifestEntry{
		Type:       encoding.EntryTypeFile,
		ID:         1,
		Size:       info.Size(),
		Mtime:      info.ModTime().UnixNano(),
		Mode:       encoding.NormalizeManifestMode(info.Mode()),
		Path:       "a.txt",
		LinkTarget: -1,
		PageCache:  pcBlob,
	}
	hdr := encoding.FormatManifestHeader(encoding.ManifestHeader{TransferID: "old-tx", Mode: "fast", LinkMbps: 1000, Concurrency: 8})
	rootEntry := encoding.ManifestEntry{Type: encoding.EntryTypeDir, ID: encoding.RootFileID, Path: filepath.Clean(root), LinkTarget: -1}
	rootLine, prevPath, prevMtime, err := encoding.MarshalManifestEntry(rootEntry, "", "")
	if err != nil {
		t.Fatalf("marshal root: %v", err)
	}
	entryLine, _, _, err := encoding.MarshalManifestEntry(oldEntry, prevPath, prevMtime)
	if err != nil {
		t.Fatalf("marshal old entry: %v", err)
	}
	body := hdr + "\n" + rootLine + "\n" + entryLine + "\n"

	entries, _, deps := runSYNCTestFull(t, root, body, "none", "recv")

	var seenA bool
	for _, e := range entries {
		if e.Path == "a.txt" {
			seenA = true
			if !bytes.Equal(e.PageCache, pcBlob) {
				t.Fatalf("expected response pc blob to echo client's; got %d bytes vs want %d bytes", len(e.PageCache), len(pcBlob))
			}
		}
	}
	if !seenA {
		t.Fatalf("a.txt missing from response manifest")
	}
	if deps.cacheRestoreCall != 1 {
		t.Fatalf("expected one EnqueueCacheRestoreBatch call, got %d", deps.cacheRestoreCall)
	}
	if deps.cacheRestoreTxID != "tx123" {
		t.Fatalf("expected batch tagged with the SYNC's transfer id (tx123), got %q", deps.cacheRestoreTxID)
	}
	if len(deps.cacheRestoreCh) != 1 {
		t.Fatalf("expected one item in the batch, got %d", len(deps.cacheRestoreCh))
	}
	if !strings.HasSuffix(deps.cacheRestoreCh[0].Path, filepath.Join(root, "a.txt")) {
		t.Fatalf("unexpected enqueued path: %q", deps.cacheRestoreCh[0].Path)
	}
	if deps.cacheRestoreCh[0].Entry.NumResidentPages() != 1 {
		t.Fatalf("expected decoded entry to report 1 resident page")
	}
}

func TestHandleSYNCCacheMapSendDoesNotEnqueue(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	// cache-map=send only attaches server-side pc; never enqueues client pcs.
	_, _, deps := runSYNCTestFull(t, root, "", "none", "send")
	if deps.cacheRestoreCall != 0 || len(deps.cacheRestoreCh) != 0 {
		t.Fatalf("expected zero EnqueueCacheRestoreBatch calls in send mode, got call=%d items=%d",
			deps.cacheRestoreCall, len(deps.cacheRestoreCh))
	}
}

// TestHandleSYNCZeroDeltaAutoAcksAndCompletes covers the happy path the
// exit-after timer depends on: when every file in the client's old
// manifest matches the on-disk entry (same size+mtime+mode), the SYNC
// handler auto-acks each one and MaybeLogTransferComplete fires once.
func TestHandleSYNCZeroDeltaAutoAcksAndCompletes(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "b.txt", "world")
	initialManifest := runTXFERTest(t, root)

	_, _, deps := runSYNCTestFull(t, root, initialManifest, "none", "")

	if len(deps.ackCalls) == 0 {
		t.Fatalf("expected auto-acks for matched files, got none")
	}
	// Every ack should target the file's full size.
	for _, c := range deps.ackCalls {
		if c.ackBytes == 0 {
			t.Fatalf("ack for fileID=%d had ackBytes=0 (must equal file size)", c.fileID)
		}
	}
	if deps.completeCalls != 1 {
		t.Fatalf("expected MaybeLogTransferComplete to fire exactly once, got %d", deps.completeCalls)
	}
}

// TestHandleSYNCDeltaDoesNotAutoAckChangedEntries proves a size/mtime
// drift between the client's old manifest and the on-disk entry leaves
// the changed file un-acked. The client will SEND+ACK it later via the
// regular path, which fires MaybeLogTransferComplete from ack.go.
func TestHandleSYNCDeltaDoesNotAutoAckChangedEntries(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "keep.txt", "unchanged")
	writeTestFile(t, root, "stale.txt", "old content")
	initialManifest := runTXFERTest(t, root)

	// Mutate stale.txt so its size/mtime no longer match the manifest.
	time.Sleep(10 * time.Millisecond)
	writeTestFile(t, root, "stale.txt", "new content!!!")

	_, _, deps := runSYNCTestFull(t, root, initialManifest, "none", "")

	// Exactly one auto-ack (for keep.txt), not two — stale.txt drifted.
	if len(deps.ackCalls) != 1 {
		t.Fatalf("expected exactly one auto-ack (keep.txt), got %d: %+v",
			len(deps.ackCalls), deps.ackCalls)
	}
	// MaybeLogTransferComplete still fires unconditionally at the end of
	// the SYNC handler; the store-side check (Done == NumFiles) decides
	// whether to actually emit the complete log. Tests use mockDeps so
	// we just verify the call count.
	if deps.completeCalls != 1 {
		t.Fatalf("expected one MaybeLogTransferComplete call, got %d", deps.completeCalls)
	}
}

func TestHandleSYNCMixedDelta(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "keep.txt", "unchanged")
	writeTestFile(t, root, "stale.txt", "old content")
	writeTestFile(t, root, "remove.txt", "will be removed")

	initialManifest := runTXFERTest(t, root)

	// Modify stale.txt (change content and mtime).
	time.Sleep(10 * time.Millisecond)
	writeTestFile(t, root, "stale.txt", "new content!!!")

	// Remove remove.txt.
	os.Remove(filepath.Join(root, "remove.txt"))

	// Add new.txt.
	writeTestFile(t, root, "new.txt", "brand new")

	newManifest, rmPaths := runSYNCTest(t, root, initialManifest)

	// Should have keep.txt, new.txt, stale.txt in manifest.
	if len(newManifest) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(newManifest))
	}
	paths := manifestPaths(newManifest)
	// Walk order is alphabetical.
	if paths[0] != "keep.txt" || paths[1] != "new.txt" || paths[2] != "stale.txt" {
		t.Fatalf("unexpected paths: %v", paths)
	}

	// remove.txt should be in removals.
	if len(rmPaths) != 1 || rmPaths[0] != "remove.txt" {
		t.Fatalf("unexpected removals: %v", rmPaths)
	}
}

// --- Test helpers ---

func writeTestFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// runTXFERTest runs TXFER and returns the raw FM/1 manifest output.
// The wire response is a sequence of FX/1 frames (file_id=0); this
// helper unframes them and concatenates the decoded payloads.
func runTXFERTest(t *testing.T, root string) string {
	t.Helper()
	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	deps := &mockDeps{}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}
	return unframeManifestWire(t, out.Bytes())
}

// unframeManifestWire parses a sequence of FX/1 manifest frames
// (file_id=0) and returns the concatenated logical payloads.
func unframeManifestWire(t *testing.T, wire []byte) string {
	t.Helper()
	var out bytes.Buffer
	for _, frame := range parseManifestFrames(t, wire) {
		switch frame.Meta.Comp {
		case encoding.EncodingZstd:
			decoded, err := encoding.DecompressZstd(frame.Payload)
			if err != nil {
				t.Fatalf("DecompressZstd: %v", err)
			}
			out.Write(decoded)
		case "none":
			out.Write(frame.Payload)
		default:
			t.Fatalf("unsupported manifest frame comp %q", frame.Meta.Comp)
		}
	}
	return out.String()
}

// runSYNCTest runs SYNC with the given old manifest body and returns parsed entries + RM paths.
// RM fileIDs in the server response are resolved to paths using oldManifest.
// The old manifest is wrapped in FX/1 frames before being handed to the server,
// and the server's framed response is unwrapped before parsing.
func runSYNCTest(t *testing.T, root string, oldManifest string) ([]encoding.ManifestEntry, []string) {
	t.Helper()
	return runSYNCTestWithComp(t, root, oldManifest, "none")
}

func runSYNCTestWithComp(t *testing.T, root string, oldManifest string, comp string) ([]encoding.ManifestEntry, []string) {
	t.Helper()
	entries, rmPaths, _ := runSYNCTestFull(t, root, oldManifest, comp, "")
	return entries, rmPaths
}

func runSYNCTestFull(t *testing.T, root string, oldManifest string, comp string, cacheMap string) ([]encoding.ManifestEntry, []string, *mockDeps) {
	t.Helper()

	// Build ID→path index from old manifest for RM resolution.
	oldByID := buildOldByID(oldManifest)

	// Build SYNC request.
	reqRaw := fmt.Sprintf(`SYNC %q mode=fast link-mbps=1000 concurrency=8 comp=%s`, root, comp)
	if cacheMap != "" {
		reqRaw += " cache-map=" + cacheMap
	}
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}

	// Frame the old manifest body.
	var input bytes.Buffer
	cw := encoding.NewChunkedManifestWriter(&input, comp, encoding.DefaultManifestChunkSize, 0)
	if oldManifest != "" {
		if _, err := cw.Write([]byte(oldManifest)); err != nil {
			t.Fatalf("frame old manifest: %v", err)
		}
		if !strings.HasSuffix(oldManifest, "\n") {
			if _, err := cw.Write([]byte("\n")); err != nil {
				t.Fatalf("frame old manifest: %v", err)
			}
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("close framed old manifest: %v", err)
	}

	deps := &mockDeps{}
	var out bytes.Buffer
	if err := handleSYNCWithInput(context.Background(), req, &input, &out, deps, nil); err != nil {
		t.Fatalf("handleSYNCWithInput: %v", err)
	}

	entries, rmPaths := parseSYNCResponse(t, unframeManifestWire(t, out.Bytes()), oldByID)
	return entries, rmPaths, deps
}

// buildOldByID parses a raw FM/1 manifest and returns a fileID→path map.
func buildOldByID(rawManifest string) map[uint64]string {
	entries, _ := parseSYNCResponseEntries(rawManifest, nil)
	m := make(map[uint64]string, len(entries))
	for _, e := range entries {
		m[e.ID] = e.Path
	}
	return m
}

// parseSYNCResponse parses the SYNC output into manifest entries and RM paths.
// oldByID maps old manifest fileIDs to paths for RM resolution.
func parseSYNCResponse(t *testing.T, raw string, oldByID map[uint64]string) ([]encoding.ManifestEntry, []string) {
	t.Helper()
	entries, rmIDs := parseSYNCResponseEntries(raw, nil)
	rmPaths := make([]string, 0, len(rmIDs))
	for _, id := range rmIDs {
		path, ok := oldByID[id]
		if !ok {
			t.Fatalf("RM fileID %d not in old manifest", id)
		}
		rmPaths = append(rmPaths, path)
	}
	return entries, rmPaths
}

func manifestPaths(entries []encoding.ManifestEntry) []string {
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = e.Path
	}
	sort.Strings(paths)
	return paths
}

// --- Fuzz test ---

func FuzzSync(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0xFF, 0x01, 0x80, 0x42, 0x10, 0x99, 0xAB, 0xCD, 0xEF, 0x12})

	f.Fuzz(func(t *testing.T, seed []byte) {
		rng := deterministicRNG(seed)
		tmpDir := t.TempDir()

		// Round 1: Create initial files and get manifest via TXFER.
		numFiles := rng.Intn(33) // 0-32
		files := fuzzCreateRandomFiles(t, tmpDir, numFiles, rng)

		oldManifest := runTXFERTest(t, tmpDir)
		if oldManifest == "" && numFiles > 0 {
			t.Fatal("expected non-empty manifest")
		}

		// Round 2: Modify filesystem and run SYNC.
		fuzzApplyRandomModifications(t, tmpDir, &files, rng)

		newEntries, rmPaths := runSYNCTest(t, tmpDir, oldManifest)
		fuzzVerifySyncResult(t, tmpDir, oldManifest, newEntries, rmPaths)

		// Round 3: Chain — use SYNC result as old manifest, modify again, SYNC again.
		if len(newEntries) == 0 {
			return
		}
		chainManifest := fuzzBuildManifestFromEntries(t, tmpDir, newEntries)
		fuzzApplyRandomModifications(t, tmpDir, &files, rng)
		newEntries2, rmPaths2 := runSYNCTest(t, tmpDir, chainManifest)
		fuzzVerifySyncResult(t, tmpDir, chainManifest, newEntries2, rmPaths2)
	})
}

func deterministicRNG(seed []byte) *rand.Rand {
	var s int64
	if len(seed) >= 8 {
		s = int64(binary.LittleEndian.Uint64(seed[:8]))
	} else {
		for i, b := range seed {
			s |= int64(b) << (uint(i) * 8)
		}
	}
	return rand.New(rand.NewSource(s))
}

func fuzzCreateRandomFiles(t *testing.T, dir string, n int, rng *rand.Rand) []string {
	t.Helper()
	files := make([]string, 0, n)
	for i := range n {
		rel := fmt.Sprintf("file_%04d.dat", i)
		if rng.Intn(3) == 0 {
			rel = fmt.Sprintf("sub%d/file_%04d.dat", rng.Intn(5), i)
		}
		size := fuzzRandomFileSize(rng)
		data := make([]byte, size)
		rng.Read(data)
		writeTestFile(t, dir, rel, string(data))
		files = append(files, rel)
	}
	return files
}

func fuzzRandomFileSize(rng *rand.Rand) int {
	switch rng.Intn(5) {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		return 64 + rng.Intn(1024)
	case 3:
		return 1024 + rng.Intn(16*1024)
	default:
		return 16*1024 + rng.Intn(48*1024)
	}
}

func fuzzApplyRandomModifications(t *testing.T, dir string, files *[]string, rng *rand.Rand) {
	t.Helper()
	// Remove some files (0-30%).
	var kept []string
	for _, f := range *files {
		if rng.Intn(100) < 30 {
			os.Remove(filepath.Join(dir, f))
		} else {
			kept = append(kept, f)
		}
	}
	// Change some files (0-20%).
	for _, f := range kept {
		if rng.Intn(100) < 20 {
			size := fuzzRandomFileSize(rng)
			data := make([]byte, size)
			rng.Read(data)
			writeTestFile(t, dir, f, string(data))
		}
	}
	// Add new files (0-8).
	numNew := rng.Intn(9)
	base := len(kept) + 1000
	for i := range numNew {
		rel := fmt.Sprintf("new_%04d.dat", base+i)
		if rng.Intn(3) == 0 {
			rel = fmt.Sprintf("newsub%d/new_%04d.dat", rng.Intn(3), base+i)
		}
		size := fuzzRandomFileSize(rng)
		data := make([]byte, size)
		rng.Read(data)
		writeTestFile(t, dir, rel, string(data))
		kept = append(kept, rel)
	}
	*files = kept
}

func fuzzVerifySyncResult(t *testing.T, dir string, oldManifestRaw string, newEntries []encoding.ManifestEntry, rmPaths []string) {
	t.Helper()

	// Walk the filesystem to get ground truth (files, dirs, symlinks).
	onDisk := make(map[string]struct{})
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if path == dir {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		onDisk[filepath.ToSlash(rel)] = struct{}{}
		return nil
	})

	// Every path on disk must appear in newEntries.
	inManifest := make(map[string]struct{}, len(newEntries))
	for _, e := range newEntries {
		inManifest[e.Path] = struct{}{}
	}
	for path := range onDisk {
		if _, ok := inManifest[path]; !ok {
			t.Errorf("path on disk %q not in new manifest", path)
		}
	}
	// Every new manifest entry must exist on disk.
	for _, e := range newEntries {
		if _, ok := onDisk[e.Path]; !ok {
			t.Errorf("manifest entry %q not on disk", e.Path)
		}
		// Verify size matches for F entries; D/S/H entries have size=0.
		if e.Type == encoding.EntryTypeFile || e.Type == 0 {
			info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(e.Path)))
			if err != nil {
				t.Errorf("stat %q: %v", e.Path, err)
				continue
			}
			if info.Size() != e.Size {
				t.Errorf("size mismatch for %q: manifest=%d disk=%d", e.Path, e.Size, info.Size())
			}
		}
	}

	// Parse old manifest paths.
	oldPaths := make(map[string]struct{})
	if oldManifestRaw != "" {
		oldEntries, _ := parseSYNCResponseEntries(oldManifestRaw, nil)
		for _, e := range oldEntries {
			oldPaths[e.Path] = struct{}{}
		}
	}

	// rmPaths must be exactly: old paths not on disk.
	rmSet := make(map[string]struct{}, len(rmPaths))
	for _, p := range rmPaths {
		if _, dup := rmSet[p]; dup {
			t.Errorf("duplicate RM path: %q", p)
		}
		rmSet[p] = struct{}{}
	}
	for oldPath := range oldPaths {
		_, stillOnDisk := onDisk[oldPath]
		_, inRM := rmSet[oldPath]
		if !stillOnDisk && !inRM {
			t.Errorf("old path %q not on disk and not in RM", oldPath)
		}
		if stillOnDisk && inRM {
			t.Errorf("old path %q still on disk but in RM", oldPath)
		}
	}
	for rmPath := range rmSet {
		if _, ok := oldPaths[rmPath]; !ok {
			t.Errorf("RM path %q not in old manifest", rmPath)
		}
	}
}

// parseSYNCResponseEntries is a non-fatal version for fuzz/helper use.
// Returns manifest entries and raw RM fileIDs (caller resolves IDs to paths).
// oldByID is unused here but accepted for API symmetry with parseSYNCResponse.
func parseSYNCResponseEntries(raw string, _ map[uint64]string) ([]encoding.ManifestEntry, []uint64) {
	var entries []encoding.ManifestEntry
	var rmIDs []uint64
	prevPath := ""
	prevMtime := ""
	seenHeader := false

	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "FM/1 ") {
			seenHeader = true
			prevPath = ""
			prevMtime = ""
			continue
		}
		if strings.HasPrefix(trimmed, "RM ") {
			id, err := strconv.ParseUint(trimmed[3:], 10, 64)
			if err == nil {
				rmIDs = append(rmIDs, id)
			}
			continue
		}
		if !seenHeader {
			continue
		}
		entry, nextPath, nextMtime, err := encoding.ParseManifestEntry(trimmed, prevPath, prevMtime)
		if err != nil {
			continue
		}
		if entry.ID == encoding.RootFileID && entry.Type == encoding.EntryTypeDir && filepath.IsAbs(entry.Path) {
			prevPath = nextPath
			prevMtime = nextMtime
			continue
		}
		entries = append(entries, entry)
		prevPath = nextPath
		prevMtime = nextMtime
	}
	return entries, rmIDs
}

// fuzzBuildManifestFromEntries creates a raw FM/1 manifest string from entries.
func fuzzBuildManifestFromEntries(t *testing.T, root string, entries []encoding.ManifestEntry) string {
	t.Helper()
	var b strings.Builder
	hdr := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID:  "fuzz-tx",
		Mode:        "fast",
		LinkMbps:    1000,
		Concurrency: 8,
	})
	b.WriteString(hdr)
	b.WriteByte('\n')
	prevPath := ""
	prevMtime := ""
	rootEntry := encoding.ManifestEntry{Type: encoding.EntryTypeDir, ID: encoding.RootFileID, Path: filepath.Clean(root), LinkTarget: -1}
	rootLine, nextPath, nextMtime, err := encoding.MarshalManifestEntry(rootEntry, prevPath, prevMtime)
	if err != nil {
		t.Fatalf("marshal root entry: %v", err)
	}
	b.WriteString(rootLine)
	b.WriteByte('\n')
	prevPath = nextPath
	prevMtime = nextMtime
	for _, e := range entries {
		line, nextPath, nextMtime, err := encoding.MarshalManifestEntry(e, prevPath, prevMtime)
		if err != nil {
			t.Fatalf("marshal entry: %v", err)
		}
		b.WriteString(line)
		b.WriteByte('\n')
		prevPath = nextPath
		prevMtime = nextMtime
	}
	return b.String()
}
