package ftcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/pagecache"
)

type ackCall struct {
	fileID   uint64
	ackBytes int64
}

type txferTestDeps struct {
	setHintsCalls    int
	setHintsTxID     string
	setHintsMode     string
	setHintsMbps     int64
	setHintsConc     int
	cacheRestoreCh   []pagecache.TouchEntry
	cacheRestoreCall int
	cacheRestoreTxID string
	ackCalls         []ackCall
	completeCalls    int
}

func (d *txferTestDeps) NewTransfer(string, int, int64) (Transfer, error) {
	return Transfer{ID: "tx123"}, nil
}

func (d *txferTestDeps) DeleteTransfer(string) bool { return true }

func (d *txferTestDeps) RegisterTransferFileState(string, <-chan TransferFileStateUpdate, uint8) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (d *txferTestDeps) ClipTransfer(string) bool { return true }

func (d *txferTestDeps) GetTransfer(string) (Transfer, bool) { return Transfer{}, false }
func (d *txferTestDeps) ListTransfers() []Transfer           { return nil }

func (d *txferTestDeps) SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	d.setHintsCalls++
	d.setHintsTxID = txferID
	d.setHintsMode = mode
	d.setHintsMbps = linkMbps
	d.setHintsConc = concurrency
	return true
}

func (d *txferTestDeps) GetTransferGentleLimiter(string, int64, int, int64) *limit.Limiter {
	return nil
}

func (d *txferTestDeps) ReportTransferObservedLink(string, int64, int, int64, float64) (TransferObservedLinkUpdate, bool) {
	return TransferObservedLinkUpdate{}, false
}

func (d *txferTestDeps) GetFile(string, uint64, string) (*os.File, FileRef, error) {
	return nil, FileRef{}, nil
}

func (d *txferTestDeps) GetFileRef(string, uint64, string) (FileRef, error) {
	return FileRef{}, nil
}

func (d *txferTestDeps) SetTransferFileState(string, uint64, uint8) bool { return true }

func (d *txferTestDeps) SetTransferFileWindowHash(string, uint64, int64, string) bool { return true }

func (d *txferTestDeps) VerifyTransferFileWindowHash(string, uint64, int64, string) bool { return true }

func (d *txferTestDeps) AcknowledgeTransferFile(_ string, fileID uint64, ackBytes int64) bool {
	d.ackCalls = append(d.ackCalls, ackCall{fileID: fileID, ackBytes: ackBytes})
	return true
}

func (d *txferTestDeps) SetTransferPageCache(string, uint64, []byte) bool { return true }

func (d *txferTestDeps) SetTransferDeadline(string, int64) bool           { return false }
func (d *txferTestDeps) RecordTransferFirstSend(string) (time.Time, bool) { return time.Time{}, false }
func (d *txferTestDeps) MarkTransferTooSlow(string) bool                  { return false }
func (d *txferTestDeps) GetTransferLimiterBps(string) int64               { return 0 }
func (d *txferTestDeps) MaybeLogTransferProgress(string)                  {}
func (d *txferTestDeps) MaybeLogTransferComplete(string)                  { d.completeCalls++ }
func (d *txferTestDeps) Root() string                                     { return "/" }
func (d *txferTestDeps) EnqueueCacheRestoreBatch(txferID string, items []pagecache.TouchEntry) {
	d.cacheRestoreCh = append(d.cacheRestoreCh, items...)
	d.cacheRestoreCall++
	d.cacheRestoreTxID = txferID
}

func TestParseTXFERRequestRequiresHints(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=12`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		t.Fatalf("parseTXFERRequest failed: %v", err)
	}
	if parsed.Mode != "fast" || parsed.LinkMbps != 900 || parsed.Concurrency != 12 {
		t.Fatalf("unexpected parsed request: %+v", parsed)
	}

	bad := []string{
		`TXFER "/tmp" link-mbps=900 concurrency=12`,
		`TXFER "/tmp" mode=fast concurrency=12`,
		`TXFER "/tmp" mode=fast link-mbps=900`,
		`TXFER "/tmp" mode=slow link-mbps=900 concurrency=12`,
		`TXFER "/tmp" mode=fast link-mbps=-1 concurrency=12`,
		`TXFER "/tmp" mode=fast link-mbps=900 concurrency=0`,
	}
	for _, raw := range bad {
		t.Run(raw, func(t *testing.T) {
			req, err := ParseRequest([]byte(raw))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			if _, err := parseTXFERRequest(req); err == nil {
				t.Fatalf("expected parseTXFERRequest to fail for %q", raw)
			}
		})
	}
}

func TestHandleTXFERStoresHintsAndEmitsFM2(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	reqRaw := fmt.Sprintf(`TXFER %q mode=gentle link-mbps=700 concurrency=6 comp=none`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	deps := &txferTestDeps{}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleTXFER failed: %v", err)
	}
	if deps.setHintsCalls != 1 {
		t.Fatalf("expected one SetTransferHints call, got %d", deps.setHintsCalls)
	}
	if deps.setHintsTxID != "tx123" || deps.setHintsMode != "gentle" || deps.setHintsMbps != 700 || deps.setHintsConc != 6 {
		t.Fatalf("unexpected SetTransferHints values: tx=%s mode=%s mbps=%d conc=%d", deps.setHintsTxID, deps.setHintsMode, deps.setHintsMbps, deps.setHintsConc)
	}
	manifest := unframeManifestWire(t, out.Bytes())
	if !strings.HasPrefix(manifest, "FM/1 tx123 ") {
		t.Fatalf("expected FM/1 header, got: %q", manifest)
	}
	if !strings.Contains(manifest, "mode=gentle") || !strings.Contains(manifest, "link-mbps=700") || !strings.Contains(manifest, "concurrency=6") {
		t.Fatalf("manifest missing required metadata: %q", manifest)
	}
	if _, err := io.WriteString(io.Discard, manifest); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
}

func TestEncodeManifestHardlinks(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello world")
	if err := os.Link(filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil {
		t.Fatalf("hardlink: %v", err)
	}

	raw := runTXFERTest(t, root)
	entries, _ := parseSYNCResponseEntries(raw, nil)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Without hardlink dedup, both entries have size=11 (total=22).
	// With hardlink dedup, one F entry has size=11 and one H entry has size=0 (total=11).
	var totalSize int64
	for _, e := range entries {
		totalSize += e.Size
	}
	if totalSize != 11 {
		t.Fatalf("expected total size 11 (hardlink dedup), got %d", totalSize)
	}

	// Exactly one entry should be type H.
	var hCount int
	for _, e := range entries {
		if e.Type == 'H' {
			hCount++
			if e.Size != 0 {
				t.Errorf("H entry %q has size=%d, want 0", e.Path, e.Size)
			}
			if e.LinkTarget < 0 {
				t.Errorf("H entry %q has no link target", e.Path)
			}
		}
	}
	if hCount != 1 {
		t.Fatalf("expected 1 H entry, got %d", hCount)
	}
}

func TestEncodeManifestEmitsD0RootEntry(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")

	raw := runTXFERTest(t, root)
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected header, root, and file lines, got %q", raw)
	}
	if strings.Contains(lines[0], ":/") {
		t.Fatalf("header should not contain root token: %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "D0 0 ") || !strings.Contains(lines[1], ":"+root) {
		t.Fatalf("expected D0 absolute root line, got %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "F1 ") {
		t.Fatalf("expected first child to use id 1, got %q", lines[2])
	}
}

func TestEncodeManifestSymlinks(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	if err := os.Symlink("a.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	raw := runTXFERTest(t, root)
	entries, _ := parseSYNCResponseEntries(raw, nil)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	var fCount, sCount int
	for _, e := range entries {
		switch e.Type {
		case 'F':
			fCount++
			if e.Size != 5 {
				t.Errorf("F entry %q has size=%d, want 5", e.Path, e.Size)
			}
		case 'S':
			sCount++
			if e.Size != 0 {
				t.Errorf("S entry %q has size=%d, want 0", e.Path, e.Size)
			}
			if e.LinkPath != "a.txt" {
				t.Errorf("S entry %q has LinkPath=%q, want %q", e.Path, e.LinkPath, "a.txt")
			}
		}
	}
	if fCount != 1 || sCount != 1 {
		t.Fatalf("expected 1 F + 1 S, got %d F + %d S", fCount, sCount)
	}
}

func TestEncodeManifestDirectories(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "sub")
	if err := os.Mkdir(subDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTestFile(t, root, "sub/b.txt", "world")

	raw := runTXFERTest(t, root)
	entries, _ := parseSYNCResponseEntries(raw, nil)

	var dCount, fCount int
	for _, e := range entries {
		switch e.Type {
		case 'D':
			dCount++
			if e.Size != 0 {
				t.Errorf("D entry %q has size=%d, want 0", e.Path, e.Size)
			}
			if e.Mode&0o777 != 0o750 {
				t.Errorf("D entry %q has mode=%o, want 0750", e.Path, e.Mode)
			}
		case 'F':
			fCount++
		}
	}
	if dCount != 1 {
		t.Fatalf("expected 1 D entry, got %d", dCount)
	}
	if fCount != 1 {
		t.Fatalf("expected 1 F entry, got %d", fCount)
	}
}

func TestHandleTXFERLogsStartWithSeparateEntryAndFileCounts(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "sub")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTestFile(t, root, "sub/data.txt", "hello")

	deps := NewRuntimeDepsWithRoot("/")
	var txferID string
	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}

	var logs bytes.Buffer
	oldFlags := log.Flags()
	oldWriter := log.Writer()
	log.SetFlags(0)
	log.SetOutput(&logs)
	defer func() {
		log.SetFlags(oldFlags)
		log.SetOutput(oldWriter)
		if txferID != "" {
			deps.DeleteTransfer(txferID)
		}
	}()

	var out bytes.Buffer
	if err := handleTXFERWithCallback(context.Background(), req, &out, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("handleTXFERWithCallback failed: %v", err)
	}
	if txferID == "" {
		t.Fatalf("expected transfer ID callback")
	}
	stored, ok := deps.GetTransfer(txferID)
	if !ok {
		t.Fatalf("expected stored transfer")
	}
	if stored.NumEntries != 2 || stored.NumFiles != 1 {
		t.Fatalf("unexpected transfer counts: entries=%d files=%d", stored.NumEntries, stored.NumFiles)
	}

	logged := logs.String()
	if !strings.Contains(logged, "txfer-start: tid="+txferID) {
		t.Fatalf("expected txfer-start log, got %q", logged)
	}
	if !strings.Contains(logged, "entries=2 files=1") {
		t.Fatalf("expected txfer-start to show separate entry and file counts, got %q", logged)
	}
}

func TestHandleTXFERDirectoryOnlyLogsImmediateComplete(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	deps := NewRuntimeDepsWithRoot("/")
	var txferID string
	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}

	var logs bytes.Buffer
	oldFlags := log.Flags()
	oldWriter := log.Writer()
	log.SetFlags(0)
	log.SetOutput(&logs)
	defer func() {
		log.SetFlags(oldFlags)
		log.SetOutput(oldWriter)
		if txferID != "" {
			deps.DeleteTransfer(txferID)
		}
	}()

	var out bytes.Buffer
	if err := handleTXFERWithCallback(context.Background(), req, &out, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("handleTXFERWithCallback failed: %v", err)
	}
	stored, ok := deps.GetTransfer(txferID)
	if !ok {
		t.Fatalf("expected stored transfer")
	}
	if stored.NumEntries != 1 || stored.NumFiles != 0 {
		t.Fatalf("unexpected transfer counts: entries=%d files=%d", stored.NumEntries, stored.NumFiles)
	}

	logged := logs.String()
	if !strings.Contains(logged, "txfer-start: tid="+txferID) {
		t.Fatalf("expected txfer-start log, got %q", logged)
	}
	if !strings.Contains(logged, "entries=1 files=0") {
		t.Fatalf("expected txfer-start to show zero regular files, got %q", logged)
	}
	if !strings.Contains(logged, "txfer-complete: tid="+txferID+" files=0") {
		t.Fatalf("expected immediate txfer-complete log, got %q", logged)
	}
}

func TestHandleTXFEROmitsPageCacheByDefault(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.bin"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=900 concurrency=4`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, &txferTestDeps{}); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}
	if strings.Contains(out.String(), "pc:") {
		t.Fatalf("manifest should not include pagecache token without cache-map flag, got %q", out.String())
	}
}

func TestHandleTXFEREmitsPageCacheWhenRequested(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("cache-map emission requires Linux mincore")
	}
	root := t.TempDir()
	path := filepath.Join(root, "warm.bin")
	if err := os.WriteFile(path, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Force the file into the page cache so mincore reports residency.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := io.ReadAll(f); err != nil {
		t.Fatalf("read: %v", err)
	}
	f.Close()

	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=900 concurrency=4 cache-map=send`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, &txferTestDeps{}); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}
	if !strings.Contains(out.String(), " pc:") {
		t.Fatalf("expected pagecache token in manifest, got %q", out.String())
	}
}

// TestEncodeManifestEmitsPageCacheWithRelativeRoot regresses the bug where
// pagecache.LoadDirectory keys its result map by absolute paths while
// WalkManifestEntries yields FullPath relative to the caller's root form, so
// the lookup silently missed every entry when the server was launched with a
// relative chroot. Encode the manifest directly with a relative root and
// assert at least one entry carries a `pc:` blob.
func TestEncodeManifestEmitsPageCacheWithRelativeRoot(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("cache-map emission requires Linux mincore")
	}
	parent := t.TempDir()
	rootDirName := "data"
	absRoot := filepath.Join(parent, rootDirName)
	if err := os.Mkdir(absRoot, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(absRoot, "warm.bin")
	if err := os.WriteFile(path, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := io.ReadAll(f); err != nil {
		t.Fatalf("read: %v", err)
	}
	f.Close()

	t.Chdir(parent)

	var out bytes.Buffer
	if err := encodeManifest(&out, "tx-rel", rootDirName, "fast", 900, 4, 0, false, true, &txferTestDeps{}); err != nil {
		t.Fatalf("encodeManifest: %v", err)
	}
	if !strings.Contains(out.String(), " pc:") {
		t.Fatalf("expected pagecache token in manifest with relative root, got %q", out.String())
	}
}

func TestParseTXFERRequestAcceptsPageCache(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=4 cache-map=send`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		t.Fatalf("parseTXFERRequest: %v", err)
	}
	if !parsed.PageCache {
		t.Fatalf("expected PageCache=true")
	}
	if parsed.CacheMap != "send" {
		t.Fatalf("expected CacheMap=send, got %q", parsed.CacheMap)
	}
}

func TestParseTXFERRequestRejectsLegacyCacheMapValues(t *testing.T) {
	for _, raw := range []string{"1", "true", "0", "false", "garbage", "recv"} {
		t.Run(raw, func(t *testing.T) {
			req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=4 cache-map=` + raw))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			if _, err := parseTXFERRequest(req); err == nil {
				t.Fatalf("expected parseTXFERRequest to reject cache-map=%q", raw)
			}
		})
	}
}

func TestParseTXFERRequestAcceptsCacheMapNone(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=4 cache-map=none`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		t.Fatalf("parseTXFERRequest: %v", err)
	}
	if parsed.PageCache {
		t.Fatalf("expected PageCache=false")
	}
	if parsed.CacheMap != "none" {
		t.Fatalf("expected CacheMap=none, got %q", parsed.CacheMap)
	}
}

func TestParseTXFERRequestRejectsGentleCacheMap(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=gentle link-mbps=900 concurrency=4 cache-map=send`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if _, err := parseTXFERRequest(req); err == nil {
		t.Fatalf("expected parseTXFERRequest to reject gentle cache-map")
	} else if !strings.Contains(err.Error(), "cache-map is not supported with gentle mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleTXFERCompZstdEmitsStreamingFrames(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello world")

	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=zstd`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, &syncTestDeps{}); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}

	frames := parseManifestFrames(t, out.Bytes())
	if len(frames) == 0 {
		t.Fatalf("expected at least one FX/1 manifest frame, got 0")
	}
	var decoded bytes.Buffer
	var sawFileHash bool
	for i, fr := range frames {
		if fr.Meta.FileID != encoding.ManifestFrameFileID {
			t.Fatalf("frame %d: unexpected file_id %d", i, fr.Meta.FileID)
		}
		if fr.Meta.Comp != encoding.EncodingZstd {
			t.Fatalf("frame %d: comp=%q, want zstd", i, fr.Meta.Comp)
		}
		dec, err := encoding.DecompressZstd(fr.Payload)
		if err != nil {
			t.Fatalf("frame %d: DecompressZstd: %v", i, err)
		}
		decoded.Write(dec)
		if fr.Trailer.FileHashToken != "" {
			sawFileHash = true
			if i != len(frames)-1 {
				t.Fatalf("file-hash on non-terminal frame %d", i)
			}
		}
	}
	if !sawFileHash {
		t.Fatalf("expected terminal frame to carry file-hash")
	}
	if !bytes.HasPrefix(decoded.Bytes(), []byte("FM/1 ")) {
		t.Fatalf("decompressed payload missing FM/1 header: %q", decoded.String())
	}
	if !bytes.Contains(decoded.Bytes(), []byte("a.txt")) {
		t.Fatalf("decompressed payload missing a.txt entry: %q", decoded.String())
	}
}

func TestHandleTXFERCompNoneEmitsStreamingFrames(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello world")

	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, &syncTestDeps{}); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}
	frames := parseManifestFrames(t, out.Bytes())
	if len(frames) == 0 {
		t.Fatalf("expected at least one frame")
	}
	var fm bytes.Buffer
	for i, fr := range frames {
		if fr.Meta.Comp != "none" {
			t.Fatalf("frame %d: comp=%q, want none", i, fr.Meta.Comp)
		}
		if fr.Meta.WireSize != fr.Meta.Size {
			t.Fatalf("frame %d: comp=none should have wsize==size, got wsize=%d size=%d", i, fr.Meta.WireSize, fr.Meta.Size)
		}
		fm.Write(fr.Payload)
	}
	if !bytes.HasPrefix(fm.Bytes(), []byte("FM/1 ")) {
		t.Fatalf("missing FM/1 prefix: %q", fm.String())
	}
	if !bytes.Contains(fm.Bytes(), []byte("a.txt")) {
		t.Fatalf("missing entry: %q", fm.String())
	}
	// Terminal trailer must carry file-hash; intermediates must not.
	for i, fr := range frames {
		hasFile := fr.Trailer.FileHashToken != ""
		if i == len(frames)-1 && !hasFile {
			t.Fatalf("terminal frame missing file-hash")
		}
		if i != len(frames)-1 && hasFile {
			t.Fatalf("non-terminal frame %d has unexpected file-hash", i)
		}
	}
}

type parsedManifestFrame struct {
	Meta    encoding.FileFrameMeta
	Payload []byte
	Trailer encoding.FrameTrailer
}

func parseManifestFrames(t *testing.T, wire []byte) []parsedManifestFrame {
	t.Helper()
	var frames []parsedManifestFrame
	for len(wire) > 0 {
		nl := bytes.IndexByte(wire, '\n')
		if nl < 0 {
			t.Fatalf("missing FX/1 header newline; rem=%q", wire)
		}
		meta, err := encoding.ParseFXHeader(string(wire[:nl]))
		if err != nil {
			t.Fatalf("ParseFXHeader: %v", err)
		}
		wire = wire[nl+1:]
		if int64(len(wire)) < meta.WireSize {
			t.Fatalf("wire short: have=%d need=%d", len(wire), meta.WireSize)
		}
		payload := wire[:meta.WireSize]
		wire = wire[meta.WireSize:]
		nl = bytes.IndexByte(wire, '\n')
		if nl < 0 {
			t.Fatalf("missing FXT/1 trailer newline; rem=%q", wire)
		}
		trailerLine := string(wire[:nl])
		trailer, err := encoding.ParseFXTrailer(trailerLine)
		if err != nil {
			t.Fatalf("ParseFXTrailer(%q): %v", trailerLine, err)
		}
		wire = wire[nl+1:]
		frames = append(frames, parsedManifestFrame{Meta: meta, Payload: append([]byte(nil), payload...), Trailer: trailer})
	}
	return frames
}

func TestParseTXFERRequestRejectsUnsupportedComp(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=4 comp=lz4`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	_, err = parseTXFERRequest(req)
	if err == nil {
		t.Fatalf("expected UNSUPPORTED_COMP error")
	}
	if pe, ok := err.(protocolErr); !ok || pe.code != "UNSUPPORTED_COMP" {
		t.Fatalf("expected protocolErr UNSUPPORTED_COMP, got %v", err)
	}
}

func TestParseTXFERRequestRejectsMaxChunkSize(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=4 max-manifest-chunk-size=1024`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	_, err = parseTXFERRequest(req)
	if err == nil {
		t.Fatalf("expected BAD_REQUEST for removed max-manifest-chunk-size arg")
	}
	if pe, ok := err.(protocolErr); !ok || pe.code != "BAD_REQUEST" {
		t.Fatalf("expected protocolErr BAD_REQUEST, got %v", err)
	}
}
