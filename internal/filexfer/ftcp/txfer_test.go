package ftcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

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
	deps := &mockDeps{}
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

func TestHandleTXFERLogsStartWithSeparateEntryAndFileCounts(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "sub")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTestFile(t, root, "sub/data.txt", "hello")

	deps := realDeps(t, "/")
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

	deps := realDeps(t, "/")
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
	if err := handleTXFER(context.Background(), req, &out, &mockDeps{}); err != nil {
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
	if err := handleTXFER(context.Background(), req, &out, &mockDeps{}); err != nil {
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
	deps := &mockDeps{}
	e := newManifestEmitter(&out, deps, "tx-rel", rootDirName, 1000)
	defer e.close()
	if err := e.start(manifestHints{Mode: "fast", LinkMbps: 900, Concurrency: 4}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := encodeManifest(e, false, true); err != nil {
		t.Fatalf("encodeManifest: %v", err)
	}
	if !strings.Contains(out.String(), " pc:") {
		t.Fatalf("expected pagecache token in manifest with relative root, got %q", out.String())
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
	if err := handleTXFER(context.Background(), req, &out, &mockDeps{}); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}

	frames := parseManifestFrames(t, out.Bytes())
	if len(frames) == 0 {
		t.Fatalf("expected at least one FX/1 manifest frame, got 0")
	}
	var decoded bytes.Buffer
	var sawFileHash bool
	for i, fr := range frames {
		if fr.Meta.FileID != encoding.FramedBodyFileID {
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
	if err := handleTXFER(context.Background(), req, &out, &mockDeps{}); err != nil {
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

// escapeTestLayout builds base/root (the chroot) beside base/secret and
// base/rootx, which share a parent and a name prefix with the root and hold
// files that no request under root may reach.
func escapeTestLayout(t *testing.T) (root string) {
	t.Helper()
	base := t.TempDir()
	root = filepath.Join(base, "root")
	writeTestFile(t, root, "sub/f.txt", "inside")
	writeTestFile(t, base, "secret/s.txt", "outside")
	writeTestFile(t, base, "rootx/s.txt", "sibling sharing the root's prefix")
	return root
}

var rootEscapeCases = []struct {
	path   string
	escape bool
}{
	{"/../secret", true},
	{"/a/../../secret", true},
	{"/..", true},
	{"/../rootx", true},
	{"/sub/../sub", false},
}

func TestHandleTXFERRejectsPathEscapingRoot(t *testing.T) {
	root := escapeTestLayout(t)
	for _, tc := range rootEscapeCases {
		t.Run(tc.path, func(t *testing.T) {
			st := store.NewStore()
			t.Cleanup(st.Close)
			deps := NewRuntimeDeps(st, WithRoot(root))
			req, err := ParseRequest([]byte(fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none`, tc.path)))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			var out bytes.Buffer
			err = handleTXFER(context.Background(), req, &out, deps)
			if !tc.escape {
				if err != nil || len(st.ListTransfers()) != 1 {
					t.Fatalf("in-root path rejected: err=%v transfers=%d", err, len(st.ListTransfers()))
				}
				return
			}
			var pe protocolErr
			if !errors.As(err, &pe) || pe.code != "UNPROCESSABLE" {
				t.Fatalf("expected UNPROCESSABLE, got err=%v manifest=%q", err, unframeManifestWire(t, out.Bytes()))
			}
			if out.Len() != 0 || len(st.ListTransfers()) != 0 {
				t.Fatalf("escape left state: manifest=%d bytes transfers=%d", out.Len(), len(st.ListTransfers()))
			}
		})
	}
}

// lexicallyUnder is an oracle independent of utils.PathWithinRoot: a clean
// path is under a clean root when it is the root, the root is "/", or it
// extends the root by a whole path component.
func lexicallyUnder(cleanRoot string, p string) bool {
	return p == cleanRoot || cleanRoot == "/" || strings.HasPrefix(p, cleanRoot+"/")
}

// FuzzResolveUnderRoot checks that a resolved path is exactly the join and
// stays under the root, and that only a request with a ".." segment can be
// refused. The server root is always absolute.
func FuzzResolveUnderRoot(f *testing.F) {
	f.Add("/srv/data", "/../secret")
	f.Add("/srv/data", "/a/../../secret")
	f.Add("/srv/data", "/..")
	f.Add("/srv/data", "/sub/../sub")
	f.Add("/srv/data", "/../dataX")
	f.Add("/srv/data", "")
	f.Add("/", "/../../etc")
	f.Add("/srv/data", "..foo/../x")
	f.Add("/srv/./data/", "a//b/./../..")

	f.Fuzz(func(t *testing.T, root string, requested string) {
		root = "/" + root
		cleanRoot := filepath.Clean(root)
		got, err := resolveUnderRoot(root, requested)
		joined := filepath.Join(root, requested)
		if err != nil {
			var pe protocolErr
			if !errors.As(err, &pe) || pe.code != "UNPROCESSABLE" {
				t.Fatalf("resolveUnderRoot(%q, %q): unexpected error %v", root, requested, err)
			}
			if lexicallyUnder(cleanRoot, joined) {
				t.Fatalf("resolveUnderRoot(%q, %q) refused in-root path %q", root, requested, joined)
			}
			if !slices.Contains(strings.Split(requested, "/"), "..") {
				t.Fatalf("resolveUnderRoot(%q, %q) refused a request without a .. segment", root, requested)
			}
			return
		}
		if got != joined || !lexicallyUnder(cleanRoot, got) {
			t.Fatalf("resolveUnderRoot(%q, %q) = %q, want in-root %q", root, requested, got, joined)
		}
	})
}

// TestParseTXFERAndSYNCRequests runs each argument list through both parsers.
// The parsers check fields in different orders: TXFER checks cache-map, and
// gentle with a cache map, before link-mbps, while SYNC checks them last. The
// cases with two bad values pin each order.
func TestParseTXFERAndSYNCRequests(t *testing.T) {
	const hints = `mode=fast link-mbps=900 concurrency=12`
	const (
		cacheMapTX   = "BAD_REQUEST: cache-map must be none or send"
		cacheMapSYNC = "BAD_REQUEST: cache-map must be none, send, or recv"
		recvTX       = "UNSUPPORTED_CACHE_MAP: cache-map=recv not supported on TXFER"
		gentle       = "BAD_REQUEST: cache-map is not supported with gentle mode"
		badMode      = "BAD_REQUEST: mode must be fast or gentle"
		badLink      = "BAD_REQUEST: link-mbps must be >= 0"
		badConc      = "BAD_REQUEST: concurrency must be > 0"
		badDeadline  = "BAD_REQUEST: deadline-ms must be >= 0"
		badComp      = "UNSUPPORTED_COMP: supported comp values: none, zstd"
	)
	// Hint fields are assigned, not named in a literal, so the test compiles
	// whether they are fields of the request or promoted from an embedded one.
	base := txferRequest{Directory: "/tmp/test", CacheMap: "none"}
	base.Mode, base.LinkMbps, base.Concurrency, base.Comp = "fast", 900, 12, "zstd"
	baseSYNC := syncRequest{Directory: "/tmp/test", CacheMap: "none"}
	baseSYNC.Mode, baseSYNC.LinkMbps, baseSYNC.Concurrency, baseSYNC.Comp = "fast", 900, 12, "zstd"
	with := func(f func(*txferRequest)) *txferRequest { r := base; f(&r); return &r }
	withSYNC := func(f func(*syncRequest)) *syncRequest { r := baseSYNC; f(&r); return &r }
	cases := []struct {
		args string
		// txfer and sync hold the expected "CODE: message" error. When txferOK
		// or syncOK is set, the parse must succeed and return it.
		txfer, sync string
		txferOK     *txferRequest
		syncOK      *syncRequest
	}{
		// Accepted requests and their defaults.
		{args: hints, txferOK: &base, syncOK: &baseSYNC},
		{args: `mode=GENTLE link-mbps=0 concurrency=1 deadline-ms=5000 comp=NONE cache-map=none`,
			txferOK: with(func(r *txferRequest) {
				r.Mode, r.LinkMbps, r.Concurrency, r.DeadlineMS, r.Comp = "gentle", 0, 1, 5000, "none"
			}),
			syncOK: withSYNC(func(r *syncRequest) {
				r.Mode, r.LinkMbps, r.Concurrency, r.DeadlineMS, r.Comp = "gentle", 0, 1, 5000, "none"
			})},
		{args: hints + ` deadline-ms=0 comp=zstd`, txferOK: &base, syncOK: &baseSYNC},
		{args: hints + ` cache-map=send`,
			txferOK: with(func(r *txferRequest) { r.CacheMap, r.PageCache = "send", true }),
			syncOK:  withSYNC(func(r *syncRequest) { r.CacheMap = "send" })},
		{args: hints + ` cache-map=SEND`,
			txferOK: with(func(r *txferRequest) { r.CacheMap, r.PageCache = "send", true }),
			syncOK:  withSYNC(func(r *syncRequest) { r.CacheMap = "send" })},
		{args: hints + ` verbose=1`, txferOK: with(func(r *txferRequest) { r.Verbose = true }), sync: "BAD_REQUEST: unknown SYNC option"},
		{args: hints + ` verbose=TRUE`, txferOK: with(func(r *txferRequest) { r.Verbose = true }), sync: "BAD_REQUEST: unknown SYNC option"},
		{args: hints + ` verbose=yes`, txferOK: &base, sync: "BAD_REQUEST: unknown SYNC option"},
		{args: hints + ` cache-map=recv`, txfer: recvTX, syncOK: withSYNC(func(r *syncRequest) { r.CacheMap = "recv" })},

		// One bad value each.
		{args: hints + ` max-manifest-chunk-size=1024`, txfer: "BAD_REQUEST: unknown TXFER option", sync: "BAD_REQUEST: unknown SYNC option"},
		{args: `link-mbps=900 concurrency=12`, txfer: badMode, sync: badMode},
		{args: `mode=slow link-mbps=900 concurrency=12`, txfer: badMode, sync: badMode},
		{args: `mode=fast concurrency=12`, txfer: badLink, sync: badLink},
		{args: `mode=fast link-mbps=-1 concurrency=12`, txfer: badLink, sync: badLink},
		{args: `mode=fast link-mbps=x concurrency=12`, txfer: badLink, sync: badLink},
		{args: `mode=fast link-mbps=900`, txfer: badConc, sync: badConc},
		{args: `mode=fast link-mbps=900 concurrency=0`, txfer: badConc, sync: badConc},
		{args: `mode=fast link-mbps=900 concurrency=x`, txfer: badConc, sync: badConc},
		{args: hints + ` deadline-ms=-1`, txfer: badDeadline, sync: badDeadline},
		{args: hints + ` deadline-ms=x`, txfer: badDeadline, sync: badDeadline},
		{args: hints + ` comp=lz4`, txfer: badComp, sync: badComp},
		{args: hints + ` cache-map=1`, txfer: cacheMapTX, sync: cacheMapSYNC},
		{args: hints + ` cache-map=true`, txfer: cacheMapTX, sync: cacheMapSYNC},
		{args: hints + ` cache-map=0`, txfer: cacheMapTX, sync: cacheMapSYNC},
		{args: hints + ` cache-map=false`, txfer: cacheMapTX, sync: cacheMapSYNC},
		{args: hints + ` cache-map=garbage`, txfer: cacheMapTX, sync: cacheMapSYNC},
		{args: `mode=gentle link-mbps=900 concurrency=12 cache-map=send`, txfer: gentle, sync: gentle},

		// Two bad values: the error names the one checked first.
		{args: `mode=gentle link-mbps=900 concurrency=12 cache-map=recv`, txfer: recvTX, sync: gentle},
		{args: `mode=gentle cache-map=send link-mbps=x concurrency=4`, txfer: gentle, sync: badLink},
		{args: `cache-map=bogus mode=bad link-mbps=900 concurrency=12`, txfer: cacheMapTX, sync: badMode},
		{args: hints + ` comp=lz4 cache-map=bogus`, txfer: cacheMapTX, sync: badComp},
		{args: `mode=bad link-mbps=900 concurrency=12 bogus=1`, txfer: "BAD_REQUEST: unknown TXFER option", sync: "BAD_REQUEST: unknown SYNC option"},
		{args: `mode=fast link-mbps=x concurrency=0`, txfer: badLink, sync: badLink},
		{args: `mode=fast link-mbps=900 concurrency=0 deadline-ms=-1`, txfer: badConc, sync: badConc},
		{args: hints + ` deadline-ms=-1 comp=lz4`, txfer: badDeadline, sync: badDeadline},
	}
	for _, tc := range cases {
		t.Run(tc.args, func(t *testing.T) {
			checkParse(t, "TXFER", `"/tmp/test" `+tc.args, tc.txfer, tc.txferOK, parseTXFERRequest)
			checkParse(t, "SYNC", `"/tmp/test" `+tc.args, tc.sync, tc.syncOK, parseSYNCRequest)
		})
	}

	// A missing directory is checked after unknown keys and before mode.
	missingDir := map[string]string{"directory": " ", "mode": "bad"}
	for _, tc := range []struct {
		verb, other Verb
		want        string
		err         func(Request) error
	}{
		{VerbTXFER, VerbSYNC, "BAD_REQUEST: missing TXFER directory", func(r Request) error { _, err := parseTXFERRequest(r); return err }},
		{VerbSYNC, VerbTXFER, "BAD_REQUEST: missing SYNC directory", func(r Request) error { _, err := parseSYNCRequest(r); return err }},
	} {
		wantErr(t, tc.err(Request{Verb: tc.verb, Params: []map[string]string{missingDir}}), tc.want)
		unknown := map[string]string{"directory": " ", "bogus": "1"}
		wantErr(t, tc.err(Request{Verb: tc.verb, Params: []map[string]string{unknown}}), "BAD_REQUEST: unknown "+tc.verb.String()+" option")
		wantErr(t, tc.err(Request{Verb: tc.verb}), "BAD_REQUEST: invalid "+tc.verb.String()+" arguments")
		wantErr(t, tc.err(Request{Verb: tc.other}), "BAD_COMMAND: not "+tc.verb.String())
	}
}

func checkParse[R any](t *testing.T, verb string, args string, want string, wantOK *R, parse func(Request) (R, error)) {
	t.Helper()
	req, err := ParseRequest([]byte(verb + " " + args))
	if err != nil {
		t.Fatalf("ParseRequest(%s %s): %v", verb, args, err)
	}
	got, err := parse(req)
	if wantOK == nil {
		wantErr(t, err, want)
		return
	}
	if err != nil || !reflect.DeepEqual(got, *wantOK) {
		t.Fatalf("%s %s = %+v, %v; want %+v", verb, args, got, err, *wantOK)
	}
}

func wantErr(t *testing.T, err error, want string) {
	t.Helper()
	var pe protocolErr
	if !errors.As(err, &pe) || pe.Error() != want {
		t.Fatalf("error = %v, want protocolErr %q", err, want)
	}
}

// goldenEpoch is the base of every mtime goldenTree sets.
var goldenEpoch = time.Unix(1700000000, 0)

// setGoldenTime sets path's atime and mtime to goldenEpoch+offset without
// following a symlink. Callers set a directory's time after creating its
// children, deepest first, since creating a child changes it.
func setGoldenTime(t *testing.T, path string, offset time.Duration) {
	t.Helper()
	want := goldenEpoch.Add(offset).UnixNano()
	ts := unix.NsecToTimespec(want)
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, path, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatalf("set time on %s: %v", path, err)
	}
	if info, err := os.Lstat(path); err != nil || info.ModTime().UnixNano() != want {
		t.Skipf("filesystem does not keep nanosecond mtimes on %s (%v)", path, err)
	}
}

// goldenTree builds a tree whose manifest the test fully determines:
//
//	a.txt      5 bytes, 0644     h1.txt, h2.txt  one inode, 0640
//	empty.txt  0 bytes, 0600     link -> a.txt   symlink
//	sub/       0750              sub/b.txt       6 bytes, 0644
//
// The root is 0755. Every entry gets a distinct nanosecond mtime.
func goldenTree(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("golden manifests assume Linux modes and nanosecond mtimes")
	}
	root := t.TempDir()
	files := []struct {
		rel     string
		content string
		mode    os.FileMode
	}{
		{"a.txt", "alpha", 0o644},
		{"empty.txt", "", 0o600},
		{"h1.txt", "linked", 0o640},
		{"sub/b.txt", "bravo!", 0o644},
	}
	for _, f := range files {
		writeTestFile(t, root, f.rel, f.content)
		if err := os.Chmod(filepath.Join(root, f.rel), f.mode); err != nil {
			t.Fatalf("chmod: %v", err)
		}
	}
	if err := os.Link(filepath.Join(root, "h1.txt"), filepath.Join(root, "h2.txt")); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	if err := os.Symlink("a.txt", filepath.Join(root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	for _, d := range []struct {
		rel  string
		mode os.FileMode
	}{{"sub", 0o750}, {".", 0o755}} {
		if err := os.Chmod(filepath.Join(root, d.rel), d.mode); err != nil {
			t.Fatalf("chmod: %v", err)
		}
	}
	for i, rel := range []string{"a.txt", "empty.txt", "h1.txt", "link", "sub/b.txt", "sub", "."} {
		setGoldenTime(t, filepath.Join(root, rel), time.Duration(i+1)*(time.Second+111*time.Nanosecond))
	}
	return root
}

// goldenText replaces dir's length-prefixed path token with $ROOT, so a
// manifest from a t.TempDir() tree compares to a fixed string.
func goldenText(raw string, dir string) string {
	return strings.ReplaceAll(raw, fmt.Sprintf("%d:%s", len(dir), dir), "$ROOT")
}

// goldenTXFER is the TXFER manifest of goldenTree with path and mtime prefix
// compression. A no-change SYNC emits the same manifest.
const goldenTXFER = `FM/1 tx123 mode=fast link-mbps=1000 concurrency=8
D0 0 0:1700000007000000777 0755 0:$ROOT
F1 5 9:1000000111 0644 0:5:a.txt
F2 0 9:2000000222 0600 0:9:empty.txt
F3 6 9:3000000333 0640 0:6:h1.txt
H4 0 0:3 0640 1:5:2.txt
S5 0 0:1700000004000000444 0777 0:4:link 5:a.txt
D6 0 9:6000000666 0750 0:3:sub
F7 6 9:5000000555 0644 3:6:/b.txt
`

func TestHandleTXFERGoldenManifest(t *testing.T) {
	for _, tc := range []struct {
		name, target, extra, want string
	}{
		{"compressed", "", "", goldenTXFER},
		{"verbose", "", "verbose=1", `FM/1 tx123 mode=fast link-mbps=1000 concurrency=8
D0 0 0:1700000007000000777 0755 0:$ROOT
F1 5 0:1700000001000000111 0644 0:5:a.txt
F2 0 0:1700000002000000222 0600 0:9:empty.txt
F3 6 0:1700000003000000333 0640 0:6:h1.txt
H4 0 0:3 0640 0:6:h2.txt
S5 0 0:1700000004000000444 0777 0:4:link 5:a.txt
D6 0 0:1700000006000000666 0750 0:3:sub
F7 6 0:1700000005000000555 0644 0:9:sub/b.txt
`},
		{"single-file", "sub/b.txt", "deadline-ms=5000", `FM/1 tx123 mode=fast link-mbps=1000 concurrency=8 deadline-ms=5000
D0 0 0:1700000006000000666 0750 0:$ROOT
F1 6 9:5000000555 0644 0:5:b.txt
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := goldenTree(t)
			deps := &mockDeps{}
			raw, _ := runTXFER(t, deps, filepath.Join(root, tc.target), tc.extra)
			dir := root
			if tc.target != "" {
				dir = filepath.Dir(filepath.Join(root, tc.target))
			}
			if got := goldenText(raw, dir); got != tc.want {
				t.Fatalf("manifest mismatch\ngot:\n%s\nwant:\n%s", got, tc.want)
			}
			// TXFER leaves completion to ACK; only SYNC checks it.
			if deps.completeCalls != 0 {
				t.Fatalf("TXFER called MaybeLogTransferComplete %d times", deps.completeCalls)
			}
		})
	}
}

// TestTXFERAndSYNCStoreState checks what TXFER and a no-change SYNC record in
// a real store: counts, the deadline, each entry's type and path hash, a
// page-cache blob for a resident file, and the SYNC's auto-ACKs completing
// the transfer.
func TestTXFERAndSYNCStoreState(t *testing.T) {
	for _, tc := range []struct {
		name, target, dir string   // target and the transfer's directory, under root
		paths             []string // entry paths by file ID, relative to dir
		types             []byte
		files             int
		size              int64
	}{
		{"directory", "", "", []string{"", "a.txt", "empty.txt", "h1.txt", "h2.txt", "link", "sub", "sub/b.txt"},
			[]byte("DFFFHSDF"), 4, 17},
		{"single-file", "sub/b.txt", "sub", []string{"", "b.txt"}, []byte("DF"), 1, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := goldenTree(t)
			dir := filepath.Join(root, tc.dir)
			// File ID 1 is a.txt or b.txt. Read it back so its pages are
			// resident for cache-map=send.
			warm := filepath.Join(dir, tc.paths[1])
			if _, err := os.ReadFile(warm); err != nil {
				t.Fatalf("read: %v", err)
			}
			deps := realDeps(t, "/")
			checkTransfer := func(t *testing.T, id string) Transfer {
				t.Helper()
				sum, ok := deps.GetTransferSummary(id)
				if !ok {
					t.Fatalf("transfer %q missing", id)
				}
				if sum.NumEntries != len(tc.paths)-1 || sum.NumFiles != tc.files || sum.TotalSize != tc.size ||
					sum.Directory != dir || sum.DeadlineMS != 5000 {
					t.Fatalf("summary: entries=%d files=%d size=%d dir=%q deadline=%d",
						sum.NumEntries, sum.NumFiles, sum.TotalSize, sum.Directory, sum.DeadlineMS)
				}
				got, _ := deps.GetTransfer(id)
				if len(got.PathHash) != len(tc.paths) {
					t.Fatalf("PathHash has %d entries, want %d", len(got.PathHash), len(tc.paths))
				}
				for i, rel := range tc.paths {
					full := filepath.Clean(filepath.Join(dir, rel))
					if got.PathHash[i] != xxh3.Hash128([]byte(full)) {
						t.Errorf("PathHash[%d] is not the hash of %q", i, full)
					}
					if got.EntryType[i] != tc.types[i] {
						t.Errorf("EntryType[%d] = %c, want %c", i, got.EntryType[i], tc.types[i])
					}
				}
				return got
			}

			raw, txferID := runTXFER(t, deps, filepath.Join(root, tc.target), "cache-map=send deadline-ms=5000")
			got := checkTransfer(t, txferID)
			if got.StateCounts[TransferStateStarted] != tc.files || got.Done != 0 {
				t.Fatalf("TXFER states: started=%d done=%d", got.StateCounts[TransferStateStarted], got.Done)
			}
			if len(got.PageCache) < 2 || len(got.PageCache[1]) == 0 {
				var ce pagecache.CacheEntry
				if err := ce.Load(warm); err == nil && !ce.Empty() {
					t.Fatalf("no page-cache blob recorded for resident %s", warm)
				}
				t.Logf("%s reports no resident pages; skipping the page-cache check", warm)
			}
			if tc.target != "" {
				return // SYNC takes only a directory.
			}
			if len(got.PageCache) > 2 && got.PageCache[2] != nil {
				t.Fatalf("page-cache blob recorded for empty.txt")
			}

			_, syncID := runSYNC(t, deps, root, raw, "none", "cache-map=send deadline-ms=5000")
			got = checkTransfer(t, syncID)
			if got.Done != 4 || got.StateCounts[TransferStateDone] != 4 || !got.CompleteLogged {
				t.Fatalf("SYNC states: done=%d doneCount=%d complete=%v", got.Done, got.StateCounts[TransferStateDone], got.CompleteLogged)
			}
			// SYNC sends cache-map=send blobs on the wire but records none.
			for i, blob := range got.PageCache {
				if len(blob) != 0 {
					t.Fatalf("SYNC recorded a page-cache blob for file %d", i)
				}
			}
		})
	}
}

// failWriter fails every write with err.
type failWriter struct{ err error }

func (w failWriter) Write([]byte) (int, error) { return 0, w.err }

// TestTXFERAndSYNCDeleteTransferOnWriteFailure checks that a manifest that
// cannot be written leaves no transfer behind. A closed pipe is the client
// going away and returns nil; any other error is INTERNAL.
func TestTXFERAndSYNCDeleteTransferOnWriteFailure(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	boom := errors.New("boom")
	handlers := map[string]func(Deps, io.Writer) error{
		"TXFER": func(deps Deps, out io.Writer) error {
			req, err := ParseRequest([]byte(fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none`, root)))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			return handleTXFER(context.Background(), req, out, deps)
		},
		"SYNC": func(deps Deps, out io.Writer) error {
			req, err := ParseRequest([]byte(fmt.Sprintf(`SYNC %q mode=fast link-mbps=1000 concurrency=8 comp=none`, root)))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			var body bytes.Buffer
			if err := encoding.NewFramedBodyWriter(&body, "none", encoding.DefaultBodyChunkSize, 0).Close(); err != nil {
				t.Fatalf("frame empty manifest: %v", err)
			}
			return handleSYNCWithInput(context.Background(), req, &body, out, deps, nil, defaultMaxSyncBodyBytes)
		},
	}
	for verb, run := range handlers {
		for _, tc := range []struct {
			err  error
			want string // "" for a nil return
		}{
			{boom, "INTERNAL: flush manifest frames: boom"},
			{io.ErrClosedPipe, ""},
		} {
			t.Run(verb+"/"+tc.err.Error(), func(t *testing.T) {
				deps := realDeps(t, "/")
				err := run(deps, failWriter{tc.err})
				if tc.want == "" {
					if err != nil {
						t.Fatalf("closed pipe returned %v, want nil", err)
					}
				} else {
					wantErr(t, err, tc.want)
				}
				if n := len(deps.ListTransfers()); n != 0 {
					t.Fatalf("failed %s left %d transfers", verb, n)
				}
			})
		}
	}
}
