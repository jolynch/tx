package tx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/zeebo/xxh3"
)

func testLimits(target, maxBytes int64) clientRequestLimits {
	return clientRequestLimits{target: target, max: maxBytes, maxSync: 1 << 30}
}

func TestRequestBatchBudgets(t *testing.T) {
	for _, limits := range []clientRequestLimits{testLimits(10, 20), testLimits(10, 10)} {
		encoded, visited, next := 0, 0, 0
		var seen [7]int
		err := visitEncodedRequests([]int{0, 1, 2, 3, 4, 5, 6}, limits, func(id int) ([]byte, error) {
			encoded++
			seen[id]++
			// Only one body plus at most one lookahead item may precede sending.
			if encoded > next+3 {
				t.Fatal("encoded later requests before sending")
			}
			return []byte("123456"), nil
		}, func(chunk requestChunk) error {
			if chunk.lo != next || chunk.hi <= next || int64(len(chunk.body)) > limits.max {
				t.Fatalf("invalid chunk: %+v", chunk)
			}
			if len(chunk.body) != 6*(chunk.hi-chunk.lo) {
				t.Fatal("body does not match targets")
			}
			next, visited = chunk.hi, visited+1
			return nil
		})
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("item %d encoded %d times", id, n)
			}
		}
		if err != nil || next != 7 || visited < 2 {
			t.Fatalf("next=%d visits=%d err=%v", next, visited, err)
		}
	}
	if _, err := encodeRequest([]int{0, 1}, 11, func(int) ([]byte, error) { return []byte("123456"), nil }); err == nil {
		t.Fatal("single request exceeded its maximum")
	}
}

// End-to-end through a real in-process server: a metadata request covering more
// items than one body may carry must split, preserve order, and return the same
// results the unsplit path would. The count assertion is what proves splitting
// actually happened rather than one oversized request slipping through.
func TestGetEntryMetadataSplitsAndPreservesOrder(t *testing.T) {
	const files = 30
	dir := t.TempDir()
	paths := make(map[uint64]string, files)
	for i := 0; i < files; i++ {
		sub := filepath.Join(dir, fmt.Sprintf("dir-%04d", i))

		paths[uint64(i+1)] = sub
	}

	emptyHash := intencoding.FormatXXH128HashToken(xxh3.Hash128(nil))
	var mu sync.Mutex
	var sendRequests int
	var itemsSeen []uint64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbSEND {
			return fmt.Errorf("unexpected verb %v", req.Verb)
		}
		mu.Lock()
		sendRequests++
		for _, item := range req.Params[1:] {
			var id uint64
			fmt.Sscanf(item["fid"], "%d", &id)
			itemsSeen = append(itemsSeen, id)
		}
		mu.Unlock()
		for _, item := range req.Params[1:] {
			var id uint64
			fmt.Sscanf(item["fid"], "%d", &id)
			if _, err := fmt.Fprintf(out,
				"FX/1 %d offset=0 size=0 wsize=0 comp=none ts=1\n"+
					"FXT/1 %d status=ok ts=1 file-hash=%s next=0 meta:size=0 meta:mtime_ns=1 "+
					"meta:mode=0755 meta:uid=0 meta:gid=0 meta:user=u meta:group=g\n",
				id, id, emptyHash); err != nil {
				return err
			}
		}
		_, err := io.WriteString(out, "OK\r\n")
		return err
	})
	defer srv.Close()

	c := NewClient(srv.URL)
	defer c.Close()
	// A small target forces several requests for this many items.
	c.cacheRequestLimits(128, 512, 1<<30)

	results, err := c.GetEntryMetadata(context.Background(), "tx1", paths)
	if err != nil {
		t.Fatalf("GetEntryMetadata: %v", err)
	}
	if len(results) != files {
		t.Fatalf("got %d results, want %d", len(results), files)
	}
	mu.Lock()
	defer mu.Unlock()
	if sendRequests < 2 {
		t.Fatalf("expected the request to split, saw %d SEND request(s)", sendRequests)
	}
	if len(itemsSeen) != files {
		t.Fatalf("server saw %d items, want %d", len(itemsSeen), files)
	}
	for i := 1; i < len(itemsSeen); i++ {
		if itemsSeen[i] <= itemsSeen[i-1] {
			t.Fatalf("item order broken across requests at %d: %d then %d", i, itemsSeen[i-1], itemsSeen[i])
		}
	}
}

// SYNC is never split, so an oversized prior manifest must be refused before it
// is transmitted. Asserting the handler never ran is the point: an error alone
// would also be satisfied by sending it and having the server reject it.
func TestSyncManifestPreflightRefusesBeforeSending(t *testing.T) {
	var reached atomic.Bool
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSYNC {
			reached.Store(true)
		}
		return fmt.Errorf("handler should not have run")
	})
	defer srv.Close()

	c := NewClient(srv.URL)
	defer c.Close()
	c.cacheRequestLimits(4*1024, 64*1024, 2048) // tiny SYNC allowance

	manifest := &Manifest{TransferID: "tx1", Root: "/remote", Mode: LoadStrategyFast, Concurrency: 1}
	for i := 0; i < 500; i++ {
		manifest.Entries = append(manifest.Entries, ManifestEntry{
			Type: intencoding.EntryTypeFile, ID: uint64(i + 1), Size: 1, Mtime: 1, Mode: 0o644,
			Path: fmt.Sprintf("padded/path/to/file-%06d.bin", i), LinkTarget: -1,
		})
	}
	_, err := c.SyncManifest(context.Background(), SyncManifestRequest{
		Directory: "/remote", Mode: LoadStrategyFast, Concurrency: 1, OldManifest: manifest,
	})
	if err == nil {
		t.Fatal("an oversized prior manifest was sent")
	}
	if !strings.Contains(err.Error(), "SYNC limit") {
		t.Fatalf("expected the error to name the SYNC limit, got %v", err)
	}
	if reached.Load() {
		t.Fatal("the manifest was transmitted; the preflight check did not run first")
	}
}

// The framed request body travels inside the per-command AEAD stream, so
// splitting must behave identically under encryption. Runs against a real
// server rather than a stub: the point is that several encrypted request/
// response cycles work in sequence on the same client.
func TestGetEntryMetadataSplitsUnderEncryption(t *testing.T) {
	const dirs = 12
	root := t.TempDir()
	for i := 0; i < dirs; i++ {
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("dir-%04d", i)), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	addr := startRealKeepAliveServer(t, intftcp.ServerOptions{
		ServerIdentity:   id,
		RootDir:          "/",
		KeepAliveTimeout: 5 * time.Second,
	})

	c := &Client{FileAddr: addr, EncryptMode: "auto"}
	defer c.Close()
	ctx := context.Background()

	manifest, err := c.GetManifest(ctx, GetManifestRequest{
		Directory: root, Mode: LoadStrategyFast, Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}

	paths := make(map[uint64]string)
	for _, e := range manifest.Manifest.Entries {
		if e.Type == intencoding.EntryTypeDir {
			paths[e.ID] = filepath.Join(root, filepath.FromSlash(e.Path))
		}
	}
	if len(paths) < dirs {
		t.Fatalf("manifest listed %d directories, want at least %d", len(paths), dirs)
	}

	// Force several requests. The unsplit body for this many items is well
	// above this target.
	c.cacheRequestLimits(128, 512, 1<<30)

	results, err := c.GetEntryMetadata(ctx, manifest.Manifest.TransferID, paths)
	if err != nil {
		t.Fatalf("GetEntryMetadata over AEAD: %v", err)
	}
	if len(results) != len(paths) {
		t.Fatalf("got %d results, want %d", len(results), len(paths))
	}
	for id := range paths {
		if results[id] == nil {
			t.Fatalf("missing metadata for id %d", id)
		}
	}
}

func TestProbeRequestLimitValidation(t *testing.T) {
	base := "PROBE cpu=1 cts0=0 sts0=0 sts1=0 probe-bytes=1 "
	defaults := defaultClientRequestLimits()
	if defaults.target != 8<<20 || defaults.max != 64<<20 || defaults.maxSync != 1<<30 {
		t.Fatalf("defaults: %+v", defaults)
	}
	for _, tc := range []struct {
		fields string
		want   clientRequestLimits
	}{
		{"", defaults},
		{"max-request-bytes=100", testLimits(100, 100)},
		{"target-request-bytes=999999999 max-request-bytes=999999999", testLimits(64<<20, 64<<20)},
	} {
		p, err := parseProbeResponseLine(base + tc.fields)
		if err != nil {
			t.Fatal(err)
		}
		if p.TargetRequestBytes != tc.want.target || p.MaxRequestBytes != tc.want.max || p.MaxSyncRequestBytes != tc.want.maxSync {
			t.Fatalf("fields=%q response=%+v", tc.fields, p)
		}
	}
	for _, key := range []string{"target-request-bytes", "max-request-bytes", "max-sync-request-bytes"} {
		for _, value := range []string{"", "0", "-1", "garbage", "9223372036854775808"} {
			if _, err := parseProbeResponseLine(base + key + "=" + value); err == nil {
				t.Fatalf("accepted %s=%s", key, value)
			}
		}
	}
}

func TestSENDBodyBudgetBeforeDial(t *testing.T) {
	var dials int
	dialErr := errors.New("reached dial")
	c := NewClient("unused:1", WithContextDialer(func(context.Context, string) (net.Conn, error) { dials++; return nil, dialErr }))
	defer c.Close()
	body := []byte("123456")
	for _, maximum := range []int64{0, -1, 5, intencoding.MaxRequestBytes + 1} {
		_, err := c.fetchFileBatchBodyTCP(context.Background(), "tid", encodedRequest{body: body, maxBytes: maximum})
		if err == nil || dials != 0 {
			t.Fatalf("max=%d err=%v dials=%d", maximum, err, dials)
		}
	}
	request := encodedRequest{body: body, maxBytes: 6}
	c.cacheRequestLimits(1, 1, 1<<30)
	_, err := c.fetchFileBatchBodyTCP(context.Background(), "tid", request)
	if !errors.Is(err, dialErr) || dials != 1 {
		t.Fatalf("captured budget changed: err=%v dials=%d", err, dials)
	}
}

func TestChecksumBatchResponseLifecycle(t *testing.T) {
	for _, timeout := range []time.Duration{0, 50 * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			var requests atomic.Int64
			srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
				if req.Verb != intftcp.VerbCXSUM {
					return fmt.Errorf("unexpected verb %v", req.Verb)
				}
				requests.Add(1)
				_, err := io.WriteString(out, "FX/1 1 offset=0 size=0 wsize=0 comp=none ts=1\n")
				if timeout > 0 {
					_, _ = io.Copy(io.Discard, out.(io.Reader))
				}
				return err
			})
			defer srv.Close()
			var conn *closeCountConn
			c := NewClient(srv.URL, WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if err != nil {
					return nil, err
				}
				conn = &closeCountConn{Conn: raw}
				return conn, nil
			}))
			defer c.Close()
			stop := errors.New("stop consuming")
			calls := 0
			err := c.VisitChecksumBatches(context.Background(), GetChecksumRequest{TransferID: "tid", Targets: []ChecksumTarget{{FileID: 1, FullPath: "/a"}, {FileID: 2, FullPath: "/b"}}}, ChecksumBatchOptions{TargetRequestBytes: 1, RequestTimeout: timeout}, func(targets []ChecksumTarget, response GetChecksumResponse) error {
				calls++
				if len(targets) != 1 || targets[0].FileID != 1 {
					t.Fatalf("unexpected batch: %+v", targets)
				}
				if timeout > 0 {
					_, err := io.Copy(io.Discard, response.Reader)
					return err
				}
				return stop
			})
			if err == nil || (timeout == 0 && !errors.Is(err, stop)) || calls != 1 || requests.Load() != 1 || conn == nil || conn.closes.Load() == 0 {
				t.Fatalf("err=%v calls=%d requests=%d conn=%v", err, calls, requests.Load(), conn)
			}
		})
	}
}
