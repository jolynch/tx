package filexfercli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/aead"
	intfilexfer "github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/fsync"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/zeebo/xxh3"
)

type ftcpTestServer struct {
	URL      string
	listener net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
}

func (s *ftcpTestServer) Close() {
	if s == nil {
		return
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.Lock()
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func newFTCPTestServer(t *testing.T, handler func(intftcp.Request, io.Writer) error) *ftcpTestServer {
	return newFTCPTestServerWithIdentity(t, nil, handler)
}

func newFTCPTestServerWithIdentity(t *testing.T, serverID *age.X25519Identity, handler func(intftcp.Request, io.Writer) error) *ftcpTestServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	s := &ftcpTestServer{URL: ln.Addr().String(), listener: ln, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer func() {
					_ = c.Close()
					s.mu.Lock()
					delete(s.conns, c)
					s.mu.Unlock()
				}()
				serveFTCPConn(c, serverID, handler)
			}(conn)
		}
	}()
	return s
}

func serveFTCPConn(conn net.Conn, serverID *age.X25519Identity, handler func(intftcp.Request, io.Writer) error) {
	br := bufio.NewReader(conn)
	firstLine, err := readFTCPLine(br)
	if err != nil {
		return
	}
	req, err := intftcp.ParseRequest([]byte(firstLine))
	if err != nil {
		_, _ = io.WriteString(conn, "ERR BAD_REQUEST "+err.Error()+"\r\n")
		return
	}

	out := io.Writer(conn)
	closeOut := func() error { return nil }
	cmdReq := req
	if req.Verb == intftcp.VerbAUTH {
		if len(req.Params) == 0 {
			_, _ = io.WriteString(conn, "ERR BAD_AUTH missing protocol\r\n")
			return
		}
		protocol := req.Params[0]["protocol"]

		if protocol == "key" {
			// Key exchange: return recommended cipher and server public key.
			if serverID == nil {
				_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED no server identity\r\n")
				return
			}
			_, _ = io.WriteString(conn, "OK "+string(aead.RecommendedCipher())+" "+serverID.Recipient().String()+"\r\n")
			return
		}

		// aes/chacha20: decode and decrypt the blob to get the client's public key.
		blobRaw := req.Params[0]["blob"]
		if blobRaw == "" || serverID == nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		opts := aead.Options{}
		switch protocol {
		case "aes":
			opts.Algorithm = aead.AlgorithmAES
		case "chacha20":
			opts.Algorithm = aead.AlgorithmChaCha20
		default:
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		blobBytes, b64Err := base64.StdEncoding.DecodeString(strings.TrimSpace(blobRaw))
		if b64Err != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED bad base64\r\n")
			return
		}
		dec, decErr := aead.DecryptWithOptions(bytes.NewReader(blobBytes), serverID, opts)
		if decErr != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		plain, readErr := io.ReadAll(dec)
		if readErr != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		fields := strings.Fields(string(plain))
		if len(fields) == 0 {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		recipient, parseErr := age.ParseX25519Recipient(fields[0])
		if parseErr != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}

		// Encrypt responses to client.
		ew, encErr := aead.Encrypt(conn, recipient, opts)
		if encErr != nil {
			return
		}
		out = ew
		closeOut = ew.Close

		// Decrypt the command from client.
		cmdReader, cmdDecErr := aead.DecryptWithOptions(br, serverID, opts)
		if cmdDecErr != nil {
			_, _ = io.WriteString(out, "ERR NOT_AUTHORIZED request decryption failed\r\n")
			_ = closeOut()
			return
		}
		br = bufio.NewReader(cmdReader)

		cmdLine, cmdErr := readFTCPLine(br)
		if cmdErr != nil {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST missing command\r\n")
			_ = closeOut()
			return
		}
		cmdReq, err = intftcp.ParseRequest([]byte(cmdLine))
		if err != nil {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST "+err.Error()+"\r\n")
			_ = closeOut()
			return
		}
	}
	if cmdReq.Verb == intftcp.VerbPROBE && len(cmdReq.Params) > 0 {
		n, convErr := strconv.ParseInt(strings.TrimSpace(cmdReq.Params[0]["probe-bytes"]), 10, 64)
		if convErr != nil || n < 0 {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST invalid probe-bytes\r\n")
			_ = closeOut()
			return
		}
		if n > 0 {
			if _, drainErr := io.CopyN(io.Discard, br, n); drainErr != nil {
				_, _ = io.WriteString(out, "ERR BAD_REQUEST invalid probe payload\r\n")
				_ = closeOut()
				return
			}
		}
	}
	if cmdReq.Verb == intftcp.VerbSYNC {
		mr := encoding.NewChunkedManifestReader(br, encoding.ChunkedManifestReaderOpts{})
		if _, drainErr := io.Copy(io.Discard, mr); drainErr != nil {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST invalid sync manifest\r\n")
			_ = closeOut()
			return
		}
	}
	if handled, handleErr := maybeWriteCLIRootMetadataOnlySEND(cmdReq, out); handled {
		if handleErr != nil {
			_, _ = io.WriteString(out, "ERR INTERNAL "+handleErr.Error()+"\r\n")
		}
		_ = closeOut()
		return
	}

	if err := handler(cmdReq, out); err != nil {
		_, _ = io.WriteString(out, "ERR INTERNAL "+err.Error()+"\r\n")
	}
	_ = closeOut()
}

func maybeWriteCLIRootMetadataOnlySEND(req intftcp.Request, out io.Writer) (bool, error) {
	if req.Verb != intftcp.VerbSEND || len(req.Params) != 2 {
		return false, nil
	}
	if req.Params[1]["fid"] != strconv.FormatUint(tx.RootFileID, 10) {
		return false, nil
	}
	if err := writeCLIMetadataFrame(out, tx.RootFileID, 100, "0755"); err != nil {
		return true, err
	}
	_, err := io.WriteString(out, "OK\r\n")
	return true, err
}

func readFTCPLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func xxh128HexCLI(data []byte) string {
	h := xxh3.Hash128(data).Bytes()
	return hex.EncodeToString(h[:])
}

func buildCLIFrame(fileID uint64, body []byte, offset int64) string {
	return buildCLIFrameWithMetadata(fileID, body, offset, nil)
}

func testOwnershipMetadata(size int64, mtimeNS int64, mode string) *tx.FileTrailerMetadata {
	return &tx.FileTrailerMetadata{
		Size:    size,
		MtimeNS: mtimeNS,
		Mode:    mode,
		UID:     strconv.Itoa(os.Getuid()),
		GID:     strconv.Itoa(os.Getgid()),
	}
}

func writeCLIMetadataFrame(out io.Writer, fileID uint64, mtimeNS int64, mode string) error {
	_, err := io.WriteString(out, buildCLIFrameWithMetadata(fileID, nil, 0, testOwnershipMetadata(0, mtimeNS, mode)))
	return err
}

func buildCLIFrameWithMetadata(fileID uint64, body []byte, offset int64, meta *tx.FileTrailerMetadata) string {
	xsum := xxh128HexCLI(body)
	header := fmt.Sprintf(
		"FX/1 %d offset=%d size=%d wsize=%d comp=none hash=xxh128:%s ts=1000\n",
		fileID,
		offset,
		len(body),
		len(body),
		xsum,
	)
	trailerParts := []string{
		fmt.Sprintf("FXT/1 %d", fileID),
		"status=ok",
		"ts=1001",
		"next=0",
		fmt.Sprintf("file-hash=xxh128:%s", xsum),
	}
	if meta != nil {
		if meta.Size > 0 {
			trailerParts = append(trailerParts, fmt.Sprintf("meta:size=%d", meta.Size))
		}
		if meta.MtimeNS > 0 {
			trailerParts = append(trailerParts, fmt.Sprintf("meta:mtime_ns=%d", meta.MtimeNS))
		}
		if meta.Mode != "" {
			trailerParts = append(trailerParts, "meta:mode="+meta.Mode)
		}
		if meta.UID != "" {
			trailerParts = append(trailerParts, "meta:uid="+meta.UID)
		}
		if meta.GID != "" {
			trailerParts = append(trailerParts, "meta:gid="+meta.GID)
		}
	}
	trailerPrefix := strings.Join(trailerParts, " ")
	h := xxh3.New()
	_, _ = h.Write([]byte(header))
	_, _ = h.Write(body)
	_, _ = h.Write([]byte(trailerPrefix))
	return fmt.Sprintf("%s%s%s hash=xxh64:%016x\n", header, string(body), trailerPrefix, h.Sum64())
}

func withVerifyBudgetGracePeriod(t *testing.T, d time.Duration) {
	t.Helper()
	prev := verifyBudgetGracePeriod
	verifyBudgetGracePeriod = d
	t.Cleanup(func() {
		verifyBudgetGracePeriod = prev
	})
}

func writeChecksumFrame(out io.Writer, fileID uint64, offset int64, size int64, hash string) error {
	_, err := encoding.WriteFrame(out, encoding.WriteArgs{
		FileID:     fileID,
		Offset:     offset,
		Size:       size,
		WSize:      0,
		Comp:       "none",
		HeaderHash: hash,
		HeaderTS:   1000,
		TrailerTS:  1001,
		FileHashes: []string{hash},
		Next:       0,
	})
	return err
}

func checksumTokenForRange(data []byte, offset int64, size int64) string {
	return encoding.FormatXXH128HashToken(xxh3.Hash128(data[offset : offset+size]))
}

func parseOptionalInt64(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}

func checksumTargetsFromRequest(t *testing.T, req intftcp.Request) []tx.ChecksumTarget {
	t.Helper()
	targets := make([]tx.ChecksumTarget, 0, len(req.Params)-1)
	for _, item := range req.Params[1:] {
		fileID, err := strconv.ParseUint(item["fid"], 10, 64)
		if err != nil {
			t.Fatalf("parse fid: %v", err)
		}
		offset, err := parseOptionalInt64(item["offset"])
		if err != nil {
			t.Fatalf("parse offset: %v", err)
		}
		size, err := parseOptionalInt64(item["size"])
		if err != nil {
			t.Fatalf("parse size: %v", err)
		}
		targets = append(targets, tx.ChecksumTarget{
			FileID:   fileID,
			FullPath: item["path"],
			Offset:   offset,
			Size:     size,
			Algo:     item["algo"],
		})
	}
	return targets
}

// setupPinchState creates the .tx directory structure for tests and writes
// a manifest (and optional progress) file. Returns the target directory path.
func setupPinchState(t *testing.T, tmp string, manifestRaw string, progressRaw string) string {
	t.Helper()
	targetDir := filepath.Join(tmp, "dst")
	pinchDir := filepath.Join(tmp, ".tx", "dst")
	if err := os.MkdirAll(pinchDir, 0o755); err != nil {
		t.Fatalf("mkdir .tx: %v", err)
	}
	serverManifestPath := filepath.Join(pinchDir, "manifest.server.zst")
	if manifestRaw != "" {
		// Parse and re-save via SaveManifest so the on-disk format matches
		// what production writes (zstd-compressed FM/1).
		m, err := tx.ParseManifest([]byte(manifestRaw))
		if err != nil {
			t.Fatalf("parse seed manifest: %v", err)
		}
		if err := tx.SaveManifest(serverManifestPath, m); err != nil {
			t.Fatalf("save manifest.server.zst: %v", err)
		}
	}
	if progressRaw != "" {
		// If the test seeded a non-fingerprinted progress body and a manifest
		// is present, automatically prepend the correct fingerprint header so
		// the runStart/runResumeRefresh validation accepts the progress.
		body := progressRaw
		if manifestRaw != "" && !strings.Contains(progressRaw, progressFingerprintHeaderPrefix) {
			m, err := tx.LoadManifest(serverManifestPath)
			if err != nil {
				t.Fatalf("load seeded manifest: %v", err)
			}
			body = fmt.Sprintf("%s%s\n%s", progressFingerprintHeaderPrefix, tx.ManifestFingerprint(m), progressRaw)
		}
		if err := os.WriteFile(filepath.Join(pinchDir, "manifest.progress"), []byte(body), 0o644); err != nil {
			t.Fatalf("write progress: %v", err)
		}
	}
	return targetDir
}

func withSyncPromptTestInput(t *testing.T, input string, isTerminal bool) {
	t.Helper()
	prevInput := syncPromptInput
	prevIsTerminal := syncPromptIsTerminal
	syncPromptInput = strings.NewReader(input)
	syncPromptIsTerminal = func() bool { return isTerminal }
	t.Cleanup(func() {
		syncPromptInput = prevInput
		syncPromptIsTerminal = prevIsTerminal
	})
}

func writeCLIProbeResponse(req intftcp.Request, out io.Writer) error {
	cts0 := req.Params[0]["cts0"]
	n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
	if err != nil || n < 0 {
		return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
	}
	if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
		return err
	}
	if n > 0 {
		if _, err := out.Write(make([]byte, n)); err != nil {
			return err
		}
	}
	_, err = io.WriteString(out, "OK\r\n")
	return err
}

func buildTestManifestRaw(transferID string, entries []string) string {
	root := "/remote"
	lines := []string{
		fmt.Sprintf("FM/1 %s mode=fast link-mbps=1000 concurrency=1", transferID),
		fmt.Sprintf("D0 0 0:100 0755 0:%d:%s", len(root), root),
	}
	lines = append(lines, entries...)
	lines = append(lines, "")
	return strings.Join(lines, "\n")
}

func buildTestManifestEntry(id uint64, size int64, mtime int64, mode os.FileMode, path string) string {
	return fmt.Sprintf("F%d %d 0:%d %s 0:%d:%s", id, size, mtime, encoding.FormatManifestMode(mode), len(path), path)
}

func buildTestDirManifestEntry(id uint64, mtime int64, mode os.FileMode, path string) string {
	return fmt.Sprintf("D%d 0 0:%d %s 0:%d:%s", id, mtime, encoding.FormatManifestMode(mode), len(path), path)
}

func buildTestHardlinkManifestEntry(id uint64, targetID uint64, mode os.FileMode, path string) string {
	return fmt.Sprintf("H%d 0 0:%d %s 0:%d:%s", id, targetID, encoding.FormatManifestMode(mode), len(path), path)
}

func buildTestSymlinkManifestEntry(id uint64, mtime int64, mode os.FileMode, path string, target string) string {
	return fmt.Sprintf("S%d 0 0:%d %s 0:%d:%s %d:%s", id, mtime, encoding.FormatManifestMode(mode), len(path), path, len(target), target)
}

func buildTestManifestEntryFromDisk(t *testing.T, fullPath string, relPath string, id uint64) string {
	t.Helper()
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("stat %s: %v", fullPath, err)
	}
	return buildTestManifestEntry(id, info.Size(), info.ModTime().UnixNano(), info.Mode(), relPath)
}

// writeManifestResponse emits a raw FM/1 manifest body wrapped in
// FX/1+FXT/1 streaming frames (file_id=0), matching the live TXFER
// wire format. Used by fake TXFER servers in tests.
func writeManifestResponse(out io.Writer, manifestRaw string) error {
	cw := encoding.NewChunkedManifestWriter(out, encoding.EncodingZstd, 32, 0)
	if _, err := io.WriteString(cw, manifestRaw); err != nil {
		return err
	}
	return cw.Close()
}

func writeSyncResponse(out io.Writer, transferID string, entries []string, removedIDs []uint64) error {
	cw := encoding.NewChunkedManifestWriter(out, encoding.EncodingZstd, 32, 0)
	if _, err := io.WriteString(cw, buildTestManifestRaw(transferID, entries)); err != nil {
		return err
	}
	for _, id := range removedIDs {
		if _, err := io.WriteString(cw, fmt.Sprintf("RM %d\n", id)); err != nil {
			return err
		}
	}
	if err := cw.Close(); err != nil {
		return err
	}
	_, err := io.WriteString(out, "OK\r\n")
	return err
}

func newUnexpectedVerbFTCPTestServer(t *testing.T) *ftcpTestServer {
	t.Helper()
	return newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
}

func TestRunCLITransferAndGet(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := strings.Join([]string{
		"FM/1 txcli mode=fast link-mbps=1000 concurrency=8",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	fileBody := []byte("hello")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			cts0 := req.Params[0]["cts0"]
			n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
			if err != nil || n < 0 {
				return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
			}
			if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
				return err
			}
			if n > 0 {
				if _, err := out.Write(make([]byte, n)); err != nil {
					return err
				}
			}
			_, err = io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbTXFER:
			dir := req.Params[0]["directory"]
			switch dir {
			case "/remote":
				if got := req.Params[0]["mode"]; got != tx.LoadStrategyFast {
					return fmt.Errorf("unexpected mode: %q", got)
				}
				if err := writeManifestResponse(out, manifestRaw); err != nil {
					return err
				}
			case "/remote/a.txt":
				singleManifest := "FM/1 txget mode=fast link-mbps=0 concurrency=8\nD0 0 0:100 0755 0:7:/remote\nF1 5 0:100 0644 0:5:a.txt\n"
				if err := writeManifestResponse(out, singleManifest); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected directory: %q", dir)
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrame(1, fileBody, 0))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	serverManifestPath := filepath.Join(tmp, ".tx", "dst", "manifest.server.zst")
	code := runTransferCLI(srv.URL, []string{"-s", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(serverManifestPath); err != nil {
		t.Fatalf("manifest.server.zst not written: %v", err)
	}
	serverManifestRaw, err := os.ReadFile(serverManifestPath)
	if err != nil {
		t.Fatalf("read manifest.server.zst: %v", err)
	}
	if frames := bytes.Count(serverManifestRaw, []byte{0x28, 0xb5, 0x2f, 0xfd}); frames < 2 {
		t.Fatalf("manifest.server.zst should preserve streamed zstd frames, got %d zstd frame(s)", frames)
	}
	if _, err := tx.LoadManifest(serverManifestPath); err != nil {
		t.Fatalf("load streamed manifest.server.zst: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	code = RunCLI([]string{srv.URL, "get", "--progress=false", "-a", "1KiB", "-o", filepath.Join(targetDir, "a.txt"), "/remote/a.txt"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "a.txt"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected output: %q", string(got))
	}
}

func TestRunCLIGetSkipWriteDiscardsOutput(t *testing.T) {
	payload := []byte("hello")
	singleManifest := "FM/1 txdevnull mode=fast link-mbps=0 concurrency=8\nD0 0 0:100 0755 0:7:/remote\nF1 5 0:100 0644 0:5:a.txt\n"
	var sawAck atomic.Bool

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, singleManifest); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrame(1, payload, 0))
			return err
		case intftcp.VerbACK:
			sawAck.Store(true)
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "get", "--skip-write", "--progress=false", "-a", "1KiB", "/remote/a.txt"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get skip-write: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !sawAck.Load() {
		t.Fatalf("expected ACK request")
	}
	if !strings.Contains(stdout.String(), "  path: "+os.DevNull) {
		t.Fatalf("expected file metrics path %q, got: %s", os.DevNull, stdout.String())
	}
}

func TestRunCLIGetCacheLoadRequestsAndTouchesHint(t *testing.T) {
	payload := []byte("hello")
	var cacheEntry pagecache.CacheEntry
	if err := cacheEntry.SetPageBits([]byte{0x01}, 1); err != nil {
		t.Fatalf("SetPageBits: %v", err)
	}
	pageCacheBlob, err := encoding.EncodePageCacheEntry(&cacheEntry)
	if err != nil {
		t.Fatalf("EncodePageCacheEntry: %v", err)
	}
	pageCacheToken := encoding.EncodePageCacheToken(pageCacheBlob)
	singleManifest := fmt.Sprintf(
		"FM/1 txpreservecache mode=fast link-mbps=0 concurrency=8\nD0 0 0:100 0755 0:7:/remote\nF1 %d 0:100 0644 0:5:a.txt %s\n",
		len(payload),
		pageCacheToken,
	)
	var sawCacheMap atomic.Bool

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if req.Params[0]["cache-map"] == "1" {
				sawCacheMap.Store(true)
			}
			if err := writeManifestResponse(out, singleManifest); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrame(1, payload, 0))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	tmp := t.TempDir()
	outputPath := filepath.Join(tmp, "a.txt")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "get", "--cache-load=full", "--progress=false", "-a", "1KiB", "-o", outputPath, "/remote/a.txt"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get cache-load: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !sawCacheMap.Load() {
		t.Fatalf("expected TXFER cache-map=1")
	}
	if pagecache.TouchSupported() && !strings.Contains(stderr.String(), "cache-touch: [ok] warmed=1/1") {
		t.Fatalf("expected cache-touch warming output, got: %s", stderr.String())
	}
}

func TestRunCLIGetProgressFileWritesStatusAndPct(t *testing.T) {
	payload := []byte("hello")
	singleManifest := "FM/1 txprogress mode=fast link-mbps=0 concurrency=8\nD0 0 0:100 0755 0:7:/remote\nF1 5 0:100 0644 0:5:a.txt\n"

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, singleManifest); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrame(1, payload, 0))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	tmp := t.TempDir()
	progressPath := filepath.Join(tmp, "progress.txt")
	outputPath := filepath.Join(tmp, "a.txt")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{
		srv.URL, "get", "--progress=false",
		"--progress-path", progressPath,
		"--progress-interval", "1h",
		"-o", outputPath,
		"/remote/a.txt",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get with progress file: expected 0, got %d stderr=%s", code, stderr.String())
	}

	raw, err := os.ReadFile(progressPath)
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	wantStatus := intfilexfer.FormatProgressStatusLine("client", "", 1, 1, int64(len(payload)), int64(len(payload)))
	want := wantStatus + "\n"
	if got := string(raw); got != want {
		t.Fatalf("unexpected progress file contents:\n got=%q\nwant=%q", got, want)
	}
}

func TestRunCLITransferWithEncryptAuto(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := strings.Join([]string{
		"FM/1 txenccli mode=fast link-mbps=1000 concurrency=8",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 2:0 0644 0:5:a.txt",
		"",
	}, "\n")

	serverID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate server identity: %v", err)
	}
	srv := newFTCPTestServerWithIdentity(t, serverID, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			cts0 := req.Params[0]["cts0"]
			n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
			if err != nil || n < 0 {
				return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
			}
			if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
				return err
			}
			if n > 0 {
				if _, err := out.Write(make([]byte, n)); err != nil {
					return err
				}
			}
			_, err = io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runTransferCLI(srv.URL, []string{"-s", "/remote", "--encrypt", "auto", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	serverManifestPath := filepath.Join(tmp, ".tx", "dst", "manifest.server.zst")
	gotManifest, err := tx.LoadManifest(serverManifestPath)
	if err != nil {
		t.Fatalf("load manifest.server.zst: %v", err)
	}
	wantManifest, err := tx.ParseManifest([]byte(manifestRaw))
	if err != nil {
		t.Fatalf("parse expected manifest: %v", err)
	}
	if tx.ManifestFingerprint(gotManifest) != tx.ManifestFingerprint(wantManifest) {
		t.Fatalf("manifest fingerprints differ after decrypted persist")
	}
}

func TestRunCLITransferWithEncryptAES(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := strings.Join([]string{
		"FM/1 txaescli mode=fast link-mbps=1000 concurrency=8",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 2:0 0644 0:5:a.txt",
		"",
	}, "\n")

	serverID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate server identity: %v", err)
	}
	srv := newFTCPTestServerWithIdentity(t, serverID, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			cts0 := req.Params[0]["cts0"]
			n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
			if err != nil || n < 0 {
				return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
			}
			if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
				return err
			}
			if n > 0 {
				if _, err := out.Write(make([]byte, n)); err != nil {
					return err
				}
			}
			_, err = io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runTransferCLI(srv.URL, []string{"-s", "/remote", "--encrypt", "aes", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	serverManifestPath := filepath.Join(tmp, ".tx", "dst", "manifest.server.zst")
	gotManifest, err := tx.LoadManifest(serverManifestPath)
	if err != nil {
		t.Fatalf("load manifest.server.zst: %v", err)
	}
	wantManifest, err := tx.ParseManifest([]byte(manifestRaw))
	if err != nil {
		t.Fatalf("parse expected manifest: %v", err)
	}
	if tx.ManifestFingerprint(gotManifest) != tx.ManifestFingerprint(wantManifest) {
		t.Fatalf("manifest fingerprints differ after decrypted persist")
	}
}

func TestRunCLIStartDownloadsAll(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstart mode=gentle link-mbps=700 concurrency=3",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"F2 4 0:101 0644 0:5:b.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				if got := p["mode"]; got != tx.LoadStrategyGentle {
					return fmt.Errorf("expected SEND mode=%s, got %q", tx.LoadStrategyGentle, got)
				}
			}
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case "1":
					if _, err := io.WriteString(out, buildCLIFrame(1, []byte("hello"), 0)); err != nil {
						return err
					}
				case "2":
					if _, err := io.WriteString(out, buildCLIFrame(2, []byte("test"), 0)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected fid: %q", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--concurrency", "2", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d stderr=%s", code, stderr.String())
	}
	out := stderr.String()
	if !strings.Contains(out, "mode: [gentle]") ||
		!strings.Contains(out, "concurrency: 2 (override from --concurrency") ||
		!strings.Contains(out, "    window: ") ||
		!strings.Contains(out, "    batch-per-window: ") ||
		!strings.Contains(out, "server: 24 cpu, 1 io-depth") ||
		!strings.Contains(out, "25% gentle-bw") {
		t.Fatalf("missing start plan output: %s", out)
	}
	// After start, staging dir is renamed to target dir.
	for _, p := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(targetDir, p)); err != nil {
			t.Fatalf("missing output %s: %v", p, err)
		}
	}
}

func TestPartialSplitDownloadPersistsAndResumesWithoutRedownloading(t *testing.T) {
	tmp := t.TempDir()
	outRoot := filepath.Join(tmp, "dst")
	progressPath := filepath.Join(tmp, ".tx", "dst", "manifest.progress")
	payload := []byte("abcdefghijklmno")
	manifest := &tx.Manifest{
		TransferID:  "txpartialresume",
		Root:        "/remote",
		Mode:        tx.LoadStrategyFast,
		LinkMbps:    1000,
		Concurrency: 1,
		Entries: []tx.ManifestEntry{
			{ID: 1, Size: int64(len(payload)), Path: "big.bin"},
		},
	}
	destPath := filepath.Join(outRoot, "big.bin")

	runDownload := func(t *testing.T, srvURL string, state map[uint64]tx.ManifestProgress) error {
		t.Helper()
		applyProgressStateToManifest(manifest, state)
		progressUpdates := make(chan tx.DownloadProgressUpdate, 32)
		stopProgress, persistProgressAck, markMetadataDone := startProgressWriter(progressPath, tx.ManifestFingerprint(manifest), state, progressUpdates, nil, io.Discard)
		defer stopProgress()
		syncWorker, stopSync := fsync.StartSyncWorker(-1, true, time.Second, io.Discard)
		defer stopSync(context.Background())
		client := tx.NewClient(srvURL, tx.WithFileRequestWindowBytes(15))
		defer client.Close()
		resp, err := downloadManifestFiles(manifestDownloadConfig{
			Client:             client,
			Manifest:           manifest,
			Entries:            manifest.Entries,
			Concurrency:        1,
			BatchMaxBytes:      5,
			SplitWindowWorkers: 3,
			ProgressUpdates:    progressUpdates,
			OutputWriter: func(entry tx.ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
				w, syncFn, err := openDownloadOutput(entry, offset, resolveDownloadDestinationPath(entry, outRoot, ""), nil, syncWorker)
				if err != nil {
					return nil, nil, err
				}
				return w, syncFn, nil
			},
			OnFileDone: func(evt tx.StartFileDoneEvent) {
				persistProgressAck(evt.File.Meta.FileID, manifest.Entries[0].Size)
				markMetadataDone(evt.File.Meta.FileID)
			},
		})
		if err == nil && len(resp.Errors) > 0 {
			err = resp.Errors[0]
		}
		return err
	}

	firstAcked := make(chan struct{})
	var firstAckOnce sync.Once
	var firstAckMu sync.Mutex
	var firstAckTokens []string
	firstSrv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			offset, err := parseOptionalInt64(target["offset"])
			if err != nil {
				return fmt.Errorf("parse offset: %w", err)
			}
			if offset == 0 {
				_, err := io.WriteString(out, buildCLIFrame(1, payload[:5], 0)+"OK\r\n")
				return err
			}
			select {
			case <-firstAcked:
			case <-time.After(5 * time.Second):
				return fmt.Errorf("timed out waiting for first split-window ACK before offset %d interruption", offset)
			}
			return fmt.Errorf("intentional interruption at offset %d", offset)
		case intftcp.VerbACK:
			firstAckMu.Lock()
			firstAckTokens = append(firstAckTokens, req.Params[0]["ack-token"])
			firstAckMu.Unlock()
			firstAckOnce.Do(func() { close(firstAcked) })
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	err := runDownload(t, firstSrv.URL, map[uint64]tx.ManifestProgress{})
	firstSrv.Close()
	if err == nil {
		t.Fatal("expected first download to fail after the first persisted split window")
	}
	firstAckMu.Lock()
	gotFirstAckTokens := append([]string(nil), firstAckTokens...)
	firstAckMu.Unlock()
	if len(gotFirstAckTokens) != 1 || !strings.HasPrefix(gotFirstAckTokens[0], "5@") {
		t.Fatalf("expected one ACK at byte 5 before interruption, got %v (err=%v)", gotFirstAckTokens, err)
	}
	state, err := loadProgressState(progressPath)
	if err != nil {
		t.Fatalf("load progress after interruption: %v", err)
	}
	if got := state[1].AckBytes; got != 5 {
		t.Fatalf("expected persisted resume offset 5, got %d", got)
	}
	partial, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read partial output: %v", err)
	}
	if string(partial) != "abcde" {
		t.Fatalf("unexpected partial output: %q", partial)
	}

	var resumedOffsetsMu sync.Mutex
	var resumedOffsets []int64
	secondSrv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			offset, err := parseOptionalInt64(target["offset"])
			if err != nil {
				return fmt.Errorf("parse offset: %w", err)
			}
			resumedOffsetsMu.Lock()
			resumedOffsets = append(resumedOffsets, offset)
			resumedOffsetsMu.Unlock()
			if offset == 0 {
				return fmt.Errorf("resume re-requested already persisted bytes")
			}
			size, err := parseOptionalInt64(target["size"])
			if err != nil {
				return fmt.Errorf("parse size: %w", err)
			}
			_, err = io.WriteString(out, buildCLIFrame(1, payload[offset:offset+size], offset)+"OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer secondSrv.Close()
	err = runDownload(t, secondSrv.URL, state)
	if err != nil {
		t.Fatalf("resume download failed: %v", err)
	}
	resumedOffsetsMu.Lock()
	resumedOffsets = append([]int64(nil), resumedOffsets...)
	resumedOffsetsMu.Unlock()
	sort.Slice(resumedOffsets, func(i, j int) bool { return resumedOffsets[i] < resumedOffsets[j] })
	if got, want := fmt.Sprint(resumedOffsets), "[5 10]"; got != want {
		t.Fatalf("unexpected resumed SEND offsets: got=%s want=%s", got, want)
	}
	complete, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read resumed output: %v", err)
	}
	if string(complete) != string(payload) {
		t.Fatalf("unexpected resumed output: %q", complete)
	}
}

func TestRunCLIStartPrintsResumeProgress(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstartresume mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 10 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "1 5 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write partial staging file: %v", err)
	}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			if got := target["offset"]; got != "5" {
				return fmt.Errorf("expected resume offset 5, got %q", got)
			}
			if got := target["size"]; got != "5" {
				return fmt.Errorf("expected resume size 5, got %q", got)
			}
			_, err := io.WriteString(out, buildCLIFrame(1, []byte("world"), 5)+"OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--progress=false", "--skip-fsync", "--concurrency", "1", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start resume: expected 0, got %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, "copy-resume: resuming 1 file(s), 5 B/10 B already copied") ||
		!strings.Contains(errText, "copy-resume:   id=1 done=5 B/10 B (50.0%)") ||
		strings.Contains(errText, "copy-resume: skipping") ||
		strings.Contains(errText, "path=a.txt") {
		t.Fatalf("missing resume progress output: %s", errText)
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "a.txt"))
	if err != nil {
		t.Fatalf("read resumed file: %v", err)
	}
	if string(got) != "helloworld" {
		t.Fatalf("unexpected resumed file contents: %q", got)
	}
}

func TestRunCLIStartPrintsSkippedResumeProgress(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstartskipresume mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"F2 10 0:101 0644 0:5:b.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "1 5 1\n2 5 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write complete staging file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "b.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write partial staging file: %v", err)
	}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			if got := target["fid"]; got != "2" {
				return fmt.Errorf("expected SEND for fid=2 only, got %q", got)
			}
			if got := target["offset"]; got != "5" {
				return fmt.Errorf("expected resume offset 5, got %q", got)
			}
			_, err := io.WriteString(out, buildCLIFrame(2, []byte("world"), 5)+"OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--progress=false", "--skip-fsync", "--concurrency", "1", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start skip resume: expected 0, got %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, "copy-resume: skipping 1 file(s), 5 B already copied") ||
		!strings.Contains(errText, "copy-resume: resuming 1 file(s), 5 B/10 B already copied") ||
		!strings.Contains(errText, "copy-resume:   id=2 done=5 B/10 B (50.0%)") {
		t.Fatalf("missing skip/resume progress output: %s", errText)
	}
}

func TestRunCLIStartUsesManifestConcurrencyDefault(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstartdefault mode=fast link-mbps=1200 concurrency=5",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			if got := req.Params[1]["mode"]; got != tx.LoadStrategyFast {
				return fmt.Errorf("expected SEND mode=%s, got %q", tx.LoadStrategyFast, got)
			}
			if _, err := io.WriteString(out, buildCLIFrame(1, []byte("hello"), 0)); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "sync-conn-fallbacks=") {
		t.Fatalf("expected sync fallback count in start summary, got %q", stdout.String())
	}
	out := stderr.String()
	if !strings.Contains(out, "mode: [fast]") ||
		!strings.Contains(out, "concurrency: 5") ||
		!strings.Contains(out, "    window: ") ||
		!strings.Contains(out, "    batch-per-window: ") ||
		!strings.Contains(out, "server: 24 cpu, 1 io-depth") {
		t.Fatalf("missing default start plan output: %s", out)
	}
}

func TestRunCLIStartDiscardSkipsTargetMutationAndLocalManifest(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txdiscard mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	keepPath := filepath.Join(targetDir, "keep.txt")
	if err := os.WriteFile(keepPath, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write keep file: %v", err)
	}
	var sawAck atomic.Bool

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			if _, err := io.WriteString(out, buildCLIFrame(1, []byte("hello"), 0)); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			sawAck.Store(true)
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return nil
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--discard", "--progress=false", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start --discard: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !sawAck.Load() {
		t.Fatalf("expected ACK request")
	}
	gotKeep, err := os.ReadFile(keepPath)
	if err != nil {
		t.Fatalf("read keep file: %v", err)
	}
	if string(gotKeep) != "keep" {
		t.Fatalf("unexpected keep file contents: %q", gotKeep)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected discarded output to be absent, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".tx", "dst", "manifest.zst")); !os.IsNotExist(err) {
		t.Fatalf("expected local manifest to be absent, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".tx", "dst", "manifest.progress")); !os.IsNotExist(err) {
		t.Fatalf("expected progress state to be removed, stat err=%v", err)
	}
}

func TestRunCLIStartDiscardSkipsCompletedMetadataRefresh(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txdiscardrefresh mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "1 5 0\n")

	srv := newUnexpectedVerbFTCPTestServer(t)
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{
		"--discard",
		"--progress=false",
		"--ack-every", "1KiB",
		targetDir,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start --discard completed refresh: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(tmp, ".tx", "dst", "manifest.progress")); !os.IsNotExist(err) {
		t.Fatalf("expected progress state to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected discarded output to be absent, stat err=%v", err)
	}
}

func TestRunCLIStartDirectoryOnlyDoesNotTransfer(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		tmp := t.TempDir()
		manifestRaw := buildTestManifestRaw("txdironly", []string{
			buildTestDirManifestEntry(1, 100, 0o750, "sub"),
		})
		targetDir := setupPinchState(t, tmp, manifestRaw, "")

		rootFID := strconv.FormatUint(tx.RootFileID, 10)
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbSEND:
				for _, p := range req.Params[1:] {
					switch p["fid"] {
					case rootFID:
						if err := writeCLIMetadataFrame(out, tx.RootFileID, 200, "0755"); err != nil {
							return err
						}
					case "1":
						if err := writeCLIMetadataFrame(out, 1, 100, "0750"); err != nil {
							return err
						}
					default:
						return fmt.Errorf("unexpected SEND fid: %q", p["fid"])
					}
				}
				_, err := io.WriteString(out, "OK\r\n")
				return err
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runStartCLI(srv.URL, []string{"--progress=false", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("start directory-only: expected 0, got %d stderr=%s", code, stderr.String())
		}

		subPath := filepath.Join(targetDir, "sub")
		info, err := os.Stat(subPath)
		if err != nil {
			t.Fatalf("stat sub: %v", err)
		}
		if !info.IsDir() {
			t.Fatalf("sub should be a directory")
		}
		if perm := info.Mode().Perm(); perm != 0o750 {
			t.Fatalf("sub mode: got %o, want 0750", perm)
		}
	})

	t.Run("discard", func(t *testing.T) {
		tmp := t.TempDir()
		manifestRaw := buildTestManifestRaw("txdironlydiscard", []string{
			buildTestDirManifestEntry(1, 100, 0o750, "sub"),
		})
		targetDir := setupPinchState(t, tmp, manifestRaw, "")

		srv := newUnexpectedVerbFTCPTestServer(t)
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runStartCLI(srv.URL, []string{"--discard", "--progress=false", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("start directory-only discard: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(filepath.Join(targetDir, "sub")); !os.IsNotExist(err) {
			t.Fatalf("expected discard mode to skip directory creation, stat err=%v", err)
		}
	})
}

func TestRunCLIStartHardlinkDedup(t *testing.T) {
	tmp := t.TempDir()
	// F1: regular file "a.txt" (5 bytes), H2: hardlink "b.txt" → file id 1
	// H entry: mtime field carries target file ID (1), size=0
	manifestRaw := strings.Join([]string{
		"FM/1 txhl mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"H2 0 0:1 0644 0:5:b.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")

	var sentFIDs []string
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				sentFIDs = append(sentFIDs, p["fid"])
				switch p["fid"] {
				case "1":
					if _, err := io.WriteString(out, buildCLIFrame(1, []byte("hello"), 0)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q (hardlink should not be sent)", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--concurrency", "1", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}

	// Only file 1 should have been sent — not the hardlink.
	if len(sentFIDs) != 1 || sentFIDs[0] != "1" {
		t.Fatalf("expected SEND for fid=1 only, got %v", sentFIDs)
	}

	// Both files should exist.
	aPath := filepath.Join(targetDir, "a.txt")
	bPath := filepath.Join(targetDir, "b.txt")
	aInfo, err := os.Stat(aPath)
	if err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}
	bInfo, err := os.Stat(bPath)
	if err != nil {
		t.Fatalf("stat b.txt: %v", err)
	}

	// Both should have the same content.
	aData, _ := os.ReadFile(aPath)
	bData, _ := os.ReadFile(bPath)
	if string(aData) != "hello" || string(bData) != "hello" {
		t.Fatalf("content mismatch: a=%q b=%q", aData, bData)
	}

	// They should share the same inode (hardlinked).
	aSys := aInfo.Sys().(*syscall.Stat_t)
	bSys := bInfo.Sys().(*syscall.Stat_t)
	if aSys.Ino != bSys.Ino {
		t.Errorf("expected hardlink (same inode), a.ino=%d b.ino=%d", aSys.Ino, bSys.Ino)
	}
}

func TestRunCLIStartSymlinks(t *testing.T) {
	tmp := t.TempDir()
	// F1: regular file "a.txt" (5 bytes), S2: symlink "link.txt" → "a.txt"
	manifestRaw := strings.Join([]string{
		"FM/1 txsym mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 5 0:100 0644 0:5:a.txt",
		"S2 0 0:100 0777 0:8:link.txt 5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case "1":
					if _, err := io.WriteString(out, buildCLIFrame(1, []byte("hello"), 0)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q (symlink should not be sent)", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--concurrency", "1", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}

	// a.txt should exist as a regular file.
	aPath := filepath.Join(targetDir, "a.txt")
	if _, err := os.Stat(aPath); err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}

	// link.txt should be a symlink pointing to "a.txt".
	linkPath := filepath.Join(targetDir, "link.txt")
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink link.txt: %v", err)
	}
	if target != "a.txt" {
		t.Errorf("symlink target: got %q, want %q", target, "a.txt")
	}
}

func TestRunCLIStartDirectories(t *testing.T) {
	tmp := t.TempDir()
	// D1: directory "sub" with mode 0750, F2: file "sub/a.txt" (5 bytes)
	// Path front-coding: prev="sub", "3:6:/a.txt" → prefix 3 chars of "sub" + "/a.txt" = "sub/a.txt"
	manifestRaw := strings.Join([]string{
		"FM/1 txdir mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"D1 0 0:100 0750 0:3:sub",
		"F2 5 1:00 0644 3:6:/a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")
	rootFID := strconv.FormatUint(tx.RootFileID, 10)
	rootMeta := testOwnershipMetadata(0, 200, "0755")
	dirMeta := testOwnershipMetadata(0, 100, "1750")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case "2":
					if _, err := io.WriteString(out, buildCLIFrame(2, []byte("hello"), 0)); err != nil {
						return err
					}
				case rootFID:
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(tx.RootFileID, nil, 0, rootMeta)); err != nil {
						return err
					}
				case "1":
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(1, nil, 0, dirMeta)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--concurrency", "1", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}

	// sub/ should exist with mode 0750.
	subPath := filepath.Join(targetDir, "sub")
	info, err := os.Stat(subPath)
	if err != nil {
		t.Fatalf("stat sub: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("sub should be a directory")
	}
	if mode := info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky); mode != os.ModeSticky|0o750 {
		t.Errorf("sub mode: got %o, want 1750", mode)
	}
	if info.ModTime().UnixNano() != dirMeta.MtimeNS {
		t.Errorf("sub mtime: got %d, want %d", info.ModTime().UnixNano(), dirMeta.MtimeNS)
	}

	// sub/a.txt should exist.
	if _, err := os.Stat(filepath.Join(subPath, "a.txt")); err != nil {
		t.Fatalf("stat sub/a.txt: %v", err)
	}
}

func TestRunCLIStartMixedManifestTypes(t *testing.T) {
	tmp := t.TempDir()
	payload := []byte("hello")
	manifestRaw := buildTestManifestRaw("txmixed-start", []string{
		buildTestDirManifestEntry(1, 100, 0o750, "sub"),
		buildTestManifestEntry(2, int64(len(payload)), 100, 0o644, "sub/a.txt"),
		buildTestHardlinkManifestEntry(3, 2, 0o644, "sub/b.txt"),
		buildTestSymlinkManifestEntry(4, 100, 0o777, "sub/link.txt", "a.txt"),
	})
	targetDir := setupPinchState(t, tmp, manifestRaw, "")
	meta := testOwnershipMetadata(int64(len(payload)), 100, "0644")
	rootFID := strconv.FormatUint(tx.RootFileID, 10)
	rootMeta := testOwnershipMetadata(0, 300, "0755")
	dirMeta := testOwnershipMetadata(0, 200, "0750")

	var sentFIDs []string
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				sentFIDs = append(sentFIDs, p["fid"])
				switch p["fid"] {
				case "2":
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(2, payload, 0, meta)); err != nil {
						return err
					}
				case rootFID:
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(tx.RootFileID, nil, 0, rootMeta)); err != nil {
						return err
					}
				case "1":
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(1, nil, 0, dirMeta)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--concurrency", "1", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start mixed types: expected 0, got %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
	}
	if !slices.Contains(sentFIDs, "1") || !slices.Contains(sentFIDs, "0") || !slices.Contains(sentFIDs, rootFID) {
		t.Fatalf("expected SENDs for file, dir, and root metadata, got %v", sentFIDs)
	}

	subPath := filepath.Join(targetDir, "sub")
	info, err := os.Stat(subPath)
	if err != nil {
		t.Fatalf("stat sub: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("sub should be a directory")
	}
	if perm := info.Mode().Perm(); perm != 0o750 {
		t.Fatalf("sub mode: got %o, want 0750", perm)
	}
	if info.ModTime().UnixNano() != dirMeta.MtimeNS {
		t.Fatalf("sub mtime: got %d, want %d", info.ModTime().UnixNano(), dirMeta.MtimeNS)
	}

	aPath := filepath.Join(subPath, "a.txt")
	bPath := filepath.Join(subPath, "b.txt")
	linkPath := filepath.Join(subPath, "link.txt")
	aData, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("read a.txt: %v", err)
	}
	if string(aData) != string(payload) {
		t.Fatalf("unexpected a.txt contents: %q", aData)
	}
	bData, err := os.ReadFile(bPath)
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(bData) != string(payload) {
		t.Fatalf("unexpected b.txt contents: %q", bData)
	}
	aInfo, err := os.Stat(aPath)
	if err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}
	bInfo, err := os.Stat(bPath)
	if err != nil {
		t.Fatalf("stat b.txt: %v", err)
	}
	aStat := aInfo.Sys().(*syscall.Stat_t)
	bStat := bInfo.Sys().(*syscall.Stat_t)
	if aStat.Ino != bStat.Ino {
		t.Fatalf("expected hardlink inode match, got %d vs %d", aStat.Ino, bStat.Ino)
	}
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink link.txt: %v", err)
	}
	if target != "a.txt" {
		t.Fatalf("symlink target: got %q, want %q", target, "a.txt")
	}
}

func TestStartTransferProbeReporterIncludesTransferTelemetry(t *testing.T) {
	oldInterval := transferProbeRefreshInterval
	transferProbeRefreshInterval = 5 * time.Millisecond
	defer func() { transferProbeRefreshInterval = oldInterval }()

	var probeCount atomic.Int64
	var firstTransferID atomic.Value
	var firstObserved atomic.Int64

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbPROBE {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		probeCount.Add(1)
		firstTransferID.CompareAndSwap(nil, req.Params[0]["txferid"])
		if probeCount.Load() == 1 {
			obs, _ := strconv.ParseInt(strings.TrimSpace(req.Params[0]["obs-link-mbps"]), 10, 64)
			firstObserved.Store(obs)
		}
		cts0 := req.Params[0]["cts0"]
		n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
		if err != nil {
			return err
		}
		if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=8 cts0=%s sts0=10 sts1=11 probe-bytes=%d wmem=4096 gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
			return err
		}
		if n > 0 {
			if _, err := out.Write(make([]byte, n)); err != nil {
				return err
			}
		}
		_, err = io.WriteString(out, "OK\r\n")
		return err
	})
	defer srv.Close()

	client := tx.NewClient(srv.URL)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	pr := startTransferProbeReporter(ctx, client, "txprobe", tx.LoadStrategyFast, 1024, 700)
	defer pr.stop()
	defer cancel()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if probeCount.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if probeCount.Load() == 0 {
		t.Fatalf("expected at least one probe refresh")
	}
	gotTxferID, _ := firstTransferID.Load().(string)
	if gotTxferID != "txprobe" {
		t.Fatalf("unexpected transfer id: %q", gotTxferID)
	}
	if firstObserved.Load() != 700 {
		t.Fatalf("unexpected initial observed link mbps: %d", firstObserved.Load())
	}
	if got := pr.linkMbps.Load(); got <= 0 {
		t.Fatalf("expected probe reporter to retain link mbps, got %d", got)
	}
	if got := pr.lastProbeUnixS.Load(); got <= 0 {
		t.Fatalf("expected probe reporter to retain last probe timestamp, got %d", got)
	}
}

func TestFormatProbeRateSuffixUsesServerLimiter(t *testing.T) {
	now := time.Unix(200, 0)
	var probe probeReporter
	probe.limiterBps.Store(100 * 1024 * 1024)
	probe.linkMbps.Store(9000)
	probe.lastProbeUnixS.Store(now.Add(-2 * time.Second).Unix())

	got := formatProbeRateSuffix(now, 25*1024*1024, &probe)
	if got != " (25% of limit=100.00 MiB/s @  2s)" {
		t.Fatalf("unexpected limiter suffix: %q", got)
	}
}

func TestFormatProbeRateSuffixFallsBackToLinkBandwidth(t *testing.T) {
	now := time.Unix(300, 0)
	var probe probeReporter
	probe.linkMbps.Store(800)
	probe.lastProbeUnixS.Store(now.Add(-10 * time.Second).Unix())

	got := formatProbeRateSuffix(now, 50*1_000_000, &probe)
	if got != " (50% of link=95.37 MiB/s @ 10s)" {
		t.Fatalf("unexpected link suffix: %q", got)
	}
}

func TestFormatProbeRateSuffixClampsLinkFallbackTo100Pct(t *testing.T) {
	now := time.Unix(320, 0)
	var probe probeReporter
	probe.linkMbps.Store(800)
	probe.lastProbeUnixS.Store(now.Add(-3 * time.Second).Unix())

	got := formatProbeRateSuffix(now, 400*1_000_000, &probe)
	if got != " (100% of link=95.37 MiB/s @  3s)" {
		t.Fatalf("unexpected clamped link suffix: %q", got)
	}
}

func TestFormatStartBatchCause(t *testing.T) {
	const mib = int64(1 << 20)
	tests := []struct {
		name string
		plan tx.BatchSizePlan
		want string
	}{
		{
			name: "window",
			plan: tx.BatchSizePlan{
				BatchMaxBytes:  32 * mib,
				ConcBatchBytes: 32 * mib,
				FloorBytes:     16 * mib,
			},
			want: "window",
		},
		{
			name: "bw-probe",
			plan: tx.BatchSizePlan{
				BatchMaxBytes:  4 * mib,
				ConcBatchBytes: 32 * mib,
				BwCeilBytes:    4 * mib,
				FloorBytes:     1 * mib,
			},
			want: "bw-probe",
		},
		{
			name: "bw-probe raised to socket size",
			plan: tx.BatchSizePlan{
				BatchMaxBytes:  16 * mib,
				ConcBatchBytes: 32 * mib,
				BwCeilBytes:    4 * mib,
				FloorBytes:     16 * mib,
			},
			want: "bw-probe, raised to socket size",
		},
		{
			name: "floor",
			plan: tx.BatchSizePlan{
				BatchMaxBytes:  16 * mib,
				ConcBatchBytes: 8 * mib,
				FloorBytes:     16 * mib,
			},
			want: "floor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatStartBatchCause(tt.plan); got != tt.want {
				t.Fatalf("formatStartBatchCause() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatStartBatchWindowLine(t *testing.T) {
	const mib = int64(1 << 20)
	got := formatStartBatchWindowLine(512*mib, tx.BatchSizePlan{
		PerFileWorkers: 24,
		ConcBatchBytes: 32 * mib,
	})
	want := "    window: 512.00 MiB / 24 per-file-workers = 32.00 MiB"
	if got != want {
		t.Fatalf("formatStartBatchWindowLine() = %q, want %q", got, want)
	}
}

func TestFormatStartBatchProbeLine(t *testing.T) {
	const mib = int64(1 << 20)

	t.Run("active", func(t *testing.T) {
		got := formatStartBatchProbeLine(1001, 96, tx.BatchSizePlan{
			ConcBatchBytes: 32 * mib,
			BwCeilBytes:    4 * mib,
		})
		want := "    bw-probe: 1001 MiB/s / 96 conc / 2 = 4.00 MiB"
		if got != want {
			t.Fatalf("formatStartBatchProbeLine() = %q, want %q", got, want)
		}
	})

	t.Run("inactive", func(t *testing.T) {
		got := formatStartBatchProbeLine(1001, 96, tx.BatchSizePlan{
			ConcBatchBytes: 32 * mib,
			BwCeilBytes:    32 * mib,
		})
		if got != "" {
			t.Fatalf("expected inactive bw-probe line to be hidden, got %q", got)
		}
	})
}

func TestFixedWidthHumanDurationKeepsShortDurationsAligned(t *testing.T) {
	short := fixedWidthHumanDuration(2 * time.Second)
	longer := fixedWidthHumanDuration(10 * time.Second)
	minute := fixedWidthHumanDuration(62 * time.Second)

	if short != "  2s" {
		t.Fatalf("unexpected short duration: %q", short)
	}
	if longer != " 10s" {
		t.Fatalf("unexpected longer duration: %q", longer)
	}
	if minute != "1m2s" {
		t.Fatalf("unexpected minute duration: %q", minute)
	}
	if len(short) != len(longer) || len(longer) != len(minute) {
		t.Fatalf("expected fixed-width durations, got lens %d %d %d", len(short), len(longer), len(minute))
	}
}

func TestFixedWidthETAKeepsDurationsAligned(t *testing.T) {
	short := fixedWidthETA(57 * time.Second)
	minute := fixedWidthETA(74 * time.Second)

	if short != "  57s" {
		t.Fatalf("unexpected short eta: %q", short)
	}
	if minute != " 1.2m" {
		t.Fatalf("unexpected minute eta: %q", minute)
	}
	if len(short) != 5 || len(minute) != 5 {
		t.Fatalf("expected 5-char eta fields, got %d and %d", len(short), len(minute))
	}
}

func TestFixedWidthETANA(t *testing.T) {
	if got := fixedWidthETANA(); got != "  n/a" {
		t.Fatalf("unexpected n/a eta: %q", got)
	}
	if len(fixedWidthETANA()) != 5 {
		t.Fatalf("expected 5-char n/a eta field, got %d", len(fixedWidthETANA()))
	}
}

func TestFormatCacheProgressLine(t *testing.T) {
	pageSize := int64(pagecache.PageSize())
	memField := func(pages int64) string {
		return encoding.HumanBytesFixedWidth(pages*pageSize, cacheProgressBytesWidth)
	}
	naBytes := fmt.Sprintf("%*s", cacheProgressBytesWidth, "n/a")
	naDur := fmt.Sprintf("%*s", cacheProgressDurationWidth, "n/a")

	for _, tc := range []struct {
		name  string
		tag   string
		state cacheProgressState
		want  string
	}{
		{
			name: "both-budgets-periodic",
			tag:  "cache-progress:",
			state: cacheProgressState{
				filesTouched: 10, totalFiles: 100,
				bytesTouched: 1 * 1024 * 1024, totalBytes: 10 * 1024 * 1024,
				pagesTouched: 1000, pageBudget: 10000,
				elapsed: 5 * time.Second, timeBudget: 30 * time.Second,
			},
			want: fmt.Sprintf(
				"cache-progress:[    10/   100]( 10.0%%) [  1.00 MiB/ 10.00 MiB]( 10.0%%) budget[   5s/  30s][%s/%s]",
				memField(1000), memField(10000),
			),
		},
		{
			name: "no-budgets-final-ok",
			tag:  "cache-touch:[ok]",
			state: cacheProgressState{
				filesTouched: 5, totalFiles: 5,
				bytesTouched: 0, totalBytes: 0,
				pagesTouched: 200, pageBudget: 0,
				elapsed: 2 * time.Second, timeBudget: 0,
			},
			want: fmt.Sprintf(
				"cache-touch:[ok][     5/     5](100.0%%) [       0 B/       0 B](  0.0%%) budget[   2s/%s][%s/%s]",
				naDur, memField(200), naBytes,
			),
		},
		{
			name: "page-budget-only-partial",
			tag:  "cache-touch:[partial-ok]",
			state: cacheProgressState{
				filesTouched: 50, totalFiles: 100,
				bytesTouched: 5 * 1024 * 1024, totalBytes: 10 * 1024 * 1024,
				pagesTouched: 7500, pageBudget: 10000,
				elapsed: 30 * time.Second, timeBudget: 0,
			},
			want: fmt.Sprintf(
				"cache-touch:[partial-ok][    50/   100]( 50.0%%) [  5.00 MiB/ 10.00 MiB]( 50.0%%) budget[  30s/%s][%s/%s]",
				naDur, memField(7500), memField(10000),
			),
		},
		{
			name: "time-budget-only",
			tag:  "cache-progress:",
			state: cacheProgressState{
				filesTouched: 1, totalFiles: 10,
				bytesTouched: 512, totalBytes: 5120,
				pagesTouched: 4, pageBudget: -1,
				elapsed: 10 * time.Second, timeBudget: 60 * time.Second,
			},
			want: fmt.Sprintf(
				"cache-progress:[     1/    10]( 10.0%%) [     512 B/  5.00 KiB]( 10.0%%) budget[  10s/ 1.0m][%s/%s]",
				memField(4), naBytes,
			),
		},
		{
			name: "overrun-clamped",
			tag:  "cache-touch:[partial-ok]",
			state: cacheProgressState{
				filesTouched: 100, totalFiles: 100,
				bytesTouched: 10 * 1024 * 1024, totalBytes: 10 * 1024 * 1024,
				pagesTouched: 12000, pageBudget: 10000,
				elapsed: 35 * time.Second, timeBudget: 30 * time.Second,
			},
			want: fmt.Sprintf(
				"cache-touch:[partial-ok][   100/   100](100.0%%) [ 10.00 MiB/ 10.00 MiB](100.0%%) budget[  35s/  30s][%s/%s]",
				memField(12000), memField(10000),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := formatCacheProgressLine(tc.tag, tc.state)
			if got != tc.want {
				t.Fatalf("formatCacheProgressLine\n  got:  %q\n  want: %q", got, tc.want)
			}
		})
	}
}

func TestFormatCacheVerifyLine(t *testing.T) {
	pageSize := int64(pagecache.PageSize())
	memField := func(pages int64) string {
		return encoding.HumanBytesFixedWidth(pages*pageSize, cacheProgressBytesWidth)
	}
	for _, tc := range []struct {
		name  string
		probe pagecache.ChunkProbeResult
		want  string
	}{
		{
			name: "full-honor",
			probe: pagecache.ChunkProbeResult{
				SampledChunks: 25600, PlannedChunks: 25600,
				ExpectedPages: 4_200_000, HonoredPages: 4_200_000,
			},
			want: fmt.Sprintf(
				"cache-verify:[ok][ 25600/ 25600 chunks](100.0%% honored) [%s/%s]",
				memField(4_200_000), memField(4_200_000),
			),
		},
		{
			name: "partial-honor",
			probe: pagecache.ChunkProbeResult{
				SampledChunks: 25600, PlannedChunks: 25600,
				ExpectedPages: 4_200_000, HonoredPages: 3_100_000,
			},
			want: fmt.Sprintf(
				"cache-verify:[ok][ 25600/ 25600 chunks]( 73.8%% honored) [%s/%s]",
				memField(3_100_000), memField(4_200_000),
			),
		},
		{
			name: "partial-budget-expired",
			probe: pagecache.ChunkProbeResult{
				SampledChunks: 12345, PlannedChunks: 25600,
				ExpectedPages: 2_200_000, HonoredPages: 1_500_000,
				Partial: true,
			},
			want: fmt.Sprintf(
				"cache-verify:[partial-ok][ 12345/ 25600 chunks]( 68.2%% honored) [%s/%s]",
				memField(1_500_000), memField(2_200_000),
			),
		},
		{
			name: "zero-expected",
			probe: pagecache.ChunkProbeResult{
				SampledChunks: 5, PlannedChunks: 5,
				ExpectedPages: 0, HonoredPages: 0,
			},
			want: fmt.Sprintf(
				"cache-verify:[ok][     5/     5 chunks](  0.0%% honored) [%s/%s]",
				memField(0), memField(0),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := formatCacheVerifyLine(tc.probe)
			if got != tc.want {
				t.Fatalf("formatCacheVerifyLine\n  got:  %q\n  want: %q", got, tc.want)
			}
		})
	}
}

func TestFormatVerifyDataSummaryLine(t *testing.T) {
	t.Run("budgeted", func(t *testing.T) {
		got := formatVerifyDataSummaryLine(10011, 14224, 100, 10*time.Second, 9876*time.Millisecond, false)
		want := "copy-verify-data: [ok] files=10011 samples=14224 budget=10s elapsed=9.876s"
		if got != want {
			t.Fatalf("formatVerifyDataSummaryLine() = %q, want %q", got, want)
		}
	})

	t.Run("sampled-percent", func(t *testing.T) {
		got := formatVerifyDataSummaryLine(42, 84, 5, 0, 1500*time.Microsecond, false)
		want := "copy-verify-data: [ok] files=42 samples=84 pct=5 elapsed=2ms"
		if got != want {
			t.Fatalf("formatVerifyDataSummaryLine() = %q, want %q", got, want)
		}
	})

	t.Run("partial-budgeted", func(t *testing.T) {
		got := formatVerifyDataSummaryLine(12, 34, 100, 10*time.Second, 1500*time.Millisecond, true)
		want := "copy-verify-data: [partial-ok] files=12 samples=34 budget=10s elapsed=1.5s"
		if got != want {
			t.Fatalf("formatVerifyDataSummaryLine() = %q, want %q", got, want)
		}
	})
}

func TestVerifyCopyDataSamplesBatchesChecksumRequests(t *testing.T) {
	tmp := t.TempDir()
	localPath := filepath.Join(tmp, "huge.bin")
	fd, err := os.Create(localPath)
	if err != nil {
		t.Fatalf("create local file: %v", err)
	}
	fileSize := int64(1200) * defaultVerifySampleFrameSize
	if err := fd.Truncate(fileSize); err != nil {
		_ = fd.Close()
		t.Fatalf("truncate local file: %v", err)
	}
	if err := fd.Close(); err != nil {
		t.Fatalf("close local file: %v", err)
	}

	manifest := &tx.Manifest{
		TransferID:  "txverify-batch",
		Root:        "/" + strings.Repeat("r", 3000),
		Concurrency: 1,
		Entries: []tx.ManifestEntry{
			{ID: 1, Size: fileSize, Path: "huge.bin"},
		},
	}
	cfg := copyCLIConfig{
		localDst:            tmp,
		verifyDataSamplePct: 100,
		concurrency:         1,
	}

	var reqCount atomic.Int64
	var maxCmdBytes atomic.Int64
	var zero [verifySampleBytes]byte
	zeroHash := encoding.FormatXXH128HashToken(xxh3.Hash128(zero[:]))
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbCXSUM {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		targets := checksumTargetsFromRequest(t, req)
		cmdBytes := estimateChecksumRequestCommandBytes(req.Params[0]["txferid"], targets)
		reqCount.Add(1)
		for {
			current := maxCmdBytes.Load()
			if int64(cmdBytes) <= current || maxCmdBytes.CompareAndSwap(current, int64(cmdBytes)) {
				break
			}
		}
		for _, target := range targets {
			hash := zeroHash
			if target.Size > 0 && target.Size < verifySampleBytes {
				hash = encoding.FormatXXH128HashToken(xxh3.Hash128(zero[:target.Size]))
			}
			if err := writeChecksumFrame(out, target.FileID, target.Offset, target.Size, hash); err != nil {
				return err
			}
		}
		return nil
	})
	defer srv.Close()

	files, samples, _, partial, err := verifyCopyDataSamples(srv.URL, cfg, manifest, io.Discard)
	if err != nil {
		t.Fatalf("verifyCopyDataSamples() err = %v", err)
	}
	if partial {
		t.Fatal("expected partial=false")
	}
	if files != 1 || samples != 1200 {
		t.Fatalf("verifyCopyDataSamples() = files=%d samples=%d, want 1/1200", files, samples)
	}
	if got := reqCount.Load(); got <= 1 {
		t.Fatalf("expected multiple checksum requests, got %d", got)
	}
	if got := maxCmdBytes.Load(); got > verifyChecksumCommandBudgetBytes {
		t.Fatalf("expected cmd bytes <= %d, got %d", verifyChecksumCommandBudgetBytes, got)
	}
	if got := maxCmdBytes.Load(); got >= 4*1024*1024 {
		t.Fatalf("expected cmd bytes below protocol limit, got %d", got)
	}
}

func TestVerifyCopyDataSamplesStopsDispatchAfterBudget(t *testing.T) {
	withVerifyBudgetGracePeriod(t, 100*time.Millisecond)

	tmp := t.TempDir()
	serverFiles := map[string][]byte{
		"/remote/a.txt": []byte("abcdefghijklmno"),
		"/remote/b.txt": []byte("pqrstuvwxyz0123"),
	}
	for serverPath, body := range serverFiles {
		localPath := filepath.Join(tmp, filepath.Base(serverPath))
		if err := os.WriteFile(localPath, body, 0o644); err != nil {
			t.Fatalf("write %s: %v", localPath, err)
		}
	}

	manifest := &tx.Manifest{
		TransferID:  "txverify-budget",
		Root:        "/remote",
		Concurrency: 1,
		Entries: []tx.ManifestEntry{
			{ID: 1, Size: int64(len(serverFiles["/remote/a.txt"])), Path: "a.txt"},
			{ID: 2, Size: int64(len(serverFiles["/remote/b.txt"])), Path: "b.txt"},
		},
	}
	cfg := copyCLIConfig{
		localDst:            tmp,
		verifyDataSamplePct: 100,
		verifyBudget:        10 * time.Millisecond,
		concurrency:         1,
	}

	var stderr bytes.Buffer
	var started atomic.Int64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbCXSUM {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		if started.Add(1) == 1 {
			time.Sleep(30 * time.Millisecond)
		}
		item := req.Params[1]
		fileID, err := strconv.ParseUint(item["fid"], 10, 64)
		if err != nil {
			return err
		}
		offset, err := parseOptionalInt64(item["offset"])
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(item["size"], 10, 64)
		if err != nil {
			return err
		}
		body := serverFiles[item["path"]]
		return writeChecksumFrame(out, fileID, offset, size, checksumTokenForRange(body, offset, size))
	})
	defer srv.Close()

	files, samples, _, partial, err := verifyCopyDataSamples(srv.URL, cfg, manifest, &stderr)
	if err != nil {
		t.Fatalf("verifyCopyDataSamples() err = %v", err)
	}
	if !partial {
		t.Fatal("expected partial=true")
	}
	if files != 1 || samples != 1 {
		t.Fatalf("verifyCopyDataSamples() = files=%d samples=%d, want 1/1", files, samples)
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("expected exactly one checksum request, got %d", got)
	}
	if !strings.Contains(stderr.String(), "copy-verify-data: budget expired, verified 1/2 files 1 samples") {
		t.Fatalf("expected partial verify budget log, got %q", stderr.String())
	}
}

func TestVerifyCopyDataSamplesReturnsMismatchDuringGrace(t *testing.T) {
	withVerifyBudgetGracePeriod(t, 100*time.Millisecond)

	tmp := t.TempDir()
	body := []byte("abcdefghijklmno")
	if err := os.WriteFile(filepath.Join(tmp, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	manifest := &tx.Manifest{
		TransferID:  "txverify-mismatch",
		Root:        "/remote",
		Concurrency: 1,
		Entries: []tx.ManifestEntry{
			{ID: 1, Size: int64(len(body)), Path: "a.txt"},
		},
	}
	cfg := copyCLIConfig{
		localDst:            tmp,
		verifyDataSamplePct: 100,
		verifyBudget:        10 * time.Millisecond,
		concurrency:         1,
	}

	var started atomic.Int64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbCXSUM {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		started.Add(1)
		time.Sleep(30 * time.Millisecond)
		item := req.Params[1]
		fileID, err := strconv.ParseUint(item["fid"], 10, 64)
		if err != nil {
			return err
		}
		offset, err := parseOptionalInt64(item["offset"])
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(item["size"], 10, 64)
		if err != nil {
			return err
		}
		return writeChecksumFrame(out, fileID, offset, size, encoding.FormatXXH128HashToken(xxh3.Hash128([]byte("wrong!!!"))))
	})
	defer srv.Close()

	_, _, _, partial, err := verifyCopyDataSamples(srv.URL, cfg, manifest, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("verifyCopyDataSamples() err = %v, want checksum mismatch", err)
	}
	if partial {
		t.Fatal("expected partial=false on mismatch")
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("expected one checksum request, got %d", got)
	}
}

func TestVerifyCopyDataSamplesForcedStopReturnsSuccess(t *testing.T) {
	withVerifyBudgetGracePeriod(t, 20*time.Millisecond)

	tmp := t.TempDir()
	body := []byte("abcdefghijklmno")
	if err := os.WriteFile(filepath.Join(tmp, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	manifest := &tx.Manifest{
		TransferID:  "txverify-forced-stop",
		Root:        "/remote",
		Concurrency: 1,
		Entries: []tx.ManifestEntry{
			{ID: 1, Size: int64(len(body)), Path: "a.txt"},
		},
	}
	cfg := copyCLIConfig{
		localDst:            tmp,
		verifyDataSamplePct: 100,
		verifyBudget:        10 * time.Millisecond,
		concurrency:         1,
	}

	var stderr bytes.Buffer
	var started atomic.Int64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbCXSUM {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		started.Add(1)
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	defer srv.Close()

	type result struct {
		files   int
		samples int
		partial bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		files, samples, _, partial, err := verifyCopyDataSamples(srv.URL, cfg, manifest, &stderr)
		done <- result{files: files, samples: samples, partial: partial, err: err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("verifyCopyDataSamples() err = %v", got.err)
		}
		if !got.partial {
			t.Fatal("expected partial=true")
		}
		if got.files != 0 || got.samples != 0 {
			t.Fatalf("verifyCopyDataSamples() = files=%d samples=%d, want 0/0", got.files, got.samples)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("verifyCopyDataSamples() did not return after forced stop")
	}

	if got := started.Load(); got != 1 {
		t.Fatalf("expected one checksum request, got %d", got)
	}
	if !strings.Contains(stderr.String(), "copy-verify-data: budget expired, verified 0/1 files 0 samples") {
		t.Fatalf("expected forced-stop budget log, got %q", stderr.String())
	}
}

func TestCompactETAUsesFractionalUnitsEarly(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{in: 59 * time.Second, want: "59s"},
		{in: 74 * time.Second, want: "1.2m"},
		{in: 95 * time.Minute, want: "1.6h"},
		{in: 36 * time.Hour, want: "1.5d"},
		{in: 15 * 24 * time.Hour, want: "2.1w"},
	}
	for _, tt := range tests {
		if got := compactETA(tt.in); got != tt.want {
			t.Fatalf("compactETA(%s) = %q, want %q", tt.in, got, tt.want)
		}
		if len(tt.want) > 5 {
			t.Fatalf("test case %q exceeds 5 chars", tt.want)
		}
	}
}

func TestVerbosityFromFlags(t *testing.T) {
	tests := []struct {
		name     string
		progress bool
		verbose  bool
		want     int
	}{
		{name: "quiet", progress: false, verbose: false, want: 0},
		{name: "progress", progress: true, verbose: false, want: 1},
		{name: "verbose", progress: false, verbose: true, want: 2},
		{name: "verbose wins", progress: true, verbose: true, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verbosityFromFlags(tt.progress, tt.verbose); got != tt.want {
				t.Fatalf("verbosityFromFlags(%v, %v) = %d, want %d", tt.progress, tt.verbose, got, tt.want)
			}
		})
	}
}

func TestParseCacheLoadFlag(t *testing.T) {
	tests := []struct {
		raw         string
		wantEnable  bool
		wantBudget  time.Duration
		wantErrPart string
	}{
		{raw: "none"},
		{raw: "full", wantEnable: true},
		{raw: "120s", wantEnable: true, wantBudget: 120 * time.Second},
		{raw: "5m", wantEnable: true, wantBudget: 5 * time.Minute},
		{raw: "0s", wantErrPart: "--cache-load duration must be > 0"},
		{raw: "-1s", wantErrPart: "--cache-load duration must be > 0"},
		{raw: "meta", wantErrPart: "unsupported --cache-load value"},
	}
	for _, tt := range tests {
		gotEnable, gotBudget, err := parseCacheLoadFlag(tt.raw)
		if tt.wantErrPart != "" {
			if err == nil || !strings.Contains(err.Error(), tt.wantErrPart) {
				t.Fatalf("parseCacheLoadFlag(%q) err = %v, want containing %q", tt.raw, err, tt.wantErrPart)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseCacheLoadFlag(%q): %v", tt.raw, err)
		}
		if gotEnable != tt.wantEnable || gotBudget != tt.wantBudget {
			t.Fatalf("parseCacheLoadFlag(%q) = (%v, %s), want (%v, %s)", tt.raw, gotEnable, gotBudget, tt.wantEnable, tt.wantBudget)
		}
	}
}

func TestPrintTransferErrors(t *testing.T) {
	buildErrors := func(n int) []error {
		errs := make([]error, 0, n)
		for i := 1; i <= n; i++ {
			errs = append(errs, fmt.Errorf("boom-%d", i))
		}
		return errs
	}

	t.Run("no errors", func(t *testing.T) {
		var buf bytes.Buffer
		printTransferErrors(&buf, "start", nil, 1)
		if got := buf.String(); got != "" {
			t.Fatalf("expected no output, got %q", got)
		}
	})

	t.Run("prints first five and summary when not verbose", func(t *testing.T) {
		var buf bytes.Buffer
		printTransferErrors(&buf, "start", buildErrors(7), 1)
		got := buf.String()
		for i := 1; i <= 5; i++ {
			want := fmt.Sprintf("start error: boom-%d\n", i)
			if !strings.Contains(got, want) {
				t.Fatalf("expected output to contain %q, got %q", want, got)
			}
		}
		if strings.Contains(got, "start error: boom-6\n") || strings.Contains(got, "start error: boom-7\n") {
			t.Fatalf("expected output to truncate after five errors, got %q", got)
		}
		if !strings.Contains(got, "start failed with 7 errors\n") {
			t.Fatalf("expected summary line, got %q", got)
		}
	})

	t.Run("prints all when verbose", func(t *testing.T) {
		var buf bytes.Buffer
		printTransferErrors(&buf, "sync", buildErrors(6), 2)
		got := buf.String()
		for i := 1; i <= 6; i++ {
			want := fmt.Sprintf("sync error: boom-%d\n", i)
			if !strings.Contains(got, want) {
				t.Fatalf("expected output to contain %q, got %q", want, got)
			}
		}
		if strings.Contains(got, "sync failed with 6 errors\n") {
			t.Fatalf("did not expect summary line in verbose mode, got %q", got)
		}
	})
}

func TestHumanBytesFixedWidthUsesPerValueUnits(t *testing.T) {
	zero := encoding.HumanBytesFixedWidth(0, fixedWidthProgressBytesWidth)
	mid := encoding.HumanBytesFixedWidth(492_340_000, fixedWidthProgressBytesWidth)
	done := encoding.HumanBytesFixedWidth(1_950_000_000, fixedWidthProgressBytesWidth)
	totalFormatted := encoding.HumanBytesFixedWidth(20_174_499_881, fixedWidthProgressBytesWidth)

	if zero != "       0 B" {
		t.Fatalf("unexpected zero progress bytes: %q", zero)
	}
	if mid != "469.53 MiB" || done != "  1.82 GiB" || totalFormatted != " 18.79 GiB" {
		t.Fatalf("unexpected fixed-width byte values: %q %q %q", mid, done, totalFormatted)
	}
}

func TestEffectiveModeLinkMbpsScalesGentleBandwidth(t *testing.T) {
	if got := effectiveModeLinkMbps(tx.LoadStrategyGentle, 8400, 25); got != 2100 {
		t.Fatalf("expected gentle link mbps 2100, got %d", got)
	}
	if got := effectiveModeLinkMbps(tx.LoadStrategyFast, 8400, 25); got != 8400 {
		t.Fatalf("expected fast link mbps 8400, got %d", got)
	}
}

func TestFormatProbeRateSuffixOmitsWhenNoProbeData(t *testing.T) {
	if got := formatProbeRateSuffix(time.Unix(400, 0), 10, &probeReporter{}); got != "" {
		t.Fatalf("expected empty suffix without probe data, got %q", got)
	}
}

func TestRunCLISyncNoOpSkipsPrompt(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, "", "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	subDir := filepath.Join(targetDir, "sub")
	if err := os.MkdirAll(subDir, 0o750); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	dirMtime := time.Unix(0, 90)
	if err := os.Chtimes(subDir, dirMtime, dirMtime); err != nil {
		t.Fatalf("chtimes subdir: %v", err)
	}
	destPath := filepath.Join(targetDir, "same.txt")
	if err := os.WriteFile(destPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write target file: %v", err)
	}
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatalf("stat target file: %v", err)
	}
	entry := buildTestManifestEntry(1, info.Size(), info.ModTime().UnixNano(), info.Mode(), "same.txt")
	dirEntry := buildTestDirManifestEntry(2, dirMtime.UnixNano(), 0o750, "sub")
	manifestRaw := buildTestManifestRaw("txsyncnoop", []string{entry, dirEntry})
	{
		m, err := tx.ParseManifest([]byte(manifestRaw))
		if err != nil {
			t.Fatalf("parse seed manifest: %v", err)
		}
		if err := tx.SaveManifest(filepath.Join(tmp, ".tx", "dst", "manifest.server.zst"), m); err != nil {
			t.Fatalf("save manifest.server.zst: %v", err)
		}
	}
	withSyncPromptTestInput(t, "\n", true)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncnoop", []string{entry, dirEntry}, nil)
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync no-op: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "sync: remote and local converged, nothing to do") {
		t.Fatalf("expected converged output, got: %s", stderr.String())
	}
	if strings.Contains(stderr.String(), "proceed?") {
		t.Fatalf("did not expect prompt for no-op sync, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncDownloadPromptDefaultsYes(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncdownload", nil), "")
	withSyncPromptTestInput(t, "\n", true)

	payload := []byte("hello")
	entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "new.txt")
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncdownload", []string{entry}, nil)
		case intftcp.VerbSEND:
			if got := req.Params[0]["txferid"]; got != "txsyncdownload" {
				return fmt.Errorf("unexpected transfer id: %q", got)
			}
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync download: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
	if err != nil {
		t.Fatalf("read synced file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected synced file: %q", string(got))
	}
	if !strings.Contains(stderr.String(), "proceed? [Y/n]: ") {
		t.Fatalf("expected [Y/n] prompt, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncDeletePromptDefaultsNo(t *testing.T) {
	tmp := t.TempDir()
	oldEntry := buildTestManifestEntry(1, 3, 100, 0o644, "old.txt")
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncdelete", []string{oldEntry}), "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	destPath := filepath.Join(targetDir, "old.txt")
	if err := os.WriteFile(destPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	withSyncPromptTestInput(t, "\n", true)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncdelete", nil, []uint64{1})
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync delete abort: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(destPath); err != nil {
		t.Fatalf("expected delete-only sync to abort before removing file: %v", err)
	}
	if !strings.Contains(stderr.String(), "proceed? [y/N]: ") {
		t.Fatalf("expected [y/N] prompt, got stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "aborted") {
		t.Fatalf("expected abort message, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncMixedPromptDefaultsNo(t *testing.T) {
	tmp := t.TempDir()
	oldEntry := buildTestManifestEntry(1, 3, 100, 0o644, "old.txt")
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncmixed", []string{oldEntry}), "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	oldPath := filepath.Join(targetDir, "old.txt")
	if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	withSyncPromptTestInput(t, "\n", true)

	entry := buildTestManifestEntry(2, 5, 100, 0o644, "new.txt")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncmixed", []string{entry}, []uint64{1})
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync mixed abort: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("expected mixed sync to abort before removing old file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected mixed sync to abort before downloading new file, err=%v", err)
	}
	if !strings.Contains(stderr.String(), "proceed? [y/N]: ") {
		t.Fatalf("expected [y/N] prompt, got stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "aborted") {
		t.Fatalf("expected abort message, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncPromptAcceptsExplicitYes(t *testing.T) {
	t.Run("download-default-yes", func(t *testing.T) {
		tmp := t.TempDir()
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesdownload", nil), "")
		withSyncPromptTestInput(t, "Y\n", true)

		payload := []byte("hello")
		entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "new.txt")
		meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				return writeSyncResponse(out, "txsyncyesdownload", []string{entry}, nil)
			case intftcp.VerbSEND:
				_, err := io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
				return err
			case intftcp.VerbACK:
				_, err := io.WriteString(out, "OK\r\n")
				return err
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync explicit yes download: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(filepath.Join(targetDir, "new.txt")); err != nil {
			t.Fatalf("expected new file to download: %v", err)
		}
		if !strings.Contains(stderr.String(), "proceed? [Y/n]: ") {
			t.Fatalf("expected [Y/n] prompt, got stderr=%s", stderr.String())
		}
	})

	t.Run("delete-default-no", func(t *testing.T) {
		tmp := t.TempDir()
		oldEntry := buildTestManifestEntry(1, 3, 100, 0o644, "old.txt")
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesdelete", []string{oldEntry}), "")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		oldPath := filepath.Join(targetDir, "old.txt")
		if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
			t.Fatalf("write old file: %v", err)
		}
		withSyncPromptTestInput(t, "y\n", true)

		syncCalls := 0
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				syncCalls++
				if syncCalls == 1 {
					return writeSyncResponse(out, "txsyncyesdelete", nil, []uint64{1})
				}
				return writeSyncResponse(out, "txsyncyesdelete", nil, nil)
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync explicit yes delete: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
			t.Fatalf("expected old file to be removed, err=%v", err)
		}
		if !strings.Contains(stderr.String(), "proceed? [y/N]: ") {
			t.Fatalf("expected [y/N] prompt, got stderr=%s", stderr.String())
		}
	})
}

func TestRunCLISyncNonTerminalSkipsPrompt(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncnonterm", nil), "")
	withSyncPromptTestInput(t, "", false)

	payload := []byte("hello")
	entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "new.txt")
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncnonterm", []string{entry}, nil)
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync non-terminal: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(targetDir, "new.txt")); err != nil {
		t.Fatalf("expected new file to download: %v", err)
	}
	if strings.Contains(stderr.String(), "proceed?") {
		t.Fatalf("did not expect prompt for non-terminal stdin, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncYesFlagBypassesPrompt(t *testing.T) {
	t.Run("delete-only", func(t *testing.T) {
		tmp := t.TempDir()
		oldEntry := buildTestManifestEntry(1, 3, 100, 0o644, "old.txt")
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesflagdelete", []string{oldEntry}), "")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		oldPath := filepath.Join(targetDir, "old.txt")
		if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
			t.Fatalf("write old file: %v", err)
		}
		withSyncPromptTestInput(t, "\n", true)

		syncCalls := 0
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				syncCalls++
				if syncCalls == 1 {
					return writeSyncResponse(out, "txsyncyesflagdelete", nil, []uint64{1})
				}
				return writeSyncResponse(out, "txsyncyesflagdelete", nil, nil)
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--yes", "--probe-size", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync --yes delete-only: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
			t.Fatalf("expected old file to be removed, err=%v", err)
		}
		if strings.Contains(stderr.String(), "proceed?") {
			t.Fatalf("did not expect prompt with --yes, got stderr=%s", stderr.String())
		}
	})

	t.Run("mixed", func(t *testing.T) {
		tmp := t.TempDir()
		oldEntry := buildTestManifestEntry(1, 3, 100, 0o644, "old.txt")
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesflagmixed", []string{oldEntry}), "")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		oldPath := filepath.Join(targetDir, "old.txt")
		if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
			t.Fatalf("write old file: %v", err)
		}
		withSyncPromptTestInput(t, "\n", true)

		payload := []byte("hello")
		entry := buildTestManifestEntry(2, int64(len(payload)), 100, 0o644, "new.txt")
		syncCalls := 0
		meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				syncCalls++
				if syncCalls == 1 {
					return writeSyncResponse(out, "txsyncyesflagmixed", []string{entry}, []uint64{1})
				}
				return writeSyncResponse(out, "txsyncyesflagmixed", []string{entry}, nil)
			case intftcp.VerbSEND:
				_, err := io.WriteString(out, buildCLIFrameWithMetadata(2, payload, 0, meta))
				return err
			case intftcp.VerbACK:
				_, err := io.WriteString(out, "OK\r\n")
				return err
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--yes", "--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync --yes mixed: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
			t.Fatalf("expected old file to be removed, err=%v", err)
		}
		got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
		if err != nil {
			t.Fatalf("read new file: %v", err)
		}
		if string(got) != string(payload) {
			t.Fatalf("unexpected new file contents: %q", string(got))
		}
		if strings.Contains(stderr.String(), "proceed?") {
			t.Fatalf("did not expect prompt with --yes, got stderr=%s", stderr.String())
		}
	})
}

func TestRunCLISyncSkipWriteSkipsTransferWhenOnlyNonFilesPending(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncskipwrite", nil), "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	filePath := filepath.Join(targetDir, "a.txt")
	if err := os.WriteFile(filePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	mtime := time.Unix(0, 100)
	if err := os.Chtimes(filePath, mtime, mtime); err != nil {
		t.Fatalf("chtimes local file: %v", err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat local file: %v", err)
	}
	fileEntry := buildTestManifestEntry(1, info.Size(), info.ModTime().UnixNano(), info.Mode(), "a.txt")
	dirEntry := "D2 0 0:100 0750 0:3:sub"

	var probeCount atomic.Int64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			probeCount.Add(1)
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncskipwrite", []string{fileEntry, dirEntry}, nil)
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--skip-write", "--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync skip-write non-files: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if got := probeCount.Load(); got != 1 {
		t.Fatalf("expected a single 1-byte discovery probe, got %d", got)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "sub")); !os.IsNotExist(err) {
		t.Fatalf("expected skip-write mode to skip directory creation, stat err=%v", err)
	}
}

func TestRunCLICopyStartPath(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	payload := []byte("hello")
	manifestRaw := buildTestManifestRaw("txcopy-start", []string{
		buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "new.txt"),
	})
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy start path: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
	if err != nil {
		t.Fatalf("read copied file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected copied file: %q", got)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".tx", "dst")); !os.IsNotExist(err) {
		t.Fatalf("expected copy to remove state dir, stat err=%v", err)
	}
}

func TestRunCLICopySyncPath(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	payload := []byte("hello")
	entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "new.txt")
	manifestRaw := buildTestManifestRaw("txcopy-sync", []string{entry})
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
	withSyncPromptTestInput(t, "", false)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSYNC:
			if got := req.Params[0]["directory"]; got != "/remote" {
				return fmt.Errorf("unexpected sync directory: %q", got)
			}
			return writeSyncResponse(out, "txcopy-sync", []string{entry}, nil)
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy sync path: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
	if err != nil {
		t.Fatalf("read synced file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected synced file: %q", got)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".tx", "dst")); !os.IsNotExist(err) {
		t.Fatalf("expected copy to remove state dir, stat err=%v", err)
	}
}

// TestRunCLICopyResumeFromPriorStateUnchanged verifies that when an
// interrupted copy left a populated .tx/<dst>/ but no LOCAL_DST, a
// subsequent `tx recv copy` invocation refreshes the manifest via SYNC,
// preserves the saved progress, and resumes downloads at the persisted
// per-file offsets instead of starting from zero.
func TestRunCLICopyResumeFromPriorStateUnchanged(t *testing.T) {
	tmp := t.TempDir()
	payload := []byte("helloworld")
	const partial = 5
	entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "a.txt")
	manifestRaw := buildTestManifestRaw("txcopy-resume", []string{entry})
	// Seed prior state: manifest.server, manifest.progress (auto-fingerprinted
	// by setupPinchState), and a partially-written staging file.
	targetDir := setupPinchState(t, tmp, manifestRaw, "1 5 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "a.txt"), payload[:partial], 0o644); err != nil {
		t.Fatalf("write partial staging file: %v", err)
	}
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	var sawTXFER bool
	var sawSYNC bool
	var sendOffset int64 = -1
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			sawTXFER = true
			return fmt.Errorf("resume path must not call TXFER")
		case intftcp.VerbSYNC:
			sawSYNC = true
			return writeSyncResponse(out, "txcopy-resume2", []string{entry}, nil)
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			off, err := parseOptionalInt64(target["offset"])
			if err != nil {
				return fmt.Errorf("parse offset: %w", err)
			}
			sendOffset = off
			_, err = io.WriteString(out, buildCLIFrameWithMetadata(1, payload[off:], off, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy resume: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if sawTXFER {
		t.Fatalf("resume path called TXFER, expected SYNC only")
	}
	if !sawSYNC {
		t.Fatalf("resume path did not call SYNC")
	}
	if sendOffset != partial {
		t.Fatalf("expected SEND offset=%d, got %d", partial, sendOffset)
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "a.txt"))
	if err != nil {
		t.Fatalf("read resumed file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected resumed file: %q", got)
	}
	if !strings.Contains(stderr.String(), "copy-resume-delta:") ||
		!strings.Contains(stderr.String(), "copy-resume-state:") {
		t.Fatalf("missing copy-resume summary lines: %s", stderr.String())
	}
}

// TestRunCLICopyResumeFromPriorStateChangedFile verifies that when the
// server reports a file with a different mtime than what we saved progress
// for, the saved progress is dropped and the file restarts from offset 0.
func TestRunCLICopyResumeFromPriorStateChangedFile(t *testing.T) {
	tmp := t.TempDir()
	payload := []byte("helloworld!!")
	priorEntry := buildTestManifestEntry(1, 10, 100, 0o644, "a.txt")
	priorManifestRaw := buildTestManifestRaw("txcopy-resume", []string{priorEntry})
	targetDir := setupPinchState(t, tmp, priorManifestRaw, "1 5 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write partial staging file: %v", err)
	}

	// SYNC response advertises a different size+mtime, so the prior progress
	// entry should not be carried forward.
	newEntry := buildTestManifestEntry(1, int64(len(payload)), 200, 0o644, "a.txt")
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 200, Mode: "0644"}

	var sendOffset int64 = -1
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txcopy-resume2", []string{newEntry}, nil)
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			off, err := parseOptionalInt64(target["offset"])
			if err != nil {
				return fmt.Errorf("parse offset: %w", err)
			}
			sendOffset = off
			_, err = io.WriteString(out, buildCLIFrameWithMetadata(1, payload[off:], off, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy resume changed: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if sendOffset != 0 {
		t.Fatalf("expected SEND offset=0 for changed file, got %d", sendOffset)
	}
	if !strings.Contains(stderr.String(), "stale[     1") {
		t.Fatalf("expected copy-resume to report one stale file, got: %s", stderr.String())
	}
}

// TestRunCLICopyResumeRemovedPathDropsStaging verifies that when the
// server's SYNC response includes RM lines, any partial staging bytes
// for those paths are removed and no SEND is issued for them.
func TestRunCLICopyResumeRemovedPathDropsStaging(t *testing.T) {
	tmp := t.TempDir()
	keptPayload := []byte("kept!")
	keepEntry := buildTestManifestEntry(1, int64(len(keptPayload)), 100, 0o644, "keep.txt")
	rmEntry := buildTestManifestEntry(2, 5, 100, 0o644, "gone.txt")
	priorManifestRaw := buildTestManifestRaw("txcopy-rm", []string{keepEntry, rmEntry})
	// Seed progress for both files.
	targetDir := setupPinchState(t, tmp, priorManifestRaw, "1 5 1\n2 3 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "keep.txt"), keptPayload, 0o644); err != nil {
		t.Fatalf("write keep staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "gone.txt"), []byte("xxx"), 0o644); err != nil {
		t.Fatalf("write gone staging: %v", err)
	}

	keepMeta := &tx.FileTrailerMetadata{Size: int64(len(keptPayload)), MtimeNS: 100, Mode: "0644"}

	var sentPaths []string
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			// New manifest: only keep.txt remains; gone.txt is removed (RM 2).
			return writeSyncResponse(out, "txcopy-rm2", []string{keepEntry}, []uint64{2})
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			path := target["path"]
			sentPaths = append(sentPaths, path)
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(1, keptPayload, 0, keepMeta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "--verify=none", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy resume rm: expected 0, got %d stderr=%s", code, stderr.String())
	}
	for _, p := range sentPaths {
		if strings.Contains(p, "gone.txt") {
			t.Fatalf("server received SEND for removed path: %v", sentPaths)
		}
	}
	if !strings.Contains(stderr.String(), "rm[     1]") {
		t.Fatalf("expected copy-resume to report one removed file, got: %s", stderr.String())
	}
	if _, err := os.Stat(filepath.Join(stagingDir, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected staging gone.txt to be removed, stat err=%v", err)
	}
}

// TestRunCLICopyCleanWipesStateDir verifies that --clean removes the
// .tx/<dst>/ state directory in addition to LOCAL_DST, forcing a full
// transfer via TXFER (not SYNC) even when a prior interrupted run left
// state behind.
func TestRunCLICopyCleanWipesStateDir(t *testing.T) {
	tmp := t.TempDir()
	payload := []byte("hello")
	entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "new.txt")
	priorManifestRaw := buildTestManifestRaw("txcopy-prior", []string{entry})
	targetDir := setupPinchState(t, tmp, priorManifestRaw, "1 3 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "new.txt"), []byte("xxx"), 0o644); err != nil {
		t.Fatalf("write staging: %v", err)
	}

	freshManifestRaw := buildTestManifestRaw("txcopy-fresh", []string{entry})
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	var sawTXFER, sawSYNC bool
	var sendOffset int64 = -1
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			sawTXFER = true
			if err := writeManifestResponse(out, freshManifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSYNC:
			sawSYNC = true
			return fmt.Errorf("--clean must not call SYNC")
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			off, err := parseOptionalInt64(target["offset"])
			if err != nil {
				return fmt.Errorf("parse offset: %w", err)
			}
			sendOffset = off
			_, err = io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "--clean", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy --clean: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if sawSYNC {
		t.Fatalf("--clean should bypass SYNC")
	}
	if !sawTXFER {
		t.Fatalf("--clean should call TXFER")
	}
	if sendOffset != 0 {
		t.Fatalf("expected SEND offset=0 after --clean, got %d", sendOffset)
	}
}

// TestRunCLICopyResumeFingerprintMismatch verifies that a manifest.progress
// file with a fingerprint that does not match the prior manifest is
// discarded with a warning, and downloads restart from offset 0.
func TestRunCLICopyResumeFingerprintMismatch(t *testing.T) {
	tmp := t.TempDir()
	payload := []byte("helloworld")
	entry := buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "a.txt")
	priorManifestRaw := buildTestManifestRaw("txcopy-fpmismatch", []string{entry})
	// Hand-craft a progress file with a clearly-wrong fingerprint header so
	// setupPinchState's auto-fingerprint logic does not overwrite it.
	rawProgress := progressFingerprintHeaderPrefix + "deadbeefdeadbeefdeadbeefdeadbeef\n1 5 0\n"
	targetDir := setupPinchState(t, tmp, priorManifestRaw, rawProgress)
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "a.txt"), payload[:5], 0o644); err != nil {
		t.Fatalf("write staging: %v", err)
	}
	meta := &tx.FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	var sendOffset int64 = -1
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txcopy-fpmismatch2", []string{entry}, nil)
		case intftcp.VerbSEND:
			target := req.Params[len(req.Params)-1]
			off, err := parseOptionalInt64(target["offset"])
			if err != nil {
				return fmt.Errorf("parse offset: %w", err)
			}
			sendOffset = off
			_, err = io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy fingerprint mismatch: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if sendOffset != 0 {
		t.Fatalf("expected SEND offset=0 after fingerprint mismatch, got %d", sendOffset)
	}
	if !strings.Contains(stderr.String(), "fingerprint mismatch") {
		t.Fatalf("expected fingerprint-mismatch warning, got: %s", stderr.String())
	}
}

// TestManifestFingerprintIgnoresHeader verifies that ManifestFingerprint
// hashes only entries — header fields (TransferID, LinkMbps, etc.) that
// vary per protocol call do not affect the fingerprint.
func TestManifestFingerprintIgnoresHeader(t *testing.T) {
	base := &tx.Manifest{
		TransferID:  "tid-A",
		Root:        "/remote",
		Mode:        tx.LoadStrategyFast,
		LinkMbps:    1000,
		Concurrency: 4,
		Entries: []tx.ManifestEntry{
			{ID: 0, Size: 10, Mtime: 100, Mode: 0o644, Path: "a.txt"},
			{ID: 1, Size: 20, Mtime: 200, Mode: 0o644, Path: "b.txt"},
			{Type: encoding.EntryTypeHard, ID: 2, Mode: 0o644, Path: "a-hard.txt", LinkTarget: 0},
		},
	}
	mutated := &tx.Manifest{
		TransferID:  "tid-B-different",
		Root:        "/remote",
		Mode:        tx.LoadStrategyGentle,
		LinkMbps:    9999,
		Concurrency: 1,
		DeadlineMS:  5000,
		Entries: []tx.ManifestEntry{
			// Same entries with different IDs (SYNC reassigns IDs).
			{ID: 42, Size: 10, Mtime: 100, Mode: 0o644, Path: "a.txt"},
			{ID: 43, Size: 20, Mtime: 200, Mode: 0o644, Path: "b.txt"},
			{Type: encoding.EntryTypeHard, ID: 44, Mode: 0o644, Path: "a-hard.txt", LinkTarget: 42},
		},
	}
	if got := tx.ManifestFingerprint(base); got != tx.ManifestFingerprint(mutated) {
		t.Fatalf("fingerprint should be stable across header / ID changes\n base=%s\n  mut=%s", got, tx.ManifestFingerprint(mutated))
	}
	// Mutating an entry changes the fingerprint.
	changed := &tx.Manifest{
		TransferID: "tid-A",
		Entries: []tx.ManifestEntry{
			{ID: 0, Size: 11, Mtime: 100, Mode: 0o644, Path: "a.txt"},
			{ID: 1, Size: 20, Mtime: 200, Mode: 0o644, Path: "b.txt"},
		},
	}
	if tx.ManifestFingerprint(base) == tx.ManifestFingerprint(changed) {
		t.Fatalf("fingerprint should change when an entry's size changes")
	}
}

func TestRunCLICopySkipFetchVerifyMeta(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	destPath := filepath.Join(targetDir, "same.txt")
	if err := os.WriteFile(destPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	if err := os.Chtimes(destPath, time.Unix(0, 100), time.Unix(0, 100)); err != nil {
		t.Fatalf("chtimes local file: %v", err)
	}
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatalf("stat local file: %v", err)
	}
	subDir := filepath.Join(targetDir, "sub")
	if err := os.MkdirAll(subDir, 0o750); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	dirMtime := time.Unix(0, 90)
	if err := os.Chtimes(subDir, dirMtime, dirMtime); err != nil {
		t.Fatalf("chtimes subdir: %v", err)
	}
	manifestRaw := buildTestManifestRaw("txcopy-verify", []string{
		buildTestDirManifestEntry(1, dirMtime.UnixNano(), 0o750, "sub"),
		buildTestManifestEntry(2, info.Size(), info.ModTime().UnixNano(), info.Mode(), "same.txt"),
	})
	if err := os.Chmod(targetDir, 0o755); err != nil {
		t.Fatalf("chmod target: %v", err)
	}
	rootInfo, err := os.Stat(targetDir)
	if err != nil {
		t.Fatalf("stat target root: %v", err)
	}
	rootFID := strconv.FormatUint(tx.RootFileID, 10)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case rootFID:
					if err := writeCLIMetadataFrame(out, tx.RootFileID, rootInfo.ModTime().UnixNano(), "0755"); err != nil {
						return err
					}
				case "1":
					if err := writeCLIMetadataFrame(out, 1, dirMtime.UnixNano(), "0750"); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--skip-fetch", "--verify", "meta", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy skip-fetch verify-meta: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "copy-verify-meta: [ok] total=2 files=1 hardlinks=0 symlinks=0 dirs=1") {
		t.Fatalf("expected verify output, got stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(tmp, ".tx", "dst")); err != nil {
		t.Fatalf("expected skip-fetch copy to preserve state dir, stat err=%v", err)
	}
}

func TestRunCLICopyMixedManifestTypesConverges(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	withSyncPromptTestInput(t, "", false)

	payload := []byte("hello")
	manifestEntries := []string{
		buildTestDirManifestEntry(1, 100, 0o750, "sub"),
		buildTestManifestEntry(2, int64(len(payload)), 100, 0o644, "sub/a.txt"),
		buildTestHardlinkManifestEntry(3, 2, 0o644, "sub/b.txt"),
		buildTestSymlinkManifestEntry(4, 100, 0o777, "sub/link.txt", "a.txt"),
	}
	manifestRaw := buildTestManifestRaw("txcopy-mixed", manifestEntries)
	meta := testOwnershipMetadata(int64(len(payload)), 100, "0644")
	rootFID := strconv.FormatUint(tx.RootFileID, 10)

	var sendCount atomic.Int64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txcopy-mixed", manifestEntries, nil)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case "2":
					sendCount.Add(1)
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(2, payload, 0, meta)); err != nil {
						return err
					}
				case rootFID:
					if err := writeCLIMetadataFrame(out, tx.RootFileID, 100, "0755"); err != nil {
						return err
					}
				case "1":
					if err := writeCLIMetadataFrame(out, 1, 100, "0750"); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy mixed first run: expected 0, got %d stderr=%s", code, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = RunCLI([]string{srv.URL, "copy", "--progress=false", "--verify", "meta", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy mixed second run: expected 0, got %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got := sendCount.Load(); got != 1 {
		t.Fatalf("expected exactly one SEND across both runs, got %d", got)
	}
	if !strings.Contains(stderr.String(), "copy-verify-meta: [ok] total=4 files=1 hardlinks=1 symlinks=1 dirs=1") {
		t.Fatalf("expected verify-meta output, got stdout=%s stderr=%s", stdout.String(), stderr.String())
	}

	subPath := filepath.Join(targetDir, "sub")
	info, err := os.Stat(subPath)
	if err != nil {
		t.Fatalf("stat sub: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o750 {
		t.Fatalf("sub mode: got %o, want 0750", perm)
	}
	aPath := filepath.Join(subPath, "a.txt")
	bPath := filepath.Join(subPath, "b.txt")
	linkPath := filepath.Join(subPath, "link.txt")
	aInfo, err := os.Stat(aPath)
	if err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}
	bInfo, err := os.Stat(bPath)
	if err != nil {
		t.Fatalf("stat b.txt: %v", err)
	}
	aStat := aInfo.Sys().(*syscall.Stat_t)
	bStat := bInfo.Sys().(*syscall.Stat_t)
	if aStat.Ino != bStat.Ino {
		t.Fatalf("expected hardlink inode match, got %d vs %d", aStat.Ino, bStat.Ino)
	}
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink link.txt: %v", err)
	}
	if target != "a.txt" {
		t.Fatalf("symlink target: got %q, want %q", target, "a.txt")
	}
}

func TestRunCLICopyMetadataApplyWarningStillRunsVerifyData(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	payload := []byte("hello")
	manifestRaw := buildTestManifestRaw("txcopy-metawarn", []string{
		buildTestManifestEntry(1, int64(len(payload)), 100, 0o644, "a.txt"),
	})
	fileMeta := &tx.FileTrailerMetadata{
		Size:    int64(len(payload)),
		MtimeNS: 100,
		Mode:    "0644",
		UID:     "not-a-number",
		GID:     strconv.Itoa(os.Getgid()),
	}
	rootFID := strconv.FormatUint(tx.RootFileID, 10)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case "1":
					if _, err := io.WriteString(out, buildCLIFrameWithMetadata(1, payload, 0, fileMeta)); err != nil {
						return err
					}
				case rootFID:
					if err := writeCLIMetadataFrame(out, tx.RootFileID, 100, "0755"); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected SEND fid: %q", p["fid"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbCXSUM:
			for _, target := range checksumTargetsFromRequest(t, req) {
				if target.FileID != 1 {
					return fmt.Errorf("unexpected checksum fid: %d", target.FileID)
				}
				hash := checksumTokenForRange(payload, target.Offset, target.Size)
				if err := writeChecksumFrame(out, target.FileID, target.Offset, target.Size, hash); err != nil {
					return err
				}
			}
			return nil
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "--skip-fsync", "--verify", "full", "/remote", targetDir}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("copy should fail when metadata mirroring fails\nstdout=%s\nstderr=%s", stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if strings.Contains(errText, "start error:") {
		t.Fatalf("metadata apply failure should be summarized at copy end, got: %s", errText)
	}
	if !strings.Contains(errText, "copy-verify-meta: [fail]") {
		t.Fatalf("expected verify-meta failure line, got: %s", errText)
	}
	if !strings.Contains(errText, "copy-verify-data: [ok]") {
		t.Fatalf("expected verify-data line after metadata failure, got: %s", errText)
	}
	if !strings.Contains(errText, "WARN copy: metadata apply") || !strings.Contains(errText, "a.txt") {
		t.Fatalf("expected final metadata warning with file path, got: %s", errText)
	}
}

func TestRunCLICopySendFailureReturnsNonzero(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := buildTestManifestRaw("txcopy-sendfail", []string{
		buildTestManifestEntry(1, 5, 100, 0o644, "a.txt"),
	})

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if err := writeManifestResponse(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			return fmt.Errorf("intentional SEND failure")
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "--verify", "none", "/remote", targetDir}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("copy should fail when SEND fails\nstdout=%s\nstderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "start error:") || !strings.Contains(stderr.String(), "intentional SEND failure") {
		t.Fatalf("expected SEND failure to be reported, got: %s", stderr.String())
	}
}

func TestRunCLIStatus(t *testing.T) {
	t.Run("list-all", func(t *testing.T) {
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			if req.Verb != intftcp.VerbSTATUS {
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
			_, err := io.WriteString(out, "OK 1\r\n{\"transfer_id\":\"abc\",\"directory\":\"/r\",\"num_files\":10,\"total_size\":1000,\"done\":3,\"done_size\":200,\"percent_files\":30.0,\"percent_bytes\":20.0,\"download_status\":{\"started\":5,\"running\":2,\"done\":3,\"missing\":0}}\r\n")
			return err
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := RunCLI([]string{srv.URL, "status"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("status list-all: expected 0, got %d stderr=%s", code, stderr.String())
		}
		output := stdout.String()
		if !strings.Contains(output, "[abc]") {
			t.Fatalf("expected transfer ID in output: %s", output)
		}
		if !strings.Contains(output, "source=[/r]") {
			t.Fatalf("expected source directory in output: %s", output)
		}
	})

	t.Run("poll-complete", func(t *testing.T) {
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			if req.Verb != intftcp.VerbSTATUS {
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
			_, err := io.WriteString(out, `OK {"transfer_id":"done1","directory":"/d","num_files":2,"total_size":500,"done":2,"done_size":500,"percent_files":100.0,"percent_bytes":100.0,"download_status":{"started":0,"running":0,"done":2,"missing":0}}`+"\r\n")
			return err
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := RunCLI([]string{srv.URL, "status", "--tid", "done1"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("status poll: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "transfer complete:") {
			t.Fatalf("expected completion output: %s", stdout.String())
		}
	})
}

func TestRunCLIUsageErrors(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if code := RunCLI([]string{}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2, got %d", code)
	}
	if code := RunCLI([]string{"127.0.0.1:1", "bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for unknown cmd, got %d", code)
	}
	// get requires exactly one REMOTE_PATH
	if code := RunCLI([]string{"127.0.0.1:1", "get"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing REMOTE_PATH on get, got %d", code)
	}
	// get requires REMOTE_PATH to be absolute
	if code := RunCLI([]string{"127.0.0.1:1", "get", "relative/path"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for relative REMOTE_PATH, got %d", code)
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "transfer", "--directory", "/tmp"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed transfer command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: transfer") {
		t.Fatalf("expected unknown transfer command, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "start", "--probe-size", "bad"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed start command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: start") {
		t.Fatalf("expected unknown start command, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "sync", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed sync command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: sync") {
		t.Fatalf("expected unknown sync command, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected copy help exit 0, got %d", code)
	}
	copyHelp := stderr.String()
	if !strings.Contains(copyHelp, `--clean`) || !strings.Contains(copyHelp, `(default false)`) {
		t.Fatalf("expected bool default in copy help, got: %s", copyHelp)
	}
	if !strings.Contains(copyHelp, `--concurrency int`) || !strings.Contains(copyHelp, `(default 0)`) {
		t.Fatalf("expected int default in copy help, got: %s", copyHelp)
	}
	if !strings.Contains(copyHelp, `--progress-interval string`) {
		t.Fatalf("expected progress-interval in copy help, got: %s", copyHelp)
	}
	lines := strings.Split(copyHelp, "\n")
	for _, line := range lines {
		if len(line) > 88 {
			t.Fatalf("expected copy help to wrap at 88 chars, got %d: %q", len(line), line)
		}
	}
	if !strings.Contains(copyHelp, "--verify string") {
		t.Fatalf("expected --verify string in copy help, got: %s", copyHelp)
	}
	if !strings.Contains(copyHelp, "--cache-load string") {
		t.Fatalf("expected --cache-load string in copy help, got: %s", copyHelp)
	}
	if strings.Contains(copyHelp, "--preserve-cache") || strings.Contains(copyHelp, "--with-cache-map") || strings.Contains(copyHelp, "--with-page-map") {
		t.Fatalf("expected old cache flags to be absent from copy help, got: %s", copyHelp)
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--preserve-cache", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --preserve-cache flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -preserve-cache") {
		t.Fatalf("expected removed --preserve-cache flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--with-cache-map", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --with-cache-map flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -with-cache-map") {
		t.Fatalf("expected removed --with-cache-map flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--with-page-map", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --with-page-map flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -with-page-map") {
		t.Fatalf("expected removed --with-page-map flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--compress", "bogus", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --compress, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --compress: unsupported --compress value") {
		t.Fatalf("expected invalid --compress error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--comp", "lz4", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --comp flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -comp") {
		t.Fatalf("expected removed --comp flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--per-file", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --per-file flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -per-file") {
		t.Fatalf("expected removed --per-file flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--probe-bytes", "1B", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --probe-bytes flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -probe-bytes") {
		t.Fatalf("expected removed --probe-bytes flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--probe-size", "bad", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --probe-size, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --probe-size") {
		t.Fatalf("expected invalid --probe-size error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--progress-interval", "bad", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --progress-interval, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --progress-interval") {
		t.Fatalf("expected invalid --progress-interval error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--cache-load", "0s", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --cache-load, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --cache-load") {
		t.Fatalf("expected invalid --cache-load error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--verify", "5%data", "--skip-fetch", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid verify/skip-fetch combo, got %d", code)
	}
	if !strings.Contains(stderr.String(), "--verify N%data/full cannot be used with --skip-fetch or --skip-write") {
		t.Fatalf("expected invalid verify data error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--verify", "0s", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for zero duration verify, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --verify") {
		t.Fatalf("expected invalid --verify error for 0s, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--verify", "30s", "--skip-fetch", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for verify duration with skip-fetch, got %d", code)
	}
	if !strings.Contains(stderr.String(), "--verify N%data/full cannot be used with --skip-fetch or --skip-write") {
		t.Fatalf("expected invalid verify/skip-fetch combo error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"--tid", "tx", "get"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing server address, got %d", code)
	}
	if !strings.Contains(stderr.String(), "host:port address") {
		t.Fatalf("expected explicit server address error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected help exit 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "  copy") || strings.Contains(stderr.String(), "\n  transfer") || strings.Contains(stderr.String(), "\n  start") || strings.Contains(stderr.String(), "\n  sync") {
		t.Fatalf("expected top-level help to mention copy only, got: %s", stderr.String())
	}
}

func TestVerboseProgressReporterIncludesAckedBytes(t *testing.T) {
	var stderr bytes.Buffer
	reporter := newVerboseProgressReporter(&stderr)
	t0 := time.Unix(0, 0)

	reporter.ReportUpdate(tx.DownloadProgressUpdate{
		FileID:      42,
		CopiedBytes: 20,
		TargetBytes: 100,
		UpdateTime:  t0,
	})
	reporter.ReportUpdate(tx.DownloadProgressUpdate{
		FileID:      42,
		AckBytes:    10,
		TargetBytes: 100,
		UpdateTime:  t0.Add(500 * time.Millisecond),
	})
	reporter.ReportUpdate(tx.DownloadProgressUpdate{
		FileID:      42,
		CopiedBytes: 40,
		TargetBytes: 100,
		UpdateTime:  t0.Add(1 * time.Second),
	})

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 progress lines, got %d: %q", len(lines), stderr.String())
	}
	if got := lines[0]; !strings.Contains(got, "file progress[42]: 20% bytes=") || !strings.Contains(got, "20 B/") || !strings.Contains(got, "[       0 B]") {
		t.Fatalf("unexpected first progress line: %q", got)
	}
	if got := lines[1]; !strings.Contains(got, "file progress[42]: 40% bytes=") || !strings.Contains(got, "40 B/") || !strings.Contains(got, "[      10 B]") {
		t.Fatalf("unexpected second progress line: %q", got)
	}
	for _, line := range lines {
		if strings.Contains(line, "tid=") {
			t.Fatalf("progress line should not include tid: %q", line)
		}
	}
}

func TestVerboseProgressReporterTimeCadenceAndCompletion(t *testing.T) {
	var stderr bytes.Buffer
	reporter := newVerboseProgressReporter(&stderr)
	t0 := time.Unix(0, 0)

	reporter.ReportUpdate(tx.DownloadProgressUpdate{
		FileID:      7,
		CopiedBytes: 5,
		TargetBytes: 100,
		UpdateTime:  t0,
	})
	reporter.ReportUpdate(tx.DownloadProgressUpdate{
		FileID:      7,
		CopiedBytes: 10,
		TargetBytes: 100,
		UpdateTime:  t0.Add(2 * time.Second),
	})
	reporter.ReportUpdate(tx.DownloadProgressUpdate{
		FileID:      7,
		CopiedBytes: 100,
		TargetBytes: 100,
		UpdateTime:  t0.Add(3 * time.Second),
	})

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 progress lines, got %d: %q", len(lines), stderr.String())
	}
	if got := lines[0]; !strings.Contains(got, "file progress[7]: 10% ") {
		t.Fatalf("expected timed 10%% line, got %q", got)
	}
	if got := lines[1]; !strings.Contains(got, "file progress[7]: 100% ") {
		t.Fatalf("expected final 100%% line, got %q", got)
	}
}

func TestVerboseProgressReporterConcurrentUse(t *testing.T) {
	var stderr bytes.Buffer
	reporter := newVerboseProgressReporter(&stderr)
	start := time.Unix(0, 0)

	var wg sync.WaitGroup
	runFile := func(fileID uint64) {
		defer wg.Done()
		for pct := int64(20); pct <= 100; pct += 20 {
			copied := pct
			reporter.ReportUpdate(tx.DownloadProgressUpdate{
				FileID:      fileID,
				CopiedBytes: copied,
				TargetBytes: 100,
				UpdateTime:  start.Add(time.Duration(copied) * time.Millisecond),
			})
			reporter.ReportUpdate(tx.DownloadProgressUpdate{
				FileID:      fileID,
				AckBytes:    copied / 2,
				TargetBytes: 100,
				UpdateTime:  start.Add(time.Duration(copied)*time.Millisecond + 500*time.Microsecond),
			})
		}
	}

	wg.Add(2)
	go runFile(1)
	go runFile(2)
	wg.Wait()

	out := stderr.String()
	if !strings.Contains(out, "file progress[1]: ") {
		t.Fatalf("expected fd=1 progress lines, got %q", out)
	}
	if !strings.Contains(out, "file progress[2]: ") {
		t.Fatalf("expected fd=2 progress lines, got %q", out)
	}
}

func TestProgressTotalsCountsResumedBaseline(t *testing.T) {
	entries := []tx.ManifestEntry{
		// Fully complete file from a prior run.
		{ID: 0, Size: 100, Type: encoding.EntryTypeFile, Progress: tx.ManifestProgress{AckBytes: 100, MetadataDone: true}},
		// Partial file (12% acked, no metadata yet).
		{ID: 1, Size: 50, Type: encoding.EntryTypeFile, Progress: tx.ManifestProgress{AckBytes: 6}},
		// Untouched file.
		{ID: 2, Size: 200, Type: encoding.EntryTypeFile},
		// Non-file entries should be ignored entirely.
		{ID: 3, Size: 999, Type: encoding.EntryTypeDir},
		{ID: 4, Size: 999, Type: encoding.EntryTypeSymlink},
	}
	totalBytes, totalFiles, priorBytes, priorFiles := progressTotals(entries)
	if totalBytes != 350 {
		t.Errorf("totalBytes: got %d want 350", totalBytes)
	}
	if totalFiles != 3 {
		t.Errorf("totalFiles: got %d want 3", totalFiles)
	}
	if priorBytes != 106 {
		t.Errorf("priorBytes: got %d want 106", priorBytes)
	}
	if priorFiles != 1 {
		t.Errorf("priorFiles: got %d want 1", priorFiles)
	}
}

func TestProgressTotalsClampsOutOfRangeAckBytes(t *testing.T) {
	entries := []tx.ManifestEntry{
		// AckBytes negative — should clamp to 0.
		{ID: 0, Size: 10, Type: encoding.EntryTypeFile, Progress: tx.ManifestProgress{AckBytes: -5}},
		// AckBytes > Size — should clamp to Size and count as copied for the
		// resume baseline even if metadata still needs refresh.
		{ID: 1, Size: 10, Type: encoding.EntryTypeFile, Progress: tx.ManifestProgress{AckBytes: 999}},
	}
	totalBytes, totalFiles, priorBytes, priorFiles := progressTotals(entries)
	if totalBytes != 20 || totalFiles != 2 {
		t.Errorf("totals: got bytes=%d files=%d, want bytes=20 files=2", totalBytes, totalFiles)
	}
	if priorBytes != 10 {
		t.Errorf("priorBytes: got %d want 10 (0 + clamp 10)", priorBytes)
	}
	if priorFiles != 1 {
		t.Errorf("priorFiles: got %d want 1", priorFiles)
	}
}

type signalWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	wrote chan struct{}
	once  sync.Once
}

func newSignalWriter() *signalWriter {
	return &signalWriter{wrote: make(chan struct{})}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.once.Do(func() { close(w.wrote) })
	return n, err
}

func (w *signalWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestVerboseStatusPollingUsesResumeFileBaseline(t *testing.T) {
	var statusCalls atomic.Int64
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbSTATUS {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		statusCalls.Add(1)
		_, err := io.WriteString(out, `OK {"transfer_id":"txresume","directory":"/r","num_files":10011,"total_size":500,"done":0,"done_size":0,"percent_files":0.0,"percent_bytes":0.0,"download_status":{"started":10011,"running":0,"done":0,"missing":0}}`+"\r\n")
		return err
	})
	defer srv.Close()

	client := tx.NewClient(srv.URL)
	defer client.Close()

	var copied atomic.Int64
	copied.Store(5 << 30)
	var doneFiles atomic.Uint64
	doneFiles.Store(4347)
	stderr := newSignalWriter()
	stop := startVerboseStatusPolling("txresume", client, &copied, 18<<30, &doneFiles, 10011, nil, stderr)

	select {
	case <-stderr.wrote:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for status progress; status calls=%d", statusCalls.Load())
	}
	stop()
	got := stderr.String()
	if !strings.Contains(got, "txfer-progress:[  4347/ 10011]") {
		t.Fatalf("expected resume file baseline in status progress, got: %s", got)
	}
	if !strings.Contains(got, "[  5.00 GiB/ 18.00 GiB]") {
		t.Fatalf("expected resume byte baseline in status progress, got: %s", got)
	}
}

// TestRunCLIStartProgressFileShowsResumedBytes guards against the bug where a
// partially-resumed transfer reported progress as 0% → (1-priorPct)% instead of
// priorPct% → 100%. The test forces a paced transfer so the progress writer's
// ticker fires mid-flight, then asserts every emitted progress line shows
// bytes.done >= priorBytes (i.e. progress never drops below the resume baseline).
func TestRunCLIStartProgressFileShowsResumedBytes(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstartprogressresume mode=fast link-mbps=700 concurrency=1",
		"D0 0 0:100 0755 0:7:/remote",
		"F1 10 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "1 5 0\n")
	stagingDir := filepath.Join(tmp, ".tx", "dst", "remote")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write partial staging file: %v", err)
	}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			// Slow the response so the 1ms progress ticker fires at least once
			// before the final stop write forces 100%.
			time.Sleep(50 * time.Millisecond)
			_, err := io.WriteString(out, buildCLIFrame(1, []byte("world"), 5)+"OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	progressPath := filepath.Join(tmp, "progress.txt")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{
		"--progress=false",
		"--skip-fsync",
		"--concurrency", "1",
		"--ack-every", "1KiB",
		"--progress-path", progressPath,
		"--progress-interval", "1ms",
		targetDir,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start resume with progress file: expected 0, got %d\nstderr=%s", code, stderr.String())
	}

	raw, err := os.ReadFile(progressPath)
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 {
		t.Fatalf("empty progress file")
	}
	for i, line := range lines {
		if !strings.Contains(line, `"total":10`) {
			t.Errorf("line %d: expected bytes total=10, got %s", i, line)
		}
		// Pull bytes.done out of the JSON manually — simple substring check
		// avoids importing encoding/json just to assert >= 5.
		marker := `"bytes":{"done":`
		idx := strings.Index(line, marker)
		if idx < 0 {
			t.Fatalf("line %d: missing bytes.done: %s", i, line)
		}
		rest := line[idx+len(marker):]
		end := strings.IndexByte(rest, ',')
		if end < 0 {
			t.Fatalf("line %d: malformed bytes.done: %s", i, line)
		}
		done, err := strconv.ParseInt(rest[:end], 10, 64)
		if err != nil {
			t.Fatalf("line %d: parse bytes.done: %v", i, err)
		}
		if done < 5 {
			t.Errorf("line %d: bytes.done=%d should be >= 5 (resume baseline): %s", i, done, line)
		}
	}
}
