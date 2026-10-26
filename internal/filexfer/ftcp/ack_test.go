package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/store"
)

func TestHandleACKLogsCompleteAfterFinalProgress(t *testing.T) {
	root := t.TempDir()
	fullPath := filepath.Join(root, "a.txt")
	if err := os.WriteFile(fullPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	deps := realDeps(t, "/")
	var txferID string
	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none`, root)
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

	var manifestOut bytes.Buffer
	if err := handleTXFERWithCallback(context.Background(), req, &manifestOut, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("handleTXFERWithCallback failed: %v", err)
	}
	entries, _ := parseSYNCResponseEntries(manifestOut.String(), nil)
	if len(entries) != 1 {
		t.Fatalf("expected one manifest entry, got %d", len(entries))
	}

	ackHash := "xxh128:0000000000000000000000000000000a"
	if !deps.SetTransferFileWindowHashes(txferID, []WindowHash{{FileID: entries[0].ID, EndBytes: entries[0].Size, HashToken: ackHash}}) {
		t.Fatalf("SetTransferFileWindowHash returned false")
	}

	ackReq, err := ParseRequest([]byte("ACK " + txferID))
	if err != nil {
		t.Fatalf("ParseRequest ACK failed: %v", err)
	}
	ackBody := framedItemBody(t, fmt.Sprintf(`fd=%d %q ack-token=%d@123@%s`, entries[0].ID, fullPath, entries[0].Size, ackHash))
	var ackOut bytes.Buffer
	if err := handleACKWithInput(context.Background(), ackReq, ackBody, &ackOut, deps); err != nil {
		t.Fatalf("handleACKWithInput failed: %v", err)
	}

	logged := logs.String()
	progressIdx := strings.LastIndex(logged, "txfer-progress:["+txferID+"]")
	completeIdx := strings.LastIndex(logged, "txfer-complete: tid="+txferID)
	if progressIdx < 0 {
		t.Fatalf("expected final progress log, got %q", logged)
	}
	if completeIdx < 0 {
		t.Fatalf("expected complete log, got %q", logged)
	}
	if progressIdx > completeIdx {
		t.Fatalf("expected progress before complete, got %q", logged)
	}
}

// An ACK applies all its records in one store call and checks progress and
// completion once per request, never per file. Under --exit-after it must not
// clone the transfer: that copy is O(files), which made each ACKed file cost
// O(files) and stalled every SEND behind the transfer lock.
func TestHandleACKTouchesTransferOncePerRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	mock := &mockDeps{filePath: path}
	deps := &exitAfterDeps{Deps: mock, onComplete: func(string) {}}

	const items = 8
	lines := make([]string, items)
	for i := range lines {
		lines[i] = fmt.Sprintf(`fd=%d %q ack-token=5@1@xxh128:0000000000000000000000000000000a`, i+1, path)
	}
	req, err := ParseRequest([]byte("ACK tx123"))
	if err != nil {
		t.Fatalf("ParseRequest ACK: %v", err)
	}
	var out bytes.Buffer
	if err := handleACKWithInput(context.Background(), req, framedItemBody(t, lines...), &out, deps); err != nil {
		t.Fatalf("handleACKWithInput: %v", err)
	}
	if len(mock.ackCalls) != items {
		t.Fatalf("applied %d acks, want %d", len(mock.ackCalls), items)
	}
	if mock.progressCalls != 1 || mock.completeCalls != 1 {
		t.Fatalf("progress checked %d times and completion %d times, want once each", mock.progressCalls, mock.completeCalls)
	}
	if mock.getTransferCalls != 0 {
		t.Fatalf("ACK cloned the transfer %d times", mock.getTransferCalls)
	}
	if mock.refBatches != 1 || mock.verifyBatches != 1 {
		t.Fatalf("ACK resolved files in %d calls and checked hashes in %d, want one each", mock.refBatches, mock.verifyBatches)
	}
}

// A later invalid acknowledgment must leave real store state untouched.
func TestHandleACKInvalidLateItemAppliesNothing(t *testing.T) {
	root := t.TempDir()
	const files = 40
	for i := 0; i < files; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%03d.bin", i)), []byte("hello"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	deps := realDeps(t, "/")

	req, err := ParseRequest([]byte(fmt.Sprintf("TXFER %q mode=fast link-mbps=100 concurrency=1 comp=none", root)))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var manifest bytes.Buffer
	var txferID string
	if err := handleTXFERWithCallback(context.Background(), req, &manifest, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("TXFER: %v", err)
	}
	entries, _ := parseSYNCResponseEntries(unframeManifestWire(t, manifest.Bytes()), nil)
	if len(entries) != files {
		t.Fatalf("expected %d entries, got %d", files, len(entries))
	}

	hash := "xxh128:0000000000000000000000000000000a"
	for _, e := range entries {
		if !deps.SetTransferFileWindowHashes(txferID, []WindowHash{{FileID: e.ID, EndBytes: e.Size, HashToken: hash}}) {
			t.Fatalf("SetTransferFileWindowHashes(%d) returned false", e.ID)
		}
	}

	// Every item is valid except one near the end, which carries a hash the
	// store will not verify.
	const badIndex = files - 5
	var body bytes.Buffer
	bw := encoding.NewFramedBodyWriter(&body, encoding.EncodingZstd, 4096, 0)
	for i, e := range entries {
		token := hash
		if i == badIndex {
			token = "xxh128:ffffffffffffffffffffffffffffffff"
		}
		fmt.Fprintf(bw, "fd=%d %q ack-token=%d@1@%s\n",
			e.ID, filepath.Join(root, filepath.Base(e.Path)), e.Size, token)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	ackReq, err := ParseRequest([]byte("ACK " + txferID))
	if err != nil {
		t.Fatalf("ParseRequest ACK: %v", err)
	}
	var out bytes.Buffer
	err = handleACKWithInput(context.Background(), ackReq, bytes.NewReader(body.Bytes()), &out, deps)
	if err == nil {
		t.Fatal("a batch containing an invalid item was accepted")
	}
	var pe protocolErr
	if !errors.As(err, &pe) || pe.code != "CONFLICT" {
		t.Fatalf("expected CONFLICT for the bad hash token, got %v", err)
	}

	// The items before the bad one are individually valid; none may have been
	// applied, which is what makes this a batch rather than a stream.
	if got, ok := deps.GetTransfer(txferID); !ok || got.DoneSize != 0 || got.Done != 0 {
		t.Fatalf("a rejected batch applied progress: Done=%d DoneSize=%d", got.Done, got.DoneSize)
	}
	if out.Len() != 0 {
		t.Fatalf("a rejected batch produced a response: %q", out.String())
	}
}

// An empty file's ACK never advances AckedSize, yet it must still count
// toward completion and clear its (fid, 0) window hash.
func TestHandleACKCompletesTransferWithEmptyFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "empty.txt"), nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	deps := realDeps(t, "/")

	var logs bytes.Buffer
	oldFlags := log.Flags()
	oldWriter := log.Writer()
	log.SetFlags(0)
	log.SetOutput(&logs)
	defer func() {
		log.SetFlags(oldFlags)
		log.SetOutput(oldWriter)
	}()

	req, err := ParseRequest([]byte(fmt.Sprintf("TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none", root)))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var manifest bytes.Buffer
	var txferID string
	if err := handleTXFERWithCallback(context.Background(), req, &manifest, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("TXFER: %v", err)
	}
	entries, _ := parseSYNCResponseEntries(unframeManifestWire(t, manifest.Bytes()), nil)
	if len(entries) != 2 {
		t.Fatalf("expected two manifest entries, got %d", len(entries))
	}

	// The empty file goes through a real SEND, which must leave it Running:
	// only its ACK may mark it Done and count it.
	var emptyID uint64
	for _, e := range entries {
		if e.Size == 0 {
			emptyID = e.ID
		}
	}
	emptyPath := filepath.Join(root, "empty.txt")
	sendReq, err := ParseRequest([]byte("SEND " + txferID))
	if err != nil {
		t.Fatalf("ParseRequest SEND: %v", err)
	}
	var sendOut bytes.Buffer
	if err := handleSENDWithOptions(context.Background(), sendReq, framedItemBody(t, fmt.Sprintf("fd=%d %q", emptyID, emptyPath)), &sendOut, deps, nil, defaultZeroCopyMinFrameBytes, 25); err != nil {
		t.Fatalf("SEND: %v", err)
	}
	frames, err := decodeFrameStream(sendOut.Bytes())
	if err != nil || len(frames) != 1 || frames[0].Trailer.FileHashToken == "" {
		t.Fatalf("SEND of empty file: frames=%d err=%v", len(frames), err)
	}
	emptyHash := frames[0].Trailer.FileHashToken
	if got, _ := deps.GetTransfer(txferID); got.State[emptyID] != TransferStateRunning || got.Done != 0 {
		t.Fatalf("after SEND: empty file state=%d Done=%d, want Running and uncounted", got.State[emptyID], got.Done)
	}

	hash := "xxh128:0000000000000000000000000000000a"
	var body bytes.Buffer
	bw := encoding.NewFramedBodyWriter(&body, encoding.EncodingZstd, 4096, 0)
	for _, e := range entries {
		token := emptyHash
		if e.ID != emptyID {
			token = hash
			if !deps.SetTransferFileWindowHashes(txferID, []WindowHash{{FileID: e.ID, EndBytes: e.Size, HashToken: hash}}) {
				t.Fatalf("SetTransferFileWindowHashes(%d) returned false", e.ID)
			}
		}
		fmt.Fprintf(bw, "fd=%d %q ack-token=%d@1@%s\n", e.ID, filepath.Join(root, filepath.Base(e.Path)), e.Size, token)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	ackReq, err := ParseRequest([]byte("ACK " + txferID))
	if err != nil {
		t.Fatalf("ParseRequest ACK: %v", err)
	}
	var out bytes.Buffer
	if err := handleACKWithInput(context.Background(), ackReq, bytes.NewReader(body.Bytes()), &out, deps); err != nil {
		t.Fatalf("ACK: %v", err)
	}
	if got, ok := deps.GetTransfer(txferID); !ok || got.Done != 2 || !got.CompleteLogged {
		t.Fatalf("transfer incomplete after ACK: Done=%d NumFiles=%d complete=%v", got.Done, got.NumFiles, got.CompleteLogged)
	}
	if !strings.Contains(logs.String(), "txfer-complete: tid="+txferID) {
		t.Fatalf("expected txfer-complete log, got %q", logs.String())
	}
	if deps.VerifyTransferFileWindowHashes(txferID, []WindowHash{{FileID: emptyID, EndBytes: 0, HashToken: emptyHash}})[0] {
		t.Fatalf("empty file's (fid, 0) window hash survived its ACK")
	}
}

// A SEND that fails to open a file answers 404, and the client ACKs it with
// -1, marking it Missing. The ENOENT can be transient: a later SEND of the
// same file succeeds, and its ACK must pass hash validation and revive the
// file, so the transfer completes with it Done rather than Missing.
func TestHandleACKRevivesMissingFile(t *testing.T) {
	root := t.TempDir()
	content := map[string]string{"a.txt": "hello", "b.txt": "world!"}
	for name, data := range content {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	deps := realDeps(t, "/")
	ctx := context.Background()

	req, err := ParseRequest([]byte(fmt.Sprintf("TXFER %q mode=fast link-mbps=1000 concurrency=8 comp=none", root)))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var manifest bytes.Buffer
	var txferID string
	if err := handleTXFERWithCallback(ctx, req, &manifest, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("TXFER: %v", err)
	}
	entries, _ := parseSYNCResponseEntries(unframeManifestWire(t, manifest.Bytes()), nil)
	if len(entries) != 2 {
		t.Fatalf("expected two manifest entries, got %d", len(entries))
	}
	ids := map[string]uint64{}
	for _, e := range entries {
		ids[filepath.Base(e.Path)] = e.ID
	}
	aPath, bPath := filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")
	aID, bID := ids["a.txt"], ids["b.txt"]

	send := func(fid uint64, path string) ([]decodedFrame, error) {
		t.Helper()
		sendReq, err := ParseRequest([]byte("SEND " + txferID))
		if err != nil {
			t.Fatalf("ParseRequest SEND: %v", err)
		}
		var out bytes.Buffer
		err = handleSENDWithOptions(ctx, sendReq, framedItemBody(t, fmt.Sprintf("fd=%d %q", fid, path)), &out, deps, nil, defaultZeroCopyMinFrameBytes, 25)
		frames, decodeErr := decodeFrameStream(out.Bytes())
		if err == nil && decodeErr != nil {
			t.Fatalf("decode SEND response: %v", decodeErr)
		}
		return frames, err
	}
	ack := func(items ...string) {
		t.Helper()
		ackReq, err := ParseRequest([]byte("ACK " + txferID))
		if err != nil {
			t.Fatalf("ParseRequest ACK: %v", err)
		}
		var out bytes.Buffer
		if err := handleACKWithInput(ctx, ackReq, framedItemBody(t, items...), &out, deps); err != nil {
			t.Fatalf("ACK %q: %v", items, err)
		}
	}

	// The first SEND finds a.txt gone and answers 404; the client marks it
	// Missing.
	if err := os.Rename(aPath, aPath+".away"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := send(aID, aPath); err == nil {
		t.Fatal("SEND of a vanished file succeeded")
	}
	ack(fmt.Sprintf("fd=%d %q ack-token=-1", aID, aPath))
	if got, _ := deps.GetTransfer(txferID); got.State[aID] != TransferStateMissing || got.Done != 1 {
		t.Fatalf("after missing ACK: state=%d Done=%d, want Missing and counted", got.State[aID], got.Done)
	}

	// The file is back, and a retry SEND delivers it whole.
	if err := os.Rename(aPath+".away", aPath); err != nil {
		t.Fatalf("rename back: %v", err)
	}
	frames, err := send(aID, aPath)
	if err != nil || len(frames) == 0 {
		t.Fatalf("retry SEND: frames=%d err=%v", len(frames), err)
	}
	aHash := frames[len(frames)-1].Trailer.FileHashToken
	if aHash == "" {
		t.Fatal("retry SEND returned no file hash")
	}
	bHash := "xxh128:0000000000000000000000000000000b"
	if !deps.SetTransferFileWindowHashes(txferID, []WindowHash{{FileID: bID, EndBytes: int64(len(content["b.txt"])), HashToken: bHash}}) {
		t.Fatal("SetTransferFileWindowHashes returned false")
	}
	ack(
		fmt.Sprintf("fd=%d %q ack-token=%d@1@%s", aID, aPath, len(content["a.txt"]), aHash),
		fmt.Sprintf("fd=%d %q ack-token=%d@1@%s", bID, bPath, len(content["b.txt"]), bHash),
	)

	got, _ := deps.GetTransfer(txferID)
	if got.State[aID] != TransferStateDone || got.AckedSize[aID] != int64(len(content["a.txt"])) {
		t.Fatalf("revived file: state=%d AckedSize=%d, want Done with every byte acknowledged", got.State[aID], got.AckedSize[aID])
	}
	if got.Done != 2 || !got.CompleteLogged || got.DoneSize != got.TotalSize {
		t.Fatalf("transfer: Done=%d NumFiles=%d DoneSize=%d TotalSize=%d complete=%v", got.Done, got.NumFiles, got.DoneSize, got.TotalSize, got.CompleteLogged)
	}

	var out bytes.Buffer
	statusReq := Request{Verb: VerbSTATUS, Params: []map[string]string{{"txferid": txferID}}}
	if err := handleSTATUS(ctx, statusReq, &out, deps); err != nil {
		t.Fatalf("STATUS: %v", err)
	}
	var status encoding.TransferStatus
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(out.String()), "OK ")), &status); err != nil {
		t.Fatalf("decode STATUS %q: %v", out.String(), err)
	}
	if status.DownloadStatus.Done != 2 || status.DownloadStatus.Missing != 0 || status.Done != 2 {
		t.Fatalf("STATUS reports %+v done=%d, want both files done and none missing", status.DownloadStatus, status.Done)
	}
}

// BenchmarkServePlaintextACKBody sends one plaintext ACK whose identity
// framed body lists 100,000 files, about 6 MiB, over loopback TCP to a real
// server, and waits for its OK. Every item acknowledges its file missing, so
// the work is reading, parsing, and validating the body rather than updating
// the store. Each op dials a connection, as a client without keep-alive does.
func BenchmarkServePlaintextACKBody(b *testing.B) {
	const files = 100_000
	const root = "/bench-ack"
	logOut := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(logOut) })

	st := store.NewStore()
	b.Cleanup(st.Close)
	transfer, err := st.NewTransfer(root, files, 0)
	if err != nil {
		b.Fatalf("NewTransfer: %v", err)
	}
	updates := make([]TransferFileStateUpdate, files)
	var body bytes.Buffer
	fmt.Fprintf(&body, "ACK %s\r\n", transfer.ID)
	w := encoding.NewFramedBodyWriter(&body, "none", encoding.DefaultBodyChunkSize, 0)
	for i := range files {
		path := fmt.Sprintf("%s/d%03d/file-%07d.bin", root, i%256, i+1)
		updates[i] = TransferFileStateUpdate{FileID: uint64(i + 1), PathHash: xxh3.Hash128([]byte(path)), FileSize: 1024}
		if _, err := fmt.Fprintf(w, "fd=%d %q ack-token=-1\n", i+1, path); err != nil {
			b.Fatalf("write item: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatalf("close body: %v", err)
	}
	st.RegisterTransferFileStates(transfer.ID, updates, TransferStateRunning)
	wire := body.Bytes()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- Serve(ln, ServerOptions{Deps: NewRuntimeDeps(st, WithRoot("/"))}) }()
	b.Cleanup(func() {
		_ = ln.Close()
		<-served
	})

	ack := func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := conn.Write(wire); err != nil {
			b.Fatalf("write ACK: %v", err)
		}
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "OK") {
			b.Fatalf("ACK reply %q: %v", line, err)
		}
	}
	ack() // mark every file missing; later ACKs leave the store unchanged
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ack()
	}
}
