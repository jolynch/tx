package ftcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jolynch/tx/internal/filexfer/encoding"
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
	if !deps.SetTransferFileWindowHash(txferID, entries[0].ID, entries[0].Size, ackHash) {
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
		if !deps.SetTransferFileWindowHash(txferID, e.ID, e.Size, hash) {
			t.Fatalf("SetTransferFileWindowHash(%d) returned false", e.ID)
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
