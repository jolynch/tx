package ftcp

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleACKLogsCompleteAfterFinalProgress(t *testing.T) {
	root := t.TempDir()
	fullPath := filepath.Join(root, "a.txt")
	if err := os.WriteFile(fullPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
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

	ackRaw := fmt.Sprintf(`ACK %s fd=%d %q ack-token=%d@123@%s`, txferID, entries[0].ID, fullPath, entries[0].Size, ackHash)
	ackReq, err := ParseRequest([]byte(ackRaw))
	if err != nil {
		t.Fatalf("ParseRequest ACK failed: %v", err)
	}
	var ackOut bytes.Buffer
	if err := handleACK(context.Background(), ackReq, &ackOut, deps); err != nil {
		t.Fatalf("handleACK failed: %v", err)
	}

	logged := logs.String()
	progressIdx := strings.LastIndex(logged, "txfer-progress: tid="+txferID)
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
