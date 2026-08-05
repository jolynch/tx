package ftcp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

func TestHandleSTATUSWritesStatusLine(t *testing.T) {
	req := Request{Verb: VerbSTATUS, Params: []map[string]string{{"txferid": "tx1"}}}
	deps := &mockDeps{
		transferOK: true,
		transfer: Transfer{
			ID:         "tx1",
			Directory:  "/tmp",
			NumEntries: 1,
			NumFiles:   1,
			TotalSize:  100,
			Done:       1,
			DoneSize:   100,
			State:      []uint8{TransferStateDone},
			EntryType:  []byte{encoding.EntryTypeFile},
		},
	}

	var out bytes.Buffer
	if err := handleSTATUS(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleSTATUS err: %v", err)
	}
	line := strings.TrimSpace(out.String())
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("unexpected status line: %q", line)
	}
	if !strings.Contains(line, `"transfer_id":"tx1"`) {
		t.Fatalf("unexpected payload: %s", line)
	}
	if !strings.Contains(line, `"num_entries":1`) {
		t.Fatalf("unexpected payload: %s", line)
	}
}

func TestTransferToStatusUsesRegularFilesOnly(t *testing.T) {
	status := transferToStatus("tx1", Transfer{
		ID:         "tx1",
		Directory:  "/tmp",
		NumEntries: 3,
		NumFiles:   1,
		TotalSize:  100,
		Done:       1,
		DoneSize:   100,
		State:      []uint8{TransferStateDone, TransferStateDone, TransferStateStarted},
		EntryType:  []byte{encoding.EntryTypeFile, encoding.EntryTypeDir, encoding.EntryTypeSymlink},
	})

	if status.NumEntries != 3 || status.NumFiles != 1 {
		t.Fatalf("unexpected status counts: entries=%d files=%d", status.NumEntries, status.NumFiles)
	}
	if status.PercentFiles != 100 {
		t.Fatalf("expected 100%% file progress, got %.1f", status.PercentFiles)
	}
	if status.DownloadStatus.Done != 1 || status.DownloadStatus.Started != 0 || status.DownloadStatus.Running != 0 || status.DownloadStatus.Missing != 0 {
		t.Fatalf("unexpected download status: %+v", status.DownloadStatus)
	}
}
