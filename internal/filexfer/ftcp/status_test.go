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

// transferToStatus reports the store's per-state counts; which entries count
// (regular files only) is the store's invariant, checked by FuzzStoreStateCounts.
func TestTransferToStatusReportsStateCounts(t *testing.T) {
	tr := Transfer{
		ID:         "tx1",
		Directory:  "/tmp",
		NumEntries: 12,
		NumFiles:   10,
		TotalSize:  100,
		Done:       5,
		DoneSize:   100,
	}
	tr.StateCounts[TransferStateStarted] = 2
	tr.StateCounts[TransferStateRunning] = 3
	tr.StateCounts[TransferStateDone] = 1
	tr.StateCounts[TransferStateMissing] = 4
	status := transferToStatus("tx1", tr)

	if status.NumEntries != 12 || status.NumFiles != 10 {
		t.Fatalf("unexpected status counts: entries=%d files=%d", status.NumEntries, status.NumFiles)
	}
	if status.PercentFiles != 50 {
		t.Fatalf("expected 50%% file progress, got %.1f", status.PercentFiles)
	}
	want := encoding.DownloadStatus{Started: 2, Running: 3, Done: 1, Missing: 4}
	if status.DownloadStatus != want {
		t.Fatalf("download status = %+v, want %+v", status.DownloadStatus, want)
	}
}
