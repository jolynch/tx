package ftcp

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/pagecache"
)

type fakeDeps struct {
	transfer   Transfer
	transferOK bool
}

func (f fakeDeps) NewTransfer(string, int, int64) (Transfer, error) { return Transfer{}, nil }
func (f fakeDeps) DeleteTransfer(string) bool                       { return true }
func (f fakeDeps) RegisterTransferFileState(string, <-chan TransferFileStateUpdate, uint8) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (f fakeDeps) ClipTransfer(string) bool                         { return true }
func (f fakeDeps) SetTransferHints(string, string, int64, int) bool { return true }
func (f fakeDeps) GetTransferGentleLimiter(string, int64, int, int64) *limit.Limiter {
	return nil
}
func (f fakeDeps) ReportTransferObservedLink(string, int64, int, int64, float64) (TransferObservedLinkUpdate, bool) {
	return TransferObservedLinkUpdate{}, false
}
func (f fakeDeps) GetTransfer(string) (Transfer, bool) {
	return f.transfer, f.transferOK
}
func (f fakeDeps) ListTransfers() []Transfer { return nil }
func (f fakeDeps) GetFile(string, uint64, string) (*os.File, FileRef, error) {
	return nil, FileRef{}, nil
}
func (f fakeDeps) GetFileRef(string, uint64, string) (FileRef, error) {
	return FileRef{}, nil
}
func (f fakeDeps) SetTransferFileState(string, uint64, uint8) bool { return true }
func (f fakeDeps) SetTransferFileWindowHash(string, uint64, int64, string) bool {
	return true
}
func (f fakeDeps) VerifyTransferFileWindowHash(string, uint64, int64, string) bool {
	return true
}
func (f fakeDeps) AcknowledgeTransferFile(string, uint64, int64) bool { return true }
func (f fakeDeps) SetTransferPageCache(string, uint64, []byte) bool   { return true }

func (f fakeDeps) SetTransferDeadline(string, int64) bool           { return false }
func (f fakeDeps) RecordTransferFirstSend(string) (time.Time, bool) { return time.Time{}, false }
func (f fakeDeps) MarkTransferTooSlow(string) bool                  { return false }
func (f fakeDeps) GetTransferLimiterBps(string) int64               { return 0 }
func (f fakeDeps) MaybeLogTransferProgress(string)                  {}
func (f fakeDeps) MaybeLogTransferComplete(string)                  {}
func (f fakeDeps) Root() string                                     { return "/" }
func (f fakeDeps) EnqueueCacheRestoreBatch(string, []pagecache.TouchEntry)   {}

func TestHandleSTATUSWritesStatusLine(t *testing.T) {
	req := Request{Verb: VerbSTATUS, Params: []map[string]string{{"txferid": "tx1"}}}
	deps := fakeDeps{
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
