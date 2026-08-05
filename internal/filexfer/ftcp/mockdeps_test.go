package ftcp

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/pagecache"
)

type ackCall struct {
	fileID   uint64
	ackBytes int64
}

// realDeps returns Deps backed by a real, isolated store — for tests that
// need genuine store semantics (ACK verification, the server loop) rather
// than a fake. The store is closed and discarded with the test, so nothing
// leaks into other tests.
func realDeps(t *testing.T, root string) Deps {
	t.Helper()
	st := store.NewStore()
	t.Cleanup(st.Close)
	return NewRuntimeDeps(st, WithRoot(root))
}

// mockDeps is the single Deps test double for this package. Handler tests
// configure the input fields they care about and assert on the recorded
// call fields; every method records unconditionally, so a test asserting a
// count of zero is still meaningful.
//
// Tests that need real store semantics use realDeps above — see ack_test.go
// and server_test.go.
type mockDeps struct {
	// Inputs.
	filePath   string // GetFile/GetFileRef serve this file when non-empty
	entryType  byte
	transfer   Transfer // returned by GetTransfer when transferOK
	transferOK bool

	// ReportTransferObservedLink return values.
	reportReturn   TransferObservedLinkUpdate
	reportReturnOK bool

	// Recorded calls.
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
	windowHashEnd    int64
	windowHash       string
	setStateCalls    int
	setWindowCalls   int
	reportTxferID    string
	reportObserved   int64
	reportBWPct      int
	reportBurst      int64
	reportEMAAlpha   float64
	reportCalled     bool
}

func (m *mockDeps) NewTransfer(string, int, int64) (Transfer, error) {
	return Transfer{ID: "tx123"}, nil
}

func (m *mockDeps) DeleteTransfer(string) bool { return true }

func (m *mockDeps) RegisterTransferFileState(string, <-chan TransferFileStateUpdate, uint8) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (m *mockDeps) ClipTransfer(string) bool { return true }

func (m *mockDeps) GetTransfer(string) (Transfer, bool) { return m.transfer, m.transferOK }

func (m *mockDeps) ListTransfers() []Transfer { return nil }

func (m *mockDeps) SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	m.setHintsCalls++
	m.setHintsTxID = txferID
	m.setHintsMode = mode
	m.setHintsMbps = linkMbps
	m.setHintsConc = concurrency
	return true
}

func (m *mockDeps) GetTransferGentleLimiter(string, int64, int, int64) *limit.Limiter {
	return nil
}

func (m *mockDeps) ReportTransferObservedLink(txferID string, observedLinkMbps int64, gentleBWPct int, burstBytes int64, emaAlpha float64) (TransferObservedLinkUpdate, bool) {
	m.reportCalled = true
	m.reportTxferID = txferID
	m.reportObserved = observedLinkMbps
	m.reportBWPct = gentleBWPct
	m.reportBurst = burstBytes
	m.reportEMAAlpha = emaAlpha
	return m.reportReturn, m.reportReturnOK
}

func (m *mockDeps) GetTransferLimiterBps(string) int64 { return 0 }

func (m *mockDeps) GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error) {
	if m.filePath == "" {
		return nil, FileRef{}, nil
	}
	fd, err := os.Open(m.filePath)
	if err != nil {
		return nil, FileRef{}, err
	}
	info, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return nil, FileRef{}, err
	}
	return fd, m.fileRef(txferID, fileID, info.Size()), nil
}

func (m *mockDeps) GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	if m.filePath == "" {
		return FileRef{}, nil
	}
	info, err := os.Stat(m.filePath)
	if err != nil {
		return FileRef{}, err
	}
	return m.fileRef(txferID, fileID, info.Size()), nil
}

func (m *mockDeps) fileRef(txferID string, fileID uint64, size int64) FileRef {
	return FileRef{
		TransferID: txferID,
		FileID:     fileID,
		Path:       m.filePath,
		Directory:  filepath.Dir(m.filePath),
		FileSize:   size,
		EntryType:  m.entryType,
	}
}

func (m *mockDeps) SetTransferFileState(string, uint64, uint8) bool {
	m.setStateCalls++
	return true
}

func (m *mockDeps) SetTransferFileWindowHash(_ string, _ uint64, endBytes int64, hashToken string) bool {
	m.setWindowCalls++
	m.windowHashEnd = endBytes
	m.windowHash = hashToken
	return true
}

func (m *mockDeps) VerifyTransferFileWindowHash(string, uint64, int64, string) bool { return true }

func (m *mockDeps) AcknowledgeTransferFile(_ string, fileID uint64, ackBytes int64) bool {
	m.ackCalls = append(m.ackCalls, ackCall{fileID: fileID, ackBytes: ackBytes})
	return true
}

func (m *mockDeps) SetTransferPageCache(string, uint64, []byte) bool { return true }

func (m *mockDeps) SetTransferDeadline(string, int64) bool           { return false }
func (m *mockDeps) RecordTransferFirstSend(string) (time.Time, bool) { return time.Time{}, false }
func (m *mockDeps) MarkTransferTooSlow(string) bool                  { return false }
func (m *mockDeps) MaybeLogTransferProgress(string)                  {}
func (m *mockDeps) MaybeLogTransferComplete(string)                  { m.completeCalls++ }
func (m *mockDeps) Root() string                                     { return "/" }

func (m *mockDeps) EnqueueCacheRestoreBatch(txferID string, items []pagecache.TouchEntry) {
	m.cacheRestoreCh = append(m.cacheRestoreCh, items...)
	m.cacheRestoreCall++
	m.cacheRestoreTxID = txferID
}
