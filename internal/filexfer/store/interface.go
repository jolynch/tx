package store

import (
	"os"
	"time"

	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/filexfer/policy"
)

// Interface is the transfer-state contract consumers depend on. *Store is
// the only implementation; methods on Store that are absent here have no
// production caller and exist for the store's own tests or benchmarks.
type Interface interface {
	// Transfer lifecycle.
	NewTransfer(directory string, numFiles int, totalSize int64, opts ...TransferOption) (Transfer, error)
	DeleteTransfer(txferID string) bool
	RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{}
	ClipTransfer(txferID string) bool

	// Reads and transfer-level hints. GetTransfer copies every per-file slice;
	// per-request and per-file paths use GetTransferSummary, and ListTransfers
	// returns summaries.
	GetTransfer(txferID string) (Transfer, bool)
	GetTransferSummary(txferID string) (Transfer, bool)
	ListTransfers() []Transfer
	SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool
	// A gentle SEND creates a transfer's limiter; an observed-link report
	// only retunes one that exists, so fast transfers never have one.
	GetTransferGentleLimiter(txferID string, fallbackLinkMbps int64, gentleBWPct int, burstBytes int64) *limit.Limiter
	ReportTransferObservedLink(txferID string, observedLinkMbps int64, gentleBWPct int, burstBytes int64, emaAlpha float64) (TransferObservedLinkUpdate, bool)
	GetTransferLimiterBps(txferID string) int64
	GetFileRefs(txferID string, lookups []FileLookup) ([]FileRef, []error)
	OpenFileRef(ref FileRef) (*os.File, error)

	// Per-file progress.
	SetTransferFilesState(txferID string, fileIDs []uint64, state uint8) bool
	SetTransferFileWindowHashes(txferID string, hashes []WindowHash) bool
	VerifyTransferFileWindowHashes(txferID string, hashes []WindowHash) []bool
	AcknowledgeTransferFiles(entries []AckEntry) bool
	SetTransferPageCache(txferID string, fileID uint64, blob []byte) bool

	// Per-file adaptive compression state, kept across a file's windows.
	// A SEND loads a window's state only when an earlier window preceded it
	// (offset above zero) and saves it only when another window follows, so
	// a single-window file never touches the store. Saves arrive with the
	// request's window hashes in RecordTransferWindows, one lock acquisition
	// per request; Load takes only the compression mutex. An entry is freed
	// when its file turns Done or Missing and when the transfer is deleted
	// or reaped; a save for a Done or Missing file is skipped. The same call
	// adds the request's delivery-rate summary to the transfer's, which the
	// completion log reports.
	LoadFileCompression(txferID string, fileID uint64) (policy.FileCompression, bool)
	RecordTransferWindows(txferID string, rec WindowRecords) bool

	// Deadlines, backing --exit-after.
	SetTransferDeadline(txferID string, deadlineMS int64) bool
	RecordTransferFirstSend(txferID string) (time.Time, bool)
	MarkTransferTooSlow(txferID string) bool

	MaybeLogTransferProgress(txferID string)
	MaybeLogTransferComplete(txferID string) bool
}

var _ Interface = (*Store)(nil)
