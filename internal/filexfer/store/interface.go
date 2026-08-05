package store

import (
	"os"
	"time"

	"github.com/jolynch/tx/internal/filexfer/limit"
)

// Interface is the transfer-state contract consumers depend on. *Store is
// the only implementation; methods on Store that are absent here have no
// production caller and exist for the store's own tests or benchmarks.
type Interface interface {
	// Transfer lifecycle.
	NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error)
	DeleteTransfer(txferID string) bool
	RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{}
	ClipTransfer(txferID string) bool

	// Reads and transfer-level hints.
	GetTransfer(txferID string) (Transfer, bool)
	ListTransfers() []Transfer
	SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool
	GetTransferGentleLimiter(txferID string, fallbackLinkMbps int64, gentleBWPct int, burstBytes int64) *limit.Limiter
	ReportTransferObservedLink(txferID string, observedLinkMbps int64, gentleBWPct int, burstBytes int64, emaAlpha float64) (TransferObservedLinkUpdate, bool)
	GetTransferLimiterBps(txferID string) int64
	GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error)
	GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error)

	// Per-file progress.
	SetTransferFileState(txferID string, fileID uint64, state uint8) bool
	SetTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool
	VerifyTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool
	AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool
	SetTransferPageCache(txferID string, fileID uint64, blob []byte) bool

	// Deadlines, backing --exit-after.
	SetTransferDeadline(txferID string, deadlineMS int64) bool
	RecordTransferFirstSend(txferID string) (time.Time, bool)
	MarkTransferTooSlow(txferID string) bool

	MaybeLogTransferProgress(txferID string)
	MaybeLogTransferComplete(txferID string)
}

var _ Interface = (*Store)(nil)
