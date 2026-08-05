package ftcp

import (
	"errors"
	"net/http"
	"path/filepath"

	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/pagecache"
)

type Transfer = store.Transfer
type TransferFileStateUpdate = store.TransferFileStateUpdate
type FileRef = store.FileRef
type FileLookupError = store.FileLookupError
type TransferObservedLinkUpdate = store.TransferObservedLinkUpdate

const (
	TransferStateStarted = store.TransferStateStarted
	TransferStateRunning = store.TransferStateRunning
	TransferStateDone    = store.TransferStateDone
	TransferStateMissing = store.TransferStateMissing
)

// Deps is everything a handler needs: the transfer store, plus the two
// server-side concerns that are not the store's business.
type Deps interface {
	store.Interface

	// EnqueueCacheRestoreBatch hands a slice of cache-restore items to a
	// server-side producer goroutine that drains them into the worker
	// pool under a bounded timeout. txferID labels the batch in the
	// pool's begin/end log lines so cache-restore activity can be
	// attributed to the SYNC that caused it. The caller does not wait
	// for completion; items still in the producer's slice when the
	// timeout (or server shutdown) fires are abandoned. Best-effort, no
	// feedback to the caller.
	EnqueueCacheRestoreBatch(txferID string, items []pagecache.TouchEntry)

	Root() string
}

// runtimeDeps embeds the store, so every store method is promoted rather
// than forwarded by hand. Only the two non-store responsibilities below
// need implementing.
type runtimeDeps struct {
	store.Interface
	rootDir string
	pool    *pagecache.RestoreWorkerPool
}

// RuntimeDepsOption configures the dependency set built by NewRuntimeDeps.
type RuntimeDepsOption func(*runtimeDeps)

// WithRoot sets the chroot that file paths are resolved against.
func WithRoot(root string) RuntimeDepsOption {
	return func(d *runtimeDeps) {
		if root == "" {
			root = "/"
		}
		d.rootDir = filepath.Clean(root)
	}
}

// WithPool wires a shared cache-restore worker pool in so SYNC
// cache-map=recv has somewhere to enqueue work.
func WithPool(pool *pagecache.RestoreWorkerPool) RuntimeDepsOption {
	return func(d *runtimeDeps) { d.pool = pool }
}

// NewRuntimeDeps builds the dependency set around st. The store is a
// required argument rather than an option because there is no process-wide
// fallback: whoever builds the deps owns the store and closes it.
func NewRuntimeDeps(st store.Interface, opts ...RuntimeDepsOption) Deps {
	d := runtimeDeps{Interface: st, rootDir: "/"}
	for _, opt := range opts {
		opt(&d)
	}
	return d
}

func (d runtimeDeps) Root() string { return d.rootDir }

// EnqueueCacheRestoreBatch forwards to the pool's SendBatch, which
// spawns its own work-scoped producer goroutine and logs the batch's
// begin/end markers. The caller returns immediately.
func (d runtimeDeps) EnqueueCacheRestoreBatch(txferID string, items []pagecache.TouchEntry) {
	if d.pool == nil || len(items) == 0 {
		return
	}
	d.pool.SendBatch(txferID, items)
}

func mapLookupError(err error) error {
	if err == nil {
		return nil
	}
	var lookupErr *store.FileLookupError
	if errors.As(err, &lookupErr) {
		return protocolErr{code: mapHTTPErrorCode(lookupErr.Code), message: lookupErr.Msg}
	}
	return protocolErr{code: mapHTTPErrorCode(http.StatusInternalServerError), message: "internal server error"}
}
