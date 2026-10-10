package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx/internal/events"
	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
)

const (
	defaultTransferTTL = 10 * time.Minute
	// State enum for transfer lifecycle. Note this block is iota-based:
	// inserting a line above TransferStateStarted shifts every state value.
	TransferStateStarted uint8 = iota
	TransferStateRunning
	TransferStateDone
	TransferStateMissing
)

// minReapInterval bounds how often the expiry goroutine wakes.
const minReapInterval = 10 * time.Millisecond

type Transfer struct {
	ID        string
	Directory string
	// RequestPath is the path the client asked for, including a single
	// file's name; Directory holds only that file's parent.
	RequestPath string
	Mode        string
	LinkMbps    int64
	Concurrency int
	NumEntries  int
	NumFiles    int
	TotalSize   int64
	Done        uint64
	DoneSize    int64
	State       []uint8
	// StateCounts counts regular-file entries by State, kept current on
	// every change so status reads need not scan State.
	StateCounts     [TransferStateMissing + 1]int
	EntryType       []byte
	PathHash        []xxh3.Uint128
	FileSize        []int64
	AckedSize       []int64
	PageCache       [][]byte // optional per-file page-cache hint blob; nil if not requested
	CreatedAt       time.Time
	ExpiresAt       time.Time
	DeadlineMS      int64     // 0 = no deadline
	FirstSendAt     time.Time // set on first gentle SEND
	TooSlow         bool      // sticky flag set when deadline is exceeded
	CompleteLogged  bool
	LastLogPct      int       // last byte-percent bucket logged (0, 10, ...)
	LastLogTime     time.Time // wall time of last progress log
	LastLogDoneSize int64     // DoneSize at last log (detect stalled transfers)
}

type TransferFileStateUpdate struct {
	FileID    uint64
	EntryType byte
	PathHash  xxh3.Uint128
	FileSize  int64
}

type FileRef struct {
	TransferID string
	FileID     uint64
	Path       string
	Directory  string
	FileSize   int64
	EntryType  byte
}

type FileLookupError struct {
	Code int
	Msg  string
}

func (e *FileLookupError) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

type managedTransfer struct {
	mu          sync.RWMutex
	deleted     bool
	transfer    Transfer
	observedEMA float64
	limiterBps  int64
	limiter     atomic.Pointer[limit.Limiter]

	// hashMu guards windowHashes alone, so recording or checking a window
	// hash needs only a shared hold of mu. Lock order: mu, then hashMu.
	hashMu       sync.Mutex
	windowHashes map[windowHashKey]windowHashState
}

// Store holds transfer state for a server. There is no process-wide
// instance: whoever runs a server owns a Store and closes it, and tests
// build their own so they cannot see each other's transfers.
type Store struct {
	// transfers maps a transfer ID to its *managedTransfer. Entries are
	// written once and read on every request, which sync.Map serves without
	// a store-wide lock.
	transfers  sync.Map
	reapPasses atomic.Int64 // reaper passes started; lets tests observe one in flight
	ttl        time.Duration
	done       chan struct{}
	stopped    chan struct{}
	closeOnce  sync.Once
	events     *events.Sink
}

type windowHashKey struct {
	fileID   uint64
	endBytes int64
}

type windowHashState struct {
	hashToken string
	expiresAt time.Time
}

type TransferObservedLinkUpdate struct {
	ObservedLinkMbps int64
	EMALinkMbps      float64
	RoundedLinkMbps  int64
	OldRateBps       int64
	NewRateBps       int64
}

// AckEntry is a single (transfer, file, ackBytes) tuple for batch acknowledgement.
type AckEntry struct {
	TxferID  string
	FileID   uint64
	AckBytes int64
}

// StoreOption configures a Store built by NewStore.
type StoreOption func(*Store)

// WithTTL overrides how long transfers survive past their last forward
// progress (see touchLocked), and how long window hashes survive past being
// stored. Chiefly useful to
// tests, which otherwise cannot reach the expiry paths inside the default
// ten minute window.
func WithTTL(d time.Duration) StoreOption {
	return func(s *Store) {
		if d > 0 {
			s.ttl = d
		}
	}
}

// WithEvents emits transfer_start when a transfer is created and
// transfer_done when it completes.
func WithEvents(sink *events.Sink) StoreOption {
	return func(s *Store) { s.events = sink }
}

// TransferOption configures one NewTransfer call.
type TransferOption func(*Transfer)

// WithRequestPath records the path the client requested.
func WithRequestPath(p string) TransferOption {
	return func(t *Transfer) { t.RequestPath = p }
}

// emitDone reports a transfer that just became complete. Callers invoke it
// after releasing the transfer's lock.
func (s *Store) emitDone(t Transfer) {
	s.events.Emit("transfer_done", t.ID,
		events.F("path", t.RequestPath),
		events.F("files", t.NumFiles),
		events.F("bytes", t.TotalSize),
		events.Dur("dur", time.Since(t.CreatedAt)))
}

// NewStore returns an isolated store with its own expiry goroutine. Callers
// are responsible for calling Close to stop that goroutine.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{
		ttl:     defaultTransferTTL,
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	go s.reapExpiredLoop()
	return s
}

// Close waits for the store's expiry goroutine to stop. It is safe to call
// repeatedly. Reads and writes still work afterwards; only reaping stops.
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		<-s.stopped
	})
}

func newManagedTransfer(transfer Transfer) *managedTransfer {
	return &managedTransfer{
		transfer:     transfer,
		windowHashes: make(map[windowHashKey]windowHashState),
	}
}

func shouldAdvanceState(current uint8, next uint8) bool {
	return next >= current
}

// countEntry adds delta to the StateCounts slot of entry idx, if it is a
// regular file. Callers uncount an entry before changing its State or
// EntryType and count it again afterwards.
func (t *Transfer) countEntry(idx int, delta int) {
	if !isRegularFileEntryType(t.EntryType[idx]) {
		return
	}
	if st := int(t.State[idx]); st < len(t.StateCounts) {
		t.StateCounts[st] += delta
	}
}

// setState moves entry idx to state, keeping StateCounts current.
func (t *Transfer) setState(idx int, state uint8) {
	t.countEntry(idx, -1)
	t.State[idx] = state
	t.countEntry(idx, 1)
}

func cloneTransfer(transfer Transfer) Transfer {
	out := transfer
	out.State = append([]uint8(nil), transfer.State...)
	out.EntryType = append([]byte(nil), transfer.EntryType...)
	out.PathHash = append([]xxh3.Uint128(nil), transfer.PathHash...)
	out.FileSize = append([]int64(nil), transfer.FileSize...)
	out.AckedSize = append([]int64(nil), transfer.AckedSize...)
	if transfer.PageCache != nil {
		out.PageCache = append([][]byte(nil), transfer.PageCache...)
	}
	return out
}

// summarizeTransfer copies only the transfer's scalar fields. The per-file
// slices are O(files), so a caller that runs per request or per file must use
// this rather than cloneTransfer.
func summarizeTransfer(transfer Transfer) Transfer {
	out := transfer
	out.State = nil
	out.EntryType = nil
	out.PathHash = nil
	out.FileSize = nil
	out.AckedSize = nil
	out.PageCache = nil
	return out
}

func isRegularFileEntryType(entryType byte) bool {
	return entryType == 0 || entryType == intencoding.EntryTypeFile
}

// formatTransferComplete returns the completion log line. Callers format it
// under the transfer lock and write it after releasing the lock, so a slow log
// destination never stalls the transfer.
func formatTransferComplete(t *Transfer) string {
	elapsed := time.Since(t.CreatedAt)
	speed := 0.0
	if elapsed.Seconds() > 0 {
		speed = float64(t.TotalSize) / elapsed.Seconds()
	}
	return fmt.Sprintf(
		"txfer-complete: tid=%s files=%d size=%s elapsed=%s speed=%s",
		t.ID,
		t.NumFiles,
		intencoding.HumanBytes(t.TotalSize),
		elapsed.Round(time.Millisecond),
		intencoding.HumanRate(speed),
	)
}

// MaybeLogTransferComplete logs the transfer's completion the first time every
// file is counted, and reports whether the transfer is complete.
func (s *Store) MaybeLogTransferComplete(txferID string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	if managed.deleted {
		managed.mu.Unlock()
		return false
	}
	t := &managed.transfer
	if t.CompleteLogged {
		managed.mu.Unlock()
		return true
	}
	if t.NumFiles <= 0 || t.Done != uint64(t.NumFiles) {
		managed.mu.Unlock()
		return false
	}
	t.CompleteLogged = true
	line := formatTransferComplete(t)
	done := summarizeTransfer(*t)
	managed.mu.Unlock()
	log.Print(line)
	s.emitDone(done)
	return true
}

func (s *Store) getManagedTransfer(txferID string) (*managedTransfer, bool) {
	v, ok := s.transfers.Load(txferID)
	if !ok {
		return nil, false
	}
	return v.(*managedTransfer), true
}

func (s *Store) create(transfer Transfer) bool {
	_, exists := s.transfers.LoadOrStore(transfer.ID, newManagedTransfer(transfer))
	return !exists
}

func (s *Store) SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.Mode = strings.ToLower(strings.TrimSpace(mode))
	managed.transfer.LinkMbps = linkMbps
	managed.transfer.Concurrency = concurrency
	if linkMbps > 0 {
		managed.observedEMA = float64(linkMbps)
	}
	return true
}

func deriveGentleRateBps(linkMbps int64, gentleBWPct int) int64 {
	if linkMbps <= 0 || gentleBWPct <= 0 {
		return 0
	}
	return (((linkMbps * 1_000_000) / 8) * int64(gentleBWPct)) / 100
}

func (s *Store) GetTransferGentleLimiter(txferID string, fallbackLinkMbps int64, gentleBWPct int, burstBytes int64) *limit.Limiter {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return nil
	}
	if limiter := managed.limiter.Load(); limiter != nil {
		return limiter
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return nil
	}
	if limiter := managed.limiter.Load(); limiter != nil {
		return limiter
	}

	linkMbps := int64(math.Round(managed.observedEMA))
	if linkMbps <= 0 {
		linkMbps = managed.transfer.LinkMbps
	}
	if linkMbps <= 0 {
		linkMbps = fallbackLinkMbps
	}
	if linkMbps <= 0 {
		return nil
	}
	rateBps := deriveGentleRateBps(linkMbps, gentleBWPct)
	if rateBps <= 0 {
		return nil
	}
	limiter, err := limit.NewLimiterFromBps(rateBps, burstBytes)
	if err != nil {
		return nil
	}
	managed.transfer.LinkMbps = linkMbps
	if managed.observedEMA <= 0 {
		managed.observedEMA = float64(linkMbps)
	}
	managed.limiterBps = rateBps
	managed.limiter.Store(limiter)
	return limiter
}

func (s *Store) GetTransferLimiterBps(txferID string) int64 {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return 0
	}
	managed.mu.RLock()
	defer managed.mu.RUnlock()
	return managed.limiterBps
}

func (s *Store) ReportTransferObservedLink(txferID string, observedLinkMbps int64, gentleBWPct int, burstBytes int64, emaAlpha float64) (TransferObservedLinkUpdate, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return TransferObservedLinkUpdate{}, false
	}
	if observedLinkMbps <= 0 {
		return TransferObservedLinkUpdate{}, false
	}
	if emaAlpha <= 0 || emaAlpha > 1 {
		emaAlpha = 0.2
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return TransferObservedLinkUpdate{}, false
	}

	ema := float64(observedLinkMbps)
	if managed.observedEMA > 0 {
		ema = (emaAlpha * float64(observedLinkMbps)) + ((1 - emaAlpha) * managed.observedEMA)
	}
	roundedLinkMbps := int64(math.Round(ema))
	if roundedLinkMbps <= 0 {
		roundedLinkMbps = observedLinkMbps
	}
	newRateBps := deriveGentleRateBps(roundedLinkMbps, gentleBWPct)
	update := TransferObservedLinkUpdate{
		ObservedLinkMbps: observedLinkMbps,
		EMALinkMbps:      ema,
		RoundedLinkMbps:  roundedLinkMbps,
		OldRateBps:       managed.limiterBps,
		NewRateBps:       newRateBps,
	}

	managed.observedEMA = ema
	managed.transfer.LinkMbps = roundedLinkMbps
	if newRateBps <= 0 {
		return update, true
	}
	if newRateBps == managed.limiterBps && managed.limiter.Load() != nil {
		return update, true
	}
	limiter, err := limit.NewLimiterFromBps(newRateBps, burstBytes)
	if err != nil {
		return TransferObservedLinkUpdate{}, false
	}
	managed.limiterBps = newRateBps
	managed.limiter.Store(limiter)
	return update, true
}

func (s *Store) SetTransferDeadline(txferID string, deadlineMS int64) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.DeadlineMS = deadlineMS
	return true
}

func (s *Store) RecordTransferFirstSend(txferID string) (time.Time, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return time.Time{}, false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return time.Time{}, false
	}
	if managed.transfer.FirstSendAt.IsZero() {
		managed.transfer.FirstSendAt = time.Now()
	}
	return managed.transfer.FirstSendAt, true
}

func (s *Store) MarkTransferTooSlow(txferID string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.TooSlow = true
	return true
}

func (s *Store) RegisterTransferFileStates(txferID string, updates []TransferFileStateUpdate, state uint8) {
	if len(updates) == 0 {
		return
	}
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return
	}

	ensureLen := func(n int) {
		if n <= len(managed.transfer.State) {
			return
		}
		oldLen := len(managed.transfer.State)
		growBy := n - oldLen
		managed.transfer.State = append(managed.transfer.State, make([]uint8, growBy)...)
		managed.transfer.EntryType = append(managed.transfer.EntryType, make([]byte, growBy)...)
		managed.transfer.PathHash = append(managed.transfer.PathHash, make([]xxh3.Uint128, growBy)...)
		managed.transfer.FileSize = append(managed.transfer.FileSize, make([]int64, growBy)...)
		managed.transfer.AckedSize = append(managed.transfer.AckedSize, make([]int64, growBy)...)
		if managed.transfer.PageCache != nil {
			for len(managed.transfer.PageCache) < n {
				managed.transfer.PageCache = append(managed.transfer.PageCache, nil)
			}
		}
		for i := oldLen; i < n; i++ {
			managed.transfer.State[i] = TransferStateStarted
			managed.transfer.countEntry(i, 1)
		}
	}

	added := false
	for _, update := range updates {
		idx := int(update.FileID)
		oldLen := len(managed.transfer.State)
		ensureLen(idx + 1)
		added = added || idx >= oldLen

		wasRegularFile := idx < oldLen && isRegularFileEntryType(managed.transfer.EntryType[idx])
		if update.FileID != intencoding.RootFileID && idx >= oldLen {
			managed.transfer.NumEntries++
		}
		managed.transfer.countEntry(idx, -1)
		managed.transfer.EntryType[idx] = update.EntryType
		isRegularFile := isRegularFileEntryType(update.EntryType)
		if !wasRegularFile && isRegularFile {
			managed.transfer.NumFiles++
		}
		if wasRegularFile && !isRegularFile {
			managed.transfer.NumFiles--
		}

		managed.transfer.TotalSize += update.FileSize - managed.transfer.FileSize[idx]
		managed.transfer.FileSize[idx] = update.FileSize
		managed.transfer.PathHash[idx] = update.PathHash
		if shouldAdvanceState(managed.transfer.State[idx], state) {
			managed.transfer.State[idx] = state
		}
		managed.transfer.countEntry(idx, 1)
	}
	// A batch that grows the transfer is manifest-walk progress. Re-registering
	// known entries is not, and entries are finite, so this stays bounded.
	if added {
		s.touchLocked(managed, time.Now())
	}
}

func (s *Store) DeleteTransfer(txferID string) bool {
	v, ok := s.transfers.LoadAndDelete(txferID)
	if !ok {
		return false
	}
	managed := v.(*managedTransfer)
	managed.mu.Lock()
	managed.deleted = true
	managed.mu.Unlock()
	return true
}

func (s *Store) GetTransfer(txferID string) (Transfer, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return Transfer{}, false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return Transfer{}, false
	}
	return cloneTransfer(managed.transfer), true
}

// GetTransferSummary returns the transfer without its per-file slices, at a
// cost independent of the file count.
func (s *Store) GetTransferSummary(txferID string) (Transfer, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return Transfer{}, false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return Transfer{}, false
	}
	return summarizeTransfer(managed.transfer), true
}

func (s *Store) GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	refs, errs := s.GetFileRefs(txferID, []FileLookup{{FileID: fileID, Path: fullPathRaw}})
	return refs[0], errs[0]
}

// FileLookup names one file of a transfer by ID and the path a request gave.
type FileLookup struct {
	FileID uint64
	Path   string
}

// GetFileRefs resolves each lookup as GetFileRef does, under one acquisition
// of the transfer's read lock, so a request resolves all its files at once
// rather than contending for the lock once per file. refs[i] and errs[i]
// answer lookups[i].
func (s *Store) GetFileRefs(txferID string, lookups []FileLookup) (refs []FileRef, errs []error) {
	refs = make([]FileRef, len(lookups))
	errs = make([]error, len(lookups))
	// A relative path is rejected before the transfer is even looked up.
	paths := make([]string, len(lookups))
	for i, l := range lookups {
		paths[i] = filepath.Clean(l.Path)
		if !filepath.IsAbs(paths[i]) {
			errs[i] = &FileLookupError{Code: http.StatusBadRequest, Msg: "path must be absolute"}
		}
	}
	fail := func(err error) ([]FileRef, []error) {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = err
			}
		}
		return refs, errs
	}
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return fail(&FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"})
	}

	// Copy what each lookup needs under the lock; check paths after it.
	type entry struct {
		digest    xxh3.Uint128
		size      int64
		entryType byte
		inRange   bool
	}
	entries := make([]entry, len(lookups))
	managed.mu.RLock()
	if managed.deleted {
		managed.mu.RUnlock()
		return fail(&FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"})
	}
	directory := managed.transfer.Directory
	for i, l := range lookups {
		if l.FileID == intencoding.RootFileID || l.FileID >= uint64(len(managed.transfer.State)) {
			continue
		}
		e := entry{
			digest:    managed.transfer.PathHash[l.FileID],
			size:      managed.transfer.FileSize[l.FileID],
			entryType: byte(intencoding.EntryTypeFile),
			inRange:   true,
		}
		if l.FileID < uint64(len(managed.transfer.EntryType)) && managed.transfer.EntryType[l.FileID] != 0 {
			e.entryType = managed.transfer.EntryType[l.FileID]
		}
		entries[i] = e
	}
	managed.mu.RUnlock()

	for i, l := range lookups {
		if errs[i] != nil {
			continue
		}
		fullPath := paths[i]
		ref := FileRef{TransferID: txferID, FileID: l.FileID, Path: fullPath, Directory: directory}
		switch {
		case l.FileID == intencoding.RootFileID:
			if fullPath != filepath.Clean(directory) {
				errs[i] = &FileLookupError{Code: http.StatusForbidden, Msg: "root metadata path must equal transfer root"}
				continue
			}
			ref.EntryType = intencoding.EntryTypeDir
			refs[i] = ref
		case !entries[i].inRange:
			errs[i] = &FileLookupError{Code: http.StatusNotFound, Msg: "file id out of range"}
		case !utils.PathWithinRoot(directory, fullPath):
			errs[i] = &FileLookupError{Code: http.StatusForbidden, Msg: "path must be within transfer root"}
		case xxh3.Hash128([]byte(fullPath)) != entries[i].digest:
			errs[i] = &FileLookupError{Code: http.StatusForbidden, Msg: "file path digest mismatch"}
		default:
			ref.FileSize = entries[i].size
			ref.EntryType = entries[i].entryType
			refs[i] = ref
		}
	}
	return refs, errs
}

// ListTransfers returns a summary of every transfer: scalars only, as
// GetTransferSummary returns.
func (s *Store) ListTransfers() []Transfer {
	var out []Transfer
	s.transfers.Range(func(_, v any) bool {
		managed := v.(*managedTransfer)
		managed.mu.RLock()
		if !managed.deleted {
			out = append(out, summarizeTransfer(managed.transfer))
		}
		managed.mu.RUnlock()
		return true
	})
	return out
}

func (s *Store) SetTransferPageCache(txferID string, fileID uint64, blob []byte) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	idx := int(fileID)
	if managed.transfer.PageCache == nil {
		managed.transfer.PageCache = make([][]byte, len(managed.transfer.State))
	}
	for len(managed.transfer.PageCache) <= idx {
		managed.transfer.PageCache = append(managed.transfer.PageCache, nil)
	}
	managed.transfer.PageCache[idx] = blob
	return true
}

func (s *Store) SetTransferFileState(txferID string, fileID uint64, state uint8) bool {
	return s.SetTransferFilesState(txferID, []uint64{fileID}, state)
}

// SetTransferFilesState advances each listed file to state under one
// acquisition of the transfer lock, so a SEND marks all its items Running at
// once rather than contending for the lock once per file. A file already at
// or past state is left alone. It refreshes the TTL once if any file advanced,
// and reports false if any file ID is out of range.
func (s *Store) SetTransferFilesState(txferID string, fileIDs []uint64, state uint8) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	ok = true
	advanced := false
	for _, fileID := range fileIDs {
		if fileID >= uint64(len(managed.transfer.State)) {
			ok = false
			continue
		}
		idx := int(fileID)
		if !shouldAdvanceState(managed.transfer.State[idx], state) || managed.transfer.State[idx] == state {
			continue
		}
		managed.transfer.setState(idx, state)
		advanced = true
	}
	if advanced {
		s.touchLocked(managed, time.Now())
	}
	return ok
}

func (s *Store) AcknowledgeTransferFiles(entries []AckEntry) bool {
	grouped := make(map[string][]AckEntry)
	order := make([]string, 0)
	for _, entry := range entries {
		if _, ok := grouped[entry.TxferID]; !ok {
			order = append(order, entry.TxferID)
		}
		grouped[entry.TxferID] = append(grouped[entry.TxferID], entry)
	}

	ok := true
	for _, txferID := range order {
		managed, found := s.getManagedTransfer(txferID)
		if !found {
			ok = false
			continue
		}

		managed.mu.Lock()
		if managed.deleted {
			managed.mu.Unlock()
			ok = false
			continue
		}
		for _, entry := range grouped[txferID] {
			if !s.acknowledgeFileLocked(managed, entry.FileID, entry.AckBytes) {
				ok = false
			}
		}
		managed.mu.Unlock()
	}
	return ok
}

func (s *Store) acknowledgeFileLocked(managed *managedTransfer, fileID uint64, ackBytes int64) bool {
	if managed == nil || managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) {
		return false
	}
	idx := int(fileID)
	currentState := managed.transfer.State[idx]

	if currentState == TransferStateMissing {
		return true
	}

	if ackBytes == -1 {
		if shouldAdvanceState(currentState, TransferStateMissing) {
			wasTerminal := currentState == TransferStateDone || currentState == TransferStateMissing
			managed.transfer.setState(idx, TransferStateMissing)
			// NumFiles counts only regular files and completion needs
			// Done == NumFiles exactly, so a missing directory or
			// symlink must not be counted.
			if !wasTerminal && isRegularFileEntryType(managed.transfer.EntryType[idx]) {
				managed.transfer.Done++
				s.touchLocked(managed, time.Now())
			}
		}
		return true
	}

	target := ackBytes
	if target < 0 {
		target = 0
	}
	maxAck := managed.transfer.FileSize[idx]
	if maxAck < 0 {
		maxAck = 0
	}
	if target > maxAck {
		target = maxAck
	}

	now := time.Now()
	if prev := managed.transfer.AckedSize[idx]; target > prev {
		managed.transfer.AckedSize[idx] = target
		managed.transfer.DoneSize += target - prev
		s.touchLocked(managed, now)
	}

	// One completion rule for every size. State doubles as the "counted"
	// marker: SEND never marks a regular file Done, so a file below Done has
	// not been counted yet. This runs even when target did not advance,
	// which is the only way an empty file is ever counted.
	if target == maxAck && managed.transfer.State[idx] < TransferStateDone && isRegularFileEntryType(managed.transfer.EntryType[idx]) {
		managed.transfer.setState(idx, TransferStateDone)
		managed.transfer.Done++
		s.touchLocked(managed, now)
	}

	managed.hashMu.Lock()
	delete(managed.windowHashes, windowHashKey{fileID: fileID, endBytes: target})
	managed.hashMu.Unlock()
	return true
}

// touchLocked restarts the transfer's TTL. It is called only where the
// transfer irreversibly advances (the manifest walk registers new entries or
// finishes, acknowledged bytes grow, a file is counted, a file state moves
// forward), never on reads or repeated requests, so a client cannot hold
// state alive without making progress. A single SEND window and
// post-completion CXSUM verification do not refresh it.
// Window hashes keep their own store-time expiry: a window is ACKed soon
// after SEND writes it, so they need no refresh. The caller holds m.mu.
func (s *Store) touchLocked(m *managedTransfer, now time.Time) {
	m.transfer.ExpiresAt = now.Add(s.ttl)
}

func (s *Store) ClipTransfer(txferID string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	if managed.deleted {
		managed.mu.Unlock()
		return false
	}
	managed.transfer.State = slices.Clip(managed.transfer.State)
	managed.transfer.PathHash = slices.Clip(managed.transfer.PathHash)
	managed.transfer.FileSize = slices.Clip(managed.transfer.FileSize)
	managed.transfer.AckedSize = slices.Clip(managed.transfer.AckedSize)
	if managed.transfer.PageCache != nil {
		managed.transfer.PageCache = slices.Clip(managed.transfer.PageCache)
	}

	// The manifest walk is over; a long walk must not eat the TTL the
	// transfer of those files needs.
	s.touchLocked(managed, time.Now())
	t := &managed.transfer
	lines := []string{fmt.Sprintf(
		"txfer-start: tid=%s dir=%s mode=%s entries=%d files=%d size=%s link=%dMbps concurrency=%d",
		t.ID, t.Directory, t.Mode,
		t.NumEntries,
		t.NumFiles,
		intencoding.HumanBytes(t.TotalSize),
		t.LinkMbps, t.Concurrency,
	)}
	emptyDone := t.NumFiles == 0
	if emptyDone {
		t.CompleteLogged = true
		lines = append(lines, formatTransferComplete(t))
	}
	done := summarizeTransfer(*t)
	managed.mu.Unlock()
	for _, line := range lines {
		log.Print(line)
	}
	if emptyDone {
		s.emitDone(done)
	}
	return true
}

func (s *Store) reapExpiredLoop() {
	defer close(s.stopped)

	// Reap at half the TTL. The floor only guards against a pathologically
	// small WithTTL spinning the goroutine; the default ten minute TTL
	// yields a five minute interval either way.
	interval := s.ttl / 2
	if interval < minReapInterval {
		interval = minReapInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		var now time.Time
		select {
		case <-s.done:
			return
		case now = <-ticker.C:
		}

		s.reapPasses.Add(1)
		s.transfers.Range(func(id, v any) bool {
			managed := v.(*managedTransfer)
			// Progress can renew the deadline after an ACK has looked up
			// this transfer. Check and mark deletion under one lock so that
			// renewal cannot be lost; a later lookup sees deleted.
			managed.mu.Lock()
			expired := !managed.deleted && !managed.transfer.ExpiresAt.After(now)
			if expired {
				managed.deleted = true
			}
			managed.mu.Unlock()
			if expired {
				s.transfers.CompareAndDelete(id, managed)
				return true
			}
			managed.hashMu.Lock()
			for key, state := range managed.windowHashes {
				if !state.expiresAt.After(now) {
					delete(managed.windowHashes, key)
				}
			}
			managed.hashMu.Unlock()
			return true
		})
	}
}

func validHashToken(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parts := strings.SplitN(raw, ":", 2)
	return len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != ""
}

func normalizeHashToken(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (s *Store) SetTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, token string) bool {
	return s.SetTransferFileWindowHashes(txferID, []WindowHash{{FileID: fileID, EndBytes: endBytes, HashToken: token}})
}

// WindowHash is the hash of one file's bytes up to EndBytes, as a SEND
// streamed them, kept so an ACK for that range can be validated.
type WindowHash struct {
	FileID    uint64
	EndBytes  int64
	HashToken string
}

// SetTransferFileWindowHashes records window hashes under one acquisition of
// the transfer's locks, so a SEND records all its files at once. It skips any
// hash that is malformed or names an out-of-range file, and then reports false.
func (s *Store) SetTransferFileWindowHashes(txferID string, hashes []WindowHash) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}
	expiresAt := time.Now().Add(s.ttl)
	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return false
	}
	ok = true
	managed.hashMu.Lock()
	defer managed.hashMu.Unlock()
	for _, h := range hashes {
		if !validHashToken(h.HashToken) || h.FileID >= uint64(len(managed.transfer.State)) || h.EndBytes < 0 {
			ok = false
			continue
		}
		managed.windowHashes[windowHashKey{fileID: h.FileID, endBytes: h.EndBytes}] = windowHashState{
			hashToken: normalizeHashToken(h.HashToken),
			expiresAt: expiresAt,
		}
	}
	return ok
}

func (s *Store) VerifyTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, token string) bool {
	return s.VerifyTransferFileWindowHashes(txferID, []WindowHash{{FileID: fileID, EndBytes: endBytes, HashToken: token}})[0]
}

// VerifyTransferFileWindowHashes checks each hash as
// VerifyTransferFileWindowHash does, under one acquisition of the transfer's
// locks, so an ACK checks all its items at once. ok[i] answers hashes[i].
func (s *Store) VerifyTransferFileWindowHashes(txferID string, hashes []WindowHash) []bool {
	ok := make([]bool, len(hashes))
	managed, found := s.getManagedTransfer(txferID)
	if !found {
		return ok
	}
	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return ok
	}
	managed.hashMu.Lock()
	defer managed.hashMu.Unlock()
	for i, h := range hashes {
		if h.EndBytes < 0 {
			continue
		}
		state, stored := managed.windowHashes[windowHashKey{fileID: h.FileID, endBytes: h.EndBytes}]
		ok[i] = stored && state.hashToken == normalizeHashToken(h.HashToken)
	}
	return ok
}

func (s *Store) NewTransfer(directory string, numFiles int, totalSize int64, opts ...TransferOption) (Transfer, error) {
	for attempts := 0; attempts < 5; attempts++ {
		txferID, err := transferID()
		if err != nil {
			return Transfer{}, err
		}
		now := time.Now()
		transfer := Transfer{
			ID:          txferID,
			Directory:   directory,
			Mode:        "",
			LinkMbps:    0,
			Concurrency: 0,
			NumEntries:  numFiles,
			NumFiles:    numFiles,
			TotalSize:   totalSize,
			Done:        0,
			DoneSize:    0,
			State:       make([]uint8, numFiles),
			EntryType:   make([]byte, numFiles),
			PathHash:    make([]xxh3.Uint128, numFiles),
			FileSize:    make([]int64, numFiles),
			AckedSize:   make([]int64, numFiles),
			CreatedAt:   now,
			ExpiresAt:   now.Add(s.ttl),
		}
		for i := range transfer.State {
			transfer.State[i] = TransferStateStarted
			transfer.EntryType[i] = intencoding.EntryTypeFile
		}
		transfer.StateCounts[TransferStateStarted] = numFiles
		for _, opt := range opts {
			opt(&transfer)
		}
		if transfer.RequestPath == "" {
			transfer.RequestPath = directory
		}
		if s.create(transfer) {
			s.events.Emit("transfer_start", transfer.ID, events.F("path", transfer.RequestPath))
			return transfer, nil
		}
	}
	return Transfer{}, fmt.Errorf("failed to allocate unique transfer id")
}

func (s *Store) RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{} {
	done := make(chan struct{})
	if updatesCh == nil {
		close(done)
		return done
	}

	go func() {
		defer close(done)
		batch := make([]TransferFileStateUpdate, 0, 1000)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			s.RegisterTransferFileStates(txferID, batch, state)
			batch = batch[:0]
		}

		for update := range updatesCh {
			batch = append(batch, update)
			if len(batch) == 1000 {
				flush()
			}
		}
		flush()
	}()
	return done
}

func (s *Store) GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error) {
	ref, err := s.GetFileRef(txferID, fileID, fullPathRaw)
	if err != nil {
		return nil, FileRef{}, err
	}
	fd, err := s.OpenFileRef(ref)
	if err != nil {
		return nil, FileRef{}, err
	}
	return fd, ref, nil
}

// OpenFileRef opens a file that GetFileRef or GetFileRefs resolved.
func (s *Store) OpenFileRef(ref FileRef) (*os.File, error) {
	fd, err := os.Open(ref.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &FileLookupError{Code: http.StatusNotFound, Msg: "file not found"}
		}
		return nil, &FileLookupError{Code: http.StatusInternalServerError, Msg: "failed to open file"}
	}
	return fd, nil
}

func (s *Store) AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool {
	return s.AcknowledgeTransferFiles([]AckEntry{{TxferID: txferID, FileID: fileID, AckBytes: ackBytes}})
}

const progressLogPctInterval = 10
const progressLogTimeInterval = 10 * time.Second
const progressLogCountWidth = 6
const progressLogBytesWidth = 10
const progressLogRateWidth = 13

func (s *Store) MaybeLogTransferProgress(txferID string) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return
	}

	managed.mu.Lock()
	line, ok := managed.progressLineLocked(time.Now())
	managed.mu.Unlock()
	if ok {
		log.Print(line)
	}
}

// progressLineLocked returns the progress log line when a percent bucket or
// the time interval has passed since the last one, and records that it was
// logged. The caller holds m.mu and writes the line after releasing it.
func (m *managedTransfer) progressLineLocked(now time.Time) (string, bool) {
	if m.deleted {
		return "", false
	}
	t := &m.transfer
	if t.TotalSize <= 0 {
		return "", false
	}

	currentPct := int(t.DoneSize * 100 / t.TotalSize)
	pctBucket := (currentPct / progressLogPctInterval) * progressLogPctInterval

	pctCrossed := pctBucket > t.LastLogPct
	timeCrossed := !t.LastLogTime.IsZero() && now.Sub(t.LastLogTime) >= progressLogTimeInterval && t.DoneSize > t.LastLogDoneSize
	if !pctCrossed && !timeCrossed {
		return "", false
	}

	t.LastLogPct = pctBucket
	t.LastLogTime = now
	t.LastLogDoneSize = t.DoneSize

	elapsed := now.Sub(t.CreatedAt)
	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(t.DoneSize) / elapsed.Seconds()
	}
	var filesPct, bytesPct float64
	if t.NumFiles > 0 {
		filesPct = float64(t.Done) * 100.0 / float64(t.NumFiles)
	}
	bytesPct = float64(t.DoneSize) * 100.0 / float64(t.TotalSize)

	return fmt.Sprintf(
		"txfer-progress:[%s] [%s/%s](%5.1f%%) [%s/%s](%5.1f%%) elapsed=%4s rate=%s",
		t.ID,
		intencoding.HumanCount(t.Done, progressLogCountWidth),
		intencoding.HumanCount(uint64(t.NumFiles), progressLogCountWidth),
		filesPct,
		intencoding.HumanBytesFixedWidth(t.DoneSize, progressLogBytesWidth),
		intencoding.HumanBytesFixedWidth(t.TotalSize, progressLogBytesWidth),
		bytesPct,
		elapsed.Truncate(time.Second),
		intencoding.HumanRateFixedWidth(rate, progressLogRateWidth),
	), true
}

func transferID() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate transfer id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
