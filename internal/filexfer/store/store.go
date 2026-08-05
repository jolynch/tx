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

	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
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
	ID              string
	Directory       string
	Mode            string
	LinkMbps        int64
	Concurrency     int
	NumEntries      int
	NumFiles        int
	TotalSize       int64
	Done            uint64
	DoneSize        int64
	State           []uint8
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
	mu           sync.RWMutex
	deleted      bool
	transfer     Transfer
	windowHashes map[windowHashKey]*windowHashState
	observedEMA  float64
	limiterBps   int64
	limiter      atomic.Pointer[limit.Limiter]
}

// Store holds transfer state for a server. There is no process-wide
// instance: whoever runs a server owns a Store and closes it, and tests
// build their own so they cannot see each other's transfers.
type Store struct {
	mu        sync.RWMutex
	transfers map[string]*managedTransfer
	ttl       time.Duration
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
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

// WithTTL overrides how long transfers and their hash state survive past
// their last update. Chiefly useful to tests, which otherwise cannot reach
// the expiry paths inside the default ten minute window.
func WithTTL(d time.Duration) StoreOption {
	return func(s *Store) {
		if d > 0 {
			s.ttl = d
		}
	}
}

// NewStore returns an isolated store with its own expiry goroutine. Callers
// are responsible for calling Close to stop that goroutine.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{
		transfers: make(map[string]*managedTransfer),
		ttl:       defaultTransferTTL,
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
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
		windowHashes: make(map[windowHashKey]*windowHashState),
	}
}

func shouldAdvanceState(current uint8, next uint8) bool {
	return next >= current
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

func isRegularFileEntryType(entryType byte) bool {
	return entryType == 0 || entryType == intencoding.EntryTypeFile
}

func logTransferComplete(t *Transfer) {
	elapsed := time.Since(t.CreatedAt)
	speed := 0.0
	if elapsed.Seconds() > 0 {
		speed = float64(t.TotalSize) / elapsed.Seconds()
	}
	log.Printf(
		"txfer-complete: tid=%s files=%d size=%s elapsed=%s speed=%s",
		t.ID,
		t.NumFiles,
		intencoding.HumanBytes(t.TotalSize),
		elapsed.Round(time.Millisecond),
		intencoding.HumanRate(speed),
	)
}

func (s *Store) MaybeLogTransferComplete(txferID string) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return
	}
	t := &managed.transfer
	if t.CompleteLogged || t.NumFiles <= 0 || t.Done != uint64(t.NumFiles) {
		return
	}
	t.CompleteLogged = true
	logTransferComplete(t)
}

func (s *Store) getManagedTransfer(txferID string) (*managedTransfer, bool) {
	s.mu.RLock()
	managed, ok := s.transfers[txferID]
	s.mu.RUnlock()
	return managed, ok
}

func (s *Store) create(transfer Transfer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.transfers[transfer.ID]; exists {
		return false
	}
	s.transfers[transfer.ID] = newManagedTransfer(transfer)
	return true
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
		}
	}

	for _, update := range updates {
		idx := int(update.FileID)
		oldLen := len(managed.transfer.State)
		ensureLen(idx + 1)

		wasRegularFile := idx < oldLen && isRegularFileEntryType(managed.transfer.EntryType[idx])
		if update.FileID != intencoding.RootFileID && idx >= oldLen {
			managed.transfer.NumEntries++
		}
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
	}
}

func (s *Store) DeleteTransfer(txferID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	managed, ok := s.transfers[txferID]
	if !ok {
		return false
	}
	managed.mu.Lock()
	managed.deleted = true
	managed.mu.Unlock()
	delete(s.transfers, txferID)
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

func (s *Store) GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	fullPath := filepath.Clean(fullPathRaw)
	if !filepath.IsAbs(fullPath) {
		return FileRef{}, &FileLookupError{Code: http.StatusBadRequest, Msg: "path must be absolute"}
	}

	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"}
	}

	managed.mu.RLock()
	if managed.deleted {
		managed.mu.RUnlock()
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"}
	}
	directory := managed.transfer.Directory
	if fileID == intencoding.RootFileID {
		managed.mu.RUnlock()
		if fullPath != filepath.Clean(directory) {
			return FileRef{}, &FileLookupError{Code: http.StatusForbidden, Msg: "root metadata path must equal transfer root"}
		}
		return FileRef{
			TransferID: txferID,
			FileID:     fileID,
			Path:       fullPath,
			Directory:  directory,
			FileSize:   0,
			EntryType:  intencoding.EntryTypeDir,
		}, nil
	}
	if fileID >= uint64(len(managed.transfer.State)) {
		managed.mu.RUnlock()
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "file id out of range"}
	}
	expectedDigest := managed.transfer.PathHash[fileID]
	fileSize := managed.transfer.FileSize[fileID]
	entryType := byte(intencoding.EntryTypeFile)
	if fileID < uint64(len(managed.transfer.EntryType)) && managed.transfer.EntryType[fileID] != 0 {
		entryType = managed.transfer.EntryType[fileID]
	}
	managed.mu.RUnlock()

	if !pathWithinRoot(directory, fullPath) {
		return FileRef{}, &FileLookupError{Code: http.StatusForbidden, Msg: "path must be within transfer root"}
	}

	computedDigest := xxh3.Hash128([]byte(fullPath))
	if expectedDigest != computedDigest {
		return FileRef{}, &FileLookupError{Code: http.StatusForbidden, Msg: "file path digest mismatch"}
	}

	return FileRef{
		TransferID: txferID,
		FileID:     fileID,
		Path:       fullPath,
		Directory:  directory,
		FileSize:   fileSize,
		EntryType:  entryType,
	}, nil
}

func (s *Store) ListTransfers() []Transfer {
	s.mu.RLock()
	transfers := make([]*managedTransfer, 0, len(s.transfers))
	for _, managed := range s.transfers {
		transfers = append(transfers, managed)
	}
	s.mu.RUnlock()

	out := make([]Transfer, 0, len(transfers))
	for _, managed := range transfers {
		managed.mu.RLock()
		if !managed.deleted {
			out = append(out, cloneTransfer(managed.transfer))
		}
		managed.mu.RUnlock()
	}
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
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) {
		return false
	}
	idx := int(fileID)
	if !shouldAdvanceState(managed.transfer.State[idx], state) || managed.transfer.State[idx] == state {
		return true
	}
	managed.transfer.State[idx] = state
	return true
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
			managed.transfer.State[idx] = TransferStateMissing
			if !wasTerminal {
				managed.transfer.Done++
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

	prev := managed.transfer.AckedSize[idx]
	if target <= prev {
		return true
	}

	delta := target - prev
	managed.transfer.AckedSize[idx] = target
	managed.transfer.DoneSize += delta

	if prev < maxAck && target == maxAck {
		managed.transfer.Done++
		if shouldAdvanceState(managed.transfer.State[idx], TransferStateDone) {
			managed.transfer.State[idx] = TransferStateDone
		}
	}

	delete(managed.windowHashes, windowHashKey{fileID: fileID, endBytes: target})
	return true
}

func (s *Store) ClipTransfer(txferID string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.State = slices.Clip(managed.transfer.State)
	managed.transfer.PathHash = slices.Clip(managed.transfer.PathHash)
	managed.transfer.FileSize = slices.Clip(managed.transfer.FileSize)
	managed.transfer.AckedSize = slices.Clip(managed.transfer.AckedSize)
	if managed.transfer.PageCache != nil {
		managed.transfer.PageCache = slices.Clip(managed.transfer.PageCache)
	}

	t := &managed.transfer
	log.Printf(
		"txfer-start: tid=%s dir=%s mode=%s entries=%d files=%d size=%s link=%dMbps concurrency=%d",
		t.ID, t.Directory, t.Mode,
		t.NumEntries,
		t.NumFiles,
		intencoding.HumanBytes(t.TotalSize),
		t.LinkMbps, t.Concurrency,
	)
	if t.NumFiles == 0 {
		t.CompleteLogged = true
		logTransferComplete(t)
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

		s.mu.Lock()
		survivors := make([]*managedTransfer, 0, len(s.transfers))
		for txferID, managed := range s.transfers {
			managed.mu.RLock()
			expired := !managed.deleted && !managed.transfer.ExpiresAt.After(now)
			managed.mu.RUnlock()
			if expired {
				managed.mu.Lock()
				managed.deleted = true
				managed.mu.Unlock()
				delete(s.transfers, txferID)
				continue
			}
			survivors = append(survivors, managed)
		}
		s.mu.Unlock()

		for _, managed := range survivors {
			managed.mu.Lock()
			if managed.deleted {
				managed.mu.Unlock()
				continue
			}
			for key, state := range managed.windowHashes {
				if !state.expiresAt.After(now) {
					delete(managed.windowHashes, key)
				}
			}
			managed.mu.Unlock()
		}
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
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) || endBytes < 0 {
		return false
	}
	if !validHashToken(token) {
		return false
	}
	managed.windowHashes[windowHashKey{fileID: fileID, endBytes: endBytes}] = &windowHashState{
		hashToken: normalizeHashToken(token),
		expiresAt: time.Now().Add(s.ttl),
	}
	return true
}

func (s *Store) VerifyTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, token string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted || endBytes < 0 {
		return false
	}
	state, ok := managed.windowHashes[windowHashKey{fileID: fileID, endBytes: endBytes}]
	if !ok {
		return false
	}
	return state.hashToken == normalizeHashToken(token)
}

func (s *Store) NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error) {
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
		if s.create(transfer) {
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
	fd, err := os.Open(ref.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "file not found"}
		}
		return nil, FileRef{}, &FileLookupError{Code: http.StatusInternalServerError, Msg: "failed to open file"}
	}
	return fd, ref, nil
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
	defer managed.mu.Unlock()
	if managed.deleted {
		return
	}
	t := &managed.transfer
	if t.TotalSize <= 0 {
		return
	}

	currentPct := int(t.DoneSize * 100 / t.TotalSize)
	pctBucket := (currentPct / progressLogPctInterval) * progressLogPctInterval
	now := time.Now()

	pctCrossed := pctBucket > t.LastLogPct
	timeCrossed := !t.LastLogTime.IsZero() && now.Sub(t.LastLogTime) >= progressLogTimeInterval && t.DoneSize > t.LastLogDoneSize
	if !pctCrossed && !timeCrossed {
		return
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

	log.Printf(
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
	)
}

func transferID() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate transfer id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func pathWithinRoot(root string, p string) bool {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return false
	}
	return !filepath.IsAbs(rel)
}
