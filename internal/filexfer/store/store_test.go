package store

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

// newTestStore gives each test its own store so they cannot see each
// other's transfers. Closed with the test, so no reap goroutine leaks.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	return newTestStoreWithOptions(t)
}

func newTestStoreWithOptions(t *testing.T, opts ...StoreOption) *Store {
	t.Helper()
	s := NewStore(opts...)
	t.Cleanup(s.Close)
	return s
}

func waitForTransferState(t *testing.T, s *Store, txferID string, check func(Transfer) bool) Transfer {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		stored, ok := s.GetTransfer(txferID)
		if ok && check(stored) {
			return stored
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, ok := s.GetTransfer(txferID)
	if !ok {
		t.Fatalf("transfer %q not found in store", txferID)
	}
	t.Fatalf("timed out waiting for expected transfer state")
	return Transfer{}
}

func TestNewTransferInitializesStateByFileID(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 3, 42)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}

	stored, ok := s.GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found in store", transfer.ID)
	}
	if len(stored.State) != 3 {
		t.Fatalf("expected state len 3, got %d", len(stored.State))
	}
	if len(stored.PathHash) != 3 {
		t.Fatalf("expected hash len 3, got %d", len(stored.PathHash))
	}
	if len(stored.EntryType) != 3 {
		t.Fatalf("expected entry-type len 3, got %d", len(stored.EntryType))
	}
	if len(stored.FileSize) != 3 {
		t.Fatalf("expected file-size len 3, got %d", len(stored.FileSize))
	}
	if len(stored.AckedSize) != 3 {
		t.Fatalf("expected acked-size len 3, got %d", len(stored.AckedSize))
	}
	if stored.NumEntries != 3 || stored.NumFiles != 3 {
		t.Fatalf("expected numEntries=numFiles=3, got entries=%d files=%d", stored.NumEntries, stored.NumFiles)
	}
	if stored.Done != 0 {
		t.Fatalf("expected done 0, got %d", stored.Done)
	}
	if stored.DoneSize != 0 {
		t.Fatalf("expected done size 0, got %d", stored.DoneSize)
	}
	for i, state := range stored.State {
		if state != TransferStateStarted {
			t.Fatalf("expected file state %d to be started, got %d", i, state)
		}
	}
}

func TestRegisterTransferFileStatePreservesPrestoredPageCacheLength(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	blob := []byte{0x01, 0x02}
	if !s.SetTransferPageCache(transfer.ID, 0, blob) {
		t.Fatalf("SetTransferPageCache returned false")
	}
	s.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 0, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/1")), FileSize: 42},
	}, TransferStateStarted)

	// FileID 0 is RootFileID, which RegisterTransferFileStates explicitly
	// excludes from NumEntries (root entries are implicit). Wait on the
	// post-Register effect we actually care about: State has been grown
	// to match the registered FileID range, and PageCache wasn't wiped or
	// truncated in the process.
	stored := waitForTransferState(t, s, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == 1 && len(stored.PageCache) == 1
	})
	if len(stored.PageCache) != len(stored.State) {
		t.Fatalf("PageCache len = %d, State len = %d", len(stored.PageCache), len(stored.State))
	}
	if !bytes.Equal(stored.PageCache[0], blob) {
		t.Fatalf("PageCache[0] = %x, want %x", stored.PageCache[0], blob)
	}
}

// TestRegisterTransferFileState registers through the channel API and checks
// every per-file slot is filled in, for each state a registration may carry.
func TestRegisterTransferFileState(t *testing.T) {
	for _, state := range []uint8{TransferStateRunning, TransferStateDone} {
		s := newTestStore(t)
		transfer, err := s.NewTransfer("/tmp/x", 1, 42)
		if err != nil {
			t.Fatalf("NewTransfer returned error: %v", err)
		}
		hashes := []xxh3.Uint128{xxh3.Hash128([]byte("/tmp/x/0")), xxh3.Hash128([]byte("/tmp/x/2"))}
		sizes := []int64{100, 300}
		updatesCh := make(chan TransferFileStateUpdate, 2)
		for i := range hashes {
			updatesCh <- TransferFileStateUpdate{FileID: uint64(i), PathHash: hashes[i], FileSize: sizes[i]}
		}
		close(updatesCh)
		s.RegisterTransferFileState(transfer.ID, updatesCh, state)

		stored := waitForTransferState(t, s, transfer.ID, func(stored Transfer) bool {
			return len(stored.State) == 2 && stored.State[0] == state && stored.State[1] == state
		})
		for i := range hashes {
			if stored.State[i] != state || stored.PathHash[i] != hashes[i] || stored.FileSize[i] != sizes[i] {
				t.Fatalf("state %d: slot %d = (state=%d size=%d hash ok=%v)", state, i, stored.State[i], stored.FileSize[i], stored.PathHash[i] == hashes[i])
			}
		}
	}
}

func TestRegisterTransferFileStateDoesNotRegressDoneToStarted(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	doneHash := xxh3.Hash128([]byte("/tmp/x/done"))
	doneUpdatesCh := make(chan TransferFileStateUpdate, 1)
	doneUpdatesCh <- TransferFileStateUpdate{FileID: 0, PathHash: doneHash, FileSize: 100}
	close(doneUpdatesCh)
	s.RegisterTransferFileState(transfer.ID, doneUpdatesCh, TransferStateDone)
	_ = waitForTransferState(t, s, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) >= 1 && stored.State[0] == TransferStateDone
	})
	startedHash := xxh3.Hash128([]byte("/tmp/x/started"))
	startedUpdatesCh := make(chan TransferFileStateUpdate, 1)
	startedUpdatesCh <- TransferFileStateUpdate{FileID: 1, PathHash: startedHash, FileSize: 100}
	close(startedUpdatesCh)
	s.RegisterTransferFileState(transfer.ID, startedUpdatesCh, TransferStateStarted)

	stored := waitForTransferState(t, s, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == 2 && stored.State[0] == TransferStateDone && stored.State[1] == TransferStateStarted
	})
	if stored.State[0] != TransferStateDone {
		t.Fatalf("expected state[0] to remain done, got %d", stored.State[0])
	}
	if stored.PathHash[1] != startedHash {
		t.Fatalf("expected hash[1] to be set from second append update")
	}
}

func TestRegisterTransferFileStateBatchOver1000(t *testing.T) {
	s := newTestStore(t)
	numFiles := 1005
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	updatesCh := make(chan TransferFileStateUpdate, numFiles)
	for i := 0; i < numFiles; i++ {
		updatesCh <- TransferFileStateUpdate{
			FileID:   uint64(i),
			PathHash: xxh3.Hash128([]byte("file-" + strconv.Itoa(i))),
			FileSize: int64(i + 1),
		}
	}
	close(updatesCh)
	s.RegisterTransferFileState(transfer.ID, updatesCh, TransferStateRunning)

	stored := waitForTransferState(t, s, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == numFiles && stored.State[numFiles-1] == TransferStateRunning
	})
	for i := 0; i < numFiles; i++ {
		if stored.State[i] != TransferStateRunning {
			t.Fatalf("expected state[%d] to be running, got %d", i, stored.State[i])
		}
	}
}

func TestAcknowledgeTransferFile(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	updatesCh := make(chan TransferFileStateUpdate, 1)
	updatesCh <- TransferFileStateUpdate{
		FileID:   0,
		PathHash: xxh3.Hash128([]byte("/tmp/x/0")),
		FileSize: 10,
	}
	close(updatesCh)
	s.RegisterTransferFileState(transfer.ID, updatesCh, TransferStateRunning)
	_ = waitForTransferState(t, s, transfer.ID, func(stored Transfer) bool {
		return len(stored.FileSize) >= 1 && stored.FileSize[0] == 10
	})

	if ok := s.AcknowledgeTransferFile(transfer.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false")
	}
	stored, ok := s.GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found", transfer.ID)
	}
	if stored.DoneSize != 4 || stored.Done != 0 {
		t.Fatalf("unexpected counters after partial ack: done=%d doneSize=%d", stored.Done, stored.DoneSize)
	}

	if ok := s.AcknowledgeTransferFile(transfer.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for repeated ack")
	}
	stored, _ = s.GetTransfer(transfer.ID)
	if stored.DoneSize != 4 || stored.Done != 0 {
		t.Fatalf("unexpected counters after repeated ack: done=%d doneSize=%d", stored.Done, stored.DoneSize)
	}

	if ok := s.AcknowledgeTransferFile(transfer.ID, 0, 12); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for oversized ack")
	}
	stored, _ = s.GetTransfer(transfer.ID)
	if stored.DoneSize != 10 || stored.Done != 1 {
		t.Fatalf("unexpected counters after completion ack: done=%d doneSize=%d", stored.Done, stored.DoneSize)
	}
	if stored.State[0] != TransferStateDone {
		t.Fatalf("expected state done after full ack, got %d", stored.State[0])
	}
}

func TestMaybeLogTransferCompleteLogsForMixedEntries(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	s.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 1, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/file")), FileSize: 10},
		{FileID: 2, EntryType: encoding.EntryTypeDir, PathHash: xxh3.Hash128([]byte("/tmp/x/sub")), FileSize: 0},
		{FileID: 3, EntryType: encoding.EntryTypeSymlink, PathHash: xxh3.Hash128([]byte("/tmp/x/link")), FileSize: 0},
	}, TransferStateStarted)

	var buf bytes.Buffer
	oldFlags := log.Flags()
	oldWriter := log.Writer()
	log.SetFlags(0)
	log.SetOutput(&buf)
	defer func() {
		log.SetFlags(oldFlags)
		log.SetOutput(oldWriter)
	}()

	if ok := s.ClipTransfer(transfer.ID); !ok {
		t.Fatalf("ClipTransfer returned false")
	}
	if s.MaybeLogTransferComplete(transfer.ID) {
		t.Fatal("MaybeLogTransferComplete reported an incomplete transfer as complete")
	}
	if ok := s.AcknowledgeTransferFile(transfer.ID, 1, 10); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false")
	}
	if !s.MaybeLogTransferComplete(transfer.ID) {
		t.Fatal("MaybeLogTransferComplete did not report completion")
	}
	if !s.MaybeLogTransferComplete(transfer.ID) {
		t.Fatal("MaybeLogTransferComplete stopped reporting completion once logged")
	}

	stored, ok := s.GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found", transfer.ID)
	}
	if stored.Done != 1 || stored.NumFiles != 1 || stored.NumEntries != 3 {
		t.Fatalf("unexpected counts after completion: done=%d files=%d entries=%d", stored.Done, stored.NumFiles, stored.NumEntries)
	}
	if stored.EntryType[1] != encoding.EntryTypeFile || stored.EntryType[2] != encoding.EntryTypeDir || stored.EntryType[3] != encoding.EntryTypeSymlink {
		t.Fatalf("unexpected entry types: %q", string(stored.EntryType))
	}
	logged := buf.String()
	if !strings.Contains(logged, "txfer-start: tid="+transfer.ID) {
		t.Fatalf("expected txfer-start log, got %q", logged)
	}
	if count := strings.Count(logged, "txfer-complete: tid="+transfer.ID); count != 1 {
		t.Fatalf("expected exactly one txfer-complete log, got %d in %q", count, logged)
	}
	if strings.Contains(logged, "files=3") {
		t.Fatalf("expected file count to exclude metadata entries, got %q", logged)
	}
}

// lockFreeLogWriter fails the test when a log line is written while the
// transfer lock is held: a slow log destination must never stall SEND or ACK.
type lockFreeLogWriter struct {
	t *testing.T
	m *managedTransfer
}

func (w lockFreeLogWriter) Write(p []byte) (int, error) {
	if !w.m.mu.TryLock() {
		w.t.Errorf("logged while holding the transfer lock: %q", p)
		return len(p), nil
	}
	w.m.mu.Unlock()
	return len(p), nil
}

// FuzzStoreStateCounts drives a transfer through arbitrary registrations,
// state changes, acknowledgments, clips, and log checks. After every step the
// running StateCounts must equal a recount of regular files by state, the
// summary must match the full transfer minus its per-file slices, and no log
// line may be written under the transfer lock.
func FuzzStoreStateCounts(f *testing.F) {
	f.Add([]byte{2, 0, 3, 1, 4, 0, 1, 5, 2, 2, 1, 9, 3, 0, 4, 0, 5, 0})
	f.Add([]byte{0, 0, 1, 2, 3, 0, 0, 2, 2, 7, 2, 2, 1, 3, 0, 5, 0})
	f.Add([]byte{3, 1, 0, 2, 2, 0, 0, 1, 1, 2, 2, 1, 0, 2, 2, 2, 0, 2, 5, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		// Each op rechecks the whole transfer; past a few hundred ops an
		// input adds runtime, not new states.
		if len(ops) > 512 {
			ops = ops[:512]
		}
		next := func() byte {
			if len(ops) == 0 {
				return 0
			}
			b := ops[0]
			ops = ops[1:]
			return b
		}
		s := newTestStore(t)
		tr, err := s.NewTransfer("/r", int(next()%4), 0)
		if err != nil {
			t.Fatalf("NewTransfer: %v", err)
		}
		managed, _ := s.getManagedTransfer(tr.ID)
		oldFlags, oldWriter := log.Flags(), log.Writer()
		log.SetFlags(0)
		log.SetOutput(lockFreeLogWriter{t: t, m: managed})
		defer func() {
			log.SetFlags(oldFlags)
			log.SetOutput(oldWriter)
		}()

		entryTypes := []byte{0, encoding.EntryTypeFile, encoding.EntryTypeDir, encoding.EntryTypeSymlink}
		for len(ops) > 0 {
			op, fid := next()%6, uint64(next()%8)
			switch op {
			case 0:
				s.RegisterTransferFileStates(tr.ID, []TransferFileStateUpdate{{
					FileID:    fid,
					EntryType: entryTypes[next()%4],
					PathHash:  xxh3.Hash128([]byte(fmt.Sprintf("/r/%d", fid))),
					FileSize:  int64(next() % 8),
				}}, next()%4)
			case 1:
				s.SetTransferFileState(tr.ID, fid, next()%4)
			case 2:
				s.AcknowledgeTransferFile(tr.ID, fid, int64(next()%10)-1) // -1 marks Missing
			case 3:
				s.ClipTransfer(tr.ID)
			case 4:
				s.MaybeLogTransferProgress(tr.ID)
			case 5:
				complete := s.MaybeLogTransferComplete(tr.ID)
				if sum, _ := s.GetTransferSummary(tr.ID); complete != sum.CompleteLogged {
					t.Fatalf("MaybeLogTransferComplete=%v but CompleteLogged=%v", complete, sum.CompleteLogged)
				}
			}

			full, ok := s.GetTransfer(tr.ID)
			if !ok {
				t.Fatal("transfer vanished")
			}
			sum, _ := s.GetTransferSummary(tr.ID)
			var want [TransferStateMissing + 1]int
			for i, st := range full.State {
				if isRegularFileEntryType(full.EntryType[i]) {
					want[st]++
				}
			}
			if full.StateCounts != want {
				t.Fatalf("StateCounts = %v, recount = %v", full.StateCounts, want)
			}
			if sum.State != nil || sum.EntryType != nil || sum.PathHash != nil ||
				sum.FileSize != nil || sum.AckedSize != nil || sum.PageCache != nil {
				t.Fatalf("summary carries per-file slices: %+v", sum)
			}
			full.State, full.EntryType, full.PathHash = nil, nil, nil
			full.FileSize, full.AckedSize, full.PageCache = nil, nil, nil
			if fmt.Sprintf("%+v", sum) != fmt.Sprintf("%+v", full) {
				t.Fatalf("summary differs from transfer:\n sum=%+v\nfull=%+v", sum, full)
			}
		}
	})
}

func TestMaybeLogTransferProgressUsesClientStyleFixedWidthLayout(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	managed, ok := s.getManagedTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found", transfer.ID)
	}
	now := time.Now()
	managed.mu.Lock()
	managed.transfer.TotalSize = 20_174_499_881
	managed.transfer.DoneSize = 18_350_000_000
	managed.transfer.NumFiles = 10127
	managed.transfer.Done = 9121
	managed.transfer.CreatedAt = now.Add(-7 * time.Second)
	managed.transfer.LastLogPct = 80
	managed.mu.Unlock()

	var buf bytes.Buffer
	oldFlags := log.Flags()
	oldWriter := log.Writer()
	log.SetFlags(0)
	log.SetOutput(&buf)
	defer func() {
		log.SetFlags(oldFlags)
		log.SetOutput(oldWriter)
	}()

	s.MaybeLogTransferProgress(transfer.ID)

	logged := strings.TrimSpace(buf.String())
	if !strings.Contains(logged, "txfer-progress:["+transfer.ID+"]") {
		t.Fatalf("expected txfer-progress prefix with bracketed tid, got %q", logged)
	}
	if strings.Contains(logged, "tid=") {
		t.Fatalf("progress line should not include tid= header, got %q", logged)
	}
	if strings.Contains(logged, "files=") {
		t.Fatalf("progress line should not include files= header, got %q", logged)
	}

	wantFiles := "[" + encoding.HumanCount(9121, progressLogCountWidth) + "/" + encoding.HumanCount(10127, progressLogCountWidth) + "]( 90.1%)"
	if !strings.Contains(logged, wantFiles) {
		t.Fatalf("progress line missing fixed-width file section %q in %q", wantFiles, logged)
	}
	bytesPct := float64(18_350_000_000) * 100.0 / float64(20_174_499_881)
	wantBytes := "[" + encoding.HumanBytesFixedWidth(18_350_000_000, progressLogBytesWidth) + "/" + encoding.HumanBytesFixedWidth(20_174_499_881, progressLogBytesWidth) + "](" + fmt.Sprintf("%5.1f%%", bytesPct) + ")"
	if !strings.Contains(logged, wantBytes) {
		t.Fatalf("progress line missing fixed-width byte section %q in %q", wantBytes, logged)
	}
	rateIdx := strings.LastIndex(logged, "rate=")
	if rateIdx < 0 {
		t.Fatalf("progress line missing rate field in %q", logged)
	}
	rateField := logged[rateIdx+len("rate="):]
	if len(rateField) != progressLogRateWidth {
		t.Fatalf("expected fixed-width rate field length %d, got %d in %q", progressLogRateWidth, len(rateField), logged)
	}
}

func TestWindowHashesTrackedPerEndOffset(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	updates := []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}
	s.RegisterTransferFileStates(transfer.ID, updates, TransferStateRunning)

	token4 := "xxh128:00000000000000000000000000000004"
	token8 := "xxh128:00000000000000000000000000000008"
	if ok := s.SetTransferFileWindowHash(transfer.ID, 0, 4, token4); !ok {
		t.Fatalf("SetTransferFileWindowHash returned false for end=4")
	}
	if ok := s.SetTransferFileWindowHash(transfer.ID, 0, 8, token8); !ok {
		t.Fatalf("SetTransferFileWindowHash returned false for end=8")
	}
	if !s.VerifyTransferFileWindowHash(transfer.ID, 0, 4, token4) {
		t.Fatalf("expected window hash verification for end=4")
	}
	if !s.VerifyTransferFileWindowHash(transfer.ID, 0, 8, token8) {
		t.Fatalf("expected window hash verification for end=8")
	}

	if ok := s.AcknowledgeTransferFile(transfer.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for end=4")
	}
	if s.VerifyTransferFileWindowHash(transfer.ID, 0, 4, token4) {
		t.Fatalf("expected end=4 window hash to be cleared after ack")
	}
	if !s.VerifyTransferFileWindowHash(transfer.ID, 0, 8, token8) {
		t.Fatalf("expected end=8 window hash to remain after end=4 ack")
	}

	if ok := s.AcknowledgeTransferFile(transfer.ID, 0, 8); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for end=8")
	}
	if s.VerifyTransferFileWindowHash(transfer.ID, 0, 8, token8) {
		t.Fatalf("expected end=8 window hash to be cleared after ack")
	}
}

func TestDeleteTransferInvalidatesManagedTransfer(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	s.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 0, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}, TransferStateRunning)

	managed, ok := s.getManagedTransfer(transfer.ID)
	if !ok {
		t.Fatalf("expected managed transfer")
	}
	if ok := s.DeleteTransfer(transfer.ID); !ok {
		t.Fatalf("DeleteTransfer returned false")
	}
	if _, ok := s.GetTransfer(transfer.ID); ok {
		t.Fatalf("deleted transfer should not be visible")
	}

	managed.mu.RLock()
	if !managed.deleted {
		managed.mu.RUnlock()
		t.Fatalf("expected managed transfer to be marked deleted")
	}
	managed.mu.RUnlock()

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if ok := s.acknowledgeFileLocked(managed, 0, 4); ok {
		t.Fatalf("expected stale managed transfer mutation to fail after delete")
	}
}

func TestAcknowledgeTransferFilesMixedTransfers(t *testing.T) {
	s := newTestStore(t)
	first, err := s.NewTransfer("/tmp/a", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer first failed: %v", err)
	}
	second, err := s.NewTransfer("/tmp/b", 1, 12)
	if err != nil {
		t.Fatalf("NewTransfer second failed: %v", err)
	}
	s.RegisterTransferFileStates(first.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/a/0")), FileSize: 10},
	}, TransferStateRunning)
	s.RegisterTransferFileStates(second.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/b/0")), FileSize: 12},
	}, TransferStateRunning)

	firstToken := "xxh128:0000000000000000000000000000000a"
	secondToken := "xxh128:0000000000000000000000000000000b"
	if ok := s.SetTransferFileWindowHash(first.ID, 0, 6, firstToken); !ok {
		t.Fatalf("SetTransferFileWindowHash first returned false")
	}
	if ok := s.SetTransferFileWindowHash(second.ID, 0, 8, secondToken); !ok {
		t.Fatalf("SetTransferFileWindowHash second returned false")
	}

	if ok := s.AcknowledgeTransferFiles([]AckEntry{
		{TxferID: first.ID, FileID: 0, AckBytes: 6},
		{TxferID: second.ID, FileID: 0, AckBytes: 8},
	}); !ok {
		t.Fatalf("AcknowledgeTransferFiles returned false")
	}

	firstStored, ok := s.GetTransfer(first.ID)
	if !ok {
		t.Fatalf("first transfer not found")
	}
	if firstStored.DoneSize != 6 || firstStored.Done != 0 {
		t.Fatalf("unexpected first counters: done=%d doneSize=%d", firstStored.Done, firstStored.DoneSize)
	}
	secondStored, ok := s.GetTransfer(second.ID)
	if !ok {
		t.Fatalf("second transfer not found")
	}
	if secondStored.DoneSize != 8 || secondStored.Done != 0 {
		t.Fatalf("unexpected second counters: done=%d doneSize=%d", secondStored.Done, secondStored.DoneSize)
	}
	if s.VerifyTransferFileWindowHash(first.ID, 0, 6, firstToken) {
		t.Fatalf("expected first window hash cleared after ack")
	}
	if s.VerifyTransferFileWindowHash(second.ID, 0, 8, secondToken) {
		t.Fatalf("expected second window hash cleared after ack")
	}
}

func TestWindowHashesAreTransferLocal(t *testing.T) {
	s := newTestStore(t)
	first, err := s.NewTransfer("/tmp/a", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer first failed: %v", err)
	}
	second, err := s.NewTransfer("/tmp/b", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer second failed: %v", err)
	}
	s.RegisterTransferFileStates(first.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/a/0")), FileSize: 10},
	}, TransferStateRunning)
	s.RegisterTransferFileStates(second.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/b/0")), FileSize: 10},
	}, TransferStateRunning)

	firstToken := "xxh128:0000000000000000000000000000000c"
	secondToken := "xxh128:0000000000000000000000000000000d"
	if ok := s.SetTransferFileWindowHash(first.ID, 0, 4, firstToken); !ok {
		t.Fatalf("SetTransferFileWindowHash first returned false")
	}
	if ok := s.SetTransferFileWindowHash(second.ID, 0, 4, secondToken); !ok {
		t.Fatalf("SetTransferFileWindowHash second returned false")
	}
	if !s.VerifyTransferFileWindowHash(first.ID, 0, 4, firstToken) {
		t.Fatalf("expected first transfer window hash")
	}
	if !s.VerifyTransferFileWindowHash(second.ID, 0, 4, secondToken) {
		t.Fatalf("expected second transfer window hash")
	}

	if ok := s.AcknowledgeTransferFile(first.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile first returned false")
	}
	if s.VerifyTransferFileWindowHash(first.ID, 0, 4, firstToken) {
		t.Fatalf("expected first transfer window hash cleared")
	}
	if !s.VerifyTransferFileWindowHash(second.ID, 0, 4, secondToken) {
		t.Fatalf("expected second transfer window hash to remain")
	}
}

func TestGetTransferSnapshotsAreCopies(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	s.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 0, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}, TransferStateRunning)

	stored, ok := s.GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("GetTransfer returned not found")
	}
	stored.State[0] = TransferStateMissing
	stored.PathHash[0] = xxh3.Hash128([]byte("mutated"))
	stored.EntryType[0] = encoding.EntryTypeDir
	stored.FileSize[0] = 999
	stored.AckedSize[0] = 999

	storedAgain, ok := s.GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("GetTransfer returned not found on second read")
	}
	if storedAgain.State[0] != TransferStateRunning {
		t.Fatalf("expected original transfer state to remain running, got %d", storedAgain.State[0])
	}
	if storedAgain.EntryType[0] != encoding.EntryTypeFile {
		t.Fatalf("expected original transfer entry type to remain file, got %q", storedAgain.EntryType[0])
	}
	if storedAgain.FileSize[0] != 10 || storedAgain.AckedSize[0] != 0 {
		t.Fatalf("expected original transfer sizes to remain unchanged")
	}
}

func setupLookupFixture(t *testing.T, s *Store, fileName string, content []byte) (string, string) {
	t.Helper()
	root := t.TempDir()
	fullPath := filepath.Join(root, fileName)
	if content != nil {
		if err := os.WriteFile(fullPath, content, 0o644); err != nil {
			t.Fatalf("write fixture file: %v", err)
		}
	}
	transfer, err := s.NewTransfer(root, 0, int64(len(content)))
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	s.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{
			FileID:    1,
			EntryType: encoding.EntryTypeFile,
			PathHash:  xxh3.Hash128([]byte(filepath.Clean(fullPath))),
			FileSize:  int64(len(content)),
		},
	}, TransferStateStarted)
	return transfer.ID, fullPath
}

func mustLookupErr(t *testing.T, err error) *FileLookupError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected lookup error, got nil")
	}
	lookupErr, ok := err.(*FileLookupError)
	if !ok {
		t.Fatalf("expected *FileLookupError, got %T (%v)", err, err)
	}
	return lookupErr
}

func TestGetFileRefTransferNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetFileRef("missing", 0, "/tmp/x")
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusNotFound || lookupErr.Msg != "transfer not found" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefFileIDOutOfRange(t *testing.T) {
	s := newTestStore(t)
	txferID, fullPath := setupLookupFixture(t, s, "a.txt", []byte("hello"))
	_, err := s.GetFileRef(txferID, 2, fullPath)
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusNotFound || lookupErr.Msg != "file id out of range" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefRootMetadataIDAllowsTransferRoot(t *testing.T) {
	s := newTestStore(t)
	txferID, fullPath := setupLookupFixture(t, s, "a.txt", []byte("hello"))
	root := filepath.Dir(fullPath)
	ref, err := s.GetFileRef(txferID, encoding.RootFileID, root)
	if err != nil {
		t.Fatalf("GetFileRef root metadata failed: %v", err)
	}
	if ref.Path != filepath.Clean(root) {
		t.Fatalf("root ref path = %q, want %q", ref.Path, filepath.Clean(root))
	}
	if ref.EntryType != encoding.EntryTypeDir {
		t.Fatalf("root ref entry type = %q, want dir", ref.EntryType)
	}
}

func TestGetFileRefRejectsNonAbsolutePath(t *testing.T) {
	s := newTestStore(t)
	txferID, _ := setupLookupFixture(t, s, "a.txt", []byte("hello"))
	_, err := s.GetFileRef(txferID, 1, "a.txt")
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusBadRequest || lookupErr.Msg != "path must be absolute" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefRejectsOutsideRoot(t *testing.T) {
	s := newTestStore(t)
	txferID, _ := setupLookupFixture(t, s, "a.txt", []byte("hello"))
	_, err := s.GetFileRef(txferID, 1, "/tmp/not-in-root.txt")
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusForbidden || lookupErr.Msg != "path must be within transfer root" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefRejectsDigestMismatch(t *testing.T) {
	s := newTestStore(t)
	txferID, fullPath := setupLookupFixture(t, s, "a.txt", []byte("hello"))
	altPath := filepath.Join(filepath.Dir(fullPath), "b.txt")
	_, err := s.GetFileRef(txferID, 1, altPath)
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusForbidden || lookupErr.Msg != "file path digest mismatch" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileReturnsNotFoundWhenMissing(t *testing.T) {
	s := newTestStore(t)
	txferID, fullPath := setupLookupFixture(t, s, "missing.txt", nil)
	fd, _, err := s.GetFile(txferID, 1, fullPath)
	if fd != nil {
		_ = fd.Close()
		t.Fatalf("expected nil fd for missing file")
	}
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusNotFound || lookupErr.Msg != "file not found" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileSuccess(t *testing.T) {
	s := newTestStore(t)
	txferID, fullPath := setupLookupFixture(t, s, "a.txt", []byte("hello"))
	fd, ref, err := s.GetFile(txferID, 1, fullPath)
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	defer fd.Close()
	if ref.TransferID != txferID || ref.FileID != 1 {
		t.Fatalf("unexpected ref IDs: %+v", ref)
	}
	if ref.Path != filepath.Clean(fullPath) {
		t.Fatalf("unexpected ref path: %q", ref.Path)
	}
	if ref.FileSize != 5 {
		t.Fatalf("unexpected ref size: %d", ref.FileSize)
	}
}

func TestGetTransferGentleLimiterInitializesFromHints(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	if ok := s.SetTransferHints(transfer.ID, "gentle", 800, 6); !ok {
		t.Fatalf("SetTransferHints failed")
	}

	limiter := s.GetTransferGentleLimiter(transfer.ID, 0, 25, 2*1024*1024)
	if limiter == nil {
		t.Fatalf("expected limiter")
	}
	cfg := limiter.Config()
	if cfg.RateBps != 25_000_000 {
		t.Fatalf("unexpected rate: got=%d want=%d", cfg.RateBps, 25_000_000)
	}
	if cfg.BurstBytes != 2*1024*1024 {
		t.Fatalf("unexpected burst: got=%d", cfg.BurstBytes)
	}
}

func TestReportTransferObservedLinkUpdatesEMAAndLimiter(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	if ok := s.SetTransferHints(transfer.ID, "gentle", 1000, 6); !ok {
		t.Fatalf("SetTransferHints failed")
	}
	initial := s.GetTransferGentleLimiter(transfer.ID, 0, 25, 1*1024*1024)
	if initial == nil {
		t.Fatalf("expected initial limiter")
	}

	update, ok := s.ReportTransferObservedLink(transfer.ID, 500, 25, 1*1024*1024, 0.2)
	if !ok {
		t.Fatalf("expected report update")
	}
	if update.ObservedLinkMbps != 500 {
		t.Fatalf("unexpected observed link: %d", update.ObservedLinkMbps)
	}
	if update.OldRateBps != 31_250_000 {
		t.Fatalf("unexpected old rate: %d", update.OldRateBps)
	}
	if update.RoundedLinkMbps != 900 {
		t.Fatalf("unexpected rounded ema link: %d", update.RoundedLinkMbps)
	}
	if update.NewRateBps != 28_125_000 {
		t.Fatalf("unexpected new rate: %d", update.NewRateBps)
	}
	stored, ok := s.GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("expected stored transfer")
	}
	if stored.LinkMbps != 900 {
		t.Fatalf("unexpected stored link mbps: %d", stored.LinkMbps)
	}
	updatedLimiter := s.GetTransferGentleLimiter(transfer.ID, 0, 25, 1*1024*1024)
	if updatedLimiter == nil {
		t.Fatalf("expected updated limiter")
	}
	if updatedLimiter == initial {
		t.Fatalf("expected limiter swap")
	}
	cfg := updatedLimiter.Config()
	if cfg.RateBps != 28_125_000 {
		t.Fatalf("unexpected updated limiter rate: %d", cfg.RateBps)
	}
}

func TestTransferDeadlineState(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	if !s.SetTransferDeadline(transfer.ID, 250) {
		t.Fatalf("SetTransferDeadline returned false")
	}
	firstSend, ok := s.RecordTransferFirstSend(transfer.ID)
	if !ok || firstSend.IsZero() {
		t.Fatalf("RecordTransferFirstSend = %v, %v", firstSend, ok)
	}
	if again, ok := s.RecordTransferFirstSend(transfer.ID); !ok || again != firstSend {
		t.Fatalf("second RecordTransferFirstSend = %v, %v; want %v, true", again, ok, firstSend)
	}
	if !s.MarkTransferTooSlow(transfer.ID) {
		t.Fatalf("MarkTransferTooSlow returned false")
	}

	stored, ok := s.GetTransfer(transfer.ID)
	if !ok || stored.DeadlineMS != 250 || stored.FirstSendAt != firstSend || !stored.TooSlow {
		t.Fatalf("unexpected deadline state: %+v, found=%v", stored, ok)
	}
}

func TestStoreIsolation(t *testing.T) {
	a := newTestStore(t)
	b := newTestStore(t)

	transfer, err := a.NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	if _, ok := a.GetTransfer(transfer.ID); !ok {
		t.Fatalf("transfer %q missing from its store", transfer.ID)
	}
	if _, ok := b.GetTransfer(transfer.ID); ok {
		t.Fatalf("transfer %q leaked into another store", transfer.ID)
	}
}

func TestWithTTLRejectsNonPositive(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		s := newTestStoreWithOptions(t, WithTTL(ttl))
		if s.ttl != defaultTransferTTL {
			t.Fatalf("WithTTL(%v) set ttl=%v", ttl, s.ttl)
		}
	}
}

func TestStoreCloseWaitsForActiveReap(t *testing.T) {
	s := NewStore(WithTTL(20 * time.Millisecond))
	transfer, err := s.NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		s.Close()
		t.Fatalf("NewTransfer: %v", err)
	}
	managed, ok := s.getManagedTransfer(transfer.ID)
	if !ok {
		s.Close()
		t.Fatalf("transfer missing")
	}

	// Hold the per-transfer lock until a reaper pass has started; it cannot
	// finish without that lock. Close must then wait for the pass.
	managed.mu.Lock()
	deadline := time.Now().Add(time.Second)
	for s.reapPasses.Load() == 0 {
		if time.Now().After(deadline) {
			managed.mu.Unlock()
			s.Close()
			t.Fatalf("reaper did not start")
		}
		time.Sleep(time.Millisecond)
	}

	started := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		close(started)
		s.Close()
		close(closed)
	}()
	<-started
	select {
	case <-closed:
		managed.mu.Unlock()
		t.Fatalf("Close returned while the reaper was active")
	case <-time.After(20 * time.Millisecond):
	}

	managed.mu.Unlock()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatalf("Close did not return after the reaper finished")
	}
	s.Close() // idempotent
}

// An ACK can already hold a managed-transfer pointer when a reaper pass
// starts. Either reaping wins and the ACK fails, or the ACK renews
// the deadline and the transfer survives; a successful renewal cannot be lost.
func TestStoreReapConcurrentACK(t *testing.T) {
	s := newTestStoreWithOptions(t, WithTTL(time.Second))
	tr, err := s.NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	managed, _ := s.getManagedTransfer(tr.ID)
	managed.mu.Lock()
	managed.transfer.FileSize[0] = 10
	managed.transfer.ExpiresAt = time.Now().Add(-time.Second)

	// Block the reaper on this transfer once its pass has started.
	deadline := time.Now().Add(5 * time.Second)
	for s.reapPasses.Load() == 0 {
		if time.Now().After(deadline) {
			managed.mu.Unlock()
			t.Fatal("reaper did not start")
		}
		time.Sleep(time.Millisecond)
	}

	ackStarted := make(chan struct{})
	ackDone := make(chan bool, 1)
	go func() {
		close(ackStarted)
		// This is the apply step of an ACK that looked up the transfer
		// before the reaper's pass began.
		managed.mu.Lock()
		ackDone <- s.acknowledgeFileLocked(managed, 0, 1)
		managed.mu.Unlock()
	}()
	<-ackStarted
	// Let the ACK queue behind the reaper's initial lock acquisition.
	time.Sleep(10 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	<-s.done
	managed.mu.Unlock()
	<-closed
	acked := <-ackDone

	got, exists := s.GetTransfer(tr.ID)
	if acked != exists {
		t.Fatalf("ACK succeeded=%v, transfer survived=%v: reaper lost a renewed deadline", acked, exists)
	}
	if acked && (got.AckedSize[0] != 1 || !got.ExpiresAt.After(tr.ExpiresAt)) {
		t.Fatalf("successful ACK did not preserve progress and renew TTL: %+v", got)
	}
}

// ttlTestFiles is enough files for one progress step per file throughout a
// ttlTestRun, so every step is a fresh, irreversible advance.
const ttlTestFiles = 64

// newTTLTestTransfer registers ttlTestFiles regular files of the given size
// in a store whose transfers expire after ttl without progress.
func newTTLTestTransfer(t *testing.T, ttl time.Duration, size int64) (*Store, string) {
	t.Helper()
	s := newTestStoreWithOptions(t, WithTTL(ttl))
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	updates := make([]TransferFileStateUpdate, ttlTestFiles)
	for i := range updates {
		path := fmt.Sprintf("/tmp/x/f%d", i+1)
		updates[i] = TransferFileStateUpdate{FileID: uint64(i + 1), EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte(path)), FileSize: size}
	}
	s.RegisterTransferFileStates(transfer.ID, updates, TransferStateStarted)
	return s, transfer.ID
}

// ttlTestRun calls step every 5ms for d, or until the transfer is reaped,
// and reports whether it survived the whole time.
func ttlTestRun(s *Store, txferID string, d time.Duration, step func(i int)) bool {
	start := time.Now()
	for i := 1; time.Since(start) < d; i++ {
		if _, ok := s.GetTransfer(txferID); !ok {
			return false
		}
		step(i)
		time.Sleep(5 * time.Millisecond)
	}
	_, ok := s.GetTransfer(txferID)
	return ok
}

// waitReaped fails the test unless the reaper removes the transfer.
func waitReaped(t *testing.T, s *Store, txferID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := s.GetTransfer(txferID); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("transfer %q was never reaped", txferID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStoreProgressExtendsTTL pins that the TTL counts from the last forward
// progress, for each kind of progress, and that the reaper still removes the
// transfer once progress stops. Expiry is enforced solely by the reap
// goroutine, so these runs drive it for real.
func TestStoreProgressExtendsTTL(t *testing.T) {
	const ttl = 60 * time.Millisecond
	tests := []struct {
		name string
		size int64
		step func(s *Store, txferID string, i int)
	}{
		{"acked bytes", 1 << 20, func(s *Store, id string, i int) { s.AcknowledgeTransferFile(id, 1, int64(i)) }},
		{"file state advances", 1, func(s *Store, id string, i int) {
			s.SetTransferFileState(id, uint64(i%ttlTestFiles+1), TransferStateRunning)
		}},
		{"empty file acks", 0, func(s *Store, id string, i int) { s.AcknowledgeTransferFile(id, uint64(i%ttlTestFiles+1), 0) }},
		{"new registrations", 1, func(s *Store, id string, i int) {
			fileID := uint64(ttlTestFiles + i)
			s.RegisterTransferFileStates(id, []TransferFileStateUpdate{
				{FileID: fileID, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte(fmt.Sprint(fileID))), FileSize: 1},
			}, TransferStateStarted)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, id := newTTLTestTransfer(t, ttl, tc.size)
			// ttlTestFiles steps of 5ms outlast the TTL several times over
			// without any file seeing a second advance.
			if !ttlTestRun(s, id, 4*ttl, func(i int) { tc.step(s, id, i) }) {
				t.Fatalf("transfer reaped despite continuous %s", tc.name)
			}
			waitReaped(t, s, id)
		})
	}

	// ClipTransfer ends the manifest walk once per transfer, so it cannot be
	// repeated in a loop; check the deadline it sets instead.
	t.Run("clip transfer", func(t *testing.T) {
		t.Parallel()
		s, id := newTTLTestTransfer(t, time.Hour, 1)
		before, _ := s.GetTransfer(id)
		time.Sleep(5 * time.Millisecond)
		s.ClipTransfer(id)
		after, _ := s.GetTransfer(id)
		if !after.ExpiresAt.After(before.ExpiresAt) {
			t.Fatalf("ClipTransfer did not refresh the TTL: before=%v after=%v", before.ExpiresAt, after.ExpiresAt)
		}
	})
}

// TestStoreNonProgressDoesNotExtendTTL guards the other half of the rule: an
// idle transfer is reaped, and repeating requests that do no work does not
// keep one alive.
func TestStoreNonProgressDoesNotExtendTTL(t *testing.T) {
	const ttl = 50 * time.Millisecond
	token := "xxh128:00000000000000000000000000000001"
	tests := []struct {
		name string
		step func(s *Store, txferID string)
	}{
		{"idle", func(*Store, string) {}},
		{"repeated requests", func(s *Store, id string) {
			s.AcknowledgeTransferFile(id, 1, 10)
			s.AcknowledgeTransferFile(id, 1, 5)
			s.SetTransferFileState(id, 1, TransferStateRunning)
			s.SetTransferFileWindowHash(id, 1, 20, token)
			s.RegisterTransferFileStates(id, []TransferFileStateUpdate{
				{FileID: 1, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/f1")), FileSize: 1 << 20},
			}, TransferStateStarted)
			s.ListTransfers()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, id := newTTLTestTransfer(t, ttl, 1<<20)
			s.SetTransferFileState(id, 1, TransferStateRunning)
			s.AcknowledgeTransferFile(id, 1, 10)
			if ttlTestRun(s, id, 8*ttl, func(int) { tc.step(s, id) }) {
				t.Fatalf("transfer outlived 8 TTLs (ttl=%v) without progress", ttl)
			}
		})
	}
}

// TestAcknowledgeZeroByteFileCompletesTransfer covers empty files, whose ACK
// never advances AckedSize, and Missing (-1) ACKs, and pins that each regular
// file is counted exactly once and non-files never are.
func TestAcknowledgeZeroByteFileCompletesTransfer(t *testing.T) {
	s := newTestStore(t)
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	s.RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 1, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/empty")), FileSize: 0},
		{FileID: 2, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/five")), FileSize: 5},
		{FileID: 3, EntryType: encoding.EntryTypeDir, PathHash: xxh3.Hash128([]byte("/tmp/x/sub")), FileSize: 0},
		{FileID: 4, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte("/tmp/x/gone")), FileSize: 7},
	}, TransferStateStarted)
	s.ClipTransfer(transfer.ID)

	assertDone := func(step string, want uint64) Transfer {
		t.Helper()
		stored, ok := s.GetTransfer(transfer.ID)
		if !ok {
			t.Fatalf("%s: transfer not found", step)
		}
		if stored.Done != want {
			t.Fatalf("%s: done=%d want %d (states=%v)", step, stored.Done, want, stored.State)
		}
		return stored
	}

	s.AcknowledgeTransferFile(transfer.ID, 1, 0)
	s.AcknowledgeTransferFile(transfer.ID, 2, 5)
	s.AcknowledgeTransferFile(transfer.ID, 4, -1) // a file that vanished counts as Missing
	s.MaybeLogTransferComplete(transfer.ID)
	stored := assertDone("ack all", 3)
	if !stored.CompleteLogged || stored.State[1] != TransferStateDone || stored.State[4] != TransferStateMissing {
		t.Fatalf("expected completion: complete=%v states=%v", stored.CompleteLogged, stored.State)
	}
	s.AcknowledgeTransferFile(transfer.ID, 1, 0)
	assertDone("re-ack empty file", 3)
	s.AcknowledgeTransferFile(transfer.ID, 1, -1)
	assertDone("missing ack after done", 3)
	s.AcknowledgeTransferFile(transfer.ID, 4, -1)
	assertDone("repeated missing ack", 3)
	s.AcknowledgeTransferFile(transfer.ID, 3, -1)
	assertDone("missing ack for directory", 3)
}
