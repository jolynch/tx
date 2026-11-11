package store

// Tests of the transfer's compression state, rate summary, and the
// bottleneck classes its log lines report (compression.go).

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/zeebo/xxh3"
)

// fileCompressionFixture registers three regular files (IDs 1 to 3, 10 bytes
// each) in a transfer with the given store.
func fileCompressionFixture(t *testing.T, s *Store) string {
	t.Helper()
	transfer, err := s.NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	updates := make([]TransferFileStateUpdate, 3)
	for i := range updates {
		path := fmt.Sprintf("/tmp/x/f%d", i+1)
		updates[i] = TransferFileStateUpdate{FileID: uint64(i + 1), EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte(path)), FileSize: 10}
	}
	s.RegisterTransferFileStates(transfer.ID, updates, TransferStateRunning)
	return transfer.ID
}

// sampleFileCompression returns a state that differs from the zero value in
// both fields, so a round trip cannot pass by returning zeros.
func sampleFileCompression() policy.FileCompression {
	p := policy.NewCompressionPolicy()
	for range 6 {
		p.DecideSampled(policy.CompressionModeNone, policy.CompressionMetrics{
			LogicalSize: 4 << 20, WireSize: 4 << 20,
			PrepareLatency: time.Millisecond, WriteLatency: 20 * time.Millisecond,
		}, nil)
	}
	return policy.FileCompression{Mode: policy.CompressionModeLz4, Policy: *p}
}

func TestFileCompressionRoundTrip(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)

	if _, ok := s.LoadFileCompression(id, 1); ok {
		t.Fatalf("Load before any Save reported a hit")
	}
	if _, ok := s.LoadFileCompression("no-such-transfer", 1); ok {
		t.Fatalf("Load on an unknown transfer reported a hit")
	}
	if _, ok := s.LoadFileCompression(id, 99); ok {
		t.Fatalf("Load on an unknown file reported a hit")
	}

	want := sampleFileCompression()
	saveFileCompression(s, id, 1, want)
	got, ok := s.LoadFileCompression(id, 1)
	if !ok {
		t.Fatalf("Load after Save reported a miss")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip changed the state:\n got %+v\nwant %+v", got, want)
	}
	if _, ok := s.LoadFileCompression(id, 2); ok {
		t.Fatalf("Save for file 1 is visible to file 2")
	}

	// A caller owns its copy: changing it must not change the stored state.
	got.Mode = policy.CompressionModeZstdDefault
	again, _ := s.LoadFileCompression(id, 1)
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("mutating a loaded copy changed the stored state")
	}

	// The last Save wins.
	next := policy.FileCompression{Mode: policy.CompressionModeNone, Policy: *policy.NewCompressionPolicy()}
	saveFileCompression(s, id, 1, next)
	if got, _ := s.LoadFileCompression(id, 1); !reflect.DeepEqual(got, next) {
		t.Fatalf("second Save did not replace the first: %+v", got)
	}

	// Save for an unknown transfer is a no-op and must not panic.
	saveFileCompression(s, "no-such-transfer", 1, want)
	if _, ok := s.LoadFileCompression("no-such-transfer", 1); ok {
		t.Fatalf("Save created state for an unknown transfer")
	}
}

func TestFileCompressionDroppedWhenFileDone(t *testing.T) {
	steps := []struct {
		name string
		done func(s *Store, id string)
	}{
		{"SetTransferFilesState", func(s *Store, id string) { s.SetTransferFilesState(id, []uint64{1}, TransferStateDone) }},
		{"ack completes file", func(s *Store, id string) { s.AcknowledgeTransferFile(id, 1, 10) }},
		{"batched ack completes file", func(s *Store, id string) {
			s.AcknowledgeTransferFiles([]AckEntry{{TxferID: id, FileID: 1, AckBytes: 10}})
		}},
	}
	for _, tc := range steps {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			id := fileCompressionFixture(t, s)
			st := sampleFileCompression()
			saveFileCompression(s, id, 1, st)
			saveFileCompression(s, id, 2, st)

			// A partial ACK is not completion.
			s.AcknowledgeTransferFile(id, 1, 4)
			if _, ok := s.LoadFileCompression(id, 1); !ok {
				t.Fatalf("state dropped by a partial ack")
			}

			tc.done(s, id)
			if _, ok := s.LoadFileCompression(id, 1); ok {
				t.Fatalf("state still present after file became Done")
			}
			if _, ok := s.LoadFileCompression(id, 2); !ok {
				t.Fatalf("state of an unfinished file was dropped")
			}
			saveFileCompression(s, id, 1, st)
			if _, ok := s.LoadFileCompression(id, 1); ok {
				t.Fatalf("Save on a Done file stored state")
			}
		})
	}
}

func TestFileCompressionDroppedWithTransfer(t *testing.T) {
	// The tests read the transfer's map directly: Load also misses once the
	// transfer leaves the store, so it cannot show the entries were freed.
	t.Run("delete", func(t *testing.T) {
		s := newTestStore(t)
		id := fileCompressionFixture(t, s)
		saveFileCompression(s, id, 1, sampleFileCompression())
		managed, _ := s.getManagedTransfer(id)
		if len(managed.compression) != 1 {
			t.Fatalf("entries after Save = %d, want 1", len(managed.compression))
		}
		if !s.DeleteTransfer(id) {
			t.Fatalf("DeleteTransfer returned false")
		}
		if managed.compression != nil {
			t.Fatalf("%d entries survived DeleteTransfer", len(managed.compression))
		}
		saveFileCompression(s, id, 1, sampleFileCompression())
		if managed.compression != nil {
			t.Fatalf("Save after DeleteTransfer stored state")
		}
	})
	t.Run("reap", func(t *testing.T) {
		s := newTestStoreWithOptions(t, WithTTL(40*time.Millisecond))
		id := fileCompressionFixture(t, s)
		saveFileCompression(s, id, 1, sampleFileCompression())
		managed, _ := s.getManagedTransfer(id)
		waitReaped(t, s, id)
		managed.mu.RLock()
		defer managed.mu.RUnlock()
		if managed.compression != nil {
			t.Fatalf("%d entries survived the transfer's reap", len(managed.compression))
		}
	})
}

// An ACK of -1 reports the file missing. Its state is freed and no later save
// is stored; a file revived by a later ACK of its bytes starts fresh.
func TestFileCompressionDroppedWhenFileMissing(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	saveFileCompression(s, id, 1, sampleFileCompression())
	saveFileCompression(s, id, 2, sampleFileCompression())

	s.AcknowledgeTransferFile(id, 1, -1)
	if _, ok := s.LoadFileCompression(id, 1); ok {
		t.Fatalf("state still present after the file went Missing")
	}
	if _, ok := s.LoadFileCompression(id, 2); !ok {
		t.Fatalf("state of another file was dropped")
	}
	saveFileCompression(s, id, 1, sampleFileCompression())
	if _, ok := s.LoadFileCompression(id, 1); ok {
		t.Fatalf("Save on a Missing file stored state")
	}

	// A partial ACK revives the file as Running; it has no state to resume.
	s.AcknowledgeTransferFile(id, 1, 4)
	if _, ok := s.LoadFileCompression(id, 1); ok {
		t.Fatalf("revived file kept state from before it went Missing")
	}
	saveFileCompression(s, id, 1, sampleFileCompression())
	if _, ok := s.LoadFileCompression(id, 1); !ok {
		t.Fatalf("Save on a revived file was rejected")
	}
}

// The state is a map value, and Go stores map values inline only up to 128
// bytes; a larger one is boxed and allocates on every insert.
func TestFileCompressionFitsInlineMapValue(t *testing.T) {
	if n := unsafe.Sizeof(policy.FileCompression{}); n > 128 {
		t.Fatalf("policy.FileCompression is %d bytes, over Go's 128-byte inline map value limit", n)
	}
}

// Load and Save are per window, but they must not allocate: the state is
// copied by value. That holds for an existing entry, and for an entry saved
// again after its file's entry was dropped, since the map keeps its buckets.
func TestFileCompressionDoesNotAllocate(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	st := sampleFileCompression()
	saveFileCompression(s, id, 1, st) // inserting the first entry allocates the map
	managed, _ := s.getManagedTransfer(id)
	updates := []FileCompressionUpdate{{FileID: 1, State: st}}

	if n := testing.AllocsPerRun(100, func() { s.RecordTransferWindows(id, WindowRecords{Compression: updates}) }); n != 0 {
		t.Fatalf("Save on an existing entry allocated %v times per call", n)
	}
	if n := testing.AllocsPerRun(100, func() {
		managed.dropFileCompression(1)
		s.RecordTransferWindows(id, WindowRecords{Compression: updates})
	}); n != 0 {
		t.Fatalf("Save after a drop allocated %v times per call", n)
	}
	var sink policy.FileCompression
	if n := testing.AllocsPerRun(100, func() { sink, _ = s.LoadFileCompression(id, 1) }); n != 0 {
		t.Fatalf("Load on an existing entry allocated %v times per call", n)
	}
	_ = sink
}

// saveFileCompression saves one file's state the way a SEND does, with its
// request's window hashes.
func saveFileCompression(s *Store, txferID string, fileID uint64, st policy.FileCompression) {
	s.RecordTransferWindows(txferID, WindowRecords{Compression: []FileCompressionUpdate{{FileID: fileID, State: st}}})
}

// A save for a file ID the transfer does not hold is ignored.
func TestFileCompressionSaveOutOfRangeIsNoOp(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	st := sampleFileCompression()
	for _, fileID := range []uint64{4, 99, 1 << 40, ^uint64(0)} {
		saveFileCompression(s, id, fileID, st)
		if _, ok := s.LoadFileCompression(id, fileID); ok {
			t.Fatalf("state stored for out-of-range file %d", fileID)
		}
	}
	managed, _ := s.getManagedTransfer(id)
	if managed.compression != nil {
		t.Fatalf("%d entries after out-of-range saves", len(managed.compression))
	}
	// An in-range update in the same batch is still stored.
	s.RecordTransferWindows(id, WindowRecords{Compression: []FileCompressionUpdate{{FileID: 99, State: st}, {FileID: 2, State: st}}})
	if got, ok := s.LoadFileCompression(id, 2); !ok || got != st {
		t.Fatalf("in-range update lost beside an out-of-range one")
	}
}

// TestTransferRatesSummarize checks the rate summary a transfer's SENDs
// build: windows and files count once each, each estimate keeps its own
// readings, the ratio covers only windows with both, the completion line
// reports them, and a deleted transfer takes no more.
// TestProgressLineShowsBottleneckSinceLastLine checks the progress line's
// bottleneck bracket: the classes of the adaptive frames recorded since the
// previous line, and no bracket when none were.
func TestProgressLineShowsBottleneckSinceLastLine(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	managed, ok := s.getManagedTransfer(id)
	if !ok {
		t.Fatal("transfer not found")
	}
	line := func(done int64) string {
		managed.mu.Lock()
		defer managed.mu.Unlock()
		managed.transfer.TotalSize, managed.transfer.DoneSize = 100, done
		got, ok := managed.progressLineLocked(time.Now())
		if !ok {
			t.Fatalf("no progress line at %d of 100 bytes", done)
		}
		return got
	}
	record := func(classes [4]uint32) {
		var r RateSummary
		r.Add(WindowRate{Classes: classes})
		if !s.RecordTransferWindows(id, WindowRecords{Rates: r}) {
			t.Fatal("RecordTransferWindows failed")
		}
	}
	if got := line(20); strings.Contains(got, "%]") {
		t.Fatalf("line before any adaptive frame has a bottleneck bracket: %q", got)
	}
	record([4]uint32{policy.BottleneckNetwork: 3, policy.BottleneckReceiver: 1})
	if got := line(40); !strings.HasSuffix(got, " [net 75% rcv 25%]") {
		t.Fatalf("first line: %q", got)
	}
	record([4]uint32{policy.BottleneckReceiver: 2})
	if got := line(60); !strings.HasSuffix(got, " [rcv 99+]") {
		t.Fatalf("second line shows %q, want only the frames since the first", got)
	}
}

func TestTransferRatesSummarize(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	var a, b, retry RateSummary
	a.Add(WindowRate{FileID: 0, FirstWindow: true, Sampled: true, KernelBps: 100 << 20, UserBps: 200 << 20, Classes: [4]uint32{2: 3, 3: 1}})
	a.Add(WindowRate{FileID: 0, KernelBps: 50 << 20})
	b.Add(WindowRate{FileID: 1, FirstWindow: true, UserBps: 10 << 20})
	// A resent first window counts as a window but not as another file.
	retry.Add(WindowRate{FileID: 1, FirstWindow: true})
	for _, rec := range []WindowRecords{{Rates: a, RatedFiles: []uint64{0}}, {Rates: b, RatedFiles: []uint64{1}}, {Rates: retry, RatedFiles: []uint64{1}}} {
		if !s.RecordTransferWindows(id, rec) {
			t.Fatal("RecordTransferWindows failed")
		}
	}
	got, ok := s.TransferRates(id)
	if !ok || got.Files != 2 || got.Windows != 4 || got.Sampled != 1 || got.Bottlenecks != (ClassCounts{Network: 3, Receiver: 1}) ||
		got.Kernel.N != 2 || got.Kernel.Min != 50<<20 || got.Kernel.Max != 100<<20 ||
		got.User.N != 2 || got.User.Min != 10<<20 || got.User.Max != 200<<20 ||
		got.RatioN != 1 || got.RatioSum != 2 {
		t.Fatalf("summary %+v", got)
	}
	line := formatRates(id, 4, got)
	for _, want := range []string{"files=2/4 windows=4 sampled=1", "kernel=[n=2 min=", "userspace=[n=2 ", "userspace/kernel=2.00x"} {
		if !strings.Contains(line, want) {
			t.Fatalf("line %q lacks %q", line, want)
		}
	}
	if line := formatRates(id, 1, RateSummary{Windows: 1}); !strings.Contains(line, "kernel=[n=0]") || !strings.Contains(line, "userspace/kernel=n/a") || strings.Contains(line, "bottleneck=") {
		t.Fatalf("line without readings: %q", line)
	}
	classes := RateSummary{Bottlenecks: ClassCounts{Unknown: 1, Sender: 20, Network: 920, Receiver: 59}}
	if line := formatRates(id, 1, classes); !strings.Contains(line, "bottleneck=[net 92% rcv  6% snd  2%] frames=1000") {
		t.Fatalf("line with bottleneck classes: %q", line)
	}
	if line := formatRates(id, 1, RateSummary{Bottlenecks: ClassCounts{Unknown: 3, Network: 1}}); !strings.Contains(line, "bottleneck=[net 25%] frames=4") {
		t.Fatalf("unknown frames must count toward the total only: %q", line)
	}
	if line := formatRates(id, 1, RateSummary{Bottlenecks: ClassCounts{Unknown: 3}}); strings.Contains(line, "bottleneck=") {
		t.Fatalf("line with no known class: %q", line)
	}
	if line := formatRates(id, 1, RateSummary{Bottlenecks: ClassCounts{Network: 999, Sender: 1}}); !strings.Contains(line, "bottleneck=[net 99+ snd <1%] frames=1000") {
		t.Fatalf("line with a whole and a trace share: %q", line)
	}
	s.DeleteTransfer(id)
	if s.RecordTransferWindows(id, WindowRecords{Rates: a}) {
		t.Fatal("a deleted transfer took a rate summary")
	}
}

// The state a window saves is the state the next one loads, policy included.
func TestFileCompressionLoadReturnsSavedPolicy(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	want := sampleFileCompression()
	if want.Policy == *policy.NewCompressionPolicy() {
		t.Fatalf("sample policy equals a fresh one")
	}
	saveFileCompression(s, id, 1, want)
	got, ok := s.LoadFileCompression(id, 1)
	if !ok || got != want {
		t.Fatalf("loaded %+v, %v; want %+v", got, ok, want)
	}
}

// Registering a file as Done frees its state, as every other route to Done
// does; registering it below Done keeps it.
func TestFileCompressionDroppedWhenRegisteredDone(t *testing.T) {
	s := newTestStore(t)
	id := fileCompressionFixture(t, s)
	st := sampleFileCompression()
	saveFileCompression(s, id, 1, st)
	saveFileCompression(s, id, 2, st)
	update := func(fileID uint64) []TransferFileStateUpdate {
		path := fmt.Sprintf("/tmp/x/f%d", fileID)
		return []TransferFileStateUpdate{{FileID: fileID, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte(path)), FileSize: 10}}
	}

	s.RegisterTransferFileStates(id, update(2), TransferStateRunning)
	if _, ok := s.LoadFileCompression(id, 2); !ok {
		t.Fatalf("state dropped by a registration below Done")
	}
	s.RegisterTransferFileStates(id, update(1), TransferStateDone)
	if _, ok := s.LoadFileCompression(id, 1); ok {
		t.Fatalf("state survived registering the file Done")
	}
	if _, ok := s.LoadFileCompression(id, 2); !ok {
		t.Fatalf("state of another file was dropped")
	}
}
