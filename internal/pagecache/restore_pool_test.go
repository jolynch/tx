package pagecache

import (
	"bytes"
	"context"
	"log"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRestoreWorkerPoolSendBatchDrains(t *testing.T) {
	if !TouchSupported() {
		t.Skip("touch not supported on this platform")
	}

	prevEvict := evictPagesFn
	prevTouch := touchPagesFn
	var observed atomic.Int64
	evictPagesFn = func(string) error {
		observed.Add(1)
		return nil
	}
	touchPagesFn = func(string, []byte, int, bool) (int, error) {
		return 0, nil
	}
	defer func() {
		evictPagesFn = prevEvict
		touchPagesFn = prevTouch
	}()

	var logBuf bytes.Buffer
	prevLog := log.Default().Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prevLog)

	p := NewRestoreWorkerPool(2, 4)
	defer p.Close()

	makeItem := func(path string) TouchEntry {
		e := &CacheEntry{}
		_ = e.SetPageBits([]byte{0x01}, 1)
		return TouchEntry{Path: path, Entry: e}
	}
	const N = 16
	batch := make([]TouchEntry, N)
	for i := range batch {
		batch[i] = makeItem("/file")
	}
	p.SendBatch("tx-demo", batch)

	// Wait for the workers to drain.
	deadline := time.Now().Add(5 * time.Second)
	for p.applied.Load() < int64(N) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := p.applied.Load(); got != int64(N) {
		t.Fatalf("applied got %d, want %d", got, N)
	}
	if got := p.dropped.Load(); got != 0 {
		t.Fatalf("expected no drops on a happy SendBatch, got %d", got)
	}

	// Drain the producer goroutine before inspecting the log buffer.
	p.Close()

	logOut := logBuf.String()
	beginRe := regexp.MustCompile(`cache-restore: batch tid=tx-demo items=16 begin`)
	endRe := regexp.MustCompile(`cache-restore: batch tid=tx-demo items=16 sent=16 dropped=0 duration=\S+`)
	if !beginRe.MatchString(logOut) {
		t.Fatalf("missing begin log line, got: %q", logOut)
	}
	if !endRe.MatchString(logOut) {
		t.Fatalf("missing end log line with duration, got: %q", logOut)
	}
	if got := len(regexp.MustCompile(`cache-restore: batch tid=`).FindAllString(logOut, -1)); got != 2 {
		t.Fatalf("expected exactly 2 batch log lines, got %d: %q", got, logOut)
	}
}

func TestRestoreWorkerPoolSendBatchAbortsOnPoolClose(t *testing.T) {
	if !TouchSupported() {
		t.Skip("touch not supported on this platform")
	}

	prevEvict := evictPagesFn
	prevTouch := touchPagesFn
	release := make(chan struct{})
	evictPagesFn = func(string) error {
		<-release
		return nil
	}
	touchPagesFn = func(string, []byte, int, bool) (int, error) {
		return 0, nil
	}
	defer func() {
		evictPagesFn = prevEvict
		touchPagesFn = prevTouch
	}()

	// Channel capacity 2, workers parked on one in-flight item. The
	// producer fills the channel + one in-flight, then blocks. Closing
	// the pool cuts the producer loose and workers exit immediately
	// (no per-item drain); the buffered items get counted as dropped
	// by Close.
	p := NewRestoreWorkerPool(1, 2)

	makeItem := func() TouchEntry {
		e := &CacheEntry{}
		_ = e.SetPageBits([]byte{0x01}, 1)
		return TouchEntry{Path: "/file", Entry: e}
	}
	const N = 10
	batch := make([]TouchEntry, N)
	for i := range batch {
		batch[i] = makeItem()
	}
	p.SendBatch("tx-abort", batch)

	// Give the producer a moment to fill the channel and block.
	time.Sleep(50 * time.Millisecond)

	close(release)
	p.Close()

	if got := p.dropped.Load(); got == 0 {
		t.Fatalf("expected some items to drop after pool close, got %d", got)
	}
	if got := p.applied.Load() + p.dropped.Load(); got != int64(N) {
		t.Fatalf("expected applied+dropped to equal %d, got applied=%d dropped=%d",
			N, p.applied.Load(), p.dropped.Load())
	}
}

func TestRestoreWorkerPoolSkipsEmptyAndMissing(t *testing.T) {
	p := NewRestoreWorkerPool(1, 4)
	defer p.Close()
	ctx := context.Background()
	if p.Send(ctx, TouchEntry{Path: "", Entry: &CacheEntry{}}) {
		t.Fatalf("empty-path send should be rejected")
	}
	if p.Send(ctx, TouchEntry{Path: "/x", Entry: &CacheEntry{}}) {
		t.Fatalf("empty-entry send should be rejected")
	}
	if p.Send(ctx, TouchEntry{Path: "/x", Entry: nil}) {
		t.Fatalf("nil-entry send should be rejected")
	}
	if got := p.accepted.Load(); got != 0 {
		t.Fatalf("expected accepted=0 after invalid items, got %d", got)
	}
	if got := p.dropped.Load(); got != 3 {
		t.Fatalf("expected dropped=3 (one per rejected item), got %d", got)
	}
}

func TestRestoreWorkerPoolNilSafe(t *testing.T) {
	var p *RestoreWorkerPool
	if p.Send(context.Background(), TouchEntry{Path: "/x"}) {
		t.Fatalf("nil pool Send should be a no-op returning false")
	}
	p.SendBatch("tx-nil", []TouchEntry{{Path: "/x"}}) // must not panic
	p.Close()                                         // must not panic
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Close() }()
	wg.Wait()
}

func TestRestoreWorkerPoolCloseAbandonsBuffered(t *testing.T) {
	if !TouchSupported() {
		t.Skip("touch not supported on this platform")
	}

	prevEvict := evictPagesFn
	prevTouch := touchPagesFn
	release := make(chan struct{})
	evictPagesFn = func(string) error {
		<-release
		return nil
	}
	touchPagesFn = func(string, []byte, int, bool) (int, error) {
		return 0, nil
	}
	defer func() {
		evictPagesFn = prevEvict
		touchPagesFn = prevTouch
	}()

	var logBuf bytes.Buffer
	prevLog := log.Default().Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prevLog)

	// One worker, capacity 4. Stub evict parks the in-flight item. Send
	// 5 items synchronously so 1 is in-flight + 4 are buffered when we
	// close.
	p := NewRestoreWorkerPool(1, 4)
	e := &CacheEntry{}
	_ = e.SetPageBits([]byte{0x01}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if !p.Send(ctx, TouchEntry{Path: "/file", Entry: e}) {
			t.Fatalf("Send %d failed", i)
		}
	}

	// Close cuts the worker loose; release the in-flight apply only after the
	// shutdown signal is closed, so the remaining buffer must be abandoned.
	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()
	<-p.done
	close(release)
	<-closed

	if got := p.dropped.Load(); got == 0 {
		t.Fatalf("expected at least one buffered item to be abandoned, got dropped=0")
	}
	if !bytes.Contains(logBuf.Bytes(), []byte("cache-restore: pool closed with")) {
		t.Fatalf("expected pool-closed log line, got: %q", logBuf.String())
	}
}

func TestRestoreWorkerPoolSendAfterCloseDrops(t *testing.T) {
	p := NewRestoreWorkerPool(1, 4)
	p.Close()
	e := &CacheEntry{}
	_ = e.SetPageBits([]byte{0x01}, 1)
	if p.Send(context.Background(), TouchEntry{Path: "/x", Entry: e}) {
		t.Fatalf("send after close should be rejected")
	}
	if got := p.dropped.Load(); got != 1 {
		t.Fatalf("expected dropped=1 after rejected post-close send, got %d", got)
	}
}
