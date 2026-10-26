//go:build linux

package memxfer

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
)

// sizes is a mix of small files and files of several frames.
func sizes() []int64 {
	var out []int64
	for i := range 40 {
		out = append(out, int64(4<<10)<<(i%5)) // 4 KiB to 64 KiB
	}
	for i := range 6 {
		out = append(out, int64(1+i%3)<<20) // 1 to 3 MiB
	}
	return out
}

// quiet silences the server's per-transfer log lines for the test.
func quiet(t testing.TB) {
	out := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(out) })
}

// TestCleanTransfer copies every file in each session mode with no faults
// and checks every byte.
func TestCleanTransfer(t *testing.T) {
	quiet(t)
	for _, mode := range Modes() {
		t.Run(mode.String(), func(t *testing.T) {
			e, err := Start(Config{Sizes: sizes(), Seed: 1, Mode: mode, KeepAlive: true})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			resp, err := e.Round(context.Background())
			if err != nil || len(resp.Errors) > 0 {
				t.Fatalf("round: %v %v", err, resp.Errors)
			}
			if p, m := e.Pending(), e.Mismatched(); len(p) > 0 || len(m) > 0 {
				t.Fatalf("pending %v, mismatched %v", p, m)
			}
		})
	}
}

// badDayRounds bounds the resume rounds a bad-day transfer may take.
const badDayRounds = 20

// TestBadDayCompletes calibrates the bad-day profile: under it, a transfer
// must still finish with every byte correct within badDayRounds resume
// rounds, in every session mode. Each mode runs a different seed, and
// TX_MEMXFER_SEED_BASE moves them all, so the calibration can be swept over
// many seeds.
func TestBadDayCompletes(t *testing.T) {
	quiet(t)
	base := uint64(1)
	if raw := os.Getenv("TX_MEMXFER_SEED_BASE"); raw != "" {
		var err error
		if base, err = strconv.ParseUint(raw, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	// A stuck mode fails here rather than at Go's test timeout.
	deadline := time.Now().Add(5 * time.Minute)
	fired := map[string]bool{}
	for m, mode := range Modes() {
		seed := base + uint64(m)
		t.Run(fmt.Sprintf("%s/seed=%d", mode, seed), func(t *testing.T) {
			inj := faults.New(faults.Config{Seed: seed, Profile: "bad-day"})
			e, err := Start(Config{Sizes: sizes(), Seed: seed, Mode: mode, KeepAlive: true, Faults: inj, KeepAliveTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			rounds := 0
			for ; rounds < badDayRounds && len(e.Pending()) > 0 && time.Now().Before(deadline); rounds++ {
				roundEnd := time.Now().Add(30 * time.Second)
				if roundEnd.After(deadline) {
					roundEnd = deadline
				}
				ctx, cancel := context.WithDeadline(context.Background(), roundEnd)
				resp, err := e.Round(ctx)
				cancel()
				if testing.Verbose() {
					t.Logf("round %d: pending %d, err %v, errors %d %v", rounds, len(e.Pending()), err, len(resp.Errors), firstN(resp.Errors, 2))
				}
			}
			if p, m := e.Pending(), e.Mismatched(); len(p) > 0 || len(m) > 0 {
				t.Fatalf("after %d rounds: pending %v, mismatched %v; faults %v; repro %s",
					rounds, p, m, inj.Counts().InjectedByKind(), inj.Repro())
			}
			t.Logf("%d rounds; faults %v", rounds, inj.Counts().InjectedByKind())
			for k := range inj.Counts().InjectedByKind() {
				fired[k] = true
			}
		})
	}
	// The profile must actually disturb the transfer, not just let it pass.
	if len(fired) < 10 {
		t.Fatalf("bad-day fired only %d kinds across every mode: %v", len(fired), fired)
	}
}

func firstN(errs []error, n int) []error { return errs[:min(n, len(errs))] }

// TestDupACKReachesServerTwice checks MSG dup sends every ACK twice and
// that every file still ends up correct. The duplicate reaches the server
// first, so the real ACK is the replay; a replayed ACK currently fails with
// CONFLICT (docs/IDEAS.md), so the round's errors are not checked here.
func TestDupACKReachesServerTwice(t *testing.T) {
	quiet(t)
	acks := func(inj *faults.Injector) int {
		sink := events.NewSink()
		var n atomic.Int64
		sink.Subscribe(func(e events.Event) {
			if v, ok := e.Get("verb"); ok && e.Name == "cmd_end" && v == "ACK" {
				n.Add(1)
			}
		})
		e, err := Start(Config{Sizes: sizes()[:8], Seed: 1, Faults: inj, Events: sink})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		if _, err := e.Round(context.Background()); err != nil {
			t.Fatal(err)
		}
		if p, m := e.Pending(), e.Mismatched(); len(p) > 0 || len(m) > 0 {
			t.Fatalf("pending %v, mismatched %v", p, m)
		}
		return int(n.Load())
	}
	clean := acks(nil)
	// Each ACK reaches the server at least twice; retries of the failing
	// replay add more.
	if dup := acks(faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyMsg: "dup=1"}})); clean == 0 || dup < 2*clean {
		t.Fatalf("server saw %d ACKs with dup, %d without; want at least twice as many", dup, clean)
	}
}

// TestRoundWaitsForWindows checks a download returns only after every
// window of a split file has stopped writing and closed its output, even
// when an ACK fails while other windows are in flight. A replayed ACK
// always fails (docs/IDEAS.md), so duplicating ACKs fails the first
// window's. Its retries take about 1.5s, so write latency makes each
// window take longer than that, keeping the next windows writing when the
// ACK gives up.
func TestRoundWaitsForWindows(t *testing.T) {
	quiet(t)
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{
		faults.FamilyMsg:  "dup=1",
		faults.FamilyDisk: "write.latency=1:β(100ms,150ms,190ms,200ms)",
	}})
	e, err := Start(Config{Sizes: []int64{8 << 20}, Seed: 1, Faults: inj})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	resp, err := e.Round(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	open, writing := e.InFlight()
	// The test needs a failed ACK. Once a replayed ACK succeeds, it must
	// find another way to fail one.
	if !slices.ContainsFunc(resp.Errors, func(err error) bool { return strings.Contains(err.Error(), "acknowledge download failed") }) {
		t.Fatalf("no ACK failed (errors %v), so the test did not reach the path it checks", resp.Errors)
	}
	if open != 0 || writing != 0 {
		t.Fatalf("download returned with %d outputs open and %d writes in flight", open, writing)
	}
	before := e.Writes()
	time.Sleep(300 * time.Millisecond)
	if after := e.Writes(); after != before {
		t.Fatalf("%d output writes happened after the download returned", after-before)
	}
}

// TestReadFlipReachesZeroCopyFiles checks read.flip corrupts files large
// enough to splice: with it on, SEND reads every frame through the faulted
// reader. No layer detects a flip on the sender's read without --verify, so
// the corrupt bytes arrive and the files mismatch.
func TestReadFlipReachesZeroCopyFiles(t *testing.T) {
	quiet(t)
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: "read.flip=1"}})
	e, err := Start(Config{Sizes: []int64{2 << 20, 3 << 20}, Seed: 1, Faults: inj})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m := e.Mismatched(); len(m) != 2 {
		t.Fatalf("mismatched %v after flipping every read; want both files", m)
	}
	// Verification rereads the source, which no longer flips, so it finds
	// the corrupt copies.
	if err := e.Restart(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Verify(0); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("verify of corrupt copies returned %v", err)
	}
}

// TestRoundsResume checks a round resumes each file from the bytes the
// server acknowledged, over a restarted server and client. The first round
// is cancelled once the server has applied its first ACK, which leaves the
// file part done.
func TestRoundsResume(t *testing.T) {
	quiet(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := events.NewSink()
	sink.Subscribe(func(ev events.Event) {
		if v, ok := ev.Get("verb"); ok && ev.Name == "cmd_end" && v == "ACK" {
			cancel()
		}
	})
	// Write latency keeps the later windows writing when the first ACK lands.
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: "write.latency=1:β(20ms,30ms,45ms,50ms)"}})
	e, err := Start(Config{Sizes: []int64{8 << 20}, Seed: 1, Faults: inj, Events: sink})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Round(ctx); err != nil {
		t.Fatal(err)
	}
	acked := e.AckedBytes(1)
	if acked <= 0 || acked >= 8<<20 || len(e.Pending()) != 1 {
		t.Fatalf("first round acknowledged %d bytes, pending %v; want part of the file", acked, e.Pending())
	}
	if err := e.Restart(nil); err != nil {
		t.Fatal(err)
	}
	fired := inj.Counts().Injected
	resp, err := e.Round(context.Background())
	if err != nil || len(resp.Errors) > 0 {
		t.Fatalf("resumed round: %v %v", err, resp.Errors)
	}
	if inj.Counts().Injected != fired {
		t.Fatalf("faults fired after Restart(nil): %v", inj.Counts().InjectedByKind())
	}
	if opened := e.Opened(1); len(opened) == 0 || slices.Min(opened) != acked {
		t.Fatalf("resumed round opened outputs at %v; want the first at %d", opened, acked)
	}
	if p, m := e.Pending(), e.Mismatched(); len(p) > 0 || len(m) > 0 {
		t.Fatalf("pending %v, mismatched %v", p, m)
	}
	if err := e.Verify(0); err != nil {
		t.Fatalf("verify of a correct copy: %v", err)
	}
}

// TestRestartJoinsOldHandlers checks Restart waits for the old server's
// handlers, so one still running cannot change the transfer under the new
// server. A round is cancelled while its SEND sleeps on read latency before
// a flipped read; if that SEND outlived the restart, it would record its
// flipped window hash over the clean round's, and the clean ACK, held until
// the old SEND ends, would fail.
func TestRestartJoinsOldHandlers(t *testing.T) {
	quiet(t)
	sink := events.NewSink()
	reading := make(chan struct{})
	oldEnded := make(chan struct{})
	var readOnce, endOnce sync.Once
	inj := faults.New(faults.Config{Seed: 1, Events: sink, Specs: map[faults.Family]string{faults.FamilyDisk: "read.latency=1:β(150ms,α=2,β=8,250ms), read.flip=1"}})
	sink.Subscribe(func(ev events.Event) {
		if kind, ok := ev.Get("kind"); ev.Name == "fault" && ok && kind == "disk.read.latency" {
			readOnce.Do(func() { close(reading) })
		}
		verb, _ := ev.Get("verb")
		if verb == "SEND" && ev.Name == "cmd_end" && inj.Counts().InjectedByKind()["disk.read.flip"] > 0 {
			endOnce.Do(func() { close(oldEnded) })
		}
		if verb == "ACK" && ev.Name == "cmd_start" {
			select {
			case <-oldEnded:
			case <-time.After(time.Second):
			}
		}
	})
	e, err := Start(Config{Sizes: []int64{4096}, Seed: 1, Faults: inj, Events: sink})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _, _ = e.Round(ctx) }()
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("the SEND never reached its read")
	}
	cancel()
	<-done
	if err := e.Restart(nil); err != nil {
		t.Fatal(err)
	}
	if r, err := e.Round(context.Background()); err != nil || len(r.Errors) > 0 {
		t.Fatalf("clean round after Restart(nil): %v %v; old faults %v", err, r.Errors, inj.Counts().InjectedByKind())
	}
}

// TestVerifyBudget checks a verify pass its budget cuts short is an error,
// never a pass over the part it reached, and that a full pass of a correct
// copy succeeds. Read latency on the server keeps the pass from finishing
// inside the budget.
func TestVerifyBudget(t *testing.T) {
	quiet(t)
	sizes := make([]int64, 16)
	for i := range sizes {
		sizes[i] = 1 << 20
	}
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: "read.latency=1:β(20ms,30ms,45ms,50ms)"}})
	e, err := Start(Config{Sizes: sizes, Seed: 1, Faults: inj})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if resp, err := e.Round(context.Background()); err != nil || len(resp.Errors) > 0 || len(e.Pending()) > 0 {
		t.Fatalf("round: %v %v, pending %v", err, resp.Errors, e.Pending())
	}
	if err := e.Verify(5 * time.Millisecond); err == nil || !strings.Contains(err.Error(), "verified only") {
		t.Fatalf("verify cut short by its budget returned %v", err)
	}
	if err := e.Verify(0); err != nil {
		t.Fatalf("full verify of a correct copy: %v", err)
	}
	// A copy with the right bytes and more after them is wrong too, as
	// the CLI's metadata check finds.
	if _, err := e.dst[0].WriteAt([]byte("extra"), sizes[0]); err != nil {
		t.Fatal(err)
	}
	if err := e.Verify(0); err == nil || len(e.Mismatched()) != 1 {
		t.Fatalf("verify of a copy with extra bytes returned %v; mismatched %v", err, e.Mismatched())
	}
}

// TestUnsyncedCountsFailedSyncs checks Env.Unsynced, the durability signal
// the fault property reads, counts outputs whose background sync failed,
// and none when every sync succeeds; and that a round whose syncs failed
// reports an error rather than success. Every file still arrives, since a
// failed sync does not stop the transfer.
//
// A failed fstat in the sync batch is covered in internal/fsync: here the
// injector faults both sides, and the server's SEND fstat shares its gate.
func TestUnsyncedCountsFailedSyncs(t *testing.T) {
	quiet(t)
	for spec, failing := range map[string]bool{"": false, "fdatasync.fail=1": true} {
		var inj *faults.Injector
		if spec != "" {
			inj = faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilySys: spec}})
		}
		e, err := Start(Config{Sizes: sizes()[:4], Seed: 1, Faults: inj})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := e.Round(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got := e.Unsynced(); (got > 0) != failing {
			t.Fatalf("%q: Unsynced %d", spec, got)
		}
		if got := len(resp.Errors) > 0; got != failing {
			t.Fatalf("%q: round reported errors %t, want %t: %v", spec, got, failing, resp.Errors)
		}
		if p := e.Pending(); len(p) > 0 {
			t.Fatalf("%q: files %v pending, want every file copied despite failed syncs", spec, p)
		}
		e.Close()
	}
}

// FuzzMemTransferUnderFaults is the pbt-faults property for whole
// transfers, in every session mode, under faults on both sides: a swarm, the
// bad-day profile, or a swarm of benign kinds only. Up to faultRounds short
// rounds run under the faults, then a bounded verify pass, then a restart
// without faults and one clean round. It checks:
//
//  1. No silent corruption or loss: after every round, each file the
//     server marks done holds exactly the source's bytes; and a round that
//     reports no errors had no background output sync fail or get skipped,
//     and, unless its deadline cut it short, left no file pending.
//  2. Verify is sound: a verify pass that reports success means every file
//     matches.
//  3. Convergence: after the clean round every file is done and correct,
//     and verification passes.
//  4. Benign faults are harmless: under only latencies and failures of
//     advisory syscalls (fadvise, readahead, mmap, mincore, setsockopt),
//     the first round finishes every file without an error.
//  5. Liveness and hygiene: each step returns within its budget, and once
//     the environment closes, the goroutines and descriptors it used are
//     released.
//
// Go's fuzzer stops any input that runs past 10s, with no message, so the
// budgets add up to about 8s and each step reports its own overrun first.
//
// It runs only with TX_PBT_FAULTS set (make pbt-faults), since it fails
// until the bugs it finds are fixed. With TX_FAULTS_* set, every input runs
// under those faults instead of its own, so a failure's repro line reruns
// it. TX_PBT_FAULTS_EXCLUDE turns kinds off, to look past a failure already
// recorded in docs/IDEAS.md.
func FuzzMemTransferUnderFaults(f *testing.F) {
	faults.SkipUnlessPBT(f)
	quiet(f)
	// The setup before f.Fuzz runs once per fuzzing process, outside any
	// input's 10s.
	warmUpProcess(f)
	for m := range Modes() {
		f.Add(uint64(m+1), uint8(m), uint8(m*37), uint8(faults.PBTSwarm))
	}
	f.Add(uint64(99), uint8(0), uint8(0), uint8(faults.PBTBadDay))       // seed#9
	f.Add(uint64(1), uint8(0), uint8(0x20), uint8(faults.PBTSwarm))      // seed#10: open.enoent leaves a file Missing (docs/IDEAS.md)
	f.Add(uint64(7), uint8(1), uint8(0x12), uint8(faults.PBTBenign))     // seed#11
	f.Add(uint64(8), uint8(6), uint8(0b0110), uint8(faults.PBTBenign))   // seed#12
	f.Add(uint64(24449), uint8(2), uint8(0x35), uint8(faults.PBTBenign)) // seed#13: the largest mix over one connection
	f.Fuzz(func(t *testing.T, seed uint64, modeByte, flags, faultSet uint8) {
		goroutines, fds := resources()
		mode := Modes()[int(modeByte)%len(Modes())]
		cfg, err := faults.PBTConfig(seed, faults.PBTFaultSet(faultSet))
		if err != nil {
			t.Fatal(err)
		}
		inj, err := faults.PBTInjector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		comp := []string{"none", "lz4", "zstd", "adapt"}[flags>>1&3]
		sizes := pbtSizes(seed, flags>>4&3)
		e, err := Start(Config{Sizes: sizes, Seed: seed, Mode: mode, KeepAlive: flags&1 != 0, Comp: comp,
			Concurrency: 1 << (flags >> 6), Faults: inj, KeepAliveTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		closed := false
		defer func() {
			if !closed {
				e.Close()
			}
		}()
		where := func() string {
			return fmt.Sprintf("mode %s, comp %s, %d files, faults %v; repro %s", mode, comp, len(sizes), inj.Counts().InjectedByKind(), inj.Repro())
		}
		// round runs one bounded round and checks liveness and property 1.
		round := func(name string, timeout time.Duration) tx.StartFromManifestResponse {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			type result struct {
				resp tx.StartFromManifestResponse
				err  error
			}
			r := within(t, name, timeout+roundGrace, where, func() result {
				resp, err := e.Round(ctx)
				return result{resp, err}
			})
			if r.err != nil {
				t.Fatalf("%s: %v; %s", name, r.err, where())
			}
			if bad := corruptDone(e); len(bad) > 0 {
				// A round resumes from bytes earlier rounds acknowledged, so
				// the corruption may date from one of them.
				t.Fatalf("after the %s, files %v are done with wrong bytes (errors %v); %s", name, bad, firstN(r.resp.Errors, 3), where())
			}
			if p := e.Pending(); len(p) > 0 && len(r.resp.Errors) == 0 && ctx.Err() == nil {
				t.Fatalf("%s reported success with files %v pending; %s", name, p, where())
			}
			if n := e.Unsynced(); n > 0 && len(r.resp.Errors) == 0 {
				t.Fatalf("%s reported success though %d output syncs failed or were skipped under it; %s", name, n, where())
			}
			return r.resp
		}
		verify := func(name string, budget time.Duration) error {
			return within(t, name, budget+budget/4+roundGrace, where, func() error { return e.Verify(budget) })
		}

		if inj.Benign() {
			// Benign faults only slow a round, so it gets a clean round's time.
			resp := round("benign round", cleanRoundTimeout)
			if len(resp.Errors) > 0 || len(e.Pending()) > 0 {
				t.Fatalf("benign faults failed the round: pending %v, errors %v; %s", e.Pending(), firstN(resp.Errors, 3), where())
			}
		}
		for r := 0; r < faultRounds && len(e.Pending()) > 0; r++ {
			round(fmt.Sprintf("fault round %d", r), faultRoundTimeout)
		}
		if err := verify("verify under faults", faultVerifyBudget); err == nil {
			if m := e.Mismatched(); len(m) > 0 {
				t.Fatalf("verify passed with files %v wrong; %s", m, where())
			}
		}

		if err := e.Restart(nil); err != nil {
			t.Fatal(err)
		}
		resp := round("clean round", cleanRoundTimeout)
		if p, m := e.Pending(), e.Mismatched(); len(p) > 0 || len(m) > 0 {
			t.Fatalf("clean round left pending %v, mismatched %v (errors %v); %s", p, m, firstN(resp.Errors, 3), where())
		}
		if err := verify("clean verify", cleanVerifyBudget); err != nil {
			t.Fatalf("verify of a correct copy: %v; %s", err, where())
		}
		e.Close()
		closed = true
		if err := released(goroutines, fds, releaseWait); err != nil {
			t.Fatalf("%v; %s", err, where())
		}
	})
}

// Each input's budget, which with Start and Close stays near 8s at worst.
const (
	faultRounds       = 2
	faultRoundTimeout = 750 * time.Millisecond
	faultVerifyBudget = 500 * time.Millisecond
	cleanRoundTimeout = 2 * time.Second
	cleanVerifyBudget = time.Second
	releaseWait       = 500 * time.Millisecond
	// roundGrace is how long past its deadline a step may take to return:
	// injected waits that take no context run to the end of their sample,
	// at most SwarmOptions.MaxDuration's 20ms each.
	roundGrace = 500 * time.Millisecond
)

// within runs op and returns its result, failing t with every goroutine's
// stack if op has not returned within limit. Go's fuzzer stops an input
// after 10s without saying why, so each step reports its own overrun.
func within[T any](t *testing.T, what string, limit time.Duration, where func() string, op func() T) T {
	t.Helper()
	done := make(chan T, 1)
	go func() { done <- op() }()
	select {
	case v := <-done:
		return v
	case <-time.After(limit):
		buf := make([]byte, 1<<20)
		t.Fatalf("%s still running after %v; %s\n%s", what, limit, where(), buf[:runtime.Stack(buf, true)])
		panic("unreachable")
	}
}

// pbtSizes picks one of four file mixes: the calibration mix, small files
// with empty and odd sizes, a few files large enough to splice, or a random
// mix up to 8 MiB a file. Each mix stops at pbtMaxBytes in all, so a clean
// round over one connection, with zstd and encryption under -race, fits in
// cleanRoundTimeout.
func pbtSizes(seed uint64, mix uint8) []int64 {
	r := rand.New(rand.NewPCG(seed, 0x53495a45)) // "SIZE"
	var out []int64
	switch mix {
	case 0:
		out = sizes()
	case 1:
		for range 8 + r.IntN(24) {
			out = append(out, []int64{0, 1, 4095, 4096, 4097, 65535, 65536, 65537}[r.IntN(8)]+r.Int64N(2))
		}
	case 2:
		for range 1 + r.IntN(3) {
			out = append(out, 1<<20+r.Int64N(4<<20))
		}
	default:
		for range 1 + r.IntN(30) {
			out = append(out, int64(math.Exp(r.Float64()*math.Log(8<<20))))
		}
	}
	var total int64
	for i, size := range out {
		if total+size >= pbtMaxBytes {
			out[i] = pbtMaxBytes - total
			return out[:i+1]
		}
		total += size
	}
	return out
}

// pbtMaxBytes caps an input's bytes: under -race, one connection with zstd
// and encryption copies about 12 MiB/s.
const pbtMaxBytes = 6 << 20

// corruptDone returns the files the server marks done whose copies differ
// from the source.
func corruptDone(e *Env) []uint64 {
	mismatched := e.Mismatched()
	var out []uint64
	for _, id := range e.Done() {
		if slices.Contains(mismatched, id) {
			out = append(out, id)
		}
	}
	return out
}

// warmUpProcess makes one clean transfer in each compression mode, so what
// the process opens on first use and keeps, such as the runtime's network
// poller and the codec pools, is open before an input counts its baseline.
func warmUpProcess(tb testing.TB) {
	for _, comp := range []string{"none", "lz4", "zstd", "adapt"} {
		e, err := Start(Config{Sizes: []int64{4 << 10, 2 << 20}, Seed: 1, Comp: comp, KeepAlive: true})
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := e.Round(context.Background()); err != nil {
			tb.Fatal(err)
		}
		e.Close()
	}
}

// resources counts the process's goroutines and open descriptors.
func resources() (goroutines, fds int) {
	ents, _ := os.ReadDir("/proc/self/fd")
	return runtime.NumGoroutine(), len(ents)
}

// released waits up to wait for the goroutines and descriptors to fall
// back to the counts before, since Serve returns before its handlers do. It
// reports what stayed, with the goroutines' stacks.
func released(goroutines, fds int, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		g, d := resources()
		if g <= goroutines && d <= fds {
			return nil
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			buf = buf[:runtime.Stack(buf, true)]
			return fmt.Errorf("leaked %d goroutines and %d descriptors:\n%s", g-goroutines, d-fds, buf)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
