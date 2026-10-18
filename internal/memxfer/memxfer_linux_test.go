//go:build linux

package memxfer

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
func quiet(t *testing.T) {
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
}
