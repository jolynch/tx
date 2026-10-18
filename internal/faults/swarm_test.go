package faults

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// Swarm draws and the pbt-faults helpers.

// TestSwarm checks swarm draws build, reproduce from their seed and from
// their repro line, respect their bounds and exclusions, and between them
// reach every kind.
func TestSwarm(t *testing.T) {
	opts := SwarmOptions{MaxRate: 0.05, MaxDuration: 10 * time.Millisecond}
	excluded := SwarmOptions{MaxRate: 0.05, MaxDuration: 10 * time.Millisecond, Exclude: []string{"disk.read.flip", "net.reset"}}
	drawn := map[Kind]bool{}
	for seed := range uint64(100) {
		cfg := Swarm(seed, opts)
		if !reflect.DeepEqual(cfg, Swarm(seed, opts)) {
			t.Fatalf("seed %d drew two configurations", seed)
		}
		i, err := Build(cfg)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		env := map[string]string{}
		for _, kv := range splitShell(i.Repro()) {
			k, v, _ := strings.Cut(kv, "=")
			env[k] = strings.Trim(v, "'")
		}
		again, err := FromEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
		if err != nil || again.rates != i.rates {
			t.Fatalf("seed %d: repro %s rebuilt %v", seed, i.Repro(), err)
		}
		without := New(Swarm(seed, excluded))
		for k := range Kind(kindCount) {
			rate := i.rates[k]
			if rate > opts.MaxRate {
				t.Fatalf("seed %d: %s at rate %g", seed, k, rate)
			}
			if rate > 0 {
				drawn[k] = true
			}
			if kinds[k].timed && rate > 0 && i.dists[k].String() != "io2" && i.dists[k].Beta().Max > opts.MaxDuration.Seconds()+1e-9 {
				t.Fatalf("seed %d: %s lasts up to %gs", seed, k, i.dists[k].Beta().Max)
			}
			// Excluding kinds leaves every other kind's draw alone.
			switch {
			case slices.Contains(excluded.Exclude, k.String()):
				if without.rates[k] != 0 {
					t.Fatalf("seed %d: excluded %s drawn", seed, k)
				}
			case without.rates[k] != rate:
				t.Fatalf("seed %d: excluding kinds moved %s from %g to %g", seed, k, rate, without.rates[k])
			}
		}
		if net := New(Swarm(seed, SwarmOptions{Families: []Family{FamilyNet}})); net.famOn[FamilyDisk] || net.famOn[FamilySys] || net.famOn[FamilyMsg] {
			t.Fatalf("seed %d: a NET-only swarm turned on %v", seed, net.Kinds())
		}
	}
	for k := range Kind(kindCount) {
		if _, ok := lookupKinds(kinds[k].family, kinds[k].name); ok && kinds[k].name != "" && !drawn[k] {
			t.Errorf("no seed drew %s", k)
		}
	}
}

// TestBenign checks Benign accepts latencies and advisory syscall failures
// and nothing else.
func TestBenign(t *testing.T) {
	for spec, want := range map[[2]string]bool{
		{"SYS", "fadvise.fail=1, mmap.fail=1, *.latency=0.1:io2"}:  true,
		{"NET", "delay=0.1:io2, pause=0.1:io2, dial.slow=0.1:io2"}: true,
		{"SYS", "fdatasync.fail=0.1"}:                              false,
		{"SYS", "splice.short=0.1"}:                                false,
		{"NET", "reset=0.001"}:                                     false,
		{"DISK", "read.latency=1:gp3, write.lost=0.01"}:            false,
	} {
		f := Family(slices.Index(familyNames[:], spec[0]))
		if got := one(t, f, spec[1]).Benign(); got != want {
			t.Errorf("%s %q: Benign %v, want %v", spec[0], spec[1], got, want)
		}
	}
	if !(*Injector)(nil).Benign() {
		t.Error("a nil injector is not benign")
	}
	// A benign swarm draws only benign kinds, and some of them.
	for seed := range uint64(20) {
		if i := New(Swarm(seed, SwarmOptions{Benign: true})); !i.Benign() || len(i.Kinds()) == 0 {
			t.Fatalf("benign swarm %d drew %v", seed, i.Kinds())
		}
	}
}

// TestPBTExclude checks a misspelled kind is an error rather than an
// exclusion that leaves every kind in.
func TestPBTExclude(t *testing.T) {
	t.Setenv(EnvPBTExclude, "disk.write.lost, sys.fdatasync.fail")
	if got, err := PBTExclude(); err != nil || !slices.Equal(got, []string{"disk.write.lost", "sys.fdatasync.fail"}) {
		t.Fatalf("PBTExclude = %v, %v", got, err)
	}
	for _, bad := range []string{"disk.write.lots", "sys.fdatasync.short"} {
		t.Setenv(EnvPBTExclude, bad)
		if _, err := PBTExclude(); err == nil {
			t.Fatalf("accepted %q, which is no kind", bad)
		}
	}
}

// TestWithout checks Without turns kinds off over a profile and its own
// family specs, and leaves an unknown name for Build to reject.
func TestWithout(t *testing.T) {
	cfg := Without(Config{Profile: "bad-day", Specs: map[Family]string{FamilySys: "*.fail=0.5"}}, []string{"net.reset", "sys.fdatasync.fail", "disk.read.latency"})
	i := New(cfg)
	if i.rates[NetReset] != 0 || i.rates[syscallKind(SysFdatasync, sysFail)] != 0 || i.rates[DiskReadLatency] != 0 {
		t.Fatalf("Without left kinds on: %v", i.Kinds())
	}
	if i.rates[NetPartial] == 0 || i.rates[syscallKind(SysFstat, sysFail)] != 0.5 {
		t.Fatalf("Without turned off other kinds: %v", i.Kinds())
	}
	if _, err := Build(Without(Config{}, []string{"net.nonsense"})); err == nil {
		t.Fatal("Without hid an unknown kind")
	}
}
