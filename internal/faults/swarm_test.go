package faults

import (
	"bytes"
	"context"
	"maps"
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

// TestUndetected checks Undetected is true exactly for injectors enabling a
// kind no layer detects without verification, and that UndetectedKinds names
// real kinds.
func TestUndetected(t *testing.T) {
	want := []string{"disk.read.flip", "disk.write.lost", "disk.write.flip"}
	if !slices.Equal(UndetectedKinds(), want) {
		t.Fatalf("UndetectedKinds = %v, want %v", UndetectedKinds(), want)
	}
	for _, k := range UndetectedKinds() {
		name, ok := strings.CutPrefix(k, "disk.")
		if !ok {
			t.Fatalf("%s is not a DISK kind", k)
		}
		inj := one(t, FamilyDisk, name+"=1")
		if !slices.Contains(inj.Kinds(), k) {
			t.Errorf("%s is not a kind: Kinds() = %v", k, inj.Kinds())
		}
		if !inj.Undetected() {
			t.Errorf("injector enabling %s: Undetected() = false", k)
		}
	}
	var nilInj *Injector
	for name, inj := range map[string]*Injector{
		"nil":       nilInj,
		"no faults": New(Config{Seed: 1}),
		"bad-day":   New(Config{Seed: 1, Profile: "bad-day"}),
		"benign":    one(t, FamilyDisk, "read.eio=1, write.short=1"),
	} {
		if inj.Undetected() {
			t.Errorf("%s injector: Undetected() = true", name)
		}
	}
}

// TestUndetectedFiles checks the injector records the files an undetected
// kind fired on, and only at the sites that change a downloaded copy.
func TestUndetectedFiles(t *testing.T) {
	data := make([]byte, 3*writeBlock)
	read := func(spec string, site Site, file uint64) *Injector {
		inj := one(t, FamilyDisk, spec)
		r := inj.WrapReaderAt(context.Background(), site, file, bytes.NewReader(data))
		if _, err := r.ReadAt(make([]byte, 1024), 0); err != nil {
			t.Fatal(err)
		}
		return inj
	}
	write := func(spec string, site Site, file uint64) *Injector {
		inj := one(t, FamilyDisk, spec)
		w := inj.WrapWriter(site, file, 0, nopWriteCloser{&bytes.Buffer{}})
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		return inj
	}
	if got := read("read.flip=1", SiteSendRead, 7).UndetectedFiles(); !slices.Equal(got, []uint64{7}) {
		t.Errorf("read.flip on a SEND read recorded %v, want [7]", got)
	}
	for _, site := range []Site{SiteCXSUMRead, SiteVerifyRead} {
		if got := read("read.flip=1", site, 7).UndetectedFiles(); got != nil {
			t.Errorf("read.flip at site %d recorded %v, want none", site, got)
		}
	}
	if got := read("read.eio=0", SiteSendRead, 7).UndetectedFiles(); got != nil {
		t.Errorf("a read with no flips recorded %v", got)
	}
	if got := write("write.flip=1", SiteOutputWrite, 3).UndetectedFiles(); !slices.Equal(got, []uint64{3}) {
		t.Errorf("write.flip recorded %v, want [3]", got)
	}
	// A writer that cannot seek gets no lost blocks, so records none.
	lost := one(t, FamilyDisk, "write.lost=1")
	if _, err := lost.WrapWriter(SiteOutputWrite, 4, 0, nopWriteCloser{&bytes.Buffer{}}).Write(data); err != nil || lost.UndetectedFiles() != nil {
		t.Errorf("write.lost on a writer that cannot seek recorded %v, %v", lost.UndetectedFiles(), err)
	}
	if got := write("write.torn=0, write.short=0", SiteOutputWrite, 3).UndetectedFiles(); got != nil {
		t.Errorf("no undetected kind recorded %v", got)
	}
	var nilInj *Injector
	if nilInj.UndetectedFiles() != nil {
		t.Error("a nil injector recorded files")
	}
	// Several files come back sorted, once each.
	inj := one(t, FamilyDisk, "write.flip=1")
	for _, id := range []uint64{9, 2, 9, 5} {
		if _, err := inj.WrapWriter(SiteOutputWrite, id, 0, nopWriteCloser{&bytes.Buffer{}}).Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if got := inj.UndetectedFiles(); !slices.Equal(got, []uint64{2, 5, 9}) {
		t.Errorf("recorded %v, want [2 5 9]", got)
	}
}

// TestSwarmUndetectedGate checks the group coin that gates the undetected
// kinds leaves every other kind's draw as it was: a swarm minus those kinds
// equals the same swarm with them excluded, and the coin comes up both ways.
func TestSwarmUndetectedGate(t *testing.T) {
	strip := func(cfg Config) map[Family]string {
		out := map[Family]string{}
		for f, spec := range cfg.Specs {
			var keep []string
			for _, e := range strings.Split(spec, ", ") {
				name, _, _ := strings.Cut(e, "=")
				if !slices.Contains(UndetectedKinds(), strings.ToLower(f.String())+"."+name) {
					keep = append(keep, e)
				}
			}
			if len(keep) > 0 {
				out[f] = strings.Join(keep, ", ")
			}
		}
		return out
	}
	opts := SwarmOptions{MaxRate: 0.05, MaxDuration: 10 * time.Millisecond}
	without := opts
	without.Exclude = UndetectedKinds()
	on, off := 0, 0
	for seed := range uint64(300) {
		cfg := Swarm(seed, opts)
		if !maps.Equal(strip(cfg), strip(Swarm(seed, without))) {
			t.Fatalf("seed %d: the gate changed other kinds: %v vs %v", seed, cfg.Specs, Swarm(seed, without).Specs)
		}
		if New(cfg).Undetected() {
			on++
		} else {
			off++
		}
	}
	// Each kind is on a quarter of the time, so about 58% of swarms have one.
	if on < 100 || off < 60 {
		t.Fatalf("%d of 300 swarms enabled an undetected kind", on)
	}
}
