package faults

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/stats"
)

// SwarmOptions bounds what Swarm draws.
type SwarmOptions struct {
	// Families limits the draw to these families; nil draws from all.
	Families []Family
	// Exclude names kinds never drawn, as Kind.String spells them, such as
	// "disk.read.flip".
	Exclude []string
	// MaxRate caps each kind's rate; zero means 0.02.
	MaxRate float64
	// MaxDuration caps the maximum of every distribution drawn; zero means
	// 20ms.
	MaxDuration time.Duration
	// Benign limits the draw to kinds Injector.Benign accepts: latencies
	// and failures of advisory syscalls. The failures, which happen a few
	// times per connection or file, draw rates up to benignMaxRate instead
	// of MaxRate, so a short transfer still meets them.
	Benign bool
}

// Swarm returns a configuration that turns on a random subset of the fault
// kinds, each at a random rate, as FoundationDB's swarm testing does: no
// one tuning of the faults then decides what a property test can reach.
// Each kind is on with probability one half. Rates are log-uniform from
// MaxRate/100 to MaxRate, and each timed kind draws its own beta
// distribution up to MaxDuration. Everything derives from seed, which also
// seeds the schedule, and the draws are written as family specs, so the
// injector's Repro line reproduces the run without the seed's draw.
func Swarm(seed uint64, opts SwarmOptions) Config {
	if opts.MaxRate <= 0 {
		opts.MaxRate = 0.02
	}
	if opts.MaxDuration <= 0 {
		opts.MaxDuration = 20 * time.Millisecond
	}
	r := rand.New(rand.NewPCG(seed, 0x5357524d)) // "SWRM"
	entries := make([][]string, familyCount)
	for k := range Kind(kindCount) {
		info := kinds[k]
		// The table holds a short kind for every syscall, but only those
		// returning a byte count take one.
		if _, ok := lookupKinds(info.family, info.name); !ok {
			continue
		}
		// Draw for every kind, on or off, so excluding one kind or family
		// leaves the others' draws as they were.
		on := r.IntN(2) == 0
		maxRate := opts.MaxRate
		if opts.Benign && !info.timed {
			maxRate = benignMaxRate
		}
		rate := maxRate * math.Pow(100, -r.Float64())
		dist := ""
		if info.timed {
			dist = ":" + swarmDist(r, opts.MaxDuration)
		}
		excluded := (opts.Families != nil && !slices.Contains(opts.Families, info.family)) || slices.Contains(opts.Exclude, k.String()) ||
			(opts.Benign && !benignKind(k))
		if on && !excluded {
			entries[info.family] = append(entries[info.family], info.name+"="+strconv.FormatFloat(rate, 'g', 3, 64)+dist)
		}
	}
	cfg := Config{Seed: seed, Specs: map[Family]string{}}
	for f, e := range entries {
		if len(e) > 0 {
			cfg.Specs[Family(f)] = strings.Join(e, ", ")
		}
	}
	return cfg
}

// benignMaxRate caps a benign swarm's advisory syscall failures.
const benignMaxRate = 0.5

// swarmDist draws a beta distribution with a maximum up to maxDur and
// writes it as β(min,avg,p99,max). Its statistics come from a real beta
// shape, so they always fit.
func swarmDist(r *rand.Rand, maxDur time.Duration) string {
	for range 8 {
		hi := float64(maxDur) * math.Pow(1000, -r.Float64()) // down to maxDur/1000
		lo := hi * 0.2 * r.Float64()
		b, err := stats.NewBeta(lo, hi, 0.3*math.Pow(15, r.Float64()), math.Pow(50, r.Float64()))
		if err != nil {
			continue
		}
		vals := []time.Duration{time.Duration(lo), time.Duration(b.Mean()), time.Duration(b.Quantile(0.99)), time.Duration(hi)}
		var parts []string
		for _, v := range vals {
			parts = append(parts, v.String())
		}
		s := "β(" + strings.Join(parts, ",") + ")"
		if _, err := ParseDist(s); err == nil {
			return s
		}
	}
	return "io2"
}

// The pbt-faults property tests (make pbt-faults) read these variables.
const (
	// EnvPBT turns the tests on. They are off otherwise, because they are
	// expected to fail until the bugs they find are fixed.
	EnvPBT = "TX_PBT_FAULTS"
	// EnvPBTExclude lists kinds, comma-separated as Kind.String spells
	// them, that the tests' swarms never draw, so a run can look past a
	// failure already recorded.
	EnvPBTExclude = "TX_PBT_FAULTS_EXCLUDE"
)

// SkipUnlessPBT skips tb unless TX_PBT_FAULTS is set.
func SkipUnlessPBT(tb interface{ Skip(args ...any) }) {
	if strings.TrimSpace(os.Getenv(EnvPBT)) == "" {
		tb.Skip("pbt-faults property: set " + EnvPBT + "=1 or run make pbt-faults")
	}
}

// PBTExclude returns the kinds TX_PBT_FAULTS_EXCLUDE lists, or an error
// naming one that is no kind, which would otherwise exclude nothing.
func PBTExclude() ([]string, error) {
	var out []string
	for _, name := range strings.Split(os.Getenv(EnvPBTExclude), ",") {
		if name = strings.TrimSpace(name); name == "" {
			continue
		}
		known := false
		for k := range Kind(kindCount) {
			if kinds[k].name != "" && k.String() == name {
				_, known = lookupKinds(kinds[k].family, kinds[k].name)
			}
		}
		if !known {
			return nil, fmt.Errorf("%s: unknown fault kind %q; name kinds as Kind.String does, such as disk.write.lost", EnvPBTExclude, name)
		}
		out = append(out, name)
	}
	return out, nil
}

// Without returns cfg with the named kinds, spelled as Kind.String spells
// them, turned off: each becomes a kind=0 entry in its family's spec, which
// overrides the profile and any wildcard. A name that is no kind is left
// for Build to report.
func Without(cfg Config, names []string) Config {
	if len(names) == 0 {
		return cfg
	}
	specs := maps.Clone(cfg.Specs)
	if specs == nil {
		specs = map[Family]string{}
	}
	for _, full := range names {
		fam, name, _ := strings.Cut(full, ".")
		f := slices.IndexFunc(familyNames[:], func(n string) bool { return strings.EqualFold(n, fam) })
		if f < 0 {
			f = int(FamilyNet) // unknown family: Build rejects the entry
			name = full
		}
		if specs[Family(f)] != "" {
			specs[Family(f)] += ", "
		}
		specs[Family(f)] += name + "=0"
	}
	cfg.Specs = specs
	return cfg
}

// PBTInjector returns the injector a pbt-faults input runs under: the one
// the TX_FAULTS_* variables describe when any is set, so a failure's repro
// line reruns its faults, and otherwise one built from cfg, the input's own
// swarm or profile.
func PBTInjector(cfg Config) (*Injector, error) {
	if i, err := FromEnv(os.LookupEnv); err != nil || i != nil {
		return i, err
	}
	return Build(cfg)
}

// advisorySyscalls are the syscalls whose failure tx skips: hints and
// socket tuning that change only speed.
var advisorySyscalls = [sysCount]bool{SysFadvise: true, SysReadahead: true, SysMmap: true, SysMincore: true, SysSetsockopt: true}

// Benign reports whether every kind i turns on only delays IO or fails an
// advisory syscall (fadvise, readahead, mmap, mincore, setsockopt), so a
// transfer under i must succeed as it would without faults, only slower.
func (i *Injector) Benign() bool {
	if i == nil {
		return true
	}
	for k := range Kind(kindCount) {
		if i.rates[k] > 0 && !benignKind(k) {
			return false
		}
	}
	return true
}

// benignKind reports whether k only delays IO or fails an advisory syscall.
func benignKind(k Kind) bool {
	if kinds[k].timed {
		return true
	}
	if k < sysKindBase {
		return false
	}
	sc, action := Syscall(int(k-sysKindBase)/sysActions), int(k-sysKindBase)%sysActions
	return action == sysFail && advisorySyscalls[sc]
}

// PBTFaultSet selects the faults a pbt-faults input runs under.
type PBTFaultSet uint8

const (
	// PBTSwarm is a swarm of every kind, as is any value without its own
	// constant.
	PBTSwarm PBTFaultSet = iota
	// PBTBadDay is the bad-day profile.
	PBTBadDay
	// PBTBenign is a swarm of benign kinds only.
	PBTBenign
)

// PBTConfig returns the configuration set gives seed, leaving out the
// kinds TX_PBT_FAULTS_EXCLUDE lists.
func PBTConfig(seed uint64, set PBTFaultSet) (Config, error) {
	exclude, err := PBTExclude()
	if err != nil {
		return Config{}, err
	}
	switch set {
	case PBTBadDay:
		return Without(Config{Seed: seed, Profile: "bad-day"}, exclude), nil
	case PBTBenign:
		return Swarm(seed, SwarmOptions{Exclude: exclude, Benign: true}), nil
	default:
		return Swarm(seed, SwarmOptions{Exclude: exclude}), nil
	}
}
