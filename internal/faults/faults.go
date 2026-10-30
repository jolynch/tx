// Package faults injects network, disk, and syscall faults so property tests
// can check that tx either finishes correctly or fails loudly when its IO
// misbehaves. A nil *Injector is the disabled state: every wrapper returns what
// it was given and every gate returns nil, so production paths pay one nil
// check per connection, file, or syscall site, never per read or write. See
// docs/arch/FAULTS.md.
package faults

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx/internal/events"
)

// Side says which end of a connection a wrapper sits on.
type Side uint8

const (
	Client Side = iota + 1
	Server
)

func (s Side) String() string {
	switch s {
	case Client:
		return "client"
	case Server:
		return "server"
	default:
		return "side(" + strconv.Itoa(int(s)) + ")"
	}
}

// Config selects what an Injector injects.
type Config struct {
	// Seed selects the fault schedule, so a run reproduces from it.
	Seed uint64
	// Profile names a set of fault specs, such as "bad-day", applied before
	// Specs.
	Profile string
	// Specs holds each family's fault spec, in the TX_FAULTS_<FAMILY>
	// syntax: comma-separated kind=rate[:dist] entries. An entry overrides
	// the profile's for the same kind.
	Specs map[Family]string
	// Events, when set, receives a "fault" event for every fault fired.
	Events *events.Sink
}

// Injector decides which faults fire and where. Create one with New or
// Build; a nil *Injector injects nothing.
type Injector struct {
	cfg   Config
	rates [kindCount]float64
	dists [kindCount]Dist
	// famOn reports whether any kind in a family has a non-zero rate.
	famOn [familyCount]bool
	// writeFaults reports whether a connection's writes can fault, which
	// costs half of the connections their zero-copy path.
	writeFaults bool

	// Operation counters for faults with no stable key of their own.
	conns    [3]atomic.Uint64 // per Side
	sysOps   [sysCount]atomic.Uint64
	loseOps  atomic.Uint64
	dupOps   atomic.Uint64
	delayOps atomic.Uint64
	accepts  atomic.Uint64
	busyOps  atomic.Uint64
	dials    atomic.Uint64
	attempts sync.Map // file key -> *atomic.Uint64
	hitMu    sync.Mutex
	hit      map[uint64]struct{} // files an undetected kind fired on

	counts struct {
		clientConns, serverConns atomic.Uint64
		opens, reads, writes     [siteCount]atomic.Uint64
		sys                      [sysCount]atomic.Uint64
		injected                 [kindCount]atomic.Uint64
	}
}

// New returns an injector for cfg, panicking if cfg's specs are invalid; use
// it for fixed configurations. An injector with no fault kinds still wraps
// every connection and file, which tests use to show the wrappers are
// transparent.
func New(cfg Config) *Injector {
	i, err := Build(cfg)
	if err != nil {
		panic(err)
	}
	return i
}

// Build returns an injector for cfg, or an error naming the first invalid
// spec entry.
func Build(cfg Config) (*Injector, error) {
	i := &Injector{cfg: cfg}
	if cfg.Profile != "" {
		prof, ok := profiles[cfg.Profile]
		if !ok {
			return nil, errors.New("unknown fault profile " + strconv.Quote(cfg.Profile))
		}
		for f, spec := range prof {
			if err := i.apply(f, spec); err != nil {
				return nil, errors.New("profile " + cfg.Profile + ": " + err.Error())
			}
		}
	}
	for f := range familyCount {
		if spec, ok := cfg.Specs[f]; ok {
			if err := i.apply(f, spec); err != nil {
				return nil, err
			}
		}
	}
	for k := range Kind(kindCount) {
		if i.rates[k] > 0 {
			i.famOn[k.Family()] = true
		}
	}
	for _, k := range []Kind{NetDelay, NetPause, NetReset, NetPartial} {
		i.writeFaults = i.writeFaults || i.rates[k] > 0
	}
	return i, nil
}

// Seed returns the seed the injector's schedule derives from.
func (i *Injector) Seed() uint64 {
	if i == nil {
		return 0
	}
	return i.cfg.Seed
}

// Repro returns the environment that reproduces this injector's schedule,
// in shell syntax.
func (i *Injector) Repro() string {
	if i == nil {
		return ""
	}
	parts := []string{EnvSeed + "=" + strconv.FormatUint(i.cfg.Seed, 10)}
	if i.cfg.Profile != "" {
		parts = append(parts, EnvProfile+"="+i.cfg.Profile)
	}
	for f := range familyCount {
		if spec, ok := i.cfg.Specs[f]; ok && strings.TrimSpace(spec) != "" {
			parts = append(parts, familyEnv[f]+"='"+spec+"'")
		}
	}
	return strings.Join(parts, " ")
}

// Counts is how many connections and files an injector has wrapped, by
// side and site, how many times each syscall gate has been asked, and how
// many times each fault kind has fired.
type Counts struct {
	ClientConns, ServerConns uint64
	Opens, Reads, Writes     [siteCount]uint64
	Sys                      [sysCount]uint64
	Injected                 [kindCount]uint64
}

// Counts returns the injector's counts so far; a nil injector has none.
func (i *Injector) Counts() Counts {
	var c Counts
	if i == nil {
		return c
	}
	c.ClientConns = i.counts.clientConns.Load()
	c.ServerConns = i.counts.serverConns.Load()
	for s := range c.Opens {
		c.Opens[s] = i.counts.opens[s].Load()
		c.Reads[s] = i.counts.reads[s].Load()
		c.Writes[s] = i.counts.writes[s].Load()
	}
	for s := range c.Sys {
		c.Sys[s] = i.counts.sys[s].Load()
	}
	for k := range c.Injected {
		c.Injected[k] = i.counts.injected[k].Load()
	}
	return c
}

// InjectedByKind returns the kinds that fired, by name, with their counts.
func (c Counts) InjectedByKind() map[string]uint64 {
	out := map[string]uint64{}
	for k, n := range c.Injected {
		if n > 0 {
			out[Kind(k).String()] = n
		}
	}
	return out
}

// Kinds returns the names of the kinds with a non-zero rate, sorted.
func (i *Injector) Kinds() []string {
	if i == nil {
		return nil
	}
	var out []string
	for k := range Kind(kindCount) {
		if i.rates[k] > 0 {
			out = append(out, k.String())
		}
	}
	sort.Strings(out)
	return out
}

// fires reports whether kind k fires for operation op of the entity key.
// The decision depends only on the seed, k, key, and op, so a schedule
// reproduces wherever its keys do.
func (i *Injector) fires(k Kind, key, op uint64) bool {
	return i.decide(k, key, op, nil)
}

// diskAt is the file and byte offset a disk fault fires at, reported in its
// event.
type diskAt struct {
	file   uint64
	offset int64
}

// firesAt is fires for a disk fault, whose event also names the file and
// offset.
func (i *Injector) firesAt(k Kind, key, op uint64, file uint64, offset int64) bool {
	return i.decide(k, key, op, &diskAt{file: file, offset: offset})
}

func (i *Injector) decide(k Kind, key, op uint64, at *diskAt) bool {
	rate := i.rates[k]
	if rate <= 0 {
		return false
	}
	if float64(mix(i.cfg.Seed, uint64(k), key, op)>>11)/(1<<53) >= rate {
		return false
	}
	i.counts.injected[k].Add(1)
	if i.cfg.Events.Enabled() {
		fields := []events.Field{events.F("kind", k.String()), events.F("key", key), events.F("op", op)}
		if at != nil {
			fields = append(fields, events.F("file", at.file), events.F("offset", at.offset))
		}
		i.cfg.Events.Emit("fault", "", fields...)
	}
	return true
}

// rng returns the random source for a fault that fired, seeded like its
// decision so its details reproduce too.
func (i *Injector) rng(k Kind, key, op uint64) *rand.Rand {
	h := mix(i.cfg.Seed^0x5bd1e9955bd1e995, uint64(k), key, op)
	return rand.New(rand.NewPCG(h, h^0x9e3779b97f4a7c15))
}

// sample draws kind k's duration for an operation.
func (i *Injector) sample(k Kind, key, op uint64) time.Duration {
	return i.dists[k].Sample(i.rng(k, key, op))
}

// attempt returns how many times key was used before, so a fault on one
// attempt at a file is not repeated on every retry of it.
func (i *Injector) attempt(key uint64) uint64 {
	v, ok := i.attempts.Load(key)
	if !ok {
		v, _ = i.attempts.LoadOrStore(key, new(atomic.Uint64))
	}
	return v.(*atomic.Uint64).Add(1) - 1
}

// mix hashes its words with splitmix64 steps.
func mix(words ...uint64) uint64 {
	var h uint64
	for _, w := range words {
		h ^= w
		h += 0x9e3779b97f4a7c15
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 27)) * 0x94d049bb133111eb
		h ^= h >> 31
	}
	return h
}

var noDeadline time.Time

var errClosed = fmt.Errorf("faults: connection closed during an injected pause: %w", net.ErrClosed)

// sleep waits d, returning early with an error when ctx ends, deadline
// passes, or closed is closed. A zero deadline and nil ctx or closed are
// ignored.
func sleep(ctx context.Context, d time.Duration, deadline time.Time, closed <-chan struct{}) error {
	if d <= 0 {
		return nil
	}
	expired := false
	if !deadline.IsZero() {
		if until := time.Until(deadline); until < d {
			d, expired = max(until, 0), true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	select {
	case <-t.C:
		if expired {
			return errDeadline
		}
		return nil
	case <-done:
		return ctx.Err()
	case <-closed:
		return errClosed
	}
}

type ctxKey struct{}

// NewContext returns ctx carrying i. A nil injector leaves ctx unchanged.
func NewContext(ctx context.Context, i *Injector) context.Context {
	if i == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, i)
}

// FromContext returns the injector ctx carries, or nil.
func FromContext(ctx context.Context) *Injector {
	if ctx == nil {
		return nil
	}
	i, _ := ctx.Value(ctxKey{}).(*Injector)
	return i
}
