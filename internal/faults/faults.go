// Package faults injects network, disk, and syscall faults so property tests
// can check that tx either finishes correctly or fails loudly when its IO
// misbehaves. A nil *Injector is the disabled state: every wrapper returns what
// it was given and every gate returns nil, so production paths pay one nil
// check per connection, file, or syscall site, never per read or write. See
// docs/arch/FAULTS.md.
package faults

import (
	"context"
	"strconv"
	"sync/atomic"
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
}

// Injector decides which faults fire and where. Create one with New; a nil
// *Injector injects nothing.
type Injector struct {
	cfg    Config
	counts struct {
		clientConns, serverConns atomic.Uint64
		opens, reads, writes     [siteCount]atomic.Uint64
		sys                      [sysCount]atomic.Uint64
	}
}

// Counts is how many connections and files an injector has wrapped, by
// side and site, and how many times each syscall gate has been asked.
type Counts struct {
	ClientConns, ServerConns uint64
	Opens, Reads, Writes     [siteCount]uint64
	Sys                      [sysCount]uint64
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
	return c
}

// New returns an injector for cfg. An injector with no fault kinds enabled
// still wraps every connection and file, which tests use to show the
// wrappers are transparent.
func New(cfg Config) *Injector {
	return &Injector{cfg: cfg}
}

// Seed returns the seed the injector's schedule derives from.
func (i *Injector) Seed() uint64 {
	if i == nil {
		return 0
	}
	return i.cfg.Seed
}

// Repro returns the environment that reproduces this injector's schedule.
func (i *Injector) Repro() string {
	if i == nil {
		return ""
	}
	return EnvSeed + "=" + strconv.FormatUint(i.cfg.Seed, 10)
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
