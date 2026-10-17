package faults

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
)

// The environment variables FromEnv reads. Each fault family has its own.
const (
	EnvSeed = "TX_FAULTS_SEED"
	EnvNet  = "TX_FAULTS_NET"
	EnvMsg  = "TX_FAULTS_MSG"
	EnvDisk = "TX_FAULTS_DISK"
	EnvSys  = "TX_FAULTS_SYS"
)

// familyEnv lists each family's variable and the fault kinds it accepts.
var familyEnv = []struct {
	name  string
	env   string
	kinds map[string]bool
}{
	{name: "NET", env: EnvNet},
	{name: "MSG", env: EnvMsg},
	{name: "DISK", env: EnvDisk},
	{name: "SYS", env: EnvSys},
}

// FromEnv builds an injector from the TX_FAULTS_* variables lookup reports.
// It returns nil, the disabled state, when none is set to a non-empty value
// or a family variable lists no entries, so a variable defined blank never
// turns injection on. TX_FAULTS_SEED alone
// enables the wrappers without injecting anything; without it, a random seed
// is chosen and reported by Repro.
func FromEnv(lookup func(string) (string, bool)) (*Injector, error) {
	lookup = nonEmpty(lookup)
	enabled := false
	cfg := Config{Seed: rand.Uint64()}
	if raw, ok := lookup(EnvSeed); ok {
		enabled = true
		seed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvSeed, err)
		}
		cfg.Seed = seed
	}
	for _, fam := range familyEnv {
		raw, ok := lookup(fam.env)
		if !ok {
			continue
		}
		entries, err := splitTopLevel(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fam.env, err)
		}
		// Only separators, such as ",", lists nothing and stays off.
		enabled = enabled || len(entries) > 0
		for _, entry := range entries {
			kind, _, _ := strings.Cut(entry, "=")
			kind = strings.TrimSpace(kind)
			if !fam.kinds[kind] {
				return nil, fmt.Errorf("%s: unknown %s fault kind %q", fam.env, fam.name, kind)
			}
		}
	}
	if !enabled {
		return nil, nil
	}
	return New(cfg), nil
}

// nonEmpty treats a variable set to blank space as unset.
func nonEmpty(lookup func(string) (string, bool)) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return "", false
		}
		return v, true
	}
}

// splitTopLevel splits a comma-separated list, skipping commas inside
// parentheses so an entry such as pause=0.01:β(1ms,5ms,40ms,50ms) stays whole.
// Empty entries are dropped.
func splitTopLevel(s string) ([]string, error) {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced ')' at byte %d", i)
			}
		case ',':
			if depth == 0 {
				if e := strings.TrimSpace(s[start:i]); e != "" {
					out = append(out, e)
				}
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced '('")
	}
	if e := strings.TrimSpace(s[start:]); e != "" {
		out = append(out, e)
	}
	return out, nil
}
