package faults

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
)

// The environment variables FromEnv reads. Each fault family has its own.
const (
	EnvSeed    = "TX_FAULTS_SEED"
	EnvProfile = "TX_FAULTS_PROFILE"
	EnvNet     = "TX_FAULTS_NET"
	EnvMsg     = "TX_FAULTS_MSG"
	EnvDisk    = "TX_FAULTS_DISK"
	EnvSys     = "TX_FAULTS_SYS"
)

var familyEnv = [familyCount]string{EnvNet, EnvMsg, EnvDisk, EnvSys}

var errDeadline = os.ErrDeadlineExceeded

// FromEnv builds an injector from the TX_FAULTS_* variables lookup reports.
// It returns nil, the disabled state, when none is set to a non-empty value
// or a family variable lists no entries, so a variable defined blank never
// turns injection on. TX_FAULTS_SEED alone enables the wrappers without
// injecting anything; without it, a random seed is chosen and reported by
// Repro.
func FromEnv(lookup func(string) (string, bool)) (*Injector, error) {
	lookup = nonEmpty(lookup)
	enabled := false
	cfg := Config{Seed: rand.Uint64(), Specs: map[Family]string{}}
	if raw, ok := lookup(EnvSeed); ok {
		enabled = true
		seed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvSeed, err)
		}
		cfg.Seed = seed
	}
	if raw, ok := lookup(EnvProfile); ok {
		enabled = true
		cfg.Profile = strings.TrimSpace(raw)
	}
	for f := range familyCount {
		raw, ok := lookup(familyEnv[f])
		if !ok {
			continue
		}
		entries, err := splitTopLevel(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", familyEnv[f], err)
		}
		// Only separators, such as ",", lists nothing and stays off.
		if len(entries) > 0 {
			enabled = true
			cfg.Specs[f] = raw
		}
	}
	if !enabled {
		return nil, nil
	}
	i, err := Build(cfg)
	if err != nil {
		return nil, err
	}
	return i, nil
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

// apply sets the rates and distributions one family's spec gives. In SYS, an
// entry naming every syscall ("*.fail") applies before entries naming one, so
// a specific entry wins whatever the order.
func (i *Injector) apply(f Family, spec string) error {
	entries, err := splitTopLevel(spec)
	if err != nil {
		return fmt.Errorf("%s: %w", familyEnv[f], err)
	}
	type rule struct {
		kinds []Kind
		rate  float64
		dist  Dist
	}
	var wild, exact []rule
	for _, entry := range entries {
		name, value, ok := strings.Cut(entry, "=")
		name = strings.TrimSpace(name)
		if !ok {
			return fmt.Errorf("%s: entry %q: want kind=rate[:dist]", familyEnv[f], entry)
		}
		ks, ok := lookupKinds(f, name)
		if !ok {
			return fmt.Errorf("%s: unknown %s fault kind %q", familyEnv[f], f, name)
		}
		rateText, distText, hasDist := strings.Cut(value, ":")
		rate, err := strconv.ParseFloat(strings.TrimSpace(rateText), 64)
		if err != nil || !(rate >= 0 && rate <= 1) {
			return fmt.Errorf("%s: %s: rate %q must be a probability from 0 to 1", familyEnv[f], name, rateText)
		}
		r := rule{kinds: ks, rate: rate}
		timed := kinds[ks[0]].timed
		switch {
		case timed && !hasDist && rate > 0:
			return fmt.Errorf("%s: %s takes a duration: %s=rate:β(min,avg,p99,max)", familyEnv[f], name, name)
		case !timed && hasDist:
			return fmt.Errorf("%s: %s takes no duration", familyEnv[f], name)
		case hasDist:
			if r.dist, err = ParseDist(distText); err != nil {
				return fmt.Errorf("%s: %s: %w", familyEnv[f], name, err)
			}
		}
		if strings.HasPrefix(name, "*.") {
			wild = append(wild, r)
		} else {
			exact = append(exact, r)
		}
	}
	for _, r := range append(wild, exact...) {
		for _, k := range r.kinds {
			i.rates[k], i.dists[k] = r.rate, r.dist
		}
	}
	return nil
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
