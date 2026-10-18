package faults

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/stats"
)

// Dist is a beta distribution of durations, as a fault spec writes it.
type Dist struct {
	text string
	b    stats.Beta // in seconds
}

func (d Dist) String() string { return d.text }

// Sample draws one duration using r.
func (d Dist) Sample(r *rand.Rand) time.Duration {
	return time.Duration(d.b.Sample(r) * float64(time.Second))
}

// Beta returns the distribution, in seconds.
func (d Dist) Beta() stats.Beta { return d.b }

// presets are drive read-latency profiles: the Netflix
// service-capacity-modeling fits for gp3 and i4i (Netflix-Skunkworks/
// service-capacity-modeling@c56c0dd7861a, stats.py _beta_dist_from_interval;
// shapes/aws/{manual_drives,auto_i4i}.json), and a Beta(2,8) io2 model.
var presets = map[string]stats.Beta{
	"io2":   {Min: 100e-6, Max: 1500e-6, Alpha: 2, Beta: 8},
	"gp3":   {Min: 400e-6, Max: 10000e-6, Alpha: 13.904287927818451, Beta: 191.45134915996172},
	"i4i":   {Min: 50e-6, Max: 2000e-6, Alpha: 17.546493426147688, Beta: 438.6623356536922},
	"ephem": {Min: 50e-6, Max: 2000e-6, Alpha: 17.546493426147688, Beta: 438.6623356536922},
}

// Preset returns a named drive latency profile: io2, gp3, i4i, or ephem.
func Preset(name string) (Dist, bool) {
	b, ok := presets[name]
	return Dist{text: name, b: b}, ok
}

// ParseDist parses a duration distribution: a preset name, or
// β(min,avg,p99,max) with values in increasing order. A key replaces the
// meaning of either middle slot: avg= (or mean=, μ=) for an average, pNN=
// for a percentile. β(min,α=A,β=B,max) gives the shape directly. Β(, beta(,
// and Beta( spell β( too, and alpha= and beta= spell α= and β=.
func ParseDist(s string) (Dist, error) {
	s = strings.TrimSpace(s)
	if d, ok := Preset(s); ok {
		return d, nil
	}
	args, ok := betaArgs(s)
	if !ok {
		return Dist{}, fmt.Errorf("distribution %q: want β(min,avg,p99,max) or one of io2, gp3, i4i, ephem", s)
	}
	b, err := parseBeta(args)
	if err != nil {
		return Dist{}, fmt.Errorf("distribution %q: %w", s, err)
	}
	return Dist{text: s, b: b}, nil
}

func betaArgs(s string) ([]string, bool) {
	for _, prefix := range []string{"β(", "Β(", "beta(", "Beta("} {
		if rest, ok := strings.CutPrefix(s, prefix); ok {
			inner, ok := strings.CutSuffix(rest, ")")
			if !ok {
				return nil, false
			}
			args := strings.Split(inner, ",")
			for i := range args {
				args[i] = strings.TrimSpace(args[i])
			}
			return args, true
		}
	}
	return nil, false
}

// statistic is one middle slot of β(...): an average, a percentile, or a
// shape parameter.
type statistic struct {
	kind  string  // "avg", "p", "alpha", or "beta"
	p     float64 // the percentile's probability
	value float64 // seconds, or the shape parameter
}

func parseBeta(args []string) (stats.Beta, error) {
	if len(args) != 4 {
		return stats.Beta{}, fmt.Errorf("want 4 values, got %d", len(args))
	}
	min, err := seconds(args[0])
	if err != nil {
		return stats.Beta{}, fmt.Errorf("min: %w", err)
	}
	max, err := seconds(args[3])
	if err != nil {
		return stats.Beta{}, fmt.Errorf("max: %w", err)
	}
	if !(min < max) {
		return stats.Beta{}, errors.New("min must be less than max")
	}
	var mid [2]statistic
	for i, defaultKind := range []string{"avg", "p"} {
		st, err := parseStatistic(args[1+i], defaultKind)
		if err != nil {
			return stats.Beta{}, err
		}
		mid[i] = st
	}
	switch {
	case mid[0].kind == "alpha" && mid[1].kind == "beta":
		return stats.NewBeta(min, max, mid[0].value, mid[1].value)
	case mid[0].kind == "alpha" || mid[0].kind == "beta" || mid[1].kind == "alpha" || mid[1].kind == "beta":
		return stats.Beta{}, errors.New("a shape takes α= in the second slot and β= in the third")
	}
	if !(min < mid[0].value && mid[0].value < mid[1].value && mid[1].value < max) {
		return stats.Beta{}, errors.New("values must increase: min, then the two statistics, then max")
	}
	switch {
	case mid[0].kind == "avg" && mid[1].kind == "avg":
		return stats.Beta{}, errors.New("two averages")
	case mid[0].kind == "avg":
		return stats.FitMeanQuantile(min, max, mid[0].value, mid[1].p, mid[1].value)
	case mid[1].kind == "avg":
		return stats.FitMeanQuantile(min, max, mid[1].value, mid[0].p, mid[0].value)
	case mid[0].p == mid[1].p:
		return stats.Beta{}, fmt.Errorf("p%g given twice", 100*mid[0].p)
	default:
		return stats.FitQuantiles(min, max, mid[0].p, mid[0].value, mid[1].p, mid[1].value)
	}
}

// parseStatistic parses one middle slot. A bare duration takes defaultKind:
// the second slot is an average and the third a p99.
func parseStatistic(arg, defaultKind string) (statistic, error) {
	key, val, keyed := strings.Cut(arg, "=")
	if !keyed {
		v, err := seconds(arg)
		if err != nil {
			return statistic{}, err
		}
		if defaultKind == "p" {
			return statistic{kind: "p", p: 0.99, value: v}, nil
		}
		return statistic{kind: "avg", value: v}, nil
	}
	switch key = strings.TrimSpace(key); key {
	case "avg", "mean", "μ", "µ": // Greek mu or the micro sign
		v, err := seconds(val)
		return statistic{kind: "avg", value: v}, err
	case "α", "alpha", "β", "beta":
		v, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil {
			return statistic{}, fmt.Errorf("%s: %w", key, err)
		}
		kind := "alpha"
		if key == "β" || key == "beta" {
			kind = "beta"
		}
		return statistic{kind: kind, value: v}, nil
	}
	if pct, ok := strings.CutPrefix(key, "p"); ok {
		p, err := strconv.ParseFloat(pct, 64)
		if err != nil || !(0 < p && p < 100) {
			return statistic{}, fmt.Errorf("percentile %q must be p followed by a number in (0, 100)", key)
		}
		v, err := seconds(val)
		return statistic{kind: "p", p: p / 100, value: v}, err
	}
	return statistic{}, fmt.Errorf("unknown statistic %q: want avg=, mean=, μ=, or pNN=", key)
}

func seconds(s string) (float64, error) {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("negative duration %s", s)
	}
	return d.Seconds(), nil
}
