// Package stats holds the beta distribution tx uses for every injected
// duration. It is a trimmed copy of the stats package in the code for the
// book "Rigor, Reason, and Reliability", which tx cannot import because it
// lives in another module's internal tree: the beta distribution, its fit
// from a mean and one percentile, and its sampler. FitQuantiles, a fit from
// two percentiles, is new here.
package stats

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
)

// Beta is a beta distribution scaled to [Min, Max].
type Beta struct {
	Min, Max    float64
	Alpha, Beta float64
}

// NewBeta returns the distribution with the given range and shape.
func NewBeta(min, max, alpha, beta float64) (Beta, error) {
	if !(min < max) {
		return Beta{}, errors.New("min must be less than max")
	}
	if !(alpha > 0) || !(beta > 0) || math.IsInf(alpha, 0) || math.IsInf(beta, 0) {
		return Beta{}, errors.New("alpha and beta must be positive and finite")
	}
	return Beta{Min: min, Max: max, Alpha: alpha, Beta: beta}, nil
}

func (d Beta) span() float64 { return d.Max - d.Min }

// Mean returns the distribution's mean.
func (d Beta) Mean() float64 {
	return d.Min + d.Alpha/(d.Alpha+d.Beta)*d.span()
}

// Variance returns the distribution's variance.
func (d Beta) Variance() float64 {
	sum := d.Alpha + d.Beta
	return d.Alpha * d.Beta / (sum * sum * (sum + 1)) * d.span() * d.span()
}

// CDF returns the probability that a sample is at most x.
func (d Beta) CDF(x float64) float64 {
	if x <= d.Min {
		return 0
	}
	if x >= d.Max {
		return 1
	}
	return regularizedIncompleteBeta(d.Alpha, d.Beta, (x-d.Min)/d.span())
}

// Quantile returns the value below which a fraction p of samples fall.
func (d Beta) Quantile(p float64) float64 {
	if p <= 0 {
		return d.Min
	}
	if p >= 1 {
		return d.Max
	}
	lo, hi := 0.0, 1.0
	for range 100 {
		mid := 0.5 * (lo + hi)
		if regularizedIncompleteBeta(d.Alpha, d.Beta, mid) < p {
			lo = mid
		} else {
			hi = mid
		}
	}
	return d.Min + 0.5*(lo+hi)*d.span()
}

// Sample draws one value using r, so a seeded r gives a reproducible
// sequence. It does not allocate.
func (d Beta) Sample(r *rand.Rand) float64 {
	return d.Min + sampleBetaUnit(d.Alpha, d.Beta, r)*d.span()
}

// fitTolerance is the largest CDF error a fit may leave at the percentile it
// matches; a fit that cannot get closer reports an error.
const fitTolerance = 1e-3

// FitMeanQuantile returns the beta distribution on [min, max] with the given
// mean whose CDF at x is p: the value x is its 100p-th percentile.
func FitMeanQuantile(min, max, mean, p, x float64) (Beta, error) {
	if err := checkRange(min, max, mean, "mean"); err != nil {
		return Beta{}, err
	}
	if err := checkRange(min, max, x, "percentile"); err != nil {
		return Beta{}, err
	}
	if !(0 < p && p < 1) {
		return Beta{}, fmt.Errorf("percentile probability %g must be in (0, 1)", p)
	}
	span := max - min
	unitMean, q := (mean-min)/span, (x-min)/span
	// For a fixed mean, beta follows alpha; search alpha in log space.
	eval := func(logA float64) (float64, float64) {
		a := math.Exp(logA)
		b := a * (1 - unitMean) / unitMean
		return b, math.Abs(regularizedIncompleteBeta(a, b, q) - p)
	}
	a, b, fitErr := searchAlpha(eval)
	if fitErr > fitTolerance {
		return Beta{}, fmt.Errorf("no beta distribution on [%g, %g] has mean %g and p%g of %g", min, max, mean, 100*p, x)
	}
	return NewBeta(min, max, a, b)
}

// FitQuantiles returns the beta distribution on [min, max] whose CDF is p1 at
// x1 and p2 at x2, with p1 < p2 and x1 < x2.
func FitQuantiles(min, max, p1, x1, p2, x2 float64) (Beta, error) {
	for _, x := range []float64{x1, x2} {
		if err := checkRange(min, max, x, "percentile"); err != nil {
			return Beta{}, err
		}
	}
	if !(0 < p1 && p1 < p2 && p2 < 1) || !(x1 < x2) {
		return Beta{}, errors.New("percentiles must increase in both probability and value")
	}
	span := max - min
	q1, q2 := (x1-min)/span, (x2-min)/span
	// For a fixed alpha, the CDF at q1 grows with beta, so bisect beta to
	// match p1 there; then search alpha for the best match of p2 at q2.
	eval := func(logA float64) (float64, float64) {
		a := math.Exp(logA)
		lo, hi := -20.0, 20.0
		for range 80 {
			mid := 0.5 * (lo + hi)
			if regularizedIncompleteBeta(a, math.Exp(mid), q1) < p1 {
				lo = mid
			} else {
				hi = mid
			}
		}
		b := math.Exp(0.5 * (lo + hi))
		err := math.Abs(regularizedIncompleteBeta(a, b, q1)-p1) + math.Abs(regularizedIncompleteBeta(a, b, q2)-p2)
		return b, err
	}
	a, b, fitErr := searchAlpha(eval)
	if fitErr > fitTolerance {
		return Beta{}, fmt.Errorf("no beta distribution on [%g, %g] has p%g of %g and p%g of %g", min, max, 100*p1, x1, 100*p2, x2)
	}
	return NewBeta(min, max, a, b)
}

func checkRange(min, max, x float64, name string) error {
	if !(min < max) || math.IsInf(min, 0) || math.IsInf(max, 0) {
		return fmt.Errorf("range [%g, %g] must be finite with min < max", min, max)
	}
	if !(min < x && x < max) {
		return fmt.Errorf("%s %g must lie strictly inside [%g, %g]", name, x, min, max)
	}
	return nil
}

// searchAlpha minimizes eval's error over log alpha: a coarse grid, then a
// shrinking step around the best point. eval returns the matching beta and
// the error for a log alpha.
func searchAlpha(eval func(logA float64) (b, err float64)) (a, b, bestErr float64) {
	bestLogA := 0.0
	bestErr = math.Inf(1)
	for i := 0; i <= 96; i++ {
		logA := -12 + 24*float64(i)/96
		if bv, err := eval(logA); err < bestErr {
			bestLogA, b, bestErr = logA, bv, err
		}
	}
	step := 2.0
	for range 40 {
		improved := false
		for _, dir := range []float64{-1, 1} {
			logA := clamp(bestLogA+dir*step, -20, 20)
			if bv, err := eval(logA); err < bestErr {
				bestLogA, b, bestErr, improved = logA, bv, err, true
			}
		}
		if !improved {
			if step *= 0.5; step < 1e-4 {
				break
			}
		}
	}
	return math.Exp(bestLogA), b, bestErr
}

func sampleBetaUnit(alpha, beta float64, r *rand.Rand) float64 {
	// For small shapes, use Johnk's algorithm.
	if alpha <= 1 && beta <= 1 {
		for {
			u1, u2 := r.Float64(), r.Float64()
			x := math.Pow(u1, 1/alpha)
			y := math.Pow(u2, 1/beta)
			sum := x + y
			if sum > 1 {
				continue
			}
			if sum > 0 {
				return x / sum
			}
			// Both round to zero: compare in log space.
			lu := math.Log(math.Max(u1, math.SmallestNonzeroFloat64)) / alpha
			lv := math.Log(math.Max(u2, math.SmallestNonzeroFloat64)) / beta
			m := math.Max(lu, lv)
			ex, ey := math.Exp(lu-m), math.Exp(lv-m)
			return ex / (ex + ey)
		}
	}
	x := sampleGamma(alpha, r)
	y := sampleGamma(beta, r)
	return x / (x + y)
}

// sampleGamma is Marsaglia and Tsang's method, boosted for shape < 1.
func sampleGamma(shape float64, r *rand.Rand) float64 {
	if shape < 1 {
		return sampleGamma(shape+1, r) * math.Pow(math.Max(r.Float64(), math.SmallestNonzeroFloat64), 1/shape)
	}
	d := shape - 1.0/3.0
	c := 1 / math.Sqrt(9*d)
	for {
		x := r.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := r.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(math.Max(u, math.SmallestNonzeroFloat64)) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

func regularizedIncompleteBeta(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	lab, _ := math.Lgamma(a + b)
	la, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	bt := math.Exp(lab - la - lb + a*math.Log(x) + b*math.Log(1-x))
	if x < (a+1)/(a+b+2) {
		return bt * betaContinuedFraction(a, b, x) / a
	}
	return 1 - bt*betaContinuedFraction(b, a, 1-x)/b
}

func betaContinuedFraction(a, b, x float64) float64 {
	const (
		maxIter = 200
		eps     = 3e-14
		fpmin   = 1e-300
	)
	qab, qap, qam := a+b, a+1, a-1
	c, d := 1.0, 1-qab*x/qap
	if math.Abs(d) < fpmin {
		d = fpmin
	}
	d = 1 / d
	h := d
	for m := 1; m <= maxIter; m++ {
		mf := float64(m)
		m2 := 2 * mf
		aa := mf * (b - mf) * x / ((qam + m2) * (a + m2))
		d = 1 + aa*d
		if math.Abs(d) < fpmin {
			d = fpmin
		}
		c = 1 + aa/c
		if math.Abs(c) < fpmin {
			c = fpmin
		}
		d = 1 / d
		h *= d * c
		aa = -(a + mf) * (qab + mf) * x / ((a + m2) * (qap + m2))
		d = 1 + aa*d
		if math.Abs(d) < fpmin {
			d = fpmin
		}
		c = 1 + aa/c
		if math.Abs(c) < fpmin {
			c = fpmin
		}
		d = 1 / d
		delta := d * c
		h *= delta
		if math.Abs(delta-1) < eps {
			break
		}
	}
	return h
}

func clamp(x, lo, hi float64) float64 {
	return math.Min(math.Max(x, lo), hi)
}
