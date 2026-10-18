package stats

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestNewBetaValidation(t *testing.T) {
	for _, tc := range []struct{ min, max, a, b float64 }{
		{10, 10, 2, 3},
		{0, 1, 0, 2},
		{0, 1, 2, -1},
		{0, 1, math.Inf(1), 2},
	} {
		if _, err := NewBeta(tc.min, tc.max, tc.a, tc.b); err == nil {
			t.Fatalf("NewBeta(%v) accepted", tc)
		}
	}
}

func TestBetaMeanVariance(t *testing.T) {
	d, err := NewBeta(10, 20, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if want := 10 + 2.0/5*10; math.Abs(d.Mean()-want) > 1e-12 {
		t.Fatalf("mean %g, want %g", d.Mean(), want)
	}
	if want := 2.0 * 3 / (5 * 5 * 6) * 100; math.Abs(d.Variance()-want) > 1e-12 {
		t.Fatalf("variance %g, want %g", d.Variance(), want)
	}
}

func TestQuantileCDFRoundTrip(t *testing.T) {
	d, err := NewBeta(64, 4096, 2.4, 5.1)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []float64{0.01, 0.1, 0.5, 0.9, 0.99} {
		if got := d.CDF(d.Quantile(p)); math.Abs(got-p) > 5e-4 {
			t.Fatalf("CDF(Quantile(%g)) = %g", p, got)
		}
	}
}

func TestFitMeanQuantile(t *testing.T) {
	orig, err := NewBeta(64, 4096, 2.2, 7)
	if err != nil {
		t.Fatal(err)
	}
	mean, p99 := orig.Mean(), orig.Quantile(0.99)
	fit, err := FitMeanQuantile(64, 4096, mean, 0.99, p99)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(fit.Mean()-mean) > 2 || math.Abs(fit.Quantile(0.99)-p99) > 4 {
		t.Fatalf("fit mean %g p99 %g, want %g and %g", fit.Mean(), fit.Quantile(0.99), mean, p99)
	}
	for _, bad := range [][5]float64{
		{1, 1, 1, 0.99, 1},      // empty range
		{0, 100, -1, 0.99, 20},  // mean outside the range
		{0, 100, 50, 0.99, 100}, // percentile on the bound
		{0, 100, 50, 1, 60},     // probability not inside (0, 1)
	} {
		if _, err := FitMeanQuantile(bad[0], bad[1], bad[2], bad[3], bad[4]); err == nil {
			t.Fatalf("FitMeanQuantile%v accepted", bad)
		}
	}
}

func TestFitQuantiles(t *testing.T) {
	orig, err := NewBeta(1, 50, 1.8, 9)
	if err != nil {
		t.Fatal(err)
	}
	p50, p99 := orig.Quantile(0.5), orig.Quantile(0.99)
	fit, err := FitQuantiles(1, 50, 0.5, p50, 0.99, p99)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(fit.Quantile(0.5)-p50) > 0.05 || math.Abs(fit.Quantile(0.99)-p99) > 0.1 {
		t.Fatalf("fit p50 %g p99 %g, want %g and %g", fit.Quantile(0.5), fit.Quantile(0.99), p50, p99)
	}
	if _, err := FitQuantiles(1, 50, 0.99, p50, 0.5, p99); err == nil {
		t.Fatal("accepted decreasing probabilities")
	}
	if _, err := FitQuantiles(1, 50, 0.5, p99, 0.99, p50); err == nil {
		t.Fatal("accepted decreasing values")
	}
}

func TestSample(t *testing.T) {
	d, err := NewBeta(64, 4096, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(42, 7))
	const n = 20000
	var sum float64
	for range n {
		x := d.Sample(r)
		if x < d.Min || x > d.Max {
			t.Fatalf("sample %g outside [%g, %g]", x, d.Min, d.Max)
		}
		sum += x
	}
	if got := sum / n; math.Abs(got-d.Mean()) > 0.02*d.Mean() {
		t.Fatalf("sample mean %g, want about %g", got, d.Mean())
	}
	// The same seed gives the same sequence.
	a, b := rand.New(rand.NewPCG(1, 2)), rand.New(rand.NewPCG(1, 2))
	for range 100 {
		if x, y := d.Sample(a), d.Sample(b); x != y {
			t.Fatalf("seeded samples differ: %g and %g", x, y)
		}
	}
	// Small shapes take the other sampler.
	small, err := NewBeta(0, 1, 0.5, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	for range 1000 {
		if x := small.Sample(r); x < 0 || x > 1 {
			t.Fatalf("small-shape sample %g outside [0, 1]", x)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = d.Sample(r) }); allocs != 0 {
		t.Fatalf("Sample allocated %.1f times", allocs)
	}
}

// TestSampleMatchesShape checks samples follow the distribution at each of
// the sampler's paths: Marsaglia and Tsang for shapes of at least 1, its
// boost for a shape below 1 (which every fitted fault latency with a long
// tail uses), and Johnk's algorithm when both shapes are at most 1. The
// shapes are asymmetric, so swapping them is caught too.
func TestSampleMatchesShape(t *testing.T) {
	for _, shape := range [][2]float64{{2, 4}, {0.3, 5}, {0.5, 0.9}} {
		d, err := NewBeta(0, 1, shape[0], shape[1])
		if err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewPCG(42, 7))
		const n = 20000
		samples := make([]float64, n)
		for k := range samples {
			samples[k] = d.Sample(r)
		}
		for _, p := range []float64{0.1, 0.5, 0.9, 0.99} {
			q := d.Quantile(p)
			below := 0
			for _, x := range samples {
				if x <= q {
					below++
				}
			}
			if got := float64(below) / n; math.Abs(got-p) > 0.015 {
				t.Fatalf("Beta(%g,%g): %.3f of samples at or below the p%g %g", shape[0], shape[1], got, 100*p, q)
			}
		}
	}
}

// FuzzFitBeta checks both fits two ways. Statistics taken from a real beta
// distribution must fit, and the fit must reproduce them. Arbitrary
// increasing statistics may fail to fit, but a fit that succeeds must match
// what was asked.
func FuzzFitBeta(f *testing.F) {
	f.Add(uint16(20), uint16(70), uint16(1000), uint16(3000), true)
	f.Add(uint16(5), uint16(5), uint16(100), uint16(400), false)
	f.Add(uint16(400), uint16(9), uint16(5000), uint16(9998), true)
	f.Fuzz(func(t *testing.T, aRaw, bRaw, xRaw, yRaw uint16, real bool) {
		const min, max = 0.0, 10000.0
		check := func(name string, ok bool, got, want float64) {
			t.Helper()
			if !ok {
				t.Fatalf("%s: got %g, want %g", name, got, want)
			}
		}
		if real {
			// Shapes from 0.5 to 50 in each parameter.
			orig, err := NewBeta(min, max, 0.5+float64(aRaw%500)/10, 0.5+float64(bRaw%500)/10)
			if err != nil {
				t.Fatal(err)
			}
			mean, p50, p99 := orig.Mean(), orig.Quantile(0.5), orig.Quantile(0.99)
			if !(min < mean && mean < p99 && p99 < max && min < p50 && p50 < p99) {
				return // a statistic at the edge of the range is not representable
			}
			fit, err := FitMeanQuantile(min, max, mean, 0.99, p99)
			if err != nil {
				t.Fatalf("Beta(%g,%g): mean %g and p99 %g did not fit: %v", orig.Alpha, orig.Beta, mean, p99, err)
			}
			check("mean", math.Abs(fit.Mean()-mean) < 1e-6*max, fit.Mean(), mean)
			check("CDF at p99", math.Abs(fit.CDF(p99)-0.99) <= 2*fitTolerance, fit.CDF(p99), 0.99)
			fit, err = FitQuantiles(min, max, 0.5, p50, 0.99, p99)
			if err != nil {
				t.Fatalf("Beta(%g,%g): p50 %g and p99 %g did not fit: %v", orig.Alpha, orig.Beta, p50, p99, err)
			}
			check("CDF at p50", math.Abs(fit.CDF(p50)-0.5) <= 2*fitTolerance, fit.CDF(p50), 0.5)
			check("CDF at p99", math.Abs(fit.CDF(p99)-0.99) <= 2*fitTolerance, fit.CDF(p99), 0.99)
			return
		}
		x, y := float64(xRaw%9998)+1, float64(yRaw%9998)+1
		if x == y {
			return
		}
		if x > y {
			x, y = y, x
		}
		if fit, err := FitMeanQuantile(min, max, x, 0.99, y); err == nil {
			check("mean", math.Abs(fit.Mean()-x) < 1e-6*max, fit.Mean(), x)
			check("CDF at p99", math.Abs(fit.CDF(y)-0.99) <= 2*fitTolerance, fit.CDF(y), 0.99)
		}
		if fit, err := FitQuantiles(min, max, 0.5, x, 0.99, y); err == nil {
			check("CDF at p50", math.Abs(fit.CDF(x)-0.5) <= 2*fitTolerance, fit.CDF(x), 0.5)
			check("CDF at p99", math.Abs(fit.CDF(y)-0.99) <= 2*fitTolerance, fit.CDF(y), 0.99)
		}
	})
}
