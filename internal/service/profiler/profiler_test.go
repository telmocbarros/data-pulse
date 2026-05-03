package profiler

import (
	"math"
	"testing"

	"github.com/telmocbarros/data-pulse/internal/models"
)

const epsilon = 1e-9

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < epsilon
}

func TestPercentileEmpty(t *testing.T) {
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile(nil, 50) = %v, want 0", got)
	}
}

func TestPercentileSingleton(t *testing.T) {
	values := []float64{42}
	for _, p := range []float64{0, 25, 50, 75, 100} {
		if got := percentile(values, p); got != 42 {
			t.Errorf("percentile([42], %v) = %v, want 42", p, got)
		}
	}
}

func TestPercentileLinearInterpolation(t *testing.T) {
	// Sorted [0, 1, 2, 3, 4]: idx for p=50 is 2.0 -> exact 2; for p=25 is 1.0 -> exact 1.
	values := []float64{0, 1, 2, 3, 4}
	cases := []struct {
		p    float64
		want float64
	}{
		{0, 0},
		{25, 1},
		{50, 2},
		{75, 3},
		{100, 4},
	}
	for _, c := range cases {
		if got := percentile(values, c.p); !almostEqual(got, c.want) {
			t.Errorf("percentile(%v, %v) = %v, want %v", values, c.p, got, c.want)
		}
	}
}

func TestPercentileInterpolatedFraction(t *testing.T) {
	// Sorted [0, 10]: idx for p=30 is 0.3; lower=0, upper=1, frac=0.3 -> 0*0.7 + 10*0.3 = 3.
	values := []float64{0, 10}
	if got := percentile(values, 30); !almostEqual(got, 3) {
		t.Errorf("percentile([0,10], 30) = %v, want 3", got)
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		want   float64
		wantOk bool
	}{
		{"float64", 3.14, 3.14, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", 42, 42, true},
		{"int64", int64(42), 42, true},
		{"int32", int32(42), 42, true},
		{"string is not numeric", "42", 0, false},
		{"bool is not numeric", true, 0, false},
		{"nil is not numeric", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64(tt.val)
			if ok != tt.wantOk {
				t.Errorf("toFloat64(%v) ok = %v, want %v", tt.val, ok, tt.wantOk)
			}
			if ok && !almostEqual(got, tt.want) {
				t.Errorf("toFloat64(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestFinaliseNumericNoCount(t *testing.T) {
	// Count==0 returns immediately without panicking on empty Values.
	n := &models.NumericProfiler{
		Count:            0,
		Values:           nil,
		TypeDistribution: map[string]float64{},
	}
	finaliseNumeric(n, 100)
	if n.Mean != 0 || n.Median != 0 {
		t.Errorf("expected zero stats for empty profile, got mean=%v median=%v", n.Mean, n.Median)
	}
}

func TestFinaliseNumericSingleDistinctValue(t *testing.T) {
	// All-same values: rangeVal == 0 short-circuits to a single histogram bucket.
	n := &models.NumericProfiler{
		Count:            5,
		Sum:              25,
		Min:              5,
		Max:              5,
		Values:           []float64{5, 5, 5, 5, 5},
		NullCount:        0,
		TypeDistribution: map[string]float64{},
	}
	finaliseNumeric(n, 5)
	if !almostEqual(n.Mean, 5) {
		t.Errorf("Mean = %v, want 5", n.Mean)
	}
	if !almostEqual(n.Median, 5) {
		t.Errorf("Median = %v, want 5", n.Median)
	}
	if !almostEqual(n.Stdv, 0) {
		t.Errorf("Stdv = %v, want 0", n.Stdv)
	}
	if len(n.Histogram) != 1 {
		t.Errorf("expected 1 histogram bucket for all-same values, got %d", len(n.Histogram))
	}
	if n.Histogram[0].Count != 5 {
		t.Errorf("bucket count = %v, want 5", n.Histogram[0].Count)
	}
}

func TestFinaliseNumericFullStats(t *testing.T) {
	// Values 1..10: mean=5.5, median=5.5, stddev=2.872... over n=10
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	n := &models.NumericProfiler{
		Count:            10,
		Sum:              55,
		Min:              1,
		Max:              10,
		Values:           append([]float64(nil), values...),
		NullCount:        2,
		TypeDistribution: map[string]float64{},
	}
	finaliseNumeric(n, 12) // 10 numeric + 2 nulls = 12 total rows

	if !almostEqual(n.Mean, 5.5) {
		t.Errorf("Mean = %v, want 5.5", n.Mean)
	}
	if !almostEqual(n.Median, 5.5) {
		t.Errorf("Median = %v, want 5.5", n.Median)
	}
	// Population stddev of 1..10 = sqrt(82.5/10) ≈ 2.8722813...
	wantStdv := math.Sqrt(82.5 / 10)
	if !almostEqual(n.Stdv, wantStdv) {
		t.Errorf("Stdv = %v, want %v", n.Stdv, wantStdv)
	}
	// NullPercent = 2 / 12 * 100 ≈ 16.666...
	if !almostEqual(n.NullPercent, 2.0/12*100) {
		t.Errorf("NullPercent = %v, want %v", n.NullPercent, 2.0/12*100)
	}
	// 10 buckets across range [1, 10]; every bucket should be reachable. Check bucket count is 10.
	if len(n.Histogram) != 10 {
		t.Errorf("expected 10 histogram buckets, got %d", len(n.Histogram))
	}
	// All values fall within range; total bucket count should equal Count.
	var total int64
	for _, b := range n.Histogram {
		total += b.Count
	}
	if total != n.Count {
		t.Errorf("histogram total %d != Count %d", total, n.Count)
	}
}
