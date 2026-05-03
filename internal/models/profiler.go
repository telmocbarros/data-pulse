package models

// HistogramBucket is one bin of a numeric histogram. Min and Max are the
// half-open bucket bounds; Count is the number of values that fell in.
type HistogramBucket struct {
	Min   float64
	Max   float64
	Count int64
}

// NumericProfiler is the per-column summary produced by the profiler for
// numeric columns: count/min/max/mean/stddev plus quartiles, the raw
// values (kept for percentile and histogram passes), null counts, and a
// fixed-width histogram.
type NumericProfiler struct {
	ColumnName       string
	Min              float64
	Max              float64
	Sum              float64 // running sum for mean calculation
	Count            int64   // total non-null values
	Mean             float64
	Median           float64
	Stdv             float64
	P25              float64
	P50              float64
	P75              float64
	Values           []float64 // collected values for median, percentiles, stddev
	NullCount        int64
	NullPercent      float64
	Histogram        []HistogramBucket
	TypeDistribution map[string]float64
}

// Ratio carries the components of a uniqueness ratio so the API can report
// numerator and denominator alongside the resolved value.
type Ratio struct {
	Value        float64
	UniqueValues int64
	TotalRows    int64
}

// Occurrence is a (value, count) pair used for category breakdowns.
type Occurrence struct {
	Value string
	Count int64
}

// CategoryProfiler is the per-column summary produced by the profiler for
// categorical columns: cardinality, uniqueness, null counts, and the
// most-frequent values keyed by their string form.
type CategoryProfiler struct {
	ColumnName         string
	Cardinality        int64
	UniquenessRatio    Ratio
	NullCount          int64
	NullPercent        float64
	MostFrequentValues map[string]*Occurrence
	TypeDistribution   map[string]float64
}

// DatasetProfiler is the full result of profiling a dataset: per-column
// numeric and category profilers, keyed by column name.
type DatasetProfiler struct {
	Category map[string]*CategoryProfiler
	Numeric  map[string]*NumericProfiler
}
