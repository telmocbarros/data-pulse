package models

type HistogramBucket struct {
	Min   float64
	Max   float64
	Count int64
}

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

type Ratio struct {
	Value        float64
	UniqueValues int64
	TotalRows    int64
}

type Occurrence struct {
	Value string
	Count int64
}

type CategoryProfiler struct {
	ColumnName         string
	Cardinality        int64
	UniquenessRatio    Ratio
	NullCount          int64
	NullPercent        float64
	MostFrequentValues map[string]*Occurrence
	TypeDistribution   map[string]float64
}

type DatasetProfiler struct {
	Category map[string]*CategoryProfiler
	Numeric  map[string]*NumericProfiler
}
