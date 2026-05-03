package profiler

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/models"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
)

// ProfileAndStore streams the dataset's rows from the DB, profiles them,
// and persists the results. progressFn receives stage milestones (5 → 70 →
// 85 → 95). The job manager auto-fires 100 on success.
//
// Cancellation: ctx cancellation aborts the streaming query and propagates
// through the producer/consumer pair via rowCh's close — ProfileDataset's
// for-range exits, ProfileAndStore observes streamErr == ctx.Err(), and no
// partial profile is persisted.
//
// The single-goroutine-writes/post-Wait-reads pattern around streamErr is
// race-free without a mutex: the producer's `defer close(rowCh)` happens-
// before ProfileDataset returns (which only happens once rowCh is closed),
// which happens-before we read streamErr.
func ProfileAndStore(
	ctx context.Context,
	datasetId string,
	tableName string,
	columnTypes map[string]string,
	progressFn func(int),
) error {
	progressFn(5)

	rowCh := make(chan map[string]any, 100)
	var streamErr error
	go func() {
		defer close(rowCh)
		streamErr = repository.StreamDatasetRows(ctx, tableName, rowCh)
	}()

	result := ProfileDataset(rowCh, columnTypes)

	if streamErr != nil {
		return fmt.Errorf("stream dataset rows: %w", streamErr)
	}
	progressFn(70)

	if err := profilerRepo.StoreProfile(datasetId, result); err != nil {
		return fmt.Errorf("storing profile: %w", err)
	}
	progressFn(85)

	numericCols := make([]string, 0, len(columnTypes))
	for name, colType := range columnTypes {
		if colType == columntype.Numerical {
			numericCols = append(numericCols, name)
		}
	}
	if err := profilerRepo.StoreCorrelationMatrix(datasetId, tableName, numericCols); err != nil {
		return fmt.Errorf("storing correlation matrix: %w", err)
	}
	progressFn(95)

	return nil
}

// columnValue carries a single value destined for a column's profiler goroutine.
type columnValue struct {
	val any
}

// ProfileDataset profiles a dataset concurrently across columns.
// Each column gets its own goroutine that processes values in parallel.
func ProfileDataset(rowCh chan map[string]any, columns map[string]string) *models.DatasetProfiler {
	profiler := createProfiler(columns)

	// Create a channel per column
	colChannels := make(map[string]chan columnValue, len(columns))
	for colName := range columns {
		colChannels[colName] = make(chan columnValue, 100)
	}

	var wg sync.WaitGroup
	var totalRows int64
	var rowCountOnce sync.Once

	// Launch a goroutine per column
	for colName, colType := range columns {
		colCh := colChannels[colName]

		if colType == columntype.Numerical {
			n := profiler.Numeric[colName]
			wg.Go(func() {
				var localRows int64
				for cv := range colCh {
					localRows++
					if cv.val == nil {
						n.NullCount++
						continue
					}
					num, ok := toFloat64(cv.val)
					if !ok {
						n.TypeDistribution["non_numeric"]++
						continue
					}
					n.TypeDistribution["numeric"]++
					n.Count++
					n.Sum += num
					n.Values = append(n.Values, num)
					if num > n.Max {
						n.Max = num
					}
					if num < n.Min {
						n.Min = num
					}
				}
				rowCountOnce.Do(func() { totalRows = localRows })
			})
		} else {
			c := profiler.Category[colName]
			wg.Go(func() {
				var localRows int64
				for cv := range colCh {
					localRows++
					if cv.val == nil {
						c.NullCount++
						continue
					}
					str := fmt.Sprintf("%v", cv.val)
					if occ, exists := c.MostFrequentValues[str]; exists {
						occ.Count++
					} else {
						c.MostFrequentValues[str] = &models.Occurrence{Value: str, Count: 1}
					}
					c.TypeDistribution[colType]++
				}
				rowCountOnce.Do(func() { totalRows = localRows })
			})
		}
	}

	// Fan out: distribute each row's values to the per-column channels
	for row := range rowCh {
		for colName := range columns {
			val, exists := row[colName]
			if !exists {
				colChannels[colName] <- columnValue{val: nil}
			} else {
				colChannels[colName] <- columnValue{val: val}
			}
		}
	}

	// Close all column channels so column goroutines finish
	for _, ch := range colChannels {
		close(ch)
	}

	wg.Wait()

	finalise(&profiler, totalRows)
	return &profiler
}

func createProfiler(columns map[string]string) models.DatasetProfiler {
	numericProfilers := make(map[string]*models.NumericProfiler)
	categoryProfilers := make(map[string]*models.CategoryProfiler)

	for name, colType := range columns {
		if colType == columntype.Numerical {
			numericProfiler := initialiseNumericProfiler(name)
			numericProfilers[name] = &numericProfiler
		} else {
			categoryProfiler := initialiseCategoryProfiler(name)
			categoryProfilers[name] = &categoryProfiler
		}
	}

	return models.DatasetProfiler{
		Numeric:  numericProfilers,
		Category: categoryProfilers,
	}
}

func initialiseNumericProfiler(name string) models.NumericProfiler {
	return models.NumericProfiler{
		ColumnName:       name,
		Min:              math.Inf(1),
		Max:              math.Inf(-1),
		Values:           make([]float64, 0),
		Histogram:        make([]models.HistogramBucket, 0),
		TypeDistribution: make(map[string]float64),
	}
}

func initialiseCategoryProfiler(name string) models.CategoryProfiler {
	return models.CategoryProfiler{
		ColumnName:         name,
		UniquenessRatio:    models.Ratio{},
		MostFrequentValues: make(map[string]*models.Occurrence),
		TypeDistribution:   make(map[string]float64),
	}
}

func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	default:
		return 0, false
	}
}

func finalise(profiler *models.DatasetProfiler, totalRows int64) {
	for _, n := range profiler.Numeric {
		finaliseNumeric(n, totalRows)
	}
	for _, c := range profiler.Category {
		finaliseCategory(c, totalRows)
	}
}

func finaliseNumeric(n *models.NumericProfiler, totalRows int64) {
	if n.Count == 0 {
		return
	}

	n.Mean = n.Sum / float64(n.Count)
	n.NullPercent = float64(n.NullCount) / float64(totalRows) * 100

	sort.Float64s(n.Values)

	n.Median = percentile(n.Values, 50)
	n.P25 = percentile(n.Values, 25)
	n.P50 = n.Median
	n.P75 = percentile(n.Values, 75)

	// standard deviation
	var sumSquaredDiff float64
	for _, v := range n.Values {
		diff := v - n.Mean
		sumSquaredDiff += diff * diff
	}
	n.Stdv = math.Sqrt(sumSquaredDiff / float64(n.Count))

	// histogram
	bucketCount := 10
	rangeVal := n.Max - n.Min
	if rangeVal == 0 {
		n.Histogram = []models.HistogramBucket{{Min: n.Min, Max: n.Max, Count: n.Count}}
		return
	}
	bucketWidth := rangeVal / float64(bucketCount)
	n.Histogram = make([]models.HistogramBucket, bucketCount)
	for i := range n.Histogram {
		n.Histogram[i].Min = n.Min + float64(i)*bucketWidth
		n.Histogram[i].Max = n.Min + float64(i+1)*bucketWidth
	}
	for _, v := range n.Values {
		idx := int((v - n.Min) / bucketWidth)
		if idx >= bucketCount {
			idx = bucketCount - 1
		}
		n.Histogram[idx].Count++
	}
}

func finaliseCategory(c *models.CategoryProfiler, totalRows int64) {
	c.Cardinality = int64(len(c.MostFrequentValues))
	c.NullPercent = float64(c.NullCount) / float64(totalRows) * 100
	c.UniquenessRatio = models.Ratio{
		UniqueValues: c.Cardinality,
		TotalRows:    totalRows,
		Value:        float64(c.Cardinality) / float64(totalRows),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p / 100 * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
