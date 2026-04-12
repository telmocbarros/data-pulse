package profiler

import (
	"fmt"
	"math"
	"sort"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/models"
)

var profilers = make(map[string]models.DatasetProfiler)

func ProfileDataset(rowCh chan map[string]any, datasetId string, columns map[string]string) (*models.DatasetProfiler, error) {
	profiler, exists := profilers[datasetId]
	if !exists {
		profiler = createProfiler(columns)
		profilers[datasetId] = profiler
	}

	var totalRows int64
	for row := range rowCh {
		totalRows++
		for colName, colType := range columns {
			val, exists := row[colName]
			if !exists || val == nil {
				if colType == columntype.IS_NUMERICAL {
					profiler.Numeric[colName].NullCount++
				} else {
					profiler.Category[colName].NullCount++
				}
				continue
			}

			if colType == columntype.IS_NUMERICAL {
				n := profiler.Numeric[colName]
				num, ok := toFloat64(val)
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
			} else {
				c := profiler.Category[colName]
				str := fmt.Sprintf("%v", val)
				if occ, exists := c.MostFrequentValues[str]; exists {
					occ.Count++
				} else {
					c.MostFrequentValues[str] = &models.Occurrence{Value: str, Count: 1}
				}
				c.TypeDistribution[colType]++
			}
		}
	}

	finalise(&profiler, totalRows)
	profilers[datasetId] = profiler
	return &profiler, nil
}

func createProfiler(columns map[string]string) models.DatasetProfiler {
	numericProfilers := make(map[string]*models.NumericProfiler)
	categoryProfilers := make(map[string]*models.CategoryProfiler)

	for name, colType := range columns {
		if colType == columntype.IS_NUMERICAL {
			numericProfiler := initialiseNumericProfiler(name)
			numericProfilers[name] = &numericProfiler
		} else {
			categoryProfiler := initialiseCategoryProfiler(name)
			categoryProfilers[name] = &categoryProfiler
		}
	}

	datasetProfiler := models.DatasetProfiler{
		Numeric:  numericProfilers,
		Category: categoryProfilers,
	}

	return datasetProfiler
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
	categoryRatio := models.Ratio{
		Value:        0,
		UniqueValues: 0,
		TotalRows:    0,
	}

	return models.CategoryProfiler{
		ColumnName:         name,
		UniquenessRatio:    categoryRatio,
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
