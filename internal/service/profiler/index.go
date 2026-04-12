package profiler

import (
	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/models"
)

var profilers = make(map[string]models.DatasetProfiler)

func ProfileDataset(rowCh chan map[string]any, datasetId string, columns map[string]string) (*models.DatasetProfiler, error) {
	profiler, exists := profilers[datasetId]
	if !exists {
		profiler = createProfiler(columns, len(columns))
		profilers[datasetId] = profiler
	}

	for row := range rowCh {
		for colName, colType := range columns {
			if colType == columntype.IS_NUMERICAL {
				numericProfiler := profiler.Numeric[colName]

				if row[colName] > numericProfiler.Max {
					numericProfiler.Max = row[colName]
				}

				if row[colName] < numericProfiler.Min {
					numericProfiler.Min = row[colName]
				}


				


				numericProfiler.Mean = 0
				numericProfiler.Median = 0
				numericProfiler.Stdv = 0
				numericProfiler.P25 = 0
				numericProfiler.P50 = 0
				numericProfiler.P75 = 0
				numericProfiler.NullCount = 0
				numericProfiler.NullPercent = 0
				numericProfiler.Histogram = []       // it is a slice
				numericProfiler.TypeDistribution= [] // it is a map
			} else {
				profiler.Category[colName].Cardinality
				profiler.Category[colName].UniquenessRatio
				profiler.Category[colName].NullCount
				profiler.Category[colName].NullPercent
				profiler.Category[colName].MostFrequentValues // it is another slice
				profiler.Category[colName].TypeDistribution   // it is a map
			}
		}
	}

	return nil, nil
}

func createProfiler(columns map[string]string, size int) models.DatasetProfiler {
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
	histogramBucketSize := 10
	return models.NumericProfiler{
		ColumnName:       name,
		Min:              0,
		Max:              0,
		Mean:             0,
		Median:           0,
		Stdv:             0,
		P25:              0,
		P50:              0,
		P75:              0,
		NullCount:        0,
		NullPercent:      0,
		Histogram:        make([]models.HistogramBucket, 0, histogramBucketSize),
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
		Cardinality:        0,
		UniquenessRatio:    categoryRatio,
		NullCount:          0,
		NullPercent:        0,
		MostFrequentValues: make(map[string]*models.Occurrence),
		TypeDistribution:   make(map[string]float64),
	}

}
