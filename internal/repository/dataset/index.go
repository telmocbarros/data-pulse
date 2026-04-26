package dataset

import (
	"database/sql"
	"fmt"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
)

// GetDatasetById returns the table_name and column types for a dataset.
func GetDatasetById(id string) (tableName string, columnTypes map[string]string, err error) {
	err = config.Storage.QueryRow(
		`SELECT table_name FROM datasets WHERE id = $1`, id,
	).Scan(&tableName)
	if err != nil {
		return "", nil, fmt.Errorf("dataset not found: %w", err)
	}

	rows, err := config.Storage.Query(
		`SELECT column_name, column_type FROM dataset_columns WHERE dataset_id = $1`, id,
	)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()

	columnTypes = make(map[string]string)
	for rows.Next() {
		var name, colType string
		if err := rows.Scan(&name, &colType); err != nil {
			return "", nil, err
		}
		columnTypes[name] = colType
	}

	return tableName, columnTypes, nil
}

func GetScatterPlotFromDataset(datasetId string, tableName string, columns []string) (scatterPlotData map[string][]map[string]float64, err error) {
	rows, err := config.Storage.Query("SELECT id, column_name FROM numeric_profiles where column_name in ($1, $2)", columns[0], columns[1])
	if err != nil {
		return nil, fmt.Errorf("Numeric profile not found: %w", err)
	}
	var columnId string
	var columnName string
	scatterPlotMetadata := make(map[string]string)

	for rows.Next() {
		if err := rows.Scan(&columnId, &columnName); err != nil {
			return nil, err
		}

		scatterPlotMetadata[columnName] = columnId
	}
	rows.Close()

	scatterPlotData = make(map[string][]map[string]float64)
	for key := range scatterPlotMetadata {
		rows, err = config.Storage.Query(fmt.Sprintf("SELECT entry_date, %s FROM %s ORDER BY entry_date ASC LIMIT 1000", key, tableName))
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		chart := make([]map[string]float64, 0)
		for rows.Next() {
			var entryDate string
			var value float64
			if err := rows.Scan(&entryDate, &value); err != nil {
				return nil, err
			}
			element := map[string]float64{entryDate: value}
			chart = append(chart, element)
		}
		scatterPlotData[key] = chart
	}
	return scatterPlotData, nil
}

func GetHistogramFromDataset(datasetId string, numBuckets int64) (histogramData map[string][]models.HistogramBucket, err error) {
	rows, err := config.Storage.Query("SELECT id, column_name FROM numeric_profiles where dataset_id = $1", datasetId)
	if err != nil {
		return nil, fmt.Errorf("Numeric profile not found: %w", err)
	}

	var histogramProfileId string
	var columnName string
	histogramMetadata := make(map[string]string)

	histogramData = make(map[string][]models.HistogramBucket)
	for rows.Next() {
		if err := rows.Scan(&histogramProfileId, &columnName); err != nil {
			return nil, err
		}

		histogramMetadata[columnName] = histogramProfileId
	}
	rows.Close()

	for key, val := range histogramMetadata {
		rows, err = config.Storage.Query("SELECT bucket_min, bucket_max, count FROM numeric_profile_histograms WHERE numeric_profile_id = $1 ORDER BY bucket_min ASC", val)
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		chart := make([]models.HistogramBucket, 0)
		for rows.Next() {
			var bucketMin, bucketMax float64
			var count int64
			if err := rows.Scan(&bucketMin, &bucketMax, &count); err != nil {
				return nil, err
			}
			chart = append(chart, models.HistogramBucket{Min: bucketMin, Max: bucketMax, Count: count})
		}
		histogramData[key] = chart
	}

	return
}

type TimeseriesDataPoint struct {
	ProductName string
	EntryDate   string
	Price       float64
	Quantity    float64
	Rating      float64
}

// GetCorrelationMatrixFromDataset returns a symmetric Pearson correlation
// matrix for the dataset's numeric columns. Missing pairs (CORR returned NULL)
// are omitted from the inner maps. If no rows are stored yet, this falls back
// to computing on demand from numericColumns and persisting the result.
func GetCorrelationMatrixFromDataset(datasetId string, tableName string, numericColumns []string) (map[string]map[string]float64, error) {
	matrix, err := readCorrelationMatrix(datasetId)
	if err != nil {
		return nil, err
	}
	if len(matrix) == 0 && len(numericColumns) >= 2 {
		if err := profilerRepo.StoreCorrelationMatrix(datasetId, tableName, numericColumns); err != nil {
			return nil, err
		}
		matrix, err = readCorrelationMatrix(datasetId)
		if err != nil {
			return nil, err
		}
	}
	for _, col := range numericColumns {
		if _, ok := matrix[col]; !ok {
			matrix[col] = make(map[string]float64)
		}
		matrix[col][col] = 1
	}
	return matrix, nil
}

func readCorrelationMatrix(datasetId string) (map[string]map[string]float64, error) {
	rows, err := config.Storage.Query(
		"SELECT column_a, column_b, pearson_r FROM correlation_matrices WHERE dataset_id = $1",
		datasetId,
	)
	if err != nil {
		return nil, fmt.Errorf("read correlation matrix: %w", err)
	}
	defer rows.Close()

	matrix := make(map[string]map[string]float64)
	for rows.Next() {
		var a, b string
		var r sql.NullFloat64
		if err := rows.Scan(&a, &b, &r); err != nil {
			return nil, err
		}
		if !r.Valid {
			continue
		}
		if _, ok := matrix[a]; !ok {
			matrix[a] = make(map[string]float64)
		}
		if _, ok := matrix[b]; !ok {
			matrix[b] = make(map[string]float64)
		}
		matrix[a][b] = r.Float64
		matrix[b][a] = r.Float64
	}
	return matrix, nil
}

func GetTimeseriesPlotFromDataset(datasetId string, tableName string, productName string) (timeseriesPlotData []TimeseriesDataPoint, err error) {
	// Depending on the table, I'll have to provide to the user which values can be chosen
	// to be presented as a timeseries plot.
	// Current example: product_name with price | rating | quantity
	rows, err := config.Storage.Query(
		fmt.Sprintf("SELECT product_name, entry_date, price, quantity, rating FROM %s WHERE product_name = $1 ORDER BY entry_date ASC", tableName),
		productName,
	)
	if err != nil {
		return nil, fmt.Errorf("Dataset data not found: %w", err)
	}
	defer rows.Close()

	timeseriesPlotData = make([]TimeseriesDataPoint, 0)

	for rows.Next() {
		var entry TimeseriesDataPoint
		err := rows.Scan(&entry.ProductName, &entry.EntryDate, &entry.Price, &entry.Quantity, &entry.Rating)
		if err != nil {
			return nil, err
		}

		timeseriesPlotData = append(timeseriesPlotData, entry)
	}

	return timeseriesPlotData, nil
}
