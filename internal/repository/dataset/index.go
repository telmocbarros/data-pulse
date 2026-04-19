package dataset

import (
	"fmt"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
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

func GetDatasetChart(datasetId string, graphType string) (charts map[string][]models.HistogramBucket, err error) {
	rows, err := config.Storage.Query("SELECT id, column_name FROM numeric_profiles where dataset_id = $1", datasetId)
	if err != nil {
		return nil, fmt.Errorf("Numeric profile not found: %w", err)
	}

	var histogramProfileId string
	var columnName string
	histogramMetadata := make(map[string]string)

	charts = make(map[string][]models.HistogramBucket)
	for rows.Next() {
		if err := rows.Scan(&histogramProfileId, &columnName); err != nil {
			return nil, err
		}
		fmt.Println("Fetching datasets columns and respective IDs")
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
		charts[key] = chart
	}

	return

}
