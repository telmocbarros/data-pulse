package dataset

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

// GetDatasetById returns the table_name and column types for an alive
// (non-soft-deleted) dataset.
func GetDatasetById(id string) (tableName string, columnTypes map[string]string, err error) {
	err = config.Storage.QueryRow(
		`SELECT table_name FROM datasets WHERE id = $1 AND deleted_at IS NULL`, id,
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

// DatasetRow mirrors a row in the datasets table.
type DatasetRow struct {
	ID          string `json:"id"`
	FileName    string `json:"file_name"`
	TableName   string `json:"table_name"`
	Size        int64  `json:"size"`
	UploadedBy  string `json:"uploaded_by"`
	Description string `json:"description"`
	CreatedAt   any    `json:"created_at"`
}

// GetDatasetRowById returns the full datasets row for an alive dataset.
func GetDatasetRowById(id string) (DatasetRow, error) {
	var d DatasetRow
	err := config.Storage.QueryRow(
		`SELECT id, file_name, table_name, size, uploaded_by, description, created_at
		 FROM datasets WHERE id = $1 AND deleted_at IS NULL`, id,
	).Scan(&d.ID, &d.FileName, &d.TableName, &d.Size, &d.UploadedBy, &d.Description, &d.CreatedAt)
	if err != nil {
		return DatasetRow{}, fmt.Errorf("dataset not found: %w", err)
	}
	return d, nil
}

// ScatterPoint is one (x, y) pair for the scatter chart.
type ScatterPoint struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// GetScatterPlotFromDataset returns up to limit (x, y) pairs from tableName.
// xColumn and yColumn must already be validated as numeric by the caller;
// this function additionally validates them as safe SQL identifiers.
func GetScatterPlotFromDataset(tableName string, xColumn string, yColumn string, limit int) ([]ScatterPoint, error) {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table name: %q", tableName)
	}
	if !sqlsafe.IsValidIdentifier(xColumn) {
		return nil, fmt.Errorf("invalid x column: %q", xColumn)
	}
	if !sqlsafe.IsValidIdentifier(yColumn) {
		return nil, fmt.Errorf("invalid y column: %q", yColumn)
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL LIMIT $1",
		xColumn, yColumn, tableName, xColumn, yColumn,
	)
	rows, err := config.Storage.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("scatter query: %w", err)
	}
	defer rows.Close()

	out := make([]ScatterPoint, 0)
	for rows.Next() {
		var p ScatterPoint
		if err := rows.Scan(&p.X, &p.Y); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// GetHistogramFromDataset returns histogram buckets keyed by column name for
// every numeric profile attached to the dataset. numBuckets is currently
// ignored — bucket count is fixed at profiling time.
func GetHistogramFromDataset(datasetId string, numBuckets int64) (map[string][]models.HistogramBucket, error) {
	profiles, err := listNumericProfileIds(datasetId)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]models.HistogramBucket, len(profiles))
	for columnName, profileId := range profiles {
		buckets, err := getHistogramBuckets(profileId)
		if err != nil {
			return nil, err
		}
		out[columnName] = buckets
	}
	return out, nil
}

func listNumericProfileIds(datasetId string) (map[string]string, error) {
	rows, err := config.Storage.Query(
		"SELECT id, column_name FROM numeric_profiles WHERE dataset_id = $1",
		datasetId,
	)
	if err != nil {
		return nil, fmt.Errorf("list numeric profiles: %w", err)
	}
	defer rows.Close()

	profiles := make(map[string]string)
	for rows.Next() {
		var id, columnName string
		if err := rows.Scan(&id, &columnName); err != nil {
			return nil, err
		}
		profiles[columnName] = id
	}
	return profiles, nil
}

func getHistogramBuckets(profileId string) ([]models.HistogramBucket, error) {
	rows, err := config.Storage.Query(
		"SELECT bucket_min, bucket_max, count FROM numeric_profile_histograms WHERE numeric_profile_id = $1 ORDER BY bucket_min ASC",
		profileId,
	)
	if err != nil {
		return nil, fmt.Errorf("read histogram buckets: %w", err)
	}
	defer rows.Close()

	buckets := make([]models.HistogramBucket, 0)
	for rows.Next() {
		var b models.HistogramBucket
		if err := rows.Scan(&b.Min, &b.Max, &b.Count); err != nil {
			return nil, err
		}
		buckets = append(buckets, b)
	}
	return buckets, nil
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

// GetTimeseriesPlotFromDataset returns rows ordered by xColumn, projecting
// xColumn and every yColumn. If seriesColumn is non-empty the rows are
// filtered where seriesColumn = seriesValue. All identifiers must already be
// validated by the caller against dataset_columns.
func GetTimeseriesPlotFromDataset(
	tableName string,
	xColumn string,
	yColumns []string,
	seriesColumn string,
	seriesValue string,
) ([]map[string]any, error) {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table name: %q", tableName)
	}
	if !sqlsafe.IsValidIdentifier(xColumn) {
		return nil, fmt.Errorf("invalid x column: %q", xColumn)
	}
	for _, y := range yColumns {
		if !sqlsafe.IsValidIdentifier(y) {
			return nil, fmt.Errorf("invalid y column: %q", y)
		}
	}
	if seriesColumn != "" && !sqlsafe.IsValidIdentifier(seriesColumn) {
		return nil, fmt.Errorf("invalid series column: %q", seriesColumn)
	}

	selectCols := append([]string{xColumn}, yColumns...)
	var query strings.Builder
	fmt.Fprintf(&query, "SELECT %s FROM %s", strings.Join(selectCols, ", "), tableName)
	args := []any{}
	if seriesColumn != "" {
		fmt.Fprintf(&query, " WHERE %s = $1", seriesColumn)
		args = append(args, seriesValue)
	}
	fmt.Fprintf(&query, " ORDER BY %s ASC", xColumn)

	rows, err := config.Storage.Query(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("timeseries query: %w", err)
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		scanTargets := make([]any, len(selectCols))
		holders := make([]any, len(selectCols))
		for i := range scanTargets {
			holders[i] = &scanTargets[i]
		}
		if err := rows.Scan(holders...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(selectCols))
		row["x"] = scanTargets[0]
		for i, y := range yColumns {
			row[y] = scanTargets[i+1]
		}
		out = append(out, row)
	}
	return out, nil
}
