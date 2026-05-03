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

// SampleScatterPlotFromDataset returns up to limit (x, y) pairs sampled
// across the table via TABLESAMPLE BERNOULLI, rather than the head-of-table
// LIMIT used by GetScatterPlotFromDataset. The sample percentage is sized
// from a row count of the (x, y) non-null intersection so the expected
// returned-row count is ~limit; we over-sample by 1.5x and apply LIMIT to
// absorb BERNOULLI's variance. If the table has fewer non-null rows than
// limit, every row is returned.
func SampleScatterPlotFromDataset(tableName string, xColumn string, yColumn string, limit int) ([]ScatterPoint, error) {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table name: %q", tableName)
	}
	if !sqlsafe.IsValidIdentifier(xColumn) {
		return nil, fmt.Errorf("invalid x column: %q", xColumn)
	}
	if !sqlsafe.IsValidIdentifier(yColumn) {
		return nil, fmt.Errorf("invalid y column: %q", yColumn)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0, got %d", limit)
	}

	var total int64
	err := config.Storage.QueryRow(
		fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL",
			tableName, xColumn, yColumn,
		),
	).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("scatter sample count: %w", err)
	}
	if total == 0 {
		return []ScatterPoint{}, nil
	}
	if total <= int64(limit) {
		return GetScatterPlotFromDataset(tableName, xColumn, yColumn, limit)
	}

	pct := (float64(limit) * 1.5 * 100.0) / float64(total)
	if pct > 100 {
		pct = 100
	}

	// TABLESAMPLE applies before WHERE, so non-null filtering still trims rows
	// after sampling. The 1.5x over-sample compensates for both BERNOULLI's
	// variance and the WHERE-induced shrinkage; LIMIT caps the final size.
	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s TABLESAMPLE BERNOULLI($1) WHERE %s IS NOT NULL AND %s IS NOT NULL LIMIT $2",
		xColumn, yColumn, tableName, xColumn, yColumn,
	)
	rows, err := config.Storage.Query(query, pct, limit)
	if err != nil {
		return nil, fmt.Errorf("scatter sample query: %w", err)
	}
	defer rows.Close()

	out := make([]ScatterPoint, 0, limit)
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

// ComputeHistogramFromDataset builds histograms with the requested bin count
// directly from the dataset table, bypassing the cached numeric_profile_histograms.
// Only columns named in numericColumns are processed.
func ComputeHistogramFromDataset(tableName string, numericColumns []string, bins int) (map[string][]models.HistogramBucket, error) {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table name: %q", tableName)
	}
	if bins < 1 {
		return nil, fmt.Errorf("bins must be >= 1, got %d", bins)
	}

	out := make(map[string][]models.HistogramBucket, len(numericColumns))
	for _, col := range numericColumns {
		if !sqlsafe.IsValidIdentifier(col) {
			return nil, fmt.Errorf("invalid column name: %q", col)
		}
		buckets, err := computeColumnHistogram(tableName, col, bins)
		if err != nil {
			return nil, fmt.Errorf("histogram for %q: %w", col, err)
		}
		out[col] = buckets
	}
	return out, nil
}

func computeColumnHistogram(tableName string, column string, bins int) ([]models.HistogramBucket, error) {
	var lo, hi sql.NullFloat64
	err := config.Storage.QueryRow(
		fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", column, column, tableName),
	).Scan(&lo, &hi)
	if err != nil {
		return nil, fmt.Errorf("min/max: %w", err)
	}
	if !lo.Valid || !hi.Valid {
		return []models.HistogramBucket{}, nil
	}
	if lo.Float64 == hi.Float64 {
		var count int64
		err := config.Storage.QueryRow(
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", tableName, column),
		).Scan(&count)
		if err != nil {
			return nil, err
		}
		return []models.HistogramBucket{{Min: lo.Float64, Max: hi.Float64, Count: count}}, nil
	}

	// WIDTH_BUCKET returns 1..bins for in-range values. We treat the upper
	// bound as inclusive by clamping any value equal to MAX into bin = bins.
	query := fmt.Sprintf(
		`SELECT LEAST(WIDTH_BUCKET(%s, $1, $2, $3), $3) AS b, COUNT(*)
		 FROM %s
		 WHERE %s IS NOT NULL
		 GROUP BY b
		 ORDER BY b`,
		column, tableName, column,
	)
	rows, err := config.Storage.Query(query, lo.Float64, hi.Float64, bins)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	width := (hi.Float64 - lo.Float64) / float64(bins)
	counts := make(map[int]int64)
	for rows.Next() {
		var bucket int
		var count int64
		if err := rows.Scan(&bucket, &count); err != nil {
			return nil, err
		}
		counts[bucket] = count
	}

	buckets := make([]models.HistogramBucket, bins)
	for i := range bins {
		buckets[i] = models.HistogramBucket{
			Min:   lo.Float64 + float64(i)*width,
			Max:   lo.Float64 + float64(i+1)*width,
			Count: counts[i+1],
		}
	}
	return buckets, nil
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

var validGroupByUnitsRepo = map[string]struct{}{"day": {}, "week": {}, "month": {}}

var validAggregateFnsRepo = map[string]struct{}{
	"AVG": {}, "SUM": {}, "MIN": {}, "MAX": {}, "COUNT": {},
}

// GetTimeseriesPlotGrouped returns rows truncated to groupByUnit on xColumn,
// with each yColumn aggregated via aggregateFn. groupByUnit must be one of
// day|week|month and aggregateFn must be one of AVG|SUM|MIN|MAX|COUNT — the
// caller is responsible for resolving these from user input. All identifiers
// are additionally validated as safe SQL identifiers.
func GetTimeseriesPlotGrouped(
	tableName string,
	xColumn string,
	yColumns []string,
	seriesColumn string,
	seriesValue string,
	groupByUnit string,
	aggregateFn string,
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
	if _, ok := validGroupByUnitsRepo[groupByUnit]; !ok {
		return nil, fmt.Errorf("invalid group-by unit: %q", groupByUnit)
	}
	if _, ok := validAggregateFnsRepo[aggregateFn]; !ok {
		return nil, fmt.Errorf("invalid aggregate function: %q", aggregateFn)
	}

	var query strings.Builder
	fmt.Fprintf(&query, "SELECT date_trunc('%s', %s) AS x", groupByUnit, xColumn)
	for _, y := range yColumns {
		fmt.Fprintf(&query, ", %s(%s) AS %s", aggregateFn, y, y)
	}
	fmt.Fprintf(&query, " FROM %s", tableName)
	args := []any{}
	if seriesColumn != "" {
		fmt.Fprintf(&query, " WHERE %s = $1", seriesColumn)
		args = append(args, seriesValue)
	}
	query.WriteString(" GROUP BY 1 ORDER BY 1 ASC")

	rows, err := config.Storage.Query(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("timeseries grouped query: %w", err)
	}
	defer rows.Close()

	colCount := 1 + len(yColumns)
	out := make([]map[string]any, 0)
	for rows.Next() {
		scanTargets := make([]any, colCount)
		holders := make([]any, colCount)
		for i := range scanTargets {
			holders[i] = &scanTargets[i]
		}
		if err := rows.Scan(holders...); err != nil {
			return nil, err
		}
		row := make(map[string]any, colCount)
		row["x"] = scanTargets[0]
		for i, y := range yColumns {
			row[y] = scanTargets[i+1]
		}
		out = append(out, row)
	}
	return out, nil
}
