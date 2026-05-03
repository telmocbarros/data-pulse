package profiler

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

// StoreProfile persists a DatasetProfiler to the database.
// This runs outside of the dataset storage transaction — if it fails,
// the dataset is still saved and profiling can be retried.
func StoreProfile(datasetId string, profile *models.DatasetProfiler) error {
	for _, n := range profile.Numeric {
		if err := storeNumericProfile(datasetId, n); err != nil {
			return fmt.Errorf("error storing numeric profile for %q: %w", n.ColumnName, err)
		}
	}
	for _, c := range profile.Category {
		if err := storeCategoryProfile(datasetId, c); err != nil {
			return fmt.Errorf("error storing category profile for %q: %w", c.ColumnName, err)
		}
	}
	return nil
}

func storeNumericProfile(datasetId string, n *models.NumericProfiler) error {
	var profileId string
	err := config.Storage.QueryRow(
		`INSERT INTO numeric_profiles
		 (dataset_id, column_name, min, max, sum, count, mean, median, stdv, p25, p50, p75, null_count, null_percent)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		 RETURNING id`,
		datasetId, n.ColumnName,
		normaliseFloat(n.Min), normaliseFloat(n.Max), n.Sum, n.Count,
		n.Mean, n.Median, n.Stdv, n.P25, n.P50, n.P75,
		n.NullCount, n.NullPercent,
	).Scan(&profileId)
	if err != nil {
		return err
	}

	if len(n.Histogram) > 0 {
		if err := storeHistogram(profileId, n.Histogram); err != nil {
			return err
		}
	}

	if len(n.TypeDistribution) > 0 {
		if err := storeNumericTypeDistribution(profileId, n.TypeDistribution); err != nil {
			return err
		}
	}

	return nil
}

func storeHistogram(profileId string, buckets []models.HistogramBucket) error {
	rows := make([][]any, len(buckets))
	for i, b := range buckets {
		rows[i] = []any{profileId, b.Min, b.Max, b.Count}
	}
	return sqlsafe.BulkInsert(config.Storage, "numeric_profile_histograms",
		[]string{"numeric_profile_id", "bucket_min", "bucket_max", "count"}, rows)
}

func storeNumericTypeDistribution(profileId string, dist map[string]float64) error {
	rows := make([][]any, 0, len(dist))
	for typeName, count := range dist {
		rows = append(rows, []any{profileId, typeName, count})
	}
	return sqlsafe.BulkInsert(config.Storage, "numeric_profile_type_distributions",
		[]string{"numeric_profile_id", "type_name", "count"}, rows)
}

func storeCategoryProfile(datasetId string, c *models.CategoryProfiler) error {
	var profileId string
	err := config.Storage.QueryRow(
		`INSERT INTO category_profiles
		 (dataset_id, column_name, cardinality, uniqueness_ratio, uniqueness_unique_values, uniqueness_total_rows, null_count, null_percent)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING id`,
		datasetId, c.ColumnName,
		c.Cardinality, c.UniquenessRatio.Value,
		c.UniquenessRatio.UniqueValues, c.UniquenessRatio.TotalRows,
		c.NullCount, c.NullPercent,
	).Scan(&profileId)
	if err != nil {
		return err
	}

	if len(c.MostFrequentValues) > 0 {
		if err := storeFrequentValues(profileId, c.MostFrequentValues); err != nil {
			return err
		}
	}

	if len(c.TypeDistribution) > 0 {
		if err := storeCategoryTypeDistribution(profileId, c.TypeDistribution); err != nil {
			return err
		}
	}

	return nil
}

func storeFrequentValues(profileId string, values map[string]*models.Occurrence) error {
	rows := make([][]any, 0, len(values))
	for _, occ := range values {
		rows = append(rows, []any{profileId, occ.Value, occ.Count})
	}
	return sqlsafe.BulkInsert(config.Storage, "category_profile_frequent_values",
		[]string{"category_profile_id", "value", "count"}, rows)
}

func storeCategoryTypeDistribution(profileId string, dist map[string]float64) error {
	rows := make([][]any, 0, len(dist))
	for typeName, count := range dist {
		rows = append(rows, []any{profileId, typeName, count})
	}
	return sqlsafe.BulkInsert(config.Storage, "category_profile_type_distributions",
		[]string{"category_profile_id", "type_name", "count"}, rows)
}

// GetProfile retrieves a stored DatasetProfiler from the database.
func GetProfile(datasetId string) (*models.DatasetProfiler, error) {
	numericProfilers, err := getNumericProfiles(datasetId)
	if err != nil {
		return nil, err
	}

	categoryProfilers, err := getCategoryProfiles(datasetId)
	if err != nil {
		return nil, err
	}

	return &models.DatasetProfiler{
		Numeric:  numericProfilers,
		Category: categoryProfilers,
	}, nil
}

func getNumericProfiles(datasetId string) (map[string]*models.NumericProfiler, error) {
	rows, err := config.Storage.Query(
		`SELECT id, column_name, min, max, sum, count, mean, median, stdv, p25, p50, p75, null_count, null_percent
		 FROM numeric_profiles WHERE dataset_id = $1`, datasetId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	profiles := make(map[string]*models.NumericProfiler)
	for rows.Next() {
		var profileId string
		n := &models.NumericProfiler{}
		err := rows.Scan(&profileId, &n.ColumnName, &n.Min, &n.Max, &n.Sum, &n.Count,
			&n.Mean, &n.Median, &n.Stdv, &n.P25, &n.P50, &n.P75,
			&n.NullCount, &n.NullPercent)
		if err != nil {
			return nil, err
		}

		histogram, err := getHistogram(profileId)
		if err != nil {
			return nil, err
		}
		n.Histogram = histogram

		typeDist, err := getNumericTypeDistribution(profileId)
		if err != nil {
			return nil, err
		}
		n.TypeDistribution = typeDist

		profiles[n.ColumnName] = n
	}

	return profiles, nil
}

func getHistogram(profileId string) ([]models.HistogramBucket, error) {
	rows, err := config.Storage.Query(
		`SELECT bucket_min, bucket_max, count FROM numeric_profile_histograms
		 WHERE numeric_profile_id = $1 ORDER BY bucket_min`, profileId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []models.HistogramBucket
	for rows.Next() {
		var b models.HistogramBucket
		if err := rows.Scan(&b.Min, &b.Max, &b.Count); err != nil {
			return nil, err
		}
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func getNumericTypeDistribution(profileId string) (map[string]float64, error) {
	rows, err := config.Storage.Query(
		`SELECT type_name, count FROM numeric_profile_type_distributions
		 WHERE numeric_profile_id = $1`, profileId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dist := make(map[string]float64)
	for rows.Next() {
		var name string
		var count float64
		if err := rows.Scan(&name, &count); err != nil {
			return nil, err
		}
		dist[name] = count
	}
	return dist, nil
}

func getCategoryProfiles(datasetId string) (map[string]*models.CategoryProfiler, error) {
	rows, err := config.Storage.Query(
		`SELECT id, column_name, cardinality, uniqueness_ratio, uniqueness_unique_values, uniqueness_total_rows, null_count, null_percent
		 FROM category_profiles WHERE dataset_id = $1`, datasetId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	profiles := make(map[string]*models.CategoryProfiler)
	for rows.Next() {
		var profileId string
		c := &models.CategoryProfiler{}
		err := rows.Scan(&profileId, &c.ColumnName, &c.Cardinality,
			&c.UniquenessRatio.Value, &c.UniquenessRatio.UniqueValues, &c.UniquenessRatio.TotalRows,
			&c.NullCount, &c.NullPercent)
		if err != nil {
			return nil, err
		}

		freqValues, err := getFrequentValues(profileId)
		if err != nil {
			return nil, err
		}
		c.MostFrequentValues = freqValues

		typeDist, err := getCategoryTypeDistribution(profileId)
		if err != nil {
			return nil, err
		}
		c.TypeDistribution = typeDist

		profiles[c.ColumnName] = c
	}

	return profiles, nil
}

func getFrequentValues(profileId string) (map[string]*models.Occurrence, error) {
	rows, err := config.Storage.Query(
		`SELECT value, count FROM category_profile_frequent_values
		 WHERE category_profile_id = $1 ORDER BY count DESC`, profileId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make(map[string]*models.Occurrence)
	for rows.Next() {
		occ := &models.Occurrence{}
		if err := rows.Scan(&occ.Value, &occ.Count); err != nil {
			return nil, err
		}
		values[occ.Value] = occ
	}
	return values, nil
}

func getCategoryTypeDistribution(profileId string) (map[string]float64, error) {
	rows, err := config.Storage.Query(
		`SELECT type_name, count FROM category_profile_type_distributions
		 WHERE category_profile_id = $1`, profileId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dist := make(map[string]float64)
	for rows.Next() {
		var name string
		var count float64
		if err := rows.Scan(&name, &count); err != nil {
			return nil, err
		}
		dist[name] = count
	}
	return dist, nil
}

// normaliseFloat converts Inf values to nil-safe DB values.
func normaliseFloat(v float64) *float64 {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return nil
	}
	return &v
}

// CorrelationPair is one upper-triangle entry of the correlation matrix.
// PearsonR is nil when CORR() returns NULL (constant or all-null pair).
type CorrelationPair struct {
	ColumnA  string
	ColumnB  string
	PearsonR *float64
}

// ComputeCorrelationMatrix runs Postgres CORR() across every unordered pair of
// numeric columns in tableName and returns the upper-triangle results.
// Returns an empty slice when fewer than 2 columns are supplied.
func ComputeCorrelationMatrix(tableName string, numericColumns []string) ([]CorrelationPair, error) {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table name: %q", tableName)
	}
	cols := make([]string, 0, len(numericColumns))
	for _, c := range numericColumns {
		if !sqlsafe.IsValidIdentifier(c) {
			return nil, fmt.Errorf("invalid column name: %q", c)
		}
		cols = append(cols, c)
	}
	sort.Strings(cols)
	if len(cols) < 2 {
		return []CorrelationPair{}, nil
	}

	var query strings.Builder
	args := []any{}
	argIdx := 1
	first := true
	pairs := make([]CorrelationPair, 0, len(cols)*(len(cols)-1)/2)
	for i := 0; i < len(cols); i++ {
		for j := i + 1; j < len(cols); j++ {
			if !first {
				query.WriteString(" UNION ALL ")
			}
			first = false
			fmt.Fprintf(&query, "SELECT $%d::text AS a, $%d::text AS b, CORR(%s, %s) AS r FROM %s",
				argIdx, argIdx+1, cols[i], cols[j], tableName)
			args = append(args, cols[i], cols[j])
			argIdx += 2
			pairs = append(pairs, CorrelationPair{ColumnA: cols[i], ColumnB: cols[j]})
		}
	}

	rows, err := config.Storage.Query(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("compute correlation: %w", err)
	}
	defer rows.Close()

	results := make(map[[2]string]*float64, len(pairs))
	for rows.Next() {
		var a, b string
		var r sql.NullFloat64
		if err := rows.Scan(&a, &b, &r); err != nil {
			return nil, err
		}
		if r.Valid {
			v := r.Float64
			results[[2]string{a, b}] = &v
		} else {
			results[[2]string{a, b}] = nil
		}
	}
	for i := range pairs {
		pairs[i].PearsonR = results[[2]string{pairs[i].ColumnA, pairs[i].ColumnB}]
	}
	return pairs, nil
}

// GetCategoryBreakdown returns the most-frequent values for a categorical
// column, ordered by descending count and capped at limit.
func GetCategoryBreakdown(datasetId string, columnName string, limit int) ([]models.Occurrence, error) {
	rows, err := config.Storage.Query(
		`SELECT v.value, v.count
		 FROM category_profile_frequent_values v
		 JOIN category_profiles p ON p.id = v.category_profile_id
		 WHERE p.dataset_id = $1 AND p.column_name = $2
		 ORDER BY v.count DESC
		 LIMIT $3`,
		datasetId, columnName, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("read category breakdown: %w", err)
	}
	defer rows.Close()

	out := make([]models.Occurrence, 0)
	for rows.Next() {
		var occ models.Occurrence
		if err := rows.Scan(&occ.Value, &occ.Count); err != nil {
			return nil, err
		}
		out = append(out, occ)
	}
	return out, nil
}

// StoreCorrelationMatrix computes pairwise Pearson correlations for the given
// numeric columns and persists them. Existing rows for the dataset are replaced.
func StoreCorrelationMatrix(datasetId string, tableName string, numericColumns []string) error {
	pairs, err := ComputeCorrelationMatrix(tableName, numericColumns)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}

	if _, err := config.Storage.Exec(
		"DELETE FROM correlation_matrices WHERE dataset_id = $1", datasetId,
	); err != nil {
		return fmt.Errorf("clear existing correlation rows: %w", err)
	}

	rows := make([][]any, len(pairs))
	for i, p := range pairs {
		rows[i] = []any{datasetId, p.ColumnA, p.ColumnB, p.PearsonR}
	}
	if err := sqlsafe.BulkInsert(config.Storage, "correlation_matrices",
		[]string{"dataset_id", "column_a", "column_b", "pearson_r"}, rows); err != nil {
		return fmt.Errorf("insert correlation rows: %w", err)
	}
	return nil
}
