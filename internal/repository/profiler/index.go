package profiler

import (
	"fmt"
	"math"
	"strings"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
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
	var query strings.Builder
	query.WriteString("INSERT INTO numeric_profile_histograms (numeric_profile_id, bucket_min, bucket_max, count) VALUES ")
	vals := []any{}
	for i, b := range buckets {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d)", i*4+1, i*4+2, i*4+3, i*4+4))
		vals = append(vals, profileId, b.Min, b.Max, b.Count)
	}

	_, err := config.Storage.Exec(query.String(), vals...)
	return err
}

func storeNumericTypeDistribution(profileId string, dist map[string]float64) error {
	var query strings.Builder
	query.WriteString("INSERT INTO numeric_profile_type_distributions (numeric_profile_id, type_name, count) VALUES ")
	vals := []any{}
	i := 0
	for typeName, count := range dist {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		vals = append(vals, profileId, typeName, count)
		i++
	}

	_, err := config.Storage.Exec(query.String(), vals...)
	return err
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
	var query strings.Builder
	query.WriteString("INSERT INTO category_profile_frequent_values (category_profile_id, value, count) VALUES ")
	vals := []any{}
	i := 0
	for _, occ := range values {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		vals = append(vals, profileId, occ.Value, occ.Count)
		i++
	}

	_, err := config.Storage.Exec(query.String(), vals...)
	return err
}

func storeCategoryTypeDistribution(profileId string, dist map[string]float64) error {
	var query strings.Builder
	query.WriteString("INSERT INTO category_profile_type_distributions (category_profile_id, type_name, count) VALUES ")
	vals := []any{}
	i := 0
	for typeName, count := range dist {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		vals = append(vals, profileId, typeName, count)
		i++
	}

	_, err := config.Storage.Exec(query.String(), vals...)
	return err
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
