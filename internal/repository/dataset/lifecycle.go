package dataset

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

// StoreDataset inserts the given rows into the dataset's dynamic table.
// Column names are inferred from the first row's keys; "created_at" is
// remapped to "entry_date" (which is the schema column name). All
// identifiers are validated as safe before being interpolated into SQL.
func StoreDataset(dbExecutor config.Executor, tableName string, datasetId string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	if !sqlsafe.IsValidIdentifier(tableName) {
		return fmt.Errorf("invalid table name: %q", tableName)
	}
	// Extract column names from the first row, sorted so the INSERT
	// column list is deterministic across batches and uploads. Map
	// iteration order is randomized, so without sorting two batches
	// in the same upload could pick different orderings — the values
	// would still bind correctly per-row, but the SQL text would
	// drift, defeating prepared-statement reuse and complicating
	// debugging.
	keys := make([]string, 0, len(rows[0]))
	for key := range rows[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	columns := make([]string, 0, len(keys)+2)
	columns = append(columns, "id", "dataset_id")
	for _, key := range keys {
		if key == "created_at" {
			columns = append(columns, "entry_date")
		} else {
			columns = append(columns, key)
		}
	}
	for _, c := range columns {
		if !sqlsafe.IsValidIdentifier(c) {
			return fmt.Errorf("invalid column name: %q", c)
		}
	}
	numCols := len(columns)
	// build column list: (id, dataset_id, col1, col2, ...)
	colList := strings.Join(columns, ", ")

	// build query with dynamic placeholders
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, colList)
	vals := []any{}
	for i, row := range rows {
		var placeholders strings.Builder
		placeholders.WriteString("(")
		for j := range numCols {
			if j > 0 {
				placeholders.WriteString(", ")
			}
			fmt.Fprintf(&placeholders, "$%d", i*numCols+j+1)
		}
		placeholders.WriteString("),")
		query += placeholders.String()

		vals = append(vals, uuid.New().String(), datasetId)
		for _, col := range columns[2:] {
			if col == "entry_date" {
				vals = append(vals, row["created_at"])
			} else {
				vals = append(vals, row[col])
			}
		}
	}
	query = query[:len(query)-1] // trim trailing comma

	if _, err := dbExecutor.Exec(query, vals...); err != nil {
		slog.Error("StoreDataset insert failed", "err", err, "table", tableName)
		return err
	}
	return nil
}

// CreateDatasetTable creates a per-dataset table named
// "<fileExtension>_datasets_<uuid>" with the supplied columns. Returns the
// generated table name. fileExtension and column names are validated.
func CreateDatasetTable(fileExtension string, columns [][]string) (string, error) {
	if !sqlsafe.IsValidIdentifier(fileExtension) {
		return "", fmt.Errorf("invalid file extension: %q", fileExtension)
	}
	for _, col := range columns {
		if !sqlsafe.IsValidIdentifier(col[0]) {
			return "", fmt.Errorf("invalid column name: %q", col[0])
		}
	}

	tableName := fmt.Sprintf("%s_datasets_%s", fileExtension, strings.ReplaceAll(uuid.New().String(), "-", ""))

	var query strings.Builder
	fmt.Fprintf(&query, "CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), dataset_id UUID, ", tableName)
	for j, col := range columns {
		if j > 0 {
			query.WriteString(", ")
		}
		name := col[0]
		if name == "created_at" {
			name = "entry_date"
		}
		fmt.Fprintf(&query, "%s %s", name, mapToDatabase(col[1]))
	}
	query.WriteString(")")

	_, err := config.Storage.Exec(query.String())
	if err != nil {
		return "", fmt.Errorf("unable to create table: %w", err)
	}

	return tableName, nil
}

// StoreDatasetMetadata inserts a row into the datasets table and returns
// the generated dataset id. The metadata map carries name, tableName,
// author, size, and description.
func StoreDatasetMetadata(metadata map[string]any) (string, error) {
	query := "INSERT INTO datasets (file_name, table_name, uploaded_by, size, description) VALUES ($1, $2, $3, $4, $5) RETURNING id"

	var id string
	err := config.Storage.QueryRow(query, metadata["name"], metadata["tableName"], metadata["author"], metadata["size"], metadata["description"]).Scan(&id)
	if err != nil {
		slog.Error("StoreDatasetMetadata insert failed", "err", err)
		return "", err
	}
	return id, nil
}

// SoftDeleteDataset marks a dataset as deleted by setting deleted_at = NOW().
// Returns ErrNotFound if no row was affected (dataset missing or already
// deleted).
func SoftDeleteDataset(id string) error {
	res, err := config.Storage.Exec(
		`UPDATE datasets SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL`,
		id,
	)
	if err != nil {
		return fmt.Errorf("soft delete dataset: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ListDatasets returns metadata for all alive (non-soft-deleted) datasets.
func ListDatasets() ([]map[string]any, error) {
	rows, err := config.Storage.Query(
		`SELECT id, file_name, table_name, size, uploaded_by, description, created_at
		 FROM datasets WHERE deleted_at IS NULL ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var datasets []map[string]any
	for rows.Next() {
		var id, fileName, tableName, uploadedBy, description string
		var size int64
		var createdAt any
		if err := rows.Scan(&id, &fileName, &tableName, &size, &uploadedBy, &description, &createdAt); err != nil {
			return nil, err
		}
		datasets = append(datasets, map[string]any{
			"id":          id,
			"file_name":   fileName,
			"table_name":  tableName,
			"size":        size,
			"uploaded_by": uploadedBy,
			"description": description,
			"created_at":  createdAt,
		})
	}
	return datasets, nil
}

// mapToDatabase converts a columntype tag to the SQL column type used by
// CreateDatasetTable.
func mapToDatabase(value string) string {
	switch value {
	case columntype.Numerical:
		return "DOUBLE PRECISION"
	case columntype.Boolean:
		return "BOOLEAN"
	case columntype.Date:
		return "TIMESTAMP"
	case columntype.Categorical:
		return "TEXT"
	default:
		return "TEXT"
	}
}
