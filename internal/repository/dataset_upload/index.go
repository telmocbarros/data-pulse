package repository

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

func StoreDataset(dbExecutor config.Executor, tableName string, datasetId string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	if !sqlsafe.IsValidIdentifier(tableName) {
		return fmt.Errorf("invalid table name: %q", tableName)
	}
	// extract column names from the first row
	columns := make([]string, 0, len(rows[0])+1)
	columns = append(columns, "id", "dataset_id")
	for key := range rows[0] {
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

	result, err := dbExecutor.Exec(query, vals...)
	if err != nil {
		fmt.Println("unable to execute insert query", err)
		return err
	}

	fmt.Println("Successfully executed the query: ", result)
	return nil
}

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

func StoreDatasetMetadata(metadata map[string]any) (string, error) {
	query := "INSERT INTO datasets (file_name, table_name, uploaded_by, size, description) VALUES ($1, $2, $3, $4, $5) RETURNING id"

	var id string
	err := config.Storage.QueryRow(query, metadata["name"], metadata["tableName"], metadata["author"], metadata["size"], metadata["description"]).Scan(&id)
	if err != nil {
		fmt.Println("unable to execute insert query", err)
		return "", err
	}
	fmt.Println("StoreDatasetMetadata: ", id)
	return id, nil
}

func StoreDatasetColumns(columns [][]string, datasetId string) error {
	rows := make([][]any, len(columns))
	for i, col := range columns {
		rows[i] = []any{datasetId, col[0], col[1]}
	}
	if err := sqlsafe.BulkInsert(config.Storage, "dataset_columns",
		[]string{"dataset_id", "column_name", "column_type"}, rows); err != nil {
		fmt.Println("unable to insert dataset columns", err)
		return err
	}
	return nil
}

// SoftDeleteDataset marks a dataset as deleted by setting deleted_at = NOW().
// Returns an error if no row was affected (dataset missing or already deleted).
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
		return fmt.Errorf("dataset not found")
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

// GetDatasetRows reads all rows from a dataset's dynamic table.
//
// Deprecated: use StreamDatasetRows. Slated for removal during the
// dataset_upload package merge.
func GetDatasetRows(tableName string) ([]map[string]any, error) {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table name: %q", tableName)
	}
	rows, err := config.Storage.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("unable to query dataset table: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	return result, nil
}

// StreamDatasetRows streams rows from tableName into rowCh row-by-row.
// The caller owns rowCh and is responsible for closing it (typically via
// `defer close(rowCh)` in the producer goroutine that invokes this function).
// Uses QueryContext so a cancellation of ctx aborts the in-flight query and
// the row scan loop.
func StreamDatasetRows(ctx context.Context, tableName string, rowCh chan<- map[string]any) error {
	if !sqlsafe.IsValidIdentifier(tableName) {
		return fmt.Errorf("invalid table name: %q", tableName)
	}
	rows, err := config.Storage.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return fmt.Errorf("unable to query dataset table: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		select {
		case rowCh <- row:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

// StoreRawFile uploads a file to MinIO under the datasets bucket.
func StoreRawFile(datasetId string, filePath string, fileName string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat file: %w", err)
	}

	objectKey := fmt.Sprintf("%s/%s", datasetId, fileName)
	_, err = config.FileStorage.PutObject(
		context.Background(),
		config.DatasetsBucket,
		objectKey,
		f,
		info.Size(),
		minio.PutObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("unable to upload file to MinIO: %w", err)
	}

	return nil
}

// validationErrorBatchSize caps rows-per-INSERT to stay well under Postgres's
// 65535-parameter limit (7 params per row × 500 = 3500 << 65535).
const validationErrorBatchSize = 500

// StoreValidationErrors bulk-inserts validation errors for a dataset.
// Errors are chunked at validationErrorBatchSize per INSERT.
func StoreValidationErrors(datasetId string, errs []models.ValidationError) error {
	if len(errs) == 0 {
		return nil
	}
	for start := 0; start < len(errs); start += validationErrorBatchSize {
		end := min(start+validationErrorBatchSize, len(errs))
		if err := storeValidationErrorChunk(datasetId, errs[start:end]); err != nil {
			return fmt.Errorf("store validation errors chunk [%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

func storeValidationErrorChunk(datasetId string, chunk []models.ValidationError) error {
	rows := make([][]any, len(chunk))
	for i, e := range chunk {
		rows[i] = []any{datasetId, e.Row, e.Column, e.Kind, e.Expected, e.Received, e.Detail}
	}
	return sqlsafe.BulkInsert(config.Storage, "dataset_validation_errors",
		[]string{"dataset_id", "row_number", "column_index", "kind", "expected", "received", "detail"},
		rows)
}

// ListValidationErrors returns a page of validation errors for the dataset,
// ordered by id ascending (insertion order). Pass limit and offset for paging.
func ListValidationErrors(datasetId string, limit int, offset int) ([]models.ValidationError, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := config.Storage.Query(
		`SELECT row_number, column_index, kind, expected, received, detail
		 FROM dataset_validation_errors
		 WHERE dataset_id = $1
		 ORDER BY id ASC
		 LIMIT $2 OFFSET $3`,
		datasetId, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list validation errors: %w", err)
	}
	defer rows.Close()

	out := make([]models.ValidationError, 0)
	for rows.Next() {
		var ve models.ValidationError
		if err := rows.Scan(&ve.Row, &ve.Column, &ve.Kind, &ve.Expected, &ve.Received, &ve.Detail); err != nil {
			return nil, err
		}
		out = append(out, ve)
	}
	return out, nil
}

func mapToDatabase(value string) string {
	switch value {
	case "IS_NUMERICAL":
		return "DOUBLE PRECISION"
	case "IS_BOOLEAN":
		return "BOOLEAN"
	case "IS_DATE":
		return "TIMESTAMP"
	case "IS_CATEGORICAL":
		return "TEXT"
	default:
		return "TEXT"
	}
}
