package dataset

import (
	"fmt"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/models"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

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
