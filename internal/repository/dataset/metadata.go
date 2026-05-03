package dataset

import (
	"fmt"

	"github.com/telmocbarros/data-pulse/config"
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
