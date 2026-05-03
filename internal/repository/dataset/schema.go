package dataset

import (
	"log/slog"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

// StoreDatasetColumns inserts one row per (column_name, column_type) for
// the given dataset id into dataset_columns.
func StoreDatasetColumns(columns [][]string, datasetId string) error {
	rows := make([][]any, len(columns))
	for i, col := range columns {
		rows[i] = []any{datasetId, col[0], col[1]}
	}
	if err := sqlsafe.BulkInsert(config.Storage, "dataset_columns",
		[]string{"dataset_id", "column_name", "column_type"}, rows); err != nil {
		slog.Error("StoreDatasetColumns insert failed", "err", err, "datasetId", datasetId)
		return err
	}
	return nil
}
