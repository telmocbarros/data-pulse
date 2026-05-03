package dataset

import (
	"context"
	"fmt"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

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
