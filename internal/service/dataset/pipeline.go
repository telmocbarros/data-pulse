package dataset

import (
	"context"
	"fmt"

	"github.com/telmocbarros/data-pulse/config"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
)

// uploadDataset drains dataCh into a transactional batch insert.
// Returns the first error encountered (transaction begin, batch insert,
// commit, or context cancellation). On error the transaction is rolled back.
//
// Format-agnostic: dataCh carries map[string]any rows whose keys match the
// dataset's column names; the upload doesn't care whether the source was
// CSV, JSON, or anything else.
func uploadDataset(ctx context.Context, dataCh chan map[string]any, tableName string, datasetId string, progressFn func(int)) error {
	tx, err := config.Storage.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	batchSize := 50
	batch := make([]map[string]any, 0, batchSize)
	batchCount := 0

	for row := range dataCh {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := repository.StoreDataset(tx, tableName, datasetId, batch); err != nil {
				return fmt.Errorf("persist batch: %w", err)
			}
			batchCount++
			progress := 40 + min(batchCount*5, 55)
			progressFn(progress)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := repository.StoreDataset(tx, tableName, datasetId, batch); err != nil {
			return fmt.Errorf("persist final batch: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
