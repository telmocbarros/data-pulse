package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/columntype"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	"golang.org/x/sync/errgroup"
)

type numberedJsonRow struct {
	Row  int32
	Data map[string]any
}

// jsonPipelineState mirrors csvPipelineState; see that struct's doc for the
// cancellation contract (ctx/cancel/parserExited).
type jsonPipelineState struct {
	ctx          context.Context
	cancel       context.CancelFunc
	parserCh     chan numberedJsonRow
	errorsCh     chan ValidationError
	parserExited chan struct{}
	columnKeys   []string
	columnTypes  map[string]string
	tableName    string
	datasetId    string
	firstRow     map[string]any
}

// ProcessJsonFile runs the full JSON pipeline synchronously.
// It is meant to be called from within a job goroutine.
func ProcessJsonFile(ctx context.Context, f io.Reader, fileName string, fileSize int64, progressFn func(int)) (string, error) {
	state, err := parseJsonFile(ctx, f, fileName, fileSize)
	if err != nil {
		return "", err
	}
	progressFn(10)
	return state.datasetId, runJsonPipeline(ctx, state, progressFn)
}

// parseJsonFile reads the file, creates metadata, and runs Stage 1 (parsing).
// The file is fully consumed when this function returns.
func parseJsonFile(ctx context.Context, f io.Reader, fileName string, fileSize int64) (*jsonPipelineState, error) {
	decoder := json.NewDecoder(f)

	// Consume opening '[' of the array
	tok, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("expected opening '[': %w", err)
	}

	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", tok)
	}

	// 1. Decode first row and extract column types
	if !decoder.More() {
		return nil, fmt.Errorf("empty JSON array")
	}

	var firstRow map[string]any
	if err := decoder.Decode(&firstRow); err != nil {
		return nil, fmt.Errorf("error decoding first row: %w", err)
	}

	ReadJsonRowAndExtractType(firstRow)

	columnKeys := make([]string, 0, len(firstRow))
	for k := range firstRow {
		columnKeys = append(columnKeys, k)
	}
	columnTypes := make(map[string]string, len(firstRow))
	for k, v := range firstRow {
		columnTypes[k] = columntype.FromGo(v)
	}

	datasetColumns := extractColumns(firstRow)

	// 2. Create table and store metadata
	tableName, err := repository.CreateDatasetTable("json", datasetColumns)
	if err != nil {
		return nil, fmt.Errorf("error while attempting to create a table: %w", err)
	}

	metadata := map[string]any{
		"name":        fileName,
		"tableName":   tableName,
		"size":        fileSize,
		"author":      "engineering",
		"description": "description",
	}

	datasetId, err := repository.StoreDatasetMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("error adding dataset metadata: %w", err)
	}

	if err = repository.StoreDatasetColumns(datasetColumns, datasetId); err != nil {
		return nil, fmt.Errorf("error adding dataset columns: %w", err)
	}

	// 3. Stage 1: Parse — decode JSON objects and send them downstream.
	// pipelineCtx is cancellable so a downstream failure (e.g. storage error)
	// can unblock the parser if it's mid-send.
	pipelineCtx, cancel := context.WithCancel(ctx)
	errorsCh := make(chan ValidationError, 100)
	parserCh := make(chan numberedJsonRow, 100)
	parserExited := make(chan struct{})

	go func() {
		defer close(parserExited)
		defer close(parserCh)

		rowNumber := int32(1) // row 0 was used for type extraction
		for decoder.More() {
			select {
			case <-pipelineCtx.Done():
				return
			default:
			}

			var row map[string]any
			if err := decoder.Decode(&row); err != nil {
				select {
				case errorsCh <- ValidationError{
					Row:    rowNumber,
					Column: -1,
					Kind:   "malformed_row",
					Detail: err.Error(),
				}:
				case <-pipelineCtx.Done():
					return
				}
				rowNumber++
				continue
			}

			ReadJsonRowAndExtractType(row)
			select {
			case parserCh <- numberedJsonRow{Row: rowNumber, Data: row}:
			case <-pipelineCtx.Done():
				return
			}
			rowNumber++
		}
	}()

	return &jsonPipelineState{
		ctx:          pipelineCtx,
		cancel:       cancel,
		parserCh:     parserCh,
		errorsCh:     errorsCh,
		parserExited: parserExited,
		columnKeys:   columnKeys,
		columnTypes:  columnTypes,
		tableName:    tableName,
		datasetId:    datasetId,
		firstRow:     firstRow,
	}, nil
}

// runJsonPipeline runs Stages 2 (validate) and 3 (store) of the JSON pipeline.
// See runCsvPipeline for the cancellation contract — this mirrors it.
func runJsonPipeline(ctx context.Context, state *jsonPipelineState, progressFn func(int)) error {
	defer state.cancel()

	// Only the error-collector writes validationErrors, and we read it after
	// g.Wait() returns (collector has exited by then), so no mutex needed.
	var validationErrors []ValidationError

	dataCh := make(chan map[string]any, 100)

	// Send first row straight to dataCh (already validated during setup).
	// Safe before goroutines start because dataCh has capacity 100.
	dataCh <- state.firstRow

	g, gctx := errgroup.WithContext(state.ctx)

	// Propagate gctx cancellation back to state.ctx so the parser goroutine
	// (which only watches state.ctx) unblocks when any pipeline stage errors.
	go func() {
		<-gctx.Done()
		state.cancel()
	}()

	g.Go(func() error {
		for ve := range state.errorsCh {
			validationErrors = append(validationErrors, ve)
		}
		return nil
	})

	g.Go(func() error {
		return uploadJsonDataset(gctx, dataCh, state.tableName, state.datasetId, progressFn)
	})

	validatorExited := make(chan struct{})
	go func() {
		<-state.parserExited
		<-validatorExited
		close(state.errorsCh)
	}()

	g.Go(func() error {
		defer close(dataCh)
		defer close(validatorExited)

		progressFn(30)

		for nr := range state.parserCh {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			for _, k := range state.columnKeys {
				v, exists := nr.Data[k]
				if !exists {
					select {
					case state.errorsCh <- ValidationError{
						Row:    nr.Row,
						Column: -1,
						Kind:   "missing_value",
						Detail: fmt.Sprintf("missing column %q", k),
					}:
					case <-gctx.Done():
						return gctx.Err()
					}
					continue
				}
				varType := columntype.FromGo(v)
				if varType != state.columnTypes[k] {
					select {
					case state.errorsCh <- ValidationError{
						Row:      nr.Row,
						Column:   -1,
						Kind:     "type_mismatch",
						Expected: state.columnTypes[k],
						Received: varType,
						Detail:   fmt.Sprintf("column %q", k),
					}:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
			}

			select {
			case dataCh <- nr.Data:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if len(validationErrors) > 0 {
		if err := repository.StoreValidationErrors(state.datasetId, validationErrors); err != nil {
			slog.Error("store validation errors failed", "err", err, "datasetId", state.datasetId)
		}
	}

	return nil
}

// uploadJsonDataset drains dataCh into a transactional batch insert.
// Returns the first error encountered (transaction begin, batch insert,
// commit, or context cancellation). On error the transaction is rolled back.
func uploadJsonDataset(ctx context.Context, dataCh chan map[string]any, tableName string, datasetId string, progressFn func(int)) error {
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
