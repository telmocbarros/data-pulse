package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
	"golang.org/x/sync/errgroup"
)

type numberedRecord struct {
	Row    int32
	Record []string
}

// csvPipelineState holds everything Stages 2+3 need after the file has been
// fully parsed. ctx is a cancellable child of the caller's context shared by
// the parser goroutine (already running) and the validator/storage goroutines
// started in runCsvPipeline. cancel is invoked by runCsvPipeline on its way
// out so the parser unblocks if it's still mid-send when the pipeline exits.
// parserExited is closed by the parser goroutine when it stops, so the
// pipeline knows when it's safe to close errorsCh.
type csvPipelineState struct {
	ctx           context.Context
	cancel        context.CancelFunc
	parserCh      chan numberedRecord
	errorsCh      chan ValidationError
	parserExited  chan struct{}
	headers       []string
	rowFieldTypes []string
	tableName     string
	datasetId     string
}

// ProcessCsvFile runs the full CSV pipeline synchronously.
// It is meant to be called from within a job goroutine.
func ProcessCsvFile(ctx context.Context, f io.Reader, fileName string, fileSize int64, progressFn func(int)) (string, error) {
	state, err := parseCsvFile(ctx, f, fileName, fileSize)
	if err != nil {
		return "", err
	}
	progressFn(10)
	return state.datasetId, runCsvPipeline(ctx, state, progressFn)
}

// parseCsvFile reads the file, creates metadata, and runs Stage 1 (parsing).
// It returns a csvPipelineState that contains the channels for Stages 2+3.
// The file is fully consumed when this function returns.
func parseCsvFile(ctx context.Context, f io.Reader, fileName string, fileSize int64) (*csvPipelineState, error) {
	csvReader := csv.NewReader(f)

	// 1. Read the file
	headers, err := csvReader.Read()
	if err != nil {
		slog.Error("read csv header failed", "err", err)
		return nil, err
	}

	// 2. extract the row field types based on the first row with data
	content, row_field_types, err := ReadCsvRowAndExtractType(csvReader)
	if err != nil {
		slog.Error("extract csv row field types failed", "err", err)
		return nil, err
	}
	jsonObj := make(map[string]any)

	// 3. Parsing the values of the first row
	for idx, value := range content {
		jsonObj[headers[idx]] = columntype.Parse(value)
	}
	datasetColumns := extractColumns(jsonObj)
	// allow csv.Reader to handle rows with wrong field count
	// instead of returning an error
	csvReader.FieldsPerRecord = -1

	// 4. Creating the metadata tables and the dataset's table
	tableName, err := repository.CreateDatasetTable("csv", datasetColumns)
	if err != nil {
		return nil, fmt.Errorf("while attempting to create a table: %w", err)
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
		return nil, fmt.Errorf("adding dataset metadata: %w", err)
	}

	if err = repository.StoreDatasetColumns(datasetColumns, datasetId); err != nil {
		return nil, fmt.Errorf("adding dataset columns: %w", err)
	}

	// 5. Stage 1: Parse — read raw CSV rows and send them downstream.
	// pipelineCtx is cancellable so a downstream failure (e.g. storage error)
	// can unblock the parser if it's mid-send.
	pipelineCtx, cancel := context.WithCancel(ctx)
	errorsCh := make(chan ValidationError, 100)
	parserCh := make(chan numberedRecord, 100)
	parserExited := make(chan struct{})
	expectedColumns := len(row_field_types)

	go func() {
		defer close(parserExited)
		defer close(parserCh)

		rowNumber := int32(2) // row 1 was used for type extraction
		for {
			select {
			case <-pipelineCtx.Done():
				return
			default:
			}

			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
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

			if len(record) != expectedColumns {
				select {
				case errorsCh <- ValidationError{
					Row:      rowNumber,
					Column:   -1,
					Kind:     "malformed_row",
					Expected: fmt.Sprintf("%d columns", expectedColumns),
					Received: fmt.Sprintf("%d columns", len(record)),
				}:
				case <-pipelineCtx.Done():
					return
				}
			}

			select {
			case parserCh <- numberedRecord{Row: rowNumber, Record: record}:
			case <-pipelineCtx.Done():
				return
			}
			rowNumber++
		}
	}()

	return &csvPipelineState{
		ctx:           pipelineCtx,
		cancel:        cancel,
		parserCh:      parserCh,
		errorsCh:      errorsCh,
		parserExited:  parserExited,
		headers:       headers,
		rowFieldTypes: row_field_types,
		tableName:     tableName,
		datasetId:     datasetId,
	}, nil
}

// runCsvPipeline runs Stages 2 (validate) and 3 (store) of the CSV pipeline.
// It does not need the file — it consumes from the channels in csvPipelineState.
//
// Cancellation: g, gctx := errgroup.WithContext(state.ctx). If the storage
// goroutine returns an error, gctx is cancelled; the validator's <-gctx.Done()
// branches fire and it returns, closing dataCh. defer state.cancel() at the
// top guarantees the parser goroutine — which only watches state.ctx — also
// unblocks, since state.ctx is gctx's parent.
func runCsvPipeline(ctx context.Context, state *csvPipelineState, progressFn func(int)) error {
	defer state.cancel()

	// Only the error-collector writes validationErrors, and we read it after
	// g.Wait() returns (collector has exited by then), so no mutex needed.
	var validationErrors []ValidationError

	dataCh := make(chan map[string]any, 100)
	expectedColumns := len(state.rowFieldTypes)

	g, gctx := errgroup.WithContext(state.ctx)

	// Propagate gctx cancellation back to state.ctx so the parser goroutine
	// (which only watches state.ctx) unblocks when any pipeline stage errors.
	// state.ctx is gctx's parent, so it isn't cancelled automatically.
	go func() {
		<-gctx.Done()
		state.cancel()
	}()

	// Error collector — drains errorsCh into validationErrors. Never fails;
	// errorsCh is closed once both producers (parser + validator) finish.
	g.Go(func() error {
		for ve := range state.errorsCh {
			validationErrors = append(validationErrors, ve)
		}
		return nil
	})

	// Stage: Store — batches rows from dataCh and writes to DB. Returning a
	// non-nil error here cancels gctx and unblocks the validator.
	g.Go(func() error {
		return uploadJsonDataset(gctx, dataCh, state.tableName, state.datasetId, progressFn)
	})

	// validatorExited signals the validator has stopped writing to errorsCh.
	// Combined with state.parserExited (closed by the parser on its way out),
	// these two are the signal that no one will write to errorsCh again, so
	// it's safe to close. The closer goroutine waits for both before closing.
	validatorExited := make(chan struct{})
	go func() {
		<-state.parserExited
		<-validatorExited
		close(state.errorsCh)
	}()

	// Stage: Validate — check types and missing values, forward valid rows to dataCh.
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

			jsonResult := make(map[string]any)
			for idx, value := range nr.Record {
				if value == "" {
					select {
					case state.errorsCh <- ValidationError{
						Row:    nr.Row,
						Column: int32(idx),
						Kind:   "missing_value",
					}:
					case <-gctx.Done():
						return gctx.Err()
					}
					continue
				}

				if idx < expectedColumns {
					variableType := columntype.Classify(value)
					if state.rowFieldTypes[idx] != variableType {
						select {
						case state.errorsCh <- ValidationError{
							Row:      nr.Row,
							Column:   int32(idx),
							Kind:     "type_mismatch",
							Expected: state.rowFieldTypes[idx],
							Received: variableType,
						}:
						case <-gctx.Done():
							return gctx.Err()
						}
					}
				}
				jsonResult[state.headers[idx]] = columntype.Parse(value)
			}

			select {
			case dataCh <- jsonResult:
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
			// Persistence failure shouldn't fail the upload — the data is in.
			slog.Error("store validation errors failed", "err", err, "datasetId", state.datasetId)
		}
	}

	return nil
}
