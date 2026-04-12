package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"sync"

	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
)

type numberedRecord struct {
	Row    int32
	Record []string
}

// csvPipelineState holds everything Stages 2+3 need after the file has been fully parsed.
type csvPipelineState struct {
	parserCh      chan numberedRecord
	errorsCh      chan ValidationError
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
		log.Println("Header format is invalid: ", err)
		return nil, err
	}
	log.Println("File headers: ", headers)

	// 2. extract the row filed types based on the first row with data
	content, row_field_types, err := ReadCsvRowAndExtractType(csvReader)
	if err != nil {
		log.Println("Something went wrong when extracting the row field types: ", err)
		return nil, err
	}
	jsonObj := make(map[string]any)

	// 3. Parsing the values of the first row
	for idx, value := range content {
		jsonObj[headers[idx]] = ParseValue(value)
	}

	log.Println("Column Types: ", row_field_types)
	datasetColumns := extractColumns(jsonObj)
	// allow csv.Reader to handle rows with wrong field count
	// instead of returning an error
	csvReader.FieldsPerRecord = -1

	// 4. Creating the metadata tables and the dataset's table
	tableName, err := repository.CreateDatasetTable("csv", datasetColumns)
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

	// 5. Stage 1: Parse — read raw CSV rows and send them downstream
	errorsCh := make(chan ValidationError, 100)
	parserCh := make(chan numberedRecord, 100)
	expectedColumns := len(row_field_types)

	go func() {
		defer close(parserCh)

		rowNumber := int32(2) // row 1 was used for type extraction
		for {
			select {
			case <-ctx.Done():
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
				case <-ctx.Done():
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
				case <-ctx.Done():
					return
				}
			}

			parserCh <- numberedRecord{Row: rowNumber, Record: record}
			rowNumber++
		}
	}()

	return &csvPipelineState{
		parserCh:      parserCh,
		errorsCh:      errorsCh,
		headers:       headers,
		rowFieldTypes: row_field_types,
		tableName:     tableName,
		datasetId:     datasetId,
	}, nil
}

// runCsvPipeline runs Stages 2 (validate) and 3 (store) of the CSV pipeline.
// It does not need the file — it consumes from the channels in csvPipelineState.
func runCsvPipeline(ctx context.Context, state *csvPipelineState, progressFn func(int)) error {
	var validationErrors []ValidationError

	dataCh := make(chan map[string]any, 100)
	expectedColumns := len(state.rowFieldTypes)

	var wg sync.WaitGroup

	// Error collector — drains errorsCh into the returned validationErrors slice
	wg.Go(func() {
		for ve := range state.errorsCh {
			validationErrors = append(validationErrors, ve)
		}
	})

	// Stage 3: Store — batches rows from dataCh and writes to DB
	wg.Go(func() {
		uploadJsonDataset(ctx, dataCh, state.tableName, state.datasetId, progressFn)
	})

	// errWg tracks goroutines that write to errorsCh so we can
	// close it safely after both the parser and validator finish.
	var errWg sync.WaitGroup

	// Stage 2: Validate — check types and missing values, forward valid rows to dataCh
	errWg.Go(func() {
		defer close(dataCh)

		progressFn(30)

		for nr := range state.parserCh {
			select {
			case <-ctx.Done():
				return
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
					case <-ctx.Done():
						return
					}
					continue
				}

				if idx < expectedColumns {
					variableType, err := ComputeVariableType(value)
					if err != nil {
						fmt.Println("Error retrieving cell variable type: ", err)
						return
					}
					if state.rowFieldTypes[idx] != variableType {
						select {
						case state.errorsCh <- ValidationError{
							Row:      nr.Row,
							Column:   int32(idx),
							Kind:     "type_mismatch",
							Expected: state.rowFieldTypes[idx],
							Received: variableType,
						}:
						case <-ctx.Done():
							return
						}
					}
				}
				jsonResult[state.headers[idx]] = ParseValue(value)
			}

			select {
			case dataCh <- jsonResult:
			case <-ctx.Done():
				return
			}
		}
	})

	// Close errorsCh once both producers (parser + validator) are done
	go func() {
		errWg.Wait()
		close(state.errorsCh)
	}()

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if len(validationErrors) > 0 {
		fmt.Printf("Completed with %d validation errors\n", len(validationErrors))
	}

	return nil
}
