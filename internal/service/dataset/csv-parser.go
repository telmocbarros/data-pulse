package dataset

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"mime/multipart"
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

// ProcessCsvFileAsync parses the file synchronously (Stage 1),
// then launches validate + store (Stages 2+3) in the background.
func ProcessCsvFileAsync(f multipart.File, fileName string, fileSize int64) error {
	state, err := parseCsvFile(f, fileName, fileSize)
	if err != nil {
		return err
	}
	go runCsvPipeline(state)
	return nil
}

func ProcessCsvFileSync(f multipart.File, fileName string, fileSize int64) []ValidationError {
	state, err := parseCsvFile(f, fileName, fileSize)
	if err != nil {
		return nil
	}

	return runCsvPipeline(state)
}

// parseCsvFile reads the file, creates metadata, and runs Stage 1 (parsing).
// It returns a csvPipelineState that contains the channels for Stages 2+3.
// The file is fully consumed when this function returns.
func parseCsvFile(f multipart.File, fileName string, fileSize int64) (*csvPipelineState, error) {
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
			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				errorsCh <- ValidationError{
					Row:    rowNumber,
					Column: -1,
					Kind:   "malformed_row",
					Detail: err.Error(),
				}
				rowNumber++
				continue
			}

			if len(record) != expectedColumns {
				errorsCh <- ValidationError{
					Row:      rowNumber,
					Column:   -1,
					Kind:     "malformed_row",
					Expected: fmt.Sprintf("%d columns", expectedColumns),
					Received: fmt.Sprintf("%d columns", len(record)),
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
func runCsvPipeline(state *csvPipelineState) []ValidationError {
	var validationErrors []ValidationError

	dataCh := make(chan map[string]any, 100)
	expectedColumns := len(state.rowFieldTypes)

	// errWg tracks goroutines that write to errorsCh so we can
	// close it safely after both the parser and validator finish.
	var errWg sync.WaitGroup

	// Stage 2: Validate — check types and missing values, forward valid rows to dataCh
	errWg.Go(func() {
		defer close(dataCh)

		for nr := range state.parserCh {
			jsonResult := make(map[string]any)
			for idx, value := range nr.Record {

				if value == "" {
					state.errorsCh <- ValidationError{
						Row:    nr.Row,
						Column: int32(idx),
						Kind:   "missing_value",
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
						state.errorsCh <- ValidationError{
							Row:      nr.Row,
							Column:   int32(idx),
							Kind:     "type_mismatch",
							Expected: state.rowFieldTypes[idx],
							Received: variableType,
						}
					}
				}
				jsonResult[state.headers[idx]] = ParseValue(value)
			}

			dataCh <- jsonResult
		}
	})

	// Close errorsCh once both producers (parser + validator) are done
	go func() {
		errWg.Wait()
		close(state.errorsCh)
	}()

	var wg sync.WaitGroup

	// Error collector — drains errorsCh into the returned validationErrors slice
	wg.Go(func() {
		for ve := range state.errorsCh {
			validationErrors = append(validationErrors, ve)
		}
	})

	// Stage 3: Store — batches rows from dataCh and writes to DB
	wg.Go(func() {
		uploadJsonDataset(dataCh, state.tableName, state.datasetId)
	})

	wg.Wait()

	return validationErrors
}
