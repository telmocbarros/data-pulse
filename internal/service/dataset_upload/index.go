package service

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"sync"
	"time"

	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
)

func ProcessCsvFile(f multipart.File, fileName string, fileSize int64) (results [][]any, validationErrors []ValidationError, err error) {
	csvReader := csv.NewReader(f)

	// 1. Read the file
	headers, err := csvReader.Read()
	if err != nil {
		log.Println("Header format is invalid: ", err)
		return nil, nil, err
	}
	log.Println("File headers: ", headers)

	// 2. extract the row filed types based on the first row with data
	content, row_field_types, err := ReadCsvRowAndExtractType(csvReader)
	if err != nil {
		log.Println("Something went wrong when extracting the row field types: ", err)
		return nil, nil, err
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
	// 4.1. Create the dynamic table for this dataset
	tableName, err := repository.CreateDatasetTable("csv", datasetColumns)
	if err != nil {
		fmt.Println("Error while attempting to create a table: ", err)
		return
	}

	// 4.2. Store dataset metadata and get the generated dataset ID
	metadata := map[string]any{
		"name":        fileName,
		"tableName":   tableName,
		"size":        fileSize,
		"author":      "engineering",
		"description": "description",
	}

	datasetId, err := repository.StoreDatasetMetadata(metadata)
	if err != nil {
		fmt.Println("Error adding dataset metadata")
		return
	}

	// 4.3. Store dataset columns
	if err = repository.StoreDatasetColumns(datasetColumns, datasetId); err != nil {
		fmt.Println("Error adding dataset columns")
		return
	}

	// 5. Concurrent pipeline: parse → validate → store
	//
	// Stage 1 (parser):   reads CSV rows, attaches row numbers, sends to parserCh.
	//                     Column-count mismatches are caught here.
	// Stage 2 (validator): checks types and missing values, sends valid rows to dataCh.
	// Stage 3 (store):     batches rows from dataCh and writes to DB.
	// Error collector:     drains errorsCh (fed by both stage 1 and 2) into validationErrors.
	//
	// errorsCh is closed only after both producers (parser + validator) are done,
	// coordinated via errWg.

	type numberedRecord struct {
		Row    int32
		Record []string
	}

	errorsCh := make(chan ValidationError, 100)
	parserCh := make(chan numberedRecord, 100)
	dataCh := make(chan map[string]any, 100)

	expectedColumns := len(row_field_types)

	// errWg tracks goroutines that write to errorsCh so we can
	// close it safely after both the parser and validator finish.
	var errWg sync.WaitGroup

	// Stage 1: Parse — read raw CSV rows and send them downstream
	errWg.Go(func() {
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
	})

	// Stage 2: Validate — check types and missing values, forward valid rows to dataCh
	errWg.Go(func() {
		defer close(dataCh)

		for nr := range parserCh {
			jsonResult := make(map[string]any)
			for idx, value := range nr.Record {

				if value == "" {
					errorsCh <- ValidationError{
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
					if row_field_types[idx] != variableType {
						errorsCh <- ValidationError{
							Row:      nr.Row,
							Column:   int32(idx),
							Kind:     "type_mismatch",
							Expected: row_field_types[idx],
							Received: variableType,
						}
					}
				}
				jsonResult[headers[idx]] = ParseValue(value)
			}
			dataCh <- jsonResult
		}
	})

	// Close errorsCh once both producers (parser + validator) are done
	go func() {
		errWg.Wait()
		close(errorsCh)
	}()

	var wg sync.WaitGroup

	// Error collector — drains errorsCh into the returned validationErrors slice
	wg.Go(func() {
		for ve := range errorsCh {
			validationErrors = append(validationErrors, ve)
		}
	})

	// Stage 3: Store — batches rows from dataCh and writes to DB
	wg.Go(func() {
		uploadJsonDataset(dataCh, tableName, datasetId)
	})

	wg.Wait()

	return results, validationErrors, nil
}

func ProcessJsonFile(f multipart.File, fileName string, fileSize int64) (jsonResults []map[string]any, validationErrors []ValidationError, err error) {
	decoder := json.NewDecoder(f)

	// Consume opening '[' of the array
	tok, err := decoder.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("expected opening '[': %w", err)
	}

	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return nil, nil, fmt.Errorf("expected JSON array, got %v", tok)
	}

	// 1. Decode first row and extract column types
	if !decoder.More() {
		return nil, nil, fmt.Errorf("empty JSON array")
	}

	var firstRow map[string]any
	if err := decoder.Decode(&firstRow); err != nil {
		return nil, nil, fmt.Errorf("error decoding first row: %w", err)
	}

	ReadJsonRowAndExtractType(firstRow)

	columnKeys := make([]string, 0, len(firstRow))
	for k := range firstRow {
		columnKeys = append(columnKeys, k)
	}
	columnTypes := make(map[string]string, len(firstRow))
	for k, v := range firstRow {
		varType, err := ComputeVariableType(fmt.Sprintf("%v", v))
		if err != nil {
			return nil, nil, fmt.Errorf("error detecting type for column %q: %w", k, err)
		}
		columnTypes[k] = varType
	}
	log.Println("Column keys: ", columnKeys)
	log.Println("Column types: ", columnTypes)

	datasetColumns := extractColumns(firstRow)

	// 2. Create table and store metadata
	tableName, err := repository.CreateDatasetTable("json", datasetColumns)
	if err != nil {
		fmt.Println("Error while attempting to create a table: ", err)
		return
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
		fmt.Println("Error adding dataset metadata")
		return
	}

	if err = repository.StoreDatasetColumns(datasetColumns, datasetId); err != nil {
		fmt.Println("Error adding dataset columns")
		return
	}

	// 3. Concurrent pipeline: parse → validate → store
	//
	// Stage 1 (parser):   decodes JSON objects, attaches row numbers, sends to parserCh.
	// Stage 2 (validator): checks types and missing columns, forwards valid rows to dataCh.
	// Stage 3 (store):     batches rows from dataCh and writes to DB.
	// Error collector:     drains errorsCh (fed by both stage 1 and 2) into validationErrors.
	//
	// errorsCh is closed only after both producers (parser + validator) are done,
	// coordinated via errWg.

	type numberedJsonRow struct {
		Row  int32
		Data map[string]any
	}

	errorsCh := make(chan ValidationError, 100)
	parserCh := make(chan numberedJsonRow, 100)
	dataCh := make(chan map[string]any, 100)

	// errWg tracks goroutines that write to errorsCh so we can
	// close it safely after both the parser and validator finish.
	var errWg sync.WaitGroup

	// Send first row straight to dataCh (already validated during setup)
	dataCh <- firstRow

	// Stage 1: Parse — decode JSON objects and send them downstream
	errWg.Go(func() {
		defer close(parserCh)

		rowNumber := int32(1) // row 0 was used for type extraction
		for decoder.More() {
			var row map[string]any
			if err := decoder.Decode(&row); err != nil {
				errorsCh <- ValidationError{
					Row:    rowNumber,
					Column: -1,
					Kind:   "malformed_row",
					Detail: err.Error(),
				}
				rowNumber++
				continue
			}

			ReadJsonRowAndExtractType(row)
			parserCh <- numberedJsonRow{Row: rowNumber, Data: row}
			rowNumber++
		}
	})

	// Stage 2: Validate — check types and missing columns, forward valid rows to dataCh
	errWg.Go(func() {
		defer close(dataCh)

		for nr := range parserCh {
			for _, k := range columnKeys {
				v, exists := nr.Data[k]
				if !exists {
					errorsCh <- ValidationError{
						Row:    nr.Row,
						Column: -1,
						Kind:   "missing_value",
						Detail: fmt.Sprintf("missing column %q", k),
					}
					continue
				}
				varType, err := ComputeVariableType(fmt.Sprintf("%v", v))
				if err != nil {
					fmt.Println("Error detecting type for column: ", err)
					return
				}
				if varType != columnTypes[k] {
					errorsCh <- ValidationError{
						Row:      nr.Row,
						Column:   -1,
						Kind:     "type_mismatch",
						Expected: columnTypes[k],
						Received: varType,
						Detail:   fmt.Sprintf("column %q", k),
					}
				}
			}
			dataCh <- nr.Data
		}
	})

	// Close errorsCh once both producers (parser + validator) are done
	go func() {
		errWg.Wait()
		close(errorsCh)
	}()

	var wg sync.WaitGroup

	// Error collector — drains errorsCh into the returned validationErrors slice
	wg.Go(func() {
		for ve := range errorsCh {
			validationErrors = append(validationErrors, ve)
		}
	})

	// Stage 3: Store — batches rows from dataCh and writes to DB
	wg.Go(func() {
		uploadJsonDataset(dataCh, tableName, datasetId)
	})

	wg.Wait()

	// Consume closing ']'
	if _, err := decoder.Token(); err != nil {
		return nil, nil, fmt.Errorf("expected closing ']': %w", err)
	}

	jsonResults = append(jsonResults, firstRow)
	return jsonResults, validationErrors, nil
}

func uploadJsonDataset(dataCh chan map[string]any, tableName string, datasetId string) {
	fmt.Println("Processing dataset ...")

	batchSize := 50
	batch := make([]map[string]any, 0, batchSize)

	for row := range dataCh {
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := repository.StoreDataset(tableName, datasetId, batch); err != nil {
				fmt.Println("Could not persist the dataset")
				return
			}
			batch = batch[:0]
		}
	}

	// flush remaining rows
	if len(batch) > 0 {
		if err := repository.StoreDataset(tableName, datasetId, batch); err != nil {
			fmt.Println("Could not persist the dataset")
		}
	}
}

func extractColumns(row map[string]any) [][]string {
	columns := make([][]string, 0, len(row))
	for key, val := range row {
		columns = append(columns, []string{key, goTypeToDBType(val)})
	}
	return columns
}

func goTypeToDBType(val any) string {
	switch val.(type) {
	case time.Time:
		return IS_DATE
	case float64, float32, int, int8, int16, int32, int64:
		return IS_NUMERICAL
	case bool:
		return IS_BOOLEAN
	default:
		return IS_TEXT
	}
}
