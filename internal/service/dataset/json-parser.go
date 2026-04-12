package dataset

import (
	"encoding/json"
	"fmt"
	"mime/multipart"
	"sync"

	"github.com/telmocbarros/data-pulse/config"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	"github.com/telmocbarros/data-pulse/internal/service/profiler"
)

type numberedJsonRow struct {
	Row  int32
	Data map[string]any
}

type jsonPipelineState struct {
	parserCh    chan numberedJsonRow
	errorsCh    chan ValidationError
	columnKeys  []string
	columnTypes map[string]string
	tableName   string
	datasetId   string
	firstRow    map[string]any
}

func ProcessJsonFileAsync(f multipart.File, fileName string, fileSize int64) error {
	state, err := parseJsonFile(f, fileName, fileSize)
	if err != nil {
		return err
	}
	go runJsonPipeline(state)
	return nil
}

func ProcessJsonFileSync(f multipart.File, fileName string, fileSize int64) []ValidationError {
	state, err := parseJsonFile(f, fileName, fileSize)
	if err != nil {
		return nil
	}
	return runJsonPipeline(state)
}

// parseJsonFile reads the file, creates metadata, and runs Stage 1 (parsing).
// The file is fully consumed when this function returns.
func parseJsonFile(f multipart.File, fileName string, fileSize int64) (*jsonPipelineState, error) {
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
		varType, err := ComputeVariableType(fmt.Sprintf("%v", v))
		if err != nil {
			return nil, fmt.Errorf("error detecting type for column %q: %w", k, err)
		}
		columnTypes[k] = varType
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

	// 3. Stage 1: Parse — decode JSON objects and send them downstream
	errorsCh := make(chan ValidationError, 100)
	parserCh := make(chan numberedJsonRow, 100)

	go func() {
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
	}()

	return &jsonPipelineState{
		parserCh:    parserCh,
		errorsCh:    errorsCh,
		columnKeys:  columnKeys,
		columnTypes: columnTypes,
		tableName:   tableName,
		datasetId:   datasetId,
		firstRow:    firstRow,
	}, nil
}

// runJsonPipeline runs Stages 2 (validate) and 3 (store) of the JSON pipeline.
func runJsonPipeline(state *jsonPipelineState) []ValidationError {
	var validationErrors []ValidationError

	dataCh := make(chan map[string]any, 100)
	profilerCh := make(chan map[string]any, 100)

	// Send first row straight to dataCh (already validated during setup)
	dataCh <- state.firstRow
	profilerCh <- state.firstRow

	var wg sync.WaitGroup

	// Error collector — drains errorsCh into the returned validationErrors slice
	wg.Go(func() {
		for ve := range state.errorsCh {
			validationErrors = append(validationErrors, ve)
		}
	})

	wg.Go(func() {
		profiler.ProfileDataset(profilerCh, state.datasetId, state.columnTypes)
	})

	// Stage 3: Store — batches rows from dataCh and writes to DB
	wg.Go(func() {
		uploadJsonDataset(dataCh, state.tableName, state.datasetId)
	})

	var errWg sync.WaitGroup

	// Stage 2: Validate — check types and missing columns, forward valid rows to dataCh
	errWg.Go(func() {
		defer close(dataCh)
		defer close(profilerCh)

		for nr := range state.parserCh {
			for _, k := range state.columnKeys {
				v, exists := nr.Data[k]
				if !exists {
					state.errorsCh <- ValidationError{
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
				if varType != state.columnTypes[k] {
					state.errorsCh <- ValidationError{
						Row:      nr.Row,
						Column:   -1,
						Kind:     "type_mismatch",
						Expected: state.columnTypes[k],
						Received: varType,
						Detail:   fmt.Sprintf("column %q", k),
					}
				}
			}
			dataCh <- nr.Data
			profilerCh <- nr.Data
		}
	})

	// Close errorsCh once both producers (parser + validator) are done
	go func() {
		errWg.Wait()
		close(state.errorsCh)
	}()

	wg.Wait()

	return validationErrors
}

func uploadJsonDataset(dataCh chan map[string]any, tableName string, datasetId string) {
	fmt.Println("Processing dataset ...")

	tx, err := config.Storage.Begin()
	if err != nil {
		fmt.Println("Could not begin transaction:", err)
		return
	}
	defer tx.Rollback()

	batchSize := 50
	batch := make([]map[string]any, 0, batchSize)

	for row := range dataCh {
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := repository.StoreDataset(tx, tableName, datasetId, batch); err != nil {
				fmt.Println("Could not persist the dataset")
				return
			}
			batch = batch[:0]
		}
	}

	// flush remaining rows
	if len(batch) > 0 {
		if err := repository.StoreDataset(tx, tableName, datasetId, batch); err != nil {
			fmt.Println("Could not persist the dataset")
			return
		}
	}

	if err := tx.Commit(); err != nil {
		fmt.Println("Could not commit transaction:", err)
	}
}
