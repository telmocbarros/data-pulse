package service

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"time"

	"github.com/telmocbarros/data-pulse/internal/models"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
)

func ProcessCsvFile(f multipart.File, fileName string) (results [][]any, validationErrors []ValidationError, err error) {
	csvReader := csv.NewReader(f)
	headers, err := csvReader.Read()
	if err != nil {
		log.Println("Header format is invalid: ", err)
		return nil, nil, err
	}
	log.Println("File headers: ", headers)
	// extract the row filed types based on the first row with data
	content, row_field_types, err := ReadCsvRowAndExtractType(csvReader)
	if err != nil {
		log.Println("Something went wrong when extracting the row field types: ", err)
		return nil, nil, err
	}
	var temp []any

	jsonObj := make(map[string]any)

	var jsonFormattedData []map[string]any

	for idx, value := range content {
		parsed := ParseValue(value)
		temp = append(temp, parsed)
		jsonObj[headers[idx]] = parsed
	}
	results = append(results, temp)
	jsonFormattedData = append(jsonFormattedData, jsonObj)

	log.Println("Column Types: ", row_field_types)
	// allow csv.Reader to handle rows with wrong field count
	// instead of returning an error
	csvReader.FieldsPerRecord = -1

	// reading remaining rows, validating types and flagging mismatches
	rowNumber := int32(2) // row 1 was used for type extraction
	expectedColumns := len(row_field_types)
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			// flag malformed rows (bad quoting, etc.) and continue
			validationErrors = append(validationErrors, ValidationError{
				Row:    rowNumber,
				Column: -1,
				Kind:   "malformed_row",
				Detail: err.Error(),
			})
			rowNumber++
			continue
		}

		// flag rows with wrong number of fields
		if len(record) != expectedColumns {
			validationErrors = append(validationErrors, ValidationError{
				Row:      rowNumber,
				Column:   -1,
				Kind:     "malformed_row",
				Expected: fmt.Sprintf("%d columns", expectedColumns),
				Received: fmt.Sprintf("%d columns", len(record)),
			})
		}

		var temp []any
		jsonResult := make(map[string]any)
		for idx, value := range record {

			// flag missing values
			if value == "" {
				validationErrors = append(validationErrors, ValidationError{
					Row:    rowNumber,
					Column: int32(idx),
					Kind:   "missing_value",
				})
				temp = append(temp, ParseValue(value))
				continue
			}

			// flag type mismatches (only for columns within expected range)
			if idx < expectedColumns {
				variableType, err := ComputeVariableType(value)
				if err != nil {
					fmt.Println("Error retrieving cell variable type: ", err)
					return nil, nil, err
				}
				if row_field_types[idx] != variableType {
					validationErrors = append(validationErrors, ValidationError{
						Row:      rowNumber,
						Column:   int32(idx),
						Kind:     "type_mismatch",
						Expected: row_field_types[idx],
						Received: variableType,
					})
				}
			}
			temp = append(temp, ParseValue(value))
			jsonResult[headers[idx]] = ParseValue(value)
		}
		jsonFormattedData = append(jsonFormattedData, jsonResult)
		results = append(results, temp)
		rowNumber++
	}

	var dataset models.Dataset
	dataset.Name = fileName
	dataset.Data = jsonFormattedData
	dataset.Columns = extractColumns(jsonFormattedData[0])

	uploadJsonDataset(dataset)

	return results, validationErrors, nil
}

func ProcessJsonFile(f multipart.File, fileName string) (jsonResults []map[string]any, validationErrors []ValidationError, err error) {
	decoder := json.NewDecoder(f)

	// Consume opening '[' of the array
	tok, err := decoder.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("expected opening '[': %w", err)
	}

	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return nil, nil, fmt.Errorf("expected JSON array, got %v", tok)
	}

	var columnKeys []string
	var columnTypes map[string]string

	// Decode one object at a time while the array has more elements
	rowNumber := int32(0)
	for decoder.More() {
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			validationErrors = append(validationErrors, ValidationError{
				Row:    rowNumber,
				Column: -1,
				Kind:   "malformed_row",
				Detail: err.Error(),
			})
			rowNumber++
			continue
		}

		ReadJsonRowAndExtractType(row)

		if rowNumber == 0 {
			// Extract column types from the first row
			columnKeys = make([]string, 0, len(row))
			for k := range row {
				columnKeys = append(columnKeys, k)
			}
			columnTypes = make(map[string]string, len(row))
			for k, v := range row {
				varType, err := ComputeVariableType(fmt.Sprintf("%v", v))
				if err != nil {
					return nil, nil, fmt.Errorf("error detecting type for column %q: %w", k, err)
				}
				columnTypes[k] = varType
			}
			log.Println("Column keys: ", columnKeys)
			log.Println("Column types: ", columnTypes)
		} else {
			// Validate subsequent rows against first-row types
			for _, k := range columnKeys {
				v, exists := row[k]
				if !exists {
					validationErrors = append(validationErrors, ValidationError{
						Row:    rowNumber,
						Column: -1,
						Kind:   "missing_value",
						Detail: fmt.Sprintf("missing column %q", k),
					})
					continue
				}
				varType, err := ComputeVariableType(fmt.Sprintf("%v", v))
				if err != nil {
					return nil, nil, fmt.Errorf("error detecting type for column %q: %w", k, err)
				}
				if varType != columnTypes[k] {
					validationErrors = append(validationErrors, ValidationError{
						Row:      rowNumber,
						Column:   -1,
						Kind:     "type_mismatch",
						Expected: columnTypes[k],
						Received: varType,
						Detail:   fmt.Sprintf("column %q", k),
					})
				}
			}
		}

		jsonResults = append(jsonResults, row)
		rowNumber++
	}

	// Consume closing ']'
	if _, err := decoder.Token(); err != nil {
		return nil, nil, fmt.Errorf("expected closing ']': %w", err)
	}

	var dataset models.Dataset
	dataset.Name = fileName
	dataset.Data = jsonResults
	dataset.Columns = extractColumns(jsonResults[0])

	uploadJsonDataset(dataset)

	return jsonResults, validationErrors, nil
}

func uploadJsonDataset(dataset models.Dataset) {
	fmt.Println("Processing json dataset ...")

	limit := 50
	start := 0
	end := limit

	// 1. Create the dynamic table for this dataset
	tableName, err := repository.CreateDatasetTable("", dataset.Columns)
	if err != nil {
		fmt.Println("Error while attempting to create a table: ", err)
		return
	}

	// 2. Store dataset metadata and get the generated dataset ID
	metadata := map[string]any{
		"name":        dataset.Name,
		"tableName":   tableName,
		"size":        len(dataset.Data),
		"author":      "engineering",
		"description": "description",
	}

	datasetId, err := repository.StoreDatasetMetadata(metadata)
	if err != nil {
		fmt.Println("Error adding dataset metadata")
		return
	}

	// 3. Store dataset columns
	if err := repository.StoreDatasetColumns(dataset.Columns, datasetId); err != nil {
		fmt.Println("Error adding dataset columns")
		return
	}

	// 4. Store dataset data in batches
	for start < len(dataset.Data) {
		if end > len(dataset.Data) {
			end = len(dataset.Data)
		}
		if err := repository.StoreDataset(tableName, datasetId, dataset.Data[start:end]); err != nil {
			fmt.Println("Could not persist the dataset")
			break
		}

		start += limit
		end += limit
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
