package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"time"
)

type ValidationError struct {
	Row      int32
	Column   int32
	Kind     string // "type_mismatch", "missing_value", "malformed_row"
	Expected string
	Received string
	Detail   string
}

const (
	IS_NUMERICAL = "IS_NUMERICAL"
	IS_BOOLEAN   = "IS_BOOLEAN"
	IS_DATE      = "IS_DATE"
	IS_TEXT      = "IS_TEXT"
)

func main() {
	http.HandleFunc("/health", health)
	http.HandleFunc("/dataset", fileUploadHandler)
	fmt.Println("Listening on PORT 8080 ...")
	http.ListenAndServe(":8080", nil)
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("healthy"))
}

func parseCsvFile(f multipart.File) (results [][]any, validationErrors []ValidationError, err error) {
	csvReader := csv.NewReader(f)
	headers, err := csvReader.Read()
	if err != nil {
		log.Println("Header format is invalid: ", err)
		return nil, nil, err
	}
	log.Println("File headers: ", headers)
	// extract the row filed types based on the first row with data
	content, row_filed_types, err := readCsvRowAndExtractType(csvReader)
	if err != nil {
		log.Println("Something went wrong when extracting the row field types: ", err)
		return nil, nil, err
	}
	var temp []any
	for _, value := range content {
		temp = append(temp, parseValue(value))
	}
	results = append(results, temp)

	log.Println("Column Types: ", row_filed_types)
	// allow csv.Reader to handle rows with wrong field count
	// instead of returning an error
	csvReader.FieldsPerRecord = -1

	// reading remaining rows, validating types and flagging mismatches
	rowNumber := int32(2) // row 1 was used for type extraction
	expectedColumns := len(row_filed_types)
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
		for idx, value := range record {
			// flag missing values
			if value == "" {
				validationErrors = append(validationErrors, ValidationError{
					Row:    rowNumber,
					Column: int32(idx),
					Kind:   "missing_value",
				})
				temp = append(temp, parseValue(value))
				continue
			}

			// flag type mismatches (only for columns within expected range)
			if idx < expectedColumns {
				variableType, err := computeVariableType(value)
				if err != nil {
					fmt.Println("Error retrieving cell variable type: ", err)
					return nil, nil, err
				}
				if row_filed_types[idx] != variableType {
					validationErrors = append(validationErrors, ValidationError{
						Row:      rowNumber,
						Column:   int32(idx),
						Kind:     "type_mismatch",
						Expected: row_filed_types[idx],
						Received: variableType,
					})
				}
			}
			temp = append(temp, parseValue(value))
		}
		results = append(results, temp)
		rowNumber++
	}
	return results, validationErrors, nil
}

func parseJsonFile(f multipart.File) (jsonResults []map[string]any, validationErrors []ValidationError, err error) {
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

		readJsonRowAndExtractType(row)

		if rowNumber == 0 {
			// Extract column types from the first row
			columnKeys = make([]string, 0, len(row))
			for k := range row {
				columnKeys = append(columnKeys, k)
			}
			columnTypes = make(map[string]string, len(row))
			for k, v := range row {
				varType, err := computeJsonVariableType(v)
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
				varType, err := computeJsonVariableType(v)
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

	return jsonResults, validationErrors, nil
}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		fmt.Println("Error retrieving multipart form")
		http.Error(w, "Error retrieving multipart form", http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	fmt.Println("content type: ", handler.Header.Get("Content-Type"))

	switch handler.Header.Get("Content-Type") {
	case "application/json":
		record, validationErrors, err := parseJsonFile(file)
		if err != nil {
			fmt.Println("Error parsing JSON file: ", err)
			http.Error(w, "Error parsing JSON file", http.StatusInternalServerError)
			return
		}
		if len(validationErrors) > 0 {
			log.Printf("JSON parsed with %d validation errors\n", len(validationErrors))
		}
		fmt.Fprintf(w, "Successfully parsed JSON file: %d rows, %d validation errors\n", len(record), len(validationErrors))

	case "text/csv":
		record, validationErrors, err := parseCsvFile(file)
		if err != nil {
			http.Error(w, "Error parsing csv file", http.StatusInternalServerError)
			return
		}
		if len(validationErrors) > 0 {
			log.Printf("CSV parsed with %d validation errors\n", len(validationErrors))
		}
		fmt.Fprintf(w, "Successfully parsed CSV file: %d rows, %d validation errors\n", len(record), len(validationErrors))
	default:
		http.Error(w, "Unsupported file type", http.StatusBadRequest)
	}
}

func computeVariableType(value string) (valueType string, err error) {
	// is empty
	if len(value) == 0 {
		return IS_TEXT, nil
	}

	// is numerical
	_, err = strconv.ParseFloat(value, 64)
	if err == nil {
		return IS_NUMERICAL, nil
	}

	// is boolean
	// after the numerical check because
	// "1" and "0" are valid booleans in Go's strconv.ParseBool
	_, err = strconv.ParseBool(value)
	if err == nil {
		return IS_BOOLEAN, nil
	}

	// is date
	_, err = time.Parse("2006-01-02 15:04:05", value)
	if err == nil {
		return IS_DATE, nil
	}
	_, err = time.Parse("2006-01-02", value)
	if err == nil {
		return IS_DATE, nil
	}

	return IS_TEXT, nil
}

func computeJsonVariableType(value any) (string, error) {
	return computeVariableType(fmt.Sprintf("%v", value))
}

func readCsvRowAndExtractType(csvReader *csv.Reader) ([]string, []string, error) {
	content, err := csvReader.Read()
	if err != nil {
		log.Println("Something went wrong reading the file: ", err)
		return nil, nil, err
	}

	var cellTypes []string
	for _, value := range content {
		variableType, err := computeVariableType(value)
		if err != nil {
			fmt.Println("Error retrieving cell variable type: ", err)
			return nil, nil, err
		}
		cellTypes = append(cellTypes, variableType)
	}

	return content, cellTypes, nil
}

func readJsonRowAndExtractType(row map[string]any) {
	for k, v := range row {
		if strValue, ok := v.(string); ok {
			row[k] = parseValue(strValue)
		}
	}
}

func parseValue(value string) any {
	intResult, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return intResult
	}

	floatResult, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return floatResult
	}

	// is boolean
	// after the numerical check because
	// "1" and "0" are valid booleans in Go's strconv.ParseBool
	booleanResult, err := strconv.ParseBool(value)
	if err == nil {
		return booleanResult
	}

	// is date
	dateTimeResult, err := time.Parse("2006-01-02 15:04:05", value)
	if err == nil {
		return dateTimeResult
	}

	dateResult, err := time.Parse("2006-01-02", value)
	if err == nil {
		return dateResult
	}

	return value
}
