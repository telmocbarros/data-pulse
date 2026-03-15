package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
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

func uploadDataset(w http.ResponseWriter, r *http.Request) {
	// 1. Parse input, type multipart/form-data

	// << is a bitwise operator. By putting a number to its right,
	// you're shifting the number on its left in its binary representation
	// by that amount of zeros
	// Usual patterns:
	// << 10 = ×1,024 (KiB)
	// << 20 = ×1,048,576 (MiB)
	// << 30 = ×1,073,741,824 (GiB)
	r.ParseMultipartForm(10 << 20)

	// 2. Retrieve file from posted form-data
	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	mimeType := handler.Header.Get("Content-Type")
	if mimeType == "" {
		fmt.Println("Invalid file type: (empty)")
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	} else if mimeType != "text/csv" && mimeType != "application/json" {
		fmt.Println("Invalid file type: ", mimeType)
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	}

	// 3. Write temporary file on our server
	// %#v	a Go-syntax representation of the value
	// (floating-point infinities and NaNs print as ±Inf and NaN)
	fmt.Printf("File name: %+v\n", handler.Filename)
	fmt.Printf("File size: %+v B\n", handler.Size)
	fmt.Printf("MIME Header: %+v\n", handler.Header)
	fmt.Printf("Content Type: %s\n", handler.Header.Get("Content-Type"))

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("Error reading file: ", err)
		http.Error(w, "Error reading file", http.StatusInternalServerError)
		return
	}

	fileToCreate, err := os.Create("temp.txt")
	if err != nil {
		fmt.Println("Error creating file: ", err)
		http.Error(w, "Error creating file", http.StatusInternalServerError)
		return
	}

	defer fileToCreate.Close()

	// 4. Return whether or not this has been succesfull
	_, err = fileToCreate.Write(fileBytes)
	if err != nil {
		fmt.Println("Error writing to file: ", err)
		http.Error(w, "Error writing to file", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Successfully Uploaded File\n")
}

func uploadDatasetInChunks(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		fmt.Println("Error parsing file: ", err)
		http.Error(w, "Error parsing file", http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	mimeType := handler.Header.Get("Content-Type")
	if mimeType == "" {
		fmt.Println("Invalid file type: (empty)")
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	} else if mimeType != "text/csv" && mimeType != "application/json" {
		fmt.Println("Invalid file type: ", mimeType)
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	}
	fileToCreate, err := os.Create("temp.txt")
	if err != nil {
		fmt.Println("Error creating file: ", err)
		http.Error(w, "Error creating file", http.StatusInternalServerError)
		return
	}
	defer fileToCreate.Close()

	written, err := io.Copy(fileToCreate, file)
	if err != nil {
		fmt.Println("Error writing to file: ", err)
		http.Error(w, "Error writing to file", http.StatusInternalServerError)
		return
	}

	fmt.Printf("Succesfully uploaded file: %v B\n", written)
	fmt.Fprintf(w, "Successfully Uploaded File\n")
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
	content, row_filed_types, err := readRowAndExtractType(csvReader)
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

func parseJsonFile(f multipart.File) (record any, err error) {
	var result any
	err = json.NewDecoder(f).Decode(&result)

	if err != nil {
		fmt.Println("Error parsing json file: ", err)
		return nil, err
	}

	return result, nil
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
		record, err := parseJsonFile(file)
		if err != nil {
			fmt.Println("Error parsing JSON file: ", err)
			http.Error(w, "Error parsing JSON file", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		jsonBytes, err := json.Marshal(record)
		if err != nil {
			fmt.Println("Error encoding JSON response: ", err)
			http.Error(w, "Error encoding JSON response", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Successfully parsed JSON file: %d rows\n", len(jsonBytes))

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

func readRowAndExtractType(csvReader *csv.Reader) ([]string, []string, error) {
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
