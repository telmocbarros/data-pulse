package service

import (
	"encoding/csv"
	"fmt"
	"log"
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

func ReadCsvRowAndExtractType(csvReader *csv.Reader) ([]string, []string, error) {
	content, err := csvReader.Read()
	if err != nil {
		log.Println("Something went wrong reading the file: ", err)
		return nil, nil, err
	}

	var cellTypes []string
	for _, value := range content {
		variableType, err := ComputeVariableType(value)
		if err != nil {
			fmt.Println("Error retrieving cell variable type: ", err)
			return nil, nil, err
		}
		cellTypes = append(cellTypes, variableType)
	}

	return content, cellTypes, nil
}

func ReadJsonRowAndExtractType(row map[string]any) {
	for k, v := range row {
		if strValue, ok := v.(string); ok {
			row[k] = ParseValue(strValue)
		}
	}
}

func ParseValue(value string) any {
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

func ComputeVariableType(value string) (valueType string, err error) {
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
	// time.Time printed via fmt.Sprintf("%v") produces this format
	_, err = time.Parse("2006-01-02 15:04:05 -0700 MST", value)
	if err == nil {
		return IS_DATE, nil
	}
	_, err = time.Parse("2006-01-02 15:04:05 +0000 UTC", value)
	if err == nil {
		return IS_DATE, nil
	}

	return IS_TEXT, nil
}
