package dataset

import (
	"encoding/csv"
	"log"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/models"
)

// ValidationError is an alias for models.ValidationError so pipeline code
// can keep using the unqualified name.
type ValidationError = models.ValidationError

func ReadCsvRowAndExtractType(csvReader *csv.Reader) ([]string, []string, error) {
	content, err := csvReader.Read()
	if err != nil {
		log.Println("Something went wrong reading the file: ", err)
		return nil, nil, err
	}

	cellTypes := make([]string, len(content))
	for i, value := range content {
		cellTypes[i] = columntype.Classify(value)
	}
	return content, cellTypes, nil
}

func ReadJsonRowAndExtractType(row map[string]any) {
	for k, v := range row {
		if strValue, ok := v.(string); ok {
			row[k] = columntype.Parse(strValue)
		}
	}
}
