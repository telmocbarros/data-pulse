package dataset

import (
	"encoding/csv"
	"log/slog"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/models"
)

// ValidationError is an alias for models.ValidationError so pipeline code
// can keep using the unqualified name.
type ValidationError = models.ValidationError

// ReadCsvRowAndExtractType reads one CSV record and classifies each cell
// into a column type via columntype.Classify. Used during pipeline setup
// to derive the schema from the first data row.
func ReadCsvRowAndExtractType(csvReader *csv.Reader) ([]string, []string, error) {
	content, err := csvReader.Read()
	if err != nil {
		slog.Error("read csv row failed", "err", err)
		return nil, nil, err
	}

	cellTypes := make([]string, len(content))
	for i, value := range content {
		cellTypes[i] = columntype.Classify(value)
	}
	return content, cellTypes, nil
}

// ReadJsonRowAndExtractType walks a decoded JSON row and replaces every
// string-typed value with its parsed Go form (int, float, bool, time.Time,
// or the original string), so downstream type checks see real Go types.
func ReadJsonRowAndExtractType(row map[string]any) {
	for k, v := range row {
		if strValue, ok := v.(string); ok {
			row[k] = columntype.Parse(strValue)
		}
	}
}
