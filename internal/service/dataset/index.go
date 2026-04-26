package dataset

import (
	"time"

	"github.com/telmocbarros/data-pulse/internal/columntype"
)

// extractColumns returns a (name, type) pair per key in row, with the type
// inferred from the Go runtime type of the value.
func extractColumns(row map[string]any) [][]string {
	columns := make([][]string, 0, len(row))
	for key, val := range row {
		columns = append(columns, []string{key, goTypeToDBType(val)})
	}
	return columns
}

// goTypeToDBType maps a Go value's dynamic type to one of the columntype
// constants used downstream by the profiler and visualization layer.
func goTypeToDBType(val any) string {
	switch val.(type) {
	case time.Time:
		return columntype.IS_DATE
	case float64, float32, int, int8, int16, int32, int64:
		return columntype.IS_NUMERICAL
	case bool:
		return columntype.IS_BOOLEAN
	default:
		return columntype.IS_CATEGORICAL
	}
}
