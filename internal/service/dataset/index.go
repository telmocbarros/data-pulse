package dataset

import (
	"time"

	"github.com/telmocbarros/data-pulse/internal/columntype"
)

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
		return columntype.IS_DATE
	case float64, float32, int, int8, int16, int32, int64:
		return columntype.IS_NUMERICAL
	case bool:
		return columntype.IS_BOOLEAN
	default:
		return columntype.IS_CATEGORICAL
	}
}
