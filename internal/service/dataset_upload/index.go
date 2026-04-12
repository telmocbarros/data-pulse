package service

import (
	"time"
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
		return IS_DATE
	case float64, float32, int, int8, int16, int32, int64:
		return IS_NUMERICAL
	case bool:
		return IS_BOOLEAN
	default:
		return IS_TEXT
	}
}
