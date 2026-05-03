package dataset

import (
	"github.com/telmocbarros/data-pulse/internal/columntype"
)

// extractColumns returns a (name, type) pair per key in row, with the type
// inferred from the Go runtime type of the value.
func extractColumns(row map[string]any) [][]string {
	columns := make([][]string, 0, len(row))
	for key, val := range row {
		columns = append(columns, []string{key, columntype.FromGo(val)})
	}
	return columns
}
