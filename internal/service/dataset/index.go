package dataset

import (
	"sort"

	"github.com/telmocbarros/data-pulse/internal/columntype"
)

// extractColumns returns a (name, type) pair per key in row, with the type
// inferred from the Go runtime type of the value. Pairs are sorted by name
// so the resulting CREATE TABLE column order is deterministic across
// uploads of the same input.
func extractColumns(row map[string]any) [][]string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	columns := make([][]string, 0, len(keys))
	for _, key := range keys {
		columns = append(columns, []string{key, columntype.FromGo(row[key])})
	}
	return columns
}
