package sqlsafe

import (
	"database/sql"
	"fmt"
	"strings"
)

// execer is the minimal subset of *sql.DB and *sql.Tx that BulkInsert needs.
// Defined locally so this package stays free of higher-level dependencies.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// BulkInsert builds and executes an INSERT statement with multiple value
// rows, all in a single SQL call:
//
//	INSERT INTO <table> (<col1>, <col2>, ...) VALUES
//	  ($1, $2, ...), ($N+1, ...), ...
//
// Each entry in rows must have exactly len(columns) values in column order.
// Returns nil if rows is empty (no SQL is executed). Identifiers are
// validated; callers do not need to pre-validate them.
//
// This is the bulk-insert equivalent of the dynamic-row builders that were
// previously copy-pasted across the repository layer.
func BulkInsert(exec execer, table string, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	if !IsValidIdentifier(table) {
		return fmt.Errorf("invalid table name: %q", table)
	}
	if len(columns) == 0 {
		return fmt.Errorf("bulk insert: columns must be non-empty")
	}
	for _, c := range columns {
		if !IsValidIdentifier(c) {
			return fmt.Errorf("invalid column name: %q", c)
		}
	}
	numCols := len(columns)
	for i, r := range rows {
		if len(r) != numCols {
			return fmt.Errorf("bulk insert: row %d has %d values, want %d", i, len(r), numCols)
		}
	}

	var query strings.Builder
	fmt.Fprintf(&query, "INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", "))

	vals := make([]any, 0, len(rows)*numCols)
	for i, r := range rows {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteByte('(')
		base := i * numCols
		for j := range numCols {
			if j > 0 {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "$%d", base+j+1)
		}
		query.WriteByte(')')
		vals = append(vals, r...)
	}

	_, err := exec.Exec(query.String(), vals...)
	return err
}
