// Package columntype defines the type tags this codebase uses for dataset
// columns and provides helpers to detect them from raw values.
package columntype

// Type tags. Persisted in dataset_columns.column_type and related tables.
const (
	Numerical   = "numerical"
	Boolean     = "boolean"
	Date        = "date"
	Categorical = "categorical"
)
