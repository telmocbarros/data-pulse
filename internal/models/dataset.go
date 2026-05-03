package models

type Dataset struct {
	Name    string
	Columns [][]string
	Size    int64
}

// ValidationError is one row-level problem caught during ingestion. Column
// is -1 for whole-row errors (e.g., malformed CSV row, missing JSON column).
// Kind is one of: "type_mismatch", "missing_value", "malformed_row".
type ValidationError struct {
	Row      int32
	Column   int32
	Kind     string
	Expected string
	Received string
	Detail   string
}
