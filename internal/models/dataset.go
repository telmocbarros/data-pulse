package models

type Dataset struct {
	Name    string
	Data    []map[string]any
	Columns [][]string
	Size    int64
}
