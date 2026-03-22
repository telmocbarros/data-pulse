package models

type Dataset struct {
	Name    string
	Columns [][]string
	Size    int64
}
