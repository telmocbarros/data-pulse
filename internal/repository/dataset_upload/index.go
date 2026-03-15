package repository

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/telmocbarros/data-pulse/config"
)

func CreateDatasetTable(tableName string, datasetId string, rows []map[string]any) error {
	if tableName == "" {
		tableName = "json_datasets"
	}
	// extract column names from the first row
	columns := make([]string, 0, len(rows[0])+1)
	columns = append(columns, "id", "dataset_id")
	for key := range rows[0] {
		if key == "created_at" {
			columns = append(columns, "entry_date")
		} else {
			columns = append(columns, key)
		}
	}
	numCols := len(columns)
	// build column list: (id, dataset_id, col1, col2, ...)
	colList := strings.Join(columns, ", ")

	// build query with dynamic placeholders
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, colList)
	vals := []any{}
	for i, row := range rows {
		var placeholders strings.Builder
		placeholders.WriteString("(")
		for j := range numCols {
			if j > 0 {
				placeholders.WriteString(", ")
			}
			fmt.Fprintf(&placeholders, "$%d", i*numCols+j+1)
		}
		placeholders.WriteString("),")
		query += placeholders.String()

		vals = append(vals, uuid.New().String(), datasetId)
		for _, col := range columns[2:] {
			if col == "entry_date" {
				vals = append(vals, row["created_at"])
			} else {
				vals = append(vals, row[col])
			}
		}
	}
	query = query[:len(query)-1] // trim trailing comma

	result, err := config.Storage.Exec(query, vals...)
	if err != nil {
		fmt.Println("unable to execute insert query", err)
		return err
	}

	fmt.Println("Successfully executed the query: ", result)
	return nil
}
