package repository

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/telmocbarros/data-pulse/config"
)

func StoreDataset(tableName string, datasetId string, rows []map[string]any) error {
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

func CreateDatasetTable(tableName string, columns [][]string) (string, error) {
	if tableName == "" {
		tableName = fmt.Sprintf("json_datasets_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	}
	var query strings.Builder
	fmt.Fprintf(&query, "CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY, dataset_id UUID, ", tableName)
	for j, col := range columns {
		if j > 0 {
			query.WriteString(", ")
		}
		if col[0] == "created_at" {
			fmt.Fprintf(&query, "%s %s", "entry_date", mapToDatabase(col[1]))

		} else {

			fmt.Fprintf(&query, "%s %s", col[0], mapToDatabase(col[1]))
		}
	}
	query.WriteString(")")

	_, err := config.Storage.Exec(query.String())
	if err != nil {
		return "", fmt.Errorf("unable to create table: %w", err)
	}

	return tableName, nil
}

func mapToDatabase(value string) string {
	switch value {
	case "IS_NUMERICAL":
		return "DOUBLE PRECISION"
	case "IS_BOOLEAN":
		return "BOOLEAN"
	case "IS_DATE":
		return "TIMESTAMP"
	case "IS_TEXT":
		return "TEXT"
	default:
		return "TEXT"
	}
}
