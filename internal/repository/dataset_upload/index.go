package repository

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/telmocbarros/data-pulse/config"
)

func StoreDataset(dbExecutor config.Executor, tableName string, datasetId string, rows []map[string]any) error {
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

	result, err := dbExecutor.Exec(query, vals...)
	if err != nil {
		fmt.Println("unable to execute insert query", err)
		return err
	}

	fmt.Println("Successfully executed the query: ", result)
	return nil
}

func CreateDatasetTable(fileExtension string, columns [][]string) (string, error) {

	tableName := fmt.Sprintf("%s_datasets_%s", fileExtension, strings.ReplaceAll(uuid.New().String(), "-", ""))

	var query strings.Builder
	fmt.Fprintf(&query, "CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), dataset_id UUID, ", tableName)
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

func StoreDatasetMetadata(metadata map[string]any) (string, error) {
	query := "INSERT INTO datasets (file_name, table_name, uploaded_by, size, description) VALUES ($1, $2, $3, $4, $5) RETURNING id"

	var id string
	err := config.Storage.QueryRow(query, metadata["name"], metadata["tableName"], metadata["author"], metadata["size"], metadata["description"]).Scan(&id)
	if err != nil {
		fmt.Println("unable to execute insert query", err)
		return "", err
	}
	fmt.Println("StoreDatasetMetadata: ", id)
	return id, nil
}

func StoreDatasetColumns(columns [][]string, datasetId string) error {
	var query strings.Builder
	query.WriteString("INSERT INTO dataset_columns (dataset_id, column_name, column_type) VALUES ")
	vals := []any{}
	for i, col := range columns {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		vals = append(vals, datasetId, col[0], col[1])
	}

	_, err := config.Storage.Exec(query.String(), vals...)
	if err != nil {
		fmt.Println("unable to insert dataset columns", err)
		return err
	}
	return nil
}

// ListDatasets returns metadata for all datasets.
func ListDatasets() ([]map[string]any, error) {
	rows, err := config.Storage.Query(
		`SELECT id, file_name, table_name, size, uploaded_by, description, created_at
		 FROM datasets ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var datasets []map[string]any
	for rows.Next() {
		var id, fileName, tableName, uploadedBy, description string
		var size int64
		var createdAt any
		if err := rows.Scan(&id, &fileName, &tableName, &size, &uploadedBy, &description, &createdAt); err != nil {
			return nil, err
		}
		datasets = append(datasets, map[string]any{
			"id":          id,
			"file_name":   fileName,
			"table_name":  tableName,
			"size":        size,
			"uploaded_by": uploadedBy,
			"description": description,
			"created_at":  createdAt,
		})
	}
	return datasets, nil
}

// GetDatasetRows reads all rows from a dataset's dynamic table.
func GetDatasetRows(tableName string) ([]map[string]any, error) {
	rows, err := config.Storage.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("unable to query dataset table: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	return result, nil
}

// StoreRawFile uploads a file to MinIO under the datasets bucket.
func StoreRawFile(datasetId string, filePath string, fileName string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat file: %w", err)
	}

	objectKey := fmt.Sprintf("%s/%s", datasetId, fileName)
	_, err = config.FileStorage.PutObject(
		context.Background(),
		config.DatasetsBucket,
		objectKey,
		f,
		info.Size(),
		minio.PutObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("unable to upload file to MinIO: %w", err)
	}

	return nil
}

func mapToDatabase(value string) string {
	switch value {
	case "IS_NUMERICAL":
		return "DOUBLE PRECISION"
	case "IS_BOOLEAN":
		return "BOOLEAN"
	case "IS_DATE":
		return "TIMESTAMP"
	case "IS_CATEGORICAL":
		return "TEXT"
	default:
		return "TEXT"
	}
}
