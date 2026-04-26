package dataset

import (
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
)

// DatasetColumn is one entry in DatasetMetadata.Columns.
type DatasetColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DatasetMetadata is the response shape for GET /api/datasets/{id}.
type DatasetMetadata struct {
	ID          string          `json:"id"`
	FileName    string          `json:"file_name"`
	TableName   string          `json:"table_name"`
	Size        int64           `json:"size"`
	UploadedBy  string          `json:"uploaded_by"`
	Description string          `json:"description"`
	CreatedAt   any             `json:"created_at"`
	Columns     []DatasetColumn `json:"columns"`
}

// GetDatasetMetadata returns the dataset's row plus column schema.
// Returns an error if the dataset doesn't exist or has been soft-deleted.
func GetDatasetMetadata(id string) (DatasetMetadata, error) {
	row, err := repository.GetDatasetRowById(id)
	if err != nil {
		return DatasetMetadata{}, err
	}

	_, columnTypes, err := repository.GetDatasetById(id)
	if err != nil {
		return DatasetMetadata{}, err
	}

	cols := make([]DatasetColumn, 0, len(columnTypes))
	for name, colType := range columnTypes {
		cols = append(cols, DatasetColumn{Name: name, Type: colType})
	}
	for i := 1; i < len(cols); i++ {
		for j := i; j > 0 && cols[j-1].Name > cols[j].Name; j-- {
			cols[j-1], cols[j] = cols[j], cols[j-1]
		}
	}

	return DatasetMetadata{
		ID:          row.ID,
		FileName:    row.FileName,
		TableName:   row.TableName,
		Size:        row.Size,
		UploadedBy:  row.UploadedBy,
		Description: row.Description,
		CreatedAt:   row.CreatedAt,
		Columns:     cols,
	}, nil
}
