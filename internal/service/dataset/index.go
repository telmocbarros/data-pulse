package dataset

import (
	"errors"
	"fmt"
	"time"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
)

var datasetGraphTypes = []string{"histogram", "scatter", "timeseries", "correlation-matrix", "category-breakdown"}

func extractColumns(row map[string]any) [][]string {
	columns := make([][]string, 0, len(row))
	for key, val := range row {
		columns = append(columns, []string{key, goTypeToDBType(val)})
	}
	return columns
}

func goTypeToDBType(val any) string {
	switch val.(type) {
	case time.Time:
		return columntype.IS_DATE
	case float64, float32, int, int8, int16, int32, int64:
		return columntype.IS_NUMERICAL
	case bool:
		return columntype.IS_BOOLEAN
	default:
		return columntype.IS_CATEGORICAL
	}
}

func GetSingleDataset(id string, graphType string) (map[string]any, error) {
	for index, value := range datasetGraphTypes {
		if graphType == value {
			break
		}
		if index == len(datasetGraphTypes) {
			return nil, errors.New("Invalid graphtype")
		}
	}
	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return nil, err
	}
	fmt.Println("Mambos------> ", tableName)

	chart, err := repository.GetDatasetChart(id, graphType)
	if err != nil {
		return nil, err
	}
	fmt.Println("Outros Mambos------> ", chart)

	return map[string]any{
		"tableName": tableName,
		"columns":   columns,
		"histogram": chart,
	}, nil

}
