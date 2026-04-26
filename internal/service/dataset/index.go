package dataset

import (
	"errors"
	"fmt"
	"slices"
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
	if !slices.Contains(datasetGraphTypes, graphType) {
		return nil, errors.New("Please insert a valid value for graphtype")
	}

	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return nil, err
	}
	chart := make(map[string]any)
	switch graphType {
	case "histogram":
		histogramData, err := repository.GetHistogramFromDataset(id, 10)
		if err != nil {
			return nil, err
		}
		for key, val := range histogramData {
			bucketList := make([]any, len(val))
			for i, bucket := range val {
				bucketList[i] = bucket
			}
			chart[key] = bucketList
		}
	case "scatter":
		// TODO: chosen columns should be dynamic.
		scatterPlotData, err := repository.GetScatterPlotFromDataset(id, tableName, []string{"rating", "price"})
		if err != nil {
			return nil, err
		}
		for key, val := range scatterPlotData {
			pointList := make([]any, len(val))
			for i, data := range val {
				pointList[i] = data
			}
			chart[key] = pointList
		}
	case "timeseries":
		// TODO: chosen columns should be dynamic.
		timeseriesPlotData, err := repository.GetTimeseriesPlotFromDataset(id, tableName, "Webcam HD")
		if err != nil {
			return nil, err
		}
		points := make([]any, len(timeseriesPlotData))
		for i, point := range timeseriesPlotData {
			points[i] = point
		}
		chart["data"] = points
	case "correlation-matrix":
		numericCols := make([]string, 0, len(columns))
		for name, colType := range columns {
			if colType == columntype.IS_NUMERICAL {
				numericCols = append(numericCols, name)
			}
		}
		matrix, err := repository.GetCorrelationMatrixFromDataset(id, tableName, numericCols)
		if err != nil {
			return nil, err
		}
		chart["matrix"] = matrix
	case "category-breakdown":
		fmt.Println("Not yet supported")
	}

	return map[string]any{
		"tableName": tableName,
		"columns":   columns,
		"chart":     chart,
	}, nil

}
