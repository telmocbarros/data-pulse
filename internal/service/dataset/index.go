package dataset

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
)

const (
	categoryBreakdownDefaultLimit = 20
	categoryBreakdownMaxLimit     = 100
	scatterDefaultLimit           = 1000
	scatterMaxLimit               = 5000
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

// resolveTimeseriesParams validates and defaults the timeseries query params
// against the dataset's column types. Returns x, ys, series, seriesValue.
// series is "" when no filter is requested.
func resolveTimeseriesParams(query url.Values, columns map[string]string) (string, []string, string, string, error) {
	x := query.Get("x")
	if x == "" {
		dateCols := columnsOfType(columns, columntype.IS_DATE)
		if len(dateCols) == 0 {
			return "", nil, "", "", errors.New("dataset has no date column for x axis")
		}
		x = dateCols[0]
	} else if columns[x] != columntype.IS_DATE {
		return "", nil, "", "", fmt.Errorf("x column %q is not a date", x)
	}

	ys := query["y"]
	if len(ys) == 0 {
		numCols := columnsOfType(columns, columntype.IS_NUMERICAL)
		if len(numCols) == 0 {
			return "", nil, "", "", errors.New("dataset has no numeric column for y axis")
		}
		ys = []string{numCols[0]}
	} else {
		for _, y := range ys {
			if columns[y] != columntype.IS_NUMERICAL {
				return "", nil, "", "", fmt.Errorf("y column %q is not numeric", y)
			}
		}
	}

	series := query.Get("series")
	seriesValue := query.Get("seriesValue")
	if series != "" {
		if columns[series] != columntype.IS_CATEGORICAL {
			return "", nil, "", "", fmt.Errorf("series column %q is not categorical", series)
		}
		if seriesValue == "" {
			return "", nil, "", "", errors.New("seriesValue is required when series is set")
		}
	}
	return x, ys, series, seriesValue, nil
}

// resolveCategoryBreakdownParams validates and defaults the category-breakdown
// query params. column defaults to the first IS_CATEGORICAL column; limit
// defaults to categoryBreakdownDefaultLimit and is capped at categoryBreakdownMaxLimit.
func resolveCategoryBreakdownParams(query url.Values, columns map[string]string) (string, int, error) {
	column := query.Get("column")
	if column == "" {
		catCols := columnsOfType(columns, columntype.IS_CATEGORICAL)
		if len(catCols) == 0 {
			return "", 0, errors.New("dataset has no categorical column")
		}
		column = catCols[0]
	} else if columns[column] != columntype.IS_CATEGORICAL {
		return "", 0, fmt.Errorf("column %q is not categorical", column)
	}

	limit := categoryBreakdownDefaultLimit
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return "", 0, fmt.Errorf("invalid limit: %q", raw)
		}
		limit = parsed
	}
	if limit > categoryBreakdownMaxLimit {
		limit = categoryBreakdownMaxLimit
	}
	return column, limit, nil
}

// resolveScatterParams validates and defaults the scatter query params.
// x and y default to the first and second IS_NUMERICAL columns respectively.
// limit defaults to scatterDefaultLimit and is capped at scatterMaxLimit.
func resolveScatterParams(query url.Values, columns map[string]string) (string, string, int, error) {
	numCols := columnsOfType(columns, columntype.IS_NUMERICAL)

	x := query.Get("x")
	if x == "" {
		if len(numCols) == 0 {
			return "", "", 0, errors.New("dataset has no numeric column for x axis")
		}
		x = numCols[0]
	} else if columns[x] != columntype.IS_NUMERICAL {
		return "", "", 0, fmt.Errorf("x column %q is not numeric", x)
	}

	y := query.Get("y")
	if y == "" {
		other := ""
		for _, c := range numCols {
			if c != x {
				other = c
				break
			}
		}
		if other == "" {
			return "", "", 0, errors.New("dataset needs at least two numeric columns for scatter")
		}
		y = other
	} else if columns[y] != columntype.IS_NUMERICAL {
		return "", "", 0, fmt.Errorf("y column %q is not numeric", y)
	}

	if x == y {
		return "", "", 0, errors.New("x and y must be different columns")
	}

	limit := scatterDefaultLimit
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return "", "", 0, fmt.Errorf("invalid limit: %q", raw)
		}
		limit = parsed
	}
	if limit > scatterMaxLimit {
		limit = scatterMaxLimit
	}
	return x, y, limit, nil
}

func columnsOfType(columns map[string]string, want string) []string {
	out := make([]string, 0)
	for name, t := range columns {
		if t == want {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func GetSingleDataset(id string, graphType string, query url.Values) (map[string]any, error) {
	if len(graphType) > 0 && !slices.Contains(datasetGraphTypes, graphType) {
		log.Println("Invalid graph type")
		return nil, errors.New("Please insert a valid value for graphtype")
	}

	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Computing %s for dataset %s (table %s)\n", graphType, id, tableName)
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
		fmt.Printf("Histogram computed for %d columns\n", len(histogramData))
	case "scatter":
		x, y, limit, err := resolveScatterParams(query, columns)
		if err != nil {
			return nil, err
		}
		points, err := repository.GetScatterPlotFromDataset(tableName, x, y, limit)
		if err != nil {
			return nil, err
		}
		chart["x"] = x
		chart["y"] = y
		chart["data"] = points
		fmt.Printf("Scatter computed with %d points (x=%s, y=%s)\n", len(points), x, y)
	case "timeseries":
		x, ys, series, seriesValue, err := resolveTimeseriesParams(query, columns)
		if err != nil {
			return nil, err
		}
		points, err := repository.GetTimeseriesPlotFromDataset(tableName, x, ys, series, seriesValue)
		if err != nil {
			return nil, err
		}
		chart["x"] = x
		chart["y"] = ys
		if series != "" {
			chart["series"] = series
			chart["seriesValue"] = seriesValue
		}
		chart["data"] = points
		fmt.Printf("Timeseries computed with %d points (x=%s, y=%v)\n", len(points), x, ys)
	case "correlation-matrix":
		numericCols := make([]string, 0, len(columns))
		for name, colType := range columns {
			if colType == columntype.IS_NUMERICAL {
				numericCols = append(numericCols, name)
			}
		}
		fmt.Printf("Correlation matrix: %d numeric columns\n", len(numericCols))
		matrix, err := repository.GetCorrelationMatrixFromDataset(id, tableName, numericCols)
		if err != nil {
			return nil, err
		}
		chart["matrix"] = matrix
		fmt.Printf("Correlation matrix computed with %d rows\n", len(matrix))
	case "category-breakdown":
		column, limit, err := resolveCategoryBreakdownParams(query, columns)
		if err != nil {
			return nil, err
		}
		breakdown, err := profilerRepo.GetCategoryBreakdown(id, column, limit)
		if err != nil {
			return nil, err
		}
		data := make([]any, len(breakdown))
		for i, occ := range breakdown {
			data[i] = map[string]any{"value": occ.Value, "count": occ.Count}
		}
		chart["column"] = column
		chart["data"] = data
		fmt.Printf("Category breakdown for %s: %d values\n", column, len(breakdown))
	default:
		log.Println("Graph type not provided")
	}

	return map[string]any{
		"tableName": tableName,
		"columns":   columns,
		"chart":     chart,
	}, nil

}
