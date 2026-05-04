package dataset

import (
	"github.com/telmocbarros/data-pulse/internal/columntype"
	"github.com/telmocbarros/data-pulse/internal/models"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
)

const (
	categoryBreakdownDefaultLimit = 20
	categoryBreakdownMaxLimit     = 100
	scatterDefaultLimit           = 1000
	scatterMaxLimit               = 5000
)

// VisualizationType enumerates the supported chart types.
type VisualizationType string

const (
	VizHistogram         VisualizationType = "histogram"
	VizScatter           VisualizationType = "scatter"
	VizTimeseries        VisualizationType = "timeseries"
	VizCorrelationMatrix VisualizationType = "correlation-matrix"
	VizCategoryBreakdown VisualizationType = "category-breakdown"
)

// IsValid reports whether v names a known chart type.
func (v VisualizationType) IsValid() bool {
	switch v {
	case VizHistogram, VizScatter, VizTimeseries, VizCorrelationMatrix, VizCategoryBreakdown:
		return true
	}
	return false
}

// HistogramParams selects the bin count. When Bins == 0 (or unset), the
// cached buckets from the profiler run are returned; any other value triggers
// live recompute against the dataset table.
type HistogramParams struct {
	Bins int `json:"bins"`
}

// HistogramResult is keyed by column name; each value is the bucket list for
// that column.
type HistogramResult struct {
	Buckets map[string][]models.HistogramBucket `json:"buckets"`
}

// ScatterParams selects the (x, y) pair and the row cap. When Sample is true
// the rows are drawn via TABLESAMPLE BERNOULLI rather than a head-of-table
// LIMIT, giving a representative spread across the dataset.
type ScatterParams struct {
	X      string `json:"x"`
	Y      string `json:"y"`
	Limit  int    `json:"limit"`
	Sample bool   `json:"sample"`
}

// ScatterResult is the response shape for a scatter visualization.
type ScatterResult struct {
	X    string                    `json:"x"`
	Y    string                    `json:"y"`
	Data []repository.ScatterPoint `json:"data"`
}

// TimeseriesParams selects the time axis, one or more numeric series, and
// optionally filters to a single value of a categorical column.
// When GroupBy is "day", "week", or "month", x values are truncated to that
// unit and each y is aggregated via Aggregate (default "avg").
type TimeseriesParams struct {
	X           string   `json:"x"`
	Y           []string `json:"y"`
	Series      string   `json:"series"`
	SeriesValue string   `json:"seriesValue"`
	GroupBy     string   `json:"groupBy"`   // "" | "day" | "week" | "month"
	Aggregate   string   `json:"aggregate"` // "" | "avg" | "sum" | "min" | "max" | "count"
}

// TimeseriesResult is the response shape for a time-series visualization.
type TimeseriesResult struct {
	X           string           `json:"x"`
	Y           []string         `json:"y"`
	Series      string           `json:"series,omitempty"`
	SeriesValue string           `json:"seriesValue,omitempty"`
	GroupBy     string           `json:"groupBy,omitempty"`
	Aggregate   string           `json:"aggregate,omitempty"`
	Data        []map[string]any `json:"data"`
}

// Aliases for the canonical timeseries-grouping allowlists owned by the
// repository layer (the maps' values are interpolated into SQL, so the
// repo is the right place for the source of truth). The service uses
// them for caller-side input validation in TimeseriesParams.resolve.
var (
	validGroupByUnits = repository.TimeseriesGroupByUnits
	validAggregateFns = repository.TimeseriesAggregateFns
)

// CorrelationMatrixParams has no caller-tunable fields today.
type CorrelationMatrixParams struct{}

// CorrelationMatrixResult is the response shape for a correlation matrix.
// Pairs are symmetric: matrix[a][b] == matrix[b][a]; matrix[c][c] == 1.
type CorrelationMatrixResult struct {
	Matrix map[string]map[string]float64 `json:"matrix"`
}

// CategoryBreakdownParams selects the categorical column and the row cap.
// When GroupBy names a second categorical column, the breakdown is 2D:
// each top-level value is split by its GroupBy value, and Limit caps the
// number of top-level values (not total rows).
type CategoryBreakdownParams struct {
	Column  string `json:"column"`
	GroupBy string `json:"groupBy"`
	Limit   int    `json:"limit"`
}

// CategoryBreakdownCell is a single value/count pair. For 2D breakdowns the
// Group field carries the GroupBy column's value for that row.
type CategoryBreakdownCell struct {
	Value string `json:"value"`
	Group string `json:"group,omitempty"`
	Count int64  `json:"count"`
}

// CategoryBreakdownResult is the response shape for a category breakdown.
// GroupBy is omitted in 1D responses.
type CategoryBreakdownResult struct {
	Column  string                  `json:"column"`
	GroupBy string                  `json:"groupBy,omitempty"`
	Data    []CategoryBreakdownCell `json:"data"`
}

const histogramMaxBins = 200

// GetHistogram returns histogram buckets for every numeric column.
// With Bins == 0 the cached profiler buckets are used (fast path).
// Any other value triggers live recompute via Postgres WIDTH_BUCKET.
func GetHistogram(id string, p HistogramParams) (HistogramResult, error) {
	if p.Bins < 0 {
		return HistogramResult{}, invalidParams("bins must be >= 0, got %d", p.Bins)
	}
	if p.Bins > histogramMaxBins {
		return HistogramResult{}, invalidParams("bins must be <= %d, got %d", histogramMaxBins, p.Bins)
	}

	if p.Bins == 0 {
		buckets, err := repository.GetHistogramFromDataset(id, 10)
		if err != nil {
			return HistogramResult{}, err
		}
		return HistogramResult{Buckets: buckets}, nil
	}

	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return HistogramResult{}, translateRepoErr(err)
	}
	numCols := columnsOfType(columns, columntype.Numerical)
	buckets, err := repository.ComputeHistogramFromDataset(tableName, numCols, p.Bins)
	if err != nil {
		return HistogramResult{}, err
	}
	return HistogramResult{Buckets: buckets}, nil
}

// GetScatter returns up to Limit (x, y) pairs for the chosen numeric columns.
// Defaults: x = first numeric column, y = second numeric column distinct from x.
func GetScatter(id string, p ScatterParams) (ScatterResult, error) {
	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return ScatterResult{}, translateRepoErr(err)
	}
	if err := p.resolve(columns); err != nil {
		return ScatterResult{}, err
	}
	var points []repository.ScatterPoint
	if p.Sample {
		points, err = repository.SampleScatterPlotFromDataset(tableName, p.X, p.Y, p.Limit)
	} else {
		points, err = repository.GetScatterPlotFromDataset(tableName, p.X, p.Y, p.Limit)
	}
	if err != nil {
		return ScatterResult{}, err
	}
	return ScatterResult{X: p.X, Y: p.Y, Data: points}, nil
}

func (p *ScatterParams) resolve(columns map[string]string) error {
	numCols := columnsOfType(columns, columntype.Numerical)

	if p.X == "" {
		if len(numCols) == 0 {
			return invalidParams("dataset has no numeric column for x axis")
		}
		p.X = numCols[0]
	} else if columns[p.X] != columntype.Numerical {
		return invalidParams("x column %q is not numeric", p.X)
	}

	if p.Y == "" {
		for _, c := range numCols {
			if c != p.X {
				p.Y = c
				break
			}
		}
		if p.Y == "" {
			return invalidParams("dataset needs at least two numeric columns for scatter")
		}
	} else if columns[p.Y] != columntype.Numerical {
		return invalidParams("y column %q is not numeric", p.Y)
	}

	if p.X == p.Y {
		return invalidParams("x and y must be different columns")
	}

	if p.Limit <= 0 {
		p.Limit = scatterDefaultLimit
	}
	if p.Limit > scatterMaxLimit {
		p.Limit = scatterMaxLimit
	}
	return nil
}

// GetTimeseries returns rows ordered by the time column, projecting each
// requested numeric column. When Series is set, rows are filtered where
// Series = SeriesValue. When GroupBy is set, x is truncated to that unit and
// each y is aggregated via Aggregate.
func GetTimeseries(id string, p TimeseriesParams) (TimeseriesResult, error) {
	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return TimeseriesResult{}, translateRepoErr(err)
	}
	if err := p.resolve(columns); err != nil {
		return TimeseriesResult{}, err
	}

	var points []map[string]any
	if p.GroupBy != "" {
		points, err = repository.GetTimeseriesPlotGrouped(
			tableName, p.X, p.Y, p.Series, p.SeriesValue,
			p.GroupBy, validAggregateFns[p.Aggregate],
		)
	} else {
		points, err = repository.GetTimeseriesPlotFromDataset(tableName, p.X, p.Y, p.Series, p.SeriesValue)
	}
	if err != nil {
		return TimeseriesResult{}, err
	}
	return TimeseriesResult{
		X:           p.X,
		Y:           p.Y,
		Series:      p.Series,
		SeriesValue: p.SeriesValue,
		GroupBy:     p.GroupBy,
		Aggregate:   p.Aggregate,
		Data:        points,
	}, nil
}

func (p *TimeseriesParams) resolve(columns map[string]string) error {
	if p.X == "" {
		dateCols := columnsOfType(columns, columntype.Date)
		if len(dateCols) == 0 {
			return invalidParams("dataset has no date column for x axis")
		}
		p.X = dateCols[0]
	} else if columns[p.X] != columntype.Date {
		return invalidParams("x column %q is not a date", p.X)
	}

	if len(p.Y) == 0 {
		numCols := columnsOfType(columns, columntype.Numerical)
		if len(numCols) == 0 {
			return invalidParams("dataset has no numeric column for y axis")
		}
		p.Y = []string{numCols[0]}
	} else {
		for _, y := range p.Y {
			if columns[y] != columntype.Numerical {
				return invalidParams("y column %q is not numeric", y)
			}
		}
	}

	if p.Series != "" {
		if columns[p.Series] != columntype.Categorical {
			return invalidParams("series column %q is not categorical", p.Series)
		}
		if p.SeriesValue == "" {
			return invalidParams("seriesValue is required when series is set")
		}
	}

	if p.GroupBy != "" {
		if _, ok := validGroupByUnits[p.GroupBy]; !ok {
			return invalidParams("groupBy must be one of day|week|month, got %q", p.GroupBy)
		}
		if p.Aggregate == "" {
			p.Aggregate = "avg"
		}
		if _, ok := validAggregateFns[p.Aggregate]; !ok {
			return invalidParams("aggregate must be one of avg|sum|min|max|count, got %q", p.Aggregate)
		}
	} else if p.Aggregate != "" {
		return invalidParams("aggregate requires groupBy to be set")
	}
	return nil
}

// GetCorrelationMatrix returns Pearson correlations across all numeric
// columns. Falls back to on-demand compute if no rows are cached.
func GetCorrelationMatrix(id string, _ CorrelationMatrixParams) (CorrelationMatrixResult, error) {
	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return CorrelationMatrixResult{}, translateRepoErr(err)
	}
	numCols := columnsOfType(columns, columntype.Numerical)
	matrix, err := repository.GetCorrelationMatrixFromDataset(id, tableName, numCols)
	if err != nil {
		return CorrelationMatrixResult{}, err
	}
	return CorrelationMatrixResult{Matrix: matrix}, nil
}

// GetCategoryBreakdown returns the most-frequent values for the chosen
// categorical column, capped at Limit. When GroupBy is set, each value is
// split by the GroupBy column via a live SQL aggregate (no profiler cache).
func GetCategoryBreakdown(id string, p CategoryBreakdownParams) (CategoryBreakdownResult, error) {
	tableName, columns, err := repository.GetDatasetById(id)
	if err != nil {
		return CategoryBreakdownResult{}, translateRepoErr(err)
	}
	if err := p.resolve(columns); err != nil {
		return CategoryBreakdownResult{}, err
	}

	if p.GroupBy != "" {
		cells, err := repository.GetCategoryBreakdownGrouped(tableName, p.Column, p.GroupBy, p.Limit)
		if err != nil {
			return CategoryBreakdownResult{}, err
		}
		data := make([]CategoryBreakdownCell, len(cells))
		for i, c := range cells {
			data[i] = CategoryBreakdownCell{Value: c.Value, Group: c.Group, Count: c.Count}
		}
		return CategoryBreakdownResult{Column: p.Column, GroupBy: p.GroupBy, Data: data}, nil
	}

	occurrences, err := profilerRepo.GetCategoryBreakdown(id, p.Column, p.Limit)
	if err != nil {
		return CategoryBreakdownResult{}, err
	}
	data := make([]CategoryBreakdownCell, len(occurrences))
	for i, occ := range occurrences {
		data[i] = CategoryBreakdownCell{Value: occ.Value, Count: occ.Count}
	}
	return CategoryBreakdownResult{Column: p.Column, Data: data}, nil
}

func (p *CategoryBreakdownParams) resolve(columns map[string]string) error {
	if p.Column == "" {
		catCols := columnsOfType(columns, columntype.Categorical)
		if len(catCols) == 0 {
			return invalidParams("dataset has no categorical column")
		}
		p.Column = catCols[0]
	} else if columns[p.Column] != columntype.Categorical {
		return invalidParams("column %q is not categorical", p.Column)
	}

	if p.GroupBy != "" {
		if columns[p.GroupBy] != columntype.Categorical {
			return invalidParams("groupBy column %q is not categorical", p.GroupBy)
		}
		if p.GroupBy == p.Column {
			return invalidParams("groupBy must differ from column")
		}
	}

	if p.Limit <= 0 {
		p.Limit = categoryBreakdownDefaultLimit
	}
	if p.Limit > categoryBreakdownMaxLimit {
		p.Limit = categoryBreakdownMaxLimit
	}
	return nil
}

func columnsOfType(columns map[string]string, want string) []string {
	out := make([]string, 0)
	for name, t := range columns {
		if t == want {
			out = append(out, name)
		}
	}
	// Sort for stable defaults.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
