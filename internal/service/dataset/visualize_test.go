package dataset

import (
	"errors"
	"testing"

	"github.com/telmocbarros/data-pulse/internal/columntype"
)

func twoNumeric() map[string]string {
	return map[string]string{
		"x": columntype.Numerical,
		"y": columntype.Numerical,
	}
}

func TestScatterResolveDefaults(t *testing.T) {
	cols := twoNumeric()
	p := ScatterParams{}
	if err := p.resolve(cols); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.X == "" || p.Y == "" || p.X == p.Y {
		t.Errorf("expected distinct X/Y defaults, got X=%q Y=%q", p.X, p.Y)
	}
	if p.Limit != scatterDefaultLimit {
		t.Errorf("Limit = %d, want default %d", p.Limit, scatterDefaultLimit)
	}
}

func TestScatterResolveLimitClamping(t *testing.T) {
	cols := twoNumeric()
	p := ScatterParams{X: "x", Y: "y", Limit: scatterMaxLimit + 100}
	if err := p.resolve(cols); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Limit != scatterMaxLimit {
		t.Errorf("Limit = %d, want clamped to %d", p.Limit, scatterMaxLimit)
	}
}

func TestScatterResolveRejectsNonNumericColumns(t *testing.T) {
	cols := map[string]string{
		"x":   columntype.Numerical,
		"cat": columntype.Categorical,
	}
	p := ScatterParams{X: "cat", Y: "x"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams for non-numeric x, got %v", err)
	}
}

func TestScatterResolveRejectsSameColumn(t *testing.T) {
	cols := twoNumeric()
	p := ScatterParams{X: "x", Y: "x"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams for X==Y, got %v", err)
	}
}

func TestScatterResolveRequiresTwoNumericColumns(t *testing.T) {
	cols := map[string]string{"x": columntype.Numerical}
	p := ScatterParams{}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams when only one numeric column, got %v", err)
	}
}

func TestTimeseriesResolveDefaults(t *testing.T) {
	cols := map[string]string{
		"date": columntype.Date,
		"y":    columntype.Numerical,
	}
	p := TimeseriesParams{}
	if err := p.resolve(cols); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.X != "date" {
		t.Errorf("X = %q, want %q", p.X, "date")
	}
	if len(p.Y) != 1 || p.Y[0] != "y" {
		t.Errorf("Y = %v, want [y]", p.Y)
	}
}

func TestTimeseriesResolveGroupByDefaultsAggregateToAvg(t *testing.T) {
	cols := map[string]string{
		"date": columntype.Date,
		"y":    columntype.Numerical,
	}
	p := TimeseriesParams{GroupBy: "day"}
	if err := p.resolve(cols); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Aggregate != "avg" {
		t.Errorf("Aggregate = %q, want default avg", p.Aggregate)
	}
}

func TestTimeseriesResolveRejectsBadGroupBy(t *testing.T) {
	cols := map[string]string{
		"date": columntype.Date,
		"y":    columntype.Numerical,
	}
	p := TimeseriesParams{GroupBy: "year"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams for bad groupBy, got %v", err)
	}
}

func TestTimeseriesResolveRejectsBadAggregate(t *testing.T) {
	cols := map[string]string{
		"date": columntype.Date,
		"y":    columntype.Numerical,
	}
	p := TimeseriesParams{GroupBy: "day", Aggregate: "median"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams for bad aggregate, got %v", err)
	}
}

func TestTimeseriesResolveAggregateRequiresGroupBy(t *testing.T) {
	cols := map[string]string{
		"date": columntype.Date,
		"y":    columntype.Numerical,
	}
	p := TimeseriesParams{Aggregate: "avg"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams when Aggregate set without GroupBy, got %v", err)
	}
}

func TestTimeseriesResolveSeriesRequiresValue(t *testing.T) {
	cols := map[string]string{
		"date": columntype.Date,
		"y":    columntype.Numerical,
		"cat":  columntype.Categorical,
	}
	p := TimeseriesParams{Series: "cat"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams for series without seriesValue, got %v", err)
	}
}

func TestCategoryBreakdownResolveDefaults(t *testing.T) {
	cols := map[string]string{"cat": columntype.Categorical}
	p := CategoryBreakdownParams{}
	if err := p.resolve(cols); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Column != "cat" {
		t.Errorf("Column = %q, want %q", p.Column, "cat")
	}
	if p.Limit != categoryBreakdownDefaultLimit {
		t.Errorf("Limit = %d, want default %d", p.Limit, categoryBreakdownDefaultLimit)
	}
}

func TestCategoryBreakdownResolveRejectsNonCategoricalGroupBy(t *testing.T) {
	cols := map[string]string{
		"cat": columntype.Categorical,
		"n":   columntype.Numerical,
	}
	p := CategoryBreakdownParams{Column: "cat", GroupBy: "n"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams for non-categorical groupBy, got %v", err)
	}
}

func TestCategoryBreakdownResolveRejectsSameGroupByAndColumn(t *testing.T) {
	cols := map[string]string{"cat": columntype.Categorical}
	p := CategoryBreakdownParams{Column: "cat", GroupBy: "cat"}
	err := p.resolve(cols)
	if err == nil || !errors.Is(err, ErrInvalidParams) {
		t.Errorf("expected ErrInvalidParams when groupBy==column, got %v", err)
	}
}

func TestCategoryBreakdownResolveLimitClamping(t *testing.T) {
	cols := map[string]string{"cat": columntype.Categorical}
	p := CategoryBreakdownParams{Column: "cat", Limit: categoryBreakdownMaxLimit + 50}
	if err := p.resolve(cols); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Limit != categoryBreakdownMaxLimit {
		t.Errorf("Limit = %d, want clamped to %d", p.Limit, categoryBreakdownMaxLimit)
	}
}
