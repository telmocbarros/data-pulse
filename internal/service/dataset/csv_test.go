package dataset

import (
	"context"
	"encoding/csv"
	"strings"
	"testing"
)

// drainCsvSource pulls all rows from src.Next, returning the rows it
// yielded plus the validation errors emitted via errCh. Caller-friendly
// wrapper around the source contract for tests.
func drainCsvSource(t *testing.T, src *csvSource) (rows []numbered[csvRecord], errs []ValidationError) {
	t.Helper()
	ctx := context.Background()
	errCh := make(chan ValidationError, 16)
	for {
		row, ok, err := src.Next(ctx, errCh)
		if err != nil {
			t.Fatalf("Next returned fatal err: %v", err)
		}
		if !ok {
			break
		}
		rows = append(rows, row)
	}
	close(errCh)
	for ve := range errCh {
		errs = append(errs, ve)
	}
	return rows, errs
}

// newCsvSource builds a csvSource over an in-memory CSV body. The body
// must NOT include a header row — csvSource expects the reader's first
// Read to be the first data row, mirroring how ProcessCsvFile sets it
// up (header consumed in setup before the source is constructed).
func newCsvSource(body string, expectedColumns int) *csvSource {
	r := csv.NewReader(strings.NewReader(body))
	r.FieldsPerRecord = -1 // tolerant mode, matches ProcessCsvFile setup
	return &csvSource{
		reader:          r,
		expectedColumns: expectedColumns,
		rowNumber:       2, // matches ProcessCsvFile's initialization
	}
}

func TestCsvSourceNormalRows(t *testing.T) {
	src := newCsvSource("a,b,c\nd,e,f\n", 3)
	rows, errs := drainCsvSource(t, src)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if got := rows[0].Data; got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("row 0 = %v, want [a b c]", got)
	}
	if rows[0].Row != 2 || rows[1].Row != 3 {
		t.Errorf("row labels = (%d, %d), want (2, 3)", rows[0].Row, rows[1].Row)
	}
	if len(errs) != 0 {
		t.Errorf("expected no errors on well-formed input, got %d", len(errs))
	}
}

func TestCsvSourceOverflowRowIsDropped(t *testing.T) {
	// Second row has 4 cells; expected is 3. csvSource must emit
	// malformed_row and NOT forward the row (so the validator never sees
	// it and never panics on v.headers[idx]).
	src := newCsvSource("a,b,c\nd,e,f,g\nh,i,j\n", 3)
	rows, errs := drainCsvSource(t, src)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (overflow row dropped)", len(rows))
	}
	if rows[0].Row != 2 || rows[1].Row != 4 {
		t.Errorf("row labels = (%d, %d), want (2, 4) — row 3 was dropped", rows[0].Row, rows[1].Row)
	}
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1 (malformed_row for the overflow)", len(errs))
	}
	if errs[0].Kind != "malformed_row" {
		t.Errorf("error kind = %q, want malformed_row", errs[0].Kind)
	}
	if errs[0].Row != 3 {
		t.Errorf("error row = %d, want 3", errs[0].Row)
	}
	if errs[0].Expected != "3 columns" || errs[0].Received != "4 columns" {
		t.Errorf("error expected/received = %q/%q, want '3 columns'/'4 columns'", errs[0].Expected, errs[0].Received)
	}
}

func TestCsvSourceUnderflowRowIsDropped(t *testing.T) {
	// Second row has 2 cells; expected is 3. Same skip-row treatment.
	src := newCsvSource("a,b,c\nd,e\nh,i,j\n", 3)
	rows, errs := drainCsvSource(t, src)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (underflow row dropped)", len(rows))
	}
	if rows[0].Row != 2 || rows[1].Row != 4 {
		t.Errorf("row labels = (%d, %d), want (2, 4)", rows[0].Row, rows[1].Row)
	}
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if errs[0].Kind != "malformed_row" || errs[0].Received != "2 columns" {
		t.Errorf("error = %+v, want kind=malformed_row received='2 columns'", errs[0])
	}
}

func TestCsvSourceEofImmediately(t *testing.T) {
	src := newCsvSource("", 3)
	rows, errs := drainCsvSource(t, src)
	if len(rows) != 0 {
		t.Errorf("got %d rows, want 0 on empty input", len(rows))
	}
	if len(errs) != 0 {
		t.Errorf("got %d errors, want 0 on empty input", len(errs))
	}
}
