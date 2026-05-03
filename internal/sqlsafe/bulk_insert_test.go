package sqlsafe

import (
	"database/sql"
	"strings"
	"testing"
)

// captureExecer records the query and args of the most recent Exec call.
// It satisfies the unexported execer interface BulkInsert depends on.
type captureExecer struct {
	gotQuery string
	gotArgs  []any
	calls    int
}

func (c *captureExecer) Exec(query string, args ...any) (sql.Result, error) {
	c.calls++
	c.gotQuery = query
	c.gotArgs = args
	return nil, nil
}

func TestBulkInsertEmptyRowsIsNoOp(t *testing.T) {
	c := &captureExecer{}
	if err := BulkInsert(c, "t", []string{"a"}, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.calls != 0 {
		t.Errorf("expected no Exec call for empty rows, got %d", c.calls)
	}
}

func TestBulkInsertRejectsBadIdentifiers(t *testing.T) {
	c := &captureExecer{}
	rows := [][]any{{1}}
	if err := BulkInsert(c, "1bad", []string{"a"}, rows); err == nil {
		t.Error("expected error for bad table name, got nil")
	}
	if err := BulkInsert(c, "t", []string{"bad-col"}, rows); err == nil {
		t.Error("expected error for bad column name, got nil")
	}
	if c.calls != 0 {
		t.Errorf("expected no Exec on validation failure, got %d", c.calls)
	}
}

func TestBulkInsertRejectsEmptyColumns(t *testing.T) {
	c := &captureExecer{}
	if err := BulkInsert(c, "t", nil, [][]any{{1}}); err == nil {
		t.Error("expected error for empty columns, got nil")
	}
	if c.calls != 0 {
		t.Errorf("expected no Exec on validation failure, got %d", c.calls)
	}
}

func TestBulkInsertRejectsRowLengthMismatch(t *testing.T) {
	c := &captureExecer{}
	rows := [][]any{{1, "ok"}, {2}} // second row too short
	err := BulkInsert(c, "t", []string{"a", "b"}, rows)
	if err == nil {
		t.Fatal("expected error for row length mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "row 1") {
		t.Errorf("error should name the offending row index, got: %v", err)
	}
	if c.calls != 0 {
		t.Errorf("expected no Exec on validation failure, got %d", c.calls)
	}
}

func TestBulkInsertBuildsCorrectQuery(t *testing.T) {
	c := &captureExecer{}
	rows := [][]any{
		{"id1", "alpha", 10},
		{"id2", "beta", 20},
	}
	if err := BulkInsert(c, "tbl", []string{"id", "name", "n"}, rows); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantQuery := "INSERT INTO tbl (id, name, n) VALUES ($1, $2, $3), ($4, $5, $6)"
	if c.gotQuery != wantQuery {
		t.Errorf("query mismatch:\n got: %q\nwant: %q", c.gotQuery, wantQuery)
	}
	wantArgs := []any{"id1", "alpha", 10, "id2", "beta", 20}
	if len(c.gotArgs) != len(wantArgs) {
		t.Fatalf("args length: got %d, want %d", len(c.gotArgs), len(wantArgs))
	}
	for i, want := range wantArgs {
		if c.gotArgs[i] != want {
			t.Errorf("args[%d] = %v, want %v", i, c.gotArgs[i], want)
		}
	}
}
