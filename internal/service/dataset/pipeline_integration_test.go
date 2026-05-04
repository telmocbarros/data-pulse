//go:build integration

// Integration tests for the CSV/JSON pipelines, gated behind the
// `integration` build tag so the default `go test ./...` sweep stays
// fast and DB-free.
//
// Run as:
//
//	go test -tags=integration -v ./internal/service/dataset/...
//
// Prerequisites:
//   - `docker compose up -d` (Postgres on localhost:5432)
//   - `go run ./cmd/migrate` (applies internal/repository/migrations/*.sql)
//   - DATABASE_URL set (defaults to the .env value used by the dev server)
//
// If the connection fails, the test is skipped with a hint rather than
// failed — keeps CI clean if the integration prerequisites aren't there.

package dataset

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/sqlsafe"
)

// setupIntegration ensures a connection to the dev DB is open. Calling it
// from each test is safe — config.SetupDatabase is idempotent in effect
// (we short-circuit when Storage is already set).
func setupIntegration(t *testing.T) {
	t.Helper()
	if config.Storage != nil {
		return
	}
	if err := config.SetupDatabase(); err != nil {
		t.Skipf("integration test skipped: %v (run `docker compose up -d && go run ./cmd/migrate` first)", err)
	}
}

// cleanupDataset drops the dataset's dynamic table and deletes the
// metadata row. dataset_columns and dataset_validation_errors are
// cascade-deleted by the FK constraints (verified in migrations
// 02 and 07).
func cleanupDataset(t *testing.T, datasetId string) {
	t.Helper()
	t.Cleanup(func() {
		var tableName string
		err := config.Storage.QueryRow(`SELECT table_name FROM datasets WHERE id=$1`, datasetId).Scan(&tableName)
		if err == nil && tableName != "" {
			if !sqlsafe.IsValidIdentifier(tableName) {
				t.Errorf("cleanup: invalid table name %q", tableName)
				return
			}
			if _, err := config.Storage.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
				t.Logf("cleanup: dropping %s: %v", tableName, err)
			}
		}
		if _, err := config.Storage.Exec(`DELETE FROM datasets WHERE id=$1`, datasetId); err != nil {
			t.Logf("cleanup: deleting dataset %s: %v", datasetId, err)
		}
	})
}

// countRows returns the row count of the named table. Uses identifier
// validation since the table name is interpolated.
func countRows(t *testing.T, tableName string) int {
	t.Helper()
	if !sqlsafe.IsValidIdentifier(tableName) {
		t.Fatalf("invalid table name: %q", tableName)
	}
	var n int
	if err := config.Storage.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&n); err != nil {
		t.Fatalf("count rows in %s: %v", tableName, err)
	}
	return n
}

// countErrorsByKind returns the count of validation errors of the given
// kind for the dataset.
func countErrorsByKind(t *testing.T, datasetId, kind string) int {
	t.Helper()
	var n int
	err := config.Storage.QueryRow(
		`SELECT COUNT(*) FROM dataset_validation_errors WHERE dataset_id=$1 AND kind=$2`,
		datasetId, kind,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count %s errors for %s: %v", kind, datasetId, err)
	}
	return n
}

// dynamicTableName looks up the per-dataset table name from datasets.
func dynamicTableName(t *testing.T, datasetId string) string {
	t.Helper()
	var name string
	if err := config.Storage.QueryRow(`SELECT table_name FROM datasets WHERE id=$1`, datasetId).Scan(&name); err != nil {
		t.Fatalf("lookup table_name for %s: %v", datasetId, err)
	}
	return name
}

// TestCsvPipelineEndToEnd drives ProcessCsvFile with a fixture mixing
// valid rows, a type_mismatch, missing_value, and a malformed_row, and
// asserts on what landed in the database.
func TestCsvPipelineEndToEnd(t *testing.T) {
	setupIntegration(t)

	// Header derives 3 columns: name (categorical), age (numerical),
	// email (categorical). Then 5 data rows:
	//   - Alice: valid
	//   - Bob: type_mismatch on age (string in a numeric column)
	//   - charlie: missing_value on name
	//   - Diana: malformed_row (4 cells > 3 expected) — DROPPED by csvSource
	//   - Eve: missing_value on email
	const fixture = `name,age,email
Alice,30,alice@example.com
Bob,not-a-number,bob@example.com
,25,charlie@example.com
Diana,40,diana@example.com,extra-cell
Eve,50,
`

	ctx := context.Background()
	datasetId, err := ProcessCsvFile(ctx, strings.NewReader(fixture), "fixture.csv", int64(len(fixture)), func(int) {})
	if err != nil {
		t.Fatalf("ProcessCsvFile: %v", err)
	}
	if _, err := uuid.Parse(datasetId); err != nil {
		t.Fatalf("returned datasetId is not a uuid: %q", datasetId)
	}
	cleanupDataset(t, datasetId)

	// Assertion 1: datasets row exists with the right file_name.
	var fileName string
	if err := config.Storage.QueryRow(
		`SELECT file_name FROM datasets WHERE id=$1`, datasetId,
	).Scan(&fileName); err != nil {
		t.Fatalf("datasets row not found: %v", err)
	}
	if fileName != "fixture.csv" {
		t.Errorf("file_name = %q, want %q", fileName, "fixture.csv")
	}

	// Assertion 2: 3 dataset_columns rows.
	var colCount int
	if err := config.Storage.QueryRow(
		`SELECT COUNT(*) FROM dataset_columns WHERE dataset_id=$1`, datasetId,
	).Scan(&colCount); err != nil {
		t.Fatalf("count dataset_columns: %v", err)
	}
	if colCount != 3 {
		t.Errorf("dataset_columns count = %d, want 3", colCount)
	}

	// Assertion 3: dynamic table has 4 rows.
	// Alice: valid -> stored.
	// Bob: type_mismatch on age (string in a Numerical column). The
	//   validator NULLs the bad cell but keeps the row, so it is stored
	//   with age IS NULL — the type_mismatch is logged separately.
	// charlie: missing_value on name; cell stored as NULL.
	// Diana: malformed_row (count mismatch) -> DROPPED by csvSource.
	// Eve: missing_value on email; cell stored as NULL.
	tableName := dynamicTableName(t, datasetId)
	if got := countRows(t, tableName); got != 4 {
		t.Errorf("%s row count = %d, want 4 (Diana should be dropped)", tableName, got)
	}

	// Assertion 4: Bob's row must have age=NULL in the dynamic table.
	// This is the pin for the "type_mismatch cells are NULL'd, not
	// crashed" contract. Before the fix, sending the string to a
	// DOUBLE PRECISION column would have rolled back the entire
	// transaction and stored 0 rows.
	if !sqlsafe.IsValidIdentifier(tableName) {
		t.Fatalf("invalid table name: %q", tableName)
	}
	var bobAge sql.NullFloat64
	err = config.Storage.QueryRow(
		fmt.Sprintf("SELECT age FROM %s WHERE name = $1", tableName), "Bob",
	).Scan(&bobAge)
	if err != nil {
		t.Fatalf("query Bob row: %v", err)
	}
	if bobAge.Valid {
		t.Errorf("Bob's age = %v, want NULL (type_mismatch should have NULL'd it)", bobAge.Float64)
	}

	// Assertion 5: validation errors per kind.
	if got := countErrorsByKind(t, datasetId, "type_mismatch"); got != 1 {
		t.Errorf("type_mismatch errors = %d, want 1 (Bob's age)", got)
	}
	if got := countErrorsByKind(t, datasetId, "missing_value"); got != 2 {
		t.Errorf("missing_value errors = %d, want 2 (charlie's name + Eve's email)", got)
	}
	if got := countErrorsByKind(t, datasetId, "malformed_row"); got != 1 {
		t.Errorf("malformed_row errors = %d, want 1 (Diana)", got)
	}
}

// TestJsonPipelineEndToEnd drives ProcessJsonFile with an array of four
// rows: one valid (becomes firstRow + bypasses validator), one with a
// type_mismatch, two with missing_value.
func TestJsonPipelineEndToEnd(t *testing.T) {
	setupIntegration(t)

	// Alice is firstRow: consumed during setup, type-derived from her
	// values, pre-sent directly to dataCh (bypasses validator).
	// Bob: type_mismatch on age (string where firstRow had a number).
	// Charlie: missing email key.
	// Diana: missing name key.
	const fixture = `[
{"name":"Alice","age":30,"email":"alice@example.com"},
{"name":"Bob","age":"not-a-number","email":"bob@example.com"},
{"name":"Charlie","age":25},
{"age":40,"email":"diana@example.com"}
]`

	ctx := context.Background()
	datasetId, err := ProcessJsonFile(ctx, strings.NewReader(fixture), "fixture.json", int64(len(fixture)), func(int) {})
	if err != nil {
		t.Fatalf("ProcessJsonFile: %v", err)
	}
	if _, err := uuid.Parse(datasetId); err != nil {
		t.Fatalf("returned datasetId is not a uuid: %q", datasetId)
	}
	cleanupDataset(t, datasetId)

	// All four rows reach storage:
	//   Alice (firstRow, pre-sent) + Bob + Charlie + Diana.
	tableName := dynamicTableName(t, datasetId)
	if got := countRows(t, tableName); got != 4 {
		t.Errorf("%s row count = %d, want 4", tableName, got)
	}

	// Pin the new contract: Bob's "not-a-number" age must be stored as
	// NULL (the validator NULLs the bad cell). Before the fix, sending
	// the string to a DOUBLE PRECISION column would have rolled back
	// the entire transaction.
	if !sqlsafe.IsValidIdentifier(tableName) {
		t.Fatalf("invalid table name: %q", tableName)
	}
	var bobAge sql.NullFloat64
	err = config.Storage.QueryRow(
		fmt.Sprintf("SELECT age FROM %s WHERE name = $1", tableName), "Bob",
	).Scan(&bobAge)
	if err != nil {
		t.Fatalf("query Bob row: %v", err)
	}
	if bobAge.Valid {
		t.Errorf("Bob's age = %v, want NULL", bobAge.Float64)
	}

	if got := countErrorsByKind(t, datasetId, "type_mismatch"); got != 1 {
		t.Errorf("type_mismatch errors = %d, want 1 (Bob)", got)
	}
	if got := countErrorsByKind(t, datasetId, "missing_value"); got != 2 {
		t.Errorf("missing_value errors = %d, want 2 (Charlie + Diana)", got)
	}
	if got := countErrorsByKind(t, datasetId, "malformed_row"); got != 0 {
		t.Errorf("malformed_row errors = %d, want 0 (no decode failures)", got)
	}
}

// TestCsvPipelineCancellation injects a cancellable context and verifies
// the pipeline returns context.Canceled rather than completing normally.
//
// Cancellation is triggered deterministically from the progressFn: when
// runPipeline's validator stage starts (progressFn(30)), we cancel.
// That moment is reachable from every CSV upload — past metadata setup,
// before the storage stage commits — so the test exercises the same
// failure mode regardless of host speed.
func TestCsvPipelineCancellation(t *testing.T) {
	setupIntegration(t)

	// 100 rows of valid data so the storage stage has work in flight
	// when the cancel arrives.
	var b strings.Builder
	b.WriteString("name,age,email\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&b, "User%d,%d,user%d@example.com\n", i, 20+i, i)
	}
	fixture := b.String()

	ctx, cancel := context.WithCancel(context.Background())
	progressFn := func(pct int) {
		// runPipeline's validator goroutine calls progressFn(30) as
		// its first action. That's our cancellation barrier — fires
		// after metadata is committed, before storage finishes.
		if pct == 30 {
			cancel()
		}
	}

	datasetId, err := ProcessCsvFile(ctx, strings.NewReader(fixture), "cancel.csv", int64(len(fixture)), progressFn)

	// The metadata row was created during setup (progressFn(10) fires
	// before the cancellation barrier), so cleanup always has something
	// to do here.
	if datasetId != "" {
		if _, parseErr := uuid.Parse(datasetId); parseErr == nil {
			cleanupDataset(t, datasetId)
		}
	}

	if err == nil {
		t.Fatal("expected an error from cancelled pipeline, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want errors.Is(err, context.Canceled)", err)
	}
}
