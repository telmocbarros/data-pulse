package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
)

// jsonRow is the parsed shape of one JSON object from the file.
type jsonRow = map[string]any

// jsonSource adapts json.Decoder to RowSource[jsonRow]. It loops past
// decode failures (emitting a malformed_row error and advancing) so the
// orchestrator only sees forwarded rows or EOF.
type jsonSource struct {
	decoder   *json.Decoder
	rowNumber int32 // rows yielded so far; firstRow was row 0 (consumed in setup)
}

func (s *jsonSource) Next(ctx context.Context, errCh chan<- ValidationError) (numbered[jsonRow], bool, error) {
	for s.decoder.More() {
		select {
		case <-ctx.Done():
			return numbered[jsonRow]{}, false, nil
		default:
		}
		s.rowNumber++

		var row jsonRow
		if err := s.decoder.Decode(&row); err != nil {
			select {
			case errCh <- ValidationError{
				Row:    s.rowNumber,
				Column: -1,
				Kind:   "malformed_row",
				Detail: err.Error(),
			}:
			case <-ctx.Done():
				return numbered[jsonRow]{}, false, nil
			}
			continue
		}

		// Mutate strings → parsed Go values in place. Same operation that
		// firstRow received during setup; preserves today's behavior of
		// keeping this work in the parser stage rather than the validator.
		ReadJsonRowAndExtractType(row)
		return numbered[jsonRow]{Row: s.rowNumber, Data: row}, true, nil
	}
	return numbered[jsonRow]{}, false, nil
}

// jsonValidator checks each parsed row against the column types derived
// from the firstRow. Iterates by key (not by index) since JSON rows are
// maps.
//
// Output contract: the returned map always contains every columnKeys
// entry. Missing keys and type-mismatched values both round-trip as
// nil so they reach the dataset table as NULL — keeping the row
// recoverable while the original error is recorded separately in
// dataset_validation_errors. Extra keys present in the input but not
// in columnKeys are dropped (they have no column in the dataset
// table). Sparse output would silently lose columns downstream because
// StoreDataset infers the SQL column list from the first row in each
// batch.
type jsonValidator struct {
	columnKeys  []string
	columnTypes map[string]string
}

func (v *jsonValidator) Validate(row numbered[jsonRow]) (jsonRow, []ValidationError, bool) {
	out := make(jsonRow, len(v.columnKeys))
	var errs []ValidationError
	for _, k := range v.columnKeys {
		val, exists := row.Data[k]
		if !exists {
			errs = append(errs, ValidationError{
				Row:    row.Row,
				Column: -1,
				Kind:   "missing_value",
				Detail: fmt.Sprintf("missing column %q", k),
			})
			out[k] = nil
			continue
		}
		varType := columntype.FromGo(val)
		if varType != v.columnTypes[k] {
			errs = append(errs, ValidationError{
				Row:      row.Row,
				Column:   -1,
				Kind:     "type_mismatch",
				Expected: v.columnTypes[k],
				Received: varType,
				Detail:   fmt.Sprintf("column %q", k),
			})
			out[k] = nil
			continue
		}
		out[k] = val
	}
	return out, errs, true
}

// ProcessJsonFile runs the full JSON ingestion pipeline synchronously.
// Meant to be called from within a job goroutine. Returns the dataset id
// on success.
func ProcessJsonFile(ctx context.Context, f io.Reader, fileName string, fileSize int64, progressFn func(int)) (string, error) {
	decoder := json.NewDecoder(f)

	// Consume opening '[' of the array.
	tok, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("expected opening '[': %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return "", fmt.Errorf("expected JSON array, got %v", tok)
	}

	// Decode firstRow and derive the column schema from it. ReadJsonRowAnd
	// ExtractType mutates strings → parsed Go values in place; the
	// resulting map is the canonical first data row.
	if !decoder.More() {
		return "", fmt.Errorf("empty JSON array")
	}
	var firstRow jsonRow
	if err := decoder.Decode(&firstRow); err != nil {
		return "", fmt.Errorf("decoding first row: %w", err)
	}
	ReadJsonRowAndExtractType(firstRow)

	// Sort columnKeys so per-row validation walks the schema in a
	// stable order. Without this, a row missing two keys would emit
	// its missing_value errors in random order, making logs and
	// dataset_validation_errors entries non-reproducible. Table
	// column order is handled separately in extractColumns and
	// StoreDataset.
	columnKeys := make([]string, 0, len(firstRow))
	for k := range firstRow {
		columnKeys = append(columnKeys, k)
	}
	sort.Strings(columnKeys)
	columnTypes := make(map[string]string, len(firstRow))
	for k, v := range firstRow {
		columnTypes[k] = columntype.FromGo(v)
	}

	datasetColumns := extractColumns(firstRow)

	tableName, err := repository.CreateDatasetTable("json", datasetColumns)
	if err != nil {
		return "", fmt.Errorf("while attempting to create a table: %w", err)
	}
	metadata := map[string]any{
		"name":        fileName,
		"tableName":   tableName,
		"size":        fileSize,
		"author":      "engineering",
		"description": "description",
	}
	datasetId, err := repository.StoreDatasetMetadata(metadata)
	if err != nil {
		return "", fmt.Errorf("adding dataset metadata: %w", err)
	}
	if err = repository.StoreDatasetColumns(datasetColumns, datasetId); err != nil {
		return "", fmt.Errorf("adding dataset columns: %w", err)
	}

	progressFn(10)

	source := &jsonSource{decoder: decoder}
	validator := &jsonValidator{columnKeys: columnKeys, columnTypes: columnTypes}
	return datasetId, runPipeline(ctx, source, validator, firstRow, tableName, datasetId, progressFn)
}
