package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"

	"github.com/telmocbarros/data-pulse/internal/columntype"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
)

// csvRecord is the parsed shape of one CSV row before validation.
type csvRecord = []string

// csvSource adapts csv.Reader to RowSource[csvRecord]. Loops past
// read failures (emitting malformed_row and advancing) so the
// orchestrator only sees forwarded rows or EOF. On column-count
// mismatch the source emits malformed_row but still forwards the
// row — preserving today's behavior; see csvValidator FIXME.
type csvSource struct {
	reader          *csv.Reader
	expectedColumns int
	// rowNumber is the row label assigned to the next yielded/erroring
	// row. Initialized to match today's pipeline: 2 for the first
	// call (mislabels the third file row as "row 2" — preserved
	// for bug-for-bug parity with the pre-unification pipeline).
	rowNumber int32
}

func (s *csvSource) Next(ctx context.Context, errCh chan<- ValidationError) (numbered[csvRecord], bool, error) {
	for {
		select {
		case <-ctx.Done():
			return numbered[csvRecord]{}, false, nil
		default:
		}

		record, err := s.reader.Read()
		if err != nil {
			if err == io.EOF {
				return numbered[csvRecord]{}, false, nil
			}
			select {
			case errCh <- ValidationError{
				Row:    s.rowNumber,
				Column: -1,
				Kind:   "malformed_row",
				Detail: err.Error(),
			}:
			case <-ctx.Done():
				return numbered[csvRecord]{}, false, nil
			}
			s.rowNumber++
			continue
		}

		if len(record) != s.expectedColumns {
			select {
			case errCh <- ValidationError{
				Row:      s.rowNumber,
				Column:   -1,
				Kind:     "malformed_row",
				Expected: fmt.Sprintf("%d columns", s.expectedColumns),
				Received: fmt.Sprintf("%d columns", len(record)),
			}:
			case <-ctx.Done():
				return numbered[csvRecord]{}, false, nil
			}
			// Forward the row anyway (preserves today's behavior; see
			// csvValidator FIXME for the latent panic this enables).
		}

		row := numbered[csvRecord]{Row: s.rowNumber, Data: record}
		s.rowNumber++
		return row, true, nil
	}
}

// csvValidator checks each parsed row against the rowFieldTypes derived
// from the first data row. Iterates by index — empty cells produce
// missing_value errors; non-matching cell classifications produce
// type_mismatch errors. Empty cells are skipped by Parse so the output
// map is intentionally sparse on those keys.
//
// FIXME(csv-overflow): when a row has more columns than the header,
// the loop indexes v.headers[idx] beyond its length and panics.
// Real CSVs rarely have this shape so it hasn't hit production. Today's
// csvSource emits malformed_row for the count mismatch but still
// forwards the row (preserved here for bug-for-bug parity with the
// pre-unification pipeline). Fix in a follow-up: either skip the row
// in csvSource on count mismatch, or clamp idx < len(headers) here.
type csvValidator struct {
	headers       []string
	rowFieldTypes []string
}

func (v *csvValidator) Validate(row numbered[csvRecord]) (map[string]any, []ValidationError, bool) {
	expectedColumns := len(v.rowFieldTypes)
	out := make(map[string]any)
	var errs []ValidationError

	for idx, value := range row.Data {
		if value == "" {
			errs = append(errs, ValidationError{
				Row:    row.Row,
				Column: int32(idx),
				Kind:   "missing_value",
			})
			continue
		}

		if idx < expectedColumns {
			variableType := columntype.Classify(value)
			if v.rowFieldTypes[idx] != variableType {
				errs = append(errs, ValidationError{
					Row:      row.Row,
					Column:   int32(idx),
					Kind:     "type_mismatch",
					Expected: v.rowFieldTypes[idx],
					Received: variableType,
				})
			}
		}
		out[v.headers[idx]] = columntype.Parse(value)
	}

	return out, errs, true
}

// ProcessCsvFile runs the full CSV ingestion pipeline synchronously.
// Meant to be called from within a job goroutine. Returns the dataset id
// on success.
func ProcessCsvFile(ctx context.Context, f io.Reader, fileName string, fileSize int64, progressFn func(int)) (string, error) {
	csvReader := csv.NewReader(f)

	// 1. Read the header row.
	headers, err := csvReader.Read()
	if err != nil {
		slog.Error("read csv header failed", "err", err)
		return "", err
	}

	// 2. Read the first data row to derive column types. The row itself
	// is consumed and discarded — only its types are used. This differs
	// from JSON, where the firstRow is preserved and pre-sent to dataCh.
	content, rowFieldTypes, err := ReadCsvRowAndExtractType(csvReader)
	if err != nil {
		slog.Error("extract csv row field types failed", "err", err)
		return "", err
	}
	jsonObj := make(map[string]any)
	for idx, value := range content {
		jsonObj[headers[idx]] = columntype.Parse(value)
	}
	datasetColumns := extractColumns(jsonObj)

	// Allow csv.Reader to handle rows with wrong field count internally
	// instead of returning an error — csvSource then emits malformed_row.
	csvReader.FieldsPerRecord = -1

	// 3. Create the dataset table and store its metadata.
	tableName, err := repository.CreateDatasetTable("csv", datasetColumns)
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

	// rowNumber starts at 2 (header was row 1, first data row was row 2 —
	// consumed above, dropped). The next row csvSource yields is row 3.
	source := &csvSource{
		reader:          csvReader,
		expectedColumns: len(rowFieldTypes),
		rowNumber:       2,
	}
	validator := &csvValidator{headers: headers, rowFieldTypes: rowFieldTypes}
	return datasetId, runPipeline(ctx, source, validator, nil, tableName, datasetId, progressFn)
}
