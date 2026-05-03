package dataset

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrDatasetNotFound is returned when a dataset id does not match any alive
// (non-soft-deleted) row. Handlers map this to 404.
var ErrDatasetNotFound = errors.New("dataset not found")

// ErrInvalidParams is returned when a caller-supplied parameter is rejected
// by the service layer's validation (resolve methods, range checks, etc.).
// Handlers map this to 400. Wrap concrete reasons with fmt.Errorf("...: %w",
// ErrInvalidParams) so the underlying message is preserved for the response
// body.
var ErrInvalidParams = errors.New("invalid params")

// translateRepoErr converts a repository error into a service-layer error.
// `sql.ErrNoRows` (wrapped by the repo via %w) becomes ErrDatasetNotFound;
// everything else passes through unchanged so the handler treats it as 500.
func translateRepoErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrDatasetNotFound, err.Error())
	}
	return err
}

// invalidParams wraps a validation reason with ErrInvalidParams so the
// handler can detect it via errors.Is. Use this for resolve-method and
// range-check failures.
func invalidParams(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidParams, fmt.Sprintf(format, args...))
}
