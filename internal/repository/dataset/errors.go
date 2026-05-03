package dataset

import "errors"

// ErrNotFound is returned by mutating operations (e.g. SoftDeleteDataset)
// when no row matched. Callers check via errors.Is.
var ErrNotFound = errors.New("dataset not found")
