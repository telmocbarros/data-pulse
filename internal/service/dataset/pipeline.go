package dataset

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/telmocbarros/data-pulse/config"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
	"golang.org/x/sync/errgroup"
)

// numbered wraps a parsed row with its row number for error reporting.
// Row numbering convention is owned by each RowSource implementation
// (CSV uses 2-based to skip the header; JSON uses 1-based since the
// firstRow consumed at row 0 isn't sent through the source).
type numbered[T any] struct {
	Row  int32
	Data T
}

// RowSource yields parsed rows from a single uploaded file. Implementations
// send malformed_row ValidationErrors directly to the errCh passed by the
// orchestrator, with a select on ctx.Done() to honor cancellation.
//
// The contract for the three return values:
//   - (row, true, nil)   forward this row to the validator stage
//   - (zero, false, nil) end of stream (clean EOF)
//   - (zero, false, err) fatal source error; orchestrator returns err
//
// Skip-this-row cases (e.g. JSON decode failure where the source emits an
// error and continues) are handled internally — Next loops until it has
// either a row to forward or has reached EOF/fatal.
type RowSource[T any] interface {
	Next(ctx context.Context, errCh chan<- ValidationError) (row numbered[T], ok bool, err error)
}

// RowValidator inspects one parsed row and produces the canonical
// map[string]any form for storage plus any ValidationErrors it found.
// ok=false skips the row entirely (no row sent to dataCh) — useful for
// rows that are wholly invalid. Both today's CSV and JSON paths always
// return ok=true even on partial failures (sparse map preserved).
type RowValidator[T any] interface {
	Validate(row numbered[T]) (out map[string]any, errs []ValidationError, ok bool)
}

// runPipeline drives the parse → validate → store stages for a single
// file upload, with shared cancellation + errgroup wiring. Returns nil
// on success, the first non-nil error from any stage, or ctx.Err() if
// the caller's context was cancelled.
//
// firstRow is pre-sent to dataCh before any goroutine starts. Pass nil
// for formats that don't have a pre-validated first row. dataCh has
// capacity 100, so a single non-nil firstRow pre-send never blocks.
//
// Cancellation: a child of ctx is created internally so a downstream
// stage failure can unblock the source mid-Next. The propagation
// watcher always exits because errgroup cancels its own context after
// Wait returns; cancel is idempotent.
//
// Validation errors emitted by the source/validator are persisted
// best-effort after Wait — a persistence failure is logged but does
// not fail the upload (matches today's behavior).
func runPipeline[T any](
	ctx context.Context,
	source RowSource[T],
	validator RowValidator[T],
	firstRow map[string]any,
	tableName, datasetId string,
	progressFn func(int),
) error {
	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	parserCh := make(chan numbered[T], 100)
	errorsCh := make(chan ValidationError, 100)
	dataCh := make(chan map[string]any, 100)
	parserExited := make(chan struct{})
	validatorExited := make(chan struct{})

	// Pre-send firstRow before any goroutine starts. Safe because dataCh
	// has capacity 100, so the send never blocks for a single value.
	if firstRow != nil {
		dataCh <- firstRow
	}

	// Parser goroutine (not in g; it produces input for the errgroup).
	// Loops on source.Next, forwarding rows to parserCh. Closes parserCh
	// and parserExited on exit (EOF, fatal, or pipelineCtx cancellation).
	var sourceErr error
	go func() {
		defer close(parserExited)
		defer close(parserCh)
		for {
			select {
			case <-pipelineCtx.Done():
				return
			default:
			}
			row, ok, err := source.Next(pipelineCtx, errorsCh)
			if err != nil {
				sourceErr = err
				return
			}
			if !ok {
				return
			}
			select {
			case parserCh <- row:
			case <-pipelineCtx.Done():
				return
			}
		}
	}()

	g, gctx := errgroup.WithContext(pipelineCtx)

	// Propagate gctx cancellation back to pipelineCtx so the parser
	// (which only watches pipelineCtx) unblocks when any g.Go errors.
	// errgroup cancels gctx on Wait return, so this goroutine always
	// exits; cancel is idempotent.
	go func() {
		<-gctx.Done()
		cancel()
	}()

	var validationErrors []ValidationError

	// Error collector — drains errorsCh into validationErrors. Never
	// fails. errorsCh is closed by the closer goroutine below once both
	// parser and validator have stopped writing to it.
	g.Go(func() error {
		for ve := range errorsCh {
			validationErrors = append(validationErrors, ve)
		}
		return nil
	})

	// Storage stage — drains dataCh into transactional batch inserts.
	g.Go(func() error {
		return uploadDataset(gctx, dataCh, tableName, datasetId, progressFn)
	})

	// Closer goroutine: errorsCh is safe to close only after both
	// producers (parser + validator) have stopped.
	go func() {
		<-parserExited
		<-validatorExited
		close(errorsCh)
	}()

	// Validator stage — drains parserCh, calls validator.Validate, forwards
	// emitted ValidationErrors and the canonical row map. Closes dataCh and
	// validatorExited on exit so the storage stage and closer goroutine
	// can terminate.
	g.Go(func() error {
		defer close(dataCh)
		defer close(validatorExited)

		progressFn(30)

		for nr := range parserCh {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			out, errs, ok := validator.Validate(nr)
			for _, ve := range errs {
				select {
				case errorsCh <- ve:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			if !ok {
				continue
			}
			select {
			case dataCh <- out:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	waitErr := g.Wait()

	// Persist any collected validation errors before returning. We do this
	// even when waitErr is non-nil (e.g. uploadDataset crashed on a
	// type-incompatible cell that the validator forwarded with type_mismatch
	// emitted) — the diagnostic trail is the most useful thing the user can
	// get on a partial failure. Skip only when the caller cancelled, since
	// they've signaled they don't want any further work for this dataset.
	if ctx.Err() == nil && len(validationErrors) > 0 {
		if err := repository.StoreValidationErrors(datasetId, validationErrors); err != nil {
			// Persistence failure shouldn't override the original error.
			slog.Error("store validation errors failed", "err", err, "datasetId", datasetId)
		}
	}

	if waitErr != nil {
		return waitErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if sourceErr != nil {
		return sourceErr
	}
	return nil
}

// uploadDataset drains dataCh into a transactional batch insert.
// Returns the first error encountered (transaction begin, batch insert,
// commit, or context cancellation). On error the transaction is rolled back.
//
// Format-agnostic: dataCh carries map[string]any rows whose keys match the
// dataset's column names; the upload doesn't care whether the source was
// CSV, JSON, or anything else.
func uploadDataset(ctx context.Context, dataCh chan map[string]any, tableName string, datasetId string, progressFn func(int)) error {
	tx, err := config.Storage.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	batchSize := 50
	batch := make([]map[string]any, 0, batchSize)
	batchCount := 0

	for row := range dataCh {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := repository.StoreDataset(tx, tableName, datasetId, batch); err != nil {
				return fmt.Errorf("persist batch: %w", err)
			}
			batchCount++
			progress := 40 + min(batchCount*5, 55)
			progressFn(progress)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := repository.StoreDataset(tx, tableName, datasetId, batch); err != nil {
			return fmt.Errorf("persist final batch: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
