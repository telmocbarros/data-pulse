# Refactor Progress

Snapshot of the multi-cluster refactor against [PROJECT_SPEC.md](../PROJECT_SPEC.md), driven by [docs/REVIEW_REFACTOR_PROCESS.md](REVIEW_REFACTOR_PROCESS.md). Update this file at the end of each session so the next one can pick up cleanly.

## Done

### Critical bugs (full tier)
1. **Silent storage failures** — `uploadJsonDataset` (used by both CSV and JSON pipelines) now returns errors; `runCsvPipeline`/`runJsonPipeline` capture them via mutex-guarded shared variable and return after `wg.Wait()`. ([service/dataset/json-parser.go](../internal/service/dataset/json-parser.go), [csv-parser.go](../internal/service/dataset/csv-parser.go))
2. **SQL injection at upload boundary** — `StoreDataset`, `CreateDatasetTable`, `GetDatasetRows` all validate identifiers via `sqlsafe.IsValidIdentifier`. Removed bogus `"json_datasets"` fallback. ([repository/dataset_upload/index.go](../internal/repository/dataset_upload/index.go))
3. **Scatter rewrite** — query-driven (`x`, `y`, `limit`), proper `(x, y)` shape, identifiers validated. SQL injection fixed as side-effect. New `ScatterPoint` type. ([repository/dataset/index.go](../internal/repository/dataset/index.go), [service/dataset/visualize.go](../internal/service/dataset/visualize.go))
4. **Handler `http.Error` fall-through** — `GetDatasetHandler` now returns after each `http.Error`; sends real 500 instead of fake-200 string. ([handler/dataset.handler.go](../internal/handler/dataset.handler.go))
5. **`defer rows.Close()` in loop** — `GetHistogramFromDataset` split into `listNumericProfileIds` + `getHistogramBuckets`. Each has its own defer.
6. **`ListenAndServe` error surfaced** — typed `http.Server` with `ReadHeaderTimeout` + read/write/idle timeouts; `log.Fatalf` on listen failure. ([cmd/server/index.go](../cmd/server/index.go))
7. **`os.Exit(1)` removed from `SetupDatabase`** — returns errors throughout. ([config/database.go](../config/database.go))

### Spec gap cluster 1 — Routing & contract (full)
- New migration [06-add-dataset-soft-delete.sql](../internal/repository/migrations/06-add-dataset-soft-delete.sql) — `deleted_at` column + partial index.
- `GetDatasetById` and `ListDatasets` now filter `deleted_at IS NULL`.
- New `GetDatasetRowById` and `SoftDeleteDataset` repo functions.
- Service refactored end-to-end **typed**:
  - [visualize.go](../internal/service/dataset/visualize.go) — `VisualizationType` enum, typed Params + Result structs per chart, one entry function each (`GetHistogram`, `GetScatter`, `GetTimeseries`, `GetCorrelationMatrix`, `GetCategoryBreakdown`). `resolve()` methods on `*Params`.
  - [metadata.go](../internal/service/dataset/metadata.go) — `GetDatasetMetadata` returns full `DatasetMetadata{ID, FileName, TableName, Size, UploadedBy, Description, CreatedAt, Columns}`.
  - [index.go](../internal/service/dataset/index.go) — slimmed to `extractColumns` + `goTypeToDBType` only.
- Handlers reshaped: `GetDatasetHandler` returns metadata only; new `VisualizeDatasetHandler` (POST + JSON body, dispatched by `Type`); new `DeleteDatasetHandler`. Shared `parseDatasetID` helper.
- New routes: `DELETE /api/datasets/{id}`, `POST /api/datasets/{id}/visualize`. Old `?graphtype=` removed.

### Spec gap cluster 2 — Visualization features (full: 4 of 4)
- **Histogram configurable bins (done).** `HistogramParams.Bins`. `Bins == 0` → cached buckets (fast path). Any other value → live recompute via `WIDTH_BUCKET`. New `repository.ComputeHistogramFromDataset`. Capped at `histogramMaxBins = 200`. Edge-case-handled: all-null, single-distinct-value, max-value clamping into top bucket via `LEAST(WIDTH_BUCKET(...), bins)`.
- **Time-series grouping (done).** `TimeseriesParams.GroupBy` (`""|"day"|"week"|"month"`) and `Aggregate` (`""|"avg"|"sum"|"min"|"max"|"count"`). `(*TimeseriesParams).resolve` validates both, defaults `Aggregate` to `"avg"` when `GroupBy` is set, rejects `Aggregate` without `GroupBy`. `GetTimeseries` branches on `GroupBy != ""` to call new `repository.GetTimeseriesPlotGrouped`, which builds `SELECT date_trunc('<unit>', x) AS x, <FN>(y_i) AS y_i FROM <table> [WHERE series = $1] GROUP BY 1 ORDER BY 1`. Repo re-validates unit+aggregate against an internal allowlist as defense-in-depth (the SQL function name and `date_trunc` unit are interpolated, not parameterized). ([visualize.go](../internal/service/dataset/visualize.go), [repository/dataset/index.go](../internal/repository/dataset/index.go))
- **Scatter random sampling (done).** `ScatterParams.Sample bool`. When false, head-of-table `LIMIT` (existing behavior). When true, new `repository.SampleScatterPlotFromDataset` does `COUNT(*)` on the (x,y)-non-null intersection, sizes a percentage as `limit*1.5*100/total`, and runs `TABLESAMPLE BERNOULLI(pct)` + `LIMIT` for a representative spread. Falls back to the LIMIT path when `total <= limit` (no sampling possible). 1.5x over-sample absorbs both BERNOULLI's variance and the post-WHERE shrinkage (TABLESAMPLE applies before WHERE). Decision: chose BERNOULLI + count over `ORDER BY random()` because the latter sorts the whole table — BERNOULLI is O(rows scanned). ([visualize.go](../internal/service/dataset/visualize.go), [repository/dataset/index.go](../internal/repository/dataset/index.go))
- **Category-breakdown 2D groupBy (done).** `CategoryBreakdownParams.GroupBy`. When unset, the existing 1D path reads cached `category_profile_frequent_values`. When set, new `repository.GetCategoryBreakdownGrouped` runs a live SQL aggregate: a CTE picks the top `limit` values of `column`, then the outer query `JOIN`s back to count rows per `(column, groupBy)` pair. `Limit` caps the number of top-level values, not total rows. Output ordering: top-level total desc → top-level value asc → per-group count desc → group value asc (deterministic). `CategoryBreakdownCell.Group` is `omitempty` so 1D responses are unchanged. `resolve` rejects non-categorical `groupBy` and `groupBy == column`. ([visualize.go](../internal/service/dataset/visualize.go), [repository/dataset/index.go](../internal/repository/dataset/index.go))

### Pipeline deadlock fix (full)
The validator could deadlock if storage errored and returned, because nothing cancelled the context — `<-ctx.Done()` never fired and the validator blocked on `dataCh <- jsonResult` forever. Fix:
- Both `csvPipelineState` and `jsonPipelineState` now own a cancellable context (`pipelineCtx, cancel := context.WithCancel(ctx)`) and a `parserExited` signal channel closed by the parser on its way out.
- `runCsvPipeline` / `runJsonPipeline` rewritten on `errgroup.WithContext(state.ctx)`: storage and validator are `g.Go(...)`. Storage returning an error cancels `gctx`, which fires the validator's `<-gctx.Done()` selects so it returns instead of blocking on `dataCh`.
- A small watcher (`<-gctx.Done(); state.cancel()`) propagates errgroup cancellation back to `state.ctx` so the parser — which only watches its own ctx — also unblocks. `state.ctx` is `gctx`'s parent, so it isn't cancelled automatically; the watcher closes that gap.
- Parser's `parserCh <- ...` send is now guarded by a `select` on `pipelineCtx.Done()` (was an unguarded send — would have blocked even on user-driven cancellation).
- `errorsCh` close is gated on both `state.parserExited` AND a new `validatorExited` channel — guarantees neither producer can write to a closed channel even when the validator returns early on cancellation.
- The previous `wg.Wait()` + mutex-guarded `uploadErr` shared-variable dance is gone — `g.Wait()` returns the first error directly.
- Promoted `golang.org/x/sync` from indirect to direct in go.mod. ([csv-parser.go](../internal/service/dataset/csv-parser.go), [json-parser.go](../internal/service/dataset/json-parser.go))

### Spec gap cluster 3 — Validation errors persistence (full)
- New migration [07-add-dataset-validation-errors.sql](../internal/repository/migrations/07-add-dataset-validation-errors.sql): `dataset_validation_errors(id BIGSERIAL, dataset_id UUID FK CASCADE, row_number, column_index, kind, expected, received, detail, created_at)` + `(dataset_id, id)` index for paginated reads.
- `models.ValidationError` moved out of `service/dataset` (now an alias there) so both the service and repo packages can reference it without an import cycle. ([models/dataset.go](../internal/models/dataset.go), [service/dataset/utils.go](../internal/service/dataset/utils.go))
- New `repository.StoreValidationErrors` (chunked bulk insert at 500/batch — well under Postgres's 65535-param limit at 7 params per row) and `repository.ListValidationErrors(datasetId, limit, offset)` (default limit 50, max 500, ordered by id asc for stable paging). ([repository/dataset_upload/index.go](../internal/repository/dataset_upload/index.go))
- CSV and JSON pipelines now call `StoreValidationErrors` instead of `fmt.Printf`. Persistence failure is logged but does **not** fail the upload — the data is already in. ([csv-parser.go](../internal/service/dataset/csv-parser.go), [json-parser.go](../internal/service/dataset/json-parser.go))
- New endpoint `GET /api/datasets/{id}/validation-errors?limit&offset` → `{limit, offset, errors: [{row, column, kind, expected?, received?, detail?}]}`. Decoupled JSON shape from internal struct so we can add fields without breaking the API. ([handler/dataset.handler.go](../internal/handler/dataset.handler.go), [cmd/server/index.go](../cmd/server/index.go))

### Spec gap cluster 4 — Profiling streaming + progress (full)
- New `repository.StreamDatasetRows(ctx, tableName, rowCh)` streams `SELECT *` row-by-row into the caller-owned `rowCh`. Uses `QueryContext` so ctx cancellation aborts the in-flight query and the row scan loop. Per-row send is guarded by `select { case rowCh <- row: case <-ctx.Done() }` so a stalled consumer can't deadlock the producer. ([repository/dataset_upload/index.go](../internal/repository/dataset_upload/index.go))
- `repository.GetDatasetRows` marked `// Deprecated:` — kept for now; real removal during the upcoming `dataset_upload` package merge (DRY tier).
- `ProfileAndStore` rewritten with new signature `(ctx, datasetId, tableName, columnTypes, progressFn)`. Streams rows through the existing `rowCh` instead of buffering the whole table. progressFn fires stage milestones: 5 (job started) → 70 (profiling done) → 85 (StoreProfile done) → 95 (StoreCorrelationMatrix done). Job manager auto-fires 100 on success. ([service/profiler/index.go](../internal/service/profiler/index.go))
- **Cancellation contract documented in code:** ctx → StreamDatasetRows returns → producer's `defer close(rowCh)` → `ProfileDataset`'s for-range exits → `ProfileAndStore` reads `streamErr == ctx.Err()` and returns it. No partial profile is persisted. The single-goroutine-writes/post-Wait-reads pattern around `streamErr` is race-free without a mutex (channel close establishes happens-before). `ProfileDataset` doesn't need a ctx parameter — its for-range responds to producer cancellation via channel close.
- All three callers updated: [profile.handler.go](../internal/handler/profile.handler.go), [file-upload.handler.go](../internal/handler/file-upload.handler.go) (handlers thread ctx + progressFn from the JobFunc closure), [cmd/cli/index.go](../cmd/cli/index.go) (passes `context.Background()` and a no-op `func(int){}`).
- **Known follow-up (out of scope):** `NumericProfiler.Values []float64` accumulates every numeric value for percentile/stddev/histogram passes in `finaliseNumeric`. Streaming the input doesn't bound this slice — separate algorithmic work (t-digest sketches, Welford's algorithm, or a second SQL pass for histogram bucketing — primitives already exist in `repository.computeColumnHistogram`).

### Idiomatic Go tier — Typed handler errors (done)
- New sentinels in [service/dataset/errors.go](../internal/service/dataset/errors.go): `ErrDatasetNotFound` and `ErrInvalidParams`. `repository/dataset_upload.ErrNotFound` exposed for the soft-delete path (repo can't import service without a cycle, so it owns its own sentinel).
- Service layer wraps repo not-found via `translateRepoErr` (matches `sql.ErrNoRows` through the repo's `%w` wrapper, otherwise passes the error through unchanged so non-found errors stay as 500).
- All `resolve()` methods and the histogram bins-range checks now return `invalidParams("...")` (a small wrapper that prepends `ErrInvalidParams` via `%w`), so callers detect them via `errors.Is`.
- New `writeServiceError` helper in [handler/dataset.handler.go](../internal/handler/dataset.handler.go) maps service errors to status codes: not-found → 404, invalid-params → 400 (with the underlying message), anything else → 500 with a **generic** "Internal server error" message. Underlying error is logged but never leaked to the response body, fixing a minor info-leak (raw `pq:` strings used to reach the client).
- `VisualizeDatasetHandler`, `GetDatasetHandler`, and `DeleteDatasetHandler` all unified on `writeServiceError`. The `// Worth tightening later.` TODO comment in VisualizeDatasetHandler is gone.

### DRY tier — Bulk-insert builder unified (done)
- New `sqlsafe.BulkInsert(exec, table, columns, rows [][]any) error` builds and executes the multi-row VALUES INSERT pattern. Validates table + column identifiers via the existing `IsValidIdentifier`. Empty `rows` is a no-op (no SQL executed). Each row's length is checked against the column count up front. Takes a minimal `interface{ Exec(string, ...any) (sql.Result, error) }` so the package stays free of higher-level dependencies. ([internal/sqlsafe/bulk_insert.go](../internal/sqlsafe/bulk_insert.go))
- Six bespoke builders deleted, all rewritten to populate a `[][]any` and call `sqlsafe.BulkInsert`:
  - `StoreDatasetColumns` (3 cols/row) — [repository/dataset_upload/index.go](../internal/repository/dataset_upload/index.go)
  - `storeValidationErrorChunk` (7 cols/row) — same file
  - `storeHistogram`, `storeNumericTypeDistribution`, `storeFrequentValues`, `storeCategoryTypeDistribution`, plus `StoreCorrelationMatrix`'s tail INSERT (3-4 cols/row each) — [repository/profiler/index.go](../internal/repository/profiler/index.go)
- `StoreDataset`'s dynamic-column INSERT (where the column names come from the data) is left alone — its shape is structurally different and folding it in would complicate the helper.

### DRY tier — Type-detection ladder consolidated (done)
- New `internal/columntype/detect.go` exposes:
  - `Detect(string) (typename, parsed any)` — single ladder: int → float → bool → date (4 layouts) → categorical.
  - `Parse(string) any` — wraps Detect, returns parsed value only (legacy `ParseValue` semantics).
  - `Classify(string) string` — wraps Detect, returns typename only (legacy `ComputeVariableType` semantics, minus the always-nil error).
  - `FromGo(any) string` — runtime-type switch for already-typed Go values; replaces `service/dataset.goTypeToDBType` and the `Classify(fmt.Sprintf("%v", v))` round-trip in json-parser.go.
- `dateFormats` covers all four layouts the old code accepted, including `2006-01-02 15:04:05 -0700 MST` (the format `time.Time` produces via `fmt.Sprintf("%v")`) and the UTC variant.
- Deleted `service/dataset.ParseValue`, `ComputeVariableType`, and `goTypeToDBType`. All callers (csv-parser, json-parser, utils, index) now use `columntype.{Parse,Classify,FromGo}` directly. Dropped the dead `if err != nil` branch from validators (the old `ComputeVariableType` had a `(string, error)` signature where the error was always nil).
- Net: one ladder instead of three (the two string ladders + the runtime-type switch). ([internal/columntype/detect.go](../internal/columntype/detect.go))

### DRY tier — Path ID parsing unified (done)
- Renamed `parseDatasetID` → `parseUUIDPath` (it was already generic — reads `r.PathValue("id")`, validates as UUID, writes 400 on failure). All ID-bearing handlers now use it.
- `profile.handler.go` switched from `extractDatasetId(r.URL.Path)` (manual `TrimPrefix`/`TrimSuffix`/`Contains` dance, no UUID validation) to `parseUUIDPath`. Old helper deleted.
- `job.handler.go` switched from `strings.TrimPrefix`/`TrimSuffix` idioms to `parseUUIDPath`. Tightening: job IDs are now UUID-validated at the boundary (jobs.id is `UUID PRIMARY KEY DEFAULT gen_random_uuid()`, so this rejects garbage with a 400 instead of letting it fall through to a 404 from the repo).
- All five handler files use the same shape now: `id, ok := parseUUIDPath(w, r); if !ok { return }`. ([dataset.handler.go](../internal/handler/dataset.handler.go), [profile.handler.go](../internal/handler/profile.handler.go), [job.handler.go](../internal/handler/job.handler.go))

## Pending

_All spec-gap clusters complete. Next up: rest of DRY tier and Idiomatic Go tier below._

## Tiers still to come (after spec gaps)

### DRY tier (remaining)
- Two parallel repo packages (`repository/dataset` and `repository/dataset_upload`) → merge. Will let us delete the deprecated `GetDatasetRows`.
- CSV and JSON pipelines mirror each other → unified pipeline.

### Idiomatic Go tier (remaining)
- Logging: `fmt.Println`/`fmt.Printf` everywhere → `slog`.
- Hand-rolled JSON in `file-upload.handler.go` and `profile.handler.go` → `json.NewEncoder`.
- Capitalized error strings.
- `columntype` constants are SCREAMING_SNAKE_CASE → idiomatic Go casing.
- Doc comments on every exported symbol.

### Tests tier (zero `_test.go` files exist)
Highest-value targets per the review:
1. `internal/columntype` — `Detect`, `Parse`, `Classify`, `FromGo` (pure functions, easy to table-test).
2. `service/profiler` — `percentile`, `finaliseNumeric`, `toFloat64`.
3. `service/dataset/visualize.go` — all `(*Params).resolve` methods (pure now, easy to table-test).
4. `internal/sqlsafe` — positive/negative table for the regex.
5. CSV/JSON pipeline integration with a known-bad fixture.

## Verification status

After each completed item: `go build ./... && go vet ./... && go test ./...`. All currently passing.

## How to resume

1. Read this file.
2. Check todo list (it persists in the conversation if continuing; otherwise reconstruct from "Pending" / "Tiers still to come" above).
3. Pick the next item from "Tiers still to come".
4. After each item: build/vet/test, then move that item from Pending → Done in this file.
