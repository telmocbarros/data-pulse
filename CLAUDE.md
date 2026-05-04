# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

When the user asks for a review, refactor pass, or cleanup cycle, follow [docs/REVIEW_REFACTOR_PROCESS.md](docs/REVIEW_REFACTOR_PROCESS.md). The standards being enforced live in [PROJECT_SPEC.md](PROJECT_SPEC.md).

## Build & Run

```bash
# Start dependencies (PostgreSQL on :5432, PgAdmin on :5050)
docker compose up -d

# Run HTTP server (listens on :8080)
go run ./cmd/server

# Run CLI tool (interactive menu for local file testing)
go run ./cmd/cli

# Run tests (unit only — integration tests are gated by a build tag)
go test ./...

# Run a single test
go test ./internal/service/dataset/ -run TestFunctionName

# Run the integration test suite (requires `docker compose up -d` + migrations applied)
go test -tags=integration ./internal/service/dataset/...
```

## Architecture

Data ingestion backend that accepts CSV/JSON file uploads, streams them through a concurrent pipeline, and stores results in PostgreSQL.

### Three-Stage Pipeline

All file processing flows through a generic channel-based pipeline (`runPipeline[T]` in `internal/service/dataset/pipeline.go`) using goroutines + errgroup:

1. **Parse** — a `RowSource[T]` adapter (`csvSource` in `csv.go`, `jsonSource` in `json.go`) streams the file and yields typed records.
2. **Validate** — a `RowValidator[T]` adapter (`csvValidator`, `jsonValidator`) checks each row against the detected schema, returning the canonical `map[string]any` plus any `ValidationError`s emitted.
3. **Store** — `uploadDataset` drains the data channel into a transactional batch insert (50 rows/batch).

Cancellation: the orchestrator owns a child context; any errgroup-stage failure cancels it, which unblocks the parser via a propagation watcher. Channels are buffered (size 100); JSON pre-sends `firstRow` before goroutines start. Processing runs synchronously inside a `jobmanager.JobFunc`; the HTTP handler returns 202 Accepted immediately and the pipeline runs in the worker pool.

### Layer Structure

- `cmd/server/` — HTTP server entry point. Routes for `/api/datasets/...`, `/api/jobs/...`, `/health`.
- `cmd/cli/` — CLI for local file testing without HTTP.
- `internal/handler/` — HTTP handlers, route to service layer; shared error mapping in `dataset.handler.go`.
- `internal/service/dataset/` — Pipeline orchestration (`pipeline.go` with generic `runPipeline[T]`), CSV/JSON adapters (`csv.go`, `json.go`), visualization service (`visualize.go`), metadata service.
- `internal/service/profiler/` — Concurrent column profiler.
- `internal/service/jobmanager/` — Background job worker pool.
- `internal/repository/dataset/` — All dataset persistence: lifecycle, schema, metadata, streaming reads, raw-file storage (MinIO), validation errors, analytics queries.
- `internal/repository/profiler/`, `internal/repository/job/` — Profile and job persistence.
- `internal/columntype/` — Type tags (`Numerical`, `Boolean`, `Date`, `Categorical`) + detection helpers (`Detect`, `Parse`, `Classify`, `FromGo`).
- `internal/sqlsafe/` — Identifier validation regex + `BulkInsert` helper for multi-row VALUES inserts.
- `internal/models/` — Domain structs (`ValidationError`, `DatasetColumn`, `Job`, etc.).
- `config/` — Database connection pool (pgx via database/sql, global `config.Storage`); MinIO client setup.
- `internal/repository/migrations/` — SQL schema for `datasets`, `dataset_columns`, `dataset_validation_errors`, `numeric_profiles`, `category_profiles`, `correlation_matrices`, `jobs`. Applied manually via psql; no Go-side runner.

### Key Conventions

- **No web framework** — uses only `net/http` standard library
- **Minimal dependencies** — pgx (Postgres driver), google/uuid, godotenv
- **Streaming** — files are processed via `io.Reader`, never loaded fully into memory.
- **Dynamic tables** — each uploaded dataset gets its own table named `{csv|json}_datasets_{uuid}`.
- **Type detection** — first data row determines column types via `internal/columntype.Classify` (CSV) or `FromGo` (JSON, post-decode); see `columntype/detect.go` for the recognition ladder.
- **Errors at the API boundary** — service layer returns sentinel errors (`ErrDatasetNotFound`, `ErrInvalidParams` from `service/dataset/errors.go`); handler's `writeServiceError` maps them to 404/400/500 with a generic 500 message (no raw `pq:` strings leaked).
- **Logging** — `slog` with the default text handler; `log.Fatalf` retained only for boot fatals.
- **Environment config** — `.env` file with `DATABASE_URL`, loaded via godotenv.
