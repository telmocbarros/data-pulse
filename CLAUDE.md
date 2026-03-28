# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Start dependencies (PostgreSQL on :5432, PgAdmin on :5050)
docker compose up -d

# Run HTTP server (listens on :8080)
go run ./cmd/server

# Run CLI tool (interactive menu for local file testing)
go run ./cmd/cli

# Run tests
go test ./...

# Run a single test
go test ./internal/service/dataset_upload/ -run TestFunctionName
```

## Architecture

Data ingestion backend that accepts CSV/JSON file uploads, streams them through a concurrent pipeline, and stores results in PostgreSQL.

### Three-Stage Pipeline

All file processing flows through a channel-based pipeline using goroutines:

1. **Parse** (`parseCsvFile`/`parseJsonFile`) — streams the file, detects column types from the first row, sends typed records through a channel
2. **Validate** (`runCsvPipeline`/`runJsonPipeline`) — checks each field against the detected schema, routes errors to an error channel and valid rows to a data channel
3. **Store** (repository layer) — batch inserts rows (50 at a time) into a dynamically-created PostgreSQL table

Channels are buffered (size 100). Processing can run in sync or async (fire-and-forget) mode.

### Layer Structure

- `cmd/server/` — HTTP server entry point, routes: `/health`, `/dataset` (POST multipart upload)
- `cmd/cli/` — CLI for local file testing without HTTP
- `internal/handler/` — HTTP handlers, routes to service based on Content-Type
- `internal/service/dataset_upload/` — Pipeline orchestration, type detection, validation
- `internal/repository/dataset_upload/` — Dynamic SQL table creation, batch inserts, metadata storage
- `internal/models/` — Domain structs (`ValidationError`, `DatasetColumn`, etc.)
- `config/` — Database connection pool setup (pgx via database/sql, global `config.Storage`)
- `migrations/` — SQL schema for `datasets` and `dataset_columns` tables

### Key Conventions

- **No web framework** — uses only `net/http` standard library
- **Minimal dependencies** — pgx (Postgres driver), google/uuid, godotenv
- **Streaming** — files are processed via `io.Reader`, never loaded fully into memory
- **Dynamic tables** — each uploaded dataset gets its own table named `{csv|json}_datasets_{uuid}`
- **Type detection** — first data row determines column types: numerical, boolean, date (multiple formats), or text (see `utils.go`)
- **Environment config** — `.env` file with `DATABASE_URL`, loaded via godotenv
