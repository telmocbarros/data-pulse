# Data Pulse

## Getting Started

```bash
# 1. Start dependencies (Postgres on :5432, MinIO on :9000, PgAdmin on :5050)
docker compose up -d

# 2. Apply schema migrations (idempotent; tracked in schema_migrations)
go run ./cmd/migrate

# 3. Run the HTTP server (listens on :8080)
go run ./cmd/server
```

The CLI is also available for local file testing without HTTP:

```bash
go run ./cmd/cli
```

### Running tests

```bash
# Unit tests (fast, no DB required)
go test ./...

# Integration tests (requires steps 1-2 above)
go test -tags=integration ./...
```

## Project Structure

```
/cmd/server/         — Application entrypoint
/internal/
  /handler/          — HTTP handlers
  /service/          — Business logic
  /repository/       — Data access layer
  /model/            — Domain types and structs
  /middleware/        — HTTP middleware
  /pipeline/         — Data processing pipelines
/pkg/                — Reusable, exportable packages
/config/             — Configuration loading
/migrations/         — Database migrations
/test/               — Integration and end-to-end tests
```

## Reading reqeuest data in chunks

`multipart/form-data` arrives as a stream over the network — the server doesn't receive the full 2 GB (an example) at once and hold it in RAM.

HTTP is built on TCP, which delivers data in small packets. The file you get from r.FormFile() is a reader — it reads from that incoming stream on demand.

So when `io.Copy` does:

1. src.Read(buf) — pulls the next 32 KB from the network stream
2. dst.Write(buf) — writes that 32 KB to disk
3. Repeat

At any point, only ~32 KB is in memory. The rest of the 2 GB is either still in transit on the network or already written to disk.

When you use `io.ReadAll(file)` instead, it keeps calling `Read` and accumulating all chunks into a growing byte slice in memory — that's what forces the full 2 GB into RAM.

The data arrives the same way in both cases (as a stream). The difference is whether you keep each chunk in memory or discard it after writing.

## Persistence Layer

User a PostgreSQL database where you create a new table every time a user uploads a new dataset.
There will be a couple of additional tables to have some visibility of the database, namely:

- DATASETS table (id, name, uploaded_at, row_count, storage_table_name, user_id, etc)
- DATASET COLUMNS table (dataset_id, column_name, column_type, column_index)
