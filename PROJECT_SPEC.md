# DataPulse — Analytics Processing Platform

## Overview

DataPulse is a data-intensive web application that allows users to upload datasets, automatically profile them, and explore the data through interactive visualizations. The project's primary goal is to showcase production-grade backend engineering in Go, with a supporting React/TypeScript frontend for rendering charts and plots.

---

## Architecture

### Backend (Go) — Primary Focus

The Go backend is the core of this project. It handles file ingestion, data parsing, profiling, aggregation, filtering, and all computation. The frontend never performs data transformations — it only renders what the backend provides.

### Frontend (React + TypeScript) — Supporting Role

A React SPA that consumes the Go API. It displays dataset profiles and renders visualizations using a charting library (Recharts, D3, or Chart.js). The UI should be functional and clean, not pixel-perfect. Its job is to make the backend's output visible.

### Database — PostgreSQL

Single database choice. Stores dataset metadata, structured dataset content, profiling results, and job status.

---

## Project Structure (Go Backend)

```
/cmd/server/            — Application entrypoint
/internal/
  /handler/             — HTTP handlers
  /service/             — Business logic
  /repository/          — Data access layer
  /model/               — Domain types and structs
  /middleware/           — HTTP middleware (logging, CORS, rate limiting)
  /pipeline/            — Data processing pipeline stages
  /profiler/            — Dataset profiling logic
  /query/               — Filter and aggregation engine
  /job/                 — Background job orchestration
/pkg/                   — Reusable, exportable packages
/config/                — Configuration loading
/migrations/            — Database migrations
/test/                  — Integration and end-to-end tests
```

---

## Core Features

### 1. Dataset Upload & Ingestion

Accept CSV and JSON file uploads. On upload, the backend:

- Streams the file rather than loading it entirely into memory, allowing large file handling without excessive memory consumption.
- Detects column types automatically (numeric, categorical, datetime, boolean).
- Validates rows and flags malformed entries, missing values, and type mismatches.
- Stores both the raw file on disk and the parsed/structured representation in PostgreSQL.
- Processes ingestion through a concurrent pipeline — parsing, validation, and storage run as composable stages connected via channels.

This is the first major Go showcase: streaming I/O, goroutine-based pipeline stages, and graceful error handling when rows are malformed.

### 2. Automatic Profiling

Immediately after a dataset is ingested, a background profiling job kicks off. The profiler computes per-column statistics:

**Numeric columns:**
- Min, max, mean, median, standard deviation
- Percentiles (25th, 50th, 75th)
- Histogram bucket counts

**Categorical columns:**
- Cardinality (number of unique values)
- Most frequent values and their counts
- Uniqueness ratio

**All columns:**
- Null/missing value count and percentage
- Data type distribution (in case of mixed-type columns)

Profiling runs concurrently across columns using goroutines. The result is stored as a profile report that the frontend can fetch and render as a dataset overview.

### 3. Visualization Endpoints

The backend exposes endpoints that return chart-ready data. The user selects a dataset and a visualization type, and the backend computes the result. Supported visualizations:

- **Histogram** — distribution of a single numeric column, with configurable bin count.
- **Scatter plot** — relationship between two numeric columns, with optional sampling for large datasets.
- **Time series** — values over time when a datetime column is present, with grouping by day/week/month.
- **Correlation matrix** — pairwise correlation coefficients across all numeric columns.
- **Category breakdown** — bar chart of value counts for a categorical column, optionally grouped by a second column.

All computation (binning, aggregation, correlation, grouping) happens in Go. The API returns arrays of coordinates or bucket values that the frontend maps directly to chart components.

### 4. Query & Filter Layer

Users can slice datasets before visualizing. The backend accepts a structured filter payload and executes it against the stored data. Supported operations:

- **Column filters** — equals, not equals, greater than, less than, contains, is null.
- **Date range filters** — between two dates for datetime columns.
- **Numeric range filters** — between two values for numeric columns.
- **Group by** — group rows by a categorical column and aggregate numeric columns (sum, avg, count, min, max).
- **Sort** — order results by one or more columns.
- **Limit/offset** — pagination for large result sets.

Filters can be combined. The backend interprets the filter payload, builds the appropriate query or in-memory operation, and returns the result. This is a lightweight query engine — not SQL exposed to the client, but a controlled, validated API that translates user intent into efficient data retrieval.

### 5. Dataset Comparison

Users can select two datasets (or two filtered views of the same dataset) and compare them. The backend computes:

- Schema differences — columns present in one but not the other, type mismatches.
- Distribution comparison — for shared numeric columns, compare means, medians, and standard deviations.
- Categorical drift — for shared categorical columns, compare value frequency distributions.
- Row count and null rate differences.

The comparison result is returned as a structured report that the frontend renders as a side-by-side view with highlighted differences. This feature goes beyond typical CRUD projects and demonstrates the ability to compose existing profiling logic into new functionality.

### 6. Background Job Processing

Heavy operations (profiling, large dataset ingestion, comparisons) run as background jobs rather than blocking the HTTP request cycle. The system includes:

- A job queue backed by an in-memory worker pool (no external dependencies like Redis for the initial implementation).
- Job status tracking — pending, running, completed, failed — stored in PostgreSQL and queryable via API.
- Progress reporting — jobs update their progress percentage as they work through pipeline stages.
- Graceful cancellation — users can cancel a running job, and the pipeline stages respect context cancellation.
- The frontend polls a status endpoint (or uses Server-Sent Events) to display progress to the user.

This showcases goroutine lifecycle management, context propagation, channel-based coordination, and clean shutdown handling.

---

## API Contract Summary

| Method | Endpoint                          | Description                              |
| ------ | --------------------------------- | ---------------------------------------- |
| POST   | /api/datasets                     | Upload a new dataset                     |
| GET    | /api/datasets                     | List all datasets                        |
| GET    | /api/datasets/:id                 | Get dataset metadata and schema          |
| DELETE | /api/datasets/:id                 | Delete a dataset                         |
| GET    | /api/datasets/:id/profile         | Get profiling results                    |
| POST   | /api/datasets/:id/visualize       | Request a visualization (type + params)  |
| POST   | /api/datasets/:id/query           | Apply filters and return results         |
| POST   | /api/datasets/compare             | Compare two datasets or filtered views   |
| GET    | /api/jobs/:id                     | Get job status and progress              |
| POST   | /api/jobs/:id/cancel              | Cancel a running job                     |

---

## Code Standards

- Go code follows Effective Go and Go Code Review Comments conventions.
- Run `go fmt`, `go vet`, and `golangci-lint` as the baseline for code quality.
- TypeScript uses strict mode with ESLint.
- All public Go functions and types must have doc comments.
- Prefer returning errors over panicking. Reserve `panic` for truly unrecoverable situations.
- Use meaningful variable and function names. Avoid single-letter names outside of short loops or well-known idioms (`i`, `ok`, `err`).
- SQL queries use parameterized statements. Never use string concatenation.
- Table-driven unit tests for all service and pipeline logic.

---

## Design Principles

- **Concurrency as a showcase.** Use goroutines, worker pools, and channels throughout the data processing layer. This is the primary differentiator.
- **Streaming over buffering.** Process files and large datasets as streams. Never load an entire file into memory when a streaming approach is possible.
- **Pipeline composition.** Design ingestion, validation, profiling, and transformation as composable stages that can be chained, tested in isolation, and reused across features.
- **Graceful degradation.** Handle partial failures (malformed rows, missing columns, cancelled jobs) without crashing the request or the server.
- **Observability.** Structured logging (`slog` or `zerolog`), request-level tracing, and Prometheus-compatible metrics on key operations (upload duration, profiling time, job queue depth).
- **Configuration via environment.** No hardcoded values. Use a config package that reads from environment variables with sensible defaults.

---

## Tech Stack

| Layer    | Technology           |
| -------- | -------------------- |
| Backend  | Go                   |
| Frontend | React + TypeScript   |
| Database | PostgreSQL           |
| Charts   | Recharts / D3        |

---

## Future Implementation: User Authentication

User authentication is not part of the initial build. The first version assumes a single implicit user. Once the core features are stable, authentication will be added as a follow-up phase.

### Planned Scope

- **Registration and login** — email/password-based authentication.
- **Password hashing** — bcrypt or argon2, never stored in plaintext.
- **Session management** — JWT tokens issued on login, validated via middleware on protected endpoints.
- **Middleware integration** — an auth middleware that extracts and validates the token, injects the user identity into the request context, and rejects unauthorized requests.
- **Dataset ownership** — datasets become scoped to the authenticated user. Users can only see, query, and delete their own datasets.
- **Role-based access (optional stretch)** — admin vs. regular user roles, where admins can view all datasets and system-wide job metrics.

### Why It's Deferred

Authentication is well-understood and adds little to the portfolio's core message, which is data processing in Go. Building it first would delay the interesting work. However, adding it later demonstrates the ability to retrofit a cross-cutting concern into an existing codebase cleanly — which is itself a useful thing to show.

### Integration Points

When authentication is added, the following areas will need updates:

- A new `/api/auth/register` and `/api/auth/login` endpoint pair.
- A `user` table in PostgreSQL with hashed passwords.
- An auth middleware applied to all `/api/datasets` and `/api/jobs` routes.
- A `user_id` foreign key added to the datasets table, with a migration.
- Repository queries updated to filter by user context.
