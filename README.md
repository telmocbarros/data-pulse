# Data Pulse

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
