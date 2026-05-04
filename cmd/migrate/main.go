// migrate applies every embedded SQL migration that hasn't been applied
// yet, in lexical order. Tracks applied versions in a schema_migrations
// table that the runner creates if missing.
//
// The database itself must already exist when this runs. With docker-
// compose that happens automatically (POSTGRES_DB in docker-compose.yaml
// provisions data-pulse-db on first container boot). For other setups
// create the database first with `psql -U postgres -c 'CREATE DATABASE
// "data-pulse-db"'`.
//
// Usage:
//
//	go run ./cmd/migrate
//
// Reads DATABASE_URL from .env (same convention as cmd/server).
package main

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/repository/migrations"
)

const bootstrapDDL = `CREATE TABLE IF NOT EXISTS schema_migrations (
	version    TEXT PRIMARY KEY,
	applied_at TIMESTAMP NOT NULL DEFAULT NOW()
)`

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if err := config.SetupDatabase(); err != nil {
		slog.Error("connect failed", "err", err)
		os.Exit(1)
	}
	defer config.Storage.Close()

	if err := run(config.Storage); err != nil {
		slog.Error("migrate failed", "err", err)
		os.Exit(1)
	}
}

func run(db *sql.DB) error {
	if _, err := db.Exec(bootstrapDDL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := loadApplied(db)
	if err != nil {
		return fmt.Errorf("load applied: %w", err)
	}

	files, err := listMigrations()
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}

	for _, name := range files {
		if applied[name] {
			slog.Info("already applied", "file", name)
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if err := apply(db, name, string(body)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
		slog.Info("applied", "file", name)
	}
	slog.Info("migrations complete")
	return nil
}

// loadApplied returns the set of migration filenames already recorded
// in schema_migrations.
func loadApplied(db *sql.DB) (map[string]bool, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

// listMigrations returns every .sql file at the embed root, sorted
// lexically (which matches chronological order because filenames start
// with a zero-padded number).
func listMigrations() ([]string, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out, nil
}

// apply runs one migration plus its schema_migrations bookkeeping row
// inside a single transaction so a partial failure leaves no trace.
func apply(db *sql.DB, name, body string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(body); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
		return err
	}
	return tx.Commit()
}
