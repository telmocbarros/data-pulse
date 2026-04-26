package config

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers pgx as a database/sql driver
	"github.com/joho/godotenv"
)

// Executor is satisfied by both *sql.DB and *sql.Tx.
type Executor interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

var Storage *sql.DB

// SetupDatabase loads environment variables, opens the Postgres pool,
// pings to verify connectivity, and assigns the pool to Storage.
func SetupDatabase() error {
	if err := godotenv.Load(); err != nil {
		return fmt.Errorf("load env file: %w", err)
	}

	pool, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	pool.SetMaxOpenConns(25)
	pool.SetMaxIdleConns(25)
	pool.SetConnMaxLifetime(5 * time.Minute)

	if err := pool.Ping(); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	Storage = pool
	return nil
}
