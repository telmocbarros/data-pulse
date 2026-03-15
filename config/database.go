package config

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers pgx as a database/sql driver
	"github.com/joho/godotenv"
)

var Storage *sql.DB

func SetupDatabase() error {
	err := godotenv.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read configuration variables: %v\n", err)
		os.Exit(1)
	}

	pool, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	pool.SetMaxOpenConns(25)
	pool.SetMaxIdleConns(25)
	pool.SetConnMaxLifetime(5 * time.Minute)

	if err = pool.Ping(); err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}

	Storage = pool
	return nil
}
