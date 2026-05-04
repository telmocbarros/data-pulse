// Package migrations bundles the SQL migration files so the cmd/migrate
// runner can execute them without needing the working directory to point
// anywhere in particular.
package migrations

import "embed"

// FS holds every .sql migration file at this directory's root. The
// runner walks them in lexical order (which is also chronological,
// since file names are zero-padded with the migration number).
//
//go:embed *.sql
var FS embed.FS
