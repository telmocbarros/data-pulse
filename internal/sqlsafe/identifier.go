package sqlsafe

import "regexp"

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// IsValidIdentifier reports whether s is safe to interpolate as a SQL
// identifier (table or column name). Postgres can't parameterize identifiers,
// so callers must validate before using fmt.Sprintf in a query string.
func IsValidIdentifier(s string) bool {
	return identifier.MatchString(s)
}
