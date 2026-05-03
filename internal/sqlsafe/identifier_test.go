package sqlsafe

import "testing"

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		// Positive cases
		{"simple lowercase", "users", true},
		{"with underscore", "user_name", true},
		{"underscore prefix", "_internal", true},
		{"alphanumeric", "col1", true},
		{"mixed case", "TableName", true},
		{"single letter", "x", true},
		{"single underscore", "_", true},
		{"trailing digits", "abc123", true},

		// Negative cases
		{"empty", "", false},
		{"leading digit", "1col", false},
		{"contains space", "col name", false},
		{"contains dash", "col-name", false},
		{"contains dot", "col.name", false},
		{"contains semicolon", "col;DROP", false},
		{"contains quote", "col'a", false},
		{"contains double quote", `col"a`, false},
		{"contains backtick", "col`a", false},
		{"contains parens", "col()", false},
		{"contains backslash", "col\\a", false},
		{"contains unicode letter", "ΓΌser", false},
		{"contains tab", "col\ta", false},
		{"contains newline", "col\na", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidIdentifier(tt.in); got != tt.want {
				t.Errorf("IsValidIdentifier(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
