package columntype

import (
	"testing"
	"time"
)

func TestDetect(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantType string
	}{
		{"empty is categorical", "", Categorical},
		{"int", "42", Numerical},
		{"negative int", "-7", Numerical},
		{"float", "3.14", Numerical},
		{"bool true", "true", Boolean},
		{"bool false", "false", Boolean},
		// strconv.ParseBool accepts "0" and "1", but the int branch wins
		// because it runs first.
		{"zero is numeric not bool", "0", Numerical},
		{"one is numeric not bool", "1", Numerical},
		{"date only", "2024-03-15", Date},
		{"datetime", "2024-03-15 12:34:56", Date},
		{"datetime with tz MST", "2024-03-15 12:34:56 -0700 MST", Date},
		{"datetime UTC fmt", "2024-03-15 12:34:56 +0000 UTC", Date},
		{"plain string", "hello", Categorical},
		{"alphanumeric", "abc123", Categorical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, _ := Detect(tt.in)
			if gotType != tt.wantType {
				t.Errorf("Detect(%q) type = %q, want %q", tt.in, gotType, tt.wantType)
			}
		})
	}
}

func TestDetectParsedTypes(t *testing.T) {
	t.Run("int returns int64", func(t *testing.T) {
		_, parsed := Detect("42")
		if _, ok := parsed.(int64); !ok {
			t.Errorf("int case: got %T, want int64", parsed)
		}
	})
	t.Run("float returns float64", func(t *testing.T) {
		_, parsed := Detect("3.14")
		if _, ok := parsed.(float64); !ok {
			t.Errorf("float case: got %T, want float64", parsed)
		}
	})
	t.Run("bool returns bool", func(t *testing.T) {
		_, parsed := Detect("true")
		if _, ok := parsed.(bool); !ok {
			t.Errorf("bool case: got %T, want bool", parsed)
		}
	})
	t.Run("date returns time.Time", func(t *testing.T) {
		_, parsed := Detect("2024-03-15")
		if _, ok := parsed.(time.Time); !ok {
			t.Errorf("date case: got %T, want time.Time", parsed)
		}
	})
	t.Run("categorical returns original string", func(t *testing.T) {
		_, parsed := Detect("hello")
		if s, ok := parsed.(string); !ok || s != "hello" {
			t.Errorf("categorical case: got (%T) %v, want string \"hello\"", parsed, parsed)
		}
	})
}

func TestParse(t *testing.T) {
	// Parse is a thin wrapper; verify it returns the same parsed value as Detect.
	v := Parse("42")
	if i, ok := v.(int64); !ok || i != 42 {
		t.Errorf("Parse(\"42\") = %v (%T), want int64(42)", v, v)
	}
}

func TestClassify(t *testing.T) {
	// Classify is a thin wrapper; verify it returns the same typename as Detect.
	if got := Classify("3.14"); got != Numerical {
		t.Errorf("Classify(\"3.14\") = %q, want %q", got, Numerical)
	}
}

func TestFromGo(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"time.Time", time.Now(), Date},
		{"float64", 3.14, Numerical},
		{"float32", float32(2.71), Numerical},
		{"int", 42, Numerical},
		{"int64", int64(42), Numerical},
		{"int32", int32(42), Numerical},
		{"int16", int16(42), Numerical},
		{"int8", int8(42), Numerical},
		{"bool", true, Boolean},
		{"string", "hello", Categorical},
		{"nil", nil, Categorical},
		{"struct", struct{}{}, Categorical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromGo(tt.val); got != tt.want {
				t.Errorf("FromGo(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}
