package dataset

import (
	"testing"

	"github.com/telmocbarros/data-pulse/internal/columntype"
)

// ---------------------------------------------------------------------
// csvValidator
// ---------------------------------------------------------------------

func newCsvValidator() *csvValidator {
	// Schema: name=Categorical, age=Numerical, email=Categorical.
	return &csvValidator{
		headers:       []string{"name", "age", "email"},
		rowFieldTypes: []string{columntype.Categorical, columntype.Numerical, columntype.Categorical},
	}
}

func TestCsvValidatorAllValidRow(t *testing.T) {
	v := newCsvValidator()
	out, errs, ok := v.Validate(numbered[csvRecord]{Row: 2, Data: []string{"Alice", "30", "alice@example.com"}})
	if !ok {
		t.Fatal("ok should always be true")
	}
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
	wantKeys := []string{"name", "age", "email"}
	for _, k := range wantKeys {
		if _, ok := out[k]; !ok {
			t.Errorf("output map missing key %q", k)
		}
	}
	if got := out["name"]; got != "Alice" {
		t.Errorf("name = %v, want Alice", got)
	}
	if got, ok := out["age"].(int64); !ok || got != 30 {
		t.Errorf("age = %v (%T), want int64(30)", out["age"], out["age"])
	}
}

func TestCsvValidatorMissingValueIsNull(t *testing.T) {
	v := newCsvValidator()
	out, errs, _ := v.Validate(numbered[csvRecord]{Row: 3, Data: []string{"", "30", "x@y.z"}})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Kind != "missing_value" || errs[0].Column != 0 {
		t.Errorf("error = %+v, want missing_value at column 0", errs[0])
	}
	// Critical: the key MUST be present in the map (not absent), with value nil.
	val, present := out["name"]
	if !present {
		t.Error("name key must be present in output map (not sparse)")
	}
	if val != nil {
		t.Errorf("name value = %v, want nil", val)
	}
}

func TestCsvValidatorTypeMismatchIsNull(t *testing.T) {
	v := newCsvValidator()
	// age column is Numerical but we pass "thirty" (string).
	out, errs, _ := v.Validate(numbered[csvRecord]{Row: 4, Data: []string{"Bob", "thirty", "b@x.z"}})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Kind != "type_mismatch" || errs[0].Column != 1 {
		t.Errorf("error = %+v, want type_mismatch at column 1", errs[0])
	}
	if errs[0].Expected != columntype.Numerical || errs[0].Received != columntype.Categorical {
		t.Errorf("error expected/received = %q/%q, want %q/%q", errs[0].Expected, errs[0].Received, columntype.Numerical, columntype.Categorical)
	}
	// Critical: age is in the map as nil — NOT as the Parse'd string. If
	// the Parse'd string reached storage, pgx would reject it on insert
	// into a DOUBLE PRECISION column, rolling back the entire batch.
	val, present := out["age"]
	if !present {
		t.Error("age key must be present in output map (not sparse)")
	}
	if val != nil {
		t.Errorf("age value = %v (%T), want nil", val, val)
	}
}

func TestCsvValidatorAlwaysReturnsAllHeaderKeys(t *testing.T) {
	// The "every key always present" invariant is the load-bearing one;
	// pin it explicitly across a few row shapes.
	v := newCsvValidator()
	cases := [][]string{
		{"Alice", "30", "alice@x"},
		{"", "", ""},
		{"", "thirty", ""},
		{"Bob", "30", ""},
	}
	for i, data := range cases {
		out, _, _ := v.Validate(numbered[csvRecord]{Row: int32(i + 2), Data: data})
		if len(out) != 3 {
			t.Errorf("case %d: out has %d keys, want 3", i, len(out))
		}
		for _, k := range []string{"name", "age", "email"} {
			if _, ok := out[k]; !ok {
				t.Errorf("case %d: missing key %q in out", i, k)
			}
		}
	}
}

// ---------------------------------------------------------------------
// jsonValidator
// ---------------------------------------------------------------------

func newJsonValidator() *jsonValidator {
	// Schema: name=Categorical, age=Numerical, email=Categorical (derived
	// from a firstRow whose age was a numeric value).
	return &jsonValidator{
		columnKeys: []string{"name", "age", "email"},
		columnTypes: map[string]string{
			"name":  columntype.Categorical,
			"age":   columntype.Numerical,
			"email": columntype.Categorical,
		},
	}
}

func TestJsonValidatorAllValidRow(t *testing.T) {
	v := newJsonValidator()
	in := numbered[jsonRow]{Row: 1, Data: jsonRow{"name": "Alice", "age": float64(30), "email": "alice@x"}}
	out, errs, ok := v.Validate(in)
	if !ok {
		t.Fatal("ok should always be true")
	}
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
	if got := out["name"]; got != "Alice" {
		t.Errorf("name = %v, want Alice", got)
	}
	if got, ok := out["age"].(float64); !ok || got != 30 {
		t.Errorf("age = %v (%T), want float64(30)", out["age"], out["age"])
	}
}

func TestJsonValidatorMissingKeyIsNull(t *testing.T) {
	v := newJsonValidator()
	// email is missing.
	out, errs, _ := v.Validate(numbered[jsonRow]{Row: 2, Data: jsonRow{"name": "Bob", "age": float64(40)}})
	if len(errs) != 1 || errs[0].Kind != "missing_value" {
		t.Fatalf("expected 1 missing_value error, got %v", errs)
	}
	val, present := out["email"]
	if !present {
		t.Error("email key must be present in output map")
	}
	if val != nil {
		t.Errorf("email value = %v, want nil", val)
	}
}

func TestJsonValidatorTypeMismatchIsNull(t *testing.T) {
	v := newJsonValidator()
	// age expected Numerical, got string.
	out, errs, _ := v.Validate(numbered[jsonRow]{Row: 3, Data: jsonRow{"name": "Bob", "age": "thirty", "email": "b@x"}})
	if len(errs) != 1 || errs[0].Kind != "type_mismatch" {
		t.Fatalf("expected 1 type_mismatch error, got %v", errs)
	}
	val, present := out["age"]
	if !present {
		t.Error("age key must be present in output map")
	}
	if val != nil {
		t.Errorf("age value = %v (%T), want nil — leaving the string in place would crash pgx on insert into DOUBLE PRECISION", val, val)
	}
}

func TestJsonValidatorDropsExtraKeys(t *testing.T) {
	// A row with an extra key not in columnKeys must NOT carry it through.
	v := newJsonValidator()
	in := numbered[jsonRow]{Row: 4, Data: jsonRow{
		"name":   "Carol",
		"age":    float64(25),
		"email":  "c@x",
		"unused": "should-be-dropped",
	}}
	out, _, _ := v.Validate(in)
	if _, present := out["unused"]; present {
		t.Errorf("output contains extra key %q (input keys outside columnKeys must be dropped)", "unused")
	}
	if len(out) != 3 {
		t.Errorf("out has %d keys, want 3 (only columnKeys)", len(out))
	}
}

func TestJsonValidatorAlwaysReturnsAllColumnKeys(t *testing.T) {
	v := newJsonValidator()
	cases := []jsonRow{
		{"name": "Alice", "age": float64(30), "email": "a@x"},
		{},
		{"name": "Bob"},
		{"age": "thirty"},
	}
	for i, data := range cases {
		out, _, _ := v.Validate(numbered[jsonRow]{Row: int32(i + 1), Data: data})
		if len(out) != 3 {
			t.Errorf("case %d: out has %d keys, want 3", i, len(out))
		}
		for _, k := range []string{"name", "age", "email"} {
			if _, ok := out[k]; !ok {
				t.Errorf("case %d: missing key %q in out", i, k)
			}
		}
	}
}
