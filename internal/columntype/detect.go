package columntype

import (
	"strconv"
	"time"
)

// dateFormats is the ordered list of layouts we recognize as IS_DATE when
// parsing strings. Order matters: longer/more-specific formats come first
// so that e.g. "2006-01-02 15:04:05 -0700 MST" doesn't get mis-classified
// by a shorter prefix-matching layout. (time.Parse rejects trailing input,
// so the longer layouts won't match the shorter date-only strings.)
var dateFormats = []string{
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05 +0000 UTC",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// Detect classifies a string value's type and (when classifiable to a
// non-string type) returns its parsed Go form. The returned typename is one
// of the package constants: IS_NUMERICAL, IS_BOOLEAN, IS_DATE, or
// IS_CATEGORICAL. Empty input is reported as IS_CATEGORICAL with parsed = "".
//
// Recognition order: int → float → bool → date → categorical. Bool comes
// after the numeric checks because strconv.ParseBool accepts "0" and "1".
func Detect(value string) (typename string, parsed any) {
	if len(value) == 0 {
		return IS_CATEGORICAL, value
	}

	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return IS_NUMERICAL, i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return IS_NUMERICAL, f
	}
	if b, err := strconv.ParseBool(value); err == nil {
		return IS_BOOLEAN, b
	}
	for _, layout := range dateFormats {
		if t, err := time.Parse(layout, value); err == nil {
			return IS_DATE, t
		}
	}
	return IS_CATEGORICAL, value
}

// Parse returns Detect's parsed value, discarding the typename. Equivalent to
// the legacy ParseValue: callers that just want the typed Go value.
func Parse(value string) any {
	_, p := Detect(value)
	return p
}

// Classify returns Detect's typename, discarding the parsed value. Equivalent
// to the legacy ComputeVariableType: callers that just want the column type.
func Classify(value string) string {
	t, _ := Detect(value)
	return t
}

// FromGo classifies a Go runtime value, mirroring Classify but without the
// fmt.Sprintf("%v", v) round-trip. Use this when the value already has a Go
// type (e.g. after JSON decoding).
func FromGo(val any) string {
	switch val.(type) {
	case time.Time:
		return IS_DATE
	case float64, float32, int, int8, int16, int32, int64:
		return IS_NUMERICAL
	case bool:
		return IS_BOOLEAN
	default:
		return IS_CATEGORICAL
	}
}
