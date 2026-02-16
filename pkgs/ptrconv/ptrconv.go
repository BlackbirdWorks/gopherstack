package ptrconv

// Int64 returns 0 when the pointer is nil.
func Int64(v *int64) int64 {
	if v == nil {
		return 0
	}

	return *v
}

// Bool returns false when the pointer is nil.
func Bool(v *bool) bool {
	if v == nil {
		return false
	}

	return *v
}

// String returns an empty string when the pointer is nil.
func String(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}

// Float64 returns 0 when the pointer is nil.
func Float64(v *float64) float64 {
	if v == nil {
		return 0
	}

	return *v
}

// Int64FromAny converts common numeric inputs to *int64.
func Int64FromAny(v any) *int64 {
	switch val := v.(type) {
	case float64:
		i := int64(val)

		return &i
	case int:
		i := int64(val)

		return &i
	}

	return nil
}

// NilIfEmpty returns nil for empty strings, otherwise the string pointer.
func NilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}
