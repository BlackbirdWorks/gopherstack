package parquetio

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// containerMark prefixes the JSON text of nested values produced by the SQL engine.
const containerMark = "\x01"

func decodeNested(s string) (any, error) {
	dec := jsontext.NewDecoder(strings.NewReader(strings.TrimPrefix(s, containerMark)))

	var out any
	if err := json.UnmarshalDecode(dec, &out, jsonx.V1(), jsonx.UseNumber()); err != nil {
		return nil, fmt.Errorf("%w: nested value %q: %w", ErrBadValue, s, err)
	}

	return normalizeNested(out), nil
}

func normalizeNested(v any) any {
	switch t := v.(type) {
	case jsonx.Number:
		if n, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return n
		}

		f, _ := strconv.ParseFloat(t.String(), 64)

		return f
	case []any:
		for i, e := range t {
			t[i] = normalizeNested(e)
		}

		return t
	case map[string]any:
		for k, e := range t {
			t[k] = normalizeNested(e)
		}

		return t
	default:
		return v
	}
}

func asObject(v any) (map[string]any, error) {
	if s, ok := v.(string); ok {
		d, err := decodeNested(s)
		if err != nil {
			return nil, err
		}

		v = d
	}

	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %v is not an object", ErrBadValue, v)
	}

	return m, nil
}

func asArray(v any) ([]any, error) {
	if s, ok := v.(string); ok {
		d, err := decodeNested(s)
		if err != nil {
			return nil, err
		}

		v = d
	}

	a, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %v is not an array", ErrBadValue, v)
	}

	return a, nil
}
