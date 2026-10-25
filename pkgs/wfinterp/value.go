// Package wfinterp is an interpreter for the Google Cloud Workflows language: YAML or JSON workflow
// sources, ${...} expressions and the standard library. It performs no I/O of its own; time, HTTP,
// logging, callbacks and connectors come from a Host.
package wfinterp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

var errTrailingJSON = errors.New("trailing data")

// FuncRef is a reference to a named function or subworkflow, as produced by ${http.default_retry_predicate}.
type FuncRef struct{ Name string }

const (
	typeNull   = "null"
	typeBool   = "bool"
	typeInt    = "int"
	typeDouble = "double"
	typeString = "string"
	typeBytes  = "bytes"
	typeList   = "list"
	typeMap    = "map"
	typeFunc   = "function"
)

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return typeNull
	case bool:
		return typeBool
	case int64:
		return typeInt
	case float64:
		return typeDouble
	case string:
		return typeString
	case []byte:
		return typeBytes
	case []any:
		return typeList
	case map[string]any:
		return typeMap
	case *FuncRef:
		return typeFunc
	default:
		return fmt.Sprintf("%T", v)
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func isNumber(v any) bool {
	_, ok := toFloat(v)

	return ok
}

func equal(a, b any) bool {
	if fa, ok := toFloat(a); ok {
		fb, isNum := toFloat(b)

		return isNum && fa == fb
	}

	switch x := a.(type) {
	case []any:
		return equalLists(x, b)
	case map[string]any:
		return equalMaps(x, b)
	case []byte:
		y, ok := b.([]byte)

		return ok && bytes.Equal(x, y)
	case *FuncRef:
		y, ok := b.(*FuncRef)

		return ok && x.Name == y.Name
	default:
		return a == b
	}
}

func equalLists(x []any, b any) bool {
	y, ok := b.([]any)
	if !ok || len(x) != len(y) {
		return false
	}

	for i := range x {
		if !equal(x[i], y[i]) {
			return false
		}
	}

	return true
}

func equalMaps(x map[string]any, b any) bool {
	y, ok := b.(map[string]any)
	if !ok || len(x) != len(y) {
		return false
	}

	for k, v := range x {
		w, has := y[k]
		if !has || !equal(v, w) {
			return false
		}
	}

	return true
}

func deepCopy(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = deepCopy(x[i])
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = deepCopy(e)
		}

		return out
	case []byte:
		return bytes.Clone(x)
	default:
		return v
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// ParseJSON decodes JSON into workflow values: integers become int64, other numbers float64.
func ParseJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	if dec.More() {
		return nil, fmt.Errorf("invalid JSON: %w", errTrailingJSON)
	}

	return normalizeJSON(v), nil
}

func normalizeJSON(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil && !strings.ContainsAny(string(x), ".eE") {
			return i
		}

		f, _ := x.Float64()

		return f
	case []any:
		for i := range x {
			x[i] = normalizeJSON(x[i])
		}

		return x
	case map[string]any:
		for k := range x {
			x[k] = normalizeJSON(x[k])
		}

		return x
	default:
		return v
	}
}

// ToJSON encodes a workflow value as JSON; bytes become base64 strings.
func ToJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(jsonSafe(v))
	if err != nil {
		return nil, fmt.Errorf("encode JSON: %w", err)
	}

	return raw, nil
}

func jsonSafe(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = jsonSafe(x[i])
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = jsonSafe(e)
		}

		return out
	case *FuncRef:
		return "function:" + x.Name
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil
		}

		return x
	default:
		return v
	}
}

func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return typeNull
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		raw, err := ToJSON(v)
		if err != nil {
			return fmt.Sprint(v)
		}

		return string(raw)
	}
}
