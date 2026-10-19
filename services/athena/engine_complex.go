package athena

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	sqlitedriver "modernc.org/sqlite"
)

// containerMark prefixes the JSON text of an ARRAY/MAP value so results render
// Trino-style while JSON-typed and plain varchar values stay untouched.
const containerMark = "\x01"

const (
	kindArray = "array"
	kindMap   = "map"
	kindRow   = "row"

	declArray = "ARRAY"
	declMap   = "MAP"
	declRow   = "ROW"

	errArgPrefix = "INVALID_FUNCTION_ARGUMENT: "
)

func isComplexDecl(decl string) bool {
	switch strings.ToUpper(decl) {
	case declArray, declMap, declRow:
		return true
	default:
		return false
	}
}

func complexTypeOf(glueType string) (string, bool) {
	t := strings.ToLower(strings.TrimSpace(glueType))

	switch {
	case strings.HasPrefix(t, "array<"):
		return declArray, true
	case strings.HasPrefix(t, "map<"):
		return declMap, true
	case strings.HasPrefix(t, "struct<"), strings.HasPrefix(t, "row("):
		return declRow, true
	default:
		return "", false
	}
}

func decodeComplex(s string) (any, bool) {
	dec := json.NewDecoder(strings.NewReader(strings.TrimPrefix(s, containerMark)))
	dec.UseNumber()

	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, false
	}

	switch out.(type) {
	case []any, map[string]any:
		return out, true
	default:
		return nil, false
	}
}

// trinoString renders a decoded JSON value the way Athena prints complex results.
func trinoString(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case json.Number:
		return t.String()
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = trinoString(e)
		}

		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = k + "=" + trinoString(t[k])
		}

		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprint(t)
	}
}

// displayComplex renders s as an Athena complex value when it is one, returning
// the rendering and its Athena type; ok=false leaves the cell for plain handling.
func displayComplex(s, decl string) (string, string, bool) {
	if !strings.HasPrefix(s, containerMark) && !isComplexDecl(decl) {
		return "", "", false
	}

	v, ok := decodeComplex(s)
	if !ok {
		return "", "", false
	}

	kind := kindArray

	if _, isMap := v.(map[string]any); isMap {
		kind = kindMap
		if strings.EqualFold(decl, declRow) {
			kind = kindRow
		}
	}

	return trinoString(v), kind, true
}

var errInvalidArg = errors.New("INVALID_FUNCTION_ARGUMENT")

func errArg(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalidArg, fmt.Sprintf(format, args...))
}

func udfAthenaArray(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = jsonElem(a)
	}

	return jsonText(out), nil
}

func subscriptArray(a []any, idx driver.Value) (driver.Value, error) {
	i, ok := toInt(idx)
	if !ok {
		return sqlNull(), nil
	}

	switch {
	case i == 0:
		return nil, errArg("SQL array indices start at 1")
	case i < 0:
		return nil, errArg("Array subscript is negative: %d", i)
	case i > int64(len(a)):
		return nil, errArg("Array subscript must be less than or equal to array length: %d > %d", i, len(a))
	}

	return sqlValue(a[i-1]), nil
}

func subscriptMap(m map[string]any, key driver.Value) (driver.Value, error) {
	k, ok := asKey(key)
	if !ok {
		return sqlNull(), nil
	}

	v, found := m[k]
	if !found {
		return nil, errArg("Key not present in map: %s", k)
	}

	return sqlValue(v), nil
}

func udfSubscript(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	if !ok || args[1] == nil {
		return sqlNull(), nil
	}

	switch c := root.(type) {
	case []any:
		return subscriptArray(c, args[1])
	case map[string]any:
		return subscriptMap(c, args[1])
	default:
		return sqlNull(), nil
	}
}

func udfStructField(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	name, nok := asString(args[1])

	m, isMap := root.(map[string]any)
	if !ok || !nok || !isMap {
		return sqlNull(), nil
	}

	for k, v := range m {
		if strings.EqualFold(k, name) {
			return sqlValue(v), nil
		}
	}

	return sqlNull(), nil
}

func udfUnmark(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return args[0], nil
	}

	return strings.TrimPrefix(s, containerMark), nil
}

// udfElem tags a json_each value as a container so lambda bodies see ARRAY/MAP elements.
func udfElem(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, _ := asString(args[1])
	if t == "array" || t == "object" {
		s, _ := asString(args[0])

		return containerMark + s, nil
	}

	return args[0], nil
}

func udfIndexList(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	n := 0

	for _, a := range args {
		if arr, ok := asArray(a); ok && len(arr) > n {
			n = len(arr)
		}
	}

	var buf bytes.Buffer

	buf.WriteByte('[')

	for i := range n {
		if i > 0 {
			buf.WriteByte(',')
		}

		buf.WriteString(strconv.Itoa(i))
	}

	buf.WriteByte(']')

	return buf.String(), nil
}

func registerComplexFuncs() {
	reg := sqlitedriver.MustRegisterDeterministicScalarFunction
	reg("athena_array", -1, udfAthenaArray)
	reg("athena_subscript", argsTwo, udfSubscript)
	reg("athena_field", argsTwo, udfStructField)
	reg("athena_unmark", 1, udfUnmark)
	reg("athena_elem", argsTwo, udfElem)
	reg("athena_index_list", argsTwo, udfIndexList)
}
