package athena

import (
	"database/sql/driver"
	"encoding/json/v2"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	// rowMarker is the reserved key listing a ROW value's field names in declaration order.
	rowMarker = "\x00row"
	// anonField prefixes the synthetic key of an unnamed ROW field.
	anonField = "\x00"
)

func rowFieldName(i int) string { return anonField + strconv.Itoa(i) }

func isAnonField(name string) bool { return strings.HasPrefix(name, anonField) }

// newRow builds a ROW value; a nil name makes the field anonymous.
func newRow(names []string, vals []any) map[string]any {
	keys := make([]any, len(names))
	m := make(map[string]any, len(names)+1)

	for i, n := range names {
		if n == "" {
			n = rowFieldName(i)
		}

		keys[i] = n
		m[n] = vals[i]
	}

	m[rowMarker] = keys

	return m
}

// rowNames returns the ordered field names when m is a ROW value.
func rowNames(m map[string]any) ([]string, bool) {
	raw, ok := m[rowMarker].([]any)
	if !ok {
		return nil, false
	}

	names := make([]string, len(raw))
	for i, r := range raw {
		names[i], _ = r.(string)
	}

	return names, true
}

func renderRow(m map[string]any, names []string) string {
	allAnon := true
	parts := make([]string, len(names))

	for i, n := range names {
		v := trinoString(m[n])
		if isAnonField(n) {
			parts[i] = v

			continue
		}

		allAnon = false
		parts[i] = n + "=" + v
	}

	if allAnon {
		return "[" + strings.Join(parts, ", ") + "]"
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

// plainJSON encodes v with ROW values as ordered JSON objects (arrays when anonymous) and no markers.
func plainJSON(v any) string {
	switch t := v.(type) {
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = plainJSON(e)
		}

		return "[" + strings.Join(parts, ",") + "]"
	case map[string]any:
		return plainMap(t)
	default:
		b, err := json.Marshal(t, jsonx.V1())
		if err != nil {
			return kwNull
		}

		return string(b)
	}
}

func plainMap(m map[string]any) string {
	if names, ok := rowNames(m); ok {
		parts := make([]string, len(names))
		anon := len(names) > 0

		for i, n := range names {
			anon = anon && isAnonField(n)
			key, _ := json.Marshal(n, jsonx.V1())
			parts[i] = string(key) + ":" + plainJSON(m[n])
		}

		if anon {
			for i, n := range names {
				parts[i] = plainJSON(m[n])
			}

			return "[" + strings.Join(parts, ",") + "]"
		}

		return "{" + strings.Join(parts, ",") + "}"
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, len(keys))

	for i, k := range keys {
		key, _ := json.Marshal(k, jsonx.V1())
		parts[i] = string(key) + ":" + plainJSON(m[k])
	}

	return "{" + strings.Join(parts, ",") + "}"
}

// stripRowMarkers rewrites a container's JSON text without ROW markers; other text is returned unchanged.
func stripRowMarkers(s string) string {
	if !strings.Contains(s, `\u0000row`) {
		return s
	}

	root, ok := parseJSONArg(s)
	if !ok {
		return s
	}

	return plainJSON(root)
}

func udfRow(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	vals := make([]any, len(args))
	names := make([]string, len(args))

	for i, a := range args {
		vals[i] = jsonElem(a)
	}

	return jsonText(newRow(names, vals)), nil
}

func rowSubscript(m map[string]any, names []string, idx driver.Value) (driver.Value, error) {
	i, ok := toInt(idx)
	if !ok {
		return sqlNull(), nil
	}

	if i < 1 || i > int64(len(names)) {
		return nil, errArg("ROW subscript out of range: %d", i)
	}

	return sqlValue(m[names[i-1]]), nil
}

// typeNode is a parsed Trino type: a scalar, array, map or row.
type typeNode struct {
	base   string
	names  []string
	params []*typeNode
}

func parseTypeNode(s string) (*typeNode, error) {
	s = strings.TrimSpace(s)
	open := strings.IndexAny(s, "(<")

	if open < 0 {
		return &typeNode{base: strings.ToLower(s)}, nil
	}

	base := strings.ToLower(strings.TrimSpace(s[:open]))
	inner := s[open+1 : len(s)-1]
	n := &typeNode{base: base}

	switch base {
	case kindRow, "struct":
		n.base = kindRow

		for _, f := range splitTopLevel(inner) {
			name, typ, ok := strings.Cut(f, " ")
			if !ok {
				return nil, errArg("Invalid ROW field definition: %s", f)
			}

			child, err := parseTypeNode(typ)
			if err != nil {
				return nil, err
			}

			n.names = append(n.names, strings.ToLower(strings.Trim(name, `"`)))
			n.params = append(n.params, child)
		}
	case kindArray, kindMap:
		for _, f := range splitTopLevel(inner) {
			child, err := parseTypeNode(f)
			if err != nil {
				return nil, err
			}

			n.params = append(n.params, child)
		}
	default:
		n.base = base
	}

	return n, nil
}

func castScalarInt(v any) (any, error) {
	switch t := v.(type) {
	case float64:
		return int64(math.Round(t)), nil
	case bool:
		if t {
			return int64(1), nil
		}

		return int64(0), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		if err != nil {
			return nil, errArg("Cannot cast '%s' to bigint", t)
		}

		return n, nil
	default:
		return v, nil
	}
}

func castScalarFloat(v any) (any, error) {
	if s, ok := v.(string); ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return nil, errArg("Cannot cast '%s' to double", s)
		}

		return f, nil
	}

	return v, nil
}

func castScalarString(v any) any {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return strconv.FormatInt(int64(t), 10)
		}

		return formatFloat(t)
	case []any, map[string]any:
		return trinoString(t)
	default:
		return fmt.Sprint(t)
	}
}

func castScalar(v any, base string) (any, error) {
	switch base {
	case typeTinyint, typeSmallint, typeInteger, typeInt, typeBigint:
		return castScalarInt(v)
	case typeDouble, typeReal, typeFloat, typeDecimal:
		return castScalarFloat(v)
	case typeVarchar, typeChar, columnTypeString:
		return castScalarString(v), nil
	case typeBoolean:
		if s, ok := v.(string); ok {
			return strings.EqualFold(strings.TrimSpace(s), strTrue), nil
		}

		return v, nil
	default:
		return v, nil
	}
}

func castRow(v any, n *typeNode) (any, error) {
	var vals []any

	switch t := v.(type) {
	case map[string]any:
		src, ok := rowNames(t)
		if !ok {
			return castRowByName(t, n)
		}

		for _, name := range src {
			vals = append(vals, t[name])
		}
	case []any:
		vals = t
	default:
		return nil, errArg("Cannot cast %T to ROW", v)
	}

	if len(vals) != len(n.names) {
		return nil, errArg("Cannot cast ROW with %d fields to ROW with %d fields", len(vals), len(n.names))
	}

	out := make([]any, len(vals))

	for i, e := range vals {
		c, err := castTo(e, n.params[i])
		if err != nil {
			return nil, err
		}

		out[i] = c
	}

	return newRow(n.names, out), nil
}

// castRowByName fills a target ROW from an unordered struct by field name.
func castRowByName(m map[string]any, n *typeNode) (any, error) {
	out := make([]any, len(n.names))

	for i, name := range n.names {
		for k, v := range m {
			if strings.EqualFold(k, name) {
				c, err := castTo(v, n.params[i])
				if err != nil {
					return nil, err
				}

				out[i] = c
			}
		}
	}

	return newRow(n.names, out), nil
}

func castTo(v any, n *typeNode) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	switch n.base {
	case kindRow:
		return castRow(v, n)
	case kindArray:
		arr, ok := v.([]any)
		if !ok || len(n.params) != 1 {
			return nil, errArg("Cannot cast %T to ARRAY", v)
		}

		out := make([]any, len(arr))

		for i, e := range arr {
			c, err := castTo(e, n.params[0])
			if err != nil {
				return nil, err
			}

			out[i] = c
		}

		return out, nil
	case kindMap:
		return v, nil
	default:
		return castScalar(v, n.base)
	}
}

func udfRowCast(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	typ, _ := asString(args[0])

	if args[1] == nil {
		return sqlNull(), nil
	}

	node, err := parseTypeNode(typ)
	if err != nil {
		return nil, err
	}

	out, err := castTo(jsonElem(args[1]), node)
	if err != nil {
		return nil, err
	}

	return sqlValue(out), nil
}

func registerRowFuncs() {
	registerScalar("athena_row", -1, udfRow)
	registerScalar("athena_row_cast", argsTwo, udfRowCast)
}

// itemBounds returns the select-item span around toks[i]: from the previous comma or SELECT to
// the next comma, FROM or closing paren, climbing out of any call that encloses toks[i].
func itemBounds(toks []token, i int) (int, int) {
	start, climbed, depth := 0, 0, 0

back:
	for j := i - 1; j >= 0; j-- {
		t := toks[j]

		switch {
		case t.isPunct(")"):
			depth++
		case t.isPunct("("):
			if depth == 0 {
				climbed++

				continue
			}

			depth--
		case depth == 0 && (t.isPunct(",") || t.isWord(kwSelect)):
			start = j + 1

			break back
		}
	}

	depth = climbed

	for j := i; j < len(toks); j++ {
		t := toks[j]

		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			if depth == 0 {
				return start, j
			}

			depth--
		case depth == 0 && (t.isPunct(",") || t.isWord(kwFrom)):
			return start, j
		}
	}

	return start, len(toks)
}

// learnRowAliases records output names of select items that build ROW values so name.field resolves.
func learnRowAliases(toks []token, into map[string]bool) {
	for i, t := range toks {
		if !t.isWord(kindRow) {
			continue
		}

		if n := nextSig(toks, i); n < 0 || !toks[n].isPunct("(") {
			continue
		}

		start, end := itemBounds(toks, i)
		_, alias := splitAliasSig(significant(toks[start:end]))

		if alias != "" {
			into[alias] = true
		}
	}
}

func isRowValue(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}

	_, marked := m[rowMarker]

	return marked
}
