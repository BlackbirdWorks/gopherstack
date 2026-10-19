package opencypher

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Result is the table produced by RETURN; a query with no RETURN has no columns.
type Result struct {
	Columns []string
	Rows    [][]any
}

// MarshalJSON renders the rows as Neptune's openCypher results array (entities use ~id/~entityType shapes).
func (r *Result) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('[')

	for i, row := range r.Rows {
		if i > 0 {
			buf.WriteByte(',')
		}

		buf.WriteByte('{')

		for j, col := range r.Columns {
			if j > 0 {
				buf.WriteByte(',')
			}

			k, _ := json.Marshal(col)
			v, err := json.Marshal(jsonValue(row[j]))
			if err != nil {
				return nil, fmt.Errorf("encode %s: %w", col, err)
			}

			buf.Write(k)
			buf.WriteByte(':')
			buf.Write(v)
		}

		buf.WriteByte('}')
	}

	buf.WriteByte(']')

	return buf.Bytes(), nil
}

func jsonValue(v any) any {
	switch t := v.(type) {
	case *Node:
		return map[string]any{
			IDProperty: t.ID, "~entityType": "node", "~labels": append([]string{}, t.Labels...),
			"~properties": jsonProps(t.Props),
		}
	case *Rel:
		return map[string]any{
			IDProperty: t.ID, "~entityType": "relationship", "~start": t.Start, "~end": t.End,
			"~type": t.Type, "~properties": jsonProps(t.Props),
		}
	case *Path:
		items := []any{}
		for i, n := range t.Nodes {
			items = append(items, jsonValue(n))
			if i < len(t.Rels) {
				items = append(items, jsonValue(t.Rels[i]))
			}
		}

		return items
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = jsonValue(e)
		}

		return out
	case map[string]any:
		return jsonProps(t)
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil
		}

		return t
	default:
		return v
	}
}

func jsonProps(p map[string]any) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = jsonValue(v)
	}

	return out
}

// ParseParameters decodes Neptune's `parameters` JSON object into engine values.
func ParseParameters(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}, nil
	}

	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()

	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, &Error{Code: CodeInvalidParameter, Detail: "parameters must be a JSON object: " + err.Error()}
	}

	for k, v := range m {
		m[k] = normalize(v)
	}

	return m, nil
}

func normalize(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}

		f, _ := t.Float64()

		return f
	case []any:
		for i, e := range t {
			t[i] = normalize(e)
		}

		return t
	case map[string]any:
		for k, e := range t {
			t[k] = normalize(e)
		}

		return t
	default:
		return v
	}
}

func isNumber(v any) bool {
	switch v.(type) {
	case int64, float64:
		return true
	default:
		return false
	}
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case int64:
		return float64(t)
	case float64:
		return t
	default:
		return math.NaN()
	}
}

// equals is Cypher equality: known=false means the result is null.
func equals(a, b any) (bool, bool) {
	if a == nil || b == nil {
		return false, false
	}

	switch x := a.(type) {
	case int64, float64:
		return isNumber(b) && toFloat(a) == toFloat(b), true
	case []any:
		return listEquals(x, b)
	case map[string]any:
		return mapEquals(x, b)
	default:
		return identityEquals(a, b), true
	}
}

func identityEquals(a, b any) bool {
	switch x := a.(type) {
	case string:
		y, ok := b.(string)

		return ok && x == y
	case bool:
		y, ok := b.(bool)

		return ok && x == y
	case *Node:
		y, ok := b.(*Node)

		return ok && x.ID == y.ID
	case *Rel:
		y, ok := b.(*Rel)

		return ok && x.ID == y.ID
	default:
		return false
	}
}

func listEquals(x []any, b any) (bool, bool) {
	y, ok := b.([]any)
	if !ok || len(x) != len(y) {
		return false, true
	}

	unknown := false

	for i := range x {
		eq, known := equals(x[i], y[i])
		if known && !eq {
			return false, true
		}

		unknown = unknown || !known
	}

	return !unknown, !unknown
}

func mapEquals(x map[string]any, b any) (bool, bool) {
	y, ok := b.(map[string]any)
	if !ok || len(x) != len(y) {
		return false, true
	}

	for k, v := range x {
		w, has := y[k]
		if !has {
			return false, true
		}

		if eq, known := equals(v, w); known && !eq {
			return false, true
		}
	}

	return true, true
}

// compare orders comparable values; ok=false means the comparison is null.
func compare(a, b any) (int, bool) {
	switch x := a.(type) {
	case int64, float64:
		if isNumber(b) {
			return cmp.Compare(toFloat(a), toFloat(b)), true
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			return compareBool(x, y), true
		}
	case []any:
		if y, ok := b.([]any); ok {
			return compareLists(x, y)
		}
	}

	return 0, false
}

func compareBool(x, y bool) int {
	switch {
	case x == y:
		return 0
	case !x:
		return -1
	default:
		return 1
	}
}

func compareLists(x, y []any) (int, bool) {
	for i := 0; i < len(x) && i < len(y); i++ {
		c, ok := compare(x[i], y[i])
		if !ok {
			return 0, false
		}

		if c != 0 {
			return c, true
		}
	}

	return cmp.Compare(len(x), len(y)), true
}

func typeRank(v any) int {
	switch v.(type) {
	case map[string]any:
		return 1
	case *Node:
		return 2 //nolint:mnd // Cypher orderability rank
	case *Rel:
		return 3 //nolint:mnd // Cypher orderability rank
	case []any:
		return 4 //nolint:mnd // Cypher orderability rank
	case string:
		return 5 //nolint:mnd // Cypher orderability rank
	case bool:
		return 6 //nolint:mnd // Cypher orderability rank
	case nil:
		return 8 //nolint:mnd // null sorts last ascending
	default:
		return 7 //nolint:mnd // numbers
	}
}

// order is the total order used by ORDER BY, min and max.
func order(a, b any) int {
	if ra, rb := typeRank(a), typeRank(b); ra != rb {
		return cmp.Compare(ra, rb)
	}

	if c, ok := compare(a, b); ok {
		return c
	}

	return strings.Compare(valueKey(a), valueKey(b))
}

// valueKey is a canonical string used for grouping and DISTINCT.
func valueKey(v any) string {
	switch t := v.(type) {
	case nil:
		return "n"
	case int64:
		return "f" + strconv.FormatFloat(float64(t), 'g', -1, 64)
	case float64:
		return "f" + strconv.FormatFloat(t, 'g', -1, 64)
	case string:
		return "s" + strconv.Quote(t)
	case bool:
		return "b" + strconv.FormatBool(t)
	case *Node:
		return "N" + t.ID
	case *Rel:
		return "R" + t.ID
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = valueKey(e)
		}

		return "[" + strings.Join(parts, ",") + "]"
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = strconv.Quote(k) + ":" + valueKey(t[k])
		}

		return "{" + strings.Join(parts, ",") + "}"
	default:
		return fmt.Sprintf("?%v", t)
	}
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "Null"
	case int64:
		return "Integer"
	case float64:
		return "Float"
	case string:
		return "String"
	case bool:
		return "Boolean"
	case []any:
		return "List"
	case map[string]any:
		return "Map"
	case *Node:
		return "Node"
	case *Rel:
		return "Relationship"
	default:
		return "Path"
	}
}

// validProperty reports whether v may be stored as a property: scalars and homogeneous scalar lists.
func validProperty(v any) bool {
	switch t := v.(type) {
	case int64, float64, string, bool:
		return true
	case []any:
		for _, e := range t {
			switch e.(type) {
			case int64, float64, string, bool:
			default:
				return false
			}
		}

		return true
	default:
		return false
	}
}
