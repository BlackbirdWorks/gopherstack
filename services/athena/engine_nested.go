package athena

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/hivetype"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	typeBigintName  = "bigint"
	typeDoubleName  = "double"
	typeBooleanName = "boolean"
)

// orderedValue is a decoded JSON value whose object keys keep document order.
type orderedValue struct {
	text  string
	keys  []string
	items []*orderedValue
	kind  byte
}

const (
	ovNull   = 'n'
	ovBool   = 'b'
	ovNum    = '#'
	ovString = 's'
	ovArray  = '['
	ovObject = '{'
)

func decodeOrdered(dec *jsontext.Decoder) (*orderedValue, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}

	switch tok.Kind() {
	case 'n':
		return &orderedValue{kind: ovNull}, nil
	case 't', 'f':
		return &orderedValue{kind: ovBool}, nil
	case '0':
		return &orderedValue{kind: ovNum, text: tok.String()}, nil
	case '"':
		return &orderedValue{kind: ovString}, nil
	case '[':
		v := &orderedValue{kind: ovArray}

		for dec.PeekKind() != ']' {
			e, derr := decodeOrdered(dec)
			if derr != nil {
				return nil, derr
			}

			v.items = append(v.items, e)
		}

		_, err = dec.ReadToken()

		return v, err
	case '{':
		v := &orderedValue{kind: ovObject}

		for dec.PeekKind() != '}' {
			k, kerr := dec.ReadToken()
			if kerr != nil {
				return nil, kerr
			}

			key := k.String()

			e, derr := decodeOrdered(dec)
			if derr != nil {
				return nil, derr
			}

			v.keys, v.items = append(v.keys, key), append(v.items, e)
		}

		_, err = dec.ReadToken()

		return v, err
	default:
		return nil, fmt.Errorf("%w: unexpected JSON token", errUnsupportedFormat)
	}
}

func parseOrdered(s string) (*orderedValue, bool) {
	dec := jsontext.NewDecoder(strings.NewReader(strings.TrimPrefix(s, containerMark)))

	v, err := decodeOrdered(dec)
	if err != nil || (v.kind != ovArray && v.kind != ovObject) {
		return nil, false
	}

	return v, true
}

// glueType infers a Hive type from the JSON shape; "" means unknown (null or empty container).
func (v *orderedValue) glueType(asRow bool) string {
	switch v.kind {
	case ovBool:
		return typeBooleanName
	case ovString:
		return columnTypeString
	case ovNum:
		if strings.ContainsAny(v.text, ".eE") {
			return typeDoubleName
		}

		return typeBigintName
	case ovArray:
		if asRow {
			return rowTypeOf(v, true)
		}

		elem := ""
		for _, e := range v.items {
			elem = mergeGlueTypes(elem, e.nestedGlueType())
		}

		return "array<" + orDefault(elem, columnTypeString) + ">"
	case ovObject:
		if asRow {
			return rowTypeOf(v, false)
		}

		return mapTypeOf(v)
	default:
		return ""
	}
}

func mapTypeOf(v *orderedValue) string {
	val := ""
	for _, e := range v.items {
		val = mergeGlueTypes(val, e.nestedGlueType())
	}

	return "map<string," + orDefault(val, columnTypeString) + ">"
}

// nestedGlueType types a value inside a container, where objects are ROW fields.
func (v *orderedValue) nestedGlueType() string {
	if v.kind == ovObject {
		return rowTypeOf(v, false)
	}

	return v.glueType(false)
}

func rowTypeOf(v *orderedValue, anon bool) string {
	parts := make([]string, len(v.items))

	for i, e := range v.items {
		name := fmt.Sprintf("_field%d", i)
		if !anon {
			name = v.keys[i]
		}

		parts[i] = name + ":" + orDefault(e.nestedGlueType(), columnTypeString)
	}

	return "struct<" + strings.Join(parts, ",") + ">"
}

// mergeGlueTypes unifies the types seen for one position; incompatible shapes fall back to string.
func mergeGlueTypes(a, b string) string {
	switch {
	case a == "" || a == b:
		return b
	case b == "":
		return a
	case a == typeBigintName && b == typeDoubleName, a == typeDoubleName && b == typeBigintName:
		return typeDoubleName
	case strings.HasPrefix(a, "array<") && strings.HasPrefix(b, "array<"):
		return "array<" + mergeGlueTypes(a[len("array<"):len(a)-1], b[len("array<"):len(b)-1]) + ">"
	default:
		return columnTypeString
	}
}

// inferNestedColumns sets the Hive type of ARRAY/MAP/ROW result columns from their values and
// rewrites anonymous ROW values (JSON arrays) as objects keyed _field0, _field1, ...
func inferNestedColumns(res *sqlResult) {
	for ci := range res.columns {
		col := &res.columns[ci]
		if !col.nested {
			continue
		}

		asRow := col.typ == kindRow
		merged := ""

		for r := range res.rows {
			cell, null := res.cell(r, ci)
			if null {
				continue
			}

			v, ok := parseOrdered(cell)
			if !ok {
				continue
			}

			if asRow && v.kind == ovArray {
				res.rows[r][ci] = anonRowJSON(cell)
			}

			merged = mergeColumnType(merged, v.glueType(asRow))
		}

		col.glue = orDefault(merged, columnTypeString)
	}
}

func mergeColumnType(a, b string) string {
	if strings.HasPrefix(a, "struct<") && strings.HasPrefix(b, "struct<") && a != b {
		return a
	}

	return mergeGlueTypes(a, b)
}

func anonRowJSON(cell string) string {
	var arr []any

	if err := json.Unmarshal(
		[]byte(strings.TrimPrefix(cell, containerMark)),
		&arr,
		jsonx.V1(),
		jsonx.UseNumber(),
	); err != nil {
		return cell
	}

	obj := make(map[string]any, len(arr))
	for i, e := range arr {
		obj[fmt.Sprintf("_field%d", i)] = e
	}

	out, err := json.Marshal(obj, jsonx.V1())
	if err != nil {
		return cell
	}

	return string(out)
}

// rowTypesOf parses the declared type of every column that contains a struct (nil for the rest).
func rowTypesOf(cols []Column) []*hivetype.Type {
	var out []*hivetype.Type

	for i, c := range cols {
		t, err := hivetype.Parse(c.Type)
		if err != nil || !hasStruct(t) {
			continue
		}

		if out == nil {
			out = make([]*hivetype.Type, len(cols))
		}

		out[i] = t
	}

	return out
}

func hasStruct(t *hivetype.Type) bool {
	switch t.Kind {
	case hivetype.Struct:
		return true
	case hivetype.Array:
		return hasStruct(t.Elem)
	case hivetype.Map:
		return hasStruct(t.Value)
	default:
		return false
	}
}

// orderStructs rewrites a loaded nested value so struct members become ROW values in declared order.
func orderStructs(raw string, t *hivetype.Type) any {
	v, ok := decodeComplex(raw)
	if !ok {
		return raw
	}

	return jsonText(toRowValue(v, t))
}

func toRowValue(v any, t *hivetype.Type) any {
	switch t.Kind {
	case hivetype.Struct:
		return structRow(v, t)
	case hivetype.Array:
		if arr, ok := v.([]any); ok {
			for i, e := range arr {
				arr[i] = toRowValue(e, t.Elem)
			}
		}

		return v
	case hivetype.Map:
		if obj, ok := v.(map[string]any); ok {
			for k, e := range obj {
				obj[k] = toRowValue(e, t.Value)
			}
		}

		return v
	default:
		return v
	}
}

func structRow(v any, t *hivetype.Type) any {
	obj, ok := v.(map[string]any)
	if !ok {
		return v
	}

	names := make([]string, len(t.Fields))
	vals := make([]any, len(t.Fields))

	for i, f := range t.Fields {
		names[i] = f.Name

		for k, x := range obj {
			if strings.EqualFold(k, f.Name) {
				vals[i] = toRowValue(x, f.Type)
			}
		}
	}

	return newRow(names, vals)
}
