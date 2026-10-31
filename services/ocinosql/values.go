package ocinosql

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// schemaView is the parsed form of a table's columns.
type schemaView struct {
	byName map[string]int
	cols   []Column
	types  []*dtype
}

func newSchemaView(cols []Column) *schemaView {
	sv := &schemaView{cols: cols, types: make([]*dtype, len(cols)), byName: make(map[string]int, len(cols))}

	for i, c := range cols {
		sv.types[i], _ = parseTypeString(c.Type)
		sv.byName[strs.Fold(c.Name)] = i
	}

	return sv
}

func (sv *schemaView) lookup(name string) (int, bool) {
	i, ok := sv.byName[strs.Fold(name)]

	return i, ok
}

const (
	decimalBase  = 10
	bigFloatPrec = 128
)

func decodeJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}

	return v, nil
}

func decodeRow(raw json.RawMessage) map[string]any {
	v, err := decodeJSON(raw)
	if err != nil {
		return map[string]any{}
	}

	m, _ := v.(map[string]any)

	return m
}

func coerceInt(v any, lo, hi int64, kind string) (any, string) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, "is not a valid " + kind
	}

	i, err := strconv.ParseInt(n.String(), 10, 64)
	if err != nil {
		f, _, ferr := big.ParseFloat(n.String(), decimalBase, bigFloatPrec, big.ToNearestEven)
		if ferr != nil || !f.IsInt() {
			return nil, "is not a valid " + kind
		}

		bi, _ := f.Int(nil)
		if !bi.IsInt64() {
			return nil, "is out of range for " + kind
		}

		i = bi.Int64()
	}

	if i < lo || i > hi {
		return nil, "is out of range for " + kind
	}

	return json.Number(strconv.FormatInt(i, 10)), ""
}

func parseTimestamp(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		for _, layout := range []string{
			time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02",
		} {
			if t, err := time.Parse(layout, x); err == nil {
				return t.UTC(), true
			}
		}
	case json.Number:
		if ms, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			return time.UnixMilli(ms).UTC(), true
		}
	}

	return time.Time{}, false
}

func formatTimestamp(t time.Time, prec int) string {
	layout := "2006-01-02T15:04:05"
	if prec > 0 {
		layout += "." + strings.Repeat("0", prec)
	}

	step := time.Duration(math.Pow10(maxTimestampPrecision - prec))

	return t.UTC().Truncate(step).Format(layout) + "Z"
}

// coerce validates v against t and returns its normalized form; the second result is a problem description.
func coerce(t *dtype, v any) (any, string) {
	if v == nil {
		return nil, ""
	}

	switch t.kind {
	case kInteger:
		return coerceInt(v, math.MinInt32, math.MaxInt32, kInteger)
	case kLong:
		return coerceInt(v, math.MinInt64, math.MaxInt64, kLong)
	case kFloat, kDouble:
		return coerceFloat(t.kind, v)
	case kString, kBoolean, kNumber, kBinary, kTimestamp, kEnum:
		return coerceScalar(t, v)
	}

	return coerceComposite(t, v)
}

func coerceScalar(t *dtype, v any) (any, string) {
	switch t.kind {
	case kNumber:
		n, ok := v.(json.Number)
		if _, rok := new(big.Rat).SetString(n.String()); !ok || !rok {
			return nil, "is not a valid NUMBER"
		}

		return n, ""
	case kString:
		if _, ok := v.(string); !ok {
			return nil, "is not a valid STRING"
		}
	case kBoolean:
		if _, ok := v.(bool); !ok {
			return nil, "is not a valid BOOLEAN"
		}
	case kBinary:
		s, ok := v.(string)
		if _, err := base64.StdEncoding.DecodeString(s); !ok || err != nil {
			return nil, "is not a valid base64-encoded BINARY"
		}
	case kTimestamp:
		ts, ok := parseTimestamp(v)
		if !ok {
			return nil, "is not a valid TIMESTAMP"
		}

		return formatTimestamp(ts, t.prec), ""
	case kEnum:
		s, _ := v.(string)
		if !strs.ContainsFold(t.enum, s) {
			return nil, "is not one of the ENUM values"
		}
	}

	return v, ""
}

func coerceComposite(t *dtype, v any) (any, string) {
	switch t.kind {
	case kArray:
		arr, ok := v.([]any)
		if !ok {
			return nil, "is not a valid ARRAY"
		}

		out := make([]any, len(arr))

		for i, e := range arr {
			n, msg := coerce(t.elem, e)
			if msg != "" {
				return nil, "element " + strconv.Itoa(i) + " " + msg
			}

			out[i] = n
		}

		return out, ""
	case kMap:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, "is not a valid MAP"
		}

		out := make(map[string]any, len(m))

		for k, e := range m {
			n, msg := coerce(t.elem, e)
			if msg != "" {
				return nil, "entry '" + k + "' " + msg
			}

			out[k] = n
		}

		return out, ""
	case kRecord:
		return coerceRecord(t, v)
	}

	return v, ""
}

func coerceRecord(t *dtype, v any) (any, string) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, "is not a valid RECORD"
	}

	out := make(map[string]any, len(t.fields))
	used := 0

	for _, f := range t.fields {
		var raw any

		for k, e := range m {
			if strs.Equal(k, f.name) {
				raw, used = e, used+1
			}
		}

		n, msg := coerce(f.typ, raw)
		if msg != "" {
			return nil, "field '" + f.name + "' " + msg
		}

		out[f.name] = n
	}

	if used != len(m) {
		return nil, "has fields that are not part of the RECORD"
	}

	return out, ""
}

func cmpNum(a, b json.Number) (int, bool) {
	ra, ok1 := new(big.Rat).SetString(a.String())
	rb, ok2 := new(big.Rat).SetString(b.String())

	if !ok1 || !ok2 {
		return 0, false
	}

	return ra.Cmp(rb), true
}

// compareVals orders two scalar values of the same family; ok is false when they are not comparable.
func compareVals(a, b any) (int, bool) {
	switch x := a.(type) {
	case json.Number:
		if y, ok := b.(json.Number); ok {
			return cmpNum(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			switch {
			case x == y:
				return 0, true
			case !x:
				return -1, true
			}

			return 1, true
		}
	}

	return 0, false
}

func keyPart(v any) string {
	if n, ok := v.(json.Number); ok {
		if r, rok := new(big.Rat).SetString(n.String()); rok {
			return r.RatString()
		}
	}

	s, _ := v.(string)

	return s
}

// rowKey builds the canonical primary-key string from a normalized row.
func rowKey(pk []string, row map[string]any) (string, *APIError) {
	parts := make([]string, len(pk))

	for i, c := range pk {
		v, ok := row[c]
		if !ok || v == nil {
			return "", errParam("primary key column '" + c + "' is missing a value")
		}

		parts[i] = keyPart(v)
	}

	b, _ := json.Marshal(parts) //nolint:errchkjson // []string cannot fail

	return string(b), nil
}

func newUUID() string { return uuid.NewString() }

func validUUID(s string) bool {
	_, err := uuid.Parse(s)

	return err == nil
}

func coerceFloat(kind string, v any) (any, string) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, "is not a valid " + kind
	}

	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil {
		return nil, "is not a valid " + kind
	}

	return json.Number(strconv.FormatFloat(f, 'g', -1, 64)), ""
}
