package bqsql

import (
	"bytes"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// ToSQLite converts a stored value into the value bound into SQLite: scalars map to INTEGER, REAL or TEXT and
// records and arrays to JSON text.
func ToSQLite(f Field, stored any) driver.Value {
	if stored == nil && !strings.EqualFold(f.Mode, ModeRepeated) {
		return nil
	}

	k, _ := KindOf(f.Type)
	if k == KindStruct || strings.EqualFold(f.Mode, ModeRepeated) {
		var buf bytes.Buffer

		writeJSON(&buf, f, stored)

		return buf.String()
	}

	s, _ := stored.(string)

	return scalarToSQLite(k, s)
}

func scalarToSQLite(k Kind, s string) driver.Value {
	switch k {
	case KindInt:
		n, _ := strconv.ParseInt(s, decimalBase, bitSize)

		return n
	case KindFloat:
		f, _ := parseFloatText(s)

		return f
	case KindNumeric, KindBigNumeric:
		f, _ := strconv.ParseFloat(s, bitSize)

		return f
	case KindBool:
		if s == litTrueX {
			return int64(1)
		}

		return int64(0)
	case KindBytes:
		b, _ := base64.StdEncoding.DecodeString(s)

		return b
	case KindUnknown, KindString, KindTimestamp, KindDate, KindTime, KindDatetime, KindJSON, KindGeography,
		KindStruct, KindArray:
		return s
	}

	return s
}

func writeJSON(buf *bytes.Buffer, f Field, stored any) {
	if strings.EqualFold(f.Mode, ModeRepeated) {
		arr, _ := stored.([]any)
		elem := f
		elem.Mode = ModeNullable

		buf.WriteByte('[')

		for i, e := range arr {
			if i > 0 {
				buf.WriteByte(',')
			}

			writeJSON(buf, elem, e)
		}

		buf.WriteByte(']')

		return
	}

	if stored == nil {
		buf.WriteString("null")

		return
	}

	k, _ := KindOf(f.Type)
	if k == KindStruct {
		writeJSONRecord(buf, f.Fields, stored)

		return
	}

	s, _ := stored.(string)
	writeJSONScalar(buf, k, s)
}

func writeJSONRecord(buf *bytes.Buffer, fields []Field, stored any) {
	rec, _ := stored.([]any)

	buf.WriteByte('{')

	for i, fl := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}

		name, _ := json.Marshal(fl.Name)
		buf.Write(name)
		buf.WriteByte(':')

		var v any
		if i < len(rec) {
			v = rec[i]
		}

		writeJSON(buf, fl, v)
	}

	buf.WriteByte('}')
}

func writeJSONScalar(buf *bytes.Buffer, k Kind, s string) {
	switch k {
	case KindInt, KindBool:
		buf.WriteString(s)
	case KindFloat:
		f, _ := parseFloatText(s)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			q, _ := json.Marshal(s)
			buf.Write(q)

			return
		}

		buf.WriteString(strconv.FormatFloat(f, 'g', -1, bitSize))
	case KindUnknown, KindString, KindBytes, KindTimestamp, KindDate, KindTime, KindDatetime, KindNumeric,
		KindBigNumeric, KindJSON, KindGeography, KindStruct, KindArray:
		q, _ := json.Marshal(s)
		buf.Write(q)
	}
}

// FromSQLite converts a value read from SQLite into the stored form for f.
func FromSQLite(f Field, v any) (any, error) {
	if v == nil {
		if strings.EqualFold(f.Mode, ModeRepeated) {
			return []any{}, nil
		}

		return nil, nil //nolint:nilnil // NULL is the stored nil
	}

	k, _ := KindOf(f.Type)
	if k == KindStruct || strings.EqualFold(f.Mode, ModeRepeated) {
		return fromJSONText(f, v)
	}

	return scalarFromSQLite(k, v)
}

func fromJSONText(f Field, v any) (any, error) {
	var text string

	switch t := v.(type) {
	case string:
		text = t
	case []byte:
		text = string(t)
	default:
		return nil, valueErr("expected JSON text for %s, got %T", f.Name, v)
	}

	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()

	var node any
	if err := dec.Decode(&node); err != nil {
		return nil, valueErr("invalid JSON for %s: %v", f.Name, err)
	}

	return fromJSONNode(f, node)
}

func fromJSONNode(f Field, node any) (any, error) {
	if strings.EqualFold(f.Mode, ModeRepeated) {
		if node == nil {
			return []any{}, nil
		}

		arr, ok := node.([]any)
		if !ok {
			return nil, valueErr("expected an array for %s", f.Name)
		}

		elem := f
		elem.Mode = ModeNullable
		out := make([]any, len(arr))

		for i, e := range arr {
			s, err := fromJSONNode(elem, e)
			if err != nil {
				return nil, err
			}

			out[i] = s
		}

		return out, nil
	}

	if node == nil {
		return nil, nil //nolint:nilnil // NULL is the stored nil
	}

	k, _ := KindOf(f.Type)
	if k == KindStruct {
		return fromJSONRecord(f, node)
	}

	if k == KindBytes {
		return storedBytes(node)
	}

	return scalarFromSQLite(k, node)
}

func fromJSONRecord(f Field, node any) (any, error) {
	obj, ok := node.(map[string]any)
	if !ok {
		return nil, valueErr("expected an object for %s", f.Name)
	}

	rec := make([]any, len(f.Fields))

	for i, fl := range f.Fields {
		cell, err := fromJSONNode(fl, jsonMember(obj, fl.Name))
		if err != nil {
			return nil, err
		}

		rec[i] = cell
	}

	return rec, nil
}

func jsonMember(obj map[string]any, name string) any {
	if v, ok := obj[name]; ok {
		return v
	}

	for k, v := range obj {
		if strings.EqualFold(k, name) {
			return v
		}
	}

	return nil
}

func scalarFromSQLite(k Kind, v any) (any, error) {
	switch k {
	case KindBool:
		return storedBool(v)
	case KindNumeric, KindBigNumeric:
		if fl, ok := v.(float64); ok {
			return FormatNumeric(fl), nil
		}
	case KindBytes:
		if b, ok := v.([]byte); ok {
			return base64.StdEncoding.EncodeToString(b), nil
		}

		if s, ok := v.(string); ok {
			return base64.StdEncoding.EncodeToString([]byte(s)), nil
		}
	case KindJSON:
		if s, ok := v.(string); ok {
			return s, nil
		}
	case KindUnknown, KindInt, KindFloat, KindString, KindTimestamp, KindDate, KindTime, KindDatetime,
		KindGeography, KindStruct, KindArray:
	}

	return scalarStored(k, v)
}
