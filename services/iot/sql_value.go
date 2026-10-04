package iot

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
)

// sqlUndefined is the SQL Undefined value: omitted from JSON output.
type sqlUndefined struct{}

// sqlRaw is a payload that is not valid JSON, kept as bytes.
type sqlRaw []byte

// sqlObject is a JSON object that keeps key order.
type sqlObject struct {
	vals map[string]any
	keys []string
}

func newSQLObject() *sqlObject { return &sqlObject{vals: map[string]any{}} }

func (o *sqlObject) set(k string, v any) {
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k)
	}

	o.vals[k] = v
}

func (o *sqlObject) get(k string) (any, bool) {
	v, ok := o.vals[k]

	return v, ok
}

func isUndef(v any) bool {
	_, ok := v.(sqlUndefined)

	return ok
}

const (
	maxExactFloatInt = 1 << 53
	jsonNull         = "null"
)

// sqlNumber converts JSON/SQL number text to Int or Decimal; v2016 folds whole decimals into Int.
func sqlNumber(text string, v2016 bool) any {
	if !strings.ContainsAny(text, ".eE") {
		if i, err := strconv.ParseInt(text, 10, 64); err == nil {
			return i
		}
	}

	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return sqlUndefined{}
	}

	return normNumber(f, v2016)
}

func normNumber(f float64, v2016 bool) any {
	if v2016 && f == math.Trunc(f) && math.Abs(f) < maxExactFloatInt {
		return int64(f)
	}

	return f
}

var errBadJSON = errors.New("invalid json")

// decodeJSONValue parses a complete JSON document.
func decodeJSONValue(data []byte, v2016 bool) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	v, err := decodeToken(dec, v2016)
	if err != nil {
		return nil, err
	}

	if _, err = dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errBadJSON
	}

	return v, nil
}

func decodeToken(dec *json.Decoder, v2016 bool) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, errBadJSON
	}

	switch t := tok.(type) {
	case json.Delim:
		if t == '{' {
			return decodeObject(dec, v2016)
		}

		return decodeArray(dec, v2016)
	case json.Number:
		return sqlNumber(t.String(), v2016), nil
	default:
		return t, nil
	}
}

func decodeObject(dec *json.Decoder, v2016 bool) (any, error) {
	obj := newSQLObject()

	for dec.More() {
		kt, err := dec.Token()

		key, ok := kt.(string)
		if err != nil || !ok {
			return nil, errBadJSON
		}

		v, verr := decodeToken(dec, v2016)
		if verr != nil {
			return nil, verr
		}

		obj.set(key, v)
	}

	if _, err := dec.Token(); err != nil {
		return nil, errBadJSON
	}

	return obj, nil
}

func decodeArray(dec *json.Decoder, v2016 bool) (any, error) {
	arr := []any{}

	for dec.More() {
		v, err := decodeToken(dec, v2016)
		if err != nil {
			return nil, err
		}

		arr = append(arr, v)
	}

	if _, err := dec.Token(); err != nil {
		return nil, errBadJSON
	}

	return arr, nil
}

// writeJSONValue serialises v; Undefined object members are omitted.
func writeJSONValue(buf *bytes.Buffer, v any) {
	switch t := v.(type) {
	case nil:
		buf.WriteString(jsonNull)
	case bool, string:
		b, _ := json.Marshal(t)
		buf.Write(b)
	case int64:
		buf.WriteString(strconv.FormatInt(t, 10))
	case float64:
		buf.WriteString(formatDecimal(t, true))
	case []any:
		writeJSONArray(buf, t)
	case *sqlObject:
		writeJSONObject(buf, t)
	case sqlRaw:
		b, _ := json.Marshal(string(t))
		buf.Write(b)
	default:
		buf.WriteString(jsonNull)
	}
}

func writeJSONArray(buf *bytes.Buffer, arr []any) {
	buf.WriteByte('[')

	for i, e := range arr {
		if i > 0 {
			buf.WriteByte(',')
		}

		writeJSONValue(buf, e)
	}

	buf.WriteByte(']')
}

func writeJSONObject(buf *bytes.Buffer, o *sqlObject) {
	buf.WriteByte('{')

	first := true

	for _, k := range o.keys {
		v := o.vals[k]
		if isUndef(v) {
			continue
		}

		if !first {
			buf.WriteByte(',')
		}

		first = false

		kb, _ := json.Marshal(k)
		buf.Write(kb)
		buf.WriteByte(':')
		writeJSONValue(buf, v)
	}

	buf.WriteByte('}')
}

// formatDecimal renders a Decimal; withPoint keeps ".0" on whole values.
func formatDecimal(f float64, withPoint bool) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return jsonNull
	}

	abs := math.Abs(f)

	var s string
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		s = strconv.FormatFloat(f, 'e', -1, 64)
	} else {
		s = strconv.FormatFloat(f, 'f', -1, 64)
	}

	if withPoint && !strings.ContainsAny(s, ".eE") && s != jsonNull {
		s += ".0"
	}

	return s
}

func jsonString(v any) string {
	var buf bytes.Buffer

	writeJSONValue(&buf, v)

	return buf.String()
}

// validDecimalString reports whether s matches ^-?\d+(\.\d+)?(E-?\d+)?$ (case-insensitive E).
func validDecimalString(s string) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}

	i, ok := scanDigits(s, i)
	if !ok {
		return false
	}

	if i < len(s) && s[i] == '.' {
		if i, ok = scanDigits(s, i+1); !ok {
			return false
		}
	}

	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && s[i] == '-' {
			i++
		}

		if i, ok = scanDigits(s, i); !ok {
			return false
		}
	}

	return i == len(s)
}

func scanDigits(s string, i int) (int, bool) {
	start := i

	for i < len(s) && isDigit(s[i]) {
		i++
	}

	return i, i > start
}

// toDecimal applies the standard conversion to Decimal.
func toDecimal(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	case string:
		if !validDecimalString(t) {
			return 0, false
		}

		f, err := strconv.ParseFloat(t, 64)

		return f, err == nil
	}

	return 0, false
}

// toIntConv applies the standard conversion to Int (Decimal rounds, String truncates).
func toIntConv(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case float64:
		return floatToInt(math.Round(t))
	case string:
		f, ok := toDecimal(t)
		if !ok {
			return 0, false
		}

		return floatToInt(math.Trunc(f))
	}

	return 0, false
}

func floatToInt(f float64) (int64, bool) {
	if math.IsNaN(f) || math.Abs(f) >= math.MaxInt64 {
		return 0, false
	}

	return int64(f), true
}

func toBoolConv(v any) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		switch {
		case strings.EqualFold(t, "true"):
			return true, true
		case strings.EqualFold(t, "false"):
			return false, true
		}
	}

	return false, false
}

// toStringConv applies the standard conversion to String; Null and Undefined do not convert.
func toStringConv(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case int64:
		return strconv.FormatInt(t, 10), true
	case float64:
		return formatDecimal(t, false), true
	case bool:
		return strconv.FormatBool(t), true
	case []any, *sqlObject:
		return jsonString(t), true
	case sqlRaw:
		return string(t), true
	}

	return "", false
}
