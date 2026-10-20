package bqsql

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	uuidVersionByte = 6
	uuidVariantByte = 8
	uuidVersionMask = 0x0f
	uuidVersion4    = 0x40
	uuidVariantMask = 0x3f
	uuidVariant     = 0x80
)

func udfUUID([]driver.Value) (driver.Value, error) {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		return nil, errBQ("random source failed")
	}

	b[uuidVersionByte] = b[uuidVersionByte]&uuidVersionMask | uuidVersion4
	b[uuidVariantByte] = b[uuidVariantByte]&uuidVariantMask | uuidVariant

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// udfStringOf implements STRING(timestamp[, zone]).
func udfStringOf(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	t, k, err := parseAny(a[0])
	if err != nil {
		return nil, err
	}

	if k == KindTimestamp {
		return t.Format("2006-01-02 15:04:05.999999") + "+00", nil
	}

	return toStr(a[0]), nil
}

func udfParseJSON(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	s := toStr(a[0])
	if !json.Valid([]byte(s)) {
		return nil, errBQ("syntax error while parsing value in JSON")
	}

	return s, nil
}

func udfJSONValue(scalarOnly bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		path := "$"
		if len(a) > 1 {
			path = toStr(a[1])
		}

		var doc any
		if err := json.Unmarshal([]byte(toStr(a[0])), &doc); err != nil {
			return nil, errBQ("Invalid JSON: %v", err)
		}

		node, ok, err := walkJSONPath(doc, path)
		if err != nil {
			return nil, err
		}

		if !ok || node == nil {
			return nil, nil
		}

		return jsonNodeText(node, scalarOnly), nil
	}
}

// jsonNodeText renders a JSON node; JSON_VALUE yields NULL for objects and arrays and unquotes strings.
func jsonNodeText(node any, scalarOnly bool) driver.Value {
	if scalarOnly {
		switch t := node.(type) {
		case map[string]any, []any:
			return nil
		case string:
			return t
		}
	}

	b, _ := json.Marshal(node)

	return string(b)
}

func walkJSONPath(doc any, path string) (any, bool, error) {
	if !strings.HasPrefix(path, "$") {
		return nil, false, errBQ("JSONPath must start with $")
	}

	rest := path[1:]
	cur := doc

	for rest != "" {
		var (
			key string
			idx = -1
		)

		switch {
		case strings.HasPrefix(rest, "."):
			end := strings.IndexAny(rest[1:], ".[")
			if end < 0 {
				end = len(rest) - 1
			}

			key, rest = rest[1:1+end], rest[1+end:]
		case strings.HasPrefix(rest, "['"):
			end := strings.Index(rest, "']")
			if end < 0 {
				return nil, false, errBQ("Invalid JSONPath: %s", path)
			}

			key, rest = rest[2:end], rest[end+2:]
		case strings.HasPrefix(rest, "["):
			end := strings.Index(rest, "]")
			if end < 0 {
				return nil, false, errBQ("Invalid JSONPath: %s", path)
			}

			n, err := strconv.Atoi(rest[1:end])
			if err != nil {
				return nil, false, errBQ("Invalid JSONPath: %s", path)
			}

			idx, rest = n, rest[end+1:]
		default:
			return nil, false, errBQ("Invalid JSONPath: %s", path)
		}

		var ok bool
		if cur, ok = stepJSON(cur, key, idx); !ok {
			return nil, false, nil
		}
	}

	return cur, true, nil
}

func stepJSON(cur any, key string, idx int) (any, bool) {
	if idx >= 0 {
		arr, ok := cur.([]any)
		if !ok || idx >= len(arr) {
			return nil, false
		}

		return arr[idx], true
	}

	obj, ok := cur.(map[string]any)
	if !ok {
		return nil, false
	}

	v, ok := obj[key]

	return v, ok
}

// udfFormat implements FORMAT with %s %d %i %f %g %e %t %T and %%.
func udfFormat(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	f := toStr(a[0])
	args := a[1:]

	var sb strings.Builder

	for i := 0; i < len(f); i++ {
		if f[i] != '%' {
			sb.WriteByte(f[i])

			continue
		}

		j := i + 1
		for j < len(f) && strings.IndexByte("-+ #0123456789.", f[j]) >= 0 {
			j++
		}

		if j >= len(f) {
			return nil, errBQ("Invalid format string")
		}

		spec := f[j]
		flags := f[i+1 : j]
		i = j

		if spec == '%' {
			sb.WriteByte('%')

			continue
		}

		if len(args) == 0 {
			return nil, errBQ("Too few arguments to FORMAT")
		}

		out, err := formatOne(spec, flags, args[0])
		if err != nil {
			return nil, err
		}

		sb.WriteString(out)

		args = args[1:]
	}

	return sb.String(), nil
}

func formatOne(spec byte, flags string, v driver.Value) (string, error) {
	if v == nil {
		if spec == 'T' || spec == 't' {
			return litNull, nil
		}

		return litNull, nil
	}

	switch spec {
	case 's', 't':
		return fmt.Sprintf("%"+flags+"s", toStr(v)), nil
	case 'T':
		if s, ok := v.(string); ok {
			return "\"" + s + "\"", nil
		}

		return toStr(v), nil
	case 'd', 'i':
		n, err := toInt(v)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%"+flags+"d", n), nil
	case 'f', 'g', 'e', 'E', 'G':
		x, err := toFloat(v)
		if err != nil {
			return "", err
		}

		if math.IsNaN(x) || math.IsInf(x, 0) {
			return FormatFloat(x), nil
		}

		return fmt.Sprintf("%"+flags+string(spec), x), nil
	case 'x', 'X', 'o':
		n, err := toInt(v)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%"+flags+string(spec), n), nil
	}

	return "", errBQ("Invalid format specifier %%%c", spec)
}
