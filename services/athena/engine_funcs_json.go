package athena

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// arrays and maps live in SQLite as JSON text.

func parseJSONArg(v driver.Value) (any, bool) {
	s, ok := asString(v)
	if !ok {
		return nil, false
	}

	var out any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, false
	}

	return out, true
}

func jsonText(v any) driver.Value {
	b, err := json.Marshal(v)
	if err != nil {
		return sqlNull()
	}

	return string(b)
}

// sqlValue maps a decoded JSON value to a SQLite value; containers stay JSON text.
func sqlValue(v any) driver.Value {
	switch t := v.(type) {
	case nil:
		return sqlNull()
	case string:
		return t
	case bool:
		if t {
			return int64(1)
		}

		return int64(0)
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return int64(t)
		}

		return t
	default:
		return jsonText(t)
	}
}

func asArray(v driver.Value) ([]any, bool) {
	p, ok := parseJSONArg(v)
	if !ok {
		return nil, false
	}

	a, ok := p.([]any)

	return a, ok
}

func jsonPathValue(root any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "$") {
		return nil, false
	}

	cur := root

	for rest := path[1:]; rest != ""; {
		var ok bool

		cur, rest, ok = stepJSONPath(cur, rest)
		if !ok {
			return nil, false
		}
	}

	return cur, true
}

func stepJSONPath(cur any, rest string) (any, string, bool) {
	switch {
	case strings.HasPrefix(rest, "["):
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			return nil, "", false
		}

		key := strings.Trim(rest[1:end], `"'`)

		if m, isMap := cur.(map[string]any); isMap && (rest[1] == '"' || rest[1] == '\'') {
			v, ok := m[key]

			return v, rest[end+1:], ok
		}

		arr, isArr := cur.([]any)
		idx, err := strconv.Atoi(key)

		if !isArr || err != nil || idx < 0 || idx >= len(arr) {
			return nil, "", false
		}

		return arr[idx], rest[end+1:], true
	case strings.HasPrefix(rest, "."):
		name := rest[1:]
		next := strings.IndexAny(name, ".[")

		if next >= 0 {
			name, rest = name[:next], name[next:]
		} else {
			rest = ""
		}

		m, ok := cur.(map[string]any)
		if !ok {
			return nil, "", false
		}

		v, found := m[name]

		return v, rest, found
	default:
		return nil, "", false
	}
}

func udfJSONExtractScalar(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	p, pok := asString(args[1])

	if !ok || !pok {
		return sqlNull(), nil
	}

	v, found := jsonPathValue(root, p)
	if !found {
		return sqlNull(), nil
	}

	switch t := v.(type) {
	case map[string]any, []any:
		return sqlNull(), nil
	case string:
		return t, nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	default:
		return fmt.Sprint(t), nil
	}
}

func udfJSONExtract(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	p, pok := asString(args[1])

	if !ok || !pok {
		return sqlNull(), nil
	}

	v, found := jsonPathValue(root, p)
	if !found {
		return sqlNull(), nil
	}

	return jsonText(v), nil
}

func udfJSONParse(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if _, ok := parseJSONArg(args[0]); !ok {
		return nil, fmt.Errorf("%w: Cannot convert value to JSON", errBadValue)
	}

	return args[0], nil
}

func udfJSONSize(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	p, pok := asString(args[1])

	if !ok || !pok {
		return sqlNull(), nil
	}

	v, found := jsonPathValue(root, p)
	if !found {
		return sqlNull(), nil
	}

	switch t := v.(type) {
	case []any:
		return int64(len(t)), nil
	case map[string]any:
		return int64(len(t)), nil
	default:
		return int64(0), nil
	}
}

func udfSplit(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, sok := asString(args[0])
	d, dok := asString(args[1])

	if !sok || !dok {
		return sqlNull(), nil
	}

	parts := strings.Split(s, d)
	if len(args) == argsThree {
		if n, ok := toInt(args[2]); ok && n > 0 && int(n) < len(parts) {
			parts = append(parts[:n-1], strings.Join(parts[n-1:], d))
		}
	}

	return jsonText(parts), nil
}

func udfSplitPart(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, sok := asString(args[0])
	d, dok := asString(args[1])
	n, nok := toInt(args[2])

	if !sok || !dok || !nok || n < 1 {
		return sqlNull(), nil
	}

	parts := strings.Split(s, d)
	if int(n) > len(parts) {
		return sqlNull(), nil
	}

	return parts[n-1], nil
}

func udfElementAt(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	switch c := root.(type) {
	case []any:
		i, iok := toInt(args[1])
		if !iok || i == 0 {
			return sqlNull(), nil
		}

		if i < 0 {
			i += int64(len(c)) + 1
		}

		if i < 1 || int(i) > len(c) {
			return sqlNull(), nil
		}

		return sqlValue(c[i-1]), nil
	case map[string]any:
		key, _ := asKey(args[1])

		return sqlValue(c[key]), nil
	default:
		return sqlNull(), nil
	}
}

func asKey(v driver.Value) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case int64:
		return strconv.FormatInt(t, 10), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
}

func udfCardinality(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	switch c := root.(type) {
	case []any:
		return int64(len(c)), nil
	case map[string]any:
		return int64(len(c)), nil
	default:
		return sqlNull(), nil
	}
}

func jsonEqual(elem any, v driver.Value) bool {
	return fmt.Sprint(sqlValue(elem)) == fmt.Sprint(v)
}

func udfContains(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	for _, e := range a {
		if jsonEqual(e, args[1]) {
			return int64(1), nil
		}
	}

	return int64(0), nil
}

func udfArrayPosition(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	for i, e := range a {
		if jsonEqual(e, args[1]) {
			return int64(i + 1), nil
		}
	}

	return int64(0), nil
}

func udfArrayJoin(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	d, dok := asString(args[1])

	if !ok || !dok {
		return sqlNull(), nil
	}

	parts := make([]string, 0, len(a))

	for _, e := range a {
		switch {
		case e != nil:
			parts = append(parts, fmt.Sprint(sqlValue(e)))
		case len(args) == argsThree:
			s, _ := asString(args[2])
			parts = append(parts, s)
		}
	}

	return strings.Join(parts, d), nil
}

func udfArrayDistinct(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	seen := map[string]bool{}
	out := make([]any, 0, len(a))

	for _, e := range a {
		k, _ := json.Marshal(e)
		if !seen[string(k)] {
			seen[string(k)] = true
			out = append(out, e)
		}
	}

	return jsonText(out), nil
}

func lessJSON(a, b any) bool {
	af, aok := a.(float64)
	bf, bok := b.(float64)

	if aok && bok {
		return af < bf
	}

	return fmt.Sprint(a) < fmt.Sprint(b)
}

func udfArraySort(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	out := slices.Clone(a)
	sort.SliceStable(out, func(i, j int) bool {
		switch {
		case out[i] == nil:
			return false
		case out[j] == nil:
			return true
		default:
			return lessJSON(out[i], out[j])
		}
	})

	return jsonText(out), nil
}

func arrayExtreme(args []driver.Value, less bool) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok || len(a) == 0 {
		return sqlNull(), nil
	}

	best := a[0]

	for _, e := range a[1:] {
		if e == nil || best == nil {
			return sqlNull(), nil
		}

		if lessJSON(e, best) == less {
			best = e
		}
	}

	return sqlValue(best), nil
}

func udfArrayMax(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	return arrayExtreme(args, false)
}

func udfArrayMin(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	return arrayExtreme(args, true)
}

func udfArrayRemove(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	out := make([]any, 0, len(a))

	for _, e := range a {
		if !jsonEqual(e, args[1]) {
			out = append(out, e)
		}
	}

	return jsonText(out), nil
}

func udfArrayConcat(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	out := []any{}

	for _, v := range args {
		a, ok := asArray(v)
		if !ok {
			return sqlNull(), nil
		}

		out = append(out, a...)
	}

	return jsonText(out), nil
}

func udfSlice(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	start, sok := toInt(args[1])
	length, lok := toInt(args[2])

	if !ok || !sok || !lok || start == 0 || length < 0 {
		return sqlNull(), nil
	}

	if start < 0 {
		start += int64(len(a)) + 1
	}

	if start < 1 || int(start) > len(a) {
		return jsonText([]any{}), nil
	}

	end := min(int(start-1+length), len(a))

	return jsonText(a[start-1 : end]), nil
}

func udfMapKeys(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	m, isMap := root.(map[string]any)

	if !ok || !isMap {
		return sqlNull(), nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return jsonText(keys), nil
}

func udfMapValues(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	root, ok := parseJSONArg(args[0])
	m, isMap := root.(map[string]any)

	if !ok || !isMap {
		return sqlNull(), nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	vals := make([]any, len(keys))
	for i, k := range keys {
		vals[i] = m[k]
	}

	return jsonText(vals), nil
}

func udfMapFromArrays(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	ks, kok := asArray(args[0])
	vs, vok := asArray(args[1])

	if !kok || !vok || len(ks) != len(vs) {
		return sqlNull(), nil
	}

	m := make(map[string]any, len(ks))
	for i, k := range ks {
		m[fmt.Sprint(k)] = vs[i]
	}

	return jsonText(m), nil
}
