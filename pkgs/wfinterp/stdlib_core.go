package wfinterp

import (
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	arity2 = 2
	arity3 = 3

	paramArg       = "arg"
	paramVal       = "val"
	paramData      = "data"
	paramSource    = "source"
	paramException = "exception"
	paramDefault   = "default"
	paramRegexp    = "regexp"
	paramBody      = "body"
	paramCondition = "condition"
	paramMap       = "map"
)

func argString(fn, name string, v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", typeErr("%s: %s must be a string, not %s", fn, name, typeName(v))
	}

	return s, nil
}

func argInt(fn, name string, v any) (int64, error) {
	i, ok := v.(int64)
	if !ok {
		return 0, typeErr("%s: %s must be an integer, not %s", fn, name, typeName(v))
	}

	return i, nil
}

func argList(fn, name string, v any) ([]any, error) {
	l, ok := v.([]any)
	if !ok {
		return nil, typeErr("%s: %s must be a list, not %s", fn, name, typeName(v))
	}

	return l, nil
}

func argMap(fn, name string, v any) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, typeErr("%s: %s must be a map, not %s", fn, name, typeName(v))
	}

	return m, nil
}

func argBytes(fn, name string, v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, typeErr("%s: %s must be bytes, not %s", fn, name, typeName(v))
	}

	return b, nil
}

func argNumber(fn, name string, v any) (float64, error) {
	f, ok := toFloat(v)
	if !ok {
		return 0, typeErr("%s: %s must be a number, not %s", fn, name, typeName(v))
	}

	return f, nil
}

func (e *Engine) addCore() {
	e.reg("len", []string{paramArg}, 1, fnLen)
	e.reg("keys", []string{paramMap}, 1, fnKeys)
	e.reg("int", []string{paramArg}, 1, fnInt)
	e.reg("double", []string{paramArg}, 1, fnDouble)
	e.reg("string", []string{paramArg}, 1, fnString)
	e.reg("get_type", []string{paramArg}, 1, func(_ *callCtx, a []any) (any, error) { return typeName(a[0]), nil })
	e.reg(
		paramDefault,
		[]string{paramVal, "default_val"},
		arity2,
		func(_ *callCtx, a []any) (any, error) {
			if a[0] != nil {
				return a[0], nil
			}

			return a[1], nil
		},
	)
	e.reg(
		"if",
		[]string{paramCondition, "if_true", "if_false"},
		arity3,
		func(_ *callCtx, a []any) (any, error) {
			b, ok := a[0].(bool)
			if !ok {
				return nil, typeErr("if: condition must be a boolean, not %s", typeName(a[0]))
			}

			if b {
				return a[1], nil
			}

			return a[2], nil
		},
	)
}

func fnLen(_ *callCtx, a []any) (any, error) {
	switch x := a[0].(type) {
	case string:
		return int64(utf8.RuneCountInString(x)), nil
	case []any:
		return int64(len(x)), nil
	case map[string]any:
		return int64(len(x)), nil
	case []byte:
		return int64(len(x)), nil
	default:
		return nil, typeErr("len: cannot take the length of a %s", typeName(a[0]))
	}
}

func fnKeys(_ *callCtx, a []any) (any, error) {
	m, err := argMap("keys", paramMap, a[0])
	if err != nil {
		return nil, err
	}

	keys := sortedKeys(m)
	out := make([]any, len(keys))

	for i, k := range keys {
		out[i] = k
	}

	return out, nil
}

func fnInt(_ *callCtx, a []any) (any, error) {
	switch x := a[0].(type) {
	case int64:
		return x, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, valueErr("int: cannot convert %v", x)
		}

		return int64(x), nil
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
			return i, nil
		}

		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return nil, valueErr("int: cannot convert %q", x)
		}

		return int64(f), nil
	default:
		return nil, typeErr("int: cannot convert a %s", typeName(a[0]))
	}
}

func fnDouble(_ *callCtx, a []any) (any, error) {
	switch x := a[0].(type) {
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return nil, valueErr("double: cannot convert %q", x)
		}

		return f, nil
	default:
		return nil, typeErr("double: cannot convert a %s", typeName(a[0]))
	}
}

func fnString(_ *callCtx, a []any) (any, error) {
	switch a[0].(type) {
	case string, int64, float64, bool:
		return formatValue(a[0]), nil
	default:
		return nil, typeErr("string: cannot convert a %s", typeName(a[0]))
	}
}

func (e *Engine) addData() {
	e.reg("list.concat", []string{"objs", paramVal}, arity2, func(_ *callCtx, a []any) (any, error) {
		l, err := argList("list.concat", "objs", a[0])
		if err != nil {
			return nil, err
		}

		return append(append([]any{}, l...), deepCopy(a[1])), nil
	})
	e.reg("list.prepend", []string{"objs", paramVal}, arity2, func(_ *callCtx, a []any) (any, error) {
		l, err := argList("list.prepend", "objs", a[0])
		if err != nil {
			return nil, err
		}

		return append([]any{deepCopy(a[1])}, l...), nil
	})
	e.reg("map.get", []string{paramMap, "keys"}, arity2, fnMapGet)
	e.reg("map.delete", []string{paramMap, "key"}, arity2, fnMapDelete)
	e.reg("map.merge", []string{"first", "second"}, arity2, func(_ *callCtx, a []any) (any, error) {
		return mergeMaps("map.merge", a, false)
	})
	e.reg(
		"map.merge_nested",
		[]string{"first", "second"},
		arity2,
		func(_ *callCtx, a []any) (any, error) {
			return mergeMaps("map.merge_nested", a, true)
		},
	)
	e.reg("json.decode", []string{paramData}, 1, fnJSONDecode)
	e.reg("json.encode", []string{paramData}, 1, func(_ *callCtx, a []any) (any, error) {
		raw, err := ToJSON(a[0])
		if err != nil {
			return nil, valueErr("json.encode: %v", err)
		}

		return raw, nil
	})
	e.reg("json.encode_to_string", []string{paramData}, 1, func(_ *callCtx, a []any) (any, error) {
		raw, err := ToJSON(a[0])
		if err != nil {
			return nil, valueErr("json.encode_to_string: %v", err)
		}

		return string(raw), nil
	})
}

func fnMapGet(_ *callCtx, a []any) (any, error) {
	cur := a[0]

	var keys []any

	switch k := a[1].(type) {
	case string:
		keys = []any{k}
	case []any:
		keys = k
	default:
		return nil, typeErr("map.get: keys must be a string or a list of strings, not %s", typeName(a[1]))
	}

	for _, k := range keys {
		ks, ok := k.(string)
		if !ok {
			return nil, typeErr("map.get: keys must be strings, not %s", typeName(k))
		}

		m, isMap := cur.(map[string]any)
		if !isMap {
			return nil, nil //nolint:nilnil // null is the documented miss value
		}

		cur = m[ks]
	}

	return cur, nil
}

func fnMapDelete(_ *callCtx, a []any) (any, error) {
	m, err := argMap("map.delete", paramMap, a[0])
	if err != nil {
		return nil, err
	}

	k, err := argString("map.delete", "key", a[1])
	if err != nil {
		return nil, err
	}

	out := copyMap(m)
	delete(out, k)

	return out, nil
}

func mergeMaps(fn string, a []any, nested bool) (any, error) {
	first, err := argMap(fn, "first", a[0])
	if err != nil {
		return nil, err
	}

	second, err := argMap(fn, "second", a[1])
	if err != nil {
		return nil, err
	}

	return mergeInto(copyMap(first), second, nested), nil
}

func copyMap(m map[string]any) map[string]any {
	out, _ := deepCopy(m).(map[string]any)

	return out
}

func mergeInto(dst, src map[string]any, nested bool) map[string]any {
	for k, v := range src {
		sm, srcMap := v.(map[string]any)
		dm, dstMap := dst[k].(map[string]any)

		if nested && srcMap && dstMap {
			dst[k] = mergeInto(dm, sm, true)

			continue
		}

		dst[k] = deepCopy(v)
	}

	return dst
}

func fnJSONDecode(_ *callCtx, a []any) (any, error) {
	var raw []byte

	switch x := a[0].(type) {
	case string:
		raw = []byte(x)
	case []byte:
		raw = x
	default:
		return nil, typeErr("json.decode: data must be bytes or a string, not %s", typeName(a[0]))
	}

	v, err := ParseJSON(raw)
	if err != nil {
		return nil, valueErr("json.decode: %v", err)
	}

	return v, nil
}

func (e *Engine) addMisc() {
	e.reg("math.abs", []string{"x"}, 1, func(_ *callCtx, a []any) (any, error) {
		if i, ok := a[0].(int64); ok {
			if i < 0 {
				return -i, nil
			}

			return i, nil
		}

		f, err := argNumber("math.abs", "x", a[0])
		if err != nil {
			return nil, err
		}

		return math.Abs(f), nil
	})
	e.reg("math.floor", []string{"x"}, 1, func(_ *callCtx, a []any) (any, error) {
		if i, ok := a[0].(int64); ok {
			return i, nil
		}

		f, err := argNumber("math.floor", "x", a[0])
		if err != nil {
			return nil, err
		}

		return int64(math.Floor(f)), nil
	})
	e.reg(
		"math.max",
		[]string{"x", "y"},
		arity2,
		func(_ *callCtx, a []any) (any, error) { return pickNumber("math.max", a, true) },
	)
	e.reg(
		"math.min",
		[]string{"x", "y"},
		arity2,
		func(_ *callCtx, a []any) (any, error) { return pickNumber("math.min", a, false) },
	)
	e.reg("retry.always", []string{paramException}, 1, func(*callCtx, []any) (any, error) { return true, nil })
	e.reg("retry.never", []string{paramException}, 1, func(*callCtx, []any) (any, error) { return false, nil })
	e.reg("http.default_retry_predicate", []string{paramException}, 1, func(_ *callCtx, a []any) (any, error) {
		return retryableDefault(a[0], false), nil
	})
	e.reg(
		"http.default_retry_predicate_non_idempotent",
		[]string{paramException},
		1,
		func(_ *callCtx, a []any) (any, error) {
			return retryableDefault(a[0], true), nil
		},
	)
	e.reg("sys.get_env", []string{"name", paramDefault}, 1, fnGetEnv)
}

func pickNumber(fn string, a []any, maxWanted bool) (any, error) {
	x, err := argNumber(fn, "x", a[0])
	if err != nil {
		return nil, err
	}

	y, err := argNumber(fn, "y", a[1])
	if err != nil {
		return nil, err
	}

	_, xi := a[0].(int64)
	_, yi := a[1].(int64)

	pick := y
	if (maxWanted && x >= y) || (!maxWanted && x <= y) {
		pick = x
	}

	if xi && yi {
		return int64(pick), nil
	}

	return pick, nil
}

func retryableDefault(e any, nonIdempotent bool) bool {
	if nonIdempotent {
		return hasTag(e, TagConnectionFailed) ||
			httpCodeIn(e, http.StatusTooManyRequests, http.StatusServiceUnavailable)
	}

	return hasTag(e, TagConnectionError) || hasTag(e, TagConnectionFailed) || hasTag(e, TagTimeoutError) ||
		httpCodeIn(e, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable,
			http.StatusGatewayTimeout)
}

func httpCodeIn(e any, codes ...int) bool {
	m, ok := e.(map[string]any)
	if !ok || !hasTag(e, TagHTTPError) {
		return false
	}

	code, _ := m["code"].(int64)

	return slices.Contains(codes, int(code))
}

func fnGetEnv(c *callCtx, a []any) (any, error) {
	name, err := argString("sys.get_env", "name", a[0])
	if err != nil {
		return nil, err
	}

	if a[1] != nil {
		if _, err = argString("sys.get_env", paramDefault, a[1]); err != nil {
			return nil, err
		}
	}

	if h := c.host(); h.Env != nil {
		if v, ok := h.Env(name); ok {
			return v, nil
		}
	}

	return a[1], nil
}
