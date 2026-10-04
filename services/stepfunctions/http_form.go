package stepfunctions

import (
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const (
	arrayIndices  = "INDICES"
	arrayRepeat   = "REPEAT"
	arrayCommas   = "COMMAS"
	arrayBrackets = "BRACKETS"
)

type formPair struct {
	key, value string
	rawValue   bool
}

// encodeForm renders body as application/x-www-form-urlencoded with nested
// keys as a[b] and arrays per the ArrayFormat option.
func encodeForm(body map[string]any, arrayFormat string) string {
	var pairs []formPair

	flattenForm("", body, arrayFormat, &pairs)

	parts := make([]string, len(pairs))

	for i, p := range pairs {
		val := formEscape(p.value)
		if p.rawValue {
			val = p.value
		}

		parts[i] = formEscape(p.key) + "=" + val
	}

	return strings.Join(parts, "&")
}

func formEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func flattenForm(prefix string, v any, format string, out *[]formPair) {
	switch t := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(t)) {
			child := k
			if prefix != "" {
				child = prefix + "[" + k + "]"
			}

			flattenForm(child, t[k], format, out)
		}
	case []any:
		flattenFormArray(prefix, t, format, out)
	default:
		*out = append(*out, formPair{key: prefix, value: formScalar(v)})
	}
}

func flattenFormArray(prefix string, items []any, format string, out *[]formPair) {
	if format == arrayCommas && allScalar(items) {
		escaped := make([]string, len(items))
		for i, it := range items {
			escaped[i] = formEscape(formScalar(it))
		}

		*out = append(*out, formPair{key: prefix, value: strings.Join(escaped, ","), rawValue: true})

		return
	}

	for i, it := range items {
		key := prefix

		switch format {
		case arrayRepeat:
		case arrayBrackets:
			key = prefix + "[]"
		default:
			key = prefix + "[" + strconv.Itoa(i) + "]"
		}

		flattenForm(key, it, format, out)
	}
}

func allScalar(items []any) bool {
	for _, it := range items {
		switch it.(type) {
		case map[string]any, []any:
			return false
		}
	}

	return true
}

func formScalar(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return ""
	}
}
