package ocievents

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// parseCondition decodes a rule condition: a JSON object whose leaves are strings, numbers, booleans or
// non-empty arrays of those (https://docs.oracle.com/en-us/iaas/Content/Events/Concepts/filterevents.htm).
func parseCondition(s string) (map[string]any, *APIError) {
	if strings.TrimSpace(s) == "" {
		return nil, errMissing("condition is required")
	}

	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errParam("condition is not valid JSON: " + err.Error())
	}

	if dec.More() {
		return nil, errParam("condition must be a single JSON object")
	}

	obj, ok := v.(map[string]any)
	if !ok {
		return nil, errParam("condition must be a JSON object")
	}

	if err := validateNode("condition", obj); err != nil {
		return nil, err
	}

	return obj, nil
}

func validateNode(path string, v any) *APIError {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if strings.Contains(k, "*") {
				return errParam(
					fmt.Sprintf("%s: wildcards are allowed in values only, not in the field name %q", path, k),
				)
			}

			if err := validateNode(path+"."+k, child); err != nil {
				return err
			}
		}
	case []any:
		if len(t) == 0 {
			return errParam(path + ": an array must list at least one value")
		}

		for _, el := range t {
			if !isScalar(el) {
				return errParam(path + ": array elements must be strings, numbers or booleans")
			}
		}
	case string, json.Number, bool:
	default:
		return errParam(path + ": null is not a valid filter value")
	}

	return nil
}

func isScalar(v any) bool {
	switch v.(type) {
	case string, json.Number, float64, bool:
		return true
	default:
		return false
	}
}

func scalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return ""
	}
}

// matchCondition reports whether every field of cond appears in event with a matching value. An empty
// condition matches everything.
func matchCondition(cond, event map[string]any) bool {
	for k, c := range cond {
		e, ok := event[k]
		if !ok || !matchValue(c, e) {
			return false
		}
	}

	return true
}

func matchValue(c, e any) bool {
	switch ev := e.(type) {
	case nil:
		return false
	case []any:
		return slicesAny(ev, func(el any) bool { return matchValue(c, el) })
	case map[string]any:
		cm, ok := c.(map[string]any)

		return ok && matchCondition(cm, ev)
	}

	if cs, ok := c.([]any); ok {
		return slicesAny(cs, func(el any) bool { return matchValue(el, e) })
	}

	if !isScalar(c) {
		return false
	}

	return wildcardMatch(scalarString(c), scalarString(e))
}

func slicesAny(list []any, fn func(any) bool) bool {
	return slices.ContainsFunc(list, fn)
}

// wildcardMatch matches s against pattern where * stands for any run of characters (including none).
func wildcardMatch(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}

	if !strings.HasPrefix(s, parts[0]) {
		return false
	}

	s = s[len(parts[0]):]
	last := parts[len(parts)-1]

	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}

		s = s[i+len(mid):]
	}

	return strings.HasSuffix(s, last)
}
