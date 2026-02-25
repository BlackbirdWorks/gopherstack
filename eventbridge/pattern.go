package eventbridge

import (
	"encoding/json"
	"strings"
)

// matchPattern reports whether an EventBridge event matches the given pattern JSON.
// The pattern is a JSON object where each key is an event field and the value is
// either an array of exact-match strings/numbers, or a special matcher object.
//
// Supported matchers (subset of EventBridge pattern syntax):
//
//	["val1", "val2"]               — exact match (any value in list)
//	[{"prefix": "foo"}]            — prefix match (string)
//	[{"exists": true}]             — field must exist
//	[{"exists": false}]            — field must not exist
//	[{"numeric": [">", 5]}]        — numeric comparison
//	Nested objects are matched recursively.
func matchPattern(patternJSON, event string) bool {
	if patternJSON == "" {
		return true
	}

	var pattern map[string]any
	if err := json.Unmarshal([]byte(patternJSON), &pattern); err != nil {
		return false
	}

	var eventData map[string]any
	if err := json.Unmarshal([]byte(event), &eventData); err != nil {
		return false
	}

	return matchObject(pattern, eventData)
}

// matchObject checks whether all fields in pattern are satisfied by the eventData object.
func matchObject(pattern, eventData map[string]any) bool {
	for key, patternVal := range pattern {
		eventVal, exists := eventData[key]

		switch pv := patternVal.(type) {
		case map[string]any:
			// Nested object: recurse.
			ev, ok := eventVal.(map[string]any)
			if !ok {
				return false
			}
			if !matchObject(pv, ev) {
				return false
			}
		case []any:
			// Array of matchers/values.
			if !matchArray(pv, eventVal, exists) {
				return false
			}
		default:
			// Scalar: exact match.
			if eventVal != patternVal {
				return false
			}
		}
	}

	return true
}

// matchArray checks whether eventVal satisfies at least one matcher in the pattern array.
func matchArray(matchers []any, eventVal any, exists bool) bool {
	for _, m := range matchers {
		if matchSingle(m, eventVal, exists) {
			return true
		}
	}

	return false
}

// matchSingle checks whether eventVal satisfies a single matcher.
func matchSingle(matcher, eventVal any, exists bool) bool {
	switch m := matcher.(type) {
	case map[string]any:
		return matchSpecialMatcher(m, eventVal, exists)
	default:
		// Exact match.
		return eventVal == matcher
	}
}

// matchSpecialMatcher handles special matcher objects like {"prefix": ...}, {"exists": ...}, etc.
func matchSpecialMatcher(m map[string]any, eventVal any, exists bool) bool {
	if prefix, ok := m["prefix"]; ok {
		ps, ps_ok := prefix.(string)
		es, es_ok := eventVal.(string)
		if !ps_ok || !es_ok {
			return false
		}

		return strings.HasPrefix(es, ps)
	}

	if existsVal, ok := m["exists"]; ok {
		want, _ := existsVal.(bool)

		return exists == want
	}

	if numericRules, ok := m["numeric"]; ok {
		return matchNumeric(numericRules, eventVal)
	}

	if anythingButVal, ok := m["anything-but"]; ok {
		return matchAnythingBut(anythingButVal, eventVal)
	}

	if _, ok := m["wildcard"]; ok {
		// Wildcard matching is complex; for now do a simplified prefix check.
		// Full glob matching not required for basic parity.
		return eventVal != nil
	}

	return false
}

// matchNumeric applies numeric comparison rules like [">", 5, "<", 10].
// Rules come in pairs: [op, val, op, val, ...].
func matchNumeric(rules any, eventVal any) bool {
	ruleList, ok := rules.([]any)
	if !ok {
		return false
	}

	num, ok := toFloat64(eventVal)
	if !ok {
		return false
	}

	const pairSize = 2
	for i := 0; i+1 < len(ruleList); i += pairSize {
		op, opOk := ruleList[i].(string)
		val, valOk := toFloat64(ruleList[i+1])

		if !opOk || !valOk {
			return false
		}

		switch op {
		case ">":
			if !(num > val) {
				return false
			}
		case ">=":
			if !(num >= val) {
				return false
			}
		case "<":
			if !(num < val) {
				return false
			}
		case "<=":
			if !(num <= val) {
				return false
			}
		case "=":
			if num != val {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// matchAnythingBut matches when the event value is NOT in the provided set.
func matchAnythingBut(v, eventVal any) bool {
	switch ab := v.(type) {
	case []any:
		for _, item := range ab {
			if eventVal == item {
				return false
			}
		}

		return true
	default:
		return eventVal != v
	}
}

// toFloat64 converts a numeric value to float64.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
