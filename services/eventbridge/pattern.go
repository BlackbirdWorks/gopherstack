package eventbridge

import (
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
)

type compiledPattern struct {
	pattern               map[string]any
	sourceExactValues     []string
	detailTypeExactValues []string
}

func compilePattern(patternJSON string) (*compiledPattern, error) {
	if patternJSON == "" {
		return &compiledPattern{}, nil
	}

	var pattern map[string]any
	if err := json.Unmarshal([]byte(patternJSON), &pattern); err != nil {
		return nil, err
	}

	if err := validatePatternObject(pattern); err != nil {
		return nil, err
	}

	return &compiledPattern{
		pattern:               pattern,
		sourceExactValues:     exactStringMatcherValues(pattern, "source"),
		detailTypeExactValues: exactStringMatcherValues(pattern, "detail-type"),
	}, nil
}

// knownMatchers is the set of valid EventBridge matcher object keys.
var knownMatchers = map[string]bool{
	"prefix":             true,
	"suffix":             true,
	"exists":             true,
	"numeric":            true,
	"anything-but":       true,
	"cidr":               true,
	"wildcard":           true,
	"equals-ignore-case": true,
}

// validatePatternObject validates the structure of an EventBridge pattern object.
// Each field value must be an array of matchers or a nested object.
func validatePatternObject(pattern map[string]any) error {
	for key, val := range pattern {
		if key == "$or" {
			alts, ok := val.([]any)
			if !ok {
				return fmt.Errorf("invalid event pattern: $or must be an array")
			}
			for _, alt := range alts {
				subPat, ok := alt.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid event pattern: $or elements must be objects")
				}
				if err := validatePatternObject(subPat); err != nil {
					return err
				}
			}
			continue
		}
		switch v := val.(type) {
		case map[string]any:
			// Nested object — recurse.
			if err := validatePatternObject(v); err != nil {
				return err
			}
		case []any:
			if err := validateMatcherArray(key, v); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid event pattern: value for field %q must be an array or object, got scalar", key)
		}
	}
	return nil
}

// validateMatcherArray validates a pattern array for a single field.
func validateMatcherArray(field string, matchers []any) error {
	for _, m := range matchers {
		switch mv := m.(type) {
		case string, float64, bool, nil:
			// Exact-match primitives are always valid.
		case map[string]any:
			if err := validateMatcherObject(field, mv); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid event pattern: matcher for field %q has unsupported type", field)
		}
	}
	return nil
}

// validateMatcherObject validates a single matcher object (e.g., {"prefix": "foo"}).
func validateMatcherObject(field string, m map[string]any) error {
	for key := range m {
		if !knownMatchers[key] {
			return fmt.Errorf("invalid event pattern: unknown matcher %q for field %q", key, field)
		}
	}
	return nil
}

func exactStringMatcherValues(pattern map[string]any, key string) []string {
	v, ok := pattern[key]
	if !ok {
		return nil
	}

	values, ok := v.([]any)
	if !ok || len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	for _, value := range values {
		s, isString := value.(string)
		if !isString {
			return nil
		}
		out = append(out, s)
	}

	return out
}

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
//	[{"anything-but": ["v1","v2"]}]— negation
//	[{"cidr": "10.0.0.0/24"}]      — CIDR IP range match
//	[{"wildcard": "com.example.*"}]— wildcard string match (* and ?)
//	Nested objects are matched recursively.
//	If the event field value is an array, any element matching the pattern satisfies it.
func matchPattern(patternJSON, event string) bool {
	compiled, err := compilePattern(patternJSON)
	if err != nil {
		return false
	}

	return matchCompiledPattern(compiled, event)
}

func matchCompiledPattern(compiled *compiledPattern, event string) bool {
	if compiled == nil || len(compiled.pattern) == 0 {
		return true
	}

	var eventData map[string]any
	if err := json.Unmarshal([]byte(event), &eventData); err != nil {
		return false
	}

	return matchObject(compiled.pattern, eventData)
}

// matchObject checks whether all fields in pattern are satisfied by the eventData object.
func matchObject(pattern, eventData map[string]any) bool {
	for key, patternVal := range pattern {
		if !matchObjectField(key, patternVal, eventData) {
			return false
		}
	}

	return true
}

// matchObjectField checks a single pattern field against the event data.
func matchObjectField(key string, patternVal any, eventData map[string]any) bool {
	// Handle $or combinator: value is an array of pattern objects, any must match.
	if key == "$or" {
		return matchOrCombinator(patternVal, eventData)
	}

	eventVal, exists := eventData[key]

	switch pv := patternVal.(type) {
	case map[string]any:
		// Nested object: recurse.
		ev, ok := eventVal.(map[string]any)
		if !ok {
			return false
		}

		return matchObject(pv, ev)
	case []any:
		// Array of matchers/values.
		return matchArray(pv, eventVal, exists)
	default:
		// Scalar: exact match.
		return eventVal == patternVal
	}
}

// matchOrCombinator evaluates an $or combinator: the value must be an array of
// pattern objects and at least one of them must fully match eventData.
func matchOrCombinator(patternVal any, eventData map[string]any) bool {
	alternatives, ok := patternVal.([]any)
	if !ok {
		return false
	}

	for _, alt := range alternatives {
		altPattern, isMap := alt.(map[string]any)
		if !isMap {
			continue
		}

		if matchObject(altPattern, eventData) {
			return true
		}
	}

	return false
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
// If the event value is a JSON array, the match succeeds if any element satisfies the matcher.
func matchSingle(matcher, eventVal any, exists bool) bool {
	if arr, ok := eventVal.([]any); ok {
		for _, elem := range arr {
			if matchSingleValue(matcher, elem, exists) {
				return true
			}
		}

		return false
	}

	return matchSingleValue(matcher, eventVal, exists)
}

// matchSingleValue checks whether a scalar eventVal satisfies a single matcher.
func matchSingleValue(matcher, eventVal any, exists bool) bool {
	switch m := matcher.(type) {
	case map[string]any:
		return matchSpecialMatcher(m, eventVal, exists)
	default:
		// Exact match.
		return eventVal == matcher
	}
}

// matchSpecialMatcher handles special matcher objects like {"prefix": ...}, {"exists": ...}, etc.
// It delegates string-only matchers to matchStringMatcher.
func matchSpecialMatcher(m map[string]any, eventVal any, exists bool) bool {
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

	if cidrVal, ok := m["cidr"]; ok {
		return matchCIDR(cidrVal, eventVal)
	}

	return matchStringMatcher(m, eventVal)
}

// matchStringMatcher handles string-based matchers: prefix, suffix, wildcard, equals-ignore-case.
func matchStringMatcher(m map[string]any, eventVal any) bool {
	es, esOk := eventVal.(string)

	if prefix, ok := m["prefix"]; ok {
		ps, psOk := prefix.(string)
		if !psOk || !esOk {
			return false
		}

		return strings.HasPrefix(es, ps)
	}

	if suffix, ok := m["suffix"]; ok {
		ss, ssOk := suffix.(string)
		if !ssOk || !esOk {
			return false
		}

		return strings.HasSuffix(es, ss)
	}

	if wildcardVal, ok := m["wildcard"]; ok {
		ws, wsOk := wildcardVal.(string)
		if !wsOk || !esOk {
			return false
		}

		return matchWildcard(ws, es)
	}

	if equalsIgnoreCase, ok := m["equals-ignore-case"]; ok {
		cs, csOk := equalsIgnoreCase.(string)
		if !csOk || !esOk {
			return false
		}

		return strings.EqualFold(es, cs)
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

		if !opOk || !valOk || !compareNumeric(op, num, val) {
			return false
		}
	}

	return true
}

// compareNumeric returns true if the comparison "num op val" holds.
func compareNumeric(op string, num, val float64) bool {
	switch op {
	case ">":
		return num > val
	case ">=":
		return num >= val
	case "<":
		return num < val
	case "<=":
		return num <= val
	case "=":
		return num == val
	default:
		return false
	}
}

// matchAnythingBut matches when the event value is NOT in the provided set.
func matchAnythingBut(v, eventVal any) bool {
	switch ab := v.(type) {
	case []any:
		return !slices.Contains(ab, eventVal)
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

// matchCIDR returns true when the event value is an IP address that falls within the CIDR range.
func matchCIDR(cidrVal, eventVal any) bool {
	cidrStr, ok := cidrVal.(string)
	if !ok {
		return false
	}

	ipStr, ok := eventVal.(string)
	if !ok {
		return false
	}

	_, ipNet, err := net.ParseCIDR(cidrStr)
	if err != nil {
		return false
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	return ipNet.Contains(ip)
}

// matchWildcard returns true when the string s matches the glob pattern.
// Supported meta-characters: '*' (any sequence) and '?' (any single character).
// Uses a standard iterative two-pointer algorithm to avoid recursion.
func matchWildcard(pattern, s string) bool {
	patternIdx, stringIdx := 0, 0
	lastStarIdx := -1
	lastStarMatch := 0

	for stringIdx < len(s) {
		switch {
		case patternIdx < len(pattern) && (pattern[patternIdx] == '?' || pattern[patternIdx] == s[stringIdx]):
			patternIdx++
			stringIdx++
		case patternIdx < len(pattern) && pattern[patternIdx] == '*':
			lastStarIdx = patternIdx
			lastStarMatch = stringIdx
			patternIdx++
		case lastStarIdx != -1:
			patternIdx = lastStarIdx + 1
			lastStarMatch++
			stringIdx = lastStarMatch
		default:
			return false
		}
	}

	for patternIdx < len(pattern) && pattern[patternIdx] == '*' {
		patternIdx++
	}

	return patternIdx == len(pattern)
}
