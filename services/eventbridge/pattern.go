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

// isKnownMatcher reports whether key is a valid EventBridge matcher object key.
func isKnownMatcher(key string) bool {
	switch key {
	case "prefix", "suffix", "exists", "numeric", "anything-but", "cidr", "wildcard", "equals-ignore-case":
		return true
	}

	return false
}

// validatePatternObject validates the structure of an EventBridge pattern object.
// Each field value must be an array of matchers or a nested object.
func validatePatternObject(pattern map[string]any) error {
	for key, val := range pattern {
		if key == "$or" {
			if err := validateOrCombinator(val); err != nil {
				return err
			}

			continue
		}

		switch v := val.(type) {
		case map[string]any:
			if err := validatePatternObject(v); err != nil {
				return err
			}
		case []any:
			if err := validateMatcherArray(key, v); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: value for field %q must be an array or object, got scalar", ErrInvalidParameter, key)
		}
	}

	return nil
}

// validateOrCombinator validates the $or combinator value.
func validateOrCombinator(val any) error {
	alts, ok := val.([]any)
	if !ok {
		return fmt.Errorf("%w: $or must be an array", ErrInvalidParameter)
	}

	for _, alt := range alts {
		subPat, isMap := alt.(map[string]any)
		if !isMap {
			return fmt.Errorf("%w: $or elements must be objects", ErrInvalidParameter)
		}

		if err := validatePatternObject(subPat); err != nil {
			return err
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
			return fmt.Errorf("%w: matcher for field %q has unsupported type", ErrInvalidParameter, field)
		}
	}

	return nil
}

// validateMatcherObject validates a single matcher object (e.g., {"prefix": "foo"}).
func validateMatcherObject(field string, m map[string]any) error {
	for key := range m {
		if !isKnownMatcher(key) {
			return fmt.Errorf("%w: unknown matcher %q for field %q", ErrInvalidParameter, key, field)
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
//	[{"wildcard": "com.example.*"}]— wildcard string match ('*' only; '\*' and '\\' are literal escapes)
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
		if !esOk {
			return false
		}

		return matchPrefixMatcher(prefix, es)
	}

	if suffix, ok := m["suffix"]; ok {
		if !esOk {
			return false
		}

		return matchSuffixMatcher(suffix, es)
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

// matchPrefixMatcher matches a prefix matcher value against the event string.
// AWS supports both a plain string prefix and a case-insensitive form:
//
//	{"prefix": "foo"}
//	{"prefix": {"equals-ignore-case": "FOO"}}
func matchPrefixMatcher(prefix any, es string) bool {
	switch p := prefix.(type) {
	case string:
		return strings.HasPrefix(es, p)
	case map[string]any:
		ci, ok := p["equals-ignore-case"].(string)
		if !ok {
			return false
		}

		return len(es) >= len(ci) && strings.EqualFold(es[:len(ci)], ci)
	default:
		return false
	}
}

// matchSuffixMatcher matches a suffix matcher value against the event string.
// AWS supports both a plain string suffix and a case-insensitive form:
//
//	{"suffix": "foo"}
//	{"suffix": {"equals-ignore-case": "FOO"}}
func matchSuffixMatcher(suffix any, es string) bool {
	switch s := suffix.(type) {
	case string:
		return strings.HasSuffix(es, s)
	case map[string]any:
		ci, ok := s["equals-ignore-case"].(string)
		if !ok {
			return false
		}

		return len(es) >= len(ci) && strings.EqualFold(es[len(es)-len(ci):], ci)
	default:
		return false
	}
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

// matchAnythingBut matches when the event value does NOT satisfy the negated rule.
//
// AWS supports several anything-but forms:
//
//	{"anything-but": "foo"}                      — scalar exclusion
//	{"anything-but": ["a", "b"]}                 — list exclusion
//	{"anything-but": {"prefix": "init"}}         — negated prefix (scalar or list)
//	{"anything-but": {"suffix": "ing"}}          — negated suffix (scalar or list)
//	{"anything-but": {"wildcard": "*ing"}}       — negated wildcard
//	{"anything-but": {"equals-ignore-case": "x"}}— negated case-insensitive equality
//	{"anything-but": {"numeric": [">", 5]}}      — negated numeric comparison
func matchAnythingBut(v, eventVal any) bool {
	switch ab := v.(type) {
	case []any:
		return !slices.Contains(ab, eventVal)
	case map[string]any:
		return !matchAnythingButObject(ab, eventVal)
	default:
		return eventVal != v
	}
}

// matchAnythingButObject reports whether eventVal satisfies the inner matcher of an
// object-form anything-but rule. Its result is negated by the caller. The inner
// value may itself be a list, in which case satisfying any element counts as a match.
func matchAnythingButObject(ab map[string]any, eventVal any) bool {
	if numericRules, ok := ab["numeric"]; ok {
		return matchNumeric(numericRules, eventVal)
	}

	for _, key := range []string{"prefix", "suffix", "wildcard", "equals-ignore-case"} {
		inner, ok := ab[key]
		if !ok {
			continue
		}

		if list, isList := inner.([]any); isList {
			for _, item := range list {
				if matchStringMatcher(map[string]any{key: item}, eventVal) {
					return true
				}
			}

			return false
		}

		return matchStringMatcher(map[string]any{key: inner}, eventVal)
	}

	return false
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

// wildcardToken is one unit of a tokenized wildcard pattern: either a
// wildcard star or a literal byte to match exactly.
type wildcardToken struct {
	isStar bool
	lit    byte
}

// tokenizeWildcardPattern splits pattern into literal bytes and stars,
// resolving EventBridge's documented backslash escapes: '\*' is a literal
// '*' and '\\' is a literal '\' ("EventBridge supports using the backslash
// character (\) to specify the literal * and \ characters in wildcard
// filters", eb-event-patterns-content-based-filtering.html#eb-filtering-wildcard-matching).
// Only '*' is a wildcard meta-character; '?' has no special meaning and is
// matched literally like any other byte.
func tokenizeWildcardPattern(pattern string) []wildcardToken {
	tokens := make([]wildcardToken, 0, len(pattern))

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		if c == '\\' && i+1 < len(pattern) && (pattern[i+1] == '*' || pattern[i+1] == '\\') {
			tokens = append(tokens, wildcardToken{lit: pattern[i+1]})
			i++

			continue
		}

		if c == '*' {
			tokens = append(tokens, wildcardToken{isStar: true})

			continue
		}

		tokens = append(tokens, wildcardToken{lit: c})
	}

	return tokens
}

// matchWildcard returns true when the string s matches the EventBridge
// wildcard pattern. Uses a standard iterative two-pointer algorithm over the
// tokenized pattern to avoid recursion.
func matchWildcard(pattern, s string) bool {
	tokens := tokenizeWildcardPattern(pattern)

	tokenIdx, stringIdx := 0, 0
	lastStarIdx := -1
	lastStarMatch := 0

	for stringIdx < len(s) {
		switch {
		case tokenIdx < len(tokens) && !tokens[tokenIdx].isStar && tokens[tokenIdx].lit == s[stringIdx]:
			tokenIdx++
			stringIdx++
		case tokenIdx < len(tokens) && tokens[tokenIdx].isStar:
			lastStarIdx = tokenIdx
			lastStarMatch = stringIdx
			tokenIdx++
		case lastStarIdx != -1:
			tokenIdx = lastStarIdx + 1
			lastStarMatch++
			stringIdx = lastStarMatch
		default:
			return false
		}
	}

	for tokenIdx < len(tokens) && tokens[tokenIdx].isStar {
		tokenIdx++
	}

	return tokenIdx == len(tokens)
}
