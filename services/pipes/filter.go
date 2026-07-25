package pipes

import (
	"encoding/json"
	"slices"
	"strings"
)

// matchesAnyFilter returns true if body passes at least one of the given filters.
// body is the raw event record body/data for any pipe source type (SQS message
// body, Kinesis record data, or a marshalled DynamoDB Streams record) -- the
// matching engine itself is source-agnostic.
//
// Filter.Pattern semantics:
//   - If Pattern is empty, every message matches (pass-through).
//   - If Pattern is valid JSON starting with '{', it is evaluated as a JSON event
//     pattern: each top-level key must be present in the parsed message body with a
//     value matching the corresponding rule array (exact string match only for now).
//   - Otherwise the pattern is treated as a literal substring and matched against
//     the raw message body (backward-compatible behaviour).
func matchesAnyFilter(body string, filters []Filter) bool {
	for _, f := range filters {
		if matchesSingleFilter(body, f) {
			return true
		}
	}

	return false
}

func matchesSingleFilter(body string, f Filter) bool {
	if f.Pattern == "" {
		return true
	}

	trimmed := strings.TrimSpace(f.Pattern)
	if strings.HasPrefix(trimmed, "{") {
		return matchesJSONPattern(body, trimmed)
	}

	// Backward-compatible substring match for non-JSON patterns.
	return strings.Contains(body, f.Pattern)
}

// matchesJSONPattern tests whether msgBody satisfies the EventBridge-style
// JSON event pattern. Only top-level field matching is implemented; nested
// field paths and advanced operators (prefix, suffix, numeric range, cidr,
// exists, anything-but) are left as future work.
//
// Pattern shape:  {"field": ["value1", "value2", ...], ...}
// Each field in the pattern must exist in the message and its value must
// equal at least one of the listed rule values.
func matchesJSONPattern(msgBody, pattern string) bool {
	var patternMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(pattern), &patternMap); err != nil {
		// Malformed pattern: fall back to substring.
		return strings.Contains(msgBody, pattern)
	}

	if len(patternMap) == 0 {
		return true
	}

	var msgMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(msgBody), &msgMap); err != nil {
		// Message body is not a JSON object; pattern cannot match.
		return false
	}

	for field, ruleRaw := range patternMap {
		msgVal, ok := msgMap[field]
		if !ok {
			return false
		}

		if !fieldMatchesRule(msgVal, ruleRaw) {
			return false
		}
	}

	return true
}

// fieldMatchesRule checks whether msgVal satisfies the EventBridge rule array.
// The rule is expected to be a JSON array of matchers. Currently only plain
// string values are supported; each string is compared for equality with the
// string representation of msgVal.
func fieldMatchesRule(msgVal, ruleRaw json.RawMessage) bool {
	var rules []json.RawMessage
	if err := json.Unmarshal(ruleRaw, &rules); err != nil {
		return false
	}

	for _, rule := range rules {
		if matchesRule(msgVal, rule) {
			return true
		}
	}

	return false
}

// matchesRule evaluates a single rule against a message field value.
// Supported rule shapes:
//   - "string"                   — exact string equality
//   - {"prefix": "pfx"}          — string prefix match
//   - {"suffix": "sfx"}          — string suffix match
//   - {"anything-but": ["a","b"]} — value must not equal any listed string
//   - {"exists": true/false}      — field presence (handled at call site; always true here)
func matchesRule(msgVal, rule json.RawMessage) bool {
	// Try plain string equality.
	var ruleStr string
	if err := json.Unmarshal(rule, &ruleStr); err == nil {
		var msgStr string
		if err2 := json.Unmarshal(msgVal, &msgStr); err2 == nil {
			return msgStr == ruleStr
		}
		// Allow numeric or bool comparison via string representation.
		return strings.Trim(string(msgVal), `"`) == ruleStr
	}

	// Try object-shaped rule: {"prefix": ..., "suffix": ..., "anything-but": ...}
	var ruleObj map[string]json.RawMessage
	if err := json.Unmarshal(rule, &ruleObj); err != nil {
		return false
	}

	var msgStr string
	_ = json.Unmarshal(msgVal, &msgStr)

	if prefixRaw, ok := ruleObj["prefix"]; ok {
		var prefix string
		if err := json.Unmarshal(prefixRaw, &prefix); err == nil {
			return strings.HasPrefix(msgStr, prefix)
		}
	}

	if suffixRaw, ok := ruleObj["suffix"]; ok {
		var suffix string
		if err := json.Unmarshal(suffixRaw, &suffix); err == nil {
			return strings.HasSuffix(msgStr, suffix)
		}
	}

	if anythingButRaw, ok := ruleObj["anything-but"]; ok {
		var excluded []string
		if err := json.Unmarshal(anythingButRaw, &excluded); err == nil {
			return !slices.Contains(excluded, msgStr)
		}
	}

	return false
}
