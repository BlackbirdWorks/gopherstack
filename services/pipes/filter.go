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
		msgVal, exists := msgMap[field]
		if !fieldMatchesRule(msgVal, exists, ruleRaw) {
			return false
		}
	}

	return true
}

// fieldMatchesRule checks whether msgVal satisfies the EventBridge rule array.
// exists reports whether the field was present in the message at all --
// needed because {"exists": false} (eb-event-patterns-content-based-filtering.html,
// "Exists matching", Pipe support: Yes) matches precisely when the field is
// absent, so evaluation cannot short-circuit on absence the way every other
// operator does.
func fieldMatchesRule(msgVal json.RawMessage, exists bool, ruleRaw json.RawMessage) bool {
	var rules []json.RawMessage
	if err := json.Unmarshal(ruleRaw, &rules); err != nil {
		return false
	}

	for _, rule := range rules {
		if matchesRule(msgVal, exists, rule) {
			return true
		}
	}

	return false
}

// matchesRule evaluates a single rule against a message field value.
// Supported rule shapes:
//   - "string"                    — exact string equality
//   - {"prefix": "pfx"}           — string prefix match
//   - {"suffix": "sfx"}           — string suffix match
//   - {"anything-but": "a"}       — value must not equal the given string
//   - {"anything-but": ["a","b"]} — value must not equal any listed string
//   - {"exists": true/false}      — field presence
//
// msgExists is false whenever the field was absent from the message; every
// operator besides exists requires a value to compare against, so absence
// fails them all except an explicit {"exists": false}.
func matchesRule(msgVal json.RawMessage, msgExists bool, rule json.RawMessage) bool {
	if ruleObj, ok := existsRuleObject(rule); ok {
		if want, wantOK := existsWant(ruleObj); wantOK {
			return msgExists == want
		}
	}

	if !msgExists {
		return false
	}

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
		return !matchesAnythingBut(anythingButRaw, msgStr)
	}

	return false
}

// existsRuleObject unmarshals rule as a JSON object, returning ok=false for
// any other shape (plain string, array, ...).
func existsRuleObject(rule json.RawMessage) (map[string]json.RawMessage, bool) {
	var ruleObj map[string]json.RawMessage
	if err := json.Unmarshal(rule, &ruleObj); err != nil {
		return nil, false
	}

	return ruleObj, true
}

// existsWant extracts {"exists": true/false}'s boolean, ok=false if the key
// is absent or not a bool.
func existsWant(ruleObj map[string]json.RawMessage) (bool, bool) {
	existsRaw, hasKey := ruleObj["exists"]
	if !hasKey {
		return false, false
	}

	var want bool
	if err := json.Unmarshal(existsRaw, &want); err != nil {
		return false, false
	}

	return want, true
}

// matchesAnythingBut reports whether msgStr equals the anything-but rule
// value, which per the docs (eb-filtering-anything-but) may be a single
// string or a list of strings -- "state": [ { "anything-but": "initializing" } ]
// is the documented single-value form, distinct from the list form.
func matchesAnythingBut(anythingButRaw json.RawMessage, msgStr string) bool {
	var single string
	if err := json.Unmarshal(anythingButRaw, &single); err == nil {
		return msgStr == single
	}

	var excluded []string
	if err := json.Unmarshal(anythingButRaw, &excluded); err == nil {
		return slices.Contains(excluded, msgStr)
	}

	return false
}
