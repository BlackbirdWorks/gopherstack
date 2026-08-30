package sns

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
)

// matchesFilterPolicyMessageBody evaluates a parsed filter policy against the
// message body when FilterPolicyScope=MessageBody. The message must be a valid
// JSON object; if it is not, no subscription receives the message.
func matchesFilterPolicyMessageBody(policy parsedFilterPolicy, message string) bool {
	if len(policy) == 0 {
		return true
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(message), &body); err != nil {
		return false
	}

	for key, conditions := range policy {
		if key == orOperatorKey && isRecognisedOrOperator(conditions) {
			if !matchesOrBody(conditions, message) {
				return false
			}

			continue
		}

		if !matchesBodyKeyConditions(body, key, conditions) {
			return false
		}
	}

	return true
}

// matchesBodyKeyConditions evaluates one FilterPolicy key against a JSON message
// body. Scalar values (string, number, bool) match directly; JSON-array values
// are expanded so the condition is satisfied when ANY element matches, mirroring
// AWS message-body array handling.
func matchesBodyKeyConditions(
	body map[string]json.RawMessage,
	key string,
	conditions []json.RawMessage,
) bool {
	rawVal, exists := body[key]
	if !exists {
		return matchesConditions("", false, conditions)
	}

	for _, candidate := range bodyMatchValues(rawVal) {
		if matchesConditions(candidate, true, conditions) {
			return true
		}
	}

	return false
}

// bodyMatchValues extracts the candidate scalar string(s) from a JSON message
// body value: a string, number, or boolean yields one candidate; a JSON array of
// scalars yields one candidate per element. A value that cannot be reduced to a
// scalar yields no candidates (the key is then treated as non-matching).
func bodyMatchValues(raw json.RawMessage) []string {
	if v, ok := scalarBodyValue(raw); ok {
		return []string{v}
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err == nil {
		out := make([]string, 0, len(arr))
		for _, elem := range arr {
			if v, ok := scalarBodyValue(elem); ok {
				out = append(out, v)
			}
		}

		return out
	}

	return nil
}

// scalarBodyValue converts a single JSON scalar (string, number, or boolean) to
// its string form for filter matching. It reports false for non-scalar values.
func scalarBodyValue(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, true
	}

	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String(), true
	}

	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return strconv.FormatBool(b), true
	}

	return "", false
}

// matchesOrBody evaluates a recognised "$or" operator against a JSON message
// body: it returns true when AT LEAST ONE sub-policy fully matches.
func matchesOrBody(subPolicies []json.RawMessage, message string) bool {
	for _, raw := range subPolicies {
		sub, err := parseFilterPolicy(string(raw))
		if err != nil {
			continue
		}

		if matchesFilterPolicyMessageBody(sub, message) {
			return true
		}
	}

	return false
}

func matchesParsedFilterPolicy(policy parsedFilterPolicy, attrs map[string]MessageAttribute) bool {
	if policy == nil {
		return true
	}

	for key, conditions := range policy {
		if key == orOperatorKey && isRecognisedOrOperator(conditions) {
			if !matchesOrAttributes(conditions, attrs) {
				return false
			}

			continue
		}

		if !matchesAttributeConditions(key, conditions, attrs) {
			return false
		}
	}

	return true
}

// matchesAttributeConditions evaluates a single FilterPolicy attribute key
// against message attributes. For String.Array attributes, each array element is
// matched independently and the condition is satisfied if ANY element matches
// (OR across elements), mirroring AWS String.Array handling.
func matchesAttributeConditions(
	key string, conditions []json.RawMessage, attrs map[string]MessageAttribute,
) bool {
	attr, attrExists := attrs[key]

	for _, candidate := range attributeMatchValues(attr, attrExists) {
		if matchesConditions(candidate, attrExists, conditions) {
			return true
		}
	}

	return false
}

// attributeMatchValues returns the set of scalar string values that a message
// attribute contributes to filter matching. A String.Array attribute (its value
// is a JSON array of strings) expands to one candidate per element; all other
// attributes yield their single StringValue. A non-existent attribute yields a
// single empty candidate so "exists":false and negated conditions still run.
func attributeMatchValues(attr MessageAttribute, attrExists bool) []string {
	if !attrExists {
		return []string{""}
	}

	if attr.DataType == "String.Array" {
		var elems []string
		if err := json.Unmarshal([]byte(attr.StringValue), &elems); err == nil && len(elems) > 0 {
			return elems
		}
	}

	return []string{attr.StringValue}
}

// matchesOrAttributes evaluates a recognised "$or" operator against message
// attributes: it returns true when AT LEAST ONE sub-policy fully matches.
func matchesOrAttributes(subPolicies []json.RawMessage, attrs map[string]MessageAttribute) bool {
	for _, raw := range subPolicies {
		sub, err := parseFilterPolicy(string(raw))
		if err != nil {
			continue
		}

		if matchesParsedFilterPolicy(sub, attrs) {
			return true
		}
	}

	return false
}

// matchObjectCondition evaluates a single JSON-object SNS filter condition such as
// {"prefix": "order-"}, {"suffix": ".jpg"}, {"anything-but": [...]},
// {"equals-ignore-case": "OrderId"}, {"exists": true}, or {"numeric": [">", 0]}.
func matchObjectCondition(value string, attrExists bool, obj map[string]json.RawMessage) bool {
	// String-operand operators share the same shape: decode a single string
	// operand and apply a predicate. They require the attribute to exist.
	stringOps := map[string]func(value, operand string) bool{
		"prefix":             strings.HasPrefix,
		"suffix":             strings.HasSuffix,
		"equals-ignore-case": strings.EqualFold,
		"wildcard":           matchWildcard,
		"cidr":               matchCIDR,
	}

	for name, pred := range stringOps {
		raw, ok := obj[name]
		if !ok {
			continue
		}

		var operand string
		if err := json.Unmarshal(raw, &operand); err != nil {
			return false
		}

		return attrExists && pred(value, operand)
	}

	if existsRaw, ok := obj["exists"]; ok {
		var existsVal bool
		if err := json.Unmarshal(existsRaw, &existsVal); err == nil {
			return attrExists == existsVal
		}

		return false
	}

	if anythingButRaw, ok := obj["anything-but"]; ok {
		return matchAnythingBut(value, attrExists, anythingButRaw)
	}

	if numericRaw, ok := obj["numeric"]; ok {
		return attrExists && matchNumericCondition(value, numericRaw)
	}

	return false
}

// matchWildcard reports whether value matches an SNS wildcard pattern. The only
// wildcard metacharacter is '*', which matches any (possibly empty) run of
// characters. All other characters match literally. AWS does not support a
// single-character wildcard, so '*' is the sole special token.
func matchWildcard(value, pattern string) bool {
	segments := strings.Split(pattern, "*")

	// No '*' in the pattern: it must match the value exactly.
	if len(segments) == 1 {
		return value == pattern
	}

	// The value must start with the first segment and end with the last segment.
	if first := segments[0]; !strings.HasPrefix(value, first) {
		return false
	}

	if last := segments[len(segments)-1]; !strings.HasSuffix(value, last) {
		return false
	}

	// Consume the value left-to-right, matching each interior segment in order.
	pos := len(segments[0])
	end := len(value) - len(segments[len(segments)-1])

	for _, seg := range segments[1 : len(segments)-1] {
		if seg == "" {
			continue
		}

		idx := strings.Index(value[pos:end], seg)
		if idx < 0 {
			return false
		}

		pos += idx + len(seg)
	}

	return pos <= end
}

// matchCIDR reports whether value is an IP address contained in the given CIDR
// block. A bare IP (no prefix length) is treated as a /32 or /128 host route,
// matching AWS which accepts either form for the cidr operator.
func matchCIDR(value, cidr string) bool {
	ip := net.ParseIP(value)
	if ip == nil {
		return false
	}

	if !strings.Contains(cidr, "/") {
		target := net.ParseIP(cidr)

		return target != nil && target.Equal(ip)
	}

	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}

	return network.Contains(ip)
}

// matchAnythingBut handles all SNS "anything-but" forms:
//   - {"anything-but": "literal"} / {"anything-but": 123}
//   - {"anything-but": ["a", "b", 1, 2]}
//   - {"anything-but": {"prefix": "order-"}}
//   - {"anything-but": {"suffix": "ball"}}
//   - {"anything-but": {"equals-ignore-case": "Tennis"}}
//   - {"anything-but": {"wildcard": "*ball"}}
//
// In every case the operator is satisfied only when the attribute exists and the
// value does NOT match the negated condition.
func matchAnythingBut(value string, attrExists bool, raw json.RawMessage) bool {
	if !attrExists {
		return false
	}

	// Try as string literal.
	var s string
	if errStr := json.Unmarshal(raw, &s); errStr == nil {
		return value != s
	}

	// Try as number literal.
	var n json.Number
	if errNum := json.Unmarshal(raw, &n); errNum == nil {
		return value != n.String()
	}

	// Try as array of literals.
	var arr []json.RawMessage
	if errArr := json.Unmarshal(raw, &arr); errArr == nil {
		return matchAnythingButArray(value, arr)
	}

	// Try as nested operator object: {"anything-but": {"prefix"|"suffix"|...: ...}}.
	var obj map[string]json.RawMessage
	if errObj := json.Unmarshal(raw, &obj); errObj == nil {
		return matchAnythingButObject(value, obj)
	}

	return true
}

// matchAnythingButObject negates a nested string operator inside an
// "anything-but" condition. It returns true when the value does NOT satisfy the
// inner operator.
func matchAnythingButObject(value string, obj map[string]json.RawMessage) bool {
	if prefixRaw, ok := obj["prefix"]; ok {
		var prefix string
		if err := json.Unmarshal(prefixRaw, &prefix); err == nil {
			return !strings.HasPrefix(value, prefix)
		}

		return false
	}

	if suffixRaw, ok := obj["suffix"]; ok {
		var suffix string
		if err := json.Unmarshal(suffixRaw, &suffix); err == nil {
			return !strings.HasSuffix(value, suffix)
		}

		return false
	}

	if eqICaseRaw, ok := obj["equals-ignore-case"]; ok {
		var want string
		if err := json.Unmarshal(eqICaseRaw, &want); err == nil {
			return !strings.EqualFold(value, want)
		}

		return false
	}

	if wildcardRaw, ok := obj["wildcard"]; ok {
		var pattern string
		if err := json.Unmarshal(wildcardRaw, &pattern); err == nil {
			return !matchWildcard(value, pattern)
		}

		return false
	}

	return false
}

// matchAnythingButArray checks that value does not equal any element in the "anything-but" array.
func matchAnythingButArray(value string, arr []json.RawMessage) bool {
	for _, item := range arr {
		var sv string
		if errI := json.Unmarshal(item, &sv); errI == nil {
			if value == sv {
				return false
			}

			continue
		}

		var nv json.Number
		if errN := json.Unmarshal(item, &nv); errN == nil && value == nv.String() {
			return false
		}
	}

	return true
}

// matchNumericCondition evaluates {"numeric": [op, num, ...]} conditions.
// Conditions are pairs of [operator, number] and ALL pairs must be satisfied (AND semantics).
func matchNumericCondition(value string, raw json.RawMessage) bool {
	valFloat, errParse := strconv.ParseFloat(value, 64)
	if errParse != nil {
		return false
	}

	var conditions []json.RawMessage
	if errUnm := json.Unmarshal(raw, &conditions); errUnm != nil {
		return false
	}

	for i := 0; i+1 < len(conditions); i += 2 {
		var op string
		if errOp := json.Unmarshal(conditions[i], &op); errOp != nil {
			return false
		}

		var num json.Number
		if errNum := json.Unmarshal(conditions[i+1], &num); errNum != nil {
			return false
		}

		threshold, errThresh := strconv.ParseFloat(num.String(), 64)
		if errThresh != nil {
			return false
		}

		if !numericOpMatches(op, valFloat, threshold) {
			return false
		}
	}

	return true
}

// numericOpMatches evaluates a single numeric comparison operator. AWS's
// numeric-value-matching page documents exactly these five operators
// (docs.aws.amazon.com/sns/latest/dg/numeric-value-matching.html); "<>" is
// not among them and is rejected earlier, at parseFilterPolicy time.
func numericOpMatches(op string, value, threshold float64) bool {
	switch op {
	case "=":
		return value == threshold
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	default:
		return false
	}
}

func matchCondition(value string, attrExists bool, raw json.RawMessage) bool {
	// Object conditions: prefix, exists, anything-but, numeric.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		return matchObjectCondition(value, attrExists, obj)
	}

	// String exact match — attribute must exist.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return attrExists && value == s
	}

	// Number exact match — attribute must exist.
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return attrExists && value == n.String()
	}

	return false
}

// matchesConditions returns true if value/existence satisfies at least one condition in the list.
func matchesConditions(value string, attrExists bool, conditions []json.RawMessage) bool {
	for _, raw := range conditions {
		if matchCondition(value, attrExists, raw) {
			return true
		}
	}

	return false
}
