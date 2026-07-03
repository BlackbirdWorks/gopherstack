package lambda

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// eventFilterMatches reports whether the given event source record satisfies the
// mapping's FilterCriteria. AWS Lambda semantics: a record passes if it matches
// AT LEAST ONE of the filters (logical OR across filters). When no filter is
// configured every record passes.
//
// Each Filter.Pattern is an AWS event-pattern document (the same content-based
// filtering syntax used by EventBridge and EventSourceMappings). A record is
// represented as a decoded map[string]any (see the *FilterView helpers), so that
// nested fields — e.g. an SQS message body parsed as JSON — can be matched.
func eventFilterMatches(fc *FilterCriteria, record map[string]any) bool {
	if fc == nil || len(fc.Filters) == 0 {
		return true
	}

	for _, f := range fc.Filters {
		if f.Pattern == "" {
			// An empty pattern in AWS matches nothing meaningful; skip it so a
			// stray empty filter does not accidentally pass everything.
			continue
		}

		var pattern map[string]json.RawMessage
		if err := json.Unmarshal([]byte(f.Pattern), &pattern); err != nil {
			continue
		}

		if patternMatchesObject(pattern, record) {
			return true
		}
	}

	return false
}

// patternMatchesObject evaluates an event-pattern object against a decoded value.
// Every key in the pattern must be satisfied (logical AND across keys).
func patternMatchesObject(pattern map[string]json.RawMessage, value map[string]any) bool {
	for key, rawRule := range pattern {
		fieldVal, present := value[key]

		if !fieldMatchesRule(rawRule, fieldVal, present) {
			return false
		}
	}

	return true
}

// fieldMatchesRule evaluates a single pattern field. rawRule is either a nested
// pattern object (recurse) or a JSON array of match rules (any of which may match).
func fieldMatchesRule(rawRule json.RawMessage, fieldVal any, present bool) bool {
	trimmed := strings.TrimSpace(string(rawRule))

	// Nested object pattern: recurse into the sub-document.
	if strings.HasPrefix(trimmed, "{") {
		nested, ok := fieldVal.(map[string]any)
		if !ok {
			return false
		}

		var subPattern map[string]json.RawMessage
		if err := json.Unmarshal(rawRule, &subPattern); err != nil {
			return false
		}

		return patternMatchesObject(subPattern, nested)
	}

	// Rule array: the field matches if any element in the array matches.
	var rules []json.RawMessage
	if err := json.Unmarshal(rawRule, &rules); err != nil {
		return false
	}

	for _, rule := range rules {
		if ruleMatchesValue(rule, fieldVal, present) {
			return true
		}
	}

	return false
}

// ruleMatchesValue evaluates one rule element against the field value. A rule is
// either a scalar (exact match) or an operator object like {"prefix":"x"}.
func ruleMatchesValue(rule json.RawMessage, fieldVal any, present bool) bool {
	trimmed := strings.TrimSpace(string(rule))

	if strings.HasPrefix(trimmed, "{") {
		return operatorMatches(rule, fieldVal, present)
	}

	return scalarMatches(rule, fieldVal, present)
}

// scalarMatches performs an exact comparison between a scalar rule and the field.
func scalarMatches(rule json.RawMessage, fieldVal any, present bool) bool {
	if !present {
		return false
	}

	var ruleVal any
	if err := json.Unmarshal(rule, &ruleVal); err != nil {
		return false
	}

	switch rv := ruleVal.(type) {
	case string:
		fv, ok := fieldVal.(string)

		return ok && fv == rv
	case float64:
		fv, ok := toFloat(fieldVal)

		return ok && fv == rv
	case bool:
		fv, ok := fieldVal.(bool)

		return ok && fv == rv
	case nil:
		return fieldVal == nil
	default:
		return false
	}
}

// operatorMatches evaluates an operator rule object such as {"prefix":"x"},
// {"exists":true}, {"anything-but":[...]}, or {"numeric":[">",5]}.
func operatorMatches(rule json.RawMessage, fieldVal any, present bool) bool {
	var op map[string]json.RawMessage
	if err := json.Unmarshal(rule, &op); err != nil {
		return false
	}

	for name, arg := range op {
		if !singleOperatorMatches(name, arg, fieldVal, present) {
			return false
		}
	}

	return true
}

// singleOperatorMatches dispatches one named operator.
func singleOperatorMatches(name string, arg json.RawMessage, fieldVal any, present bool) bool {
	switch name {
	case "exists":
		return existsMatches(arg, present)
	case "prefix":
		return affixMatches(arg, fieldVal, present, strings.HasPrefix)
	case "suffix":
		return affixMatches(arg, fieldVal, present, strings.HasSuffix)
	case "equals-ignore-case":
		return equalsIgnoreCaseMatches(arg, fieldVal, present)
	case "anything-but":
		return anythingButMatches(arg, fieldVal, present)
	case "numeric":
		return numericMatches(arg, fieldVal, present)
	default:
		return false
	}
}

func existsMatches(arg json.RawMessage, present bool) bool {
	var want bool
	if err := json.Unmarshal(arg, &want); err != nil {
		return false
	}

	return want == present
}

func affixMatches(arg json.RawMessage, fieldVal any, present bool, fn func(string, string) bool) bool {
	if !present {
		return false
	}

	var want string
	if err := json.Unmarshal(arg, &want); err != nil {
		return false
	}

	fv, ok := fieldVal.(string)

	return ok && fn(fv, want)
}

func equalsIgnoreCaseMatches(arg json.RawMessage, fieldVal any, present bool) bool {
	if !present {
		return false
	}

	var want string
	if err := json.Unmarshal(arg, &want); err != nil {
		return false
	}

	fv, ok := fieldVal.(string)

	return ok && strings.EqualFold(fv, want)
}

// anythingButMatches matches when the field is present and NOT equal to any of
// the listed values. The argument may be a scalar or an array of scalars.
func anythingButMatches(arg json.RawMessage, fieldVal any, present bool) bool {
	if !present {
		return false
	}

	trimmed := strings.TrimSpace(string(arg))
	if !strings.HasPrefix(trimmed, "[") {
		return !scalarMatches(arg, fieldVal, present)
	}

	var values []json.RawMessage
	if err := json.Unmarshal(arg, &values); err != nil {
		return false
	}

	for _, v := range values {
		if scalarMatches(v, fieldVal, present) {
			return false
		}
	}

	return true
}

// numericMatches evaluates a numeric rule like ["=",1] or [">",0,"<=",10].
func numericMatches(arg json.RawMessage, fieldVal any, present bool) bool {
	if !present {
		return false
	}

	fv, ok := toFloat(fieldVal)
	if !ok {
		return false
	}

	var tokens []json.RawMessage
	if err := json.Unmarshal(arg, &tokens); err != nil {
		return false
	}

	for i := 0; i+1 < len(tokens); i += 2 {
		var operator string
		if err := json.Unmarshal(tokens[i], &operator); err != nil {
			return false
		}

		var operand float64
		if err := json.Unmarshal(tokens[i+1], &operand); err != nil {
			return false
		}

		if !numericComparison(operator, fv, operand) {
			return false
		}
	}

	return true
}

func numericComparison(operator string, fieldVal, operand float64) bool {
	const epsilon = 1e-9

	switch operator {
	case "=":
		return math.Abs(fieldVal-operand) < epsilon
	case "!=":
		return math.Abs(fieldVal-operand) >= epsilon
	case ">":
		return fieldVal > operand
	case ">=":
		return fieldVal >= operand
	case "<":
		return fieldVal < operand
	case "<=":
		return fieldVal <= operand
	default:
		return false
	}
}

// toFloat coerces a decoded JSON value (float64 or numeric string) to a float64.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()

		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)

		return f, err == nil
	default:
		return 0, false
	}
}

// sqsFilterView builds the decoded record shape used for SQS filter matching.
// The message body is parsed as JSON when it is a valid JSON object so that
// content-based filters such as {"body":{"type":["order"]}} work; otherwise the
// raw string body is used.
func sqsFilterView(msg *SQSMessage) map[string]any {
	view := map[string]any{
		"messageId":     msg.MessageID,
		"receiptHandle": msg.ReceiptHandle,
		"md5OfBody":     msg.MD5OfBody,
	}

	if body, ok := parseJSONObject(msg.Body); ok {
		view["body"] = body
	} else {
		view["body"] = msg.Body
	}

	if len(msg.Attributes) > 0 {
		attrs := make(map[string]any, len(msg.Attributes))
		for k, v := range msg.Attributes {
			attrs[k] = v
		}

		view["attributes"] = attrs
	}

	if len(msg.MessageAttributes) > 0 {
		ma := make(map[string]any, len(msg.MessageAttributes))
		for k, v := range msg.MessageAttributes {
			ma[k] = map[string]any{"stringValue": v.StringValue, "dataType": v.DataType}
		}

		view["messageAttributes"] = ma
	}

	return view
}

// kinesisFilterView builds the decoded record shape used for Kinesis filter
// matching. The record data is base64-decoded and parsed as JSON when possible.
func kinesisFilterView(partitionKey, sequenceNumber string, data []byte) map[string]any {
	inner := map[string]any{
		"partitionKey":   partitionKey,
		"sequenceNumber": sequenceNumber,
	}

	if obj, ok := parseJSONObjectBytes(data); ok {
		inner["data"] = obj
	} else {
		inner["data"] = base64.StdEncoding.EncodeToString(data)
	}

	return map[string]any{"kinesis": inner}
}

// dynamoDBFilterView builds the decoded record shape used for DynamoDB stream
// filter matching.
func dynamoDBFilterView(eventName string, newImage, oldImage, keys map[string]any) map[string]any {
	inner := map[string]any{}
	if newImage != nil {
		inner["NewImage"] = newImage
	}

	if oldImage != nil {
		inner["OldImage"] = oldImage
	}

	if keys != nil {
		inner["Keys"] = keys
	}

	return map[string]any{
		"eventName": eventName,
		"dynamodb":  inner,
	}
}

func parseJSONObject(s string) (map[string]any, bool) {
	return parseJSONObjectBytes([]byte(s))
}

func parseJSONObjectBytes(b []byte) (map[string]any, bool) {
	trimmed := strings.TrimSpace(string(b))
	if !strings.HasPrefix(trimmed, "{") {
		return nil, false
	}

	var obj map[string]any
	if err := json.Unmarshal(b, &obj); err != nil {
		return nil, false
	}

	return obj, true
}
