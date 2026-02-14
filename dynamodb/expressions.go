package dynamodb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// evaluateExpression is a naive implementation of DynamoDB expression evaluation.
// It supports basic comparisons and some functions.
// This is NOT a full parser; it uses simple string splitting/tokenizing which is fragile but sufficient for V1.
func evaluateExpression(expression string, item map[string]interface{}, attrValues map[string]interface{}, attrNames map[string]string) (bool, error) {
	if expression == "" {
		return true, nil
	}

	// 1. Resolve Attribute Names (#name -> realName)
	// We'll replace them in the string for simplicity, though this can be risky if values contain the token.
	// A better way is to look them up during evaluation.
	// For this simple implementation, we'll try to resolve names during token processing.

	// 2. Simple recursive/precedence parser or just split by AND/OR?
	// Let's support simple "AND" logic for now (all conditions must match).
	// If "OR" is present, we might split by it first.
	// Limit: Supports "A AND B" or "A OR B", but maybe not complex nesting without a real parser.

	// Strategy: Split by " AND " and ensure all parts are true.
	// TODO: Support OR and parentheses.

	conditions := strings.Split(expression, " AND ")
	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		// Check for NOT
		negate := false
		if strings.HasPrefix(cond, "NOT ") {
			negate = true
			cond = strings.TrimPrefix(cond, "NOT ")
		}

		result, err := evaluateCondition(cond, item, attrValues, attrNames)
		if err != nil {
			return false, err
		}
		if negate {
			result = !result
		}
		if !result {
			return false, nil
		}
	}

	return true, nil
}

func evaluateCondition(cond string, item map[string]interface{}, attrValues map[string]interface{}, attrNames map[string]string) (bool, error) {
	// 1. Functions
	if strings.HasPrefix(cond, "attribute_exists(") {
		path := extractFunctionArg(cond)
		realPath := resolvePath(path, attrNames)
		_, exists := getAttributeValue(item, realPath)
		return exists, nil
	}
	if strings.HasPrefix(cond, "attribute_not_exists(") {
		path := extractFunctionArg(cond)
		realPath := resolvePath(path, attrNames)
		_, exists := getAttributeValue(item, realPath)
		return !exists, nil
	}
	if strings.HasPrefix(cond, "begins_with(") {
		args := extractFunctionArgs(cond) // path, :val
		if len(args) != 2 {
			return false, fmt.Errorf("Invalid begins_with args: %s", cond)
		}
		path := resolvePath(args[0], attrNames)
		targetVal := resolveValue(args[1], attrValues)

		val, exists := getAttributeValue(item, path)
		if !exists {
			return false, nil
		}

		// Unwrap before ToString to avoid JSON structure comparison
		val = unwrapAttributeValue(val)
		targetVal = unwrapAttributeValue(targetVal)

		return strings.HasPrefix(toString(val), toString(targetVal)), nil
	}
	if strings.HasPrefix(cond, "contains(") {
		args := extractFunctionArgs(cond) // path, :val
		if len(args) != 2 {
			return false, fmt.Errorf("Invalid contains args")
		}
		path := resolvePath(args[0], attrNames)
		targetVal := resolveValue(args[1], attrValues)

		val, exists := getAttributeValue(item, path)
		if !exists {
			return false, nil
		}

		val = unwrapAttributeValue(val)
		targetVal = unwrapAttributeValue(targetVal)

		return strings.Contains(toString(val), toString(targetVal)), nil
	}

	// 2. Comparisons
	ops := []string{"=", "<>", "<=", ">=", "<", ">"}
	for _, op := range ops {
		if strings.Contains(cond, " "+op+" ") {
			parts := strings.SplitN(cond, " "+op+" ", 2)
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			lhsVal, lhsExists := getAttributeValue(item, resolvePath(left, attrNames))
			rhsVal := resolveValue(right, attrValues) // right ensures it's :val or specific value? usually :val

			if !lhsExists {
				// Special case: strict inequality with missing attribute?
				// AWS spec says comparison with missing attribute is slightly complex, usually evaluates to false?
				// "The attribute must exist to be comparable".
				// EXCEPT for null?
				return false, nil // Missing attribute fails comparison
			}

			return compareValues(lhsVal, op, rhsVal), nil
		}
	}

	return false, fmt.Errorf("Unknown or unsupported condition: %s", cond)
}

func extractFunctionArg(s string) string {
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")
	if start == -1 || end == -1 {
		return ""
	}
	return strings.TrimSpace(s[start+1 : end])
}

func extractFunctionArgs(s string) []string {
	argStr := extractFunctionArg(s)
	parts := strings.Split(argStr, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func resolvePath(path string, attrNames map[string]string) string {
	if strings.HasPrefix(path, "#") {
		if name, ok := attrNames[path]; ok {
			return name
		}
	}
	// TODO: Handle dot syntax "info.rating"
	return path
}

func resolveValue(token string, attrValues map[string]interface{}) interface{} {
	if strings.HasPrefix(token, ":") {
		if val, ok := attrValues[token]; ok {
			return val
		}
	}
	// Literal? Naive string return
	return token
}

func getAttributeValue(item map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current = item

	for i, part := range parts {
		val, exists := current[part]
		if !exists {
			return nil, false
		}

		if i == len(parts)-1 {
			return val, true
		}

		// Need to traverse deeper. val MUST be a Map Attribute Value {"M": ...}
		valMap, ok := val.(map[string]interface{})
		if !ok {
			return nil, false
		}

		inner, isMap := valMap["M"]
		if !isMap {
			return nil, false
		}

		innerMap, ok := inner.(map[string]interface{})
		if !ok {
			return nil, false
		}

		current = innerMap
	}
	return current, true
}

func unwrapAttributeValue(v interface{}) interface{} {
	// v might be a map[string]interface{} representing DynamoDB JSON
	// e.g. {"S": "val"} or {"N": "123"}
	m, ok := v.(map[string]interface{})
	if !ok {
		return v // Already unwrapped or unknown
	}

	if val, ok := m["S"]; ok {
		return val
	}
	if val, ok := m["N"]; ok {
		// N is string in JSON, needs parsing to float?
		// But in map[string]interface{} from json.Unmarshal, it might be string.
		if s, ok := val.(string); ok {
			// Naive parse, ignoring error for now (or treat as string)
			// Actually, let's keep it as string if it doesn't look like number?
			// DynamoDB N is string.
			// Let's try to return float64 for comparison?
			// For simplicity in V1, let's return the string representation of Number,
			// relying on compareValues to handle numeric conversion if needed,
			// OR we parse it here.
			// Let's return the string representation for now, consistent with "S".
			return s
		}
		return val
	}
	if val, ok := m["BOOL"]; ok {
		return val
	}
	if _, ok := m["NULL"]; ok {
		return nil
	}
	// List "L", Map "M", etc. - return raw map or recursive?
	// For now return raw map if no S/N/BOOL found.
	return v
}

func toString(v interface{}) string {
	v = unwrapAttributeValue(v)
	switch s := v.(type) {
	case string:
		return s
	case bool:
		return fmt.Sprintf("%v", s)
	case float64:
		return fmt.Sprintf("%v", s)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func compareValues(lhs interface{}, op string, rhs interface{}) bool {
	lhs = unwrapAttributeValue(lhs)
	rhs = unwrapAttributeValue(rhs)

	lhsStr := toString(lhs)
	rhsStr := toString(rhs)

	// If both look like numbers, compare as numbers
	// (This is a V1 heuristic, ideal is strict type check)
	// But since we returned N as string above, we re-parse here?
	// Or we should have parsed in unwrap.
	// Let's rely on string comparison for now unless strictly numeric types are passed.

	// Issue: N="10" vs N="2". String: "10" < "2". Number: 10 > 2.
	// We MUST parse numbers.

	// Try parsing as float
	// Try parsing as float
	var lNum, rNum float64
	var lIsNum, rIsNum bool

	// Check if they are float64 already (from JSON number)
	if f, ok := lhs.(float64); ok {
		lNum = f
		lIsNum = true
	} else {
		// Try parse
		if val, err := strconv.ParseFloat(lhsStr, 64); err == nil {
			lNum = val
			lIsNum = true
		}
	}

	if f, ok := rhs.(float64); ok {
		rNum = f
		rIsNum = true
	} else {
		// Try parse
		if val, err := strconv.ParseFloat(rhsStr, 64); err == nil {
			rNum = val
			rIsNum = true
		}
	}

	switch op {
	case "=":
		return lhsStr == rhsStr
	case "<>":
		return lhsStr != rhsStr
	case "<":
		if lIsNum && rIsNum {
			return lNum < rNum
		}
		return lhsStr < rhsStr
	case ">":
		if lIsNum && rIsNum {
			return lNum > rNum
		}
		return lhsStr > rhsStr
	case "<=":
		if lIsNum && rIsNum {
			return lNum <= rNum
		}
		return lhsStr <= rhsStr
	case ">=":
		if lIsNum && rIsNum {
			return lNum >= rNum
		}
		return lhsStr >= rhsStr
	}
	return false
}

// projectItem creates a new item containing only the attributes specified in projectionExpression.
// It supports dot notation for nested attributes (e.g., "info.rating").
// If projectionExpression is empty, it returns the original item.
func projectItem(item map[string]interface{}, projectionExpression string, attrNames map[string]string) map[string]interface{} {
	if projectionExpression == "" {
		return item
	}

	newItem := make(map[string]interface{})
	paths := strings.Split(projectionExpression, ",")

	for _, path := range paths {
		path = strings.TrimSpace(path)
		// Resolve top-level path or full path?
		// AWS resolves aliases in the path. e.g. "#a.#b" -> "info.rating"
		// We need to resolve names *before* splitting by dot?
		// Actually, standard says:
		// "A projection expression is a string that identifies the attributes you want. To retrieve a single attribute, specify its name. For multiple, use comma."
		// "To specify a nested attribute, use a dot."
		// "If an attribute name begins with a number or contains a space... use an expression attribute name."

		// Simplified V1 resolution:
		// Split by dot, resolve each part if it starts with #
		// Then traverse and build newItem.

		realPath := resolveComplexPath(path, attrNames)

		// Get value from source
		val, exists := getAttributeValue(item, realPath)
		if exists {
			// Set value in destination
			// This is tricky: if path is "info.rating", we must create "info" map in newItem if not exists.
			setAttributeValue(newItem, realPath, val)
		}
	}
	return newItem
}

func resolveComplexPath(path string, attrNames map[string]string) string {
	parts := strings.Split(path, ".")
	var resolvedParts []string
	for _, p := range parts {
		if strings.HasPrefix(p, "#") {
			if name, ok := attrNames[p]; ok {
				resolvedParts = append(resolvedParts, name)
			} else {
				resolvedParts = append(resolvedParts, p) // validation error strictly, but fallback
			}
		} else {
			resolvedParts = append(resolvedParts, p)
		}
	}
	return strings.Join(resolvedParts, ".")
}

func setAttributeValue(item map[string]interface{}, path string, value interface{}) {
	parts := strings.Split(path, ".")
	var current = item

	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}

		// We are traversing deep.
		// Check if current[part] exists.
		var nextMap map[string]interface{}

		if existing, ok := current[part]; ok {
			// It expects a Map Attribute Value {"M": ...}
			if m, ok := existing.(map[string]interface{}); ok {
				if inner, isMap := m["M"]; isMap {
					if innerMap, ok := inner.(map[string]interface{}); ok {
						nextMap = innerMap
					}
				}
			}
			if nextMap == nil {
				// Conflict or unexpected structure, abort update (simple strategy)
				return
			}
		} else {
			// Create new Map Attribute Value
			nextMap = make(map[string]interface{})
			current[part] = map[string]interface{}{
				"M": nextMap,
			}
		}
		current = nextMap
	}
}
