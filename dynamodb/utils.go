package dynamodb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func unwrapAttributeValue(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}

	// DynamoDB AttributeValue types
	if val, exists := m["S"]; exists {
		return val
	}
	if val, exists := m["N"]; exists {
		return val
	}
	if val, exists := m["B"]; exists {
		return val
	}
	if val, exists := m["BOOL"]; exists {
		return val
	}
	if _, exists := m["NULL"]; exists {
		return nil
	}
	if val, exists := m["M"]; exists {
		return val
	}
	if val, exists := m["L"]; exists {
		return val
	}

	// Also handle sets if encountered (SS, NS, BS)
	if val, exists := m["SS"]; exists {
		return val
	}
	if val, exists := m["NS"]; exists {
		return val
	}
	if val, exists := m["BS"]; exists {
		return val
	}

	return v
}

func parseStr(v any) string {
	return toString(v)
}

func parseNumeric(v any) (float64, bool) {
	unwrapped := unwrapAttributeValue(v)
	switch val := unwrapped.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}

	return 0, false
}

func resolveValue(token string, attrValues map[string]any) any {
	if strings.HasPrefix(token, ":") {
		if val, ok := attrValues[token]; ok {
			return val
		}
	}

	return token
}

func compareValues(lhs any, op string, rhs any) bool {
	lhs = unwrapAttributeValue(lhs)
	rhs = unwrapAttributeValue(rhs)

	lhsStr := toString(lhs)
	rhsStr := toString(rhs)
	lNum, lIsNum := parseNumeric(lhs)
	rNum, rIsNum := parseNumeric(rhs)

	switch op {
	case "=":
		return lhsStr == rhsStr
	case "<>":
		return lhsStr != rhsStr
	case "<":
		return compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a < b }, func(a, b string) bool { return a < b })
	case ">":
		return compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a > b }, func(a, b string) bool { return a > b })
	case "<=":
		return compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a <= b }, func(a, b string) bool { return a <= b })
	case ">=":
		return compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a >= b }, func(a, b string) bool { return a >= b })
	}

	return false
}

func compareOrdered(lNum, rNum float64, lIsNum, rIsNum bool, lStr, rStr string, numCmp func(float64, float64) bool, strCmp func(string, string) bool) bool {
	if lIsNum && rIsNum {
		return numCmp(lNum, rNum)
	}

	return strCmp(lStr, rStr)
}

// splitANDConditions splits an expression by " AND " while preserving BETWEEN ... AND ... clauses.
// Note: This is legacy but needed by Query PK/SK splitting logic.
func splitANDConditions(expression string) []string {
	rawParts := strings.Split(expression, " AND ")
	conditions := make([]string, 0, len(rawParts))

	for i := 0; i < len(rawParts); i++ {
		part := rawParts[i]
		if strings.Contains(part, " BETWEEN ") && i+1 < len(rawParts) {
			part = part + " AND " + rawParts[i+1]
			i++
		}

		conditions = append(conditions, part)
	}

	return conditions
}

func toString(v any) string {
	unwrapped := unwrapAttributeValue(v)
	switch s := unwrapped.(type) {
	case string:
		return s
	case bool:
		return strconv.FormatBool(s)
	case float64:
		return fmt.Sprintf("%v", s)
	case int, int64, int32:
		return fmt.Sprintf("%v", s)
	case nil:
		return ""
	default:
		b, err := json.Marshal(s)
		if err != nil {
			return fmt.Sprintf("%v", s)
		}

		return string(b)
	}
}
