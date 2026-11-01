package dynamoattr

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamonum"
)

// UnwrapAttributeValue converts a DynamoDB wire attribute map into a bare value when possible.
//
// Direct lookups instead of ranging: a wire map has exactly one type key, and a
// map iterator costs more than checking the (overwhelmingly common) S/N keys directly.
func UnwrapAttributeValue(v any) any {
	m, isMap := v.(map[string]any)
	if !isMap || len(m) == 0 {
		return v
	}

	if val, ok := m["S"]; ok {
		return val
	}
	if val, ok := m["N"]; ok {
		return val
	}

	return unwrapAttributeValueRare(m, v)
}

func unwrapAttributeValueRare(m map[string]any, v any) any {
	if val, ok := m["B"]; ok {
		return val
	}
	if val, ok := m["BOOL"]; ok {
		return val
	}
	if _, ok := m["NULL"]; ok {
		return nil
	}
	if val, ok := m["M"]; ok {
		return val
	}
	if val, ok := m["L"]; ok {
		return val
	}
	if val, ok := m["SS"]; ok {
		return val
	}
	if val, ok := m["NS"]; ok {
		return val
	}
	if val, ok := m["BS"]; ok {
		return val
	}

	return v
}

// ParseNumeric tries to read a value as an exact DynamoDB number.
func ParseNumeric(v any) (dynamonum.Num, bool) {
	unwrapped := UnwrapAttributeValue(v)
	switch val := unwrapped.(type) {
	case float64:
		return dynamonum.FromFloat(val)
	case int:
		return dynamonum.FromInt64(int64(val)), true
	case int64:
		return dynamonum.FromInt64(val), true
	case string:
		if x, err := dynamonum.Parse(val); err == nil {
			return x, true
		}
	}

	return dynamonum.Num{}, false
}

// ResolveValue maps expression tokens to attribute values when prefixed with ':'.
func ResolveValue(token string, attrValues map[string]any) any {
	if strings.HasPrefix(token, ":") {
		if val, ok := attrValues[token]; ok {
			return val
		}
	}

	return token
}

// CompareValues compares DynamoDB attribute values against an operator.
// Two numeric operands compare exactly; anything else compares as strings.
func CompareValues(lhs any, op string, rhs any) bool {
	lhs = UnwrapAttributeValue(lhs)
	rhs = UnwrapAttributeValue(rhs)

	var c int

	if lNum, ok := ParseNumeric(lhs); ok {
		if rNum, ok2 := ParseNumeric(rhs); ok2 {
			c = lNum.Cmp(rNum)

			return cmpResult(op, c)
		}
	}

	return cmpResult(op, strings.Compare(ToString(lhs), ToString(rhs)))
}

func cmpResult(op string, c int) bool {
	switch op {
	case "=":
		return c == 0
	case "<>":
		return c != 0
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	case ">=":
		return c >= 0
	}

	return false
}

// SplitANDConditions splits an expression by " AND " while preserving BETWEEN ... AND ... clauses.
func SplitANDConditions(expression string) []string {
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

// ToString renders DynamoDB attribute values into comparable strings.
func ToString(v any) string {
	unwrapped := UnwrapAttributeValue(v)
	switch s := unwrapped.(type) {
	case string:
		return s
	case bool:
		return strconv.FormatBool(s)
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64)
	case int:
		return strconv.Itoa(s)
	case int64:
		return strconv.FormatInt(s, 10)
	case int32:
		return strconv.FormatInt(int64(s), 10)
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
