package datasync

import (
	"fmt"
	"slices"
	"strings"
)

// matchFilterOperator evaluates one Filter.Operator (types.Operator,
// datasync@v1.61.4 types/enums.go) against a single stored value and the
// filter's Values list (OR within one filter's Values, matching how AWS's
// own filter docs describe "the values that you want to filter for").
func matchFilterOperator(operator, actual string, values []string) (bool, error) {
	switch operator {
	case "", "Equals", "In":
		return slices.Contains(values, actual), nil
	case "NotEquals":
		return !slices.Contains(values, actual), nil
	case "Contains":
		return slices.ContainsFunc(values, func(v string) bool { return strings.Contains(actual, v) }), nil
	case "NotContains":
		return !slices.ContainsFunc(values, func(v string) bool { return strings.Contains(actual, v) }), nil
	case "BeginsWith":
		return slices.ContainsFunc(values, func(v string) bool { return strings.HasPrefix(actual, v) }), nil
	case "LessThan", "LessThanOrEqual", "GreaterThan", "GreaterThanOrEqual":
		return slices.ContainsFunc(values, func(v string) bool { return compareOrdered(operator, actual, v) }), nil
	default:
		return false, fmt.Errorf("%w: unrecognized filter Operator %q", ErrInvalidParameter, operator)
	}
}

func compareOrdered(operator, actual, value string) bool {
	switch operator {
	case "LessThan":
		return actual < value
	case "LessThanOrEqual":
		return actual <= value
	case "GreaterThan":
		return actual > value
	default: // GreaterThanOrEqual
		return actual >= value
	}
}
