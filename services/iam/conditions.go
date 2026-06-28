package iam

import (
	"net"
	"slices"
	"strings"
)

// ctxKeySourceIP is the IAM condition key for the caller's source IP address.
const ctxKeySourceIP = "aws:sourceip"

// ConditionContext holds per-request context values that are resolved against
// IAM policy Condition blocks. All fields are optional; missing keys simply
// fail to match condition operators that require them.
type ConditionContext struct {
	Extra    map[string]string `json:"extra,omitempty"`
	SourceIP string            `json:"sourceIP,omitempty"`
	Username string            `json:"username,omitempty"`
	UserID   string            `json:"userID,omitempty"`
}

// conditionMatches returns true if all condition operators in the map are satisfied
// by the provided context. An empty or nil condition map is always satisfied.
func conditionMatches(condition map[string]map[string]any, ctx ConditionContext) bool {
	for operator, keyValues := range condition {
		if !evalOperator(operator, keyValues, ctx) {
			return false
		}
	}

	return true
}

// evalOperator evaluates a single IAM condition operator block (e.g. "StringEquals": {...}).
// Each key in keyValues is a condition key; each value is either a string or []string.
// All key-value pairs within the block must match (AND semantics within an operator).
func evalOperator(operator string, keyValues map[string]any, ctx ConditionContext) bool {
	op := strings.ToLower(operator)

	for condKey, condVal := range keyValues {
		values := conditionValues(condVal)
		ctxVal := resolveContextKey(condKey, ctx)
		if !evalSingleCondition(op, ctxVal, values) {
			return false
		}
	}

	return true
}

// conditionValues normalises a condition value that may be a single string or
// a JSON array of strings into a []string.
func conditionValues(v any) []string {
	switch val := v.(type) {
	case string:

		return []string{val}
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}

		return out
	}

	return nil
}

// resolveContextKey maps a condition key name to its value from the ConditionContext.
// Keys are normalised to lower-case before lookup.
func resolveContextKey(key string, ctx ConditionContext) string {
	lower := strings.ToLower(key)

	switch lower {
	case ctxKeySourceIP:
		// ContextEntries from SimulatePolicy requests are stored in Extra.
		if ctx.Extra != nil {
			if v, ok := ctx.Extra[ctxKeySourceIP]; ok {
				return v
			}
		}

		return ctx.SourceIP
	case "aws:username":

		return ctx.Username
	case "aws:userid":

		return ctx.UserID
	default:
		if ctx.Extra != nil {
			if v, ok := ctx.Extra[lower]; ok {
				return v
			}
		}

		return ""
	}
}

// evalSingleCondition evaluates one condition key against the resolved context
// value and the required condition values, using the given IAM operator.
// Returns true if the condition is satisfied.
func evalSingleCondition(operator, ctxVal string, condVals []string) bool {
	// IfExists suffix: if the key is missing (empty), condition is always true.
	baseOp, ifExists := strings.CutSuffix(operator, "ifexists")
	if ifExists && ctxVal == "" {
		return true
	}
	// Restore the stripped suffix if we didn't find "ifexists" (CutSuffix gives
	// back the original string when the suffix is absent).
	if !ifExists {
		baseOp = operator
	}

	if result, ok := evalStringCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	if result, ok := evalIPARNCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	// Unknown operator — treat as not matching (conservative default).
	return false
}

// evalStringCondition handles string and bool/null condition operators.
func evalStringCondition(baseOp, ctxVal string, condVals []string) (bool, bool) {
	switch baseOp {
	case "stringequals":

		return anyStringEquals(ctxVal, condVals), true
	case "stringnotequals":

		return !anyStringEquals(ctxVal, condVals), true
	case "stringequalsignorecase":

		return anyStringEquals(strings.ToLower(ctxVal), toLower(condVals)), true
	case "stringnotequalsignorecase":

		return !anyStringEquals(strings.ToLower(ctxVal), toLower(condVals)), true
	case "stringlike":

		return anyStringLike(ctxVal, condVals), true
	case "stringnotlike":

		return !anyStringLike(ctxVal, condVals), true
	case "bool":

		return anyStringEquals(strings.ToLower(ctxVal), toLower(condVals)), true
	case "null":

		return evalNullCondition(ctxVal, condVals), true
	}

	return false, false
}

// evalNullCondition evaluates the "null" IAM condition operator.
func evalNullCondition(ctxVal string, condVals []string) bool {
	isNull := ctxVal == ""
	for _, v := range condVals {
		if strings.EqualFold(v, "true") && isNull {
			return true
		}
		if strings.EqualFold(v, "false") && !isNull {
			return true
		}
	}

	return false
}

// evalIPARNCondition handles IP address and ARN condition operators.
func evalIPARNCondition(baseOp, ctxVal string, condVals []string) (bool, bool) {
	switch baseOp {
	case "ipaddress":

		return anyIPMatch(ctxVal, condVals), true
	case "notipaddress":

		return !anyIPMatch(ctxVal, condVals), true
	case "arnequals", "arnlike":

		return anyStringLike(strings.ToLower(ctxVal), toLower(condVals)), true
	case "arnnotequals", "arnnotlike":

		return !anyStringLike(strings.ToLower(ctxVal), toLower(condVals)), true
	}

	return false, false
}

// anyStringEquals returns true if ctxVal equals any value in condVals.
func anyStringEquals(ctxVal string, condVals []string) bool {
	return slices.Contains(condVals, ctxVal)
}

// anyStringLike returns true if ctxVal matches any wildcard pattern in condVals.
func anyStringLike(ctxVal string, condVals []string) bool {
	for _, pattern := range condVals {
		if wildcardMatch(pattern, ctxVal) {
			return true
		}
	}

	return false
}

// anyIPMatch returns true if ctxVal is an IP address that falls within any of
// the CIDR ranges (or equals any IP address literal) in condVals.
func anyIPMatch(ctxVal string, condVals []string) bool {
	ip := net.ParseIP(ctxVal)

	for _, v := range condVals {
		if strings.Contains(v, "/") {
			_, ipNet, err := net.ParseCIDR(v)
			if err == nil && ip != nil && ipNet.Contains(ip) {
				return true
			}
		} else if v == ctxVal {
			return true
		}
	}

	return false
}

// toLower returns a new slice with all strings lower-cased.
func toLower(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.ToLower(s)
	}

	return out
}
