package iam

import (
	"bytes"
	"encoding/base64"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ctxKeySourceIP is the IAM condition key for the caller's source IP address.
const ctxKeySourceIP = "aws:sourceip"

// ConditionContext holds per-request context values that are resolved against
// IAM policy Condition blocks. All fields are optional; missing keys simply
// fail to match condition operators that require them.
type ConditionContext struct {
	Extra map[string]string `json:"extra,omitempty"`
	// PrincipalTags are the tags on the calling principal (IAM user/role),
	// exposed to policies as ${aws:PrincipalTag/<key>} and the
	// aws:PrincipalTag/<key> condition key.
	PrincipalTags map[string]string `json:"principalTags,omitempty"`
	// RequestTags are the tags supplied in the current request (e.g. a
	// CreateUser Tags parameter), exposed as ${aws:RequestTag/<key>} and the
	// aws:RequestTag/<key> condition key.
	RequestTags map[string]string `json:"requestTags,omitempty"`
	SourceIP    string            `json:"sourceIP,omitempty"`
	Username    string            `json:"username,omitempty"`
	UserID      string            `json:"userID,omitempty"`
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
		if v, ok := resolveTagKey(lower, ctx); ok {
			return v
		}
		if ctx.Extra != nil {
			if v, ok := ctx.Extra[lower]; ok {
				return v
			}
		}

		return ""
	}
}

// resolveTagKey resolves the aws:PrincipalTag/<key> and aws:RequestTag/<key>
// condition keys (already lower-cased) from the context tag maps. The second
// return value is true only when the tag is present in the corresponding map;
// otherwise the caller falls through to Extra (which is how simulated
// ContextEntries supply tag values).
func resolveTagKey(lower string, ctx ConditionContext) (string, bool) {
	if k, ok := strings.CutPrefix(lower, "aws:principaltag/"); ok {
		return lookupTag(ctx.PrincipalTags, k)
	}
	if k, ok := strings.CutPrefix(lower, "aws:requesttag/"); ok {
		return lookupTag(ctx.RequestTags, k)
	}

	return "", false
}

// lookupTag performs a case-insensitive lookup of key in tags. IAM tag keys are
// case-sensitive in storage, but condition/variable references arrive
// lower-cased, so we match case-insensitively for practical parity.
func lookupTag(tags map[string]string, key string) (string, bool) {
	for k, v := range tags {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}

	return "", false
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

	// Set qualifiers operate over a multi-valued context key. gopherstack
	// serialises multi-valued ContextEntries as a comma-joined string, so we
	// split back into the value set here.
	if rest, ok := strings.CutPrefix(baseOp, "forallvalues:"); ok {
		return evalForAllValues(rest, ctxVal, condVals)
	}
	if rest, ok := strings.CutPrefix(baseOp, "foranyvalue:"); ok {
		return evalForAnyValue(rest, ctxVal, condVals)
	}

	return evalBaseOperator(baseOp, ctxVal, condVals)
}

// evalBaseOperator dispatches a single (non-set-qualified) IAM condition
// operator to the matching operator family and returns whether it matched.
// Unknown operators conservatively return false.
func evalBaseOperator(baseOp, ctxVal string, condVals []string) bool {
	if result, ok := evalStringCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	if result, ok := evalIPARNCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	if result, ok := evalNumericCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	if result, ok := evalDateCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	if result, ok := evalBinaryCondition(baseOp, ctxVal, condVals); ok {
		return result
	}
	// Unknown operator — treat as not matching (conservative default).
	return false
}

// splitContextSet splits a comma-joined multi-valued context key into its
// individual values. An empty context value yields an empty set.
func splitContextSet(ctxVal string) []string {
	if ctxVal == "" {
		return nil
	}

	return strings.Split(ctxVal, ",")
}

// evalForAllValues implements the ForAllValues: set qualifier: the condition is
// satisfied only if every value of the multi-valued context key matches the
// base operator. An empty context set is vacuously true (AWS semantics).
func evalForAllValues(baseOp, ctxVal string, condVals []string) bool {
	for _, v := range splitContextSet(ctxVal) {
		if !evalBaseOperator(baseOp, v, condVals) {
			return false
		}
	}

	return true
}

// evalForAnyValue implements the ForAnyValue: set qualifier: the condition is
// satisfied if at least one value of the multi-valued context key matches the
// base operator. An empty context set is false (AWS semantics).
func evalForAnyValue(baseOp, ctxVal string, condVals []string) bool {
	for _, v := range splitContextSet(ctxVal) {
		if evalBaseOperator(baseOp, v, condVals) {
			return true
		}
	}

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

// evalNumericCondition handles numeric operators.
func evalNumericCondition(baseOp, ctxVal string, condVals []string) (bool, bool) {
	switch baseOp {
	case "numericequals",
		"numericnotequals",
		"numericlessthan",
		"numericlessthanequals",
		"numericgreaterthan",
		"numericgreaterthanequals":
		ctxNum, err := strconv.ParseFloat(ctxVal, 64)
		if err != nil {
			return false, true // invalid number => no match
		}
		for _, v := range condVals {
			condNum, errParse := strconv.ParseFloat(v, 64)
			if errParse != nil {
				continue
			}
			match := false
			switch baseOp {
			case "numericequals":
				match = ctxNum == condNum
			case "numericnotequals":
				match = ctxNum != condNum
			case "numericlessthan":
				match = ctxNum < condNum
			case "numericlessthanequals":
				match = ctxNum <= condNum
			case "numericgreaterthan":
				match = ctxNum > condNum
			case "numericgreaterthanequals":
				match = ctxNum >= condNum
			}
			if match {
				return true, true
			}
		}

		return false, true
	}

	return false, false
}

// evalDateCondition handles date operators.
func evalDateCondition(baseOp, ctxVal string, condVals []string) (bool, bool) {
	switch baseOp {
	case "dateequals",
		"datenotequals",
		"datelessthan",
		"datelessthanequals",
		"dategreaterthan",
		"dategreaterthanequals":
		ctxTime, err := time.Parse(time.RFC3339, ctxVal)
		if err != nil {
			return false, true
		}
		for _, v := range condVals {
			condTime, errParse := time.Parse(time.RFC3339, v)
			if errParse != nil {
				continue
			}
			match := false
			switch baseOp {
			case "dateequals":
				match = ctxTime.Equal(condTime)
			case "datenotequals":
				match = !ctxTime.Equal(condTime)
			case "datelessthan":
				match = ctxTime.Before(condTime)
			case "datelessthanequals":
				match = ctxTime.Before(condTime) || ctxTime.Equal(condTime)
			case "dategreaterthan":
				match = ctxTime.After(condTime)
			case "dategreaterthanequals":
				match = ctxTime.After(condTime) || ctxTime.Equal(condTime)
			}
			if match {
				return true, true
			}
		}

		return false, true
	}

	return false, false
}

// evalBinaryCondition handles binary operators.
func evalBinaryCondition(baseOp, ctxVal string, condVals []string) (bool, bool) {
	if baseOp == "binaryequals" {
		ctxBytes, err := base64.StdEncoding.DecodeString(ctxVal)
		if err != nil {
			return false, true
		}
		for _, v := range condVals {
			condBytes, errParse := base64.StdEncoding.DecodeString(v)
			if errParse == nil && bytes.Equal(ctxBytes, condBytes) {
				return true, true
			}
		}

		return false, true
	}

	return false, false
}
