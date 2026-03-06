package iam

import (
	"encoding/json"
	"strings"
)

// PolicyDocument is the parsed representation of an IAM policy JSON document.
type PolicyDocument struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// Statement represents a single IAM policy statement.
type Statement struct {
	// Action can be a single string or a list of strings.
	Action any `json:"Action,omitempty"`
	// NotAction matches any action NOT in this set (logical negation of Action).
	NotAction any `json:"NotAction,omitempty"`
	// Resource can be a single string or a list of strings.
	Resource any `json:"Resource,omitempty"`
	// NotResource matches any resource NOT in this set (logical negation of Resource).
	NotResource any `json:"NotResource,omitempty"`
	// Condition is a map of operator → contextKey → value(s).
	Condition map[string]map[string]any `json:"Condition,omitempty"`
	// Principal is ignored in enforcement; stored for completeness.
	Principal any    `json:"Principal,omitempty"`
	Effect    string `json:"Effect"`
}

// anyStrings normalises an IAM field that can be either a single JSON string
// or a JSON array of strings into a []string.
func anyStrings(v any) []string {
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

// EvaluationResult is the outcome of IAM policy evaluation.
type EvaluationResult int

const (
	// EvalImplicitDeny means no Allow statement matched — access is denied by default.
	EvalImplicitDeny EvaluationResult = iota
	// EvalAllow means an Allow statement matched and no Deny overrode it.
	EvalAllow
	// EvalExplicitDeny means an explicit Deny statement matched — access is denied.
	EvalExplicitDeny
)

// EvaluatePolicies evaluates a set of policy document JSON strings against a requested
// action and resource. Returns EvalAllow if any Allow statement matches the action and
// resource and no Deny statement matches. Returns EvalExplicitDeny if any Deny statement
// matches. Returns EvalImplicitDeny if no Allow statement matches.
//
// The action is case-insensitive and supports wildcards (* and ?).
// The resource supports wildcards (* and ?).
//
// ctx carries request-derived context values used to evaluate Condition blocks
// and to substitute policy variables. Pass an empty ConditionContext{} when
// no conditions or variables are needed.
func EvaluatePolicies(policyDocs []string, action, resource string, ctx ConditionContext) EvaluationResult {
	result := EvalImplicitDeny

	for _, doc := range policyDocs {
		expanded := SubstituteVariables(doc, ctx)

		var pd PolicyDocument
		if err := json.Unmarshal([]byte(expanded), &pd); err != nil {
			continue
		}

		for _, stmt := range pd.Statement {
			if !stmtActionMatches(stmt, action) {
				continue
			}

			if !stmtResourceMatches(stmt, resource) {
				continue
			}

			if !conditionMatches(stmt.Condition, ctx) {
				continue
			}

			switch strings.ToUpper(stmt.Effect) {
			case "DENY":
				return EvalExplicitDeny
			case "ALLOW":
				result = EvalAllow
			}
		}
	}

	return result
}

// stmtActionMatches returns true if the statement's Action/NotAction covers the given action.
func stmtActionMatches(s Statement, action string) bool {
	if s.NotAction != nil {
		// NotAction: matches when the action is NOT in the list.
		for _, a := range anyStrings(s.NotAction) {
			if wildcardMatchCaseInsensitive(a, action) {
				return false
			}
		}

		return true
	}

	// Standard Action field.
	for _, a := range anyStrings(s.Action) {
		if wildcardMatchCaseInsensitive(a, action) {
			return true
		}
	}

	return false
}

// stmtResourceMatches returns true if the statement's Resource/NotResource covers the given resource.
func stmtResourceMatches(s Statement, resource string) bool {
	if s.NotResource != nil {
		// NotResource: matches when the resource is NOT in the list.
		for _, r := range anyStrings(s.NotResource) {
			if wildcardMatch(r, resource) {
				return false
			}
		}

		return true
	}

	// Standard Resource field.
	for _, r := range anyStrings(s.Resource) {
		if wildcardMatch(r, resource) {
			return true
		}
	}

	return false
}

// wildcardMatchCaseInsensitive performs case-insensitive wildcard matching.
func wildcardMatchCaseInsensitive(pattern, value string) bool {
	return wildcardMatch(strings.ToLower(pattern), strings.ToLower(value))
}

// wildcardMatch performs wildcard pattern matching supporting * (zero or more chars) and ? (one char).
// Both pattern and value are expected to already be in the desired case.
func wildcardMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}

	p := []rune(pattern)
	v := []rune(value)

	// dp[i][j] = true if p[:i] matches v[:j]
	dp := make([][]bool, len(p)+1)
	for i := range dp {
		dp[i] = make([]bool, len(v)+1)
	}

	dp[0][0] = true

	for i := 1; i <= len(p); i++ {
		if p[i-1] == '*' {
			dp[i][0] = dp[i-1][0]
		}
	}

	for i := 1; i <= len(p); i++ {
		for j := 1; j <= len(v); j++ {
			switch p[i-1] {
			case '*':
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			case '?', v[j-1]:
				dp[i][j] = dp[i-1][j-1]
			}
		}
	}

	return dp[len(p)][len(v)]
}
