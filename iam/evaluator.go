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
	Action    any    `json:"Action"`
	Resource  any    `json:"Resource"`
	Principal any    `json:"Principal,omitempty"`
	Effect    string `json:"Effect"`
}

// statementActions returns the normalized list of actions from a Statement.
func statementActions(s Statement) []string {
	switch v := s.Action.(type) {
	case string:
		return []string{v}
	case []any:
		strs := make([]string, 0, len(v))
		for _, a := range v {
			if str, ok := a.(string); ok {
				strs = append(strs, str)
			}
		}

		return strs
	}

	return nil
}

// statementResources returns the normalized list of resources from a Statement.
func statementResources(s Statement) []string {
	switch v := s.Resource.(type) {
	case string:
		return []string{v}
	case []any:
		strs := make([]string, 0, len(v))
		for _, r := range v {
			if str, ok := r.(string); ok {
				strs = append(strs, str)
			}
		}

		return strs
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
func EvaluatePolicies(policyDocs []string, action, resource string) EvaluationResult {
	result := EvalImplicitDeny

	for _, doc := range policyDocs {
		var pd PolicyDocument
		if err := json.Unmarshal([]byte(doc), &pd); err != nil {
			continue
		}

		for _, stmt := range pd.Statement {
			if !actionMatches(stmt, action) {
				continue
			}

			if !resourceMatches(stmt, resource) {
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

// actionMatches returns true if the statement's Action set includes the given action.
func actionMatches(s Statement, action string) bool {
	for _, a := range statementActions(s) {
		if wildcardMatchCaseInsensitive(a, action) {
			return true
		}
	}

	return false
}

// resourceMatches returns true if the statement's Resource set includes the given resource.
func resourceMatches(s Statement, resource string) bool {
	for _, r := range statementResources(s) {
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
