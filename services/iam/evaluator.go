package iam

import (
	"encoding/json"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

// PolicyDocument is the parsed representation of an IAM policy JSON document.
type PolicyDocument struct {
	Version   string      `json:"Version,omitempty"`
	Statement []Statement `json:"Statement,omitempty"`
}

// Statement represents a single IAM policy statement.
type Statement struct {
	// Action can be a single string or a list of strings.
	Action any `json:"Action"`
	// NotAction matches any action NOT in this set (logical negation of Action).
	NotAction any `json:"NotAction"`
	// Resource can be a single string or a list of strings.
	Resource any `json:"Resource"`
	// NotResource matches any resource NOT in this set (logical negation of Resource).
	NotResource any `json:"NotResource"`
	// Condition is a map of operator → contextKey → value(s).
	Condition map[string]map[string]any `json:"Condition,omitempty"`
	// Principal is ignored in enforcement; stored for completeness.
	Principal any    `json:"Principal"`
	Effect    string `json:"Effect,omitempty"`
}

// parsedPolicyCache memoizes json.Unmarshal of a policy document string, keyed
// by the exact text that was parsed. A policy document string is immutable
// once stored — PutUserPolicy/CreatePolicyVersion/SetDefaultPolicyVersion/
// attach/detach all produce a *different* document string rather than
// mutating one in place — so the string itself is a safe cache key that
// needs no invalidation: a changed policy is a cache miss on a new key, not
// a stale hit on the old one. EvaluatePolicies keys on the
// post-SubstituteVariables text, not the stored template, so two callers
// whose ${aws:username}-style variables expand differently never collide.
//
// The parsed PolicyDocument is returned by value but its Statement slice and
// any nested maps are shared across callers unmutated — every consumer of a
// cache hit (EvaluatePolicies' loop below) only reads it, never mutates it.
//
//nolint:gochecknoglobals // process-lifetime memoization cache; concurrency-safe via safemap.Map
var parsedPolicyCache = safemap.New[string, *cachedPolicyDoc]("iam.evaluator.parsedPolicyCache")

// cachedPolicyDoc is one parsedPolicyCache entry. ok mirrors json.Unmarshal's
// error as a bool: EvaluatePolicies silently skips a malformed document, so a
// parse failure must be memoized too, or a persistently-malformed document
// would re-parse (and re-fail) on every call.
type cachedPolicyDoc struct {
	doc PolicyDocument
	ok  bool
}

// parsePolicyDocumentCached parses expanded, using parsedPolicyCache to skip
// the json.Unmarshal on a repeat call with the same text.
func parsePolicyDocumentCached(expanded string) (PolicyDocument, bool) {
	if cached, found := parsedPolicyCache.Get(expanded); found {
		return cached.doc, cached.ok
	}

	var pd PolicyDocument

	err := json.Unmarshal([]byte(expanded), &pd)
	entry := &cachedPolicyDoc{doc: pd, ok: err == nil}
	parsedPolicyCache.Set(expanded, entry)

	return entry.doc, entry.ok
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

		pd, ok := parsePolicyDocumentCached(expanded)
		if !ok {
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
//
// Allocation-free greedy glob match: O(len(p)+len(v)) time, O(1) space. The
// former DP table allocated O(len(p)*len(v)) from caller-supplied
// SimulateCustomPolicy input (gopherstack-it6k). starIdx/matchIdx backtrack:
// on a mismatch after a '*', retry it as having consumed one more value rune.
// Equivalence with the old DP is fuzz-tested in
// wildcard_match_whitebox_test.go (FuzzWildcardMatchMatchesDPReference).
func wildcardMatch(pattern, value string) bool {
	p := []rune(pattern)
	v := []rune(value)

	var pIdx, vIdx int

	starIdx, matchIdx := -1, 0

	for vIdx < len(v) {
		switch {
		// '*' must be checked before the literal/'?' case below: it is
		// always a metacharacter, even when v[vIdx] happens to be the
		// literal rune '*' too (p[pIdx] == v[vIdx] would otherwise match
		// it as a literal '*' instead of opening a wildcard run).
		case pIdx < len(p) && p[pIdx] == '*':
			starIdx = pIdx
			matchIdx = vIdx
			pIdx++
		case pIdx < len(p) && (p[pIdx] == '?' || p[pIdx] == v[vIdx]):
			pIdx++
			vIdx++
		case starIdx != -1:
			pIdx = starIdx + 1
			matchIdx++
			vIdx = matchIdx
		default:
			return false
		}
	}

	for pIdx < len(p) && p[pIdx] == '*' {
		pIdx++
	}

	return pIdx == len(p)
}
