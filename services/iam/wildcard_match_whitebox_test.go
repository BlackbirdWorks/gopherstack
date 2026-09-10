package iam

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWildcardMatch_Semantics locks down wildcardMatch's existing matching
// behaviour so the gopherstack-it6k allocation-size-overflow hardening below
// cannot silently change what an IAM policy pattern matches.
func TestWildcardMatch_Semantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{name: "exact match", pattern: "abc", value: "abc", want: true},
		{name: "exact no match", pattern: "abc", value: "xyz", want: false},
		{name: "star matches everything", pattern: "*", value: "anything at all", want: true},
		{name: "star matches empty", pattern: "*", value: "", want: true},
		{name: "prefix wildcard match", pattern: "abc*", value: "abcdef", want: true},
		{name: "prefix wildcard no match", pattern: "abc*", value: "xyzdef", want: false},
		{name: "suffix wildcard match", pattern: "*abc", value: "xyzabc", want: true},
		{name: "suffix wildcard no match", pattern: "*abc", value: "abcxyz", want: false},
		{name: "middle wildcard match", pattern: "a*c", value: "abbbbc", want: true},
		{name: "middle wildcard no match", pattern: "a*c", value: "abbbbd", want: false},
		{name: "question mark match", pattern: "a?c", value: "abc", want: true},
		{name: "question mark requires one char", pattern: "a?c", value: "ac", want: false},
		{name: "question mark rejects two chars", pattern: "a?c", value: "abbc", want: false},
		{name: "multiple stars collapse", pattern: "a**b", value: "ab", want: true},
		{name: "multiple stars with content", pattern: "a**b", value: "axxxb", want: true},
		{name: "empty pattern matches empty value", pattern: "", value: "", want: true},
		{name: "empty pattern no match nonempty value", pattern: "", value: "a", want: false},
		{name: "nonempty pattern no match empty value", pattern: "a", value: "", want: false},
		{name: "star pattern no match empty pattern semantics", pattern: "*", value: "x", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, wildcardMatch(tt.pattern, tt.value))
		})
	}
}

// TestWildcardMatch_LongStarPatternStillMatches is the regression test for
// the fail-open bug the length-guard fix would have introduced (rejected in
// review): a Deny statement's Resource/Action pattern is a legitimate,
// stored-policy-sized string (up to maxRolePolicySize=10240 bytes, well
// over any small guard threshold) made entirely of '*'. Real AWS keeps
// matching a pattern like this against any value; a length guard would
// have made wildcardMatch return false past its cap, silently disabling
// the Deny (stmtResourceMatches/stmtActionMatches treat "doesn't match" as
// "this Deny doesn't apply") or, symmetrically, making an oversized
// NotAction/NotResource Allow match MORE broadly. The greedy matcher has no
// such cap: it matches a 5000-star pattern against a short value exactly
// like it would a single "*".
func TestWildcardMatch_LongStarPatternStillMatches(t *testing.T) {
	t.Parallel()

	longStarPattern := strings.Repeat("*", 5000)

	assert.True(t, wildcardMatch(longStarPattern, "s3:GetObject"))
	assert.True(t, wildcardMatch(longStarPattern, ""))
	assert.True(t, wildcardMatch(longStarPattern, strings.Repeat("x", 5000)))
}

// wildcardMatchDPReference is the pre-gopherstack-it6k wildcardMatch body,
// copied verbatim from git (`git show HEAD:services/iam/evaluator.go`,
// before either the rejected length-guard or the greedy rewrite). Kept only
// as a fuzz oracle: the O(len(p)*len(v)) DP table is exactly the allocation
// CodeQL flagged and the memory-exhaustion vector the guard was meant to
// bound, so this must never be reachable from production code -- only from
// the fuzz target below, and only with bounded-size fuzz inputs.
func wildcardMatchDPReference(pattern, value string) bool {
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

// FuzzWildcardMatchMatchesDPReference proves the allocation-free greedy
// wildcardMatch agrees with the original DP implementation for every input,
// not just the hand-picked semantics cases above. Seeded with the semantics
// table plus adversarial glob-matching cases: nested/adjacent stars,
// star-question combinations, the classic "a*a*a*a*b" vs "aaaa...a"
// backtracking-blowup pattern, multi-byte unicode on both sides, and empty
// strings on each side.
func FuzzWildcardMatchMatchesDPReference(f *testing.F) {
	seeds := []struct {
		pattern string
		value   string
	}{
		{"abc", "abc"},
		{"abc", "xyz"},
		{"*", "anything at all"},
		{"*", ""},
		{"abc*", "abcdef"},
		{"abc*", "xyzdef"},
		{"*abc", "xyzabc"},
		{"*abc", "abcxyz"},
		{"a*c", "abbbbc"},
		{"a*c", "abbbbd"},
		{"a?c", "abc"},
		{"a?c", "ac"},
		{"a?c", "abbc"},
		{"a**b", "ab"},
		{"a**b", "axxxb"},
		{"", ""},
		{"", "a"},
		{"a", ""},
		{"*", "x"},
		{"**", "anything"},
		{"**", ""},
		{"*?*", "a"},
		{"*?*", ""},
		{"?*?", "ab"},
		{"?*?", "a"},
		{"****", "x"},
		{"*?*?*", "abcd"},
		{"a*a*a*a*b", "aaaaaaaaaa"},
		{"a*a*a*a*b", "aaaaaaaaaab"},
		{"a*a*a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"日本語*", "日本語です"},
		{"*語です", "日本語です"},
		{"日?語", "日本語"},
		{"*", "日本語🎉"},
		{"🎉*🎉", "🎉middle🎉"},
		{"?", "🎉"},
	}

	for _, s := range seeds {
		f.Add(s.pattern, s.value)
	}

	f.Fuzz(func(t *testing.T, pattern, value string) {
		// The DP reference allocates O(len(pattern)*len(value)); cap fuzz
		// input size so the oracle itself stays cheap (this bound has
		// nothing to do with production behaviour, which the deleted
		// length-guard proved must have no such cap).
		const maxFuzzLen = 200
		if len(pattern) > maxFuzzLen || len(value) > maxFuzzLen {
			t.Skip("input too large for the DP reference oracle")
		}

		got := wildcardMatch(pattern, value)
		want := wildcardMatchDPReference(pattern, value)

		assert.Equal(t, want, got, "pattern=%q value=%q", pattern, value)
	})
}
