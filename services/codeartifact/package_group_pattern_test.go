package codeartifact //nolint:testpackage // needs access to the unexported pattern-matching engine.

// Internal (white-box) test file for package_group_pattern.go's unexported
// matching engine. Every other test file in this package is external
// (package codeartifact_test) and exercises the HTTP/backend surface; this
// one alone needs direct access to parseGroupPattern/matches/specificityRank/
// isProperSubsetPattern to pin down the AWS pattern-matching algorithm's
// edge cases precisely (see package_group_pattern.go's doc comment for the
// spec this implements).

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGroupPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pattern    string
		wantValue  string
		wantSegs   []string
		wantTarget groupPatternTarget
		wantType   groupMatchType
		wantErr    bool
	}{
		{
			name:       "all_formats_wildcard",
			pattern:    "/*",
			wantTarget: groupTargetFormat,
			wantType:   groupMatchWildcard,
			wantValue:  "",
			wantSegs:   []string{},
		},
		{
			name:       "specific_format",
			pattern:    "/npm/*",
			wantTarget: groupTargetNamespace,
			wantType:   groupMatchWildcard,
			wantValue:  "",
			wantSegs:   []string{"npm"},
		},
		{
			name:       "format_and_namespace_prefix",
			pattern:    "/maven/com.anycompany~",
			wantTarget: groupTargetNamespace,
			wantType:   groupMatchPrefix,
			wantValue:  "com.anycompany",
			wantSegs:   []string{"maven"},
		},
		{
			name:       "format_and_namespace_exact",
			pattern:    "/npm/space/*",
			wantTarget: groupTargetName,
			wantType:   groupMatchWildcard,
			wantValue:  "",
			wantSegs:   []string{"npm", "space"},
		},
		{
			name:       "format_namespace_name_prefix",
			pattern:    "/npm/space/anycompany-ui~",
			wantTarget: groupTargetName,
			wantType:   groupMatchPrefix,
			wantValue:  "anycompany-ui",
			wantSegs:   []string{"npm", "space"},
		},
		{
			name:       "full_exact_match",
			pattern:    "/maven/org.apache.logging.log4j/log4j-core$",
			wantTarget: groupTargetName,
			wantType:   groupMatchExact,
			wantValue:  "log4j-core",
			wantSegs:   []string{"maven", "org.apache.logging.log4j"},
		},
		{
			name:       "blank_namespace_for_python",
			pattern:    "/python//requests$",
			wantTarget: groupTargetName,
			wantType:   groupMatchExact,
			wantValue:  "requests",
			wantSegs:   []string{"python", ""},
		},
		{name: "empty_string", pattern: "", wantErr: true},
		{name: "no_leading_slash", pattern: "npm/*", wantErr: true},
		{name: "too_short", pattern: "/", wantErr: true},
		{name: "too_many_components", pattern: "/npm/ns/name/extra$", wantErr: true},
		{name: "missing_suffix", pattern: "/npm/space/foo", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := parseGroupPattern(tt.pattern)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTarget, p.target)
			assert.Equal(t, tt.wantType, p.matchType)
			assert.Equal(t, tt.wantValue, p.value)
			assert.Equal(t, tt.wantSegs, p.segments)
		})
	}
}

func TestGroupPattern_Matches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		pattern                    string
		format, namespace, pkgName string
		want                       bool
	}{
		{name: "root_matches_anything", pattern: "/*", format: "npm", namespace: "", pkgName: "react", want: true},
		{name: "format_wildcard_matches_same_format", pattern: "/npm/*", format: "npm", pkgName: "react", want: true},
		{
			name: "format_wildcard_rejects_other_format", pattern: "/npm/*",
			format: "pypi", pkgName: "boto3", want: false,
		},
		{
			name: "namespace_prefix_matches", pattern: "/maven/com.anycompany~",
			format: "maven", namespace: "com.anycompany.utils", pkgName: "core", want: true,
		},
		{
			name: "namespace_prefix_rejects_non_boundary", pattern: "/maven/com.anycompany~",
			format: "maven", namespace: "com.anycompanyplus", pkgName: "core", want: false,
		},
		{
			name: "namespace_exact_matches_any_name", pattern: "/npm/space/*",
			format: "npm", namespace: "space", pkgName: "anything", want: true,
		},
		{
			name: "namespace_exact_rejects_other_namespace", pattern: "/npm/space/*",
			format: "npm", namespace: "other", pkgName: "anything", want: false,
		},
		{
			name: "name_prefix_matches", pattern: "/npm/space/anycompany-ui~",
			format: "npm", namespace: "space", pkgName: "anycompany-ui-components", want: true,
		},
		{
			name: "name_prefix_rejects_non_boundary_food", pattern: "/npm/space/foo~",
			format: "npm", namespace: "space", pkgName: "food", want: false,
		},
		{
			name: "name_prefix_accepts_boundary_dash", pattern: "/npm/space/foo~",
			format: "npm", namespace: "space", pkgName: "foo-bar", want: true,
		},
		{
			name: "full_exact_matches", pattern: "/maven/org.apache.logging.log4j/log4j-core$",
			format: "maven", namespace: "org.apache.logging.log4j", pkgName: "log4j-core", want: true,
		},
		{
			name: "full_exact_rejects_different_name", pattern: "/maven/org.apache.logging.log4j/log4j-core$",
			format: "maven", namespace: "org.apache.logging.log4j", pkgName: "log4j-api", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := parseGroupPattern(tt.pattern)
			require.NoError(t, err)
			assert.Equal(t, tt.want, p.matches(tt.format, tt.namespace, tt.pkgName))
		})
	}
}

func TestNormalizeWeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "casefold", input: "AsyncStorage", want: "asyncstorage"},
		{name: "dash_to_dot", input: "foo-bar", want: "foo.bar"},
		{name: "dot_unchanged", input: "foo.bar", want: "foo.bar"},
		{name: "underscore_to_dot", input: "foo_bar", want: "foo.bar"},
		{name: "run_of_separators_collapses", input: "foo..bar", want: "foo.bar"},
		{name: "mixed_run_collapses", input: "foo-._bar", want: "foo.bar"},
		{name: "no_separator_stays_distinct", input: "foobar", want: "foobar"},
		{name: "empty", input: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, normalizeWeak(tt.input))
		})
	}
}

// TestGroupPattern_MatchesWeak pins AWS's documented weak-match examples
// from "Strong and weak match" / "Additional variations": case variations
// and dash/dot/underscore-run variations of a pattern's value weak-match
// (but do not strong-match) that pattern.
func TestGroupPattern_MatchesWeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		pattern                    string
		format, namespace, pkgName string
		wantStrong, wantWeak       bool
	}{
		{
			name: "exact_match_is_strong_and_weak", pattern: "/npm//anycompany-spicy-client$",
			format: "npm", pkgName: "anycompany-spicy-client", wantStrong: true, wantWeak: true,
		},
		{
			name: "case_variation_is_weak_only", pattern: "/npm//anycompany-spicy-client$",
			format: "npm", pkgName: "AnyCompany-Spicy-Client", wantStrong: false, wantWeak: true,
		},
		{
			name: "dash_dot_variation_is_weak_only", pattern: "/npm//my-package$",
			format: "npm", pkgName: "my.package", wantStrong: false, wantWeak: true,
		},
		{
			name: "underscore_variation_is_weak_only", pattern: "/npm//my-package$",
			format: "npm", pkgName: "my_package", wantStrong: false, wantWeak: true,
		},
		{
			name: "doubled_dash_variation_is_weak_only", pattern: "/npm//my-package$",
			format: "npm", pkgName: "my--package", wantStrong: false, wantWeak: true,
		},
		{
			name: "removed_separator_does_not_match", pattern: "/npm//my-package$",
			format: "npm", pkgName: "mypackage", wantStrong: false, wantWeak: false,
		},
		{
			name: "different_package_does_not_weak_match", pattern: "/npm//my-package$",
			format: "npm", pkgName: "other-package", wantStrong: false, wantWeak: false,
		},
		{
			name: "prefix_weak_match_case", pattern: "/npm/space/foo~",
			format: "npm", namespace: "space", pkgName: "FOO-bar", wantStrong: false, wantWeak: true,
		},
		{
			name: "wildcard_always_strong", pattern: "/npm/*",
			format: "npm", pkgName: "Anything", wantStrong: true, wantWeak: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := parseGroupPattern(tt.pattern)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStrong, p.matches(tt.format, tt.namespace, tt.pkgName), "strong match")
			assert.Equal(t, tt.wantWeak, p.matchesWeak(tt.format, tt.namespace, tt.pkgName), "weak match")
		})
	}
}

func TestIsProperSubsetPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{name: "namespace_subset_of_format", a: "/npm/space/*", b: "/npm/*", want: true},
		{name: "format_not_subset_of_namespace", a: "/npm/*", b: "/npm/space/*", want: false},
		{name: "root_is_superset_of_everything", a: "/npm/space/react$", b: "/*", want: true},
		{name: "unrelated_formats_not_subset", a: "/npm/*", b: "/pypi/*", want: false},
		{
			name: "exact_subset_of_prefix_same_target", a: "/npm/space/foo$", b: "/npm/space/foo~", want: true,
		},
		{
			name: "longer_prefix_subset_of_shorter_prefix",
			a:    "/npm/space/anycompany-ui~", b: "/npm/space/anycompany~", want: true,
		},
		{
			name: "shorter_prefix_not_subset_of_longer_prefix",
			a:    "/npm/space/anycompany~", b: "/npm/space/anycompany-ui~", want: false,
		},
		{name: "wildcard_not_subset_of_prefix", a: "/npm/space/*", b: "/npm/space/foo~", want: false},
		{name: "nothing_subset_of_exact_at_same_target", a: "/npm/space/foo~", b: "/npm/space/bar$", want: false},
		{
			name: "different_literal_prefix_segment_blocks_subset",
			a:    "/npm/spaceX/react$", b: "/npm/space/*", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := parseGroupPattern(tt.a)
			require.NoError(t, err)
			pb, err := parseGroupPattern(tt.b)
			require.NoError(t, err)

			assert.Equal(t, tt.want, isProperSubsetPattern(pa, pb))
		})
	}
}

func TestGroupPattern_SpecificityRankOrdering(t *testing.T) {
	t.Parallel()

	// Every pattern in this list must be strictly more specific (higher
	// rank) than the one before it, mirroring the documented hierarchy
	// /* > /npm/* > /npm/space/* > /npm/space/anycompany~ > /npm/space/anycompany-ui~ > /npm/space/foo$.
	patterns := []string{
		"/*",
		"/npm/*",
		"/npm/space/*",
		"/npm/space/anycompany~",
		"/npm/space/anycompany-ui~",
		"/npm/space/foo$",
	}

	var prevRank int

	for i, pat := range patterns {
		p, err := parseGroupPattern(pat)
		require.NoError(t, err)

		rank := p.specificityRank()
		if i > 0 {
			assert.Greater(t, rank, prevRank, "pattern %q should rank more specific than %q", pat, patterns[i-1])
		}
		prevRank = rank
	}
}

func TestHasPrefixWordBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, s, prefix string
		want            bool
	}{
		{name: "exact_match", s: "foo", prefix: "foo", want: true},
		{name: "dash_boundary", s: "foo-bar", prefix: "foo", want: true},
		{name: "no_boundary_letters", s: "food", prefix: "foo", want: false},
		{name: "no_boundary_letters_foot", s: "foot", prefix: "foo", want: false},
		{name: "not_a_prefix", s: "bar", prefix: "foo", want: false},
		// An empty prefix immediately followed by a word character is not a
		// word boundary (the word "anything" continues past position 0);
		// this mirrors AWS's rule that "~" always follows a word character,
		// so a genuinely empty prefix value never arises from a real pattern.
		{name: "empty_prefix_before_word_char_is_no_boundary", s: "anything", prefix: "", want: false},
		{name: "empty_prefix_and_empty_string_matches", s: "", prefix: "", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, hasPrefixWordBoundary(tt.s, tt.prefix))
		})
	}
}
