package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_FilterPatternMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		message string
		name    string
		want    bool
	}{
		{
			name:    "empty_pattern_matches_all",
			pattern: "",
			message: "anything",
			want:    true,
		},
		{
			name:    "simple_substring_match",
			pattern: "ERROR",
			message: "2024-01-01 ERROR: something bad",
			want:    true,
		},
		{
			name:    "simple_substring_no_match",
			pattern: "ERROR",
			message: "2024-01-01 INFO: all good",
			want:    false,
		},
		{
			name:    "multi_term_and_all_present",
			pattern: "ERROR bad",
			message: "ERROR: something bad happened",
			want:    true,
		},
		{
			name:    "multi_term_and_one_missing",
			pattern: "ERROR bad",
			message: "ERROR: something happened",
			want:    false,
		},
		{
			// AWS: "?" optional terms are ignored when combined with required
			// terms, so this reduces to requiring "ERROR".
			name:    "optional_ignored_when_combined_with_required",
			pattern: "?DEBUG ERROR",
			message: "ERROR but not debug",
			want:    true,
		},
		{
			// Same pattern, message lacks the required "ERROR" term => no match.
			name:    "optional_ignored_required_absent",
			pattern: "?DEBUG ERROR",
			message: "DEBUG only",
			want:    false,
		},
		{
			// A standalone "?" optional term is OR semantics: contains DEBUG => match.
			name:    "optional_single_present",
			pattern: "?DEBUG",
			message: "DEBUG: verbose log",
			want:    true,
		},
		{
			// Multiple "?" optional terms are OR-ed: ARGUMENTS present => match.
			name:    "optional_or_one_present",
			pattern: "?ERROR ?ARGUMENTS",
			message: "[420] INVALID ARGUMENTS",
			want:    true,
		},
		{
			// None of the optional terms present => no match.
			name:    "optional_or_none_present",
			pattern: "?ERROR ?ARGUMENTS",
			message: "[200] OK REQUEST",
			want:    false,
		},
		{
			// "-" exclude term: ARGUMENTS present => excluded.
			name:    "exclude_term_present",
			pattern: "ERROR -ARGUMENTS",
			message: "[419] MISSING ARGUMENTS that are ERROR",
			want:    false,
		},
		{
			// "-" exclude term absent, required ERROR present => match.
			name:    "exclude_term_absent",
			pattern: "ERROR -ARGUMENTS",
			message: "[401] UNAUTHORIZED REQUEST ERROR",
			want:    true,
		},
		{
			name:    "quoted_exact_match",
			pattern: `"exact phrase"`,
			message: "this is an exact phrase in here",
			want:    true,
		},
		{
			name:    "quoted_no_match",
			pattern: `"exact phrase"`,
			message: "not in this message",
			want:    false,
		},
		{
			name:    "wildcard_asterisk",
			pattern: "ERR*",
			message: "ERRORED: bad",
			want:    true,
		},
		{
			name:    "wildcard_asterisk_no_match",
			pattern: "ERR*bad",
			message: "WARNbad",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudwatchlogs.FilterPatternMatches(tt.pattern, tt.message)
			assert.Equal(t, tt.want, got)
		})
	}
}
