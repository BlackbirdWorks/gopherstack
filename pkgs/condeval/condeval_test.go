package condeval_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/condeval"
)

// glob is a minimal '*'/'?' matcher, standing in for each caller's own
// wildcardMatch (services/iam and services/sts each keep their own).
func glob(pattern, value string) bool {
	if pattern == "*" {
		return true
	}

	prefix, suffix, ok := strings.Cut(pattern, "*")
	if !ok {
		return pattern == value
	}

	return strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix)
}

func TestArnMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{
			name:    "wildcard_confined_to_last_segment",
			pattern: "arn:aws:sqs:us-east-1:123456789012:my-*",
			value:   "arn:aws:sqs:us-east-1:123456789012:my-queue",
			want:    true,
		},
		{
			name:    "wildcard_does_not_span_segments",
			pattern: "arn:aws:s3:*:mybucket",
			value:   "arn:aws:s3:us-east-1:123456789012:mybucket",
			want:    false,
		},
		{
			name:    "case_sensitive_no_match",
			pattern: "arn:aws:iam::123456789012:role/prod",
			value:   "arn:aws:iam::123456789012:role/Prod",
			want:    false,
		},
		{
			name:    "differing_segment_count_no_match",
			pattern: "arn:aws:iam::123456789012:role/prod",
			value:   "not-an-arn-at-all",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, condeval.ArnMatch(tt.pattern, tt.value, glob))
		})
	}
}

func TestAnyArnMatch(t *testing.T) {
	t.Parallel()

	patterns := []string{"arn:aws:iam::111111111111:role/other", "arn:aws:iam::222222222222:role/*"}

	assert.True(t, condeval.AnyArnMatch(patterns, "arn:aws:iam::222222222222:role/deploy", glob))
	assert.False(t, condeval.AnyArnMatch(patterns, "arn:aws:iam::333333333333:role/deploy", glob))
}

func TestParseDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want time.Time
		name string
		in   string
	}{
		{name: "rfc3339", in: "2023-06-15T12:00:00Z", want: time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)},
		{name: "date_only", in: "2023-06-15", want: time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)},
		{name: "epoch_seconds", in: "1686830400", want: time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := condeval.ParseDate(tt.in)
			require.True(t, ok)
			assert.True(t, tt.want.Equal(got), "got %v, want %v", got, tt.want)
		})
	}

	t.Run("invalid", func(t *testing.T) {
		t.Parallel()

		_, ok := condeval.ParseDate("not-a-date")
		assert.False(t, ok)
	})
}

func TestCompareDate(t *testing.T) {
	t.Parallel()

	earlier := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	later := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		actual, candidate time.Time
		name              string
		op                string
		want              bool
	}{
		{name: "equals_true", op: "dateequals", actual: earlier, candidate: earlier, want: true},
		{name: "equals_false", op: "dateequals", actual: earlier, candidate: later, want: false},
		{name: "not_equals", op: "datenotequals", actual: earlier, candidate: later, want: true},
		{name: "less_than", op: "datelessthan", actual: earlier, candidate: later, want: true},
		{name: "less_than_equal_at_boundary", op: "datelessthanequals", actual: later, candidate: later, want: true},
		{name: "greater_than", op: "dategreaterthan", actual: later, candidate: earlier, want: true},
		{
			name: "greater_than_equal_at_boundary", op: "dategreaterthanequals",
			actual: earlier, candidate: earlier, want: true,
		},
		{name: "unrecognized_op", op: "bogus", actual: earlier, candidate: later, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, condeval.CompareDate(tt.op, tt.actual, tt.candidate))
		})
	}
}
