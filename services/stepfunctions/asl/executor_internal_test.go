package asl

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMapCopyCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		want int
	}{
		{
			name: "zero_size",
			size: 0,
			want: 1,
		},
		{
			name: "small_size",
			size: 7,
			want: 8,
		},
		{
			name: "max_int_size",
			size: math.MaxInt,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mapCopyCapacity(tt.size)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestComputeRetryDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		intervalSeconds int
		backoffRate     float64
		attempts        int
		want            time.Duration
	}{
		{
			name:            "normal_backoff",
			intervalSeconds: 2,
			backoffRate:     2,
			attempts:        3,
			want:            16 * time.Second,
		},
		{
			name:            "negative_interval_clamped_to_zero",
			intervalSeconds: -1,
			backoffRate:     2,
			attempts:        1,
			want:            0,
		},
		{
			name:            "infinite_backoff_clamped_to_zero",
			intervalSeconds: 2,
			backoffRate:     math.MaxFloat64,
			attempts:        2,
			want:            0,
		},
		{
			name:            "finite_overflow_clamped_to_max_retry_delay",
			intervalSeconds: 10,
			backoffRate:     10,
			attempts:        5,
			want:            maxRetryDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := computeRetryDelay(tt.intervalSeconds, tt.backoffRate, tt.attempts)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJSONPathCacheBounded(t *testing.T) {
	t.Parallel()

	cache := newJSONPathCache(2)

	getCachedJSONPathParts("a.b", cache)
	getCachedJSONPathParts("c.d", cache)
	getCachedJSONPathParts("e.f", cache)

	assert.EqualValues(t, 2, cache.size.Load())

	// Cache uses "accept until full, then reject new keys" policy.
	// First two keys remain cached; later keys are not inserted.
	_, foundA := cache.load("a.b")
	assert.True(t, foundA)

	_, foundC := cache.load("c.d")
	assert.True(t, foundC)

	_, foundE := cache.load("e.f")
	assert.False(t, foundE)
}

func TestJSONPathCacheNilFallback(t *testing.T) {
	t.Parallel()

	parts := getCachedJSONPathParts("x.y.z", nil)
	assert.Equal(t, []string{"x", "y", "z"}, parts)
}

func TestStringMatchesPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		// AWS doc example: "log-*.txt" matches "log-20190101.txt".
		{name: "doc_example_match", value: "log-20190101.txt", pattern: "log-*.txt", want: true},
		{name: "doc_example_no_match_ext", value: "log-20190101.log", pattern: "log-*.txt", want: false},
		// '*' matches the empty string.
		{name: "star_matches_empty", value: "logtxt", pattern: "log*txt", want: true},
		// Leading and trailing wildcards.
		{name: "leading_star", value: "abcdef", pattern: "*def", want: true},
		{name: "trailing_star", value: "abcdef", pattern: "abc*", want: true},
		{name: "only_star", value: "anything", pattern: "*", want: true},
		{name: "only_star_empty_value", value: "", pattern: "*", want: true},
		// Multiple wildcards require backtracking.
		{name: "multiple_stars", value: "axbxcxd", pattern: "a*b*c*d", want: true},
		{name: "multiple_stars_no_match", value: "axbxc", pattern: "a*b*c*d", want: false},
		// Anchoring: pattern must match the whole string.
		{name: "anchored_no_partial", value: "prefix-log-1.txt", pattern: "log-*.txt", want: false},
		// Exact match with no wildcard.
		{name: "exact_match", value: "hello", pattern: "hello", want: true},
		{name: "exact_no_match", value: "hello", pattern: "world", want: false},
		// Empty pattern only matches empty string.
		{name: "empty_pattern_empty_value", value: "", pattern: "", want: true},
		{name: "empty_pattern_nonempty_value", value: "x", pattern: "", want: false},
		// Escaped literal asterisk: "\\*" in the Go string is a backslash + '*'.
		{name: "escaped_star_literal_match", value: "a*b", pattern: `a\*b`, want: true},
		{name: "escaped_star_literal_no_wildcard", value: "axb", pattern: `a\*b`, want: false},
		// Escaped backslash: "\\\\" is backslash + backslash.
		{name: "escaped_backslash_match", value: `a\b`, pattern: `a\\b`, want: true},
		// Trailing backslash with no following char is treated as a literal mismatch.
		{name: "consecutive_stars", value: "abc", pattern: "a**c", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := stringMatchesPattern(tt.value, tt.pattern)
			assert.Equal(t, tt.want, got)
		})
	}
}
