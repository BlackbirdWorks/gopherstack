package cloudwatchlogs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestParsedInsightsQueryCache_CappedAtMax verifies that the parsed-query cache
// stays at or below the configured cap even when more unique query strings are
// submitted, preventing unbounded memory growth on long-running backends.
func TestParsedInsightsQueryCache_CappedAtMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		queries int
	}{
		{name: "at cap", queries: cloudwatchlogs.DefaultParsedQueryCacheSizeForTest},
		{name: "one over cap", queries: cloudwatchlogs.DefaultParsedQueryCacheSizeForTest + 1},
		{name: "well over cap", queries: cloudwatchlogs.DefaultParsedQueryCacheSizeForTest + 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := cloudwatchlogs.NewInMemoryBackend()

			_, err := b.CreateLogGroup(ctx, "test-group", "", "")
			require.NoError(t, err)

			var sqErr error
			for i := range tc.queries {
				// Each iteration uses a different (but valid) query string so
				// each one triggers a new cache entry.
				q := fmt.Sprintf("fields @timestamp, @message | filter @message like /msg-%d/", i)
				_, sqErr = b.StartQuery(ctx, fmt.Sprintf("qid-%d", i), q, []string{"test-group"}, 0, 9999999999)
				require.NoError(t, sqErr, "StartQuery[%d] must succeed", i)
			}

			got := b.GetParsedInsightsQueryCacheSize()
			require.LessOrEqual(t, got, cloudwatchlogs.DefaultParsedQueryCacheSizeForTest,
				"parsed query cache must not exceed the cap after %d unique queries", tc.queries)
		})
	}
}

// TestParsedInsightsQueryCache_DeduplicatesIdenticalQueries verifies that
// submitting the same query string multiple times does not grow the cache —
// each unique string is cached only once.
func TestParsedInsightsQueryCache_DeduplicatesIdenticalQueries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := cloudwatchlogs.NewInMemoryBackend()

	_, err := b.CreateLogGroup(ctx, "test-group", "", "")
	require.NoError(t, err)

	const query = "fields @timestamp, @message"
	const repeats = 20

	var sqErr error
	for i := range repeats {
		_, sqErr = b.StartQuery(ctx, fmt.Sprintf("qid-%d", i), query, []string{"test-group"}, 0, 9999999999)
		require.NoError(t, sqErr, "StartQuery[%d] must succeed", i)
	}

	require.Equal(t, 1, b.GetParsedInsightsQueryCacheSize(),
		"identical query strings must produce exactly one cache entry")
}

// TestCompiledPatternCache_CappedAtMax verifies that the compiled metric-filter
// pattern cache stays at or below its configured cap when many distinct patterns
// are submitted via PutMetricFilter + PutLogEvents, preventing unbounded memory
// growth on long-running backends.
func TestCompiledPatternCache_CappedAtMax(t *testing.T) {
	t.Parallel()

	const overflow = cloudwatchlogs.MaxCompiledPatternCacheForTest + 100

	tests := []struct {
		name     string
		patterns int
	}{
		{name: "at cap", patterns: cloudwatchlogs.MaxCompiledPatternCacheForTest},
		{name: "one over cap", patterns: cloudwatchlogs.MaxCompiledPatternCacheForTest + 1},
		{name: "well over cap", patterns: overflow},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := cloudwatchlogs.NewInMemoryBackend()

			const group = "test-group"
			const stream = "test-stream"

			_, err := b.CreateLogGroup(ctx, group, "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(ctx, group, stream)
			require.NoError(t, err)

			// Register one metric filter per unique pattern.
			for i := range tc.patterns {
				pattern := fmt.Sprintf("?unique-token-%d", i)
				err = b.PutMetricFilter(ctx, group, fmt.Sprintf("f%d", i), pattern,
					[]cloudwatchlogs.MetricTransformation{
						{MetricName: "m", MetricNamespace: "ns", MetricValue: "1"},
					})
				require.NoError(t, err)
			}

			// PutLogEvents to trigger matchingMetricFilters → getCompiledPattern
			// for every registered filter.
			now := time.Now().UnixMilli()
			_, err = b.PutLogEvents(ctx, group, stream, "",
				[]cloudwatchlogs.InputLogEvent{{Timestamp: now, Message: "ping"}})
			require.NoError(t, err)

			got := b.GetCompiledPatternCacheSize()
			require.LessOrEqual(t, got, cloudwatchlogs.MaxCompiledPatternCacheForTest,
				"compiled pattern cache must not exceed the cap after %d unique patterns", tc.patterns)
		})
	}
}
