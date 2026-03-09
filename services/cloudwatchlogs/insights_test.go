package cloudwatchlogs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// helpers shared across insights tests.
func makeInsightsBackend(t *testing.T) *cloudwatchlogs.InMemoryBackend {
	t.Helper()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, err := b.CreateLogGroup("/grp")
	require.NoError(t, err)
	_, err = b.CreateLogStream("/grp", "s")
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	_, err = b.PutLogEvents("/grp", "s", []cloudwatchlogs.InputLogEvent{
		{Message: "ERROR: disk full", Timestamp: now - 3000},
		{Message: "INFO: startup complete", Timestamp: now - 2000},
		{Message: "ERROR: connection refused", Timestamp: now - 1000},
	})
	require.NoError(t, err)

	return b
}

func TestInsightsQuery_FieldsProjection(t *testing.T) {
	t.Parallel()

	b := makeInsightsBackend(t)

	info, err := b.StartQuery("q1", "fields @timestamp, @message", []string{"/grp"}, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, cloudwatchlogs.QueryStatusComplete, info.Status)

	results, _, status, err := b.GetQueryResults("q1")
	require.NoError(t, err)
	assert.Equal(t, cloudwatchlogs.QueryStatusComplete, status)
	assert.Len(t, results, 3)

	// Each row should have @timestamp and @message fields.
	for _, row := range results {
		fields := make(map[string]string)
		for _, f := range row {
			fields[f.Field] = f.Value
		}
		assert.Contains(t, fields, "@timestamp")
		assert.Contains(t, fields, "@message")
	}
}

func TestInsightsQuery_FilterRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantLen int
	}{
		{
			name:    "filter_error",
			query:   "filter @message like /ERROR/",
			wantLen: 2,
		},
		{
			name:    "filter_no_match",
			query:   "filter @message like /CRITICAL/",
			wantLen: 0,
		},
		{
			name:    "filter_string_literal",
			query:   `filter @message like "INFO"`,
			wantLen: 1,
		},
		{
			name:    "filter_all_match",
			query:   "filter @message like /./",
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := makeInsightsBackend(t)
			_, err := b.StartQuery("q1", tt.query, []string{"/grp"}, 0, 0)
			require.NoError(t, err)

			results, _, _, err := b.GetQueryResults("q1")
			require.NoError(t, err)
			assert.Len(t, results, tt.wantLen)
		})
	}
}

func TestInsightsQuery_Sort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantFirst string
		wantLast  string
	}{
		{
			name:      "sort_asc",
			query:     "fields @message | sort @timestamp asc",
			wantFirst: "ERROR: disk full",
			wantLast:  "ERROR: connection refused",
		},
		{
			name:      "sort_desc",
			query:     "fields @message | sort @timestamp desc",
			wantFirst: "ERROR: connection refused",
			wantLast:  "ERROR: disk full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := makeInsightsBackend(t)
			_, err := b.StartQuery("q1", tt.query, []string{"/grp"}, 0, 0)
			require.NoError(t, err)

			results, _, _, err := b.GetQueryResults("q1")
			require.NoError(t, err)
			require.Len(t, results, 3)

			firstMsg := fieldValue(results[0], "@message")
			lastMsg := fieldValue(results[2], "@message")
			assert.Equal(t, tt.wantFirst, firstMsg)
			assert.Equal(t, tt.wantLast, lastMsg)
		})
	}
}

func TestInsightsQuery_Limit(t *testing.T) {
	t.Parallel()

	b := makeInsightsBackend(t)
	_, err := b.StartQuery("q1", "fields @message | limit 2", []string{"/grp"}, 0, 0)
	require.NoError(t, err)

	results, _, _, err := b.GetQueryResults("q1")
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestInsightsQuery_StatsCountBy(t *testing.T) {
	t.Parallel()

	b := makeInsightsBackend(t)
	_, err := b.StartQuery("q1", "stats count(*) by @message", []string{"/grp"}, 0, 0)
	require.NoError(t, err)

	results, _, _, err := b.GetQueryResults("q1")
	require.NoError(t, err)
	assert.Len(t, results, 3) // 3 unique messages

	// Each row has @message and count(*) fields.
	for _, row := range results {
		fields := make(map[string]string)
		for _, f := range row {
			fields[f.Field] = f.Value
		}
		assert.Contains(t, fields, "@message")
		assert.Contains(t, fields, "count(*)")
	}
}

func TestInsightsQuery_StatsCountNoBy(t *testing.T) {
	t.Parallel()

	b := makeInsightsBackend(t)
	_, err := b.StartQuery("q1", "stats count(*)", []string{"/grp"}, 0, 0)
	require.NoError(t, err)

	results, _, _, err := b.GetQueryResults("q1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "count(*)", results[0][0].Field)
	assert.Equal(t, "3", results[0][0].Value)
}

func TestInsightsQuery_TimeRange(t *testing.T) {
	t.Parallel()

	b := makeInsightsBackend(t)
	now := time.Now().UnixMilli()

	// Query within a narrow time range — should only include events in range.
	_, err := b.StartQuery("q1", "fields @message", []string{"/grp"}, now-2500, now-500)
	require.NoError(t, err)

	results, stats, _, err := b.GetQueryResults("q1")
	require.NoError(t, err)
	// 2 events fall within now-2500 to now-500 (now-2000 and now-1000).
	assert.Len(t, results, 2)
	assert.InDelta(t, float64(3), stats.RecordsScanned, 0.1) // all 3 were scanned
}

func TestInsightsQuery_DescribeQueries_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	for i := range 5 {
		qid := "qid-" + string(rune('a'+i))
		_, err := b.StartQuery(qid, "fields @message", []string{}, 0, 0)
		require.NoError(t, err)
	}

	// First page.
	page1, nextToken, err := b.DescribeQueries("", "", "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, nextToken)

	// Second page.
	page2, nextToken2, err := b.DescribeQueries("", "", nextToken, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, nextToken2)

	// Third page.
	page3, nextToken3, err := b.DescribeQueries("", "", nextToken2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, nextToken3)
}

// fieldValue extracts the value of a named field from a result row.
func fieldValue(row []cloudwatchlogs.ResultField, name string) string {
	for _, f := range row {
		if f.Field == name {
			return f.Value
		}
	}

	return ""
}
