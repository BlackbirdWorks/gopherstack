package cloudwatchlogs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_StartQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name        string
		queryString string
		logGroups   []string
		wantErr     bool
	}{
		{
			name: "success_empty_group",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/my/group", "", "")
			},
			queryString: "fields @timestamp, @message",
			logGroups:   []string{"/my/group"},
		},
		{
			name: "success_with_events",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/my/group", "", "")
				_, _ = b.CreateLogStream(context.Background(), "/my/group", "stream")
				_, _ = b.PutLogEvents(
					context.Background(),
					"/my/group",
					"stream",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "hello world", Timestamp: 1000},
						{Message: "error occurred", Timestamp: 2000},
					},
				)
			},
			queryString: "fields @timestamp, @message",
			logGroups:   []string{"/my/group"},
		},
		{
			name:        "nonexistent_group_is_ok",
			queryString: "fields @timestamp",
			logGroups:   []string{"/nonexistent"},
		},
		{
			name:        "invalid_query_limit",
			queryString: "limit notanumber",
			logGroups:   []string{},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			info, err := b.StartQuery(
				context.Background(),
				"qid-1",
				tt.queryString,
				tt.logGroups,
				0,
				0,
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "qid-1", info.QueryID)
			assert.Equal(t, cloudwatchlogs.QueryStatusRunning, info.Status)
		})
	}
}

func TestCloudWatchLogsBackend_GetQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name       string
		queryID    string
		wantErr    error
		wantStatus cloudwatchlogs.QueryStatus
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "/grp", "s")
				_, _ = b.PutLogEvents(
					context.Background(),
					"/grp",
					"s",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "msg1", Timestamp: 1000},
					},
				)
				_, _ = b.StartQuery(
					context.Background(),
					"qid-1",
					"fields @message",
					[]string{"/grp"},
					0,
					0,
				)
			},
			queryID:    "qid-1",
			wantStatus: cloudwatchlogs.QueryStatusComplete,
		},
		{
			name:    "not_found",
			queryID: "no-such-query",
			wantErr: cloudwatchlogs.ErrQueryNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			results, stats, status, err := b.GetQueryResults(tt.queryID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
			assert.NotNil(t, results)
			assert.GreaterOrEqual(t, stats.RecordsScanned, float64(0))
		})
	}
}

func TestCloudWatchLogsBackend_QueryStats_BytesScanned(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "/grp", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "/grp", "s")
	require.NoError(t, err)

	msg1 := "hello world"
	msg2 := "error occurred"
	_, err = b.PutLogEvents(context.Background(), "/grp", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: msg1, Timestamp: 1000},
		{Message: msg2, Timestamp: 2000},
	})
	require.NoError(t, err)

	_, err = b.StartQuery(context.Background(), "q1", "fields @message", []string{"/grp"}, 0, 0)
	require.NoError(t, err)

	_, stats, _, err := b.GetQueryResults("q1")
	require.NoError(t, err)

	wantBytes := float64(len(msg1) + len(msg2))
	assert.InDelta(t, wantBytes, stats.BytesScanned, 0)
	assert.InDelta(t, float64(2), stats.RecordsScanned, 0)
}

func TestCloudWatchLogsBackend_StopQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name    string
		queryID string
	}{
		{
			name: "success_running_query",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.StartQuery(
					context.Background(),
					"qid-running",
					"fields @message",
					[]string{},
					0,
					0,
				)
				require.NoError(t, err)
				// Place the query back into Running state so StopQuery can cancel it.
				cloudwatchlogs.SetQueryStatusInternal(
					b,
					"qid-running",
					cloudwatchlogs.QueryStatusRunning,
				)

				return "qid-running"
			},
		},
		{
			name: "already_complete_cancels",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.StartQuery(
					context.Background(),
					"qid-done",
					"fields @message",
					[]string{},
					0,
					0,
				)
				require.NoError(t, err)
				// Query is already Complete after synchronous execution; StopQuery
				// still succeeds and transitions it to Cancelled (emulator behaviour).
				return "qid-done"
			},
		},
		{
			name:    "not_found",
			queryID: "no-such-query",
			wantErr: cloudwatchlogs.ErrQueryNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			qid := tt.queryID
			if tt.setup != nil {
				qid = tt.setup(t, b)
			}

			err := b.StopQuery(qid)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify status is now Cancelled.
			_, _, status, getErr := b.GetQueryResults(qid)
			require.NoError(t, getErr)
			assert.Equal(t, cloudwatchlogs.QueryStatusCancelled, status)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name         string
		logGroupName string
		status       string
		wantLen      int
	}{
		{
			name: "all_queries",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/grp", "", "")
				_, _ = b.StartQuery(
					context.Background(),
					"q1",
					"fields @message",
					[]string{"/grp"},
					0,
					0,
				)
				_, _ = b.StartQuery(
					context.Background(),
					"q2",
					"fields @timestamp",
					[]string{"/grp"},
					0,
					0,
				)
			},
			wantLen: 2,
		},
		{
			name: "filter_by_group",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/grp1", "", "")
				_, _ = b.CreateLogGroup(context.Background(), "/grp2", "", "")
				_, _ = b.StartQuery(
					context.Background(),
					"q1",
					"fields @message",
					[]string{"/grp1"},
					0,
					0,
				)
				_, _ = b.StartQuery(
					context.Background(),
					"q2",
					"fields @message",
					[]string{"/grp2"},
					0,
					0,
				)
			},
			logGroupName: "/grp1",
			wantLen:      1,
		},
		{
			name: "filter_by_status",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.StartQuery(context.Background(), "q1", "fields @message", []string{}, 0, 0)
				_, _, _, _ = b.GetQueryResults("q1") // Transition to Complete
				_, _ = b.StartQuery(context.Background(), "q2", "fields @message", []string{}, 0, 0)
				// Move q2 back to Running so StopQuery can cancel it (AWS parity).
				cloudwatchlogs.SetQueryStatusInternal(b, "q2", cloudwatchlogs.QueryStatusRunning)
				_ = b.StopQuery("q2")
			},
			status:  "Complete",
			wantLen: 1,
		},
		{
			name:    "empty",
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			queries, _, err := b.DescribeQueries(tt.logGroupName, tt.status, "", 0)
			require.NoError(t, err)
			assert.Len(t, queries, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_QueryEviction_TTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name: "expired_queries_evicted_on_next_start",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				// Use a short TTL so the existing queries expire before the trigger query.
				b.SetQueryTTL(time.Millisecond)
				_, _ = b.StartQuery(
					context.Background(),
					"old-1",
					"fields @message",
					[]string{},
					0,
					0,
				)
				_, _ = b.StartQuery(
					context.Background(),
					"old-2",
					"fields @message",
					[]string{},
					0,
					0,
				)
				// Sleep well beyond the TTL to avoid any scheduling jitter.
				time.Sleep(20 * time.Millisecond)
				// This new query triggers eviction; old-1 and old-2 should be removed.
				_, _ = b.StartQuery(
					context.Background(),
					"new-1",
					"fields @message",
					[]string{},
					0,
					0,
				)
			},
			wantLen: 1,
		},
		{
			name: "no_eviction_before_ttl",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				// Use a very long TTL so nothing expires.
				b.SetQueryTTL(time.Hour)
				_, _ = b.StartQuery(context.Background(), "q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery(context.Background(), "q2", "fields @message", []string{}, 0, 0)
			},
			wantLen: 2,
		},
		{
			name: "ttl_disabled",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				b.SetQueryTTL(0)
				_, _ = b.StartQuery(context.Background(), "q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery(context.Background(), "q2", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery(context.Background(), "q3", "fields @message", []string{}, 0, 0)
			},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			queries, _, err := b.DescribeQueries("", "", "", 0)
			require.NoError(t, err)
			assert.Len(t, queries, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_QueryEviction_MaxCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name       string
		wantHasID  string
		wantLackID string
		wantLen    int
	}{
		{
			name: "oldest_evicted_when_cap_reached",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				b.SetQueryTTL(0) // disable TTL so only cap applies
				b.SetMaxQueries(2)
				_, _ = b.StartQuery(
					context.Background(),
					"first",
					"fields @message",
					[]string{},
					0,
					0,
				)
				_, _ = b.StartQuery(
					context.Background(),
					"second",
					"fields @message",
					[]string{},
					0,
					0,
				)
				// This triggers eviction of the oldest ("first").
				_, _ = b.StartQuery(
					context.Background(),
					"third",
					"fields @message",
					[]string{},
					0,
					0,
				)
			},
			wantLen:    2,
			wantHasID:  "third",
			wantLackID: "first",
		},
		{
			name: "below_cap_no_eviction",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				b.SetQueryTTL(0)
				b.SetMaxQueries(5)
				_, _ = b.StartQuery(context.Background(), "q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery(context.Background(), "q2", "fields @message", []string{}, 0, 0)
			},
			wantLen: 2,
		},
		{
			name: "cap_disabled",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				b.SetQueryTTL(0)
				b.SetMaxQueries(0) // disabled
				for i := range 20 {
					_, _ = b.StartQuery(
						context.Background(),
						fmt.Sprintf("q%d", i),
						"fields @message",
						[]string{},
						0,
						0,
					)
				}
			},
			wantLen: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			queries, _, err := b.DescribeQueries("", "", "", 100)
			require.NoError(t, err)
			assert.Len(t, queries, tt.wantLen)

			if tt.wantHasID != "" || tt.wantLackID != "" {
				ids := make([]string, len(queries))
				for i, q := range queries {
					ids[i] = q.QueryID
				}
				if tt.wantHasID != "" {
					assert.Contains(t, ids, tt.wantHasID)
				}
				if tt.wantLackID != "" {
					assert.NotContains(t, ids, tt.wantLackID)
				}
			}
		})
	}
}

func TestCloudWatchLogsBackend_StartQuery_ParsedQueryCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		queryStrings  []string
		wantCacheSize int
	}{
		{
			name:          "same_query_reused",
			queryStrings:  []string{"fields @message", "fields @message", "fields @message"},
			wantCacheSize: 1,
		},
		{
			name:          "different_queries_cached",
			queryStrings:  []string{"fields @message", "fields @timestamp"},
			wantCacheSize: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			for i, queryString := range tt.queryStrings {
				_, err := b.StartQuery(
					context.Background(),
					fmt.Sprintf("q-%d", i),
					queryString,
					[]string{},
					0,
					0,
				)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCacheSize, b.GetParsedInsightsQueryCacheSize())
		})
	}
}

func TestCloudWatchLogsBackend_RecordsMatched(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	events := []cloudwatchlogs.InputLogEvent{
		{Message: "ERROR: oops", Timestamp: now},
		{Message: "INFO: ok", Timestamp: now + 1},
		{Message: "ERROR: again", Timestamp: now + 2},
	}
	_, err = b.PutLogEvents(context.Background(), "g", "s", "", events)
	require.NoError(t, err)

	// Query with a filter that matches only 2 of the 3 events.
	_, err = b.StartQuery(context.Background(), "q1",
		`filter @message like /ERROR/`,
		[]string{"g"}, 0, 0)
	require.NoError(t, err)

	rows, stats, status, err := b.GetQueryResults("q1")
	require.NoError(t, err)
	assert.Equal(t, cloudwatchlogs.QueryStatusComplete, status)
	assert.InDelta(t, float64(2), stats.RecordsMatched, 0)
	assert.InDelta(t, float64(3), stats.RecordsScanned, 0)
	assert.Len(t, rows, 2)
}

func TestCloudWatchLogsBackend_ListLogGroupsForQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name       string
		queryID    string
		wantGroups []string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp1", "", "")
				require.NoError(t, err)
				info, err := b.StartQuery(
					context.Background(),
					"qid1",
					"fields @message",
					[]string{"grp1"},
					0,
					0,
				)
				require.NoError(t, err)

				return info.QueryID
			},
			wantGroups: []string{"grp1"},
		},
		{
			name:    "not_found",
			queryID: "nonexistent-query",
			wantErr: cloudwatchlogs.ErrQueryNotFound,
		},
		{
			name:    "empty_id",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			qid := tt.queryID
			if tt.setup != nil {
				qid = tt.setup(t, b)
			}

			groups, err := b.ListLogGroupsForQuery(qid)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantGroups, groups)
		})
	}
}
