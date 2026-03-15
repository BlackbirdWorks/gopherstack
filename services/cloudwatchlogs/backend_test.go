package cloudwatchlogs_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_CreateLogGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setup           func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		createGroup     string
		wantErr         error
		wantName        string
		wantArnContains string
	}{
		{
			name:            "success",
			createGroup:     "/my/group",
			wantName:        "/my/group",
			wantArnContains: "/my/group",
		},
		{
			name: "already_exists",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup("dup-group")
				require.NoError(t, err)
			},
			createGroup: "dup-group",
			wantErr:     cloudwatchlogs.ErrLogGroupAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			g, err := b.CreateLogGroup(tt.createGroup)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, g.LogGroupName)
			}

			if tt.wantArnContains != "" {
				assert.Contains(t, g.Arn, tt.wantArnContains)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DeleteLogGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name    string
		group   string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup("to-delete")
				require.NoError(t, err)
			},
			group: "to-delete",
		},
		{
			name:    "not_found",
			group:   "nonexistent",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteLogGroup(tt.group)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			groups, _, err := b.DescribeLogGroups("", "", 0)
			require.NoError(t, err)
			assert.Empty(t, groups)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name          string
		prefix        string
		token         string
		wantFirstName string
		limit         int
		wantCount     int
	}{
		{
			name: "prefix",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("/prod/app")
				_, _ = b.CreateLogGroup("/dev/app")
			},
			prefix:        "/prod",
			wantCount:     1,
			wantFirstName: "/prod/app",
		},
		{
			name: "beyond_end",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("/group/a")
			},
			token:     "999",
			limit:     10,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			groups, next, err := b.DescribeLogGroups(tt.prefix, tt.token, tt.limit)
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, groups, tt.wantCount)

			if tt.wantFirstName != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstName, groups[0].LogGroupName)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogGroups_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	for i := range 5 {
		_, _ = b.CreateLogGroup("/group/" + string(rune('a'+i)))
	}

	page1, token, err := b.DescribeLogGroups("", "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, token)

	page2, token2, err := b.DescribeLogGroups("", token, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, token2)

	page3, token3, err := b.DescribeLogGroups("", token2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3)
}

func TestCloudWatchLogsBackend_CreateLogStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setup           func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		group           string
		stream          string
		wantErr         error
		wantName        string
		wantArnContains []string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("my-group")
			},
			group:           "my-group",
			stream:          "my-stream",
			wantName:        "my-stream",
			wantArnContains: []string{"my-group", "my-stream"},
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			stream:  "stream",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "already_exists",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "dup")
			},
			group:   "grp",
			stream:  "dup",
			wantErr: cloudwatchlogs.ErrLogStreamAlreadyExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			s, err := b.CreateLogStream(tt.group, tt.stream)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, s.LogStreamName)
			}

			for _, substr := range tt.wantArnContains {
				assert.Contains(t, s.Arn, substr)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		setup         func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name          string
		group         string
		prefix        string
		wantFirstName string
		wantCount     int
	}{
		{
			name: "all_streams",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "stream-a")
				_, _ = b.CreateLogStream("grp", "stream-b")
			},
			group:     "grp",
			wantCount: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "prod-stream")
				_, _ = b.CreateLogStream("grp", "dev-stream")
			},
			group:         "grp",
			prefix:        "prod",
			wantCount:     1,
			wantFirstName: "prod-stream",
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			streams, next, err := b.DescribeLogStreams(tt.group, tt.prefix, "", 0)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, streams, tt.wantCount)

			if tt.wantFirstName != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstName, streams[0].LogStreamName)
			}
		})
	}
}

func TestCloudWatchLogsBackend_PutLogEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name    string
		group   string
		stream  string
		events  []cloudwatchlogs.InputLogEvent
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "stream")
			},
			group:  "grp",
			stream: "stream",
			events: []cloudwatchlogs.InputLogEvent{
				{Message: "first", Timestamp: 1000},
				{Message: "second", Timestamp: 2000},
			},
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			stream:  "stream",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "stream_not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
			},
			group:   "grp",
			stream:  "nonexistent",
			wantErr: cloudwatchlogs.ErrLogStreamNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			token, err := b.PutLogEvents(tt.group, tt.stream, tt.events)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, token)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		setup             func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		startTime         *int64
		endTime           *int64
		name              string
		group             string
		stream            string
		nextToken         string
		wantFirstMessage  string
		limit             int
		wantCount         int
		wantNonEmptyFwBwd bool
	}{
		{
			name: "all_events",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "stream")
				_, _ = b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
					{Message: "msg1", Timestamp: 1000},
					{Message: "msg2", Timestamp: 2000},
					{Message: "msg3", Timestamp: 3000},
				})
			},
			group:             "grp",
			stream:            "stream",
			wantCount:         3,
			wantNonEmptyFwBwd: true,
		},
		{
			name: "time_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "stream")
				_, _ = b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
					{Message: "old", Timestamp: 100},
					{Message: "new", Timestamp: 5000},
				})
			},
			group:            "grp",
			stream:           "stream",
			startTime:        int64Ptr(1000),
			wantCount:        1,
			wantFirstMessage: "new",
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			stream:  "stream",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "stream_not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
			},
			group:   "grp",
			stream:  "nonexistent",
			wantErr: cloudwatchlogs.ErrLogStreamNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			evts, fwd, bwd, err := b.GetLogEvents(tt.group, tt.stream, tt.startTime, tt.endTime, tt.limit, tt.nextToken)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, evts, tt.wantCount)

			if tt.wantNonEmptyFwBwd {
				assert.NotEmpty(t, fwd)
				assert.NotEmpty(t, bwd)
			}

			if tt.wantFirstMessage != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstMessage, evts[0].Message)
			}
		})
	}
}

func TestCloudWatchLogsBackend_GetLogEvents_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_, _ = b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 1},
		{Message: "b", Timestamp: 2},
		{Message: "c", Timestamp: 3},
	})

	evts, fwd, _, err := b.GetLogEvents("grp", "stream", nil, nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, evts, 2)

	evts2, _, _, err := b.GetLogEvents("grp", "stream", nil, nil, 2, fwd)
	require.NoError(t, err)
	assert.Len(t, evts2, 1)
}

func TestCloudWatchLogsBackend_FilterLogEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr          error
		setup            func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		startTime        *int64
		endTime          *int64
		name             string
		group            string
		pattern          string
		nextToken        string
		wantFirstMessage string
		streams          []string
		limit            int
		wantCount        int
	}{
		{
			name: "pattern_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "s1")
				_, _ = b.CreateLogStream("grp", "s2")
				_, _ = b.PutLogEvents("grp", "s1", []cloudwatchlogs.InputLogEvent{
					{Message: "ERROR: something bad", Timestamp: 1000},
				})
				_, _ = b.PutLogEvents("grp", "s2", []cloudwatchlogs.InputLogEvent{
					{Message: "INFO: all good", Timestamp: 2000},
				})
			},
			group:            "grp",
			pattern:          "ERROR",
			wantCount:        1,
			wantFirstMessage: "ERROR: something bad",
		},
		{
			name: "stream_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "s1")
				_, _ = b.CreateLogStream("grp", "s2")
				_, _ = b.PutLogEvents("grp", "s1", []cloudwatchlogs.InputLogEvent{
					{Message: "from s1", Timestamp: 1000},
				})
				_, _ = b.PutLogEvents("grp", "s2", []cloudwatchlogs.InputLogEvent{
					{Message: "from s2", Timestamp: 2000},
				})
			},
			group:            "grp",
			streams:          []string{"s1"},
			wantCount:        1,
			wantFirstMessage: "from s1",
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "time_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_, _ = b.CreateLogStream("grp", "s")
				_, _ = b.PutLogEvents("grp", "s", []cloudwatchlogs.InputLogEvent{
					{Message: "old", Timestamp: 100},
					{Message: "new", Timestamp: 9000},
				})
			},
			group:            "grp",
			startTime:        int64Ptr(1000),
			endTime:          int64Ptr(10000),
			wantCount:        1,
			wantFirstMessage: "new",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			evts, _, err := b.FilterLogEvents(
				tt.group,
				tt.streams,
				tt.pattern,
				tt.startTime,
				tt.endTime,
				tt.limit,
				tt.nextToken,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, evts, tt.wantCount)

			if tt.wantFirstMessage != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstMessage, evts[0].Message)
			}
		})
	}
}

func TestCloudWatchLogsBackend_FilterLogEvents_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s")

	for i := range 5 {
		_, _ = b.PutLogEvents("grp", "s", []cloudwatchlogs.InputLogEvent{
			{Message: "msg", Timestamp: int64(i * 100)},
		})
	}

	evts, token, err := b.FilterLogEvents("grp", nil, "", nil, nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, evts, 2)
	assert.NotEmpty(t, token)

	evts2, _, err := b.FilterLogEvents("grp", nil, "", nil, nil, 10, token)
	require.NoError(t, err)
	assert.Len(t, evts2, 3)
}

func TestCloudWatchLogsBackend_PutLogEvents_UpdatesTimestamps(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s")

	_, _ = b.PutLogEvents("grp", "s", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 500},
		{Message: "b", Timestamp: 1500},
	})

	streams, _, err := b.DescribeLogStreams("grp", "", "", 0)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.NotNil(t, streams[0].FirstEventTimestamp)
	require.NotNil(t, streams[0].LastEventTimestamp)
	assert.Equal(t, int64(500), *streams[0].FirstEventTimestamp)
	assert.Equal(t, int64(1500), *streams[0].LastEventTimestamp)
}

func int64Ptr(v int64) *int64 { return new(v) }

func TestCloudWatchLogsBackend_PutSubscriptionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		setup          func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name           string
		group          string
		filterName     string
		filterPattern  string
		destinationArn string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
			},
			group:          "grp",
			filterName:     "my-filter",
			filterPattern:  "",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		},
		{
			name: "update_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_ = b.PutSubscriptionFilter("grp", "f", "", "arn:aws:lambda:us-east-1:123456789012:function:old")
			},
			group:          "grp",
			filterName:     "f",
			filterPattern:  "ERROR",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:new",
		},
		{
			name:       "group_not_found",
			group:      "nonexistent",
			filterName: "f",
			wantErr:    cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "limit_exceeded",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_ = b.PutSubscriptionFilter("grp", "f1", "", "arn:aws:lambda:us-east-1:123456789012:function:a")
				_ = b.PutSubscriptionFilter("grp", "f2", "", "arn:aws:lambda:us-east-1:123456789012:function:b")
			},
			group:          "grp",
			filterName:     "f3",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:c",
			wantErr:        cloudwatchlogs.ErrSubscriptionFilterLimitExceed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.PutSubscriptionFilter(tt.group, tt.filterName, tt.filterPattern, tt.destinationArn)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, err := b.DescribeSubscriptionFilters(tt.group, "", "", 0)
			require.NoError(t, err)

			found := false
			for _, f := range filters {
				if f.FilterName == tt.filterName {
					found = true
					assert.Equal(t, tt.destinationArn, f.DestinationArn)
					assert.Equal(t, tt.filterPattern, f.FilterPattern)
				}
			}
			assert.True(t, found, "filter not found after put")
		})
	}
}

func TestCloudWatchLogsBackend_DescribeSubscriptionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		setup         func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name          string
		group         string
		prefix        string
		nextToken     string
		wantFirstName string
		wantCount     int
		limit         int
	}{
		{
			name: "all_filters",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_ = b.PutSubscriptionFilter("grp", "filter-a", "", "arn:aws:lambda:us-east-1:123456789012:function:a")
				_ = b.PutSubscriptionFilter("grp", "filter-b", "", "arn:aws:lambda:us-east-1:123456789012:function:b")
			},
			group:     "grp",
			wantCount: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_ = b.PutSubscriptionFilter(
					"grp",
					"prod-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
				)
				_ = b.PutSubscriptionFilter("grp", "dev-filter", "", "arn:aws:lambda:us-east-1:123456789012:function:b")
			},
			group:         "grp",
			prefix:        "prod",
			wantCount:     1,
			wantFirstName: "prod-filter",
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "beyond_end",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_ = b.PutSubscriptionFilter("grp", "f", "", "arn:aws:lambda:us-east-1:123456789012:function:a")
			},
			group:     "grp",
			nextToken: "999",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			filters, _, err := b.DescribeSubscriptionFilters(tt.group, tt.prefix, tt.nextToken, tt.limit)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, filters, tt.wantCount)

			if tt.wantFirstName != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstName, filters[0].FilterName)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DeleteSubscriptionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name       string
		group      string
		filterName string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
				_ = b.PutSubscriptionFilter("grp", "my-filter", "", "arn:aws:lambda:us-east-1:123456789012:function:a")
			},
			group:      "grp",
			filterName: "my-filter",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("grp")
			},
			group:      "grp",
			filterName: "nonexistent",
			wantErr:    cloudwatchlogs.ErrSubscriptionFilterNotFound,
		},
		{
			name:       "group_not_found",
			group:      "nonexistent",
			filterName: "f",
			wantErr:    cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteSubscriptionFilter(tt.group, tt.filterName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, ferr := b.DescribeSubscriptionFilters(tt.group, "", "", 0)
			require.NoError(t, ferr)
			assert.Empty(t, filters)
		})
	}
}

func TestCloudWatchLogsBackend_PutLogEvents_SubscriptionDelivery(t *testing.T) {
	t.Parallel()

	type deliveredPayload struct {
		destinationArn string
		payload        []byte
	}

	var delivered []deliveredPayload

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(func(_ context.Context, dst string, p []byte) error {
		delivered = append(delivered, deliveredPayload{dst, p})

		return nil
	})

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_ = b.PutSubscriptionFilter("grp", "my-filter", "", "arn:aws:lambda:us-east-1:123456789012:function:target")

	_, err := b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
		{Message: "hello", Timestamp: 1000},
	})
	require.NoError(t, err)

	// Wait for the delivery goroutine to finish before asserting.
	b.Drain()

	assert.Len(t, delivered, 1)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:target", delivered[0].destinationArn)
	assert.NotEmpty(t, delivered[0].payload)
}

func TestCloudWatchLogsBackend_PutLogEvents_BoundedWorkerPool(t *testing.T) {
	t.Parallel()

	const (
		numEvents  = 20
		workersCap = 4
	)

	// concurrencyHigh tracks the highest observed concurrent delivery count.
	var mu sync.Mutex
	var inFlight, concurrencyHigh int

	ready := make(chan struct{})

	// reachedCap is closed once workersCap goroutines are simultaneously in the deliverer.
	var atCap sync.Once
	reachedCap := make(chan struct{})

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(func(ctx context.Context, _ string, _ []byte) error {
		mu.Lock()
		inFlight++
		if inFlight > concurrencyHigh {
			concurrencyHigh = inFlight
		}
		if inFlight >= workersCap {
			atCap.Do(func() { close(reachedCap) })
		}
		mu.Unlock()

		// Hold until the test signals all goroutines to proceed.
		select {
		case <-ready:
		case <-ctx.Done():
		}

		mu.Lock()
		inFlight--
		mu.Unlock()

		return nil
	})

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Limit to workersCap concurrent workers so we can verify the cap is respected.
	b.SetDeliveryWorkers(workersCap)
	b.SetDeliveryTimeout(0) // disable timeout so the hold above doesn't race
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_ = b.PutSubscriptionFilter("grp", "f", "", "arn:aws:lambda:us-east-1:123456789012:function:fn")

	for i := range numEvents {
		_, err := b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
			{Message: fmt.Sprintf("msg-%d", i), Timestamp: int64(i)},
		})
		require.NoError(t, err)
	}

	// Wait until the semaphore is full before inspecting peak concurrency.
	<-reachedCap

	mu.Lock()
	peak := concurrencyHigh
	mu.Unlock()

	// The peak concurrency must not exceed the configured worker cap.
	assert.LessOrEqual(t, peak, workersCap)

	close(ready)
	b.Drain()
}

func TestCloudWatchLogsBackend_Close_CancelsInFlightDeliveries(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	deliveryCancelled := make(chan struct{}, 1)

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(func(ctx context.Context, _ string, _ []byte) error {
		// Signal that the delivery goroutine has started and is in progress.
		close(started)
		// Block until the context is cancelled.
		<-ctx.Done()
		select {
		case deliveryCancelled <- struct{}{}:
		default:
		}

		return ctx.Err()
	})

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetDeliveryTimeout(0) // disable timeout so Close() is the only cancellation source
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_ = b.PutSubscriptionFilter("grp", "f", "", "arn:aws:lambda:us-east-1:123456789012:function:fn")

	_, err := b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
		{Message: "hello", Timestamp: 1},
	})
	require.NoError(t, err)

	// Wait until the goroutine has started and is blocking inside the deliverer before closing.
	<-started

	// Close cancels the lifecycle context and waits for the goroutine to exit.
	b.Close()

	select {
	case <-deliveryCancelled:
		// goroutine observed context cancellation — expected
	default:
		require.FailNow(t, "expected in-flight delivery to be cancelled by Close()")
	}
}

func TestCloudWatchLogsBackend_DeleteLogGroup_ClearsSubscriptionFilters(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, _ = b.CreateLogGroup("grp")
	_ = b.PutSubscriptionFilter("grp", "f", "", "arn:aws:lambda:us-east-1:123456789012:function:a")
	require.NoError(t, b.DeleteLogGroup("grp"))

	_, _ = b.CreateLogGroup("grp")
	filters, _, err := b.DescribeSubscriptionFilters("grp", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, filters)
}

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
				_, _ = b.CreateLogGroup("/my/group")
			},
			queryString: "fields @timestamp, @message",
			logGroups:   []string{"/my/group"},
		},
		{
			name: "success_with_events",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("/my/group")
				_, _ = b.CreateLogStream("/my/group", "stream")
				_, _ = b.PutLogEvents("/my/group", "stream", []cloudwatchlogs.InputLogEvent{
					{Message: "hello world", Timestamp: 1000},
					{Message: "error occurred", Timestamp: 2000},
				})
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

			info, err := b.StartQuery("qid-1", tt.queryString, tt.logGroups, 0, 0)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "qid-1", info.QueryID)
			assert.Equal(t, cloudwatchlogs.QueryStatusComplete, info.Status)
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
				_, _ = b.CreateLogGroup("/grp")
				_, _ = b.CreateLogStream("/grp", "s")
				_, _ = b.PutLogEvents("/grp", "s", []cloudwatchlogs.InputLogEvent{
					{Message: "msg1", Timestamp: 1000},
				})
				_, _ = b.StartQuery("qid-1", "fields @message", []string{"/grp"}, 0, 0)
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

func TestCloudWatchLogsBackend_StopQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name    string
		queryID string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.StartQuery("qid-1", "fields @message", []string{}, 0, 0)
			},
			queryID: "qid-1",
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

			err := b.StopQuery(tt.queryID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify status is now Cancelled.
			_, _, status, getErr := b.GetQueryResults(tt.queryID)
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
				_, _ = b.CreateLogGroup("/grp")
				_, _ = b.StartQuery("q1", "fields @message", []string{"/grp"}, 0, 0)
				_, _ = b.StartQuery("q2", "fields @timestamp", []string{"/grp"}, 0, 0)
			},
			wantLen: 2,
		},
		{
			name: "filter_by_group",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup("/grp1")
				_, _ = b.CreateLogGroup("/grp2")
				_, _ = b.StartQuery("q1", "fields @message", []string{"/grp1"}, 0, 0)
				_, _ = b.StartQuery("q2", "fields @message", []string{"/grp2"}, 0, 0)
			},
			logGroupName: "/grp1",
			wantLen:      1,
		},
		{
			name: "filter_by_status",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.StartQuery("q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("q2", "fields @message", []string{}, 0, 0)
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
				_, _ = b.StartQuery("old-1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("old-2", "fields @message", []string{}, 0, 0)
				// Sleep well beyond the TTL to avoid any scheduling jitter.
				time.Sleep(20 * time.Millisecond)
				// This new query triggers eviction; old-1 and old-2 should be removed.
				_, _ = b.StartQuery("new-1", "fields @message", []string{}, 0, 0)
			},
			wantLen: 1,
		},
		{
			name: "no_eviction_before_ttl",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				// Use a very long TTL so nothing expires.
				b.SetQueryTTL(time.Hour)
				_, _ = b.StartQuery("q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("q2", "fields @message", []string{}, 0, 0)
			},
			wantLen: 2,
		},
		{
			name: "ttl_disabled",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				b.SetQueryTTL(0)
				_, _ = b.StartQuery("q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("q2", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("q3", "fields @message", []string{}, 0, 0)
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
				_, _ = b.StartQuery("first", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("second", "fields @message", []string{}, 0, 0)
				// This triggers eviction of the oldest ("first").
				_, _ = b.StartQuery("third", "fields @message", []string{}, 0, 0)
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
				_, _ = b.StartQuery("q1", "fields @message", []string{}, 0, 0)
				_, _ = b.StartQuery("q2", "fields @message", []string{}, 0, 0)
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
					_, _ = b.StartQuery(fmt.Sprintf("q%d", i), "fields @message", []string{}, 0, 0)
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

func TestCloudWatchLogsBackend_SetRetentionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		days    *int32
		name    string
		group   string
	}{
		{
			name: "set_retention",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup("grp")
				require.NoError(t, err)
			},
			group: "grp",
			days:  ptr32(30),
		},
		{
			name: "clear_retention",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup("grp")
				require.NoError(t, err)
				require.NoError(t, b.SetRetentionPolicy("grp", ptr32(30)))
			},
			group: "grp",
			days:  nil,
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			days:    ptr32(7),
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.SetRetentionPolicy(tt.group, tt.days)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify the retention is reflected in DescribeLogGroups.
			groups, _, gErr := b.DescribeLogGroups("", "", 100)
			require.NoError(t, gErr)
			require.Len(t, groups, 1)

			if tt.days == nil {
				assert.Nil(t, groups[0].RetentionInDays)
			} else {
				require.NotNil(t, groups[0].RetentionInDays)
				assert.Equal(t, *tt.days, *groups[0].RetentionInDays)
			}
		})
	}
}

func ptr32(v int32) *int32 {
	r := v

	return &r
}

func TestCloudWatchLogsBackend_PutLogEvents_EventCap(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup("g")
	require.NoError(t, err)
	_, err = b.CreateLogStream("g", "s")
	require.NoError(t, err)

	// Write MaxEventsPerStream + 100 events in batches to trigger the cap.
	const batchSize = 1000
	now := time.Now().UnixMilli()
	batches := (cloudwatchlogs.MaxEventsPerStream + 100) / batchSize
	for i := range batches {
		events := make([]cloudwatchlogs.InputLogEvent, batchSize)
		for j := range batchSize {
			events[j] = cloudwatchlogs.InputLogEvent{
				Message:   fmt.Sprintf("msg-%d-%d", i, j),
				Timestamp: now + int64(i*batchSize+j),
			}
		}
		_, putErr := b.PutLogEvents("g", "s", events)
		require.NoError(t, putErr)
	}

	// Only MaxEventsPerStream events should remain.
	got, _, _, err := b.GetLogEvents("g", "s", nil, nil, cloudwatchlogs.MaxEventsPerStream+200, "")
	require.NoError(t, err)
	assert.LessOrEqual(t, len(got), cloudwatchlogs.MaxEventsPerStream)
}

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
			name:    "negation_term_present",
			pattern: "?DEBUG ERROR",
			message: "ERROR but not debug",
			want:    true,
		},
		{
			name:    "negation_term_excluded",
			pattern: "?DEBUG",
			message: "DEBUG: verbose log",
			want:    false,
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

func TestCloudWatchLogsBackend_RecordsMatched(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup("g")
	require.NoError(t, err)
	_, err = b.CreateLogStream("g", "s")
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	events := []cloudwatchlogs.InputLogEvent{
		{Message: "ERROR: oops", Timestamp: now},
		{Message: "INFO: ok", Timestamp: now + 1},
		{Message: "ERROR: again", Timestamp: now + 2},
	}
	_, err = b.PutLogEvents("g", "s", events)
	require.NoError(t, err)

	// Query with a filter that matches only 2 of the 3 events.
	_, err = b.StartQuery("q1",
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

func TestJanitor_SweepRetention(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup("g")
	require.NoError(t, err)
	_, err = b.CreateLogStream("g", "s")
	require.NoError(t, err)

	// Events from 10 days ago (should be evicted with 7-day retention).
	old := time.Now().AddDate(0, 0, -10).UnixMilli()
	// Recent events (should be kept).
	recent := time.Now().UnixMilli()

	events := []cloudwatchlogs.InputLogEvent{
		{Message: "old-1", Timestamp: old},
		{Message: "old-2", Timestamp: old + 1},
		{Message: "recent-1", Timestamp: recent},
	}
	_, err = b.PutLogEvents("g", "s", events)
	require.NoError(t, err)

	// Set retention to 7 days.
	require.NoError(t, b.SetRetentionPolicy("g", ptr32(7)))

	// Run janitor sweep.
	j := cloudwatchlogs.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	// Only recent events should remain.
	got, _, _, err := b.GetLogEvents("g", "s", nil, nil, 100, "")
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, "recent-1", got[0].Message)
}

func TestJanitor_SweepNoRetention(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup("g")
	require.NoError(t, err)
	_, err = b.CreateLogStream("g", "s")
	require.NoError(t, err)

	old := time.Now().AddDate(0, 0, -10).UnixMilli()
	_, err = b.PutLogEvents("g", "s", []cloudwatchlogs.InputLogEvent{
		{Message: "old", Timestamp: old},
	})
	require.NoError(t, err)

	// No retention policy set — janitor should leave events untouched.
	j := cloudwatchlogs.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	got, _, _, err := b.GetLogEvents("g", "s", nil, nil, 100, "")
	require.NoError(t, err)
	assert.Len(t, got, 1)
}
