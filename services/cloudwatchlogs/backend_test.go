package cloudwatchlogs_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
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
				_, err := b.CreateLogGroup(context.Background(), "dup-group", "", "")
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

			g, err := b.CreateLogGroup(context.Background(), tt.createGroup, "", "")

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
				_, err := b.CreateLogGroup(context.Background(), "to-delete", "", "")
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

			err := b.DeleteLogGroup(context.Background(), tt.group)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 0)
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
				_, _ = b.CreateLogGroup(context.Background(), "/prod/app", "", "")
				_, _ = b.CreateLogGroup(context.Background(), "/dev/app", "", "")
			},
			prefix:        "/prod",
			wantCount:     1,
			wantFirstName: "/prod/app",
		},
		{
			name: "beyond_end",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/group/a", "", "")
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

			groups, next, err := b.DescribeLogGroups(
				context.Background(),
				tt.prefix,
				tt.token,
				tt.limit,
			)
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
		_, _ = b.CreateLogGroup(context.Background(), "/group/"+string(rune('a'+i)), "", "")
	}

	page1, token, err := b.DescribeLogGroups(context.Background(), "", "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, token)

	page2, token2, err := b.DescribeLogGroups(context.Background(), "", token, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, token2)

	page3, token3, err := b.DescribeLogGroups(context.Background(), "", token2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3)
}

func TestCloudWatchLogsBackend_PaginationToken_Opaque(t *testing.T) {
	t.Parallel()

	// Verify that emitted nextTokens are not plain decimal integers (opaque encoding).
	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	for i := range 5 {
		_, _ = b.CreateLogGroup(context.Background(), fmt.Sprintf("/grp-%d", i), "", "")
	}

	_, token, err := b.DescribeLogGroups(context.Background(), "", "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	// Token must not be a bare integer string.
	_, parseErr := fmt.Sscanf(token, "%d", new(int))
	require.Error(t, parseErr, "nextToken should be opaque (not a plain integer), got %q", token)

	// Token must be valid base64.
	_, decodeErr := base64.StdEncoding.DecodeString(token)
	assert.NoError(t, decodeErr, "nextToken should be base64-encoded, got %q", token)
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
				_, _ = b.CreateLogGroup(context.Background(), "my-group", "", "")
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "dup")
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

			s, err := b.CreateLogStream(context.Background(), tt.group, tt.stream)

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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "stream-a")
				_, _ = b.CreateLogStream(context.Background(), "grp", "stream-b")
			},
			group:     "grp",
			wantCount: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "prod-stream")
				_, _ = b.CreateLogStream(context.Background(), "grp", "dev-stream")
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

			streams, next, err := b.DescribeLogStreams(
				context.Background(),
				tt.group,
				tt.prefix,
				"",
				"",
				false,
				0,
			)

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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
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

			token, err := b.PutLogEvents(context.Background(), tt.group, tt.stream, "", tt.events)

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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"stream",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "msg1", Timestamp: 1000},
						{Message: "msg2", Timestamp: 2000},
						{Message: "msg3", Timestamp: 3000},
					},
				)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"stream",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "old", Timestamp: 100},
						{Message: "new", Timestamp: 5000},
					},
				)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
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

			evts, fwd, bwd, err := b.GetLogEvents(
				context.Background(),
				tt.group,
				tt.stream,
				tt.startTime,
				tt.endTime,
				tt.limit,
				tt.nextToken,
				true,
			)

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

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_, _ = b.PutLogEvents(context.Background(), "grp", "stream", "", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 1},
		{Message: "b", Timestamp: 2},
		{Message: "c", Timestamp: 3},
	})

	evts, fwd, _, err := b.GetLogEvents(
		context.Background(),
		"grp",
		"stream",
		nil,
		nil,
		2,
		"",
		true,
	)
	require.NoError(t, err)
	assert.Len(t, evts, 2)

	evts2, _, _, err := b.GetLogEvents(
		context.Background(),
		"grp",
		"stream",
		nil,
		nil,
		2,
		fwd,
		true,
	)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "s1")
				_, _ = b.CreateLogStream(context.Background(), "grp", "s2")
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"s1",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "ERROR: something bad", Timestamp: 1000},
					},
				)
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"s2",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "INFO: all good", Timestamp: 2000},
					},
				)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "s1")
				_, _ = b.CreateLogStream(context.Background(), "grp", "s2")
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"s1",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "from s1", Timestamp: 1000},
					},
				)
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"s2",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "from s2", Timestamp: 2000},
					},
				)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_, _ = b.CreateLogStream(context.Background(), "grp", "s")
				_, _ = b.PutLogEvents(
					context.Background(),
					"grp",
					"s",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "old", Timestamp: 100},
						{Message: "new", Timestamp: 9000},
					},
				)
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

			evts, _, _, err := b.FilterLogEvents(
				context.Background(),
				cloudwatchlogs.FilterLogEventsParams{
					GroupName:     tt.group,
					StreamNames:   tt.streams,
					FilterPattern: tt.pattern,
					StartTime:     tt.startTime,
					EndTime:       tt.endTime,
					Limit:         tt.limit,
					NextToken:     tt.nextToken,
				},
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

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "s")

	for i := range 5 {
		_, _ = b.PutLogEvents(context.Background(), "grp", "s", "", []cloudwatchlogs.InputLogEvent{
			{Message: "msg", Timestamp: int64(i * 100)},
		})
	}

	evts, token, _, err := b.FilterLogEvents(
		context.Background(), cloudwatchlogs.FilterLogEventsParams{GroupName: "grp", Limit: 2})
	require.NoError(t, err)
	assert.Len(t, evts, 2)
	assert.NotEmpty(t, token)

	evts2, _, _, err := b.FilterLogEvents(
		context.Background(),
		cloudwatchlogs.FilterLogEventsParams{GroupName: "grp", Limit: 10, NextToken: token},
	)
	require.NoError(t, err)
	assert.Len(t, evts2, 3)
}

func TestCloudWatchLogsBackend_FilterLogEvents_EventShape(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "s1")
	_, _ = b.CreateLogStream(context.Background(), "grp", "s2")
	_, _ = b.PutLogEvents(context.Background(), "grp", "s1", "", []cloudwatchlogs.InputLogEvent{
		{Message: "from s1", Timestamp: 2000},
	})
	_, _ = b.PutLogEvents(context.Background(), "grp", "s2", "", []cloudwatchlogs.InputLogEvent{
		{Message: "from s2", Timestamp: 1000},
	})

	evts, _, searched, err := b.FilterLogEvents(
		context.Background(), cloudwatchlogs.FilterLogEventsParams{GroupName: "grp"})
	require.NoError(t, err)
	require.Len(t, evts, 2)

	// Interleaved across streams and sorted ascending by timestamp.
	assert.Equal(t, "from s2", evts[0].Message)
	assert.Equal(t, "s2", evts[0].LogStreamName)
	assert.Equal(t, "from s1", evts[1].Message)
	assert.Equal(t, "s1", evts[1].LogStreamName)

	// Each event carries a non-empty, unique eventId.
	assert.NotEmpty(t, evts[0].EventID)
	assert.NotEmpty(t, evts[1].EventID)
	assert.NotEqual(t, evts[0].EventID, evts[1].EventID)

	// searchedLogStreams is present (AWS returns an empty list).
	assert.NotNil(t, searched)
	assert.Empty(t, searched)
}

func TestCloudWatchLogsBackend_FilterLogEvents_StreamNamePrefix(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	for _, s := range []string{"app-1", "app-2", "sys-1"} {
		_, _ = b.CreateLogStream(context.Background(), "grp", s)
		_, _ = b.PutLogEvents(context.Background(), "grp", s, "", []cloudwatchlogs.InputLogEvent{
			{Message: "msg from " + s, Timestamp: 1000},
		})
	}

	evts, _, _, err := b.FilterLogEvents(context.Background(), cloudwatchlogs.FilterLogEventsParams{
		GroupName:           "grp",
		LogStreamNamePrefix: "app-",
	})
	require.NoError(t, err)
	require.Len(t, evts, 2)
	for _, e := range evts {
		assert.True(t, strings.HasPrefix(e.LogStreamName, "app-"))
	}
}

func TestCloudWatchLogsBackend_FilterLogEvents_PrefixAndNamesMutuallyExclusive(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")

	_, _, _, err := b.FilterLogEvents(context.Background(), cloudwatchlogs.FilterLogEventsParams{
		GroupName:           "grp",
		StreamNames:         []string{"s1"},
		LogStreamNamePrefix: "s",
	})
	require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
}

func TestCloudWatchLogsBackend_PutLogEvents_UpdatesTimestamps(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "s")

	_, _ = b.PutLogEvents(context.Background(), "grp", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 500},
		{Message: "b", Timestamp: 1500},
	})

	streams, _, err := b.DescribeLogStreams(context.Background(), "grp", "", "", "", false, 0)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:old",
					"",
					"",
				)
			},
			group:          "grp",
			filterName:     "f",
			filterPattern:  "ERROR",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:new",
		},
		{
			name:           "group_not_found",
			group:          "nonexistent",
			filterName:     "f",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:fn",
			wantErr:        cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "limit_exceeded",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f1",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f2",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:b",
					"",
					"",
				)
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

			err := b.PutSubscriptionFilter(
				context.Background(),
				tt.group,
				tt.filterName,
				tt.filterPattern,
				tt.destinationArn,
				"",
				"",
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, err := b.DescribeSubscriptionFilters(
				context.Background(),
				tt.group,
				"",
				"",
				0,
			)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"filter-a",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"filter-b",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:b",
					"",
					"",
				)
			},
			group:     "grp",
			wantCount: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"prod-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"", "",
				)
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"dev-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:b",
					"",
					"",
				)
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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
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

			filters, _, err := b.DescribeSubscriptionFilters(
				context.Background(),
				tt.group,
				tt.prefix,
				tt.nextToken,
				tt.limit,
			)

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
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"my-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
			},
			group:      "grp",
			filterName: "my-filter",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
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

			err := b.DeleteSubscriptionFilter(context.Background(), tt.group, tt.filterName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, ferr := b.DescribeSubscriptionFilters(
				context.Background(),
				tt.group,
				"",
				"",
				0,
			)
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

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
		func(_ context.Context, dst string, p []byte) error {
			delivered = append(delivered, deliveredPayload{dst, p})

			return nil
		},
	)

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"my-filter",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:target",
		"",
		"",
	)

	now := time.Now().UnixMilli()
	_, err := b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "hello", Timestamp: now},
		},
	)
	require.NoError(t, err)

	// Wait for the delivery goroutine to finish before asserting.
	b.Drain()

	assert.Len(t, delivered, 1)
	assert.Equal(
		t,
		"arn:aws:lambda:us-east-1:123456789012:function:target",
		delivered[0].destinationArn,
	)
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

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
		func(ctx context.Context, _ string, _ []byte) error {
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
		},
	)

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Limit to workersCap concurrent workers so we can verify the cap is respected.
	b.SetDeliveryWorkers(workersCap)
	b.SetDeliveryTimeout(0) // disable timeout so the hold above doesn't race
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"f",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:fn",
		"",
		"",
	)

	for i := range numEvents {
		_, err := b.PutLogEvents(
			context.Background(),
			"grp",
			"stream",
			"",
			[]cloudwatchlogs.InputLogEvent{
				{Message: fmt.Sprintf("msg-%d", i), Timestamp: int64(i)},
			},
		)
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

func TestCloudWatchLogsBackend_PutLogEvents_SubscriptionDelivery_PerDeliveryTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		timeout              time.Duration
		wantFastCtxCancelled bool
	}{
		{
			name:                 "fresh_timeout_per_delivery",
			timeout:              20 * time.Millisecond,
			wantFastCtxCancelled: false,
		},
		{
			name:                 "timeout_disabled",
			timeout:              0,
			wantFastCtxCancelled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const (
				slowDestination = "arn:aws:lambda:us-east-1:123456789012:function:slow"
				fastDestination = "arn:aws:lambda:us-east-1:123456789012:function:fast"
			)

			var (
				mu               sync.Mutex
				fastCtxCancelled bool
			)
			fastCalled := make(chan struct{}, 1)

			deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
				func(ctx context.Context, dst string, _ []byte) error {
					switch dst {
					case slowDestination:
						if tt.timeout <= 0 {
							return nil
						}
						<-ctx.Done()

						return ctx.Err()
					case fastDestination:
						mu.Lock()
						fastCtxCancelled = ctx.Err() != nil
						mu.Unlock()
						select {
						case fastCalled <- struct{}{}:
						default:
						}
					}

					return nil
				},
			)

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			b.SetDeliveryTimeout(tt.timeout)
			b.SetSubscriptionDeliverer(deliverer)

			_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
			_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
			_ = b.PutSubscriptionFilter(
				context.Background(),
				"grp",
				"slow-filter",
				"",
				slowDestination,
				"",
				"",
			)
			_ = b.PutSubscriptionFilter(
				context.Background(),
				"grp",
				"fast-filter",
				"",
				fastDestination,
				"",
				"",
			)

			_, err := b.PutLogEvents(
				context.Background(),
				"grp",
				"stream",
				"",
				[]cloudwatchlogs.InputLogEvent{
					{Message: "hello", Timestamp: 1},
				},
			)
			require.NoError(t, err)

			b.Drain()

			select {
			case <-fastCalled:
			default:
				require.FailNow(t, "expected fast destination delivery call")
			}

			mu.Lock()
			assert.Equal(t, tt.wantFastCtxCancelled, fastCtxCancelled)
			mu.Unlock()
		})
	}
}

func TestCloudWatchLogsBackend_Close_CancelsInFlightDeliveries(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	deliveryCancelled := make(chan struct{}, 1)

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
		func(ctx context.Context, _ string, _ []byte) error {
			// Signal that the delivery goroutine has started and is in progress.
			close(started)
			// Block until the context is cancelled.
			<-ctx.Done()
			select {
			case deliveryCancelled <- struct{}{}:
			default:
			}

			return ctx.Err()
		},
	)

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetDeliveryTimeout(0) // disable timeout so Close() is the only cancellation source
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"f",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:fn",
		"",
		"",
	)

	_, err := b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "hello", Timestamp: 1},
		},
	)
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
	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"f",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:a",
		"",
		"",
	)
	require.NoError(t, b.DeleteLogGroup(context.Background(), "grp"))

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	filters, _, err := b.DescribeSubscriptionFilters(context.Background(), "grp", "", "", 0)
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
				_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
				require.NoError(t, err)
			},
			group: "grp",
			days:  ptr32(30),
		},
		{
			name: "clear_retention",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
				require.NoError(t, err)
				require.NoError(t, b.SetRetentionPolicy(context.Background(), "grp", ptr32(30)))
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

			err := b.SetRetentionPolicy(context.Background(), tt.group, tt.days)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify the retention is reflected in DescribeLogGroups.
			groups, _, gErr := b.DescribeLogGroups(context.Background(), "", "", 100)
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
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)

	// Write MaxEventsPerStream + 500 events (guaranteed overflow) in batches.
	const batchSize = 1000
	const total = cloudwatchlogs.MaxEventsPerStream + 500
	now := time.Now().UnixMilli()
	written := 0
	for written < total {
		size := batchSize
		if written+size > total {
			size = total - written
		}
		events := make([]cloudwatchlogs.InputLogEvent, size)
		for j := range size {
			events[j] = cloudwatchlogs.InputLogEvent{
				Message:   fmt.Sprintf("msg-%d", written+j),
				Timestamp: now + int64(written+j),
			}
		}
		_, putErr := b.PutLogEvents(context.Background(), "g", "s", "", events)
		require.NoError(t, putErr)
		written += size
	}

	// Exactly MaxEventsPerStream events should remain (the newest ones).
	got, _, _, err := b.GetLogEvents(
		context.Background(),
		"g",
		"s",
		nil,
		nil,
		cloudwatchlogs.MaxEventsPerStream+1000,
		"",
		true,
	)
	require.NoError(t, err)
	assert.Len(t, got, cloudwatchlogs.MaxEventsPerStream)

	// The oldest events (msg-0 through msg-499) should have been dropped.
	// The newest events should be present: msg-500 through msg-10499.
	assert.Equal(t, fmt.Sprintf("msg-%d", 500), got[0].Message)
	assert.Equal(t, fmt.Sprintf("msg-%d", total-1), got[len(got)-1].Message)

	// FirstEventTimestamp should reflect the oldest retained event.
	streams, _, sErr := b.DescribeLogStreams(context.Background(), "g", "", "", "", false, 10)
	require.NoError(t, sErr)
	require.Len(t, streams, 1)
	require.NotNil(t, streams[0].FirstEventTimestamp)
	assert.Equal(t, now+500, *streams[0].FirstEventTimestamp)
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

func TestJanitor_SweepRetention(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
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
	_, err = b.PutLogEvents(context.Background(), "g", "s", "", events)
	require.NoError(t, err)

	// Set retention to 7 days.
	require.NoError(t, b.SetRetentionPolicy(context.Background(), "g", ptr32(7)))

	// Run janitor sweep.
	j := cloudwatchlogs.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	// Only recent events should remain.
	got, _, _, err := b.GetLogEvents(context.Background(), "g", "s", nil, nil, 100, "", true)
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, "recent-1", got[0].Message)
}

func TestJanitor_SweepNoRetention(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)

	old := time.Now().AddDate(0, 0, -10).UnixMilli()
	_, err = b.PutLogEvents(context.Background(), "g", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: "old", Timestamp: old},
	})
	require.NoError(t, err)

	// No retention policy set — janitor should leave events untouched.
	j := cloudwatchlogs.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	got, _, _, err := b.GetLogEvents(context.Background(), "g", "s", nil, nil, 100, "", true)
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

func TestJanitor_SweepUpdatesStreamMetadata(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)

	// Old events (before retention cutoff).
	old := time.Now().AddDate(0, 0, -10).UnixMilli()
	// Recent event (within retention window).
	recent := time.Now().UnixMilli()

	_, err = b.PutLogEvents(context.Background(), "g", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: "old", Timestamp: old},
		{Message: "recent", Timestamp: recent},
	})
	require.NoError(t, err)

	// Set 7-day retention.
	require.NoError(t, b.SetRetentionPolicy(context.Background(), "g", ptr32(7)))

	j := cloudwatchlogs.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	// Stream metadata should reflect only the remaining (recent) event.
	streams, _, sErr := b.DescribeLogStreams(context.Background(), "g", "", "", "", false, 10)
	require.NoError(t, sErr)
	require.Len(t, streams, 1)
	require.NotNil(t, streams[0].FirstEventTimestamp)
	assert.Equal(t, recent, *streams[0].FirstEventTimestamp)
	require.NotNil(t, streams[0].LastEventTimestamp)
	assert.Equal(t, recent, *streams[0].LastEventTimestamp)
}

func TestJanitor_SweepEmptyStreamClearsMetadata(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)

	// Only old events (all should be evicted).
	old := time.Now().AddDate(0, 0, -10).UnixMilli()
	_, err = b.PutLogEvents(context.Background(), "g", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: "old", Timestamp: old},
	})
	require.NoError(t, err)

	require.NoError(t, b.SetRetentionPolicy(context.Background(), "g", ptr32(7)))

	j := cloudwatchlogs.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	// All events gone — stream metadata should be cleared.
	streams, _, sErr := b.DescribeLogStreams(context.Background(), "g", "", "", "", false, 10)
	require.NoError(t, sErr)
	require.Len(t, streams, 1)
	assert.Nil(t, streams[0].FirstEventTimestamp)
	assert.Nil(t, streams[0].LastEventTimestamp)
}

func TestCloudWatchLogsBackend_DeleteLogStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *cloudwatchlogs.InMemoryBackend)
		name    string
		group   string
		stream  string
	}{
		{
			name:    "group_not_found",
			group:   "nonexistent",
			stream:  "s",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "stream_not_found",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
			},
			group:   "g",
			stream:  "nonexistent",
			wantErr: cloudwatchlogs.ErrLogStreamNotFound,
		},
		{
			name: "success",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
				_, _ = b.CreateLogStream(context.Background(), "g", "s")
			},
			group:  "g",
			stream: "s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteLogStream(context.Background(), tt.group, tt.stream)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify stream and events are gone.
			streams, _, sErr := b.DescribeLogStreams(
				context.Background(),
				tt.group,
				"",
				"",
				"",
				false,
				100,
			)
			require.NoError(t, sErr)
			assert.Empty(t, streams)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogEvents_StartFromHead(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)

	// Put 5 events with recent timestamps (ascending).
	base := time.Now().UnixMilli()
	events := []cloudwatchlogs.InputLogEvent{
		{Message: "e1", Timestamp: base + 1},
		{Message: "e2", Timestamp: base + 2},
		{Message: "e3", Timestamp: base + 3},
		{Message: "e4", Timestamp: base + 4},
		{Message: "e5", Timestamp: base + 5},
	}
	_, err = b.PutLogEvents(context.Background(), "g", "s", "", events)
	require.NoError(t, err)

	// startFromHead=true, limit=2: should return oldest 2 events.
	got, _, _, err := b.GetLogEvents(context.Background(), "g", "s", nil, nil, 2, "", true)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "e1", got[0].Message)
	assert.Equal(t, "e2", got[1].Message)

	// startFromHead=false (AWS default), limit=2: should return newest 2 events.
	got, _, _, err = b.GetLogEvents(context.Background(), "g", "s", nil, nil, 2, "", false)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "e4", got[0].Message)
	assert.Equal(t, "e5", got[1].Message)

	// nextToken takes precedence over startFromHead.
	got, _, _, err = b.GetLogEvents(context.Background(), "g", "s", nil, nil, 2, "0", false)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "e1", got[0].Message)
}

func TestCloudWatchLogsBackend_DescribeExportTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *cloudwatchlogs.InMemoryBackend)
		taskID     string
		statusCode string
		wantLen    int
		wantErr    bool
	}{
		{
			name: "no_filter_returns_all",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddExportTaskInternal(
					b,
					cloudwatchlogs.ExportTask{TaskID: "t1", Status: "COMPLETED", CreationTime: 1},
				)
				cloudwatchlogs.AddExportTaskInternal(
					b,
					cloudwatchlogs.ExportTask{TaskID: "t2", Status: "RUNNING", CreationTime: 2},
				)
			},
			wantLen: 2,
		},
		{
			name: "filter_by_task_id",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddExportTaskInternal(
					b,
					cloudwatchlogs.ExportTask{TaskID: "t1", Status: "COMPLETED", CreationTime: 1},
				)
				cloudwatchlogs.AddExportTaskInternal(
					b,
					cloudwatchlogs.ExportTask{TaskID: "t2", Status: "RUNNING", CreationTime: 2},
				)
			},
			taskID:  "t1",
			wantLen: 1,
		},
		{
			name: "filter_by_status",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddExportTaskInternal(
					b,
					cloudwatchlogs.ExportTask{TaskID: "t1", Status: "COMPLETED", CreationTime: 1},
				)
				cloudwatchlogs.AddExportTaskInternal(
					b,
					cloudwatchlogs.ExportTask{TaskID: "t2", Status: "RUNNING", CreationTime: 2},
				)
			},
			statusCode: "COMPLETED",
			wantLen:    1,
		},
		{
			name:    "empty_returns_empty",
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tasks, _, err := b.DescribeExportTasks(tt.taskID, tt.statusCode, 50, "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tasks, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeImportTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *cloudwatchlogs.InMemoryBackend)
		taskID  string
		wantLen int
		wantErr bool
	}{
		{
			name: "no_filter_returns_all",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddImportTaskInternal(
					b,
					cloudwatchlogs.ImportTask{ImportID: "i1", CreationTime: 1},
				)
				cloudwatchlogs.AddImportTaskInternal(
					b,
					cloudwatchlogs.ImportTask{ImportID: "i2", CreationTime: 2},
				)
			},
			wantLen: 2,
		},
		{
			name: "filter_by_task_id",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddImportTaskInternal(
					b,
					cloudwatchlogs.ImportTask{ImportID: "i1", CreationTime: 1},
				)
				cloudwatchlogs.AddImportTaskInternal(
					b,
					cloudwatchlogs.ImportTask{ImportID: "i2", CreationTime: 2},
				)
			},
			taskID:  "i1",
			wantLen: 1,
		},
		{
			name:    "empty_returns_empty",
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tasks, _, err := b.DescribeImportTasks(tt.taskID, 50, "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tasks, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeDeliveries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *cloudwatchlogs.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name: "returns_all",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddDeliveryInternal(
					b,
					cloudwatchlogs.Delivery{ID: "d1", CreationTime: 1},
				)
				cloudwatchlogs.AddDeliveryInternal(
					b,
					cloudwatchlogs.Delivery{ID: "d2", CreationTime: 2},
				)
			},
			wantLen: 2,
		},
		{
			name:    "empty_returns_empty",
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			deliveries, _, err := b.DescribeDeliveries(50, "")
			require.NoError(t, err)
			assert.Len(t, deliveries, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_GetAndDeleteDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *cloudwatchlogs.InMemoryBackend)
		name    string
		id      string
	}{
		{
			name: "get_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddDeliveryInternal(b, cloudwatchlogs.Delivery{ID: "d1"})
			},
			id: "d1",
		},
		{
			name:    "get_missing",
			id:      "nonexistent",
			wantErr: cloudwatchlogs.ErrDeliveryNotFound,
		},
		{
			name:    "get_empty_id",
			id:      "",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			d, err := b.GetDelivery(tt.id)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.id, d.ID)
		})
	}

	t.Run("delete_existing", func(t *testing.T) {
		t.Parallel()

		b := cloudwatchlogs.NewInMemoryBackend()
		cloudwatchlogs.AddDeliveryInternal(b, cloudwatchlogs.Delivery{ID: "d1"})

		err := b.DeleteDelivery("d1")
		require.NoError(t, err)

		_, err = b.GetDelivery("d1")
		require.ErrorIs(t, err, cloudwatchlogs.ErrDeliveryNotFound)
	})

	t.Run("delete_missing", func(t *testing.T) {
		t.Parallel()

		b := cloudwatchlogs.NewInMemoryBackend()
		err := b.DeleteDelivery("nonexistent")
		require.ErrorIs(t, err, cloudwatchlogs.ErrDeliveryNotFound)
	})
}

func TestCloudWatchLogsBackend_LogAnomalyDetectorLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *cloudwatchlogs.InMemoryBackend)
		name    string
		arnToOp string
		op      string
		newFreq string
	}{
		{
			name: "list_all",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/1",
					CreationTimeStamp:  1,
				})
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/2",
					CreationTimeStamp:  2,
				})
			},
			op: "list",
		},
		{
			name: "delete_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/1",
				})
			},
			op:      "delete",
			arnToOp: "arn:aws:logs:::detector/1",
		},
		{
			name:    "delete_missing",
			op:      "delete",
			arnToOp: "arn:aws:logs:::detector/nonexistent",
			wantErr: cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name: "update_freq",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn:  "arn:aws:logs:::detector/1",
					EvaluationFrequency: "FIVE_MIN",
				})
			},
			op:      "update",
			arnToOp: "arn:aws:logs:::detector/1",
			newFreq: "ONE_HOUR",
		},
		{
			name: "update_invalid_freq",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
					AnomalyDetectorArn: "arn:aws:logs:::detector/1",
				})
			},
			op:      "update",
			arnToOp: "arn:aws:logs:::detector/1",
			newFreq: "INVALID",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "list":
				var detectors []cloudwatchlogs.LogAnomalyDetector
				detectors, _, err = b.ListLogAnomalyDetectors(nil, 50, "")
				require.NoError(t, err)
				assert.Len(t, detectors, 2)

				return
			case "delete":
				err = b.DeleteLogAnomalyDetector(tt.arnToOp)
			case "update":
				err = b.UpdateLogAnomalyDetector(tt.arnToOp, tt.newFreq, 0)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_ScheduledQueryLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *cloudwatchlogs.InMemoryBackend)
		name     string
		arn      string
		newState string
		op       string
	}{
		{
			name: "create_and_list",
			op:   "list",
		},
		{
			name: "delete_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateScheduledQuery(
					"q1",
					"fields @message",
					"cron(0 * * * ? *)",
					"",
					"ENABLED",
				)
			},
			op: "delete_first",
		},
		{
			name:    "delete_missing",
			op:      "delete_direct",
			arn:     "arn:nonexistent",
			wantErr: cloudwatchlogs.ErrScheduledQueryNotFound,
		},
		{
			name: "update_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateScheduledQuery(
					"q1",
					"fields @message",
					"cron(0 * * * ? *)",
					"",
					"ENABLED",
				)
			},
			op:       "update_first",
			newState: "DISABLED",
		},
		{
			name: "update_invalid_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateScheduledQuery(
					"q1",
					"fields @message",
					"cron(0 * * * ? *)",
					"",
					"ENABLED",
				)
			},
			op:       "update_first",
			newState: "INVALID",
			wantErr:  cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "list":
				_, _ = b.CreateScheduledQuery(
					"q1",
					"fields @message",
					"cron(0 * * * ? *)",
					"",
					"ENABLED",
				)
				var queries []cloudwatchlogs.ScheduledQuery
				queries, _, err = b.ListScheduledQueries(50, "")
				require.NoError(t, err)
				assert.Len(t, queries, 1)

				return
			case "delete_first":
				var queries []cloudwatchlogs.ScheduledQuery
				queries, _, err = b.ListScheduledQueries(50, "")
				require.NoError(t, err)
				require.Len(t, queries, 1)
				err = b.DeleteScheduledQuery(queries[0].Arn)
			case "delete_direct":
				err = b.DeleteScheduledQuery(tt.arn)
			case "update_first":
				var queries []cloudwatchlogs.ScheduledQuery
				queries, _, err = b.ListScheduledQueries(50, "")
				require.NoError(t, err)
				require.Len(t, queries, 1)
				err = b.UpdateScheduledQuery(queries[0].Arn, tt.newState)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_PutAndDescribeAccountPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *cloudwatchlogs.InMemoryBackend)
		name       string
		policyName string
		policyType string
		policyDoc  string
		wantLen    int
		callPut    bool
	}{
		{
			name:       "create_and_describe_all",
			policyName: "my-policy",
			policyType: "DATA_PROTECTION_POLICY",
			policyDoc:  `{"version":"2021-06-01"}`,
			callPut:    true,
			wantLen:    1,
		},
		{
			name: "describe_filtered_by_type",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.PutAccountPolicy("p1", "DATA_PROTECTION_POLICY", "{}", "", "")
				_, _ = b.PutAccountPolicy("p2", "SUBSCRIPTION_FILTER_POLICY", "{}", "", "")
			},
			policyType: "DATA_PROTECTION_POLICY",
			wantLen:    1,
		},
		{
			name:       "invalid_policy_type",
			policyName: "p",
			policyType: "INVALID_TYPE",
			callPut:    true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:       "missing_name",
			policyName: "",
			policyType: "DATA_PROTECTION_POLICY",
			callPut:    true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			if tt.callPut {
				_, err := b.PutAccountPolicy(tt.policyName, tt.policyType, tt.policyDoc, "", "")
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}
				require.NoError(t, err)
			}

			policies, _, err := b.DescribeAccountPolicies(tt.policyType, "", nil, 0, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Len(t, policies, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_DisassociateKmsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr            error
		name               string
		logGroupName       string
		resourceIdentifier string
	}{
		{
			name:         "by_log_group",
			logGroupName: "my-group",
		},
		{
			name:               "by_resource_id",
			resourceIdentifier: "arn:aws:logs:::query-results",
		},
		{
			name:    "both_empty_fails",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.logGroupName != "" {
				err := b.AssociateKmsKey(tt.logGroupName, "", "arn:aws:kms:::key/1")
				require.NoError(t, err)
			}
			if tt.resourceIdentifier != "" {
				err := b.AssociateKmsKey("", tt.resourceIdentifier, "arn:aws:kms:::key/1")
				require.NoError(t, err)
			}

			err := b.DisassociateKmsKey(tt.logGroupName, tt.resourceIdentifier)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_MetricFilterLifecycle(t *testing.T) {
	t.Parallel()

	transformation := cloudwatchlogs.MetricTransformation{
		MetricName:      "ErrorCount",
		MetricNamespace: "MyApp",
		MetricValue:     "1",
	}

	tests := []struct {
		wantErr    error
		setup      func(b *cloudwatchlogs.InMemoryBackend)
		name       string
		groupName  string
		filterName string
		pattern    string
		op         string
		transforms []cloudwatchlogs.MetricTransformation
		wantLen    int
	}{
		{
			name:       "put_and_describe",
			groupName:  "grp",
			filterName: "f1",
			pattern:    "ERROR",
			transforms: []cloudwatchlogs.MetricTransformation{transformation},
			op:         "put_then_describe",
			wantLen:    1,
		},
		{
			name: "describe_with_prefix",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"grp",
					"err-filter",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
				_ = b.PutMetricFilter(
					context.Background(),
					"grp",
					"warn-filter",
					"WARN",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
			},
			op:      "describe_prefix",
			wantLen: 1,
		},
		{
			name: "delete_filter",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"grp",
					"f1",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
			},
			groupName:  "grp",
			filterName: "f1",
			op:         "delete",
		},
		{
			name:       "put_missing_group",
			groupName:  "nonexistent",
			filterName: "f1",
			pattern:    "ERROR",
			transforms: []cloudwatchlogs.MetricTransformation{transformation},
			op:         "put",
			wantErr:    cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name:       "put_missing_filter_name",
			groupName:  "grp",
			filterName: "",
			pattern:    "ERROR",
			transforms: []cloudwatchlogs.MetricTransformation{transformation},
			op:         "put_no_setup",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name: "delete_missing_filter",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
			},
			groupName:  "grp",
			filterName: "nonexistent",
			op:         "delete",
			wantErr:    cloudwatchlogs.ErrMetricFilterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "put_then_describe":
				_, innerErr := b.CreateLogGroup(context.Background(), tt.groupName, "", "")
				require.NoError(t, innerErr)
				err = b.PutMetricFilter(
					context.Background(),
					tt.groupName,
					tt.filterName,
					tt.pattern,
					tt.transforms,
				)
				require.NoError(t, err)
				var filters []cloudwatchlogs.MetricFilter
				filters, _, err = b.DescribeMetricFilters(
					context.Background(),
					tt.groupName,
					"",
					"",
					"",
					"",
					50,
				)
				require.NoError(t, err)
				assert.Len(t, filters, tt.wantLen)

				return
			case "describe_prefix":
				var filters []cloudwatchlogs.MetricFilter
				filters, _, err = b.DescribeMetricFilters(
					context.Background(),
					"grp",
					"err",
					"",
					"",
					"",
					50,
				)
				require.NoError(t, err)
				assert.Len(t, filters, tt.wantLen)

				return
			case "delete":
				err = b.DeleteMetricFilter(context.Background(), tt.groupName, tt.filterName)
			case "put":
				err = b.PutMetricFilter(
					context.Background(),
					tt.groupName,
					tt.filterName,
					tt.pattern,
					tt.transforms,
				)
			case "put_no_setup":
				err = b.PutMetricFilter(
					context.Background(),
					tt.groupName,
					tt.filterName,
					tt.pattern,
					tt.transforms,
				)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_TestMetricFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		pattern   string
		messages  []string
		wantCount int
	}{
		{
			name:      "matches_substring",
			pattern:   "ERROR",
			messages:  []string{"this is an ERROR message", "this is fine", "another ERROR"},
			wantCount: 2,
		},
		{
			name:      "no_matches",
			pattern:   "CRITICAL",
			messages:  []string{"info message", "debug message"},
			wantCount: 0,
		},
		{
			name:     "empty_pattern",
			pattern:  "",
			messages: []string{"any message"},
			wantErr:  cloudwatchlogs.ErrValidation,
		},
		{
			name:      "empty_messages",
			pattern:   "ERROR",
			messages:  []string{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			matches, err := b.TestMetricFilter(tt.pattern, tt.messages)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, matches, tt.wantCount)
			for i, m := range matches {
				assert.NotEmpty(t, m.EventMessage)
				assert.Positive(t, m.EventNumber)
				assert.NotNil(t, m.ExtractedValues)
				_ = i
			}
		})
	}
}

func TestCloudWatchLogsBackend_QueryDefinitionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(b *cloudwatchlogs.InMemoryBackend)
		name        string
		opName      string
		queryString string
		id          string
		prefix      string
		op          string
		wantLen     int
	}{
		{
			name:        "put_and_describe_all",
			opName:      "my-query",
			queryString: "fields @message | limit 20",
			op:          "put_then_describe",
			wantLen:     1,
		},
		{
			name: "describe_with_prefix",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.PutQueryDefinition(
					"prod-errors",
					"fields @message | filter @message like /ERROR/",
					"",
					nil,
				)
				_, _ = b.PutQueryDefinition("dev-logs", "fields @message | limit 10", "", nil)
			},
			op:      "describe_prefix",
			prefix:  "prod",
			wantLen: 1,
		},
		{
			name: "delete_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.PutQueryDefinition("q1", "fields @message", "", nil)
			},
			op: "delete_first",
		},
		{
			name:    "delete_missing",
			op:      "delete_direct",
			id:      "nonexistent-id",
			wantErr: cloudwatchlogs.ErrQueryDefinitionNotFound,
		},
		{
			name:    "put_missing_name",
			opName:  "",
			op:      "put_direct",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "put_then_describe":
				var id string
				id, err = b.PutQueryDefinition(tt.opName, tt.queryString, "", nil)
				require.NoError(t, err)
				assert.NotEmpty(t, id)
				var defs []cloudwatchlogs.QueryDefinition
				defs, _, err = b.DescribeQueryDefinitions("", 50, "")
				require.NoError(t, err)
				assert.Len(t, defs, tt.wantLen)

				return
			case "describe_prefix":
				var defs []cloudwatchlogs.QueryDefinition
				defs, _, err = b.DescribeQueryDefinitions(tt.prefix, 50, "")
				require.NoError(t, err)
				assert.Len(t, defs, tt.wantLen)

				return
			case "delete_first":
				var defs []cloudwatchlogs.QueryDefinition
				defs, _, err = b.DescribeQueryDefinitions("", 50, "")
				require.NoError(t, err)
				require.Len(t, defs, 1)
				err = b.DeleteQueryDefinition(defs[0].QueryDefinitionID)
			case "delete_direct":
				err = b.DeleteQueryDefinition(tt.id)
			case "put_direct":
				_, err = b.PutQueryDefinition(tt.opName, tt.queryString, "", nil)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_StoredBytesTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		messages        []string
		wantStreamBytes int64
		wantGroupBytes  int64
	}{
		{
			name:            "tracks_bytes_on_put",
			messages:        []string{"hello", "world"},
			wantStreamBytes: 10, // len("hello") + len("world")
			wantGroupBytes:  10,
		},
		{
			name:            "single_message",
			messages:        []string{"test"},
			wantStreamBytes: 4,
			wantGroupBytes:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "g", "s")
			require.NoError(t, err)

			events := make([]cloudwatchlogs.InputLogEvent, len(tt.messages))
			for i, m := range tt.messages {
				events[i] = cloudwatchlogs.InputLogEvent{Message: m, Timestamp: int64(i + 1)}
			}
			_, err = b.PutLogEvents(context.Background(), "g", "s", "", events)
			require.NoError(t, err)

			streams, _, err := b.DescribeLogStreams(
				context.Background(),
				"g",
				"",
				"",
				"",
				false,
				10,
			)
			require.NoError(t, err)
			require.Len(t, streams, 1)
			assert.Equal(t, tt.wantStreamBytes, streams[0].StoredBytes)

			groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 10)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			assert.Equal(t, tt.wantGroupBytes, groups[0].StoredBytes)
		})
	}

	t.Run("delete_stream_subtracts_bytes", func(t *testing.T) {
		t.Parallel()

		b := cloudwatchlogs.NewInMemoryBackend()
		_, err := b.CreateLogGroup(context.Background(), "g", "", "")
		require.NoError(t, err)
		_, err = b.CreateLogStream(context.Background(), "g", "s")
		require.NoError(t, err)

		_, err = b.PutLogEvents(context.Background(), "g", "s", "", []cloudwatchlogs.InputLogEvent{
			{Message: "hello", Timestamp: 1},
		})
		require.NoError(t, err)

		groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 10)
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, int64(5), groups[0].StoredBytes)

		err = b.DeleteLogStream(context.Background(), "g", "s")
		require.NoError(t, err)

		groups, _, err = b.DescribeLogGroups(context.Background(), "", "", 10)
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, int64(0), groups[0].StoredBytes)
	})
}

func TestCloudWatchLogsBackend_MetricFilterCount(t *testing.T) {
	t.Parallel()

	transformation := cloudwatchlogs.MetricTransformation{
		MetricName:      "Errors",
		MetricNamespace: "App",
		MetricValue:     "1",
	}

	tests := []struct {
		setup     func(b *cloudwatchlogs.InMemoryBackend)
		name      string
		wantCount int32
	}{
		{
			name: "no_filters",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
			},
			wantCount: 0,
		},
		{
			name: "two_filters",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"g",
					"f1",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
				_ = b.PutMetricFilter(
					context.Background(),
					"g",
					"f2",
					"WARN",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
			},
			wantCount: 2,
		},
		{
			name: "after_delete",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"g",
					"f1",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
				_ = b.DeleteMetricFilter(context.Background(), "g", "f1")
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 10)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			assert.Equal(t, tt.wantCount, groups[0].MetricFilterCount)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogAnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name        string
		detectorArn string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)

				return arn
			},
		},
		{
			name:        "not_found",
			detectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:nonexistent",
			wantErr:     cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name:    "empty_arn",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.detectorArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			d, err := b.GetLogAnomalyDetector(arn)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, d)
			assert.Equal(t, arn, d.AnomalyDetectorArn)
		})
	}
}

func TestCloudWatchLogsBackend_GetScheduledQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name     string
		queryArn string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateScheduledQuery("q1", "fields @message", "", "", "ENABLED")
				require.NoError(t, err)

				return arn
			},
		},
		{
			name:     "not_found",
			queryArn: "arn:aws:logs:us-east-1:123:scheduled-query:nonexistent",
			wantErr:  cloudwatchlogs.ErrScheduledQueryNotFound,
		},
		{
			name:    "empty_arn",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.queryArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			sq, err := b.GetScheduledQuery(arn)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, sq)
			assert.Equal(t, arn, sq.Arn)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogGroupFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		setup        func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name         string
		logGroupName string
		wantFields   int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "my-group", "", "")
				require.NoError(t, err)
			},
			logGroupName: "my-group",
			wantFields:   4,
		},
		{
			name:         "not_found",
			logGroupName: "nonexistent",
			wantErr:      cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name:    "empty_name",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			fields, err := b.GetLogGroupFields(context.Background(), tt.logGroupName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, fields, tt.wantFields)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name    string
		pointer string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "g", "", "")
				require.NoError(t, err)
				_, err = b.CreateLogStream(context.Background(), "g", "s")
				require.NoError(t, err)
				_, err = b.PutLogEvents(
					context.Background(),
					"g",
					"s",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: "hello world", Timestamp: 1000},
					},
				)
				require.NoError(t, err)
				// Get the ptr from GetLogEvents
				evts, _, _, err := b.GetLogEvents(
					context.Background(),
					"g",
					"s",
					nil,
					nil,
					10,
					"",
					true,
				)
				require.NoError(t, err)
				require.Len(t, evts, 1)

				return evts[0].Ptr
			},
		},
		{
			name:    "invalid_pointer",
			pointer: "not-base64!@#",
			wantErr: cloudwatchlogs.ErrValidation,
		},
		{
			name:    "empty_pointer",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			ptr := tt.pointer
			if tt.setup != nil {
				ptr = tt.setup(t, b)
			}

			record, err := b.GetLogRecord(context.Background(), ptr)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, record, "@message")
			assert.Contains(t, record, "@timestamp")
			assert.Equal(t, "hello world", record["@message"])
		})
	}
}

func TestCloudWatchLogsBackend_ListAnomalies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr            error
		setup              func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name               string
		anomalyDetectorArn string
		wantCount          int
		wantNextEmpty      bool
	}{
		{
			name:               "empty_arn_returns_empty",
			anomalyDetectorArn: "",
			wantNextEmpty:      true,
		},
		{
			name: "valid_detector_no_anomalies",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)

				return arn
			},
			wantNextEmpty: true,
		},
		{
			name: "returns_seeded_anomalies",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-1",
					Description:        "spike in errors",
					FirstSeen:          1000,
					LastSeen:           2000,
					Active:             true,
				})
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-2",
					Description:        "unusual pattern",
					FirstSeen:          3000,
					LastSeen:           4000,
					Active:             true,
				})

				return detectorArn
			},
			wantCount:     2,
			wantNextEmpty: true,
		},
		{
			name: "pagination_returns_token",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				for i := range 5 {
					cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
						AnomalyDetectorArn: detectorArn,
						AnomalyID:          fmt.Sprintf("anomaly-%d", i),
						FirstSeen:          int64(i * 1000),
					})
				}

				return detectorArn
			},
			wantCount:     2,
			wantNextEmpty: false,
		},
		{
			name:               "detector_not_found",
			anomalyDetectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:nonexistent",
			wantErr:            cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.anomalyDetectorArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			limit := 10
			if tt.name == "pagination_returns_token" {
				limit = 2
			}

			anomalies, next, err := b.ListAnomalies(arn, limit, "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, anomalies, tt.wantCount)
			if tt.wantNextEmpty {
				assert.Empty(t, next)
			} else {
				assert.NotEmpty(t, next)
			}
		})
	}
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

func TestCloudWatchLogsBackend_GetScheduledQueryHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name       string
		queryArn   string
		wantMinLen int
	}{
		{
			name: "returns_initial_run_from_create",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateScheduledQuery("q1", "fields @message", "", "", "ENABLED")
				require.NoError(t, err)

				return arn
			},
			wantMinLen: 1,
		},
		{
			name: "returns_seeded_runs",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateScheduledQuery("q2", "fields @message", "", "", "ENABLED")
				require.NoError(t, err)
				cloudwatchlogs.AddScheduledQueryRunInternal(
					b,
					arn,
					cloudwatchlogs.ScheduledQueryRunSummary{
						Arn:            arn,
						RunStatus:      "FAILED",
						ExecutionTime:  500,
						InvocationTime: 400,
					},
				)

				return arn
			},
			wantMinLen: 2,
		},
		{
			name:     "not_found",
			queryArn: "arn:aws:logs:us-east-1:123:scheduled-query:nonexistent",
			wantErr:  cloudwatchlogs.ErrScheduledQueryNotFound,
		},
		{
			name:    "empty_arn",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.queryArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			summaries, next, err := b.GetScheduledQueryHistory(arn, "", 0)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(summaries), tt.wantMinLen)
			assert.Empty(t, next)
		})
	}
}

func TestCloudWatchLogsBackend_UpdateAnomaly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr            error
		setup              func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name               string
		anomalyDetectorArn string
		anomalyID          string
		suppressionType    string
		checkSuppression   bool
	}{
		{
			name: "success_no_suppression",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-1",
					Active:             true,
				})

				return detectorArn
			},
			anomalyID:       "anomaly-1",
			suppressionType: "NO_SUPPRESSION",
		},
		{
			name: "success_limited_suppression_clears_on_no_suppression",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)
				cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
					AnomalyDetectorArn: detectorArn,
					AnomalyID:          "anomaly-suppressed",
					Active:             true,
				})

				return detectorArn
			},
			anomalyID:        "anomaly-suppressed",
			suppressionType:  "LIMITED",
			checkSuppression: true,
		},
		{
			name: "anomaly_not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				detectorArn, err := b.CreateLogAnomalyDetector(
					[]string{"arn:aws:logs:us-east-1:123:log-group:test"}, "det", "", "", "", 0,
				)
				require.NoError(t, err)

				return detectorArn
			},
			anomalyID: "nonexistent-anomaly",
			wantErr:   cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name:               "detector_not_found",
			anomalyDetectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:nonexistent",
			anomalyID:          "anomaly-1",
			wantErr:            cloudwatchlogs.ErrLogAnomalyDetectorNotFound,
		},
		{
			name:      "empty_arn",
			anomalyID: "anomaly-1",
			wantErr:   cloudwatchlogs.ErrValidation,
		},
		{
			name:               "empty_anomaly_id",
			anomalyDetectorArn: "arn:aws:logs:us-east-1:123:log-anomaly-detector:x",
			wantErr:            cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.anomalyDetectorArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			suppressionType := tt.suppressionType
			if suppressionType == "" {
				suppressionType = "NO_SUPPRESSION"
			}

			err := b.UpdateAnomaly(tt.anomalyID, arn, suppressionType)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.checkSuppression {
				// Verify the suppression state was persisted.
				anomalies, _, listErr := b.ListAnomalies(arn, 10, "")
				require.NoError(t, listErr)
				require.Len(t, anomalies, 1)
				assert.Equal(t, suppressionType, anomalies[0].SuppressedState)
				assert.NotZero(t, anomalies[0].SuppressedDate)
			}
		})
	}
}

func TestCloudWatchLogsBackend_ListLogGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		prefix  string
		wantLen int
	}{
		{
			name: "all_groups",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp-a", "", "")
				require.NoError(t, err)
				_, err = b.CreateLogGroup(context.Background(), "grp-b", "", "")
				require.NoError(t, err)
			},
			wantLen: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp-a", "", "")
				require.NoError(t, err)
				_, err = b.CreateLogGroup(context.Background(), "other-b", "", "")
				require.NoError(t, err)
			},
			prefix:  "grp",
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

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			groups, _, err := b.ListLogGroups(context.Background(), tt.prefix, "", 50)

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_ValidLogGroupName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		logGroupName string
	}{
		{
			name:         "valid_name",
			logGroupName: "/aws/lambda/my-function",
		},
		{
			name:         "valid_with_dots_dashes",
			logGroupName: "my.log-group_1",
		},
		{
			name:         "valid_hash",
			logGroupName: "group#1",
		},
		{
			name:         "invalid_space",
			logGroupName: "group name with spaces",
			wantErr:      cloudwatchlogs.ErrValidation,
		},
		{
			name:         "invalid_empty",
			logGroupName: "",
			wantErr:      cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), tt.logGroupName, "", "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogStreams_Ordering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		orderBy    string
		wantFirst  string
		descending bool
	}{
		{
			name:      "by_name_asc",
			orderBy:   "LogStreamName",
			wantFirst: "aaa",
		},
		{
			// AWS rejects descending=true when orderBy is LogStreamName.
			name:       "by_name_desc_invalid",
			orderBy:    "LogStreamName",
			descending: true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:      "by_name_asc_default_orderby",
			orderBy:   "",
			wantFirst: "aaa",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "g", "zzz")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "g", "aaa")
			require.NoError(t, err)

			streams, _, err := b.DescribeLogStreams(
				context.Background(),
				"g",
				"",
				"",
				tt.orderBy,
				tt.descending,
				50,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			require.Len(t, streams, 2)
			assert.Equal(t, tt.wantFirst, streams[0].LogStreamName)
		})
	}
}

func TestCloudWatchLogsBackend_ValidRetentionDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		days    int32
	}{
		{name: "valid_7_days", days: 7},
		{name: "valid_30_days", days: 30},
		{name: "valid_365_days", days: 365},
		{name: "invalid_10_days", days: 10, wantErr: cloudwatchlogs.ErrValidation},
		{name: "invalid_999_days", days: 999, wantErr: cloudwatchlogs.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			days := tt.days
			err = b.SetRetentionPolicy(context.Background(), "g", &days)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_DeleteLogGroup_CleansMetricFilters(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	err = b.PutMetricFilter(
		context.Background(),
		"g",
		"f1",
		"",
		[]cloudwatchlogs.MetricTransformation{
			{MetricName: "m", MetricNamespace: "ns", MetricValue: "1"},
		},
	)
	require.NoError(t, err)

	err = b.DeleteLogGroup(context.Background(), "g")
	require.NoError(t, err)

	// Re-create the group and check metric filters are gone.
	_, err = b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	filters, _, err := b.DescribeMetricFilters(context.Background(), "g", "", "", "", "", 50)
	require.NoError(t, err)
	assert.Empty(t, filters)
}

func TestCloudWatchLogsBackend_LogRecordPtrInOutputEvent(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "g", "s")
	require.NoError(t, err)
	_, err = b.PutLogEvents(context.Background(), "g", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: "msg1", Timestamp: 100},
		{Message: "msg2", Timestamp: 200},
	})
	require.NoError(t, err)

	evts, _, _, err := b.GetLogEvents(context.Background(), "g", "s", nil, nil, 10, "", true)
	require.NoError(t, err)
	require.Len(t, evts, 2)

	for i, ev := range evts {
		assert.NotEmpty(t, ev.Ptr, "event %d should have a Ptr", i)
		// Each pointer should be decodable and map back to an event.
		record, rerr := b.GetLogRecord(context.Background(), ev.Ptr)
		require.NoError(t, rerr, "event %d pointer should be decodable", i)
		assert.Equal(t, ev.Message, record["@message"])
	}
}

func TestCloudWatchLogsBackend_BoundedMaps(t *testing.T) {
	t.Parallel()

	t.Run("export_tasks_limit", func(t *testing.T) {
		t.Parallel()

		b := cloudwatchlogs.NewInMemoryBackend()
		// Seed tasks directly using internal helper to avoid the limit.
		for i := range 1000 {
			cloudwatchlogs.AddExportTaskInternal(b, cloudwatchlogs.ExportTask{
				TaskID:       fmt.Sprintf("task-%d", i),
				LogGroupName: "g",
				Destination:  "bucket",
				Status:       "COMPLETED",
				CreationTime: int64(i + 1),
				From:         1,
				To:           2,
			})
		}

		_, err := b.CreateLogGroup(context.Background(), "g", "", "")
		require.NoError(t, err)
		_, err = b.CreateExportTask("", "g", "", "bucket2", "", 1, 2)
		require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
	})
}

func TestCloudWatchLogsBackend_MetricFilterEmission(t *testing.T) {
	t.Parallel()

	type emittedMetric struct {
		namespace string
		name      string
		value     float64
	}

	var mu sync.Mutex
	var emitted []emittedMetric

	emitter := cloudwatchlogs.MetricEmitterFunc(
		func(namespace, name string, value float64, _ string) error {
			mu.Lock()
			emitted = append(emitted, emittedMetric{namespace: namespace, name: name, value: value})
			mu.Unlock()

			return nil
		},
	)

	b := cloudwatchlogs.NewInMemoryBackend()
	b.SetMetricEmitter(emitter)

	_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "grp", "stream")
	require.NoError(t, err)

	err = b.PutMetricFilter(
		context.Background(),
		"grp",
		"errors",
		"ERROR",
		[]cloudwatchlogs.MetricTransformation{
			{MetricNamespace: "MyApp", MetricName: "ErrorCount", MetricValue: "1"},
		},
	)
	require.NoError(t, err)

	// Two events: one matches the filter pattern, one does not.
	_, err = b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "ERROR: something went wrong", Timestamp: time.Now().UnixMilli()},
			{Message: "INFO: all good", Timestamp: time.Now().UnixMilli()},
		},
	)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, emitted, 1, "expected exactly one metric emission for the ERROR event")
	assert.Equal(t, "MyApp", emitted[0].namespace)
	assert.Equal(t, "ErrorCount", emitted[0].name)
	assert.InDelta(t, 1.0, emitted[0].value, 0.001)
}

func TestCloudWatchLogsBackend_MetricFilterEmission_NoEmitterNoPanic(t *testing.T) {
	t.Parallel()

	// No emitter set — PutLogEvents should succeed silently.
	b := cloudwatchlogs.NewInMemoryBackend()

	_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "grp", "stream")
	require.NoError(t, err)

	err = b.PutMetricFilter(
		context.Background(),
		"grp",
		"errors",
		"ERROR",
		[]cloudwatchlogs.MetricTransformation{
			{MetricNamespace: "MyApp", MetricName: "ErrorCount", MetricValue: "1"},
		},
	)
	require.NoError(t, err)

	_, err = b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "ERROR: kaboom", Timestamp: time.Now().UnixMilli()},
		},
	)
	require.NoError(t, err)
}

// ---- Metric filter field extraction (MetricValue "$name" / "$.path" references) ----
//
// A metric filter's MetricValue may be a literal number (published as-is for every
// matched event) or a "$"-prefixed field reference that must be extracted from each
// individual matched log event: "$size" for a named field in a space-delimited pattern
// ("[ip, level, size]"), "$.bytes" for a JSON selector pattern. Real CloudWatch Logs
// silently skips publishing a data point for a matched event whose referenced field is
// absent or non-numeric -- it does not fabricate a value (DefaultValue is documented for
// periods with zero *matching* events, not failed per-event extraction), so these cases
// assert zero emissions rather than a fallback constant.

func TestCloudWatchLogsBackend_MetricFilterEmission_FieldExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		filterPattern string
		metricValue   string
		message       string
		wantEmitted   []float64 // nil means "no emission for this event"
	}{
		{
			name:          "json_field_extracted_per_event",
			filterPattern: `{ $.level = "ERROR" }`,
			metricValue:   "$.bytes",
			message:       `{"level":"ERROR","bytes":512}`,
			wantEmitted:   []float64{512},
		},
		{
			name:          "space_field_extracted_per_event",
			filterPattern: "[ip, level, bytes]",
			metricValue:   "$bytes",
			message:       "1.2.3.4 ERROR 256",
			wantEmitted:   []float64{256},
		},
		{
			name:          "missing_json_field_skips_emission",
			filterPattern: `{ $.level = "ERROR" }`,
			metricValue:   "$.bytes",
			message:       `{"level":"ERROR"}`,
			wantEmitted:   nil,
		},
		{
			name:          "non_numeric_field_skips_emission",
			filterPattern: `{ $.level = "ERROR" }`,
			metricValue:   "$.bytes",
			message:       `{"level":"ERROR","bytes":"lots"}`,
			wantEmitted:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex
			var emitted []float64

			emitter := cloudwatchlogs.MetricEmitterFunc(
				func(_, _ string, value float64, _ string) error {
					mu.Lock()
					emitted = append(emitted, value)
					mu.Unlock()

					return nil
				},
			)

			b := cloudwatchlogs.NewInMemoryBackend()
			b.SetMetricEmitter(emitter)

			_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "grp", "stream")
			require.NoError(t, err)

			err = b.PutMetricFilter(
				context.Background(),
				"grp",
				"mf",
				tt.filterPattern,
				[]cloudwatchlogs.MetricTransformation{
					{MetricNamespace: "MyApp", MetricName: "Bytes", MetricValue: tt.metricValue},
				},
			)
			require.NoError(t, err)

			_, err = b.PutLogEvents(
				context.Background(),
				"grp",
				"stream",
				"",
				[]cloudwatchlogs.InputLogEvent{
					{Message: tt.message, Timestamp: time.Now().UnixMilli()},
				},
			)
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()

			if tt.wantEmitted == nil {
				assert.Empty(t, emitted)

				return
			}

			require.Len(t, emitted, len(tt.wantEmitted))
			for i, want := range tt.wantEmitted {
				assert.InDelta(t, want, emitted[i], 0.001)
			}
		})
	}
}

func TestCloudWatchLogsBackend_TestMetricFilter_ExtractedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pattern    string
		messages   []string
		wantValues []map[string]string // one entry per expected match, in order
	}{
		{
			name:    "json_pattern_extracts_referenced_selector",
			pattern: `{ $.level = "ERROR" }`,
			messages: []string{
				`{"level":"ERROR","bytes":512}`,
				`{"level":"INFO","bytes":10}`,
			},
			wantValues: []map[string]string{
				{"$.level": "ERROR"},
			},
		},
		{
			name:    "space_pattern_extracts_named_fields",
			pattern: "[ip, level, bytes]",
			messages: []string{
				"1.2.3.4 ERROR 256",
				"5.6.7.8 INFO 10",
			},
			wantValues: []map[string]string{
				{"$ip": "1.2.3.4", "$level": "ERROR", "$bytes": "256"},
				{"$ip": "5.6.7.8", "$level": "INFO", "$bytes": "10"},
			},
		},
		{
			name:    "plain_text_pattern_has_no_addressable_fields",
			pattern: "ERROR",
			messages: []string{
				"an ERROR occurred",
			},
			wantValues: []map[string]string{
				{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			matches, err := b.TestMetricFilter(tt.pattern, tt.messages)
			require.NoError(t, err)
			require.Len(t, matches, len(tt.wantValues))

			for i, want := range tt.wantValues {
				assert.Equal(t, want, matches[i].ExtractedValues)
			}
		})
	}
}

// ---- Item 1: LogGroupClass ----

func TestCloudWatchLogsBackend_CreateLogGroup_WithClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		logGroupClass string
		kmsKeyID      string
		wantClass     string
		wantKmsKeyID  string
	}{
		{
			name:      "default_class_is_standard",
			wantClass: cloudwatchlogs.LogGroupClassStandard,
		},
		{
			name:          "explicit_standard",
			logGroupClass: cloudwatchlogs.LogGroupClassStandard,
			wantClass:     cloudwatchlogs.LogGroupClassStandard,
		},
		{
			name:          "infrequent_access",
			logGroupClass: cloudwatchlogs.LogGroupClassInfrequentAccess,
			wantClass:     cloudwatchlogs.LogGroupClassInfrequentAccess,
		},
		{
			name:          "invalid_class",
			logGroupClass: "NONEXISTENT",
			wantErr:       cloudwatchlogs.ErrValidation,
		},
		{
			name:         "with_kms_key",
			kmsKeyID:     "arn:aws:kms:us-east-1:123:key/abc",
			wantClass:    cloudwatchlogs.LogGroupClassStandard,
			wantKmsKeyID: "arn:aws:kms:us-east-1:123:key/abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			g, err := b.CreateLogGroup(
				context.Background(),
				"/test/group",
				tt.logGroupClass,
				tt.kmsKeyID,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantClass, g.LogGroupClass)
			assert.Equal(t, tt.wantKmsKeyID, g.KmsKeyID)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogGroups_ReturnsClass(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(
		context.Background(),
		"/ia",
		cloudwatchlogs.LogGroupClassInfrequentAccess,
		"",
	)
	require.NoError(t, err)
	_, err = b.CreateLogGroup(
		context.Background(),
		"/std",
		cloudwatchlogs.LogGroupClassStandard,
		"",
	)
	require.NoError(t, err)

	groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 50)
	require.NoError(t, err)
	require.Len(t, groups, 2)

	classMap := make(map[string]string)
	for _, g := range groups {
		classMap[g.LogGroupName] = g.LogGroupClass
	}

	assert.Equal(t, cloudwatchlogs.LogGroupClassInfrequentAccess, classMap["/ia"])
	assert.Equal(t, cloudwatchlogs.LogGroupClassStandard, classMap["/std"])
}

// ---- Item 2: PutLogEvents RejectedLogEventsInfo ----

func TestCloudWatchLogsBackend_PutLogEvents_RejectedLogEventsInfo(t *testing.T) {
	t.Parallel()

	now := time.Now().UnixMilli()
	tooOld := now - 15*24*60*60*1000 // 15 days ago (beyond 14d hard cap)
	tooNew := now + 3*60*60*1000     // 3 hours in the future

	tests := []struct {
		wantErr      error
		name         string
		events       []cloudwatchlogs.InputLogEvent
		wantAccepted int
		wantTooOld   bool
		wantTooNew   bool
		wantExpired  bool
	}{
		{
			name: "all_valid",
			events: []cloudwatchlogs.InputLogEvent{
				{Message: "ok", Timestamp: now},
			},
			wantAccepted: 1,
		},
		{
			name: "too_new_rejected",
			events: []cloudwatchlogs.InputLogEvent{
				{Message: "ok", Timestamp: now},
				{Message: "future", Timestamp: tooNew},
			},
			wantAccepted: 1,
			wantTooNew:   true,
		},
		{
			name: "too_old_rejected",
			events: []cloudwatchlogs.InputLogEvent{
				{Message: "old", Timestamp: tooOld},
				{Message: "ok", Timestamp: now},
			},
			wantAccepted: 1,
			wantTooOld:   true,
		},
		{
			name: "message_too_large",
			events: []cloudwatchlogs.InputLogEvent{
				{Message: string(make([]byte, 256*1024+1)), Timestamp: now},
			},
			wantErr: cloudwatchlogs.ErrValidation,
		},
		{
			name: "synthetic_timestamps_accepted",
			events: []cloudwatchlogs.InputLogEvent{
				{Message: "test", Timestamp: 1},
				{Message: "test2", Timestamp: 1000},
			},
			wantAccepted: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "g", "s")
			require.NoError(t, err)

			result, err := b.PutLogEvents(context.Background(), "g", "s", "", tt.events)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)

			got, _, _, err := b.GetLogEvents(
				context.Background(),
				"g",
				"s",
				nil,
				nil,
				1000,
				"",
				true,
			)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantAccepted)

			if tt.wantTooOld || tt.wantTooNew || tt.wantExpired {
				require.NotNil(t, result.RejectedLogEventsInfo)
				if tt.wantTooOld {
					assert.NotNil(t, result.RejectedLogEventsInfo.TooOldLogEventEndIndex)
				}
				if tt.wantTooNew {
					assert.NotNil(t, result.RejectedLogEventsInfo.TooNewLogEventStartIndex)
				}
				if tt.wantExpired {
					assert.NotNil(t, result.RejectedLogEventsInfo.ExpiredLogEventEndIndex)
				}
			} else {
				assert.Nil(t, result.RejectedLogEventsInfo)
			}
		})
	}
}

// ---- Item 3: SequenceToken is ignored (matches current AWS behavior) ----
//
// aws-sdk-go-v2 cloudwatchlogs.PutLogEvents doc: "The sequence token is now
// ignored in PutLogEvents actions. PutLogEvents actions are always accepted and
// never return InvalidSequenceTokenException or DataAlreadyAcceptedException
// even if the sequence token is not valid." So every case below must succeed
// regardless of whether the supplied token matches the stream's actual length,
// and NextSequenceToken must reflect the real post-append event count rather
// than echoing (or validating against) the caller-supplied token.

func TestCloudWatchLogsBackend_PutLogEvents_SequenceTokenIgnored(t *testing.T) {
	t.Parallel()

	now := time.Now().UnixMilli()

	tests := []struct {
		name          string
		sequenceToken string
		setupEvents   int
	}{
		{
			name:          "no_token",
			setupEvents:   0,
			sequenceToken: "",
		},
		{
			name:          "matching_token",
			setupEvents:   2,
			sequenceToken: "2",
		},
		{
			name:          "stale_token_still_accepted",
			setupEvents:   2,
			sequenceToken: "99",
		},
		{
			name:          "token_on_empty_stream_matching",
			setupEvents:   0,
			sequenceToken: "0",
		},
		{
			name:          "token_on_empty_stream_wrong_still_accepted",
			setupEvents:   0,
			sequenceToken: "5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "g", "s")
			require.NoError(t, err)

			for i := range tt.setupEvents {
				_, err = b.PutLogEvents(
					context.Background(),
					"g",
					"s",
					"",
					[]cloudwatchlogs.InputLogEvent{
						{Message: fmt.Sprintf("event-%d", i), Timestamp: now},
					},
				)
				require.NoError(t, err)
			}

			result, err := b.PutLogEvents(
				context.Background(),
				"g",
				"s",
				tt.sequenceToken,
				[]cloudwatchlogs.InputLogEvent{
					{Message: "new event", Timestamp: now},
				},
			)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, strconv.Itoa(tt.setupEvents+1), result.NextSequenceToken)
		})
	}
}

// ---- Item 6: MetricTransformation Dimensions + Unit ----

func TestCloudWatchLogsBackend_MetricTransformation_DimensionsAndUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDimensions  map[string]string
		name            string
		wantUnit        string
		transformations []cloudwatchlogs.MetricTransformation
	}{
		{
			name: "with_dimensions_and_unit",
			transformations: []cloudwatchlogs.MetricTransformation{
				{
					MetricNamespace: "MyApp",
					MetricName:      "Errors",
					MetricValue:     "1",
					Unit:            "Count",
					Dimensions: map[string]string{
						"Service": "api",
						"Env":     "prod",
					},
				},
			},
			wantDimensions: map[string]string{"Service": "api", "Env": "prod"},
			wantUnit:       "Count",
		},
		{
			name: "without_dimensions",
			transformations: []cloudwatchlogs.MetricTransformation{
				{
					MetricNamespace: "MyApp",
					MetricName:      "Requests",
					MetricValue:     "1",
				},
			},
			wantUnit: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			err = b.PutMetricFilter(
				context.Background(),
				"g",
				"filter1",
				"ERROR",
				tt.transformations,
			)
			require.NoError(t, err)

			filters, _, err := b.DescribeMetricFilters(
				context.Background(),
				"g",
				"",
				"",
				"",
				"",
				50,
			)
			require.NoError(t, err)
			require.Len(t, filters, 1)
			require.Len(t, filters[0].MetricTransformations, 1)

			mf := filters[0].MetricTransformations[0]
			assert.Equal(t, tt.wantUnit, mf.Unit)

			if tt.wantDimensions != nil {
				assert.Equal(t, tt.wantDimensions, mf.Dimensions)
			}
		})
	}
}

// ---- Item 7: SubscriptionFilter RoleArn + Distribution ----

func TestCloudWatchLogsBackend_PutSubscriptionFilter_RoleArnAndDistribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr          error
		name             string
		roleArn          string
		distribution     string
		wantDistribution string
		wantRoleArn      string
	}{
		{
			name:             "default_distribution_is_random",
			wantDistribution: cloudwatchlogs.DistributionRandom,
		},
		{
			name:             "explicit_random",
			distribution:     cloudwatchlogs.DistributionRandom,
			wantDistribution: cloudwatchlogs.DistributionRandom,
		},
		{
			name:             "by_log_stream",
			distribution:     cloudwatchlogs.DistributionByLogStream,
			wantDistribution: cloudwatchlogs.DistributionByLogStream,
		},
		{
			name:         "invalid_distribution",
			distribution: "INVALID",
			wantErr:      cloudwatchlogs.ErrValidation,
		},
		{
			name:             "with_role_arn",
			roleArn:          "arn:aws:iam::123456789012:role/MyRole",
			wantRoleArn:      "arn:aws:iam::123456789012:role/MyRole",
			wantDistribution: cloudwatchlogs.DistributionRandom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			err = b.PutSubscriptionFilter(
				context.Background(),
				"g", "f1", "", "arn:aws:kinesis:us-east-1:123:stream/s",
				tt.roleArn, tt.distribution,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, err := b.DescribeSubscriptionFilters(context.Background(), "g", "", "", 50)
			require.NoError(t, err)
			require.Len(t, filters, 1)

			f := filters[0]
			assert.Equal(t, tt.wantDistribution, f.Distribution)
			assert.Equal(t, tt.wantRoleArn, f.RoleArn)
		})
	}
}

func TestCloudWatchLogsBackend_PutSubscriptionFilter_UpdatePreservesFields(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	// Create with role and distribution.
	err = b.PutSubscriptionFilter(
		context.Background(),
		"g", "f1", "ERROR", "arn:aws:kinesis:us-east-1:123:stream/s",
		"arn:aws:iam::123:role/r", cloudwatchlogs.DistributionByLogStream,
	)
	require.NoError(t, err)

	// Update with new pattern.
	err = b.PutSubscriptionFilter(
		context.Background(),
		"g", "f1", "WARN", "arn:aws:kinesis:us-east-1:123:stream/s",
		"arn:aws:iam::123:role/r2", cloudwatchlogs.DistributionRandom,
	)
	require.NoError(t, err)

	filters, _, err := b.DescribeSubscriptionFilters(context.Background(), "g", "", "", 50)
	require.NoError(t, err)
	require.Len(t, filters, 1)

	f := filters[0]
	assert.Equal(t, "WARN", f.FilterPattern)
	assert.Equal(t, "arn:aws:iam::123:role/r2", f.RoleArn)
	assert.Equal(t, cloudwatchlogs.DistributionRandom, f.Distribution)
}

// ---- Item 10: AccountPolicy FIELD_INDEX_POLICY + TRANSFORMER_POLICY + Scope ----

func TestCloudWatchLogsBackend_PutAccountPolicy_ExtendedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		name              string
		policyType        string
		scope             string
		selectionCriteria string
		wantScope         string
	}{
		{
			name:       "data_protection_policy",
			policyType: "DATA_PROTECTION_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "subscription_filter_policy",
			policyType: "SUBSCRIPTION_FILTER_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "field_index_policy",
			policyType: "FIELD_INDEX_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "transformer_policy",
			policyType: "TRANSFORMER_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "invalid_type",
			policyType: "INVALID_TYPE",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:              "selection_criteria_scope",
			policyType:        "DATA_PROTECTION_POLICY",
			scope:             "SELECTION_CRITERIA",
			selectionCriteria: "logGroupName LIKE '/aws/lambda/%'",
			wantScope:         "SELECTION_CRITERIA",
		},
		{
			name:       "selection_criteria_scope_missing_criteria",
			policyType: "DATA_PROTECTION_POLICY",
			scope:      "SELECTION_CRITERIA",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:       "invalid_scope",
			policyType: "DATA_PROTECTION_POLICY",
			scope:      "INVALID_SCOPE",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			policy, err := b.PutAccountPolicy(
				"p1",
				tt.policyType,
				"{}",
				tt.scope,
				tt.selectionCriteria,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, policy)
			assert.Equal(t, tt.wantScope, policy.Scope)
			if tt.selectionCriteria != "" {
				assert.Equal(t, tt.selectionCriteria, policy.SelectionCriteria)
			}
		})
	}
}

// ---- Item 17: AnomalyDetector VisibilityTime validation + status ----

func TestCloudWatchLogsBackend_CreateLogAnomalyDetector_VisibilityTimeValidation(t *testing.T) {
	t.Parallel()

	const msPerDay = 24 * 60 * 60 * 1000

	tests := []struct {
		wantErr               error
		name                  string
		wantStatus            string
		anomalyVisibilityTime int64
	}{
		{
			name:       "zero_accepted",
			wantStatus: "INITIALIZING",
		},
		{
			name:                  "7_days_min",
			anomalyVisibilityTime: 7 * msPerDay,
			wantStatus:            "INITIALIZING",
		},
		{
			name:                  "90_days_max",
			anomalyVisibilityTime: 90 * msPerDay,
			wantStatus:            "INITIALIZING",
		},
		{
			name:                  "30_days_valid",
			anomalyVisibilityTime: 30 * msPerDay,
			wantStatus:            "INITIALIZING",
		},
		{
			name:                  "6_days_too_small",
			anomalyVisibilityTime: 6 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
		{
			name:                  "91_days_too_large",
			anomalyVisibilityTime: 91 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			groupARN := "arn:aws:logs:us-east-1:123456789012:log-group:g"
			detectorARN, err := b.CreateLogAnomalyDetector(
				[]string{groupARN}, "test-detector", "", "", "",
				tt.anomalyVisibilityTime,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, detectorARN)

			detector, err := b.GetLogAnomalyDetector(detectorARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, detector.DetectorStatus)
			assert.NotZero(t, detector.LastModifiedTimeStamp)
		})
	}
}

func TestCloudWatchLogsBackend_UpdateLogAnomalyDetector_SetsLastModified(t *testing.T) {
	t.Parallel()

	const msPerDay = 24 * 60 * 60 * 1000

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	groupARN := "arn:aws:logs:us-east-1:123456789012:log-group:g"
	arn, err := b.CreateLogAnomalyDetector([]string{groupARN}, "d", "", "", "", 0)
	require.NoError(t, err)

	before, err := b.GetLogAnomalyDetector(arn)
	require.NoError(t, err)
	createdAt := before.LastModifiedTimeStamp

	time.Sleep(2 * time.Millisecond)

	err = b.UpdateLogAnomalyDetector(arn, "FIVE_MIN", 30*msPerDay)
	require.NoError(t, err)

	after, err := b.GetLogAnomalyDetector(arn)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, after.LastModifiedTimeStamp, createdAt)
}

func TestCloudWatchLogsBackend_UpdateLogAnomalyDetector_VisibilityTimeValidation(t *testing.T) {
	t.Parallel()

	const msPerDay = 24 * 60 * 60 * 1000

	tests := []struct {
		wantErr               error
		name                  string
		anomalyVisibilityTime int64
	}{
		{
			name:                  "valid_30_days",
			anomalyVisibilityTime: 30 * msPerDay,
		},
		{
			name:                  "too_small_6_days",
			anomalyVisibilityTime: 6 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
		{
			name:                  "too_large_91_days",
			anomalyVisibilityTime: 91 * msPerDay,
			wantErr:               cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn, err := b.CreateLogAnomalyDetector(
				[]string{"arn:aws:logs:us-east-1:123:log-group:g"}, "", "", "", "", 0,
			)
			require.NoError(t, err)

			err = b.UpdateLogAnomalyDetector(arn, "", tt.anomalyVisibilityTime)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---- Item 26: DescribeLogStreams orderBy validation ----

func TestCloudWatchLogsBackend_DescribeLogStreams_OrderByValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		orderBy    string
		prefix     string
		descending bool
	}{
		{
			name:    "name_asc_valid",
			orderBy: "LogStreamName",
		},
		{
			name:       "name_desc_invalid",
			orderBy:    "LogStreamName",
			descending: true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:    "last_event_time_valid",
			orderBy: "LastEventTime",
		},
		{
			name:       "last_event_time_desc_valid",
			orderBy:    "LastEventTime",
			descending: true,
		},
		{
			name:    "last_event_time_with_prefix_invalid",
			orderBy: "LastEventTime",
			prefix:  "stream-",
			wantErr: cloudwatchlogs.ErrValidation,
		},
		{
			name:    "empty_orderby_asc_valid",
			orderBy: "",
		},
		{
			name:       "empty_orderby_desc_invalid",
			orderBy:    "",
			descending: true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			_, _, err = b.DescribeLogStreams(
				context.Background(),
				"g",
				tt.prefix,
				"",
				tt.orderBy,
				tt.descending,
				50,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---- Item 27: PutQueryDefinition verify existing ID ----

func TestCloudWatchLogsBackend_PutQueryDefinition_UpdateVerifiesID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		name              string
		queryDefinitionID string
		createFirst       bool
	}{
		{
			name:              "create_new_no_id",
			queryDefinitionID: "",
		},
		{
			name:              "update_existing_id",
			queryDefinitionID: "placeholder",
			createFirst:       true,
		},
		{
			name:              "update_nonexistent_id",
			queryDefinitionID: "00000000-0000-0000-0000-000000000000",
			createFirst:       false,
			wantErr:           cloudwatchlogs.ErrQueryDefinitionNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			queryID := tt.queryDefinitionID
			if tt.createFirst {
				var err error
				queryID, err = b.PutQueryDefinition("initial", "fields @message", "", nil)
				require.NoError(t, err)
			}

			_, err := b.PutQueryDefinition("updated", "fields @timestamp", queryID, nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---- Item 30: DescribeAccountPolicies pagination ----

func TestCloudWatchLogsBackend_DescribeAccountPolicies_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()

	// Create 5 policies.
	for i := range 5 {
		_, err := b.PutAccountPolicy(
			fmt.Sprintf("policy-%02d", i),
			"DATA_PROTECTION_POLICY",
			"{}",
			"", "",
		)
		require.NoError(t, err)
	}

	// Page 1.
	page1, token1, err := b.DescribeAccountPolicies("", "", nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, token1)
	assert.Equal(t, "policy-00", page1[0].PolicyName)
	assert.Equal(t, "policy-01", page1[1].PolicyName)

	// Page 2.
	page2, token2, err := b.DescribeAccountPolicies("", "", nil, 2, token1)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, token2)

	// Page 3 (last).
	page3, token3, err := b.DescribeAccountPolicies("", "", nil, 2, token2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3)
}

func TestCloudWatchLogsBackend_DescribeAccountPolicies_FilterByType(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.PutAccountPolicy("p1", "DATA_PROTECTION_POLICY", "{}", "", "")
	require.NoError(t, err)
	_, err = b.PutAccountPolicy("p2", "FIELD_INDEX_POLICY", "{}", "", "")
	require.NoError(t, err)
	_, err = b.PutAccountPolicy("p3", "TRANSFORMER_POLICY", "{}", "", "")
	require.NoError(t, err)

	tests := []struct {
		name       string
		policyType string
		wantLen    int
	}{
		{
			name:       "filter_data_protection",
			policyType: "DATA_PROTECTION_POLICY",
			wantLen:    1,
		},
		{
			name:       "filter_field_index",
			policyType: "FIELD_INDEX_POLICY",
			wantLen:    1,
		},
		{
			name:       "filter_transformer",
			policyType: "TRANSFORMER_POLICY",
			wantLen:    1,
		},
		{
			name:    "no_filter_all",
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policies, _, descErr := b.DescribeAccountPolicies(tt.policyType, "", nil, 0, "")
			require.NoError(t, descErr)
			assert.Len(t, policies, tt.wantLen)
		})
	}
}
