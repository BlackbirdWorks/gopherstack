package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatchlogs"
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
