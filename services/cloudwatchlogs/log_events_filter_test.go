package cloudwatchlogs_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
