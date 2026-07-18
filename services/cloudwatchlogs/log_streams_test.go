package cloudwatchlogs_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
