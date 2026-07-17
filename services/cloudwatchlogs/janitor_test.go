package cloudwatchlogs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestCloudWatchLogsJanitor_TaskTimeout_WithJanitor verifies that the variadic
// taskTimeout is propagated into the janitor's TaskTimeout field via WithJanitor.
func TestCloudWatchLogsJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
		want        time.Duration
	}{
		{
			name:        "zero_timeout",
			taskTimeout: 0,
			want:        0,
		},
		{
			name:        "30s_timeout",
			taskTimeout: 30 * time.Second,
			want:        30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
			h.WithJanitor(time.Minute, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

// TestCloudWatchLogsJanitor_Run_ExitsOnCancel verifies that the janitor
// goroutine exits promptly when the parent context is cancelled.
func TestCloudWatchLogsJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	j := cloudwatchlogs.NewJanitor(b, 10*time.Millisecond)
	j.TaskTimeout = 30 * time.Second

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "janitor did not exit after context cancellation")
	}
}

// TestCloudWatchLogsJanitor_SweepOnce_WithTaskTimeout verifies that SweepOnce
// correctly purges log events past the group's retention policy, even when a
// TaskTimeout is set.
func TestCloudWatchLogsJanitor_SweepOnce_WithTaskTimeout(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()

	_, err := b.CreateLogGroup(context.Background(), "test-group", "", "")
	require.NoError(t, err)

	_, err = b.CreateLogStream(context.Background(), "test-group", "test-stream")
	require.NoError(t, err)

	// Set a 1-day retention policy.
	days := int32(1)
	require.NoError(t, b.SetRetentionPolicy(context.Background(), "test-group", &days))

	// Put an event with a timestamp 2 days in the past (should be swept).
	oldTimestampMs := time.Now().Add(-48 * time.Hour).UnixMilli()
	_, err = b.PutLogEvents(context.Background(), "test-group", "test-stream", "", []cloudwatchlogs.InputLogEvent{
		{Timestamp: oldTimestampMs, Message: "old event"},
	})
	require.NoError(t, err)

	j := cloudwatchlogs.NewJanitor(b, time.Minute)
	j.TaskTimeout = 30 * time.Second

	j.SweepOnce(t.Context())

	events, _, _, err := b.GetLogEvents(context.Background(), "test-group", "test-stream", nil, nil, 100, "", true)
	require.NoError(t, err)
	assert.Empty(t, events, "old events should have been swept")
}

// TestCloudWatchLogsJanitor_DefaultInterval verifies that a zero interval in WithJanitor
// results in the default interval being used.
func TestCloudWatchLogsJanitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "zero_uses_default",
			interval: 0,
			want:     cloudwatchlogs.DefaultJanitorInterval,
		},
		{
			name:     "custom_interval_propagated",
			interval: 5 * time.Minute,
			want:     5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
			h.WithJanitor(tt.interval)

			assert.Equal(t, tt.want, h.GetJanitorInterval())
		})
	}
}

func TestCloudWatchLogsJanitor_SweepOnce_RespectsContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		cancelBeforeSweep bool
		wantEvents        int
	}{
		{
			name:              "active_context_sweeps",
			cancelBeforeSweep: false,
			wantEvents:        0,
		},
		{
			name:              "cancelled_context_skips_sweep",
			cancelBeforeSweep: true,
			wantEvents:        1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			_, err := b.CreateLogGroup(context.Background(), "test-group", "", "")
			require.NoError(t, err)

			_, err = b.CreateLogStream(context.Background(), "test-group", "test-stream")
			require.NoError(t, err)

			days := int32(1)
			require.NoError(t, b.SetRetentionPolicy(context.Background(), "test-group", &days))

			oldTimestampMs := time.Now().Add(-48 * time.Hour).UnixMilli()
			_, err = b.PutLogEvents(
				context.Background(),
				"test-group",
				"test-stream",
				"",
				[]cloudwatchlogs.InputLogEvent{
					{Timestamp: oldTimestampMs, Message: "old event"},
				},
			)
			require.NoError(t, err)

			j := cloudwatchlogs.NewJanitor(b, time.Minute)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancelBeforeSweep {
				cancel()
			}

			j.SweepOnce(ctx)

			events, _, _, getErr := b.GetLogEvents(
				context.Background(),
				"test-group",
				"test-stream",
				nil,
				nil,
				100,
				"",
				true,
			)
			require.NoError(t, getErr)
			assert.Len(t, events, tt.wantEvents)
		})
	}
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

func TestHandler_WithJanitor_StartWorker(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(b).WithJanitor(0)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, h.StartWorker(ctx))
}

func TestHandler_StartWorker_NoJanitor(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	// Should be a no-op.
	require.NoError(t, h.StartWorker(t.Context()))
}
