package kinesis_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func newFISKinesisHandler() *kinesis.Handler {
	backend := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	return kinesis.NewHandler(backend)
}

func TestKinesis_FISActions(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()
	actions := h.FISActions()

	ids := make([]string, len(actions))
	for i, a := range actions {
		ids[i] = a.ActionID
	}

	assert.Contains(t, ids, "aws:kinesis:stream-provisioned-throughput-exception")
}

func TestKinesis_FISActions_TargetType(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	actions := h.FISActions()
	require.Len(t, actions, 1)
	assert.Equal(t, "aws:kinesis:stream", actions[0].TargetType)
}

func TestKinesis_FISActions_Parameters(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	actions := h.FISActions()
	require.Len(t, actions, 1)

	paramNames := make([]string, len(actions[0].Parameters))
	for i, p := range actions[0].Parameters {
		paramNames[i] = p.Name
	}

	assert.Contains(t, paramNames, "duration")
	assert.Contains(t, paramNames, "percentage")
}

func TestKinesis_ExecuteFISAction_ThroughputException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stream   string
		targets  []string
		duration time.Duration
		wantErr  bool
	}{
		{
			name:    "known_stream_no_duration",
			stream:  "my-stream",
			targets: []string{"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"},
			wantErr: false,
		},
		{
			name:     "known_stream_with_duration",
			stream:   "timed-stream",
			targets:  []string{"arn:aws:kinesis:us-east-1:000000000000:stream/timed-stream"},
			duration: 100 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:    "no_targets",
			targets: []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newFISKinesisHandler()

			// Create the stream if needed.
			if tt.stream != "" {
				err := h.Backend.CreateStream(&kinesis.CreateStreamInput{
					StreamName: tt.stream,
					ShardCount: 1,
				})
				require.NoError(t, err)
			}

			err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
				ActionID: "aws:kinesis:stream-provisioned-throughput-exception",
				Targets:  tt.targets,
				Duration: tt.duration,
			})

			require.NoError(t, err)

			// Verify throughput exception is active on the stream.
			if tt.stream != "" && len(tt.targets) > 0 {
				_, putErr := h.Backend.PutRecord(&kinesis.PutRecordInput{
					StreamName:   tt.stream,
					PartitionKey: "key",
					Data:         []byte("data"),
				})
				require.ErrorIs(t, putErr, kinesis.ErrProvisionedThroughputExceeded)

				// After the duration, the fault should clear.
				if tt.duration > 0 {
					time.Sleep(tt.duration + 50*time.Millisecond)

					_, putAfter := h.Backend.PutRecord(&kinesis.PutRecordInput{
						StreamName:   tt.stream,
						PartitionKey: "key",
						Data:         []byte("data"),
					})
					assert.NoError(t, putAfter, "PutRecord should succeed after fault expires")
				}
			}
		})
	}
}

func TestKinesis_ExecuteFISAction_ThroughputException_ZeroPercentage(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	const streamName = "zero-pct-stream"
	const sampleSize = 50

	err := h.Backend.CreateStream(&kinesis.CreateStreamInput{
		StreamName: streamName,
		ShardCount: 1,
	})
	require.NoError(t, err)

	// Activate fault with 0% — no requests should ever be throttled.
	err = h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID:   "aws:kinesis:stream-provisioned-throughput-exception",
		Targets:    []string{streamName},
		Parameters: map[string]string{"percentage": "0"},
	})
	require.NoError(t, err)

	// With 0% probability, all PutRecord calls should succeed.
	for range sampleSize {
		_, putErr := h.Backend.PutRecord(&kinesis.PutRecordInput{
			StreamName:   streamName,
			PartitionKey: "key",
			Data:         []byte("data"),
		})
		require.NoError(t, putErr, "PutRecord should not be throttled at 0%%")
	}
}

func TestKinesis_ParseThrottlePercentage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  float64
	}{
		{name: "empty_defaults_to_100pct", input: "", want: 1.0},
		{name: "invalid_string_defaults_to_100pct", input: "abc", want: 1.0},
		{name: "negative_defaults_to_100pct", input: "-5", want: 1.0},
		{name: "zero_means_no_fault", input: "0", want: 0.0},
		{name: "50_pct", input: "50", want: 0.5},
		{name: "100_means_always_fault", input: "100", want: 1.0},
		{name: "above_100_means_always_fault", input: "150", want: 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := kinesis.ParseThrottlePercentageForTest(tt.input)
			assert.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

func TestKinesis_ExecuteFISAction_Unknown(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:kinesis:unknown-action",
		Targets:  []string{"some-stream"},
	})

	require.NoError(t, err)
}

func TestKinesis_ExecuteFISAction_ThroughputException_CtxCancel(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	const streamName = "ctx-cancel-stream"

	err := h.Backend.CreateStream(&kinesis.CreateStreamInput{
		StreamName: streamName,
		ShardCount: 1,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Activate indefinite fault (dur==0).
	err = h.ExecuteFISAction(ctx, service.FISActionExecution{
		ActionID: "aws:kinesis:stream-provisioned-throughput-exception",
		Targets:  []string{streamName},
		Duration: 0,
	})
	require.NoError(t, err)

	_, putErr := h.Backend.PutRecord(&kinesis.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: "key",
		Data:         []byte("data"),
	})
	require.ErrorIs(t, putErr, kinesis.ErrProvisionedThroughputExceeded, "fault should be active")

	// Cancel ctx (simulates StopExperiment).
	cancel()

	// Fault should clear promptly.
	require.Eventually(t, func() bool {
		_, putAfterErr := h.Backend.PutRecord(&kinesis.PutRecordInput{
			StreamName:   streamName,
			PartitionKey: "key",
			Data:         []byte("data"),
		})

		return putAfterErr == nil
	}, 2*time.Second, 20*time.Millisecond, "fault should clear after ctx cancel")
}

func TestKinesis_ThroughputFault_ZeroPercentage_NoThrottle(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	const streamName = "zero-pct-stream"

	err := h.Backend.CreateStream(&kinesis.CreateStreamInput{
		StreamName: streamName,
		ShardCount: 1,
	})
	require.NoError(t, err)

	// Activate with 0% percentage — no requests should be throttled.
	err = h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:kinesis:stream-provisioned-throughput-exception",
		Targets:  []string{streamName},
		Parameters: map[string]string{
			"percentage": "0",
		},
		Duration: 0,
	})
	require.NoError(t, err)

	// With 0% probability, PutRecord should never be throttled.
	for range 10 {
		_, putErr := h.Backend.PutRecord(&kinesis.PutRecordInput{
			StreamName:   streamName,
			PartitionKey: "key",
			Data:         []byte("data"),
		})
		require.NoError(t, putErr, "0%% probability should never throttle")
	}
}

func TestKinesis_ThroughputFault_PartialPercentage(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	const streamName = "partial-pct-stream"

	err := h.Backend.CreateStream(&kinesis.CreateStreamInput{
		StreamName: streamName,
		ShardCount: 1,
	})
	require.NoError(t, err)

	// Activate with 50% percentage.
	err = h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:kinesis:stream-provisioned-throughput-exception",
		Targets:  []string{streamName},
		Parameters: map[string]string{
			"percentage": "50",
		},
		Duration: 0,
	})
	require.NoError(t, err)

	// With 50% probability, some requests may be throttled and some not.
	// Just verify the action activates without error and the code path runs.
	throttledCount := 0
	total := 50

	for range total {
		_, putErr := h.Backend.PutRecord(&kinesis.PutRecordInput{
			StreamName:   streamName,
			PartitionKey: "key",
			Data:         []byte("data"),
		})
		if putErr != nil {
			throttledCount++
		}
	}

	// With 50% probability over 50 tries, it's astronomically unlikely to get 0% or 100%.
	assert.Positive(t, throttledCount, "50%% probability should throttle some requests")
	assert.Less(t, throttledCount, total, "50%% probability should not throttle all requests")
}

func TestKinesis_ExecuteFISAction_NonInMemoryBackend(t *testing.T) {
	t.Parallel()

	// A handler with a nil backend should gracefully skip FIS actions.
	h := kinesis.NewHandler(nil)

	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:kinesis:stream-provisioned-throughput-exception",
		Targets:  []string{"some-stream"},
	})

	require.NoError(t, err)
}

func TestKinesis_ThroughputFaultActiveLocked_LazyEviction(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	const streamName = "lazy-evict-kinesis-stream"

	err := backend.CreateStream(&kinesis.CreateStreamInput{
		StreamName: streamName,
		ShardCount: 1,
	})
	require.NoError(t, err)

	// Inject an already-expired fault directly (no goroutine, guaranteed expired).
	backend.InjectExpiredThroughputFaultForTest(streamName)

	// PutRecord should succeed because the fault is expired — lazy eviction fires inside.
	_, putErr := backend.PutRecord(&kinesis.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: "key",
		Data:         []byte("data"),
	})
	require.NoError(t, putErr, "expired fault should not throttle requests")

	// After lazy eviction, a second PutRecord should also succeed.
	_, putErr2 := backend.PutRecord(&kinesis.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: "key2",
		Data:         []byte("data2"),
	})
	require.NoError(t, putErr2, "second PutRecord after eviction should also succeed")
}

func TestKinesis_ScheduleThroughputFaultCleanup_MissingEntry_Continue(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled so cleanup fires synchronously

	// Call cleanup with a stream that was never added to the map.
	// Should hit the !exists continue branch without panicking.
	backend.ScheduleThroughputFaultCleanupForTest(ctx, []string{"never-added-stream"}, time.Millisecond)
}
