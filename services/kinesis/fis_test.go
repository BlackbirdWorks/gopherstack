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

func TestKinesis_ExecuteFISAction_Unknown(t *testing.T) {
	t.Parallel()

	h := newFISKinesisHandler()

	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:kinesis:unknown-action",
		Targets:  []string{"some-stream"},
	})

	require.NoError(t, err)
}
