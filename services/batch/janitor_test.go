package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestBatchJanitor_TaskTimeout_WithJanitor verifies that the variadic taskTimeout
// parameter is stored in the janitor's TaskTimeout field when passed to WithJanitor.
func TestBatchJanitor_TaskTimeout_WithJanitor(t *testing.T) {
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
		{
			name:        "1min_timeout",
			taskTimeout: time.Minute,
			want:        time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1"))
			h.WithJanitor(time.Minute, time.Hour, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

// TestBatchJanitor_Run_ExitsOnCancel verifies that the janitor loop exits
// promptly when the parent context is cancelled.
func TestBatchJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := batch.NewInMemoryBackend("000000000000", "us-east-1")
	j := batch.NewJanitor(b, 10*time.Millisecond, time.Hour)
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

// TestBatchJanitor_SweepOnce_WithTaskTimeout verifies that SweepOnce works correctly
// even when a TaskTimeout is configured on the janitor.
func TestBatchJanitor_SweepOnce_WithTaskTimeout(t *testing.T) {
	t.Parallel()

	b := batch.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.RegisterJobDefinition("sweep-timeout-test", "container", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeregisterJobDefinition("sweep-timeout-test:1"))

	// Set DeregisteredAt in the past so it will be swept.
	b.SetJobDefinitionDeregisteredAt("sweep-timeout-test:1", time.Now().Add(-25*time.Hour))

	j := batch.NewJanitor(b, time.Minute, 24*time.Hour)
	j.TaskTimeout = 30 * time.Second

	require.Equal(t, 1, b.JobDefinitionCount())

	j.SweepOnce(t.Context())

	assert.Equal(t, 0, b.JobDefinitionCount())
}
