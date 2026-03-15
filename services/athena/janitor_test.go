package athena_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestJanitor_SweepCompletedExecutions verifies that the janitor removes
// query executions in terminal states older than the configured TTL.
func TestJanitor_SweepCompletedExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		state           string
		completionDelay time.Duration // negative means in the past
		ttl             time.Duration
		wantEvicted     bool
	}{
		{
			name:            "evict_succeeded_past_ttl",
			state:           "SUCCEEDED",
			completionDelay: -2 * time.Hour,
			ttl:             time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "evict_failed_past_ttl",
			state:           "FAILED",
			completionDelay: -2 * time.Hour,
			ttl:             time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "evict_cancelled_past_ttl",
			state:           "CANCELLED",
			completionDelay: -2 * time.Hour,
			ttl:             time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "keep_succeeded_within_ttl",
			state:           "SUCCEEDED",
			completionDelay: -30 * time.Minute,
			ttl:             time.Hour,
			wantEvicted:     false,
		},
		{
			name:            "keep_running",
			state:           "RUNNING",
			completionDelay: -2 * time.Hour,
			ttl:             time.Hour,
			wantEvicted:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend()

			// Create a query execution with the given completion state.
			id, err := backend.StartQueryExecution("SELECT 1", "primary",
				athena.QueryExecutionContext{Database: "default"},
				athena.ResultConfiguration{})
			require.NoError(t, err)

			// Override the execution's state and completion time.
			backend.SetQueryExecutionState(id, tt.state, tt.completionDelay)

			janitor := athena.NewJanitor(backend, time.Hour, tt.ttl)
			janitor.SweepOnce(t.Context())

			_, err = backend.GetQueryExecution(id)
			if tt.wantEvicted {
				assert.ErrorIs(t, err, athena.ErrNotFound)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestJanitor_PreservesActiveExecutions verifies that non-terminal executions
// and recently completed ones are not removed by the janitor.
func TestJanitor_PreservesActiveExecutions(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend()

	// Create an execution that just completed (within TTL).
	recentID, err := backend.StartQueryExecution("SELECT 2", "primary",
		athena.QueryExecutionContext{Database: "default"},
		athena.ResultConfiguration{})
	require.NoError(t, err)

	// Create one that is old enough to evict.
	oldID, err := backend.StartQueryExecution("SELECT 3", "primary",
		athena.QueryExecutionContext{Database: "default"},
		athena.ResultConfiguration{})
	require.NoError(t, err)

	backend.SetQueryExecutionState(oldID, "SUCCEEDED", -25*time.Hour)

	janitor := athena.NewJanitor(backend, time.Hour, 24*time.Hour)
	janitor.SweepOnce(t.Context())

	_, err = backend.GetQueryExecution(recentID)
	require.NoError(t, err, "recent execution should be preserved")

	_, err = backend.GetQueryExecution(oldID)
	assert.ErrorIs(t, err, athena.ErrNotFound, "old execution should be evicted")
}

// TestJanitor_RunContext verifies that the janitor stops when the context is cancelled.
func TestJanitor_RunContext(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend()
	janitor := athena.NewJanitor(backend, 10*time.Millisecond, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		janitor.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}
}
