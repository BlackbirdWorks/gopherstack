package athena_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestJanitor_EvictsQueryResultsWithExecutions verifies that the janitor evicts
// a query's cached result set alongside the execution it belongs to, closing the
// queryResults leak (previously only queryExecutions were swept).
func TestJanitor_EvictsQueryResultsWithExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		state           string
		completionDelay time.Duration
		wantEvicted     bool
	}{
		{name: "evict_old_succeeded", state: "SUCCEEDED", completionDelay: -2 * time.Hour, wantEvicted: true},
		{name: "evict_old_failed", state: "FAILED", completionDelay: -2 * time.Hour, wantEvicted: true},
		{name: "keep_recent", state: "SUCCEEDED", completionDelay: -1 * time.Minute, wantEvicted: false},
		{name: "keep_running", state: "RUNNING", completionDelay: -2 * time.Hour, wantEvicted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")

			id, err := b.StartQueryExecution("SELECT 1", "primary",
				athena.QueryExecutionContext{Database: "default"},
				athena.ResultConfiguration{}, nil, nil)
			require.NoError(t, err)

			require.Equal(t, 1, b.QueryResultCount(), "SELECT caches a result set")

			b.SetQueryExecutionState(id, tt.state, tt.completionDelay)

			athena.NewJanitor(b, time.Hour, time.Hour).SweepOnce(t.Context())

			if tt.wantEvicted {
				assert.Equal(t, 0, b.QueryExecutionCount())
				assert.Equal(t, 0, b.QueryResultCount(), "result set must be evicted with its execution")
			} else {
				assert.Equal(t, 1, b.QueryExecutionCount())
				assert.Equal(t, 1, b.QueryResultCount())
			}
		})
	}
}

// TestJanitor_EvictsStaleSessions verifies terminal, stale sessions are swept and
// active/recent ones are preserved.
func TestJanitor_EvictsStaleSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		delay       time.Duration
		terminate   bool
		wantEvicted bool
	}{
		{name: "evict_old_terminated", terminate: true, delay: -2 * time.Hour, wantEvicted: true},
		{name: "keep_recent_terminated", terminate: true, delay: -1 * time.Minute, wantEvicted: false},
		{name: "keep_active", terminate: false, wantEvicted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")

			id, _, err := b.StartSession("primary", "desc", "",
				athena.EngineConfiguration{}, athena.SessionConfiguration{},
				athena.MonitoringConfiguration{}, "")
			require.NoError(t, err)
			require.Equal(t, 1, b.SessionCount())

			if tt.terminate {
				b.SetSessionTerminated(id, tt.delay)
			}

			athena.NewJanitor(b, time.Hour, time.Hour).SweepOnce(t.Context())

			if tt.wantEvicted {
				assert.Equal(t, 0, b.SessionCount())
			} else {
				assert.Equal(t, 1, b.SessionCount())
			}
		})
	}
}

// TestJanitor_EvictsStaleCalculations verifies terminal, stale calculations are
// swept and recent/active ones are preserved.
func TestJanitor_EvictsStaleCalculations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		state       string
		delay       time.Duration
		wantEvicted bool
	}{
		{name: "evict_old_completed", state: "COMPLETED", delay: -2 * time.Hour, wantEvicted: true},
		{name: "evict_old_failed", state: "FAILED", delay: -2 * time.Hour, wantEvicted: true},
		{name: "keep_recent_completed", state: "COMPLETED", delay: -1 * time.Minute, wantEvicted: false},
		{name: "keep_running", state: "RUNNING", delay: -2 * time.Hour, wantEvicted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")

			sid, _, err := b.StartSession("primary", "desc", "",
				athena.EngineConfiguration{}, athena.SessionConfiguration{},
				athena.MonitoringConfiguration{}, "")
			require.NoError(t, err)

			cid, _, err := b.StartCalculationExecution(sid, "calc", "print(1)")
			require.NoError(t, err)
			require.Equal(t, 1, b.CalculationCount())

			b.SetCalculationCompletion(cid, tt.state, tt.delay)

			// Keep the parent session out of the sweep for this assertion.
			athena.NewJanitor(b, time.Hour, time.Hour).SweepOnce(t.Context())

			if tt.wantEvicted {
				assert.Equal(t, 0, b.CalculationCount())
			} else {
				assert.Equal(t, 1, b.CalculationCount())
			}
		})
	}
}
