package gcpworkflows_test

import (
	"testing"
	"time"

	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

const sleeper = `
main:
  steps:
    - nap:
        call: sys.sleep
        args: {seconds: 3600}
    - done:
        return: ok
`

func (e *env) state(t *testing.T, name string) executionspb.Execution_State {
	t.Helper()

	got, err := e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: name})
	require.NoError(t, err)

	return got.GetState()
}

func TestExecutionConcurrencyQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStates []executionspb.Execution_State
		active     int
		queued     int
		wantCode   codes.Code
	}{
		{
			name: "queued over the active quota", active: 1, queued: 5, wantStates: []executionspb.Execution_State{
				executionspb.Execution_ACTIVE, executionspb.Execution_QUEUED, executionspb.Execution_QUEUED,
			},
		},
		{
			name: "backlog full is resource exhausted", active: 1, queued: 1,
			wantStates: []executionspb.Execution_State{
				executionspb.Execution_ACTIVE, executionspb.Execution_QUEUED,
			},
			wantCode: codes.ResourceExhausted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.clock.holdSleeps()
			e.handler.Backend.SetExecutionLimits(tt.active, tt.queued)

			w := e.createWorkflow(t, "quota", sleeper)

			var names []string

			for range tt.wantStates {
				names = append(names, e.execute(t, w.GetName(), "").GetName())
			}

			for i, want := range tt.wantStates {
				assert.Equal(t, want, e.state(t, names[i]))
			}

			if tt.wantCode != codes.OK {
				_, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{Parent: w.GetName()})
				assert.Equal(t, tt.wantCode, code(err))
			}
		})
	}
}

func TestQueuedExecutionStartsWhenCapacityFrees(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.clock.holdSleeps()
	e.handler.Backend.SetExecutionLimits(1, 5)

	w := e.createWorkflow(t, "promote", sleeper)
	first := e.execute(t, w.GetName(), "").GetName()
	second := e.execute(t, w.GetName(), "").GetName()
	third := e.execute(t, w.GetName(), "").GetName()

	_, err := e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: first})
	require.NoError(t, err)

	assert.Equal(t, executionspb.Execution_ACTIVE, e.state(t, second), "oldest queued execution starts first")
	assert.Equal(t, executionspb.Execution_QUEUED, e.state(t, third))

	cancelled, err := e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: third})
	require.NoError(t, err)
	assert.Equal(t, executionspb.Execution_CANCELLED, cancelled.GetState(), "a queued execution can be cancelled")
}

func TestExecutionExpiresAfterOneYear(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.clock.holdSleeps()

	w := e.createWorkflow(t, "forever", sleeper)
	name := e.execute(t, w.GetName(), "").GetName()

	require.Equal(t, executionspb.Execution_ACTIVE, e.state(t, name))
	assert.Equal(t, []time.Duration{365 * 24 * time.Hour}, e.clock.fireTimers())

	got, err := e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, executionspb.Execution_FAILED, got.GetState())
	assert.Contains(t, got.GetError().GetPayload(), "TimeoutError")
}

func TestExecutionQuotaHoldsUnderConcurrentCreates(t *testing.T) {
	t.Parallel()

	const (
		active  = 3
		queued  = 4
		callers = 16
	)

	e := newEnv(t)
	e.clock.holdSleeps()
	e.handler.Backend.SetExecutionLimits(active, queued)

	w := e.createWorkflow(t, "race", sleeper)
	results := make(chan executionspb.Execution_State, callers)

	for range callers {
		go func() {
			ex, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{Parent: w.GetName()})
			if err != nil {
				results <- executionspb.Execution_STATE_UNSPECIFIED

				return
			}

			results <- ex.GetState()
		}()
	}

	var nActive, nQueued, nRejected int

	for range callers {
		switch <-results {
		case executionspb.Execution_ACTIVE:
			nActive++
		case executionspb.Execution_QUEUED:
			nQueued++
		default:
			nRejected++
		}
	}

	assert.Equal(t, active, nActive)
	assert.Equal(t, queued, nQueued)
	assert.Equal(t, callers-active-queued, nRejected)
}

func TestFinishedExecutionsExpireAfterRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		want    codes.Code
	}{
		{"kept inside retention", 89 * 24 * time.Hour, codes.OK},
		{"dropped after retention", 91 * 24 * time.Hour, codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ex := e.run(t, "ret", "- r:\n    return: 1\n", "")
			require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState())

			e.clock.advance(tt.advance)

			_, err := e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: ex.GetName()})
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestCancelledQueuedExecutionsFreeBacklog(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.clock.holdSleeps()
	e.handler.Backend.SetExecutionLimits(1, 2)

	w := e.createWorkflow(t, "backlog", sleeper)
	e.execute(t, w.GetName(), "")

	for range 5 {
		q := e.execute(t, w.GetName(), "").GetName()

		_, err := e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: q})
		require.NoError(t, err)
	}

	first := e.execute(t, w.GetName(), "").GetName()
	second := e.execute(t, w.GetName(), "").GetName()

	assert.Equal(t, executionspb.Execution_QUEUED, e.state(t, first))
	assert.Equal(t, executionspb.Execution_QUEUED, e.state(t, second))

	_, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{Parent: w.GetName()})
	assert.Equal(t, codes.ResourceExhausted, code(err))
}
