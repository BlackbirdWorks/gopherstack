package swf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decisionTypeStartChildWorkflowExecution avoids repeating the bare
// "StartChildWorkflowExecution" string literal (see decisionHandlers() in
// decision_tasks.go for the real dispatch-table key this must match).
const decisionTypeStartChildWorkflowExecution = "StartChildWorkflowExecution"

// mustLiveExecution returns the live (mutable) *WorkflowExecution backing
// domain "dom"+workflowID+runID, bypassing DescribeWorkflowExecution's
// defensive copy so tests can backdate StartTimestamp directly.
func mustLiveExecution(t *testing.T, b *InMemoryBackend, workflowID, runID string) *WorkflowExecution {
	t.Helper()

	live, ok := b.executions.Get(executionKey("dom", workflowID, runID))
	require.True(t, ok)

	return live
}

// TestSweepTimedOutExecutionsLocked_Evaluation pins the sweep's boundary
// behaviour against an explicitly supplied evaluation instant -- proving the
// sweeper's clock is a parameter, not an internal time.Now() call, so this
// needs no sleeping or synctest.
func TestSweepTimedOutExecutionsLocked_Evaluation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout string
		elapsed time.Duration
		want    bool
	}{
		{"past deadline times out", "60", 61 * time.Second, true},
		{"exactly at deadline times out", "60", 60 * time.Second, true},
		{"before deadline stays running", "60", 59 * time.Second, false},
		{"unset timeout never expires", "", 365 * 24 * time.Hour, false},
		{"none timeout never expires", "NONE", 365 * 24 * time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

			started, err := b.StartWorkflowExecution(StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
				ExecutionStartToCloseTimeout: tt.timeout,
			})
			require.NoError(t, err)

			live := mustLiveExecution(t, b, "wf-1", started.RunID)
			evalAt := time.UnixMilli(int64(live.StartTimestamp * milliDivisor)).Add(tt.elapsed)

			b.mu.Lock("test")
			swept := b.sweepTimedOutExecutionsLocked(evalAt)
			b.mu.Unlock()

			if tt.want {
				assert.Equal(t, 1, swept)
				assert.Equal(t, statusTimedOut, live.Status)
				assert.Equal(t, statusTimedOut, live.CloseStatus)
			} else {
				assert.Equal(t, 0, swept)
				assert.Equal(t, statusRunning, live.Status)
			}
		})
	}
}

// TestTimeoutExecutionLocked_HistoryEvent verifies the WorkflowExecutionTimedOut
// event carries exactly the two fields real SWF defines (childPolicy,
// timeoutType=START_TO_CLOSE) -- see timeout_sweep.go's citation.
func TestTimeoutExecutionLocked_HistoryEvent(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	started, err := b.StartWorkflowExecution(StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
		ExecutionStartToCloseTimeout: "60",
		ChildPolicy:                  "TERMINATE",
	})
	require.NoError(t, err)

	live := mustLiveExecution(t, b, "wf-1", started.RunID)
	evalAt := time.UnixMilli(int64(live.StartTimestamp * milliDivisor)).Add(61 * time.Second)

	b.mu.Lock("test")
	swept := b.sweepTimedOutExecutionsLocked(evalAt)
	b.mu.Unlock()
	require.Equal(t, 1, swept)

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", started.RunID, 0, "", false)

	var attrs map[string]any
	for i := range events {
		if events[i].EventType == "WorkflowExecutionTimedOut" {
			attrs, _ = events[i].Attributes["workflowExecutionTimedOutEventAttributes"].(map[string]any)
		}
	}
	require.NotNil(t, attrs, "expected a WorkflowExecutionTimedOut history event")
	assert.Equal(t, "TERMINATE", attrs["childPolicy"])
	assert.Equal(t, "START_TO_CLOSE", attrs["timeoutType"])

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1", started.RunID)
	require.NoError(t, err)
	assert.Equal(t, "TIMED_OUT", exec.CloseStatus)
	assert.NotZero(t, exec.CloseTimestamp)
}

// TestTimeoutExecutionLocked_CascadesChildPolicy verifies a timed-out
// execution's ChildPolicy cascades onto its own open children exactly like
// terminateExecutionLocked's cascade (applyChildPolicyLocked is shared code
// between the two paths) -- this is the gopherstack-7gse gap: real SWF
// invokes the child policy on exactly two events, TerminateWorkflowExecution
// and an execution timing out, and only the first was reachable before this.
func TestTimeoutExecutionLocked_CascadesChildPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		childPolicy  string
		wantChildTID string
	}{
		{"terminate cascades to terminate the child", "TERMINATE", statusTerminated},
		{"request_cancel cascades a cancel request, child stays open", "REQUEST_CANCEL", statusRunning},
		{"abandon leaves the child untouched", "ABANDON", statusRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			require.NoError(t, b.RegisterWorkflowType("dom", "childType", "1.0", "", WorkflowTypeDefaults{}))

			parent, err := b.StartWorkflowExecution(StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
				ExecutionStartToCloseTimeout: "60",
				ChildPolicy:                  tt.childPolicy,
			})
			require.NoError(t, err)

			parentTask := b.PollForDecisionTask("dom", "parent-tasks", 0, "")
			require.NotNil(t, parentTask)
			require.NoError(t, b.RespondDecisionTaskCompleted(parentTask.TaskToken, "", []Decision{{
				DecisionType: decisionTypeStartChildWorkflowExecution,
				StartChildWorkflowExecutionAttrs: &StartChildWorkflowExecutionDecisionAttrs{
					WorkflowID:   "child-1",
					WorkflowType: WorkflowTypeRef{Name: "childType", Version: "1.0"},
					TaskList:     "child-tasks",
				},
			}}))

			live := mustLiveExecution(t, b, "parent-1", parent.RunID)
			evalAt := time.UnixMilli(int64(live.StartTimestamp * milliDivisor)).Add(61 * time.Second)

			b.mu.Lock("test")
			swept := b.sweepTimedOutExecutionsLocked(evalAt)
			b.mu.Unlock()
			require.Equal(t, 1, swept)

			child, err := b.DescribeWorkflowExecution("dom", "child-1", "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantChildTID, child.Status)

			if tt.childPolicy == "REQUEST_CANCEL" {
				assert.True(t, child.CancelRequested)
			}
		})
	}
}

// TestDescribeWorkflowExecution_SweepsOnRead verifies the sweep is actually
// wired into a public read entry point, not just callable in isolation:
// backdating StartTimestamp into the real past (no sleep, no fabricated
// clock) is enough for a plain DescribeWorkflowExecution call to observe the
// execution as TIMED_OUT, since Describe now sweeps with the real
// time.Now() before resolving the execution.
func TestDescribeWorkflowExecution_SweepsOnRead(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	started, err := b.StartWorkflowExecution(StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
		ExecutionStartToCloseTimeout: "60",
	})
	require.NoError(t, err)

	live := mustLiveExecution(t, b, "wf-1", started.RunID)
	live.StartTimestamp -= 3600 // an hour in the real past, well past the 60s timeout

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1", started.RunID)
	require.NoError(t, err)
	assert.Equal(t, statusTimedOut, exec.Status)
	assert.Equal(t, statusTimedOut, exec.CloseStatus)
}

// TestSnapshotRestore_TimedOutExecution verifies a TIMED_OUT execution's
// Status/CloseStatus/CloseTimestamp survive Snapshot/Restore. No new field
// was added for timeout support (Status/CloseStatus/CloseTimestamp/
// ChildPolicy already existed and already round-trip via the "clean"
// executions store.Table -- see store.go), so this exercises the TIMED_OUT
// value through that existing path rather than pinning a schema change.
func TestSnapshotRestore_TimedOutExecution(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	started, err := b.StartWorkflowExecution(StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
		ExecutionStartToCloseTimeout: "60",
		ChildPolicy:                  "TERMINATE",
	})
	require.NoError(t, err)

	live := mustLiveExecution(t, b, "wf-1", started.RunID)
	evalAt := time.UnixMilli(int64(live.StartTimestamp * milliDivisor)).Add(61 * time.Second)

	b.mu.Lock("test")
	swept := b.sweepTimedOutExecutionsLocked(evalAt)
	b.mu.Unlock()
	require.Equal(t, 1, swept)

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	restored, err := b2.DescribeWorkflowExecution("dom", "wf-1", started.RunID)
	require.NoError(t, err)
	assert.Equal(t, statusTimedOut, restored.Status)
	assert.Equal(t, statusTimedOut, restored.CloseStatus)
	assert.InDelta(t, live.CloseTimestamp, restored.CloseTimestamp, 0)
}
