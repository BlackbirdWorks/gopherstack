package swf_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// completeOpenRun polls the open decision task for domain/taskList and
// completes the workflow execution via a CompleteWorkflowExecution decision.
func completeOpenRun(t *testing.T, b *swf.InMemoryBackend, domain, taskList string) {
	t.Helper()

	token := pollDecisionTask(t, b, domain, taskList)
	decisions := []swf.Decision{{
		DecisionType:                   "CompleteWorkflowExecution",
		CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{Result: "done"},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))
}

// TestMultiRunHistory pins gopherstack-jsi8: executions/history are keyed by
// domain+workflowID+runID, so a second run of the same workflowId does not
// collide with or overwrite an earlier, already-closed run.
func TestMultiRunHistory(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	run1, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-multi", TaskList: "default",
	})
	require.NoError(t, err)

	completeOpenRun(t, b, "dom", "default")

	run2, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-multi", TaskList: "default",
	})
	require.NoError(t, err)

	require.NotEqual(t, run1.RunID, run2.RunID, "successive runs must get distinct run IDs")

	tests := []struct {
		name       string
		runID      string
		wantRunID  string
		wantStatus string
	}{
		{
			name:       "explicit old run id returns the closed first run",
			runID:      run1.RunID,
			wantRunID:  run1.RunID,
			wantStatus: "COMPLETED",
		},
		{
			name:       "explicit new run id returns the open second run",
			runID:      run2.RunID,
			wantRunID:  run2.RunID,
			wantStatus: "RUNNING",
		},
		{
			name:       "empty run id resolves to the currently open run",
			runID:      "",
			wantRunID:  run2.RunID,
			wantStatus: "RUNNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exec, descErr := b.DescribeWorkflowExecution("dom", "wf-multi", tt.runID)
			require.NoError(t, descErr)
			assert.Equal(t, tt.wantRunID, exec.RunID)
			assert.Equal(t, tt.wantStatus, exec.Status)
		})
	}

	t.Run("closed run history does not leak into the new run's history", func(t *testing.T) {
		t.Parallel()

		run2Events, _ := b.GetWorkflowExecutionHistory("dom", "wf-multi", run2.RunID, 0, "", false)
		for _, ev := range run2Events {
			assert.NotEqual(t, "WorkflowExecutionCompleted", ev.EventType,
				"run2's history must not contain run1's completion event")
		}

		run1Events, _ := b.GetWorkflowExecutionHistory("dom", "wf-multi", run1.RunID, 0, "", false)
		var sawCompleted bool
		for _, ev := range run1Events {
			if ev.EventType == "WorkflowExecutionCompleted" {
				sawCompleted = true
			}
		}
		assert.True(t, sawCompleted, "run1's own history must contain its completion event")
	})

	t.Run("starting a third run while the second is still open is rejected", func(t *testing.T) {
		t.Parallel()

		_, startErr := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
			Domain: "dom", WorkflowID: "wf-multi", TaskList: "default",
		})
		require.ErrorIs(t, startErr, swf.ErrWorkflowAlreadyStarted)
	})

	t.Run("empty run id falls back to the most recent run once none is open", func(t *testing.T) {
		t.Parallel()

		b2 := swf.NewInMemoryBackend()
		require.NoError(t, b2.RegisterDomain("dom", "", "NONE"))

		first, startErr := b2.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
			Domain: "dom", WorkflowID: "wf-closed", TaskList: "default",
		})
		require.NoError(t, startErr)
		completeOpenRun(t, b2, "dom", "default")

		exec, descErr := b2.DescribeWorkflowExecution("dom", "wf-closed", "")
		require.NoError(t, descErr)
		assert.Equal(t, first.RunID, exec.RunID)
		assert.Equal(t, "COMPLETED", exec.Status)
	})
}

// TestLRUEvictionPurgesPendingDecisionTasks pins the other half of
// gopherstack-jsi8 ("LRU-eviction ghost queue rows"): once the backend's
// execution cache reaches its retention cap and evicts the oldest run
// (maxWorkflowExecutions, unexported -- 10_000 as of this test), that run's
// still-pending decision task must be purged from its task list's queue too,
// not left behind as a ghost entry referencing an execution/history that no
// longer exists.
func TestLRUEvictionPurgesPendingDecisionTasks(t *testing.T) {
	t.Parallel()

	const maxWorkflowExecutions = 10_000

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	for i := range maxWorkflowExecutions {
		_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
			Domain:     "dom",
			WorkflowID: "wf-evict-" + strconv.Itoa(i),
			TaskList:   "shared",
		})
		require.NoError(t, err)
	}

	// The maxWorkflowExecutions-th create pushed the cache over its cap,
	// evicting the very first run (wf-evict-0) -- its pending decision task
	// must have been purged along with it, leaving one fewer pending task
	// than executions created.
	assert.Equal(t, maxWorkflowExecutions-1, b.CountPendingDecisionTasks("dom", "shared"))

	_, err := b.DescribeWorkflowExecution("dom", "wf-evict-0", "")
	require.ErrorIs(t, err, swf.ErrNotFound, "the evicted run's execution row must be gone")

	task := b.PollForDecisionTask("dom", "shared", 0, "")
	require.NotNil(t, task)
	assert.NotEqual(t, "wf-evict-0", task.WorkflowID, "the evicted run's ghost task must not be pollable")
}
