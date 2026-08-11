package swf

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decisionHandlerTypes() []string {
	types := make([]string, 0, len(decisionHandlers()))
	for t := range decisionHandlers() {
		types = append(types, t)
	}
	slices.Sort(types)

	return types
}

// TestDecisionHandlers_CoverAllDecisionTypes is a table-driven test over the
// decision-type dispatch table (decision_tasks.go's decisionHandlers),
// asserting every SWF decision type this service claims to support has a
// registered handler -- guards against a decision type silently falling
// through the dispatch table with no handler (a disguised no-op).
func TestDecisionHandlers_CoverAllDecisionTypes(t *testing.T) {
	t.Parallel()

	want := []string{
		"CancelTimer",
		"CancelWorkflowExecution",
		"CompleteWorkflowExecution",
		"ContinueAsNewWorkflowExecution",
		"FailWorkflowExecution",
		"RecordMarker",
		"RequestCancelActivityTask",
		"RequestCancelExternalWorkflowExecution",
		"ScheduleActivityTask",
		"SignalExternalWorkflowExecution",
		"StartChildWorkflowExecution",
		"StartTimer",
	}

	got := decisionHandlerTypes()
	assert.ElementsMatch(t, want, got, "every SWF decision type must have a dispatch table entry")

	for _, dt := range want {
		t.Run(dt, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
			})
			require.NoError(t, err)

			task := b.PollForDecisionTask("dom", "tasks", 0, "")
			require.NotNil(t, task)

			// A bare decision (nil attrs) must never panic or error --
			// every handler is expected to treat missing attrs as a
			// silent no-op, exactly like real AWS's per-decision-type
			// attribute requirement (see e.g. ScheduleActivityTask).
			err = b.RespondDecisionTaskCompleted(task.TaskToken, "", []Decision{{DecisionType: dt}})
			require.NoError(t, err)
		})
	}
}
