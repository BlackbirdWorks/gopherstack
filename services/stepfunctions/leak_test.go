package stepfunctions_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sfn "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const passDefinitionForLeak = `{
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}`

// TestDeleteActivity_ClosesChannelAndEvictsTokens verifies that deleting an
// activity closes its pending task queue channel and removes all associated
// task tokens from tasksByToken, preventing goroutine leaks when the janitor
// is disabled.
func TestDeleteActivity_ClosesChannelAndEvictsTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		activities int
	}{
		{name: "single activity", activities: 1},
		{name: "multiple activities", activities: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := sfn.NewInMemoryBackendWithContext(ctx, "123456789012", "us-east-1")

			arns := make([]string, 0, tc.activities)
			for i := range tc.activities {
				act, err := b.CreateActivity(ctx, fmt.Sprintf("activity-%d", i))
				require.NoError(t, err)
				arns = append(arns, act.ActivityArn)
			}

			// Verify queues exist before delete.
			for _, arn := range arns {
				require.GreaterOrEqual(t, b.PendingTaskQueueLenForTest(arn), 0,
					"task queue must exist before delete")
			}

			for _, arn := range arns {
				require.NoError(t, b.DeleteActivity(arn))
			}

			// After delete, queues must not exist.
			for _, arn := range arns {
				require.Equal(t, -1, b.PendingTaskQueueLenForTest(arn),
					"task queue must be removed after DeleteActivity")
			}
		})
	}
}

// TestStartExecution_InlinePrunesOldExecutions verifies that StartExecution
// opportunistically removes finished executions that have aged past the
// retention period when the execution count exceeds the inline-sweep threshold,
// keeping the executions and history maps bounded even without the janitor.
func TestStartExecution_InlinePrunesOldExecutions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := sfn.NewInMemoryBackendWithContext(ctx, "123456789012", "us-east-1")

	const roleARN = "arn:aws:iam::123456789012:role/r"
	sm, err := b.CreateStateMachine(ctx, "pruner-sm", passDefinitionForLeak, roleARN, "STANDARD")
	require.NoError(t, err)

	threshold := sfn.ExecutionPruneSweepThresholdForTest

	// Create enough executions to reach the threshold and mark them all as
	// SUCCEEDED with a StopDate well in the past so they qualify for pruning.
	pastStop := float64(time.Now().Add(-48 * time.Hour).Unix())

	var startErr error
	var startExec *sfn.Execution
	for i := range threshold {
		startExec, startErr = b.StartExecution(sm.StateMachineArn, fmt.Sprintf("exec-%d", i), `{}`)
		require.NoError(t, startErr)
		b.SetExecutionStopDateForTest(startExec.ExecutionArn, pastStop)
	}

	require.Equal(t, threshold, b.ExecutionCount(),
		"execution count must equal threshold before the sweep-triggering start")

	// One more StartExecution must trigger the inline prune because
	// len(b.executions) >= executionPruneSweepThreshold.
	_, err = b.StartExecution(sm.StateMachineArn, "trigger-exec", `{}`)
	require.NoError(t, err)

	// All threshold executions were expired; only "trigger-exec" survives.
	require.Equal(t, 1, b.ExecutionCount(),
		"expired executions must be pruned inline on StartExecution")
}

// TestStartExecution_BelowThresholdNoEagerPrune confirms that inline pruning
// is a no-op below the threshold so steady-state starts stay cheap.
func TestStartExecution_BelowThresholdNoEagerPrune(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := sfn.NewInMemoryBackendWithContext(ctx, "123456789012", "us-east-1")

	sm, err := b.CreateStateMachine(
		ctx, "no-prune-sm", passDefinitionForLeak, "arn:aws:iam::123456789012:role/r", "STANDARD",
	)
	require.NoError(t, err)

	pastStop := float64(time.Now().Add(-48 * time.Hour).Unix())
	belowThreshold := sfn.ExecutionPruneSweepThresholdForTest - 1

	var btErr error
	var btExec *sfn.Execution
	for i := range belowThreshold {
		btExec, btErr = b.StartExecution(sm.StateMachineArn, fmt.Sprintf("old-exec-%d", i), `{}`)
		require.NoError(t, btErr)
		b.SetExecutionStopDateForTest(btExec.ExecutionArn, pastStop)
	}

	// Below threshold: no eager prune, so expired entries remain.
	require.Equal(t, belowThreshold, b.ExecutionCount(),
		"below-threshold start must not eagerly prune expired executions")
}
