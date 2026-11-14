package stepfunctions_test

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testsnap"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// The trace header must be set before the execution is visible to other callers.
func TestStartExecutionTraceHeader_NoRaceWithReplay(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	t.Cleanup(b.Destroy)

	sm, err := b.CreateStateMachine(t.Context(), "sm-trace", waitDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	const trace = "Root=1-5759e988-bd862e3fe1be46a994272793"

	var wg sync.WaitGroup

	arns := make([]string, 8)

	for i := range arns {
		wg.Go(func() {
			got, startErr := b.StartExecutionWithTrace(sm.StateMachineArn, "exec-trace", `{}`, trace)
			if assert.NoError(t, startErr) {
				_ = got.TraceHeader
				arns[i] = got.ExecutionArn
			}
		})
	}

	wg.Wait()

	desc, err := b.DescribeExecution(arns[0])
	require.NoError(t, err)
	assert.Equal(t, trace, desc.TraceHeader)
}

// The execution returned from StartExecution must not change when the
// interpreter goroutine completes it.
func TestStartExecution_ReturnedNotAliasedToInterpreter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
		t.Cleanup(b.Destroy)

		sm, err := b.CreateStateMachine(t.Context(), "sm-alias", passDefinition, validRoleARN, "STANDARD")
		require.NoError(t, err)

		held, err := b.StartExecutionWithTrace(sm.StateMachineArn, "exec-alias", `{"in":1}`, "Root=1-abc")
		require.NoError(t, err)

		testsnap.Unchanged(t, held, synctest.Wait)

		desc, err := b.DescribeExecution(held.ExecutionArn)
		require.NoError(t, err)
		assert.Equal(t, "SUCCEEDED", desc.Status)
		assert.Equal(t, "Root=1-abc", desc.TraceHeader)
	})
}
