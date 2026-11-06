package ssm

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A copy handed out while an automation is InProgress must not alias the step
// slice that the lazy completion on a later read rewrites in place.
func TestAutomationExecutionCopy_NotAliasedToCompletion(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend().WithAutomationExecDelay(time.Hour)
	ctx := t.Context()

	started, err := b.StartAutomationExecution(ctx, &StartAutomationExecutionInput{DocumentName: "AWS-RunShellScript"})
	require.NoError(t, err)

	b.mu.Lock("test")
	exec, ok := b.automationExecutionsStore(defaultRegion).Get(started.AutomationExecutionID)
	require.True(t, ok)

	exec.Steps = []AutomationStepExec{{StepName: "s1", StepStatus: "Pending"}}
	b.mu.Unlock()

	in := &GetAutomationExecutionInput{AutomationExecutionID: started.AutomationExecutionID}

	held, err := b.GetAutomationExecution(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "InProgress", held.AutomationExecution.Status)

	b.mu.Lock("test")
	exec.completeAfter = 1
	b.mu.Unlock()

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = held.AutomationExecution.Steps[0].StepStatus
			}
		}
	})

	done, err := b.GetAutomationExecution(ctx, in)
	close(stop)
	wg.Wait()

	require.NoError(t, err)
	assert.Equal(t, "Success", done.AutomationExecution.Steps[0].StepStatus)
	assert.Equal(t, "Pending", held.AutomationExecution.Steps[0].StepStatus)
}
