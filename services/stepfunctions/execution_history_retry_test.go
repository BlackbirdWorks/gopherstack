package stepfunctions_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestExecutionHistory_TaskAttemptsAndCaughtFailure(t *testing.T) {
	t.Parallel()

	const task = `"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:f",`

	tests := []struct {
		name       string
		definition string
		wantStatus string
		wantTypes  []string
	}{
		{
			name: "retry",
			definition: `{"StartAt":"T","States":{"T":{` + task +
				`"Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":1,"MaxAttempts":1}],"End":true}}}`,
			wantTypes: []string{
				"TaskScheduled", "TaskStarted", "TaskFailed", "TaskScheduled", "TaskStarted", "TaskFailed",
			},
			wantStatus: "FAILED",
		},
		{
			name: "catch",
			definition: `{"StartAt":"T","States":{"T":{` + task +
				`"Catch":[{"ErrorEquals":["States.ALL"],"Next":"D"}],"End":true},"D":{"Type":"Succeed"}}}`,
			wantTypes:  []string{"TaskScheduled", "TaskStarted", "TaskFailed"},
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
				b.SetLambdaInvoker(&mockLambdaForBackend{returnErr: assert.AnError})

				sm, err := b.CreateStateMachine(context.Background(), "sm", tt.definition, "arn:role", "STANDARD")
				require.NoError(t, err)
				exec, err := b.StartExecution(sm.StateMachineArn, "e1", "{}")
				require.NoError(t, err)
				time.Sleep(2 * time.Second)
				synctest.Wait()

				got, err := b.DescribeExecution(exec.ExecutionArn)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, got.Status)

				events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
				require.NoError(t, err)
				assert.Equal(t, tt.wantTypes, taskEventTypes(events))
			})
		})
	}
}
