package stepfunctions_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/stepfunctions"
)

const passDefinition = `{
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}`

func TestStepFunctionsBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *stepfunctions.InMemoryBackend)
	}{
		{name: "CreateStateMachine", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, err := b.CreateStateMachine("my-sm", "{}", "arn:aws:iam::123456789012:role/role", "STANDARD")
			require.NoError(t, err)
			assert.Equal(t, "my-sm", sm.Name)
			assert.Contains(t, sm.StateMachineArn, "my-sm")
			assert.Equal(t, "ACTIVE", sm.Status)
			assert.Equal(t, "STANDARD", sm.Type)
		}},
		{name: "CreateStateMachine/DefaultType", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, err := b.CreateStateMachine("typed-sm", "{}", "arn:role", "")
			require.NoError(t, err)
			assert.Equal(t, "STANDARD", sm.Type)
		}},
		{name: "CreateStateMachine/AlreadyExists", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			_, err := b.CreateStateMachine("dup-sm", "{}", "arn:role", "STANDARD")
			require.NoError(t, err)

			_, err = b.CreateStateMachine("dup-sm", "{}", "arn:role", "STANDARD")
			require.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
		}},
		{name: "DescribeStateMachine", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, err := b.CreateStateMachine("desc-sm", `{"Comment":"test"}`, "arn:role", "EXPRESS")
			require.NoError(t, err)

			got, err := b.DescribeStateMachine(sm.StateMachineArn)
			require.NoError(t, err)
			assert.Equal(t, "desc-sm", got.Name)
			assert.Equal(t, "EXPRESS", got.Type)
			assert.JSONEq(t, `{"Comment":"test"}`, got.Definition)
		}},
		{name: "DescribeStateMachine/NotFound", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			_, err := b.DescribeStateMachine("arn:aws:states:us-east-1:123:stateMachine:nonexistent")
			require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
		}},
		{name: "ListStateMachines", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			_, _ = b.CreateStateMachine("alpha-sm", "{}", "arn:role", "STANDARD")
			_, _ = b.CreateStateMachine("beta-sm", "{}", "arn:role", "STANDARD")

			sms, next, err := b.ListStateMachines("", 0)
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, sms, 2)
		}},
		{name: "ListStateMachines/Pagination", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			for i := range 5 {
				_, _ = b.CreateStateMachine(
					"sm-"+string(rune('a'+i)), "{}", "arn:role", "STANDARD",
				)
			}

			page1, next, err := b.ListStateMachines("", 2)
			require.NoError(t, err)
			assert.Len(t, page1, 2)
			assert.NotEmpty(t, next)

			page2, next2, err := b.ListStateMachines(next, 2)
			require.NoError(t, err)
			assert.Len(t, page2, 2)
			assert.NotEmpty(t, next2)

			page3, next3, err := b.ListStateMachines(next2, 2)
			require.NoError(t, err)
			assert.Len(t, page3, 1)
			assert.Empty(t, next3)
		}},
		{name: "ListStateMachines/EmptyToken", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			// nextToken beyond size returns empty
			sms, next, err := b.ListStateMachines("999", 0)
			require.NoError(t, err)
			assert.Empty(t, sms)
			assert.Empty(t, next)
		}},
		{name: "DeleteStateMachine", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, err := b.CreateStateMachine("to-delete", "{}", "arn:role", "STANDARD")
			require.NoError(t, err)

			err = b.DeleteStateMachine(sm.StateMachineArn)
			require.NoError(t, err)

			_, err = b.DescribeStateMachine(sm.StateMachineArn)
			require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
		}},
		{name: "DeleteStateMachine/NotFound", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			err := b.DeleteStateMachine("arn:aws:states:us-east-1:123:stateMachine:nonexistent")
			require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
		}},
		{name: "StartExecution", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("exec-sm", "{}", "arn:role", "STANDARD")
			exec, err := b.StartExecution(sm.StateMachineArn, "exec1", `{"key":"value"}`)
			require.NoError(t, err)
			assert.Contains(t, exec.ExecutionArn, "exec1")
			assert.Equal(t, "SUCCEEDED", exec.Status)
			assert.JSONEq(t, `{"key":"value"}`, exec.Output)
			assert.NotNil(t, exec.StopDate)
		}},
		{name: "StartExecution/SMNotFound", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			_, err := b.StartExecution("arn:nonexistent", "exec1", "")
			require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
		}},
		{name: "StartExecution/AlreadyExists", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("dup-exec-sm", "{}", "arn:role", "STANDARD")
			_, err := b.StartExecution(sm.StateMachineArn, "exec1", "")
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, "exec1", "")
			require.ErrorIs(t, err, stepfunctions.ErrExecutionAlreadyExists)
		}},
		{name: "DescribeExecution", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("desc-exec-sm", "{}", "arn:role", "STANDARD")
			exec, _ := b.StartExecution(sm.StateMachineArn, "exec1", `{"x":1}`)

			got, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, "SUCCEEDED", got.Status)
			assert.Equal(t, `{"x":1}`, got.Input)
		}},
		{name: "DescribeExecution/NotFound", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			_, err := b.DescribeExecution("arn:nonexistent")
			require.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
		}},
		{name: "ListExecutions", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("list-exec-sm", "{}", "arn:role", "STANDARD")
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-a", "")
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-b", "")

			execs, next, err := b.ListExecutions(sm.StateMachineArn, "", "", 0)
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, execs, 2)
		}},
		{name: "ListExecutions/StatusFilter", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("filter-sm", "{}", "arn:role", "STANDARD")
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-a", "")

			// Filter by RUNNING - should return 0 (auto-SUCCEEDED)
			running, _, err := b.ListExecutions(sm.StateMachineArn, "RUNNING", "", 0)
			require.NoError(t, err)
			assert.Empty(t, running)

			// Filter by SUCCEEDED - should return 1
			succeeded, _, err := b.ListExecutions(sm.StateMachineArn, "SUCCEEDED", "", 0)
			require.NoError(t, err)
			assert.Len(t, succeeded, 1)
		}},
		{name: "GetExecutionHistory", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("hist-sm", "{}", "arn:role", "STANDARD")
			exec, _ := b.StartExecution(sm.StateMachineArn, "exec-h", "")

			events, next, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, events, 2)
			assert.Equal(t, "ExecutionStarted", events[0].Type)
			assert.Equal(t, "ExecutionSucceeded", events[1].Type)
		}},
		{name: "GetExecutionHistory/ReverseOrder", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("hist-rev-sm", "{}", "arn:role", "STANDARD")
			exec, _ := b.StartExecution(sm.StateMachineArn, "exec-rev", "")

			events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, true)
			require.NoError(t, err)
			assert.Equal(t, "ExecutionSucceeded", events[0].Type)
			assert.Equal(t, "ExecutionStarted", events[1].Type)
		}},
		{name: "GetExecutionHistory/NotFound", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			_, _, err := b.GetExecutionHistory("arn:nonexistent", "", 0, false)
			require.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
		}},
		{name: "StopExecution", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, _ := b.CreateStateMachine("stop-sm", "{}", "arn:role", "STANDARD")
			exec, _ := b.StartExecution(sm.StateMachineArn, "exec-stop", "")

			err := b.StopExecution(exec.ExecutionArn, "MyError", "stopped by test")
			require.NoError(t, err)

			got, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, "ABORTED", got.Status)
			assert.Equal(t, "MyError", got.Error)
			assert.Equal(t, "stopped by test", got.Cause)
			assert.NotNil(t, got.StopDate)
		}},
		{name: "StopExecution/NotFound", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			err := b.StopExecution("arn:nonexistent", "", "")
			require.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
		}},
		{name: "SetLambdaInvoker", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			// Setting nil is a no-op but shouldn't panic.
			b.SetLambdaInvoker(nil)
		}},
		{name: "SetLogger", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			// SetLogger with a real logger should not panic.
			b.SetLogger(slog.Default())
		}},
		{name: "StartExecution/ASL_Pass", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			sm, err := b.CreateStateMachine("asl-pass", passDefinition, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "asl-exec-1", `{"key":"val"}`)
			require.NoError(t, err)
			// Use DescribeExecution (returns a copy) to safely read status — avoids a data race
			// with the goroutine launched inside StartExecution that also writes to the execution struct.
			initialDesc, initDescErr := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, initDescErr)
			assert.Contains(t, []string{"RUNNING", "SUCCEEDED"}, initialDesc.Status)

			// Wait for ASL execution to complete.
			require.Eventually(t, func() bool {
				desc, descErr := b.DescribeExecution(exec.ExecutionArn)

				return descErr == nil && desc.Status == "SUCCEEDED"
			}, 5*time.Second, 50*time.Millisecond, "execution should succeed")

			desc, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, "SUCCEEDED", desc.Status)
			assert.Contains(t, desc.Output, "key")
		}},
		{name: "StartExecution/ASL_Fail", run: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
			failDef := `{
"StartAt": "F",
"States": {
"F": {"Type": "Fail", "Error": "MyErr", "Cause": "test cause"}
}
}`

			sm, err := b.CreateStateMachine("asl-fail-sm", failDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "asl-fail-exec", `{}`)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				desc, descErr := b.DescribeExecution(exec.ExecutionArn)

				return descErr == nil && desc.Status == "FAILED"
			}, 5*time.Second, 50*time.Millisecond, "execution should fail")

			desc, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, "FAILED", desc.Status)
			assert.Equal(t, "MyErr", desc.Error)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
