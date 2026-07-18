package stepfunctions_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestStartExecutionASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		definition      string
		input           string
		wantStatus      string
		wantOutputKey   string
		wantError       string
		checkInitStatus bool
	}{
		{
			name:            "Pass",
			definition:      passDefinition,
			input:           `{"key":"val"}`,
			checkInitStatus: true,
			wantStatus:      "SUCCEEDED",
			wantOutputKey:   "key",
		},
		{
			name:       "Fail",
			definition: failDefinition,
			input:      `{}`,
			wantStatus: "FAILED",
			wantError:  "MyErr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			sm, err := b.CreateStateMachine(context.Background(), "asl-"+tt.name, tt.definition, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "asl-exec", tt.input)
			require.NoError(t, err)

			if tt.checkInitStatus {
				// Use DescribeExecution (returns a copy) to safely read status — avoids a data race
				// with the goroutine launched inside StartExecution that also writes to the execution struct.
				initialDesc, initDescErr := b.DescribeExecution(exec.ExecutionArn)
				require.NoError(t, initDescErr)
				assert.Contains(t, []string{"RUNNING", "SUCCEEDED"}, initialDesc.Status)
			}

			require.Eventually(t, func() bool {
				desc, descErr := b.DescribeExecution(exec.ExecutionArn)

				return descErr == nil && desc.Status == tt.wantStatus
			}, 5*time.Second, 50*time.Millisecond, "execution should reach "+tt.wantStatus)

			desc, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Status)

			if tt.wantOutputKey != "" {
				assert.Contains(t, desc.Output, tt.wantOutputKey)
			}
			if tt.wantError != "" {
				assert.Equal(t, tt.wantError, desc.Error)
			}
		})
	}
}

func TestExecution_SucceedsAfterPass(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"succ-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "succ-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", desc.Status)
	assert.NotNil(t, desc.StopDate)
}

func TestExecution_FailStateProducesFailedStatus(t *testing.T) {
	t.Parallel()

	failDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"ErrFoo","Cause":"test cause","End":true}}}`
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"fail-sm",
		failDef,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "fail-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "FAILED"
	}, 5*time.Second, 20*time.Millisecond)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", desc.Status)
	assert.Equal(t, "ErrFoo", desc.Error)
	assert.Equal(t, "test cause", desc.Cause)
}

// TestAudit_StartExecution_ExpressMachineSucceeds verifies that
// StartExecution (asynchronous execution) is permitted on EXPRESS state
// machines, matching AWS's "Asynchronous Express Workflows" support -- only
// StartSyncExecution is restricted to EXPRESS.
func TestStartExecution_ExpressMachineSucceeds(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"exp-async",
		minimalDefinition,
		validRoleARN,
		"EXPRESS",
	)
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "e1", "{}")
	require.NoError(t, err)
	assert.Contains(t, exec.ExecutionArn, "e1")
}

func TestStartSyncExecution_StandardMachineReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"std-sync",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	_, err = b.StartSyncExecution(sm.StateMachineArn, "sync-e1", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineTypeNotSupported)
}

func TestStartSyncExecution_Express_Succeeds(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"exp-sync",
		minimalDefinition,
		validRoleARN,
		"EXPRESS",
	)
	require.NoError(t, err)

	result, err := b.StartSyncExecution(sm.StateMachineArn, "sync-e2", `{"ok":true}`)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", result.Status)
}

func TestStartSyncExecution_Express_InputPayload(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"exp-input",
		minimalDefinition,
		validRoleARN,
		"EXPRESS",
	)
	require.NoError(t, err)

	result, err := b.StartSyncExecution(sm.StateMachineArn, "s1", `{"hello":"world"}`)
	require.NoError(t, err)
	assert.JSONEq(t, `{"hello":"world"}`, result.Input)
}

// ─── StopExecution ────────────────────────────────────────────────────────────

// TestStartSyncExecution verifies synchronous EXPRESS execution.
func TestStartSyncExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs       error
		name        string
		smType      string
		definition  string
		input       string
		wantStatus  string
		wantOutput  string
		wantErrCode string
		wantErr     bool
	}{
		{
			name:       "success_pass",
			smType:     "EXPRESS",
			definition: exprPassDef,
			input:      `{"x":1}`,
			wantStatus: "SUCCEEDED",
			wantOutput: `{"x":1}`,
		},
		{
			name:        "fail_state",
			smType:      "EXPRESS",
			definition:  exprFailDef,
			input:       `{}`,
			wantStatus:  "FAILED",
			wantErrCode: "SyncErr",
		},
		{
			name:       "standard_sm_rejected",
			smType:     "STANDARD",
			definition: exprPassDef,
			input:      `{}`,
			wantErr:    true,
			errIs:      stepfunctions.ErrStateMachineTypeNotSupported,
		},
		{
			name:    "nonexistent_sm",
			smType:  "EXPRESS",
			wantErr: true,
			errIs:   stepfunctions.ErrStateMachineDoesNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()

			var smARN string
			if !errors.Is(tt.errIs, stepfunctions.ErrStateMachineDoesNotExist) {
				sm, smErr := b.CreateStateMachine(
					context.Background(),
					"sync-sm-"+tt.name,
					tt.definition,
					"arn:role",
					tt.smType,
				)
				require.NoError(t, smErr)
				smARN = sm.StateMachineArn
			} else {
				smARN = "arn:aws:states:us-east-1:123456789012:stateMachine:nosuchsm"
			}

			result, err := b.StartSyncExecution(smARN, "", tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.wantStatus, result.Status)
			assert.Greater(t, result.StartDate, float64(0))
			assert.Greater(t, result.StopDate, float64(0))
			assert.NotEmpty(t, result.ExecutionArn)

			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, result.Output)
			}
			if tt.wantErrCode != "" {
				assert.Equal(t, tt.wantErrCode, result.Error)
			}
		})
	}
}

func TestStartExecution_WaitForTaskToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sendResult      func(*stepfunctions.InMemoryBackend, string) error
		name            string
		wantStatus      string
		wantOutput      string
		wantError       string
		wantCauseSubstr string
	}{
		{
			name: "send_task_success_completes_execution",
			sendResult: func(b *stepfunctions.InMemoryBackend, token string) error {
				return b.SendTaskSuccess(token, `{"result":"ok"}`)
			},
			wantStatus: "SUCCEEDED",
			wantOutput: `{"result":"ok"}`,
		},
		{
			name: "send_task_failure_fails_execution",
			sendResult: func(b *stepfunctions.InMemoryBackend, token string) error {
				return b.SendTaskFailure(token, "WorkerFailed", "worker failure")
			},
			wantStatus:      "FAILED",
			wantError:       "TaskFailed",
			wantCauseSubstr: "ActivityTaskFailed: WorkerFailed",
		},
	}

	const def = `{
"StartAt": "Send",
"States": {
  "Send": {
    "Type": "Task",
    "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
    "Parameters": {
      "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
      "MessageBody": "callback"
    },
    "End": true
  }
}
}`

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sqsMock := &mockStepFunctionsSQS{}
			b.SetSQSIntegration(sqsMock)

			sm, err := b.CreateStateMachine(context.Background(), "wait-token-sm-"+tt.name, def, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "wait-token-exec-"+tt.name, `{}`)
			require.NoError(t, err)

			var taskToken string
			require.Eventually(t, func() bool {
				tokens := b.TaskTokensForTest()
				if len(tokens) == 0 {
					return false
				}
				taskToken = tokens[0]

				return taskToken != ""
			}, 5*time.Second, 25*time.Millisecond)

			err = tt.sendResult(b, taskToken)
			require.NoError(t, err)

			var described *stepfunctions.Execution
			require.Eventually(t, func() bool {
				execution, describeErr := b.DescribeExecution(exec.ExecutionArn)
				if describeErr != nil {
					return false
				}
				described = execution

				return described.Status != "RUNNING"
			}, 5*time.Second, 25*time.Millisecond)

			assert.Equal(t, tt.wantStatus, described.Status)
			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, described.Output)
			}
			if tt.wantError != "" {
				assert.Equal(t, tt.wantError, described.Error)
			}
			if tt.wantCauseSubstr != "" {
				assert.Contains(t, described.Cause, tt.wantCauseSubstr)
			}

			assert.Equal(t, 1, sqsMock.callCount)
		})
	}
}

func TestBackend_RunParsedExecution_FailState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		wantStatus string
	}{
		{
			name:       "fail_state_execution",
			definition: sfnFailDefinition,
			wantStatus: "FAILED",
		},
		{
			name:       "pass_state_execution",
			definition: sfnPassDefinition,
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"run-sm-"+tt.name,
				tt.definition,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "run-exec", `{}`)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				desc, descErr := b.DescribeExecution(exec.ExecutionArn)

				return descErr == nil && desc.Status == tt.wantStatus
			}, 5*time.Second, 50*time.Millisecond)

			desc, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Status)
		})
	}
}

// ---- Persistence: Handler Snapshot/Restore delegation ----

func TestParallelState_WithCatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		definition  string
		input       string
		wantStatus  string
		wantRecover bool
	}{
		{
			name:        "branch_error_caught_by_catch_clause",
			definition:  parallelWithCatchDef,
			input:       `{"x": 1}`,
			wantStatus:  "SUCCEEDED",
			wantRecover: true,
		},
		{
			name:       "all_branches_succeed",
			definition: parallelSuccessDef,
			input:      `{}`,
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"parallel-catch-"+tt.name,
				tt.definition,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "exec-"+tt.name, tt.input)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status != "RUNNING"
			}, 10*time.Second, 25*time.Millisecond)

			d, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, d.Status, "unexpected execution status")
		})
	}
}

func TestWaitState_TimestampPast(t *testing.T) {
	t.Parallel()

	b := newSFBackend()
	sm, err := b.CreateStateMachine(context.Background(), "wait-ts", waitTimestampPastDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "wait-exec", `{"x": 1}`)
	require.NoError(t, err)

	// Past timestamp → should complete quickly.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 25*time.Millisecond)
}

func TestChoiceState_AndOrCombiners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantMatch string
	}{
		{
			name:      "and_combiner_both_conditions_true",
			input:     `{"x": 5, "y": "foo", "a": false, "b": false}`,
			wantMatch: "both",
		},
		{
			name:      "or_combiner_one_condition_true",
			input:     `{"x": -1, "y": "bar", "a": true, "b": false}`,
			wantMatch: "either",
		},
		{
			name:      "default_when_no_match",
			input:     `{"x": -1, "y": "bar", "a": false, "b": false}`,
			wantMatch: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"choice-"+tt.name,
				choiceAndOrDef,
				"arn:role",
				"EXPRESS",
			)
			require.NoError(t, err)

			result, err := b.StartSyncExecution(sm.StateMachineArn, "", tt.input)
			require.NoError(t, err)
			require.Equal(t, "SUCCEEDED", result.Status)
			assert.Contains(t, result.Output, tt.wantMatch)
		})
	}
}
