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

const (
	exprPassDef = `{
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}`
	exprFailDef = `{
"StartAt": "F",
"States": {
"F": {"Type": "Fail", "Error": "SyncErr", "Cause": "sync failure"}
}
}`
)

func newSFBackend() *stepfunctions.InMemoryBackend {
	return stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

type mockStepFunctionsSQS struct {
	callCount int
}

func (m *mockStepFunctionsSQS) SFNSendMessage(
	_ context.Context,
	_, _, _, _ string,
	_ int,
) (string, string, error) {
	m.callCount++

	return "msg-id", "md5", nil
}

// TestHistoryEventCap verifies that history recording is capped at maxHistoryEvents.
func TestHistoryEventCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		preFill       int
		addMoreEvents int
		wantLen       int
	}{
		{
			name:          "cap_enforced_when_at_limit",
			preFill:       stepfunctions.MaxHistoryEventsForTest,
			addMoreEvents: 5,
			wantLen:       stepfunctions.MaxHistoryEventsForTest,
		},
		{
			name:          "events_recorded_below_cap",
			preFill:       10,
			addMoreEvents: 5,
			wantLen:       15,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(context.Background(), "cap-sm", exprPassDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "cap-exec", "{}")
			require.NoError(t, err)

			execARN := exec.ExecutionArn

			// Wait for execution to reach terminal state so goroutine is done.
			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(execARN)

				return e == nil && d.Status != "RUNNING"
			}, 5*time.Second, 50*time.Millisecond)

			// Pre-fill history to the desired count using the exported test helper.
			b.FillHistoryForTest(execARN, tt.preFill)

			// Try to add more events via the exported recorder helper.
			for range tt.addMoreEvents {
				b.RecordStateEnteredForTest(execARN, "ExtraState", "Pass")
			}

			histLen := b.HistoryLenForTest(execARN)
			assert.Equal(t, tt.wantLen, histLen)
		})
	}
}

// TestDeleteStateMachine_TombstoneOnlyRunning verifies that completed executions
// do not accumulate tombstones when a state machine is deleted.
func TestDeleteStateMachine_TombstoneOnlyRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		waitForCompletion bool
		wantTombstone     bool
	}{
		{
			name:              "completed_execution_no_tombstone",
			waitForCompletion: true,
			wantTombstone:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(context.Background(), "tomb-sm", exprPassDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "tomb-exec", "{}")
			require.NoError(t, err)

			execARN := exec.ExecutionArn

			if tt.waitForCompletion {
				require.Eventually(t, func() bool {
					d, e := b.DescribeExecution(execARN)

					return e == nil && d.Status != "RUNNING"
				}, 5*time.Second, 50*time.Millisecond)
			}

			err = b.DeleteStateMachine(sm.StateMachineArn)
			require.NoError(t, err)

			hasTombstone := b.HasTombstoneForTest(execARN)
			assert.Equal(t, tt.wantTombstone, hasTombstone)
		})
	}
}

// TestUpdateStateMachine verifies state machine definition and roleArn updates.
func TestUpdateStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs         error
		name          string
		newDefinition string
		newRoleArn    string
		checkDef      string
		checkRole     string
		wantErr       bool
	}{
		{
			name: "update_definition",
			newDefinition: `{
"StartAt": "S",
"States": {"S": {"Type": "Succeed"}}}`,
			newRoleArn: "",
			checkDef:   "Succeed",
			checkRole:  "arn:aws:iam::123456789012:role/original",
		},
		{
			name:          "update_role_only",
			newDefinition: "",
			newRoleArn:    "arn:aws:iam::123456789012:role/new-role",
			checkRole:     "arn:aws:iam::123456789012:role/new-role",
		},
		{
			name:          "invalid_definition",
			newDefinition: `{}`,
			wantErr:       true,
			errIs:         stepfunctions.ErrInvalidDefinition,
		},
		{
			name:       "invalid_role_arn",
			newRoleArn: "invalid-role",
			wantErr:    true,
			errIs:      stepfunctions.ErrInvalidRoleArn,
		},
		{
			name:    "nonexistent_sm",
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
					"update-sm", exprPassDef,
					"arn:aws:iam::123456789012:role/original", "STANDARD",
				)
				require.NoError(t, smErr)
				smARN = sm.StateMachineArn
			} else {
				smARN = "arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent"
			}

			updateDate, err := b.UpdateStateMachine(smARN, tt.newDefinition, tt.newRoleArn)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Greater(t, updateDate, float64(0))

			sm, err := b.DescribeStateMachine(smARN)
			require.NoError(t, err)
			assert.Greater(t, sm.UpdatedDate, float64(0))

			if tt.checkDef != "" {
				assert.Contains(t, sm.Definition, tt.checkDef)
			}
			if tt.checkRole != "" {
				assert.Equal(t, tt.checkRole, sm.RoleArn)
			}
		})
	}
}

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
			errIs:      stepfunctions.ErrInvalidExecutionType,
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
				sm, smErr := b.CreateStateMachine(context.Background(), "sync-sm-"+tt.name, tt.definition, "arn:role", tt.smType)
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

// TestActivity_CreateDescribeDelete tests the activity lifecycle.
func TestActivity_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		actName   string
		duplicate bool
	}{
		{
			name:    "create_and_describe",
			actName: "my-activity",
		},
		{
			name:      "duplicate_create",
			actName:   "dup-activity",
			duplicate: true,
			wantErr:   stepfunctions.ErrActivityAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()

			a, err := b.CreateActivity(context.Background(), tt.actName)
			require.NoError(t, err)
			assert.Equal(t, tt.actName, a.Name)
			assert.Contains(t, a.ActivityArn, ":activity:"+tt.actName)
			assert.Greater(t, a.CreationDate, float64(0))

			if tt.duplicate {
				_, err = b.CreateActivity(context.Background(), tt.actName)
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			described, err := b.DescribeActivity(a.ActivityArn)
			require.NoError(t, err)
			assert.Equal(t, tt.actName, described.Name)

			err = b.DeleteActivity(a.ActivityArn)
			require.NoError(t, err)

			_, err = b.DescribeActivity(a.ActivityArn)
			require.ErrorIs(t, err, stepfunctions.ErrActivityDoesNotExist)
		})
	}
}

// TestActivity_GetActivityTaskAndSendSuccess tests the activity task polling and success flow.
func TestActivity_GetActivityTaskAndSendSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		output     string
		wantOutput string
	}{
		{
			name:       "success_with_output",
			input:      `{"key":"val"}`,
			output:     `{"result":"ok"}`,
			wantOutput: `{"result":"ok"}`,
		},
		{
			name:       "empty_output",
			input:      `{}`,
			output:     `{}`,
			wantOutput: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "task-act-" + tt.name)
			require.NoError(t, err)

			resultCh := make(chan string, 1)
			errCh := make(chan error, 1)

			go func() {
				out, invokeErr := b.InvokeActivity(t.Context(), a.ActivityArn, tt.input, 0)
				if invokeErr != nil {
					errCh <- invokeErr

					return
				}
				resultCh <- out
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			task, err := b.GetActivityTask(ctx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken, "expected a task to be available")
			assert.Equal(t, tt.input, task.Input)

			err = b.SendTaskSuccess(task.TaskToken, tt.output)
			require.NoError(t, err)

			select {
			case out := <-resultCh:
				assert.Equal(t, tt.wantOutput, out)
			case invokeErr := <-errCh:
				require.NoError(t, invokeErr)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for InvokeActivity result")
			}
		})
	}
}

// TestActivity_GetActivityTaskAndSendFailure tests the activity task failure flow.
func TestActivity_GetActivityTaskAndSendFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		errCode     string
		cause       string
		wantErrCode string
	}{
		{
			name:        "task_failed",
			errCode:     "ActivityFailed",
			cause:       "worker error",
			wantErrCode: "ActivityFailed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "fail-act-" + tt.name)
			require.NoError(t, err)

			invokeErrCh := make(chan error, 1)

			go func() {
				_, invokeErr := b.InvokeActivity(t.Context(), a.ActivityArn, `{}`, 0)
				invokeErrCh <- invokeErr
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			task, err := b.GetActivityTask(ctx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken, "expected a task to be available")

			err = b.SendTaskFailure(task.TaskToken, tt.errCode, tt.cause)
			require.NoError(t, err)

			select {
			case invokeErr := <-invokeErrCh:
				require.Error(t, invokeErr)
				assert.Contains(t, invokeErr.Error(), tt.wantErrCode)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for InvokeActivity failure")
			}
		})
	}
}

// TestActivity_GetActivityTask_Timeout verifies that GetActivityTask returns an empty task
// when no task is available within the poll timeout.
func TestActivity_GetActivityTask_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			name:    "returns_empty_on_timeout",
			timeout: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "timeout-act-" + tt.name)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), tt.timeout)
			defer cancel()

			task, err := b.GetActivityTask(ctx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			// AWS returns empty response (no task token) when poll times out.
			assert.Empty(t, task.TaskToken)
		})
	}
}

func TestActivity_InvokeCancellationRemovesTaskToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sendResult func(*stepfunctions.InMemoryBackend, string) error
		name       string
	}{
		{
			name: "cancelled_invoke_rejects_send_task_success",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskSuccess(taskToken, `{"status":"late"}`)
			},
		},
		{
			name: "cancelled_invoke_rejects_send_task_failure",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskFailure(taskToken, "ActivityFailed", "late failure")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "cancel-act-" + tt.name)
			require.NoError(t, err)

			invokeCtx, cancelInvoke := context.WithCancel(t.Context())
			defer cancelInvoke()

			invokeErrCh := make(chan error, 1)
			go func() {
				_, invokeErr := b.InvokeActivity(invokeCtx, a.ActivityArn, `{}`, 0)
				invokeErrCh <- invokeErr
			}()

			pollCtx, cancelPoll := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelPoll()

			task, err := b.GetActivityTask(pollCtx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken)

			cancelInvoke()

			require.Eventually(t, func() bool {
				select {
				case invokeErr := <-invokeErrCh:
					return errors.Is(invokeErr, context.Canceled)
				default:
					return false
				}
			}, 2*time.Second, 25*time.Millisecond)

			err = tt.sendResult(b, task.TaskToken)
			require.ErrorIs(t, err, stepfunctions.ErrTaskTokenNotFound)
		})
	}
}

func TestActivity_DeleteActivityRemovesOutstandingTaskTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sendResult func(*stepfunctions.InMemoryBackend, string) error
		name       string
	}{
		{
			name: "delete_activity_rejects_send_task_failure",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskFailure(taskToken, "ActivityFailed", "worker failed")
			},
		},
		{
			name: "delete_activity_rejects_send_task_success",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskSuccess(taskToken, `{"ok":true}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "delete-act-" + tt.name)
			require.NoError(t, err)

			invokeCtx, cancelInvoke := context.WithCancel(t.Context())
			defer cancelInvoke()

			invokeErrCh := make(chan error, 1)
			go func() {
				_, invokeErr := b.InvokeActivity(invokeCtx, a.ActivityArn, `{}`, 0)
				invokeErrCh <- invokeErr
			}()

			pollCtx, cancelPoll := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelPoll()

			task, err := b.GetActivityTask(pollCtx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken)

			err = b.DeleteActivity(a.ActivityArn)
			require.NoError(t, err)

			err = tt.sendResult(b, task.TaskToken)
			require.ErrorIs(t, err, stepfunctions.ErrTaskTokenNotFound)

			cancelInvoke()
			require.Eventually(t, func() bool {
				select {
				case invokeErr := <-invokeErrCh:
					return errors.Is(invokeErr, context.Canceled)
				default:
					return false
				}
			}, 2*time.Second, 25*time.Millisecond)
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
