package stepfunctions_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const passDefinition = `{
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}`

const waitDefinition = `{
"StartAt": "W",
"States": {
"W": {"Type": "Wait", "Seconds": 3600, "End": true}
}
}`

const failDefinition = `{
"StartAt": "F",
"States": {
"F": {"Type": "Fail", "Error": "MyErr", "Cause": "test cause"}
}
}`

func TestCreateStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr             error
		name                string
		smName              string
		definition          string
		preCreateDefinition string // if set, used for the preCreate call instead of definition
		roleArn             string
		smType              string
		wantName            string
		wantStatus          string
		wantType            string
		preCreate           bool
	}{
		{
			name:       "basic",
			smName:     "my-sm",
			definition: passDefinition,
			roleArn:    "arn:aws:iam::123456789012:role/role",
			smType:     "STANDARD",
			wantName:   "my-sm",
			wantStatus: "ACTIVE",
			wantType:   "STANDARD",
		},
		{
			name:       "DefaultType",
			smName:     "typed-sm",
			definition: passDefinition,
			roleArn:    "arn:role",
			smType:     "",
			wantType:   "STANDARD",
		},
		{
			name:                "AlreadyExists",
			smName:              "dup-sm",
			preCreateDefinition: passDefinition,
			definition:          `{"StartAt":"T","States":{"T":{"Type":"Succeed"}}}`,
			roleArn:             "arn:role",
			smType:              "STANDARD",
			preCreate:           true,
			wantErr:             stepfunctions.ErrStateMachineAlreadyExists,
		},
		{
			name:       "InvalidDefinition",
			smName:     "invalid-sm",
			definition: `{}`,
			roleArn:    "arn:role",
			smType:     "STANDARD",
			wantErr:    stepfunctions.ErrInvalidDefinition,
		},
		{
			name:       "InvalidRoleArn",
			smName:     "invalid-role",
			definition: passDefinition,
			roleArn:    "not-an-arn",
			smType:     "STANDARD",
			wantErr:    stepfunctions.ErrInvalidRoleArn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			if tt.preCreate {
				preCreateDef := tt.definition
				if tt.preCreateDefinition != "" {
					preCreateDef = tt.preCreateDefinition
				}
				_, err := b.CreateStateMachine(context.Background(), tt.smName, preCreateDef, tt.roleArn, tt.smType)
				require.NoError(t, err)
			}

			sm, err := b.CreateStateMachine(context.Background(), tt.smName, tt.definition, tt.roleArn, tt.smType)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, sm.Name)
				assert.Contains(t, sm.StateMachineArn, tt.wantName)
			}
			if tt.wantStatus != "" {
				assert.Equal(t, tt.wantStatus, sm.Status)
			}
			if tt.wantType != "" {
				assert.Equal(t, tt.wantType, sm.Type)
			}
		})
	}
}

func TestDescribeStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		createName string
		createDef  string
		createType string
		descArn    string
		wantName   string
		wantType   string
		wantDef    string
	}{
		{
			name:       "success",
			createName: "desc-sm",
			createDef:  passDefinition,
			createType: "EXPRESS",
			wantName:   "desc-sm",
			wantType:   "EXPRESS",
			wantDef:    passDefinition,
		},
		{
			name:    "NotFound",
			descArn: "arn:aws:states:us-east-1:123:stateMachine:nonexistent",
			wantErr: stepfunctions.ErrStateMachineDoesNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			arn := tt.descArn
			if tt.createName != "" {
				sm, err := b.CreateStateMachine(
					context.Background(),
					tt.createName,
					tt.createDef,
					"arn:role",
					tt.createType,
				)
				require.NoError(t, err)
				arn = sm.StateMachineArn
			}

			got, err := b.DescribeStateMachine(arn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, got.Name)
			assert.Equal(t, tt.wantType, got.Type)
			assert.JSONEq(t, tt.wantDef, got.Definition)
		})
	}
}

func TestListStateMachines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		setupNames []string
		maxResults int
		wantCount  int
		wantNext   bool
	}{
		{
			name:       "basic",
			setupNames: []string{"alpha-sm", "beta-sm"},
			wantCount:  2,
		},
		{
			// nextToken beyond size returns empty
			name:      "EmptyToken",
			token:     "999",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			for _, name := range tt.setupNames {
				_, _ = b.CreateStateMachine(context.Background(), name, passDefinition, "arn:role", "STANDARD")
			}

			sms, next, err := b.ListStateMachines(context.Background(), tt.token, tt.maxResults)
			require.NoError(t, err)
			assert.Len(t, sms, tt.wantCount)
			if tt.wantNext {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()
		b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

		for i := range 5 {
			_, _ = b.CreateStateMachine(
				context.Background(),
				"sm-"+string(rune('a'+i)), passDefinition, "arn:role", "STANDARD",
			)
		}

		page1, next, err := b.ListStateMachines(context.Background(), "", 2)
		require.NoError(t, err)
		assert.Len(t, page1, 2)
		assert.NotEmpty(t, next)

		page2, next2, err := b.ListStateMachines(context.Background(), next, 2)
		require.NoError(t, err)
		assert.Len(t, page2, 2)
		assert.NotEmpty(t, next2)

		page3, next3, err := b.ListStateMachines(context.Background(), next2, 2)
		require.NoError(t, err)
		assert.Len(t, page3, 1)
		assert.Empty(t, next3)
	})
}

func TestDeleteStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		deleteArn string
		createSM  bool
	}{
		{
			name:     "success",
			createSM: true,
		},
		{
			name:      "NotFound",
			deleteArn: "arn:aws:states:us-east-1:123:stateMachine:nonexistent",
			wantErr:   stepfunctions.ErrStateMachineDoesNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			arn := tt.deleteArn
			if tt.createSM {
				sm, err := b.CreateStateMachine(
					context.Background(),
					"to-delete",
					passDefinition,
					"arn:role",
					"STANDARD",
				)
				require.NoError(t, err)
				arn = sm.StateMachineArn
			}

			err := b.DeleteStateMachine(arn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			_, err = b.DescribeStateMachine(arn)
			require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
		})
	}
}

func TestStartExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr         error
		name            string
		smArn           string
		smType          string
		execName        string
		input           string
		wantArnContains string
		createSM        bool
		preCreateExec   bool
	}{
		{
			name:            "basic",
			createSM:        true,
			smType:          "STANDARD",
			execName:        "exec1",
			input:           `{"key":"value"}`,
			wantArnContains: "exec1",
		},
		{
			name:     "SMNotFound",
			smArn:    "arn:nonexistent",
			execName: "exec1",
			wantErr:  stepfunctions.ErrStateMachineDoesNotExist,
		},
		{
			name:          "AlreadyExists",
			createSM:      true,
			smType:        "STANDARD",
			execName:      "exec1",
			preCreateExec: true,
			wantErr:       stepfunctions.ErrExecutionAlreadyExists,
		},
		{
			// AWS allows asynchronous StartExecution on EXPRESS state
			// machines too (only StartSyncExecution is EXPRESS-only).
			name:            "ExpressAllowsAsyncStartExecution",
			createSM:        true,
			smType:          "EXPRESS",
			execName:        "exec1",
			input:           `{"key":"value"}`,
			wantArnContains: "exec1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			smArn := tt.smArn
			if tt.createSM {
				smType := tt.smType
				if smType == "" {
					smType = "STANDARD"
				}
				sm, err := b.CreateStateMachine(context.Background(), "exec-sm", passDefinition, "arn:role", smType)
				require.NoError(t, err)
				smArn = sm.StateMachineArn
			}

			if tt.preCreateExec {
				_, err := b.StartExecution(smArn, tt.execName, "")
				require.NoError(t, err)
			}

			exec, err := b.StartExecution(smArn, tt.execName, tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			if tt.wantArnContains != "" {
				assert.Contains(t, exec.ExecutionArn, tt.wantArnContains)
			}
		})
	}
}

func TestDescribeExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		input        string
		executionArn string
		wantStatus   string
		wantInput    string
		createExec   bool
	}{
		{
			name:       "success",
			createExec: true,
			input:      `{"x":1}`,
			wantStatus: "SUCCEEDED",
			wantInput:  `{"x":1}`,
		},
		{
			name:         "NotFound",
			executionArn: "arn:nonexistent",
			wantErr:      stepfunctions.ErrExecutionDoesNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			arn := tt.executionArn
			if tt.createExec {
				sm, err := b.CreateStateMachine(
					context.Background(),
					"desc-exec-sm",
					passDefinition,
					"arn:role",
					"STANDARD",
				)
				require.NoError(t, err)
				exec, err := b.StartExecution(sm.StateMachineArn, "exec1", tt.input)
				require.NoError(t, err)
				arn = exec.ExecutionArn
				// Wait for the async executor to finish.
				require.Eventually(t, func() bool {
					desc, descErr := b.DescribeExecution(arn)

					return descErr == nil && desc.Status != "RUNNING"
				}, 5*time.Second, 50*time.Millisecond)
			}

			got, err := b.DescribeExecution(arn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantInput, got.Input)
		})
	}
}

func TestListExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusFilter string
		execNames    []string
		wantCount    int
	}{
		{
			name:      "basic",
			execNames: []string{"exec-a", "exec-b"},
			wantCount: 2,
		},
		{
			// Filter by RUNNING - should return 0 (auto-SUCCEEDED)
			name:         "StatusFilter/RUNNING",
			execNames:    []string{"exec-a"},
			statusFilter: "RUNNING",
			wantCount:    0,
		},
		{
			// Filter by SUCCEEDED - should return 1
			name:         "StatusFilter/SUCCEEDED",
			execNames:    []string{"exec-a"},
			statusFilter: "SUCCEEDED",
			wantCount:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			sm, err := b.CreateStateMachine(
				context.Background(),
				"list-exec-sm",
				passDefinition,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)
			for _, name := range tt.execNames {
				_, err = b.StartExecution(sm.StateMachineArn, name, "")
				require.NoError(t, err)
			}

			// Wait for async executions to complete before checking status filters.
			require.Eventually(t, func() bool {
				execs, _, listErr := b.ListExecutions(sm.StateMachineArn, "", "", 0)
				if listErr != nil {
					return false
				}
				for _, ex := range execs {
					if ex.Status == "RUNNING" {
						return false
					}
				}

				return true
			}, 5*time.Second, 50*time.Millisecond)

			execs, next, err := b.ListExecutions(sm.StateMachineArn, tt.statusFilter, "", 0)
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, execs, tt.wantCount)
		})
	}
}

func TestGetExecutionHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		executionArn string
		wantFirst    string
		wantSecond   string
		wantLen      int
		createExec   bool
		reverse      bool
	}{
		{
			name:       "forward",
			createExec: true,
			wantLen:    4,
			wantFirst:  "ExecutionStarted",
			wantSecond: "PassStateEntered",
		},
		{
			name:       "ReverseOrder",
			createExec: true,
			reverse:    true,
			wantLen:    4,
			wantFirst:  "ExecutionSucceeded",
			wantSecond: "PassStateExited",
		},
		{
			name:         "NotFound",
			executionArn: "arn:nonexistent",
			wantErr:      stepfunctions.ErrExecutionDoesNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			arn := tt.executionArn
			if tt.createExec {
				sm, err := b.CreateStateMachine(context.Background(), "hist-sm", passDefinition, "arn:role", "STANDARD")
				require.NoError(t, err)
				exec, err := b.StartExecution(sm.StateMachineArn, "exec-h", "")
				require.NoError(t, err)
				arn = exec.ExecutionArn
				// Wait for async execution to complete.
				require.Eventually(t, func() bool {
					desc, descErr := b.DescribeExecution(arn)

					return descErr == nil && desc.Status != "RUNNING"
				}, 5*time.Second, 50*time.Millisecond)
			}

			events, next, err := b.GetExecutionHistory(arn, "", 0, tt.reverse)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, events, tt.wantLen)
			assert.Equal(t, tt.wantFirst, events[0].Type)
			assert.Equal(t, tt.wantSecond, events[1].Type)
		})
	}
}

const taskLambdaDefinition = `{
"StartAt": "T",
"States": {
"T": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn", "End": true}
}
}`

// TestGetExecutionHistory_TaskEventDetails verifies that Task lifecycle
// history events (TaskScheduled/TaskSucceeded/TaskFailed) and state
// entered/exited events carry their AWS-documented detail payloads
// (resource, input, output, error, cause) rather than an empty details
// object.
func TestGetExecutionHistory_TaskEventDetails(t *testing.T) {
	t.Parallel()

	t.Run("succeeded_task_populates_resource_and_output", func(t *testing.T) {
		t.Parallel()

		b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
		b.SetLambdaInvoker(&mockLambdaForBackend{})

		sm, err := b.CreateStateMachine(
			context.Background(),
			"hist-task-sm",
			taskLambdaDefinition,
			"arn:role",
			"STANDARD",
		)
		require.NoError(t, err)

		exec, err := b.StartExecution(sm.StateMachineArn, "exec-task-ok", `{"in": 1}`)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			desc, descErr := b.DescribeExecution(exec.ExecutionArn)

			return descErr == nil && desc.Status != "RUNNING"
		}, 5*time.Second, 50*time.Millisecond)

		events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
		require.NoError(t, err)

		var sawScheduled, sawSucceeded, sawStateEntered bool

		for _, ev := range events {
			switch ev.Type {
			case "TaskScheduled":
				require.NotNil(t, ev.TaskScheduledEventDetails)
				assert.Equal(
					t,
					"arn:aws:lambda:us-east-1:000000000000:function:fn",
					ev.TaskScheduledEventDetails.Resource,
				)
				sawScheduled = true
			case "TaskSucceeded":
				require.NotNil(t, ev.TaskSucceededEventDetails)
				assert.Contains(t, ev.TaskSucceededEventDetails.Output, "ok")
				sawSucceeded = true
			case "TaskStateEntered":
				require.NotNil(t, ev.StateEnteredEventDetails)
				assert.Contains(t, ev.StateEnteredEventDetails.Input, `"in":1`)
				sawStateEntered = true
			}
		}

		assert.True(t, sawScheduled, "expected a TaskScheduled event")
		assert.True(t, sawSucceeded, "expected a TaskSucceeded event")
		assert.True(t, sawStateEntered, "expected a TaskStateEntered event with populated input")
	})

	t.Run("failed_task_populates_error_and_cause", func(t *testing.T) {
		t.Parallel()

		b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
		b.SetLambdaInvoker(&mockLambdaForBackend{returnErr: assert.AnError})

		sm, err := b.CreateStateMachine(
			context.Background(),
			"hist-task-fail-sm",
			taskLambdaDefinition,
			"arn:role",
			"STANDARD",
		)
		require.NoError(t, err)

		exec, err := b.StartExecution(sm.StateMachineArn, "exec-task-fail", `{}`)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			desc, descErr := b.DescribeExecution(exec.ExecutionArn)

			return descErr == nil && desc.Status != "RUNNING"
		}, 5*time.Second, 50*time.Millisecond)

		events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
		require.NoError(t, err)

		var sawFailed bool

		for _, ev := range events {
			if ev.Type == "TaskFailed" {
				require.NotNil(t, ev.TaskFailedEventDetails)
				assert.NotEmpty(t, ev.TaskFailedEventDetails.Error)
				assert.NotEmpty(t, ev.TaskFailedEventDetails.Cause)
				sawFailed = true
			}
		}

		assert.True(t, sawFailed, "expected a TaskFailed event")
	})
}

func TestStopExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		executionArn string
		stopError    string
		stopCause    string
		wantStatus   string
		wantError    string
		wantCause    string
		createExec   bool
	}{
		{
			name:       "success",
			createExec: true,
			stopError:  "MyError",
			stopCause:  "stopped by test",
			wantStatus: "ABORTED",
			wantError:  "MyError",
			wantCause:  "stopped by test",
		},
		{
			name:         "NotFound",
			executionArn: "arn:nonexistent",
			wantErr:      stepfunctions.ErrExecutionDoesNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			arn := tt.executionArn
			if tt.createExec {
				sm, err := b.CreateStateMachine(context.Background(), "stop-sm", waitDefinition, "arn:role", "STANDARD")
				require.NoError(t, err)
				exec, err := b.StartExecution(sm.StateMachineArn, "exec-stop", "")
				require.NoError(t, err)
				arn = exec.ExecutionArn
				// Wait for execution to enter RUNNING before stopping it.
				require.Eventually(t, func() bool {
					desc, descErr := b.DescribeExecution(arn)

					return descErr == nil && desc.Status == "RUNNING"
				}, 5*time.Second, 10*time.Millisecond)
			}

			err := b.StopExecution(arn, tt.stopError, tt.stopCause)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			got, err := b.DescribeExecution(arn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantError, got.Error)
			assert.Equal(t, tt.wantCause, got.Cause)
			assert.NotNil(t, got.StopDate)
		})
	}
}

func TestSetLambdaInvoker(t *testing.T) {
	t.Parallel()
	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Setting nil is a no-op but shouldn't panic.
	b.SetLambdaInvoker(nil)
}

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
