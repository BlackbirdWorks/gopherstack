package stepfunctions_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

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

func TestRedriveExecution_RedriveCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		redrives         int
		wantRedriveCount int
	}{
		{
			name:             "single_redrive",
			redrives:         1,
			wantRedriveCount: 1,
		},
		{
			name:             "double_redrive",
			redrives:         2,
			wantRedriveCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			failDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Err","Cause":"test"}}}`
			sm, err := b.CreateStateMachine(
				context.Background(),
				"redrive-sm-"+tt.name,
				failDef,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "redrive-exec-"+tt.name, `{}`)
			require.NoError(t, err)

			// Wait for failure.
			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status == "FAILED"
			}, 5*time.Second, 50*time.Millisecond)

			// Perform redrives.
			for range tt.redrives {
				require.Eventually(t, func() bool {
					d, e := b.DescribeExecution(exec.ExecutionArn)

					return e == nil && d.Status == "FAILED"
				}, 5*time.Second, 50*time.Millisecond)

				_, redriveErr := b.RedriveExecution(exec.ExecutionArn)
				require.NoError(t, redriveErr)
			}

			// Wait for final completion.
			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status != "RUNNING"
			}, 5*time.Second, 50*time.Millisecond)

			described, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRedriveCount, described.RedriveCount)
			assert.NotNil(t, described.RedriveDate)
		})
	}
}

// ─── Backend Name Validation ─────────────────────────────────────────────────

func TestBackend_ValidateName_Execution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		execName string
	}{
		{
			name:     "valid_name",
			execName: "good-exec",
			wantErr:  nil,
		},
		{
			name:     "empty_name_allowed",
			execName: "",
			wantErr:  nil,
		},
		{
			name:     "name_too_long",
			execName: strings.Repeat("e", 81),
			wantErr:  stepfunctions.ErrInvalidName,
		},
		{
			name:     "name_with_invalid_chars",
			execName: "bad<exec>",
			wantErr:  stepfunctions.ErrInvalidName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			smName := "name-val-sm-" + tt.name[:min(len(tt.name), 20)]
			sm, err := b.CreateStateMachine(context.Background(), smName, sfnPassDefinition, "arn:role", "STANDARD")
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, tt.execName, `{}`)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestARN_Execution(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	sm, err := b.CreateStateMachine(
		context.Background(),
		"my-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "my-exec", "{}")
	require.NoError(t, err)
	assert.Equal(
		t,
		"arn:aws:states:us-east-1:123456789012:execution:my-sm:my-exec",
		exec.ExecutionArn,
	)
}

func TestExecutionName_TooLong(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"exec-name-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	longName := strings.Repeat("x", 81)
	_, err = b.StartExecution(sm.StateMachineArn, longName, "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
}

// ─── Execution Lifecycle ──────────────────────────────────────────────────────

func TestExecution_StartAndDescribe(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"desc-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "desc-exec", `{"x":1}`)
	require.NoError(t, err)
	assert.NotEmpty(t, exec.ExecutionArn)
	assert.Equal(t, sm.StateMachineArn, exec.StateMachineArn)
	assert.Equal(t, "desc-exec", exec.Name)
	assert.Equal(t, `{"x":1}`, exec.Input)
}

func TestExecution_StartsRunning(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Use a Wait state to keep execution running long enough to observe RUNNING status.
	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	sm, err := b.CreateStateMachine(
		context.Background(),
		"run-sm",
		waitDef,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "run-exec", "{}")
	require.NoError(t, err)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", desc.Status)
}

func TestExecution_DuplicateNameReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"dup-exec-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	_, err = b.StartExecution(sm.StateMachineArn, "dup-exec", "{}")
	require.NoError(t, err)

	_, err = b.StartExecution(sm.StateMachineArn, "dup-exec", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionAlreadyExists)
}

func TestExecution_NotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.DescribeExecution("arn:aws:states:us-east-1:123:execution:sm:none")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}

func TestExecution_StateMachineNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.StartExecution("arn:aws:states:us-east-1:123:stateMachine:ghost", "e1", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

// ─── StartExecution / StartSyncExecution type enforcement ────────────────────

func TestStopExecution_SetsAborted(t *testing.T) {
	t.Parallel()

	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"stop-test-sm",
		waitDef,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "stop-e1", "{}")
	require.NoError(t, err)

	require.NoError(t, b.StopExecution(exec.ExecutionArn, "MyErr", "test cause"))

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "ABORTED", desc.Status)
	assert.Equal(t, "MyErr", desc.Error)
	assert.Equal(t, "test cause", desc.Cause)
	assert.NotNil(t, desc.StopDate)
}

func TestStopExecution_IdempotentOnTerminalState(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"idm-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "idm-exec", "{}")
	require.NoError(t, err)

	// Wait for execution to reach terminal state.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status != "RUNNING"
	}, 5*time.Second, 20*time.Millisecond)

	// Must not error and must not overwrite terminal status.
	err = b.StopExecution(exec.ExecutionArn, "ShouldNotOverwrite", "nope")
	require.NoError(t, err)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", desc.Status, "terminal status must not be overwritten")
	assert.NotEqual(t, "ShouldNotOverwrite", desc.Error)
}

func TestStopExecution_NotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.StopExecution("arn:aws:states:us-east-1:123:execution:sm:ghost", "", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}

// ─── GetExecutionHistory ──────────────────────────────────────────────────────

func TestListExecutions_StatusFilter_RUNNING(t *testing.T) {
	t.Parallel()

	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"list-sm",
		waitDef,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "list-run-e", "{}")
	require.NoError(t, err)

	// Give the execution time to start.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "RUNNING"
	}, 5*time.Second, 20*time.Millisecond)

	running, _, err := b.ListExecutions(sm.StateMachineArn, "RUNNING", "", 100)
	require.NoError(t, err)
	assert.Len(t, running, 1)
	assert.Equal(t, exec.ExecutionArn, running[0].ExecutionArn)

	succeeded, _, err := b.ListExecutions(sm.StateMachineArn, "SUCCEEDED", "", 100)
	require.NoError(t, err)
	assert.Empty(t, succeeded)
}

func TestListExecutions_Pagination(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"list-page-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	for i := range 4 {
		_, err = b.StartExecution(sm.StateMachineArn, fmt.Sprintf("pexec-%d", i), "{}")
		require.NoError(t, err)
	}

	page1, next, err := b.ListExecutions(sm.StateMachineArn, "", "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, next)
	assert.Len(t, page1, 2)

	page2, next2, err := b.ListExecutions(sm.StateMachineArn, "", next, 2)
	require.NoError(t, err)
	assert.Empty(t, next2)
	assert.Len(t, page2, 2)
}

func TestListExecutions_EmptyForNewSM(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"empty-list-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	execs, next, err := b.ListExecutions(sm.StateMachineArn, "", "", 100)
	require.NoError(t, err)
	assert.Empty(t, execs)
	assert.Empty(t, next)
}

// ─── DescribeStateMachineForExecution ─────────────────────────────────────────

func TestDescribeStateMachineForExecution(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"dsm-for-exec-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "dsm-exec", "{}")
	require.NoError(t, err)

	got, err := b.DescribeStateMachineForExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, sm.StateMachineArn, got.StateMachineArn)
	assert.Equal(t, "STANDARD", got.Type)
}

// ─── Activities ───────────────────────────────────────────────────────────────

func TestRedriveExecution_NotRedrivable(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"redrive-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "rd-exec", "{}")
	require.NoError(t, err)

	// SUCCEEDED executions cannot be redriven.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	_, err = b.RedriveExecution(exec.ExecutionArn)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionNotRedrivable)
}

// ─── roleArn validation ───────────────────────────────────────────────────────

func TestInput_ExactLimit_Succeeds(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"input-size-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	// Build payload just under 256 KiB — account for JSON overhead.
	payload := `{"data":"` + strings.Repeat("x", 256*1024-12) + `"}`
	_, err = b.StartExecution(sm.StateMachineArn, "size-exec", payload)
	require.NoError(t, err)
}

func TestInput_OverLimit_Fails(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"input-over-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	oversize := `{"data":"` + strings.Repeat("x", 256*1024+1) + `"}`
	_, err = b.StartExecution(sm.StateMachineArn, "over-exec", oversize)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidExecutionInput)
}

// ─── Handler HTTP response codes ──────────────────────────────────────────────

func TestListExecutions_OrderedByStartDateDesc(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	sm, err := b.CreateStateMachine(context.Background(), "order-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	names := []string{"exec-a", "exec-b", "exec-c"}
	startDates := make([]float64, len(names))

	for i, name := range names {
		exec, execErr := b.StartExecution(sm.StateMachineArn, name, "{}")
		require.NoError(t, execErr)

		startDates[i] = exec.StartDate

		// Small sleep to ensure distinct start timestamps.
		time.Sleep(5 * time.Millisecond)
	}

	execs, _, err := b.ListExecutions(sm.StateMachineArn, "", "", 10)
	require.NoError(t, err)
	require.Len(t, execs, 3)

	// Most recent first.
	for i := 1; i < len(execs); i++ {
		assert.GreaterOrEqual(t, execs[i-1].StartDate, execs[i].StartDate,
			"expected descending startDate order at index %d", i)
	}

	// First result should be the last started.
	assert.Equal(t, names[2], execs[0].Name)
}

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

// TestPerf_ListExecutionsStatusIndex verifies that the smExecsByStatus index
// correctly filters executions by status without a full scan.
func TestListExecutionsStatusIndex(t *testing.T) {
	t.Parallel()

	bk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	ctx := context.Background()

	sm, err := bk.CreateStateMachine(
		ctx, "perf-sm",
		`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
		"arn:aws:iam::123456789012:role/r", "STANDARD",
	)
	require.NoError(t, err)
	smARN := sm.StateMachineArn

	const numExecs = 5
	execARNs := make([]string, 0, numExecs)

	for i := range numExecs {
		exec, startErr := bk.StartExecution(smARN, fmt.Sprintf("exec-%d", i), `{}`)
		require.NoError(t, startErr)
		execARNs = append(execARNs, exec.ExecutionArn)
	}

	// Wait for all executions to finish.
	require.Eventually(t, func() bool {
		for _, arn := range execARNs {
			exec, descErr := bk.DescribeExecution(arn)
			if descErr != nil || exec.Status == "RUNNING" {
				return false
			}
		}

		return true
	}, 5*time.Second, 20*time.Millisecond)

	// Count actual statuses.
	succeededCount := 0
	runningCount := 0

	for _, arn := range execARNs {
		exec, _ := bk.DescribeExecution(arn)
		switch exec.Status {
		case "SUCCEEDED":
			succeededCount++
		case "RUNNING":
			runningCount++
		}
	}

	tests := []struct {
		name         string
		statusFilter string
		wantCount    int
	}{
		{
			name:         "filter RUNNING returns only running executions",
			statusFilter: "RUNNING",
			wantCount:    runningCount,
		},
		{
			name:         "filter SUCCEEDED returns only succeeded executions",
			statusFilter: "SUCCEEDED",
			wantCount:    succeededCount,
		},
		{
			name:         "no filter returns all executions",
			statusFilter: "",
			wantCount:    numExecs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			execs, _, listErr := bk.ListExecutions(smARN, tt.statusFilter, "", 0)
			require.NoError(t, listErr)
			assert.Len(t, execs, tt.wantCount)

			for _, exec := range execs {
				if tt.statusFilter != "" {
					assert.Equal(t, tt.statusFilter, exec.Status)
				}
			}
		})
	}
}

func TestBackend_ListExecutions_WithStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusFilter string
	}{
		{
			name:         "filter_running_returns_no_error",
			statusFilter: "RUNNING",
		},
		{
			name:         "filter_succeeded_returns_no_error",
			statusFilter: "SUCCEEDED",
		},
		{
			name:         "no_filter_returns_all",
			statusFilter: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"filter-sm",
				`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, "exec1", `{}`)
			require.NoError(t, err)

			// Verify ListExecutions returns no error for each status filter
			execs, _, err := b.ListExecutions(sm.StateMachineArn, tt.statusFilter, "", 0)
			require.NoError(t, err)

			if tt.statusFilter == "" {
				assert.GreaterOrEqual(t, len(execs), 1, "unfiltered list should contain at least the started execution")
			}
		})
	}
}

// ---- parseNextToken via ListExecutions with pagination ----

func TestBackend_ListExecutions_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nextToken  string
		maxResults int
		wantLen    int
	}{
		{
			name:       "page_1_max_1",
			nextToken:  "",
			maxResults: 1,
			wantLen:    1,
		},
		{
			name:       "page_2_via_token",
			nextToken:  "1", // start from index 1
			maxResults: 10,
			wantLen:    1,
		},
		{
			name:       "invalid_token_treated_as_zero",
			nextToken:  "not-a-number",
			maxResults: 10,
			wantLen:    2,
		},
		{
			name:       "negative_token_treated_as_zero",
			nextToken:  "-5",
			maxResults: 10,
			wantLen:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"page-sm",
				`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			// Create two executions so we have something to paginate
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-a", `{}`)
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-b", `{}`)

			// Wait for executions to complete to avoid race condition
			require.Eventually(t, func() bool {
				execs, _, listErr := b.ListExecutions(sm.StateMachineArn, "", "", 0)
				if listErr != nil {
					return false
				}

				doneCount := 0
				for _, e := range execs {
					if e.Status != "RUNNING" {
						doneCount++
					}
				}

				return doneCount == 2
			}, 5*time.Second, 50*time.Millisecond)

			execs, _, err := b.ListExecutions(sm.StateMachineArn, "", tt.nextToken, tt.maxResults)
			require.NoError(t, err)
			assert.Len(t, execs, tt.wantLen)
		})
	}
}

// ---- runParsedExecution: exec result with error in result object ----

// parallelWithCatchDef is a state machine where one Parallel branch always fails,
// and the Parallel state has a Catch clause that routes to a recovery Pass state.
const parallelWithCatchDef = `{
"StartAt": "ParallelStep",
"States": {
  "ParallelStep": {
    "Type": "Parallel",
    "Branches": [
      {
        "StartAt": "GoodBranch",
        "States": {
          "GoodBranch": {"Type": "Pass", "End": true}
        }
      },
      {
        "StartAt": "BadBranch",
        "States": {
          "BadBranch": {"Type": "Fail", "Error": "BranchError", "Cause": "branch failed"}
        }
      }
    ],
    "Catch": [
      {
        "ErrorEquals": ["States.ALL"],
        "Next": "Recovery",
        "ResultPath": "$.error"
      }
    ],
    "Next": "Done"
  },
  "Recovery": {"Type": "Pass", "End": true},
  "Done": {"Type": "Pass", "End": true}
}
}`

// parallelSuccessDef is a state machine with two successful parallel branches.
const parallelSuccessDef = `{
"StartAt": "ParallelStep",
"States": {
  "ParallelStep": {
    "Type": "Parallel",
    "Branches": [
      {
        "StartAt": "A",
        "States": {"A": {"Type": "Pass", "Result": {"val": 1}, "End": true}}
      },
      {
        "StartAt": "B",
        "States": {"B": {"Type": "Pass", "Result": {"val": 2}, "End": true}}
      }
    ],
    "End": true
  }
}
}`

// waitTimestampPastDef uses a Timestamp in the past so the wait completes immediately.
const waitTimestampPastDef = `{
"StartAt": "Wait",
"States": {
  "Wait": {
    "Type": "Wait",
    "Timestamp": "2020-01-01T00:00:00Z",
    "Next": "Done"
  },
  "Done": {"Type": "Pass", "End": true}
}
}`

// choiceAndOrDef tests And/Or combiners in Choice state.
const choiceAndOrDef = `{
"StartAt": "Choose",
"States": {
  "Choose": {
    "Type": "Choice",
    "Choices": [
      {
        "And": [
          {"Variable": "$.x", "NumericGreaterThan": 0},
          {"Variable": "$.y", "StringEquals": "foo"}
        ],
        "Next": "Both"
      },
      {
        "Or": [
          {"Variable": "$.a", "BooleanEquals": true},
          {"Variable": "$.b", "BooleanEquals": true}
        ],
        "Next": "Either"
      }
    ],
    "Default": "Neither"
  },
  "Both":    {"Type": "Pass", "Result": {"match": "both"},   "End": true},
  "Either":  {"Type": "Pass", "Result": {"match": "either"}, "End": true},
  "Neither": {"Type": "Pass", "Result": {"match": "none"},   "End": true}
}
}`
