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

const failDefinition = `{
"StartAt": "F",
"States": {
"F": {"Type": "Fail", "Error": "MyErr", "Cause": "test cause"}
}
}`

func TestCreateStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		smName     string
		definition string
		roleArn    string
		smType     string
		preCreate  bool
		wantName   string
		wantStatus string
		wantType   string
		wantErr    error
	}{
		{
			name:       "basic",
			smName:     "my-sm",
			definition: "{}",
			roleArn:    "arn:aws:iam::123456789012:role/role",
			smType:     "STANDARD",
			wantName:   "my-sm",
			wantStatus: "ACTIVE",
			wantType:   "STANDARD",
		},
		{
			name:       "DefaultType",
			smName:     "typed-sm",
			definition: "{}",
			roleArn:    "arn:role",
			smType:     "",
			wantType:   "STANDARD",
		},
		{
			name:       "AlreadyExists",
			smName:     "dup-sm",
			definition: "{}",
			roleArn:    "arn:role",
			smType:     "STANDARD",
			preCreate:  true,
			wantErr:    stepfunctions.ErrStateMachineAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			if tt.preCreate {
				_, err := b.CreateStateMachine(tt.smName, tt.definition, tt.roleArn, tt.smType)
				require.NoError(t, err)
			}

			sm, err := b.CreateStateMachine(tt.smName, tt.definition, tt.roleArn, tt.smType)
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
		name       string
		createName string
		createDef  string
		createType string
		descArn    string
		wantName   string
		wantType   string
		wantDef    string
		wantErr    error
	}{
		{
			name:       "success",
			createName: "desc-sm",
			createDef:  `{"Comment":"test"}`,
			createType: "EXPRESS",
			wantName:   "desc-sm",
			wantType:   "EXPRESS",
			wantDef:    `{"Comment":"test"}`,
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
				sm, err := b.CreateStateMachine(tt.createName, tt.createDef, "arn:role", tt.createType)
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
		setupNames []string
		token      string
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
				_, _ = b.CreateStateMachine(name, "{}", "arn:role", "STANDARD")
			}

			sms, next, err := b.ListStateMachines(tt.token, tt.maxResults)
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
	})
}

func TestDeleteStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createSM  bool
		deleteArn string
		wantErr   error
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
				sm, err := b.CreateStateMachine("to-delete", "{}", "arn:role", "STANDARD")
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
		name            string
		createSM        bool
		smArn           string
		execName        string
		input           string
		preCreateExec   bool
		wantArnContains string
		wantStatus      string
		wantOutput      string
		wantStopDate    bool
		wantErr         error
	}{
		{
			name:            "basic",
			createSM:        true,
			execName:        "exec1",
			input:           `{"key":"value"}`,
			wantArnContains: "exec1",
			wantStatus:      "SUCCEEDED",
			wantOutput:      `{"key":"value"}`,
			wantStopDate:    true,
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
			execName:      "exec1",
			preCreateExec: true,
			wantErr:       stepfunctions.ErrExecutionAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			smArn := tt.smArn
			if tt.createSM {
				sm, _ := b.CreateStateMachine("exec-sm", "{}", "arn:role", "STANDARD")
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
			if tt.wantStatus != "" {
				assert.Equal(t, tt.wantStatus, exec.Status)
			}
			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, exec.Output)
			}
			if tt.wantStopDate {
				assert.NotNil(t, exec.StopDate)
			}
		})
	}
}

func TestDescribeExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createExec   bool
		input        string
		executionArn string
		wantStatus   string
		wantInput    string
		wantErr      error
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
				sm, _ := b.CreateStateMachine("desc-exec-sm", "{}", "arn:role", "STANDARD")
				exec, _ := b.StartExecution(sm.StateMachineArn, "exec1", tt.input)
				arn = exec.ExecutionArn
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
		execNames    []string
		statusFilter string
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

			sm, _ := b.CreateStateMachine("list-exec-sm", "{}", "arn:role", "STANDARD")
			for _, name := range tt.execNames {
				_, _ = b.StartExecution(sm.StateMachineArn, name, "")
			}

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
		name         string
		createExec   bool
		executionArn string
		reverse      bool
		wantLen      int
		wantFirst    string
		wantSecond   string
		wantErr      error
	}{
		{
			name:       "forward",
			createExec: true,
			wantLen:    2,
			wantFirst:  "ExecutionStarted",
			wantSecond: "ExecutionSucceeded",
		},
		{
			name:       "ReverseOrder",
			createExec: true,
			reverse:    true,
			wantLen:    2,
			wantFirst:  "ExecutionSucceeded",
			wantSecond: "ExecutionStarted",
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
				sm, _ := b.CreateStateMachine("hist-sm", "{}", "arn:role", "STANDARD")
				exec, _ := b.StartExecution(sm.StateMachineArn, "exec-h", "")
				arn = exec.ExecutionArn
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

func TestStopExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createExec   bool
		executionArn string
		stopError    string
		stopCause    string
		wantStatus   string
		wantError    string
		wantCause    string
		wantErr      error
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
				sm, _ := b.CreateStateMachine("stop-sm", "{}", "arn:role", "STANDARD")
				exec, _ := b.StartExecution(sm.StateMachineArn, "exec-stop", "")
				arn = exec.ExecutionArn
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

func TestSetLogger(t *testing.T) {
	t.Parallel()
	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// SetLogger with a real logger should not panic.
	b.SetLogger(slog.Default())
}

func TestStartExecutionASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		definition      string
		input           string
		checkInitStatus bool
		wantStatus      string
		wantOutputKey   string
		wantError       string
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

			sm, err := b.CreateStateMachine("asl-"+tt.name, tt.definition, "arn:role", "STANDARD")
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
