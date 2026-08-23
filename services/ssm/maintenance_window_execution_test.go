package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestMaintenanceWindowExecutions(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	win, err := b.CreateMaintenanceWindow(context.Background(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window",
		Schedule: "rate(7 days)",
		Duration: 2,
	})
	require.NoError(t, err)

	windowID := win.WindowID

	rec := doRequest(t, h, "DescribeMaintenanceWindowExecutions", `{"WindowId":"`+windowID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	execID := "mwexec-" + windowID

	rec = doRequest(
		t,
		h,
		"DescribeMaintenanceWindowExecutionTasks",
		`{"WindowExecutionId":"`+execID+`"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(
		t,
		h,
		"DescribeMaintenanceWindowExecutionTaskInvocations",
		`{"WindowExecutionId":"`+execID+`","TaskId":"task-x"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeMaintenanceWindowSchedule", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(
		t,
		h,
		"GetMaintenanceWindowExecution",
		`{"WindowId":"`+windowID+`","WindowExecutionId":"`+execID+`"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestGetMaintenanceWindowExecution_FullOutput verifies that the three MW
// execution Get operations return timing and status fields populated from
// the stored window and task data (not just a bare Status field).
func TestGetMaintenanceWindowExecution_FullOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration int32
	}{
		{name: "1-hour window", duration: 1},
		{name: "4-hour window", duration: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			win, err := b.CreateMaintenanceWindow(ctx, &ssm.CreateMaintenanceWindowInput{
				Name:     "test-win",
				Schedule: "rate(7 days)",
				Duration: tc.duration,
			})
			require.NoError(t, err)

			execID := "mwexec-" + win.WindowID

			out, err := b.GetMaintenanceWindowExecution(
				ctx,
				&ssm.GetMaintenanceWindowExecutionInput{
					WindowID:          win.WindowID,
					WindowExecutionID: execID,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, win.WindowID, out.WindowID)
			assert.Equal(t, execID, out.WindowExecutionID)
			assert.Equal(t, "Success", out.Status)
			assert.NotEmpty(t, out.StatusDetails)
			assert.NotZero(t, out.StartTime, "StartTime must be populated")
			assert.NotZero(t, out.EndTime, "EndTime must be populated")

			taskOut, err := b.GetMaintenanceWindowExecutionTask(
				ctx,
				&ssm.GetMaintenanceWindowExecutionTaskInput{
					WindowExecutionID: execID,
					TaskExecutionID:   "taskexec-some-task",
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "Success", taskOut.Status)
			assert.NotEmpty(t, taskOut.StatusDetails)
			assert.NotZero(t, taskOut.StartTime)

			invOut, err := b.GetMaintenanceWindowExecutionTaskInvocation(
				ctx,
				&ssm.GetMaintenanceWindowExecutionTaskInvocationInput{
					WindowExecutionID: execID,
					TaskExecutionID:   "taskexec-some-task",
					InvocationID:      "inv-001",
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "Success", invOut.Status)
			assert.NotZero(t, invOut.StartTime)
		})
	}
}

func TestCancelMaintenanceWindowExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantExecID string
		wantStatus int
	}{
		{
			name:       "returns_execution_id",
			body:       `{"WindowExecutionId":"wex-0123456789abcdef0"}`,
			wantStatus: http.StatusOK,
			wantExecID: "wex-0123456789abcdef0",
		},
		{
			// WindowExecutionId is required on the real op
			// (api_op_CancelMaintenanceWindowExecution.go) -- an empty body
			// previously echoed an empty ID back with 200 instead of
			// rejecting with ValidationException.
			name:       "empty_execution_id",
			body:       `{"WindowExecutionId":""}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CancelMaintenanceWindowExecution", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantExecID, resp["WindowExecutionId"])
		})
	}
}

func TestDescribeMaintenanceWindowExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		windowID  string // override when createWin is false
		wantCount int
		createWin bool
	}{
		{
			name:      "unknown_window_returns_empty",
			windowID:  "mw-does-not-exist",
			wantCount: 0,
		},
		{
			name:      "existing_window_returns_one_execution",
			createWin: true,
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			wid := tc.windowID
			if tc.createWin {
				wid = createTestWindow(t, b)
			}

			out, err := b.DescribeMaintenanceWindowExecutions(ctx,
				&ssm.DescribeMaintenanceWindowExecutionsInput{WindowID: wid})
			require.NoError(t, err)
			assert.Len(t, out.WindowExecutions, tc.wantCount)

			for _, exec := range out.WindowExecutions {
				assert.Equal(t, wid, exec.WindowID)
				assert.NotEmpty(t, exec.WindowExecutionID)
				assert.Equal(t, "Success", exec.Status)
			}
		})
	}
}

func TestDescribeMaintenanceWindowExecutionTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		useExecID func(wid string) string // nil = use correct mwexec- prefix
		name      string
		wantCount int
		taskCount int
	}{
		{
			name:      "unknown_exec_id_returns_empty",
			useExecID: func(_ string) string { return "not-mwexec-format" },
			wantCount: 0,
		},
		{
			name:      "no_tasks_registered_returns_empty",
			wantCount: 0,
		},
		{
			name:      "one_task_registered",
			taskCount: 1,
			wantCount: 1,
		},
		{
			name:      "two_tasks_registered",
			taskCount: 2,
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			wid := createTestWindow(t, b)

			// Register tasks.
			for range tc.taskCount {
				_, err := b.RegisterTaskWithMaintenanceWindow(ctx,
					&ssm.RegisterTaskWithMaintenanceWindowInput{
						WindowID: wid,
						TaskArn:  "AWS-RunShellScript",
						TaskType: "RUN_COMMAND",
					})
				require.NoError(t, err)
			}

			execID := "mwexec-" + wid
			if tc.useExecID != nil {
				execID = tc.useExecID(wid)
			}

			out, err := b.DescribeMaintenanceWindowExecutionTasks(ctx,
				&ssm.DescribeMaintenanceWindowExecutionTasksInput{
					WindowExecutionID: execID,
				})
			require.NoError(t, err)
			assert.Len(t, out.WindowExecutionTaskIdentities, tc.wantCount)

			for _, task := range out.WindowExecutionTaskIdentities {
				assert.Equal(t, execID, task.WindowExecutionID)
				assert.NotEmpty(t, task.TaskExecutionID)
				assert.NotEmpty(t, task.TaskARN)
				assert.Equal(t, "Success", task.Status)
			}
		})
	}
}

func TestDescribeMaintenanceWindowExecutionTaskInvocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		execID    string
		taskID    string
		wantCount int
	}{
		{
			name:      "unknown_exec_id_returns_empty",
			execID:    "not-mwexec-format",
			taskID:    "task-x",
			wantCount: 0,
		},
		{
			name:      "valid_exec_id_returns_one_invocation",
			execID:    "mwexec-mw-test123",
			taskID:    "task-exec-001",
			wantCount: 1,
		},
		{
			name:      "valid_exec_id_with_task_returns_invocation",
			execID:    "mwexec-mw-abc",
			taskID:    "task-exec-002",
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			out, err := b.DescribeMaintenanceWindowExecutionTaskInvocations(ctx,
				&ssm.DescribeMaintenanceWindowExecutionTaskInvocationsInput{
					WindowExecutionID: tc.execID,
					TaskID:            tc.taskID,
				})
			require.NoError(t, err)
			assert.Len(t, out.WindowExecutionTaskInvocationIdentities, tc.wantCount)

			for _, inv := range out.WindowExecutionTaskInvocationIdentities {
				assert.Equal(t, tc.execID, inv.WindowExecutionID)
				assert.Equal(t, tc.taskID, inv.TaskExecutionID)
				assert.NotEmpty(t, inv.InvocationID)
				assert.Equal(t, "Success", inv.Status)
			}
		})
	}
}

// TestMaintenanceWindowOps_RequireRequiredFields locks in that every
// maintenance-window Get/Describe op with a required field on the real SDK
// rejects an empty (or partially empty) body -- DescribeMaintenanceWindowExecutions
// (api_op_DescribeMaintenanceWindowExecutions.go, WindowId),
// DescribeMaintenanceWindowExecutionTasks (WindowExecutionId),
// DescribeMaintenanceWindowExecutionTaskInvocations (WindowExecutionId+TaskId),
// DescribeMaintenanceWindowTargets/-Tasks (WindowId),
// DescribeMaintenanceWindowsForTarget (ResourceType+Targets),
// GetMaintenanceWindowExecution (WindowExecutionId),
// GetMaintenanceWindowExecutionTask (WindowExecutionId+TaskExecutionId),
// GetMaintenanceWindowExecutionTaskInvocation
// (WindowExecutionId+TaskExecutionId+InvocationId), and GetMaintenanceWindowTask
// (WindowId+WindowTaskId). Every one of these previously fabricated a
// synthetic "Succeeded" record instead of rejecting the request.
func TestMaintenanceWindowOps_RequireRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		body string
	}{
		{name: "describe_executions", op: "DescribeMaintenanceWindowExecutions", body: `{}`},
		{name: "describe_execution_tasks", op: "DescribeMaintenanceWindowExecutionTasks", body: `{}`},
		{
			name: "describe_execution_task_invocations_missing_task_id",
			op:   "DescribeMaintenanceWindowExecutionTaskInvocations",
			body: `{"WindowExecutionId":"mwexec-x"}`,
		},
		{name: "describe_targets", op: "DescribeMaintenanceWindowTargets", body: `{}`},
		{name: "describe_tasks", op: "DescribeMaintenanceWindowTasks", body: `{}`},
		{name: "describe_windows_for_target", op: "DescribeMaintenanceWindowsForTarget", body: `{}`},
		{
			name: "describe_windows_for_target_missing_targets",
			op:   "DescribeMaintenanceWindowsForTarget",
			body: `{"ResourceType":"INSTANCE"}`,
		},
		{name: "get_execution", op: "GetMaintenanceWindowExecution", body: `{}`},
		{name: "get_execution_task", op: "GetMaintenanceWindowExecutionTask", body: `{}`},
		{
			name: "get_execution_task_missing_task_exec_id",
			op:   "GetMaintenanceWindowExecutionTask",
			body: `{"WindowExecutionId":"mwexec-x"}`,
		},
		{name: "get_execution_task_invocation", op: "GetMaintenanceWindowExecutionTaskInvocation", body: `{}`},
		{
			name: "get_execution_task_invocation_missing_invocation_id",
			op:   "GetMaintenanceWindowExecutionTaskInvocation",
			body: `{"WindowExecutionId":"mwexec-x","TaskExecutionId":"taskexec-y"}`,
		},
		{name: "get_task", op: "GetMaintenanceWindowTask", body: `{}`},
		{name: "get_task_missing_task_id", op: "GetMaintenanceWindowTask", body: `{"WindowId":"mw-x"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "op=%s body=%s", tt.op, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}
