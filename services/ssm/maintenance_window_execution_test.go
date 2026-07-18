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
		`{"WindowExecutionId":"`+execID+`"}`,
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
			assert.False(t, out.StartTime.IsZero(), "StartTime must be populated")
			assert.NotNil(t, out.EndTime, "EndTime must be populated")

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
			assert.False(t, taskOut.StartTime.IsZero())

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
			assert.False(t, invOut.StartTime.IsZero())
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
			name:       "empty_execution_id",
			body:       `{"WindowExecutionId":""}`,
			wantStatus: http.StatusOK,
			wantExecID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CancelMaintenanceWindowExecution", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

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
			name:      "empty_window_id_returns_empty",
			wantCount: 0,
		},
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
			wantCount: 0,
		},
		{
			name:      "valid_exec_id_returns_one_invocation",
			execID:    "mwexec-mw-test123",
			taskID:    "task-exec-001",
			wantCount: 1,
		},
		{
			name:      "valid_exec_id_no_task_returns_invocation",
			execID:    "mwexec-mw-abc",
			taskID:    "",
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
