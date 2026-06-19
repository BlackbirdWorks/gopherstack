package ssm_test

// mw_exec_state_test.go — table-driven tests for maintenance window execution
// tracking and association execution targets (go-rds0).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// DescribeAssociationExecutionTargets
// ---------------------------------------------------------------------------

func TestDescribeAssociationExecutionTargets(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test struct readability over field alignment
		input         ssm.DescribeAssociationExecutionTargetsInput
		targets       []ssm.AssociationTarget
		name          string
		assocInstance string
		wantCount     int
	}{
		{
			name:      "empty_assoc_id_returns_empty",
			input:     ssm.DescribeAssociationExecutionTargetsInput{AssociationID: ""},
			wantCount: 0,
		},
		{
			name:      "unknown_assoc_returns_empty",
			input:     ssm.DescribeAssociationExecutionTargetsInput{AssociationID: "assoc-does-not-exist"},
			wantCount: 0,
		},
		{
			name:          "instance_only_returns_one_target",
			assocInstance: "i-1234567890abcdef0",
			wantCount:     1,
		},
		{
			name:      "explicit_targets_returned",
			targets:   []ssm.AssociationTarget{{Key: "InstanceIds", Values: []string{"i-aaa", "i-bbb"}}},
			wantCount: 2,
		},
		{
			name:          "instance_plus_targets",
			assocInstance: "i-base",
			targets:       []ssm.AssociationTarget{{Key: "InstanceIds", Values: []string{"i-extra"}}},
			wantCount:     2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			var assocID string
			if tc.assocInstance != "" || len(tc.targets) > 0 {
				out, err := b.CreateAssociation(ctx, &ssm.CreateAssociationInput{
					Name:       "AWS-RunShellScript",
					InstanceID: tc.assocInstance,
					Targets:    tc.targets,
				})
				require.NoError(t, err)
				assocID = out.AssociationDescription.AssociationID
			}

			input := tc.input
			if assocID != "" {
				input.AssociationID = assocID
			}

			out, err := b.DescribeAssociationExecutionTargets(ctx, &input)
			require.NoError(t, err)
			assert.Len(t, out.AssociationExecutionTargets, tc.wantCount)

			// All returned targets must carry the association ID.
			for _, tgt := range out.AssociationExecutionTargets {
				assert.Equal(t, assocID, tgt.AssociationID)
				assert.NotEmpty(t, tgt.ExecutionID)
				assert.NotEmpty(t, tgt.Status)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DescribeMaintenanceWindowExecutions
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// DescribeMaintenanceWindowExecutionTasks
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// DescribeMaintenanceWindowExecutionTaskInvocations
// ---------------------------------------------------------------------------

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
