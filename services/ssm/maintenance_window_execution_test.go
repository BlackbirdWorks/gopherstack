package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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
func TestStubOps_SimpleCalls(t *testing.T) {
	t.Parallel()

	// These operations accept empty bodies and return stub responses.
	ops := []string{
		"CreateResourceDataSync",
		"DeleteInventory",
		"DeleteResourcePolicy",
		"DeregisterManagedInstance",
		"DescribeActivations",
		"DescribeAssociationExecutionTargets",
		"DescribeAssociationExecutions",
		"DescribeAutomationExecutions",
		"DescribeAutomationStepExecutions",
		"DescribeAvailablePatches",
		"DescribeEffectiveInstanceAssociations",
		"DescribeEffectivePatchesForPatchBaseline",
		"DescribeInstanceAssociationsStatus",
		"DescribeInstanceInformation",
		"DescribeInstancePatchStates",
		"DescribeInstancePatchStatesForPatchGroup",
		"DescribeInstancePatches",
		"DescribeInstanceProperties",
		"DescribeInventoryDeletions",
		"DescribeMaintenanceWindowExecutionTaskInvocations",
		"DescribeMaintenanceWindowExecutionTasks",
		"DescribeMaintenanceWindowExecutions",
		"DescribeMaintenanceWindowSchedule",
		"DescribeMaintenanceWindowTargets",
		"DescribeMaintenanceWindowTasks",
		"DescribeMaintenanceWindowsForTarget",
		"DescribePatchGroupState",
		"DescribePatchGroups",
		"DescribePatchProperties",
		"DescribeSessions",
		"DisassociateOpsItemRelatedItem",
		"GetAccessToken",
		"GetCalendarState",
		"GetConnectionStatus",
		"GetDeployablePatchSnapshotForInstance",
		"GetExecutionPreview",
		"GetInventory",
		"GetInventorySchema",
		"GetMaintenanceWindowExecution",
		"GetMaintenanceWindowExecutionTask",
		"GetMaintenanceWindowExecutionTaskInvocation",
		"GetMaintenanceWindowTask",
		"GetOpsSummary",
		"GetPatchBaselineForPatchGroup",
		"GetResourcePolicies",
		"GetServiceSetting",
		"LabelParameterVersion",
		"ListAssociationVersions",
		"ListAssociations",
		"ListComplianceItems",
		"ListComplianceSummaries",
		"ListDocumentMetadataHistory",
		"ListInventoryEntries",
		"ListNodes",
		"ListNodesSummary",
		"ListOpsItemEvents",
		"ListOpsItemRelatedItems",
		"ListOpsMetadata",
		"ListResourceComplianceSummaries",
		"ListResourceDataSync",
		"PutComplianceItems",
		"PutInventory",
		"PutResourcePolicy",
		"RegisterDefaultPatchBaseline",
		"ResetServiceSetting",
		"ResumeSession",
		"SendAutomationSignal",
		"StartAccessRequest",
		"StartAssociationsOnce",
		"StartAutomationExecution",
		"StartChangeRequestExecution",
		"StartExecutionPreview",
		"StartSession",
		"StopAutomationExecution",
		"TerminateSession",
		"UnlabelParameterVersion",
		"UpdateDocumentDefaultVersion",
		"UpdateDocumentMetadata",
		"UpdateManagedInstanceRole",
		"UpdateResourceDataSync",
		"UpdateServiceSetting",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, op, `{}`)
			assert.Equal(t, http.StatusOK, rec.Code, "op=%s body=%s", op, rec.Body.String())
		})
	}
}

// TestStubOps_DeleteMaintenanceWindow exercises the window-backed delete stub.
func TestStubOps_DeleteMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window",
		Schedule: "cron(0 9 ? * MON *)",
		Duration: 2,
		Cutoff:   1,
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"WindowId": mw.WindowID})
	rec := doRequest(t, h, "DeleteMaintenanceWindow", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_GetMaintenanceWindow exercises that stub.
func TestStubOps_GetMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window-2",
		Schedule: "cron(0 9 ? * MON *)",
		Duration: 2,
		Cutoff:   1,
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"WindowId": mw.WindowID})
	rec := doRequest(t, h, "GetMaintenanceWindow", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_UpdateMaintenanceWindow exercises that stub.
func TestStubOps_UpdateMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window-3",
		Schedule: "cron(0 9 ? * MON *)",
		Duration: 2,
		Cutoff:   1,
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"WindowId": mw.WindowID, "Name": "updated-window"})
	rec := doRequest(t, h, "UpdateMaintenanceWindow", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_RegisterTargetWithMaintenanceWindow requires valid window.
func TestStubOps_RegisterTargetWithMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window-4",
		Schedule: "cron(0 9 ? * MON *)",
		Duration: 2,
		Cutoff:   1,
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{
		"WindowId":     mw.WindowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-1234567890abcdef0"}},
		},
	})
	rec := doRequest(t, h, "RegisterTargetWithMaintenanceWindow", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_RegisterTaskWithMaintenanceWindow requires valid window.
func TestStubOps_RegisterTaskWithMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window-5",
		Schedule: "cron(0 9 ? * MON *)",
		Duration: 2,
		Cutoff:   1,
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{
		"WindowId": mw.WindowID,
		"TaskType": "RUN_COMMAND",
		"TaskArn":  "AWS-RunShellScript",
	})
	rec := doRequest(t, h, "RegisterTaskWithMaintenanceWindow", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
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
func TestCreateMaintenanceWindow_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_window",
			body: `{"Name":"MyWindow","Schedule":"cron(0 2 ? * SUN *)",` +
				`"Duration":4,"Cutoff":1,"AllowUnassociatedTargets":true}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "minimal_window",
			body:       `{"Name":"Min","Schedule":"rate(1 day)","Duration":2,"Cutoff":0}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateMaintenanceWindow", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateMaintenanceWindowOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.WindowID)
			assert.True(t, strings.HasPrefix(resp.WindowID, "mw-"))
			assert.Equal(t, 1, backend.MaintenanceWindowCount())
		})
	}
}
func TestCreateMaintenanceWindow_ValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"Schedule":"cron(0 2 ? * SUN *)","Duration":4,"Cutoff":1}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateMaintenanceWindow", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// TestRefinement2_CreateMaintenanceWindow_DurationCutoffValidation verifies duration/cutoff rules.
func TestCreateMaintenanceWindow_DurationCutoffValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "duration_zero",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":0,"Cutoff":0}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "duration_too_high",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":25,"Cutoff":1}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "cutoff_equals_duration",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":4,"Cutoff":4}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "cutoff_greater_than_duration",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":4,"Cutoff":5}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "valid_duration_and_cutoff",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":4,"Cutoff":1}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "max_duration",
			body:       `{"Name":"w","Schedule":"rate(1 day)","Duration":24,"Cutoff":1}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateMaintenanceWindow", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			}
		})
	}
}
func TestFull_MaintenanceWindow_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateMaintenanceWindow", map[string]any{
		"Name":              "TestMW",
		"Schedule":          "cron(0 2 ? * SUN *)",
		"Duration":          4,
		"Cutoff":            1,
		"AllowUnassociated": false,
	})
	assert.Equal(t, http.StatusOK, code)
	windowID := out["WindowId"].(string)
	assert.NotEmpty(t, windowID)

	// Get
	code, out = mustPost(t, h, "GetMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestMW", out["Name"])

	// Update
	code, _ = mustPost(t, h, "UpdateMaintenanceWindow", map[string]any{
		"WindowId": windowID,
		"Name":     "UpdatedMW",
	})
	assert.Equal(t, http.StatusOK, code)

	// Describe
	code, out = mustPost(t, h, "DescribeMaintenanceWindows", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	windows := out["WindowIdentities"].([]any)
	assert.NotEmpty(t, windows)

	// Register target
	code, out = mustPost(t, h, "RegisterTargetWithMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-001"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	targetID := out["WindowTargetId"].(string)
	assert.NotEmpty(t, targetID)

	// DescribeTargets
	code, out = mustPost(t, h, "DescribeMaintenanceWindowTargets", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	targets := out["Targets"].([]any)
	assert.Len(t, targets, 1)

	// Register task
	code, out = mustPost(t, h, "RegisterTaskWithMaintenanceWindow", map[string]any{
		"WindowId":       windowID,
		"TaskArn":        "AWS-RunShellScript",
		"TaskType":       "RUN_COMMAND",
		"MaxConcurrency": "1",
		"MaxErrors":      "0",
		"Targets": []map[string]any{
			{"Key": "WindowTargetIds", "Values": []string{targetID}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	taskID := out["WindowTaskId"].(string)
	assert.NotEmpty(t, taskID)

	// DescribeTasks
	code, out = mustPost(t, h, "DescribeMaintenanceWindowTasks", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	tasks := out["Tasks"].([]any)
	assert.Len(t, tasks, 1)

	// GetMaintenanceWindowTask
	code, out = mustPost(t, h, "GetMaintenanceWindowTask", map[string]any{
		"WindowId":     windowID,
		"WindowTaskId": taskID,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, taskID, out["WindowTaskId"])

	// UpdateTarget
	code, _ = mustPost(t, h, "UpdateMaintenanceWindowTarget", map[string]any{
		"WindowId":       windowID,
		"WindowTargetId": targetID,
		"Name":           "UpdatedTarget",
	})
	assert.Equal(t, http.StatusOK, code)

	// UpdateTask
	code, _ = mustPost(t, h, "UpdateMaintenanceWindowTask", map[string]any{
		"WindowId":       windowID,
		"WindowTaskId":   taskID,
		"MaxConcurrency": "2",
	})
	assert.Equal(t, http.StatusOK, code)

	// DeregisterTarget
	code, _ = mustPost(t, h, "DeregisterTargetFromMaintenanceWindow", map[string]any{
		"WindowId":       windowID,
		"WindowTargetId": targetID,
	})
	assert.Equal(t, http.StatusOK, code)

	// DeregisterTask
	code, _ = mustPost(t, h, "DeregisterTaskFromMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"WindowTaskId": taskID,
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeleteMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusBadRequest, code)
}
func TestFull_MaintenanceWindow_DescribeForTarget(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	_, out := mustPost(t, h, "CreateMaintenanceWindow", map[string]any{
		"Name":     "MW2",
		"Schedule": "cron(0 3 ? * MON *)",
		"Duration": 2,
		"Cutoff":   1,
	})
	windowID := out["WindowId"].(string)

	mustPost(t, h, "RegisterTargetWithMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "tag:Env", "Values": []string{"prod"}},
		},
	})

	code, out := mustPost(t, h, "DescribeMaintenanceWindowsForTarget", map[string]any{
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "tag:Env", "Values": []string{"prod"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["WindowIdentities"])
}

// TestCreateMaintenanceWindow_Validation covers MW duration/cutoff validation.
func TestCreateMaintenanceWindow_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    ssm.CreateMaintenanceWindowInput
		wantErr  bool
		wantCode int
	}{
		{
			name: "duration_zero",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 0,
				Cutoff:   0,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "duration_too_large",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 25,
				Cutoff:   1,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "cutoff_equals_duration",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 4,
				Cutoff:   4,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "name_empty",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "",
				Duration: 4,
				Cutoff:   1,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "valid_window",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-valid",
				Duration: 4,
				Cutoff:   1,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateMaintenanceWindow(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateMaintenanceWindow covers all field update branches.
func TestUpdateMaintenanceWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  ssm.UpdateMaintenanceWindowInput
		wantErr bool
	}{
		{
			name: "not_found",
			update: ssm.UpdateMaintenanceWindowInput{
				WindowID: "mw-does-not-exist",
				Name:     "newname",
			},
			wantErr: true,
		},
		{
			name: "update_all_fields",
			update: ssm.UpdateMaintenanceWindowInput{
				Name:        "new-name",
				Description: "new-desc",
				Schedule:    "cron(0 3 * * ? *)",
				Duration:    6,
				Cutoff:      1,
				Enabled: func() *bool {
					v := false

					return &v
				}(),
			},
			wantErr: false,
		},
		{
			name:    "update_no_changes",
			update:  ssm.UpdateMaintenanceWindowInput{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			if !tt.wantErr {
				out, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
					Name:     "my-window",
					Duration: 4,
					Cutoff:   1,
					Schedule: "cron(0 2 * * ? *)",
				})
				require.NoError(t, err)
				tt.update.WindowID = out.WindowID
			}

			_, err := b.UpdateMaintenanceWindow(context.TODO(), &tt.update)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrMaintenanceWindowNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCreateMaintenanceWindow_WithTags covers the tags path.
func TestCreateMaintenanceWindow_WithTags(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	out, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "tagged-window",
		Duration: 4,
		Cutoff:   1,
		Schedule: "cron(0 2 * * ? *)",
		Tags:     []ssm.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.WindowID)
}
func TestMaintenanceWindowsForTarget_FindsByTarget(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a maintenance window.
	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "prod-window",
		Schedule: "cron(0 2 ? * SUN *)",
		Duration: 4,
		Cutoff:   1,
	})
	require.NoError(t, err)

	// Register a target.
	body, _ := json.Marshal(map[string]any{
		"WindowId":     mw.WindowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-aabbcc112233"}},
		},
	})
	rec := doRequest(t, h, "RegisterTargetWithMaintenanceWindow", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeMaintenanceWindowsForTarget should find the window.
	body, _ = json.Marshal(map[string]any{
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-aabbcc112233"}},
		},
	})
	rec = doRequest(t, h, "DescribeMaintenanceWindowsForTarget", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), mw.WindowID)
	assert.Contains(t, rec.Body.String(), "prod-window")
}
func TestMaintenanceWindowTask_ServiceRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		serviceRoleArn string
		maxConcurrency string
		maxErrors      string
	}{
		{
			name:           "with_service_role_and_concurrency",
			serviceRoleArn: "arn:aws:iam::123456789012:role/MaintenanceWindowRole",
			maxConcurrency: "50%",
			maxErrors:      "10%",
		},
		{
			name:           "without_service_role",
			serviceRoleArn: "",
			maxConcurrency: "1",
			maxErrors:      "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
				Name:     "sra-window-" + tt.name,
				Schedule: "cron(0 2 ? * SUN *)",
				Duration: 3,
				Cutoff:   1,
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"WindowId":       mw.WindowID,
				"TaskType":       "RUN_COMMAND",
				"TaskArn":        "AWS-RunShellScript",
				"ServiceRoleArn": tt.serviceRoleArn,
				"MaxConcurrency": tt.maxConcurrency,
				"MaxErrors":      tt.maxErrors,
			})
			rec := doRequest(t, h, "RegisterTaskWithMaintenanceWindow", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var regResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
			taskID := regResp["WindowTaskId"].(string)

			// GetMaintenanceWindowTask should return ServiceRoleArn.
			body, _ = json.Marshal(map[string]any{
				"WindowId":     mw.WindowID,
				"WindowTaskId": taskID,
			})
			rec = doRequest(t, h, "GetMaintenanceWindowTask", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.serviceRoleArn != "" {
				assert.Contains(t, rec.Body.String(), tt.serviceRoleArn)
			}

			assert.Contains(t, rec.Body.String(), tt.maxConcurrency)
		})
	}
}
func TestMaintenanceWindowTask_UpdateServiceRoleArn(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "update-sra-window",
		Schedule: "cron(0 2 ? * SUN *)",
		Duration: 3,
		Cutoff:   1,
	})
	require.NoError(t, err)

	// Register task without role.
	body, _ := json.Marshal(map[string]any{
		"WindowId": mw.WindowID,
		"TaskType": "RUN_COMMAND",
		"TaskArn":  "AWS-RunShellScript",
	})
	rec := doRequest(t, h, "RegisterTaskWithMaintenanceWindow", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
	taskID := regResp["WindowTaskId"].(string)

	// Update with ServiceRoleArn.
	body, _ = json.Marshal(map[string]any{
		"WindowId":       mw.WindowID,
		"WindowTaskId":   taskID,
		"ServiceRoleArn": "arn:aws:iam::123456789012:role/UpdatedRole",
	})
	rec = doRequest(t, h, "UpdateMaintenanceWindowTask", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdatedRole")
}

// TestDeregisterTargetFromMaintenanceWindow_TableDriven verifies the response fields
// and error handling for DeregisterTargetFromMaintenanceWindow.
func TestDeregisterTargetFromMaintenanceWindow_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   int
		targetExists bool
		wantWindowID bool
		wantTargetID bool
	}{
		{
			name:         "registered_target_deregisters_with_ids",
			targetExists: true,
			wantStatus:   http.StatusOK,
			wantWindowID: true,
			wantTargetID: true,
		},
		{
			name:         "nonexistent_target_returns_error",
			targetExists: false,
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			mw, err := b.CreateMaintenanceWindow(
				context.Background(),
				&ssm.CreateMaintenanceWindowInput{
					Name:     "test-win",
					Schedule: "cron(0 9 ? * MON *)",
					Duration: 2,
					Cutoff:   1,
				},
			)
			require.NoError(t, err)

			var windowTargetID string
			if tt.targetExists {
				reg, regErr := b.RegisterTargetWithMaintenanceWindow(
					context.Background(),
					&ssm.RegisterTargetWithMaintenanceWindowInput{
						WindowID:     mw.WindowID,
						ResourceType: "INSTANCE",
						Targets: []ssm.WindowTarget{
							{Key: "InstanceIds", Values: []string{"i-1234"}},
						},
					},
				)
				require.NoError(t, regErr)
				windowTargetID = reg.WindowTargetID
			} else {
				windowTargetID = "wt-nonexistent"
			}

			body, _ := json.Marshal(map[string]any{
				"WindowId":       mw.WindowID,
				"WindowTargetId": windowTargetID,
			})
			rec := doRequest(t, h, "DeregisterTargetFromMaintenanceWindow", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				if tt.wantWindowID {
					assert.Equal(t, mw.WindowID, resp["WindowId"])
				}
				if tt.wantTargetID {
					assert.Equal(t, windowTargetID, resp["WindowTargetId"])
				}
			}
		})
	}
}

// TestDeregisterTaskFromMaintenanceWindow_TableDriven verifies the response fields
// and error handling for DeregisterTaskFromMaintenanceWindow.
func TestDeregisterTaskFromMaintenanceWindow_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   int
		taskExists   bool
		wantWindowID bool
		wantTaskID   bool
	}{
		{
			name:         "registered_task_deregisters_with_ids",
			taskExists:   true,
			wantStatus:   http.StatusOK,
			wantWindowID: true,
			wantTaskID:   true,
		},
		{
			name:       "nonexistent_task_returns_error",
			taskExists: false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			mw, err := b.CreateMaintenanceWindow(
				context.Background(),
				&ssm.CreateMaintenanceWindowInput{
					Name:     "test-win",
					Schedule: "cron(0 9 ? * MON *)",
					Duration: 2,
					Cutoff:   1,
				},
			)
			require.NoError(t, err)

			var windowTaskID string
			if tt.taskExists {
				reg, regErr := b.RegisterTaskWithMaintenanceWindow(
					context.Background(),
					&ssm.RegisterTaskWithMaintenanceWindowInput{
						WindowID: mw.WindowID,
						TaskArn:  "arn:aws:lambda:us-east-1:123456789012:function:MyFunc",
						TaskType: "LAMBDA",
					},
				)
				require.NoError(t, regErr)
				windowTaskID = reg.WindowTaskID
			} else {
				windowTaskID = "wt-nonexistent-task"
			}

			body, _ := json.Marshal(map[string]any{
				"WindowId":     mw.WindowID,
				"WindowTaskId": windowTaskID,
			})
			rec := doRequest(t, h, "DeregisterTaskFromMaintenanceWindow", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				if tt.wantWindowID {
					assert.Equal(t, mw.WindowID, resp["WindowId"])
				}
				if tt.wantTaskID {
					assert.Equal(t, windowTaskID, resp["WindowTaskId"])
				}
			}
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
