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

func TestStubOps_SimpleCalls(t *testing.T) {
	t.Parallel()

	// These operations accept empty bodies and return stub responses.
	// GetAccessToken and StartAccessRequest are deliberately NOT listed here:
	// both have real required fields (AccessRequestId; Reason+Targets) and
	// now correctly reject an empty body with ValidationException — see
	// TestGetAccessToken_RequiresAccessRequestID and
	// TestAccessRequest_ValidationRequiresReasonAndTargets in sessions_test.go.
	// ListNodesSummary is also NOT listed here: it has a real required field
	// (Aggregators) and now correctly rejects an empty body with
	// InvalidAggregatorException — see TestListNodesSummary_MissingAggregators
	// in list_nodes_summary_test.go.
	// StartChangeRequestExecution and UpdateResourceDataSync are also NOT
	// listed here (gopherstack-4ggy): both now correctly reject an empty
	// body -- Runbooks and SyncSource/SyncType respectively are required and
	// were previously dropped entirely. See TestChangeRequest
	// (automations_test.go) and TestResourceDataSync_CRUD (activations_test.go).
	// DeleteResourcePolicy and PutResourcePolicy are also NOT listed here
	// (gopherstack-enpq): both now correctly reject an empty body --
	// ResourceArn/PolicyId/PolicyHash (Delete) and ResourceArn/Policy (Put)
	// are required and were previously dropped entirely. See
	// TestResourcePolicies_RequiredFields in resource_policies_test.go.
	// PutComplianceItems is also NOT listed here (gopherstack-enpq): it now
	// correctly rejects an empty body -- ResourceId/ResourceType/
	// ComplianceType/ExecutionSummary (with ExecutionSummary.ExecutionTime)
	// are all required and were previously dropped entirely. See
	// TestPutComplianceItems_RequiredFields in inventory_test.go.
	// CreateResourceDataSync is also NOT listed here (gopherstack-enpq):
	// S3Destination/SyncSource had NO Go struct members at all, so an empty
	// body always created a sync with no destination/source config that
	// could never be reached again except via UpdateResourceDataSync. Now
	// S3Destination is required (SyncType defaults to SyncToDestination when
	// omitted). See TestCreateResourceDataSync_RequiredFields.
	// UpdateDocumentMetadata and ListDocumentMetadataHistory are also NOT
	// listed here (gopherstack-enpq): both now correctly reject an empty
	// body -- Name (both) and DocumentReviews/Metadata (respectively) are
	// required and were previously entirely unvalidated. See
	// TestUpdateDocumentMetadata_RequiresDocumentReviews and
	// TestListDocumentMetadataHistory_RequiresNameAndMetadata in
	// document_metadata_test.go.
	// UpdateDocumentDefaultVersion is also NOT listed here (gopherstack-enpq):
	// Name and DocumentVersion are both required on the real op but an empty
	// body previously returned a silent empty-success stub instead of
	// ValidationException. See TestUpdateDocumentDefaultVersion_RequiresFields.
	// DescribeAssociationExecutionTargets, DescribeAssociationExecutions,
	// ListAssociationVersions and StartAssociationsOnce are also NOT listed
	// here (gopherstack-enpq): AssociationId (the first three) and
	// AssociationIds (the last) are all required on the real ops
	// (api_op_DescribeAssociationExecutionTargets.go,
	// api_op_DescribeAssociationExecutions.go,
	// api_op_ListAssociationVersions.go, api_op_StartAssociationsOnce.go) but
	// an empty body previously returned a silent empty-success stub instead
	// of ValidationException. See TestAssociationOps_RequireAssociationID in
	// associations_test.go.
	// GetCalendarState, DescribeAutomationStepExecutions, GetExecutionPreview,
	// SendAutomationSignal, StartAutomationExecution, StartExecutionPreview
	// and StopAutomationExecution are also NOT listed here (gopherstack-enpq):
	// CalendarNames/AutomationExecutionId/ExecutionPreviewId/
	// (AutomationExecutionId+SignalType)/DocumentName/DocumentName/
	// AutomationExecutionId respectively are all required on the real ops but
	// an empty body previously returned a silent empty-success stub instead
	// of ValidationException -- see automations_test.go
	// (TestGetCalendarState_RequiresCalendarNames and the *_Lifecycle/
	// *_RealClient tests) and wire_field_fixes_test.go.
	// DescribeAutomationExecutions stays on this list: its real input
	// (Filters/MaxResults/NextToken) is entirely optional.
	// DisassociateOpsItemRelatedItem is also NOT listed here (gopherstack-enpq):
	// OpsItemId and AssociationId are both required on the real op
	// (api_op_DisassociateOpsItemRelatedItem.go) but an empty body previously
	// returned a silent empty-success stub instead of ValidationException.
	// See TestOpsItemRelatedItemOps_RequireRequiredFields in ops_items_test.go.
	// DescribeMaintenanceWindowExecutionTaskInvocations/-Tasks/-Executions,
	// DescribeMaintenanceWindowTargets/-Tasks, DescribeMaintenanceWindowsForTarget,
	// and GetMaintenanceWindowExecution/-Task/-TaskInvocation/GetMaintenanceWindowTask
	// are also NOT listed here (gopherstack-enpq): each has at least one
	// required field on the real op (WindowExecutionId+TaskId /
	// WindowExecutionId / WindowId / WindowId / WindowId /
	// ResourceType+Targets / WindowExecutionId / WindowExecutionId+
	// TaskExecutionId / WindowExecutionId+TaskExecutionId+InvocationId /
	// WindowId+WindowTaskId respectively) that was previously entirely
	// unvalidated -- every one of these ops fabricated a synthetic
	// "Succeeded" execution/task/invocation record even for a body missing
	// every field. See TestMaintenanceWindowOps_RequireRequiredFields in
	// maintenance_window_execution_test.go.
	// DescribeMaintenanceWindowSchedule stays on this list: its real input
	// (Filters/MaxResults/NextToken/ResourceType/Targets/WindowId) is
	// entirely optional.
	ops := []string{
		"DeleteInventory",
		"DeregisterManagedInstance",
		"DescribeActivations",
		"DescribeAutomationExecutions",
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
		"DescribeMaintenanceWindowSchedule",
		"DescribePatchGroupState",
		"DescribePatchGroups",
		"DescribePatchProperties",
		"DescribeSessions",
		"GetConnectionStatus",
		"GetDeployablePatchSnapshotForInstance",
		"GetInventory",
		"GetInventorySchema",
		"GetOpsSummary",
		"GetPatchBaselineForPatchGroup",
		"GetResourcePolicies",
		"GetServiceSetting",
		"LabelParameterVersion",
		"ListAssociations",
		"ListComplianceItems",
		"ListComplianceSummaries",
		"ListInventoryEntries",
		"ListNodes",
		"ListOpsItemEvents",
		"ListOpsItemRelatedItems",
		"ListOpsMetadata",
		"ListResourceComplianceSummaries",
		"ListResourceDataSync",
		"PutInventory",
		"RegisterDefaultPatchBaseline",
		"ResetServiceSetting",
		"ResumeSession",
		"StartSession",
		"TerminateSession",
		"UnlabelParameterVersion",
		"UpdateManagedInstanceRole",
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

// TestMaintenanceWindow_ScheduleFieldsRoundTrip locks in
// StartDate/EndDate/ScheduleTimezone/ScheduleOffset, which were entirely
// absent from CreateMaintenanceWindowInput/UpdateMaintenanceWindowInput and
// silently discarded even when a client sent them. Confirmed present in
// aws-sdk-go-v2/service/ssm@v1.73.4's api_op_CreateMaintenanceWindow.go and
// api_op_UpdateMaintenanceWindow.go (the latter also updates
// AllowUnassociatedTargets, previously create-only in this emulator).
func TestMaintenanceWindow_ScheduleFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	rec := doRequest(t, h, "CreateMaintenanceWindow", `{
		"Name": "ScheduledWindow",
		"Schedule": "cron(0 2 ? * SUN *)",
		"ScheduleTimezone": "America/Los_Angeles",
		"ScheduleOffset": 2,
		"StartDate": "2026-01-01T00:00:00Z",
		"EndDate": "2026-12-31T00:00:00Z",
		"Duration": 4,
		"Cutoff": 1
	}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var created ssm.CreateMaintenanceWindowOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	out, err := b.GetMaintenanceWindow(context.Background(), &ssm.GetMaintenanceWindowInput{WindowID: created.WindowID})
	require.NoError(t, err)
	assert.Equal(t, "America/Los_Angeles", out.ScheduleTimezone)
	assert.EqualValues(t, 2, out.ScheduleOffset)
	assert.Equal(t, "2026-01-01T00:00:00Z", out.StartDate)
	assert.Equal(t, "2026-12-31T00:00:00Z", out.EndDate)

	allowFalse := false
	updated, err := b.UpdateMaintenanceWindow(context.Background(), &ssm.UpdateMaintenanceWindowInput{
		WindowID:                 created.WindowID,
		ScheduleTimezone:         "UTC",
		AllowUnassociatedTargets: &allowFalse,
	})
	require.NoError(t, err)
	assert.Equal(t, "UTC", updated.ScheduleTimezone)
	assert.False(t, updated.AllowUnassociatedTargets)
	// Fields not touched by this update must survive untouched.
	assert.Equal(t, "2026-01-01T00:00:00Z", updated.StartDate)
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

// TestCreateMaintenanceWindow_DurationCutoffValidation verifies duration/cutoff rules.
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
	h := newHandler()

	// Create
	code, out := postJSON(t, h, "CreateMaintenanceWindow", map[string]any{
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
	code, out = postJSON(t, h, "GetMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestMW", out["Name"])

	// Update
	code, _ = postJSON(t, h, "UpdateMaintenanceWindow", map[string]any{
		"WindowId": windowID,
		"Name":     "UpdatedMW",
	})
	assert.Equal(t, http.StatusOK, code)

	// Describe
	code, out = postJSON(t, h, "DescribeMaintenanceWindows", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	windows := out["WindowIdentities"].([]any)
	assert.NotEmpty(t, windows)

	// Register target
	code, out = postJSON(t, h, "RegisterTargetWithMaintenanceWindow", map[string]any{
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
	code, out = postJSON(t, h, "DescribeMaintenanceWindowTargets", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	targets := out["Targets"].([]any)
	assert.Len(t, targets, 1)

	// Register task
	code, out = postJSON(t, h, "RegisterTaskWithMaintenanceWindow", map[string]any{
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
	code, out = postJSON(t, h, "DescribeMaintenanceWindowTasks", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	tasks := out["Tasks"].([]any)
	assert.Len(t, tasks, 1)

	// GetMaintenanceWindowTask
	code, out = postJSON(t, h, "GetMaintenanceWindowTask", map[string]any{
		"WindowId":     windowID,
		"WindowTaskId": taskID,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, taskID, out["WindowTaskId"])

	// UpdateTarget
	code, _ = postJSON(t, h, "UpdateMaintenanceWindowTarget", map[string]any{
		"WindowId":       windowID,
		"WindowTargetId": targetID,
		"Name":           "UpdatedTarget",
	})
	assert.Equal(t, http.StatusOK, code)

	// UpdateTask
	code, _ = postJSON(t, h, "UpdateMaintenanceWindowTask", map[string]any{
		"WindowId":       windowID,
		"WindowTaskId":   taskID,
		"MaxConcurrency": "2",
	})
	assert.Equal(t, http.StatusOK, code)

	// DeregisterTarget
	code, _ = postJSON(t, h, "DeregisterTargetFromMaintenanceWindow", map[string]any{
		"WindowId":       windowID,
		"WindowTargetId": targetID,
	})
	assert.Equal(t, http.StatusOK, code)

	// DeregisterTask
	code, _ = postJSON(t, h, "DeregisterTaskFromMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"WindowTaskId": taskID,
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = postJSON(t, h, "DeleteMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = postJSON(t, h, "GetMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_MaintenanceWindow_DescribeForTarget(t *testing.T) {
	t.Parallel()
	h := newHandler()

	_, out := postJSON(t, h, "CreateMaintenanceWindow", map[string]any{
		"Name":     "MW2",
		"Schedule": "cron(0 3 ? * MON *)",
		"Duration": 2,
		"Cutoff":   1,
	})
	windowID := out["WindowId"].(string)

	postJSON(t, h, "RegisterTargetWithMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "tag:Env", "Values": []string{"prod"}},
		},
	})

	code, out := postJSON(t, h, "DescribeMaintenanceWindowsForTarget", map[string]any{
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
			name: "schedule_empty",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 4,
				Cutoff:   1,
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
