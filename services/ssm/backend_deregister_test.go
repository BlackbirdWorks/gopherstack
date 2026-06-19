package ssm_test

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDeregisterPatchBaselineForPatchGroup_TableDriven verifies the response fields
// and error handling for DeregisterPatchBaselineForPatchGroup.
func TestDeregisterPatchBaselineForPatchGroup_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		baselineID     string
		patchGroup     string
		wantBaselineID string
		wantPatchGroup string
		wantStatus     int
	}{
		{
			name:           "deregisters_and_returns_ids",
			baselineID:     "pb-0123456789abcdef0",
			patchGroup:     "Production",
			wantStatus:     http.StatusOK,
			wantBaselineID: "pb-0123456789abcdef0",
			wantPatchGroup: "Production",
		},
		{
			name:           "empty_baseline_id_returns_ok",
			baselineID:     "",
			patchGroup:     "Staging",
			wantStatus:     http.StatusOK,
			wantPatchGroup: "Staging",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			body, _ := json.Marshal(map[string]any{
				"BaselineId": tt.baselineID,
				"PatchGroup": tt.patchGroup,
			})

			rec := doRequest(t, h, "DeregisterPatchBaselineForPatchGroup", string(body))
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantBaselineID != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantBaselineID, resp["BaselineId"])
				assert.Equal(t, tt.wantPatchGroup, resp["PatchGroup"])
			}
		})
	}
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

// TestDeleteActivation_TableDriven verifies success and not-found for DeleteActivation.
func TestDeleteActivation_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		wantStatus int
		setupFirst bool
	}{
		{
			name:       "deletes_existing_activation",
			setupFirst: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_activation_returns_error",
			setupFirst: false,
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "ActivationNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			activationID := "act-nonexistent"
			if tt.setupFirst {
				act, err := b.CreateActivation(context.Background(), &ssm.CreateActivationInput{
					IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
					RegistrationLimit: 1,
				})
				require.NoError(t, err)
				activationID = act.ActivationID
			}

			body, _ := json.Marshal(map[string]any{"ActivationId": activationID})
			rec := doRequest(t, h, "DeleteActivation", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
			}
		})
	}
}

// TestDeleteAssociation_TableDriven verifies success and not-found for DeleteAssociation.
func TestDeleteAssociation_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		wantStatus int
		setupFirst bool
	}{
		{
			name:       "deletes_existing_association",
			setupFirst: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_association_returns_error",
			setupFirst: false,
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "AssociationDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			assocID := "assoc-nonexistent"
			if tt.setupFirst {
				assoc, err := b.CreateAssociation(context.Background(), &ssm.CreateAssociationInput{
					Name:       "AWS-RunShellScript",
					InstanceID: "i-1234567890abcdef0",
				})
				require.NoError(t, err)
				assocID = assoc.AssociationDescription.AssociationID
			}

			body, _ := json.Marshal(map[string]any{"AssociationId": assocID})
			rec := doRequest(t, h, "DeleteAssociation", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
			}
		})
	}
}

// TestUpdateOpsItem_TableDriven verifies UpdateOpsItem with multiple field updates.
func TestUpdateOpsItem_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		wantErrMsg string
		wantStatus int
		setupFirst bool
	}{
		{
			name:       "updates_title_and_status",
			setupFirst: true,
			update:     map[string]any{"Title": "Updated Title", "Status": "Resolved"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_ops_item_returns_error",
			setupFirst: false,
			update:     map[string]any{"Title": "Should Fail"},
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "OpsItemNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			opsItemID := "oi-nonexistent"
			if tt.setupFirst {
				item, err := b.CreateOpsItem(context.Background(), &ssm.CreateOpsItemInput{
					Title:    "Original Title",
					Source:   "test",
					Severity: "2",
					Category: "Performance",
				})
				require.NoError(t, err)
				opsItemID = item.OpsItemID
			}

			payload := map[string]any{"OpsItemId": opsItemID}
			maps.Copy(payload, tt.update)

			body, _ := json.Marshal(payload)
			rec := doRequest(t, h, "UpdateOpsItem", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
			}
		})
	}
}
