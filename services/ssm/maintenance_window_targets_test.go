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
