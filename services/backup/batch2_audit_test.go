package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- 1. StartCopyJob CreationDate epoch seconds ----

func TestStartCopyJobCreationDateEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "creation_date_is_epoch_seconds"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/src-vault", `{}`)

			resp := doBatch1Request(t, h, http.MethodPut, "/backup-jobs", `{
				"BackupVaultName": "src-vault",
				"ResourceArn": "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
				"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
			}`)
			require.Equal(t, http.StatusOK, resp.Code)
			var startData map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &startData))
			jobID := startData["BackupJobId"].(string)

			rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-copy-1"
			copyResp := doBatch1Request(t, h, http.MethodPut, "/copy-jobs", `{
				"RecoveryPointArn": "`+rpArn+`",
				"SourceBackupVaultName": "src-vault",
				"DestinationBackupVaultArn": "arn:aws:backup:us-east-1:000000000000:backup-vault:dst-vault",
				"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
			}`)
			require.Equal(t, http.StatusOK, copyResp.Code)
			_ = jobID

			var copyData map[string]any
			require.NoError(t, json.Unmarshal(copyResp.Body.Bytes(), &copyData))
			assert.NotEmpty(t, copyData["CopyJobId"], "CopyJobId must be present")

			creationDate, exists := copyData["CreationDate"]
			require.True(t, exists, "CreationDate must be present in StartCopyJob response")
			_, isFloat := creationDate.(float64)
			assert.True(t, isFloat,
				"CreationDate must be epoch seconds (float64), got %T: %v", creationDate, creationDate)
		})
	}
}

// ---- 2. DescribeGlobalSettings LastUpdateTime epoch seconds ----

func TestGlobalSettingsLastUpdateTimeEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "last_update_time_is_epoch_seconds"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			// Update settings so LastUpdateTime is set.
			doBatch1Request(t, h, http.MethodPut, "/global-settings",
				`{"GlobalSettings":{"isCrossAccountBackupEnabled":"true"}}`)

			resp := doBatch1Request(t, h, http.MethodGet, "/global-settings", "")
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			lastUpdate, exists := data["LastUpdateTime"]
			require.True(t, exists, "LastUpdateTime must be present")
			_, isFloat := lastUpdate.(float64)
			assert.True(t, isFloat,
				"LastUpdateTime must be epoch seconds (float64), got %T: %v", lastUpdate, lastUpdate)
		})
	}
}

// ---- 3. DescribeProtectedResource LastBackupTime epoch seconds ----

func TestProtectedResourceLastBackupTimeEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		createJob  bool
	}{
		{
			name:       "seeded_resource_returns_epoch",
			createJob:  true,
			resourceID: "arn:aws:ec2:us-east-1:000000000000:instance/i-pr-test",
		},
		{
			name:       "never_backed_up_resource_not_found",
			createJob:  false,
			resourceID: "arn:aws:ec2:us-east-1:000000000000:instance/i-unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, b := newBatch1Handler(t)

			if tc.createJob {
				doBatch1Request(t, h, http.MethodPut, "/backup-vaults/pr-vault", `{}`)
				startResp := doBatch1Request(t, h, http.MethodPut, "/backup-jobs", `{
					"BackupVaultName": "pr-vault",
					"ResourceArn": "`+tc.resourceID+`",
					"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
				}`)
				var startData map[string]any
				require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startData))
				// Real AWS Backup jobs complete asynchronously; the emulator
				// models that via the janitor. Complete it synchronously here
				// so the resource is recorded as protected.
				require.NoError(t, b.CompleteBackupJob(startData["BackupJobId"].(string)))
			}

			resp := doBatch1Request(t, h, http.MethodGet, "/resources/"+tc.resourceID, "")

			if !tc.createJob {
				// A resource that was never backed up is not "protected" --
				// AWS returns ResourceNotFoundException, not a fabricated record.
				assert.Equal(t, http.StatusNotFound, resp.Code)

				return
			}

			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			lastBackup, exists := data["LastBackupTime"]
			require.True(t, exists, "LastBackupTime must be present")
			_, isFloat := lastBackup.(float64)
			assert.True(t, isFloat,
				"LastBackupTime must be epoch seconds (float64), got %T: %v", lastBackup, lastBackup)
		})
	}
}

// ---- 4. UpdateBackupPlan UpdateDate in response ----

func TestUpdateBackupPlanUpdateDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_returns_update_date"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			createResp := doBatch1Request(t, h, http.MethodPut, "/backup/plans", `{
				"BackupPlan": {
					"BackupPlanName": "plan-upd",
					"Rules": [{"RuleName": "r1", "TargetBackupVaultName": "v1"}]
				}
			}`)
			require.Equal(t, http.StatusOK, createResp.Code)
			var createData map[string]any
			require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createData))
			planID := createData["BackupPlanId"].(string)

			updateResp := doBatch1Request(t, h, http.MethodPost, "/backup/plans/"+planID, `{
				"BackupPlan": {
					"BackupPlanName": "plan-upd",
					"Rules": [{"RuleName": "r2", "TargetBackupVaultName": "v1"}]
				}
			}`)
			require.Equal(t, http.StatusOK, updateResp.Code)

			var updateData map[string]any
			require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &updateData))

			updateDate, exists := updateData["UpdateDate"]
			require.True(t, exists, "UpdateDate must be present in UpdateBackupPlan response")
			_, isFloat := updateDate.(float64)
			assert.True(t, isFloat,
				"UpdateDate must be epoch seconds (float64), got %T: %v", updateDate, updateDate)
		})
	}
}
