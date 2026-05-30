package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// ---- 1. DeleteBackupVault rejects vault with recovery points ----

func TestDeleteVaultWithRecoveryPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seedRP     bool
		wantStatus int
	}{
		{
			name:       "empty_vault_deletes_ok",
			seedRP:     false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "vault_with_recovery_point_rejected",
			seedRP:     true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, be := newHandlerAndBackend()
			rec := doREST(t, h, http.MethodPut, "/backup-vaults/dvault", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			if tc.seedRP {
				rpArn := "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-001"
				err := be.AddRecoveryPoint("dvault", &backup.RecoveryPoint{
					RecoveryPointArn: rpArn,
					BackupVaultName:  "dvault",
					Status:           "COMPLETED",
				})
				require.NoError(t, err)
			}

			rec2 := doREST(t, h, http.MethodDelete, "/backup-vaults/dvault", nil)
			assert.Equal(t, tc.wantStatus, rec2.Code)
		})
	}
}

// ---- 2. GetBackupVaultAccessPolicy returns real BackupVaultArn ----

func TestGetBackupVaultAccessPolicyArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns_real_vault_arn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandlerAndBackend()
			doREST(t, h, http.MethodPut, "/backup-vaults/arnvault", nil)
			doREST(t, h, http.MethodPut, "/backup-vaults/arnvault/access-policy", map[string]any{
				"Policy": `{"Version":"2012-10-17","Statement":[]}`,
			})

			rec := doREST(t, h, http.MethodGet, "/backup-vaults/arnvault/access-policy", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			arn, _ := resp["BackupVaultArn"].(string)
			assert.Contains(t, arn, "arnvault", "BackupVaultArn must contain vault name, got empty string")
			assert.Equal(t, "arnvault", resp["BackupVaultName"])
		})
	}
}

// ---- 3. GetBackupVaultNotifications returns real BackupVaultArn ----

func TestGetBackupVaultNotificationsArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns_real_vault_arn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandlerAndBackend()
			doREST(t, h, http.MethodPut, "/backup-vaults/notifarnvault", nil)
			doREST(t, h, http.MethodPut, "/backup-vaults/notifarnvault/notification-configuration",
				map[string]any{
					"BackupVaultEvents": []string{"BACKUP_JOB_STARTED"},
					"SNSTopicArn":       "arn:aws:sns:us-east-1:123456789012:topic",
				})

			rec := doREST(t, h, http.MethodGet, "/backup-vaults/notifarnvault/notification-configuration", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			arn, _ := resp["BackupVaultArn"].(string)
			assert.Contains(t, arn, "notifarnvault", "BackupVaultArn must contain vault name, got empty string")
		})
	}
}

// ---- 4. GetRecoveryPointRestoreMetadata returns real BackupVaultArn and BackupVaultName ----

func TestGetRecoveryPointRestoreMetadataArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns_vault_arn_and_name"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, be := newHandlerAndBackend()
			rpArn := seedVaultAndRP(t, h, be, "metaarnvault")

			rec := doREST(t, h, http.MethodGet,
				"/backup-vaults/metaarnvault/recovery-points/"+rpArn+"/restore-metadata", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)

			vaultArn, _ := resp["BackupVaultArn"].(string)
			assert.Contains(t, vaultArn, "metaarnvault",
				"BackupVaultArn should reference vault name, got %q", vaultArn)
			assert.Equal(t, "metaarnvault", resp["BackupVaultName"])
			assert.Equal(t, rpArn, resp["RecoveryPointArn"])
		})
	}
}

// ---- 5. ListRecoveryPointsByBackupVault includes BackupSizeInBytes ----

func TestListRecoveryPointsBackupSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		backupSizeInBytes int64
		expectInResponse  bool
	}{
		{
			name:              "non_zero_size_included",
			backupSizeInBytes: 2048,
			expectInResponse:  true,
		},
		{
			name:              "zero_size_omitted",
			backupSizeInBytes: 0,
			expectInResponse:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, be := newHandlerAndBackend()
			doREST(t, h, http.MethodPut, "/backup-vaults/sizevault", nil)
			rpArn := "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-size-001"
			err := be.AddRecoveryPoint("sizevault", &backup.RecoveryPoint{
				RecoveryPointArn:  rpArn,
				BackupVaultName:   "sizevault",
				Status:            "COMPLETED",
				BackupSizeInBytes: tc.backupSizeInBytes,
			})
			require.NoError(t, err)

			rec := doREST(t, h, http.MethodGet, "/backup-vaults/sizevault/recovery-points", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			rps, _ := resp["RecoveryPoints"].([]any)
			require.Len(t, rps, 1)
			item := rps[0].(map[string]any)

			_, present := item["BackupSizeInBytes"]
			assert.Equal(t, tc.expectInResponse, present)
			if tc.expectInResponse {
				assert.InDelta(t, float64(tc.backupSizeInBytes), item["BackupSizeInBytes"], 0)
			}
		})
	}
}

// ---- 6. ListBackupPlanVersions includes VersionId and CreationDate ----

func TestListBackupPlanVersionsFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "version_fields_present"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			doREST(t, h, http.MethodPut, "/backup-vaults/vv-vault", nil)
			planID := createBackupPlan(t, h, "vv-plan")

			rec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID+"/versions", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			versions, ok := resp["BackupPlanVersionsList"].([]any)
			assert.True(t, ok)
			require.NotEmpty(t, versions)

			v := versions[0].(map[string]any)
			assert.NotEmpty(t, v["BackupPlanId"], "BackupPlanId should be present")
			assert.NotEmpty(t, v["BackupPlanName"], "BackupPlanName should be present")
			assert.NotEmpty(t, v["VersionId"], "VersionId should be present")
			assert.NotNil(t, v["CreationDate"], "CreationDate should be present")
		})
	}
}

// ---- 7. ListBackupVaults includes EncryptionKeyArn when set ----

func TestListBackupVaultsEncryptionKeyArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		encryptionKeyArn string
		expectInResponse bool
	}{
		{
			name:             "encrypted_vault_shows_key_arn",
			encryptionKeyArn: "arn:aws:kms:us-east-1:123456789012:key/test-key",
			expectInResponse: true,
		},
		{
			name:             "unencrypted_vault_omits_key_arn",
			encryptionKeyArn: "",
			expectInResponse: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			doREST(t, h, http.MethodPut, "/backup-vaults/kvault", map[string]any{
				"EncryptionKeyArn": tc.encryptionKeyArn,
			})

			rec := doREST(t, h, http.MethodGet, "/backup-vaults", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			list, _ := resp["BackupVaultList"].([]any)
			require.NotEmpty(t, list)

			var found map[string]any
			for _, item := range list {
				m := item.(map[string]any)
				if m["BackupVaultName"] == "kvault" {
					found = m

					break
				}
			}
			require.NotNil(t, found, "kvault not found in list")

			_, present := found["EncryptionKeyArn"]
			assert.Equal(t, tc.expectInResponse, present)
			if tc.expectInResponse {
				assert.Equal(t, tc.encryptionKeyArn, found["EncryptionKeyArn"])
			}
		})
	}
}

// ---- 8. RestoreTestingPlan StartWindowHours round-trip ----

func TestRestoreTestingPlanStartWindowHours(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startWindowHours any
		name             string
		wantHours        float64
		wantInResponse   bool
	}{
		{
			name:             "start_window_hours_round_trips",
			startWindowHours: 4,
			wantHours:        4,
			wantInResponse:   true,
		},
		{
			name:             "zero_start_window_omitted",
			startWindowHours: 0,
			wantHours:        0,
			wantInResponse:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()

			rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
				"RestoreTestingPlan": map[string]any{
					"RestoreTestingPlanName": "swh-plan",
					"ScheduleExpression":     "cron(0 1 * * ? *)",
					"StartWindowHours":       tc.startWindowHours,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doREST(t, h, http.MethodGet, "/restore-testing/plans/swh-plan", nil)
			assert.Equal(t, http.StatusOK, rec2.Code)
			resp := parseResp(t, rec2)
			planDoc, _ := resp["RestoreTestingPlan"].(map[string]any)
			require.NotNil(t, planDoc)

			hours, present := planDoc["StartWindowHours"]
			assert.Equal(t, tc.wantInResponse, present)
			if tc.wantInResponse {
				assert.InDelta(t, tc.wantHours, hours, 0)
			}
		})
	}
}

// TestRestoreTestingPlanStartWindowHoursUpdate verifies UpdateRestoreTestingPlan
// preserves and updates StartWindowHours.
func TestRestoreTestingPlanStartWindowHoursUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialHours      any
		updatedHours      any
		name              string
		wantHoursAfterUpd float64
		wantPresent       bool
	}{
		{
			name:              "update_start_window_hours",
			initialHours:      2,
			updatedHours:      8,
			wantHoursAfterUpd: 8,
			wantPresent:       true,
		},
		{
			name:              "preserve_hours_when_not_in_update",
			initialHours:      3,
			updatedHours:      0,
			wantHoursAfterUpd: 3,
			wantPresent:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()

			doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
				"RestoreTestingPlan": map[string]any{
					"RestoreTestingPlanName": "swh-update-plan",
					"StartWindowHours":       tc.initialHours,
				},
			})

			doREST(t, h, http.MethodPut, "/restore-testing/plans/swh-update-plan",
				map[string]any{
					"RestoreTestingPlan": map[string]any{
						"StartWindowHours": tc.updatedHours,
					},
				})

			rec := doREST(t, h, http.MethodGet, "/restore-testing/plans/swh-update-plan", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			planDoc, _ := resp["RestoreTestingPlan"].(map[string]any)
			require.NotNil(t, planDoc)

			hours, present := planDoc["StartWindowHours"]
			assert.Equal(t, tc.wantPresent, present)
			if tc.wantPresent {
				assert.InDelta(t, tc.wantHoursAfterUpd, hours, 0)
			}
		})
	}
}

// TestRestoreTestingPlanListStartWindowHours verifies ListRestoreTestingPlans
// includes StartWindowHours when non-zero.
func TestRestoreTestingPlanListStartWindowHours(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hours       any
		name        string
		wantHours   float64
		wantPresent bool
	}{
		{
			name:        "list_includes_start_window_hours",
			hours:       6,
			wantPresent: true,
			wantHours:   6,
		},
		{
			name:        "list_omits_zero_start_window_hours",
			hours:       0,
			wantPresent: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()

			doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
				"RestoreTestingPlan": map[string]any{
					"RestoreTestingPlanName": "swh-list-plan",
					"StartWindowHours":       tc.hours,
				},
			})

			rec := doREST(t, h, http.MethodGet, "/restore-testing/plans", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			plans, _ := resp["RestoreTestingPlans"].([]any)
			require.NotEmpty(t, plans)

			plan := plans[0].(map[string]any)
			hours, present := plan["StartWindowHours"]
			assert.Equal(t, tc.wantPresent, present)
			if tc.wantPresent {
				assert.InDelta(t, tc.wantHours, hours, 0)
			}
		})
	}
}

// ---- 9. StartCopyJob (backend) and HTTP DescribeCopyJob fields ----

// TestStartCopyJobAndDescribeViaHTTP verifies that a copy job started via the
// backend is visible and correctly shaped when retrieved via the HTTP handler.
// This also confirms the CopyJobId is non-empty and the response wraps in "CopyJob".
func TestStartCopyJobAndDescribeViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "describe_copy_job_wraps_in_copy_job_key"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, be := newHandlerAndBackend()

			job := be.StartCopyJob(
				"arn:aws:backup:us-east-1:123456789012:recovery-point:rp-001",
				"src-vault",
				"arn:aws:backup:us-east-1:123456789012:backup-vault:dst-vault",
				"arn:aws:iam::123456789012:role/backup-role",
			)
			require.NotEmpty(t, job.CopyJobID)

			rec := doREST(t, h, http.MethodGet, "/copy-jobs/"+job.CopyJobID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)

			copyJobDoc, ok := resp["CopyJob"].(map[string]any)
			assert.True(t, ok, "response must have CopyJob wrapper key")
			if ok {
				assert.Equal(t, job.CopyJobID, copyJobDoc["CopyJobId"])
			}
		})
	}
}
