package backup_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestRecoveryPointOps(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	// Create vault and recovery point
	vault, _ := b.CreateBackupVault("test-vault", "", "", nil)
	b.AddRecoveryPoint(
		vault.BackupVaultName,
		&backup.RecoveryPoint{ //nolint:exhaustruct // test fixture only needs specific fields
			RecoveryPointArn: "arn:rp-test",
			BackupVaultName:  vault.BackupVaultName,
			ResourceArn:      "arn:aws:ec2:us-east-1:000000000000:volume/vol-test",
			Status:           "COMPLETED",
		},
	)

	t.Run("list by resource ARN", func(t *testing.T) {
		t.Parallel()
		rps := b.ListRecoveryPointsByResource("arn:aws:ec2:us-east-1:000000000000:volume/vol-test")
		assert.NotEmpty(t, rps)
	})

	t.Run("update lifecycle", func(t *testing.T) {
		t.Parallel()
		err := b.UpdateRecoveryPointLifecycle(vault.BackupVaultName, "arn:rp-test", 30, 365)
		require.NoError(t, err)
	})

	t.Run("index details", func(t *testing.T) {
		t.Parallel()
		status, err := b.GetRecoveryPointIndexDetails(vault.BackupVaultName, "arn:rp-test")
		require.NoError(t, err)
		assert.NotEmpty(t, status)
	})
}

func TestRecoveryPointNewFields(t *testing.T) {
	t.Parallel()

	h, b := newHandler(t)

	// Create vault + recovery point.
	doRequest(t, h, http.MethodPut, "/backup-vaults/rp-vault", `{}`)

	rp := &backup.RecoveryPoint{
		RecoveryPointArn:     "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-1",
		BackupVaultName:      "rp-vault",
		BackupVaultArn:       "arn:aws:backup:us-east-1:000000000000:backup-vault:rp-vault",
		Status:               "COMPLETED",
		StorageClass:         "WARM",
		IsEncrypted:          true,
		EncryptionKeyArn:     "arn:aws:kms:us-east-1:000000000000:key/abc123",
		SourceBackupVaultArn: "arn:aws:backup:us-east-1:000000000000:backup-vault:src-vault",
	}
	require.NoError(t, b.AddRecoveryPoint("rp-vault", rp))

	tests := []struct {
		checkResp func(t *testing.T, resp map[string]any)
		name      string
	}{
		{
			name: "storage_class_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, "WARM", resp["StorageClass"])
			},
		},
		{
			name: "is_encrypted_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, true, resp["IsEncrypted"])
			},
		},
		{
			name: "encryption_key_arn_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc123", resp["EncryptionKeyArn"])
			},
		},
		{
			name: "source_backup_vault_arn_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(
					t,
					"arn:aws:backup:us-east-1:000000000000:backup-vault:src-vault",
					resp["SourceBackupVaultArn"],
				)
			},
		},
	}

	descResp := doRequest(t, h, http.MethodGet,
		"/backup-vaults/rp-vault/recovery-points/"+rp.RecoveryPointArn, "")
	require.Equal(t, http.StatusOK, descResp.Code)

	var respData map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &respData))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.checkResp(t, respData)
		})
	}
}

// ---- VaultLockConfig GetBackupVaultLockConfig helper ----
// (used in TestVaultLockLockDate; exposed via export if not present)

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

// TestRecoveryPointHandlers exercises the simple single-call recovery point
// endpoints (list/describe/restore-metadata/delete/disassociate) that don't
// need their own bespoke setup.
func TestRecoveryPointHandlers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T)
		name string
	}{
		{
			name: "list_by_vault",
			ops: func(t *testing.T) {
				t.Helper()
				h, be := newHandlerAndBackend()
				rpArn := seedVaultAndRP(t, h, be, "listvault")

				rec := doREST(t, h, http.MethodGet, "/backup-vaults/listvault/recovery-points", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				rps, ok := resp["RecoveryPoints"].([]any)
				assert.True(t, ok)
				assert.Len(t, rps, 1)
				firstRP := rps[0].(map[string]any)
				assert.Equal(t, rpArn, firstRP["RecoveryPointArn"])
			},
		},
		{
			name: "list_by_vault_empty",
			ops: func(t *testing.T) {
				t.Helper()
				h, _ := newHandlerAndBackend()
				doREST(t, h, http.MethodPut, "/backup-vaults/emptyvault", nil)

				rec := doREST(t, h, http.MethodGet, "/backup-vaults/emptyvault/recovery-points", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				rps, ok := resp["RecoveryPoints"].([]any)
				assert.True(t, ok)
				assert.Empty(t, rps)
			},
		},
		{
			name: "describe",
			ops: func(t *testing.T) {
				t.Helper()
				h, be := newHandlerAndBackend()
				rpArn := seedVaultAndRP(t, h, be, "describevault")

				rec := doREST(t, h, http.MethodGet,
					fmt.Sprintf("/backup-vaults/describevault/recovery-points/%s", rpArn), nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, rpArn, resp["RecoveryPointArn"])
			},
		},
		{
			name: "describe_not_found",
			ops: func(t *testing.T) {
				t.Helper()
				h, _ := newHandlerAndBackend()
				doREST(t, h, http.MethodPut, "/backup-vaults/nfvault", nil)

				rec := doREST(
					t,
					h,
					http.MethodGet,
					"/backup-vaults/nfvault/recovery-points/arn:aws:backup:us-east-1:123456789012:recovery-point:nonexistent",
					nil,
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "get_restore_metadata",
			ops: func(t *testing.T) {
				t.Helper()
				h, be := newHandlerAndBackend()
				rpArn := seedVaultAndRP(t, h, be, "metavault")

				rec := doREST(t, h, http.MethodGet,
					fmt.Sprintf("/backup-vaults/metavault/recovery-points/%s/restore-metadata", rpArn), nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.NotNil(t, resp["RestoreMetadata"])
			},
		},
		{
			name: "delete",
			ops: func(t *testing.T) {
				t.Helper()
				h, be := newHandlerAndBackend()
				rpArn := seedVaultAndRP(t, h, be, "delvault")

				rec := doREST(t, h, http.MethodDelete,
					fmt.Sprintf("/backup-vaults/delvault/recovery-points/%s", rpArn), nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify deleted
				rec2 := doREST(t, h, http.MethodGet,
					fmt.Sprintf("/backup-vaults/delvault/recovery-points/%s", rpArn), nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "disassociate",
			ops: func(t *testing.T) {
				t.Helper()
				h, be := newHandlerAndBackend()
				rpArn := seedVaultAndRP(t, h, be, "disassocvault")

				rec := doREST(t, h, http.MethodPost,
					fmt.Sprintf("/backup-vaults/disassocvault/recovery-points/%s/disassociate", rpArn), nil)
				// DisassociateRecoveryPoint removes the IAM role association
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.ops(t)
		})
	}
}
