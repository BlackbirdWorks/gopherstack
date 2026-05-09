package backup_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newHandlerAndBackend() (*backup.Handler, *backup.InMemoryBackend) {
	be := backup.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return backup.NewHandler(be), be
}

func seedVaultAndRP(t *testing.T, h *backup.Handler, be *backup.InMemoryBackend, vaultName string) string {
	t.Helper()

	// Create vault via HTTP
	rec := doREST(t, h, http.MethodPut, "/backup-vaults/"+vaultName, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Add recovery point directly via backend
	rpArn := fmt.Sprintf("arn:aws:backup:us-east-1:123456789012:recovery-point:rp-%s-001", vaultName)
	now := time.Now().UTC()
	rp := &backup.RecoveryPoint{
		RecoveryPointArn:  rpArn,
		BackupVaultName:   vaultName,
		BackupVaultArn:    fmt.Sprintf("arn:aws:backup:us-east-1:123456789012:backup-vault:%s", vaultName),
		ResourceArn:       "arn:aws:ec2:us-east-1:123456789012:instance/i-test",
		ResourceType:      "EC2",
		IAMRoleArn:        "arn:aws:iam::123456789012:role/AWSBackupDefaultServiceRole",
		Status:            "COMPLETED",
		CreationDate:      now,
		BackupSizeInBytes: 1024,
	}
	err := be.AddRecoveryPoint(vaultName, rp)
	require.NoError(t, err)

	return rpArn
}

func TestRecoveryPoint_ListByVault(t *testing.T) {
	t.Parallel()

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
}

func TestRecoveryPoint_ListByVault_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/emptyvault", nil)

	rec := doREST(t, h, http.MethodGet, "/backup-vaults/emptyvault/recovery-points", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	rps, ok := resp["RecoveryPoints"].([]any)
	assert.True(t, ok)
	assert.Empty(t, rps)
}

func TestRecoveryPoint_Describe(t *testing.T) {
	t.Parallel()

	h, be := newHandlerAndBackend()
	rpArn := seedVaultAndRP(t, h, be, "describevault")

	rec := doREST(t, h, http.MethodGet,
		fmt.Sprintf("/backup-vaults/describevault/recovery-points/%s", rpArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	assert.Equal(t, rpArn, resp["RecoveryPointArn"])
}

func TestRecoveryPoint_Describe_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/nfvault", nil)

	rec := doREST(t, h, http.MethodGet,
		"/backup-vaults/nfvault/recovery-points/arn:aws:backup:us-east-1:123456789012:recovery-point:nonexistent",
		nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRecoveryPoint_GetRestoreMetadata(t *testing.T) {
	t.Parallel()

	h, be := newHandlerAndBackend()
	rpArn := seedVaultAndRP(t, h, be, "metavault")

	rec := doREST(t, h, http.MethodGet,
		fmt.Sprintf("/backup-vaults/metavault/recovery-points/%s/restore-metadata", rpArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	assert.NotNil(t, resp["RestoreMetadata"])
}

func TestRecoveryPoint_Delete(t *testing.T) {
	t.Parallel()

	h, be := newHandlerAndBackend()
	rpArn := seedVaultAndRP(t, h, be, "delvault")

	rec := doREST(t, h, http.MethodDelete,
		fmt.Sprintf("/backup-vaults/delvault/recovery-points/%s", rpArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec2 := doREST(t, h, http.MethodGet,
		fmt.Sprintf("/backup-vaults/delvault/recovery-points/%s", rpArn), nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestVaultAccessPolicy_PutGetDelete(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/policyvault", nil)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	rec := doREST(t, h, http.MethodPut, "/backup-vaults/policyvault/access-policy", map[string]any{
		"Policy": policy,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/backup-vaults/policyvault/access-policy", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
	resp := parseResp(t, rec2)
	assert.Equal(t, "policyvault", resp["BackupVaultName"])

	rec3 := doREST(t, h, http.MethodDelete, "/backup-vaults/policyvault/access-policy", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doREST(t, h, http.MethodGet, "/backup-vaults/policyvault/access-policy", nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}

func TestVaultLockConfiguration_PutDelete(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/lockvault", nil)

	rec := doREST(t, h, http.MethodPut, "/backup-vaults/lockvault/vault-lock", map[string]any{
		"MinRetentionDays": 7,
		"MaxRetentionDays": 365,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodDelete, "/backup-vaults/lockvault/vault-lock", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestVaultNotifications_PutGetDelete(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/notifvault", nil)

	rec := doREST(t, h, http.MethodPut, "/backup-vaults/notifvault/notification-configuration", map[string]any{
		"BackupVaultEvents": []string{"BACKUP_JOB_STARTED"},
		"SNSTopicArn":       "arn:aws:sns:us-east-1:123456789012:backup-topic",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/backup-vaults/notifvault/notification-configuration", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
	resp := parseResp(t, rec2)
	assert.Equal(t, "notifvault", resp["BackupVaultName"])

	rec3 := doREST(t, h, http.MethodDelete, "/backup-vaults/notifvault/notification-configuration", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doREST(t, h, http.MethodGet, "/backup-vaults/notifvault/notification-configuration", nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}

func TestRecoveryPoint_Disassociate(t *testing.T) {
	t.Parallel()

	h, be := newHandlerAndBackend()
	rpArn := seedVaultAndRP(t, h, be, "disassocvault")

	rec := doREST(t, h, http.MethodPost,
		fmt.Sprintf("/backup-vaults/disassocvault/recovery-points/%s/disassociate", rpArn), nil)
	// DisassociateRecoveryPoint removes the IAM role association
	assert.Equal(t, http.StatusOK, rec.Code)
}
