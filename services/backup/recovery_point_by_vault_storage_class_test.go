package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestListRecoveryPointsByBackupVault_NoStorageClassLeak proves
// ListRecoveryPointsByBackupVault no longer leaks "StorageClass" -- a real
// DescribeRecoveryPointOutput member (backup@v1.64.0 deserializers.go, case
// "StorageClass") but NOT a member of RecoveryPointByBackupVault, this op's
// own item type (its deserializer has no such case). DescribeRecoveryPoint
// must still emit it: it genuinely has the field.
func TestListRecoveryPointsByBackupVault_NoStorageClassLeak(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("123456789012", "us-east-1")
	h := backup.NewHandler(b)

	_, err := b.CreateBackupVault("sc-vault", "", "", nil)
	require.NoError(t, err)

	rp := &backup.RecoveryPoint{
		RecoveryPointArn: "arn:aws:backup:us-east-1:123456789012:recovery-point:sc-rp",
		BackupVaultName:  "sc-vault",
		ResourceArn:      "arn:aws:ec2:us-east-1:123456789012:instance/i-sc",
		ResourceType:     "EC2",
		Status:           "COMPLETED",
		StorageClass:     "WARM",
		CreationDate:     time.Now().UTC(),
	}
	require.NoError(t, b.AddRecoveryPoint("sc-vault", rp))

	listRec := doRequest(t, h, http.MethodGet, "/backup-vaults/sc-vault/recovery-points", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	items, ok := listResp["RecoveryPoints"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	item := items[0].(map[string]any)
	_, hasStorageClass := item["StorageClass"]
	assert.False(t, hasStorageClass, "RecoveryPointByBackupVault has no StorageClass member")

	descRec := doRequest(
		t, h, http.MethodGet,
		"/backup-vaults/sc-vault/recovery-points/"+rp.RecoveryPointArn,
		"",
	)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, "WARM", descResp["StorageClass"], "DescribeRecoveryPoint genuinely has StorageClass")
}
