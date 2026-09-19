package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListBackupVaults_LockAndCreatorFields proves ListBackupVaults never
// emitted Locked/LockDate/CreatorRequestId/EncryptionKeyType (real
// BackupVaultListMember members, backup@v1.64.0 types.go), unlike
// DescribeBackupVault which already derives all four the same way --
// gopherstack-21my.
func TestListBackupVaults_LockAndCreatorFields(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
		BackupVaultName:  aws.String("lbv-vault"),
		CreatorRequestId: aws.String("req-123"),
		EncryptionKeyArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/lbv-key"),
	})
	require.NoError(t, err)

	_, err = client.PutBackupVaultLockConfiguration(ctx, &backupsdk.PutBackupVaultLockConfigurationInput{
		BackupVaultName:  aws.String("lbv-vault"),
		MinRetentionDays: aws.Int64(7),
		MaxRetentionDays: aws.Int64(365),
	})
	require.NoError(t, err)

	out, err := client.ListBackupVaults(ctx, &backupsdk.ListBackupVaultsInput{})
	require.NoError(t, err)
	require.Len(t, out.BackupVaultList, 1)

	v := out.BackupVaultList[0]
	assert.Equal(t, "req-123", aws.ToString(v.CreatorRequestId), "CreatorRequestId must not be dropped")
	assert.Equal(t, types.EncryptionKeyTypeCustomerManagedKmsKey, v.EncryptionKeyType)
	require.NotNil(t, v.Locked)
	assert.True(t, *v.Locked, "Locked must reflect the vault lock config")
	require.NotNil(t, v.MinRetentionDays)
	assert.EqualValues(t, 7, *v.MinRetentionDays)
	require.NotNil(t, v.MaxRetentionDays)
	assert.EqualValues(t, 365, *v.MaxRetentionDays)
}
