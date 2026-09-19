package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestProtectedResource_LastBackupVaultArnAndRecoveryPointArn proves
// DescribeProtectedResource/ListProtectedResources/
// ListProtectedResourcesByBackupVault never emitted LastBackupVaultArn or
// LastRecoveryPointArn (real ProtectedResource members, backup@v1.64.0
// types.go) even though both values are fully derivable at the point
// CompleteBackupJob already resolves the vault and creates the recovery
// point -- gopherstack-21my.
func TestProtectedResource_LastBackupVaultArnAndRecoveryPointArn(t *testing.T) {
	t.Parallel()

	b, client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String("pr-vault"),
	})
	require.NoError(t, err)

	const resourceArn = "arn:aws:ec2:us-east-1:123456789012:instance/i-pr"

	startOut, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
		BackupVaultName: aws.String("pr-vault"),
		ResourceArn:     aws.String(resourceArn),
		IamRoleArn:      aws.String("arn:aws:iam::123456789012:role/backup-role"),
	})
	require.NoError(t, err)

	janitor := backup.NewJanitor(b, time.Hour, time.Hour)
	janitor.SweepOnce(ctx)

	descJob, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
		BackupJobId: startOut.BackupJobId,
	})
	require.NoError(t, err)
	require.NotNil(t, descJob.RecoveryPointArn)

	desc, err := client.DescribeProtectedResource(ctx, &backupsdk.DescribeProtectedResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "pr-vault", vaultNameFromArn(t, aws.ToString(desc.LastBackupVaultArn)))
	assert.Equal(t, aws.ToString(descJob.RecoveryPointArn), aws.ToString(desc.LastRecoveryPointArn))

	listed, err := client.ListProtectedResources(ctx, &backupsdk.ListProtectedResourcesInput{})
	require.NoError(t, err)
	require.Len(t, listed.Results, 1)
	assert.Equal(t, aws.ToString(desc.LastBackupVaultArn), aws.ToString(listed.Results[0].LastBackupVaultArn))
	assert.Equal(t, aws.ToString(desc.LastRecoveryPointArn), aws.ToString(listed.Results[0].LastRecoveryPointArn))

	byVault, err := client.ListProtectedResourcesByBackupVault(ctx, &backupsdk.ListProtectedResourcesByBackupVaultInput{
		BackupVaultName: aws.String("pr-vault"),
	})
	require.NoError(t, err)
	require.Len(t, byVault.Results, 1)
	assert.Equal(t, aws.ToString(desc.LastBackupVaultArn), aws.ToString(byVault.Results[0].LastBackupVaultArn))
	assert.Equal(t, aws.ToString(desc.LastRecoveryPointArn), aws.ToString(byVault.Results[0].LastRecoveryPointArn))
}

// vaultNameFromArn extracts the trailing vault-name segment of a backup
// vault ARN for assertion purposes.
func vaultNameFromArn(t *testing.T, arn string) string {
	t.Helper()
	require.NotEmpty(t, arn)

	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == ':' {
			return arn[i+1:]
		}
	}

	return arn
}
