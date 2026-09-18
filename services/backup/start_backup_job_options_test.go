package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartBackupJob_BackupOptionsAndStartWindow drives StartBackupJob
// through the real aws-sdk-go-v2 client and proves BackupOptions and
// StartWindowMinutes -- both previously accepted on the wire and completely
// dropped -- now round-trip through DescribeBackupJob: BackupOptions is
// echoed verbatim (DescribeBackupJobOutput.BackupOptions,
// api_op_DescribeBackupJob.go), and StartWindowMinutes drives the computed
// StartBy timestamp (StartBy = CreationDate + StartWindowMinutes).
func TestStartBackupJob_BackupOptionsAndStartWindow(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String("options-vault"),
	})
	require.NoError(t, err)

	const startWindowMinutes = 120

	startOut, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
		BackupVaultName:    aws.String("options-vault"),
		ResourceArn:        aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-options"),
		IamRoleArn:         aws.String("arn:aws:iam::123456789012:role/backup-role"),
		BackupOptions:      map[string]string{"WindowsVSS": "enabled"},
		StartWindowMinutes: aws.Int64(startWindowMinutes),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
		BackupJobId: startOut.BackupJobId,
	})
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"WindowsVSS": "enabled"}, descOut.BackupOptions)
	require.NotNil(t, descOut.StartBy)
	require.NotNil(t, descOut.CreationDate)
	assert.Equal(
		t,
		startWindowMinutes*time.Minute,
		descOut.StartBy.Sub(*descOut.CreationDate),
	)
}
