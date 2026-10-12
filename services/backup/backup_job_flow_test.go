package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestBackupJobFlow_RealClient(t *testing.T) {
	t.Parallel()

	const roleArn = "arn:aws:iam::123456789012:role/backup-role"

	tests := []struct {
		lifecycle    *types.Lifecycle
		lock         *backupsdk.PutBackupVaultLockConfigurationInput
		name         string
		resourceArn  string
		wantType     string
		wantName     string
		wantMessage  string
		wantState    types.BackupJobState
		wantSweeps   int
		wantNoRPoint bool
	}{
		{
			name:        "dynamodb_table",
			resourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
			wantType:    "DynamoDB", wantName: "orders", wantState: types.BackupJobStateCompleted,
		},
		{
			name:        "ebs_volume",
			resourceArn: "arn:aws:ec2:us-east-1:123456789012:volume/vol-0abc",
			wantType:    "EBS", wantName: "vol-0abc", wantState: types.BackupJobStateCompleted,
		},
		{
			name:        "rds_instance",
			resourceArn: "arn:aws:rds:us-east-1:123456789012:db:prod",
			wantType:    "RDS", wantName: "prod", wantState: types.BackupJobStateCompleted,
		},
		{
			name:        "lock_min_retention_violated",
			resourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
			lock:        &backupsdk.PutBackupVaultLockConfigurationInput{MinRetentionDays: aws.Int64(7)},
			lifecycle:   &types.Lifecycle{DeleteAfterDays: aws.Int64(2)},
			wantType:    "DynamoDB", wantState: types.BackupJobStateFailed, wantNoRPoint: true,
			wantMessage: "shorter",
		},
		{
			name:        "lock_max_retention_indefinite",
			resourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
			lock:        &backupsdk.PutBackupVaultLockConfigurationInput{MaxRetentionDays: aws.Int64(30)},
			wantType:    "DynamoDB", wantState: types.BackupJobStateFailed, wantNoRPoint: true,
			wantMessage: "maximum",
		},
		{
			name:        "lock_satisfied",
			resourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
			lock: &backupsdk.PutBackupVaultLockConfigurationInput{
				MinRetentionDays: aws.Int64(7), MaxRetentionDays: aws.Int64(30),
			},
			lifecycle: &types.Lifecycle{DeleteAfterDays: aws.Int64(14)},
			wantType:  "DynamoDB", wantName: "orders", wantState: types.BackupJobStateCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(
				ctx,
				&backupsdk.CreateBackupVaultInput{BackupVaultName: aws.String("flow-vault")},
			)
			require.NoError(t, err)

			if tt.lock != nil {
				tt.lock.BackupVaultName = aws.String("flow-vault")
				_, err = client.PutBackupVaultLockConfiguration(ctx, tt.lock)
				require.NoError(t, err)
			}

			started, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("flow-vault"),
				ResourceArn:     aws.String(tt.resourceArn),
				IamRoleArn:      aws.String(roleArn),
				Lifecycle:       tt.lifecycle,
			})
			require.NoError(t, err)

			describe := func() *backupsdk.DescribeBackupJobOutput {
				out, dErr := client.DescribeBackupJob(
					ctx,
					&backupsdk.DescribeBackupJobInput{BackupJobId: started.BackupJobId},
				)
				require.NoError(t, dErr)

				return out
			}

			job := describe()
			assert.Equal(t, types.BackupJobStateCreated, job.State)
			assert.Equal(t, tt.wantType, aws.ToString(job.ResourceType))

			janitor := backup.NewJanitor(b, time.Hour, time.Hour)
			janitor.SweepOnce(ctx)
			assert.Equal(t, types.BackupJobStateRunning, describe().State)

			janitor.SweepOnce(ctx)
			job = describe()
			assert.Equal(t, tt.wantState, job.State)
			assert.Contains(t, aws.ToString(job.StatusMessage), tt.wantMessage)

			rps, err := client.ListRecoveryPointsByBackupVault(ctx, &backupsdk.ListRecoveryPointsByBackupVaultInput{
				BackupVaultName: aws.String("flow-vault"),
			})
			require.NoError(t, err)

			if tt.wantNoRPoint {
				assert.Empty(t, rps.RecoveryPoints)

				return
			}

			require.Len(t, rps.RecoveryPoints, 1)
			assert.Equal(t, tt.wantType, aws.ToString(rps.RecoveryPoints[0].ResourceType))
			assert.Equal(t, tt.wantName, aws.ToString(rps.RecoveryPoints[0].ResourceName))
		})
	}
}

func TestCopyJobVaultLock_RealClient(t *testing.T) {
	t.Parallel()

	const roleArn = "arn:aws:iam::123456789012:role/backup-role"

	tests := []struct {
		lifecycle *types.Lifecycle
		name      string
		wantState types.CopyJobState
	}{
		{
			name:      "within_bounds",
			lifecycle: &types.Lifecycle{DeleteAfterDays: aws.Int64(14)},
			wantState: types.CopyJobStateCompleted,
		},
		{
			name:      "too_short",
			lifecycle: &types.Lifecycle{DeleteAfterDays: aws.Int64(2)},
			wantState: types.CopyJobStateFailed,
		},
		{name: "indefinite_with_max", wantState: types.CopyJobStateFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			ctx := t.Context()

			for _, name := range []string{"src", "dst"} {
				_, err := client.CreateBackupVault(
					ctx,
					&backupsdk.CreateBackupVaultInput{BackupVaultName: aws.String(name)},
				)
				require.NoError(t, err)
			}

			_, err := client.PutBackupVaultLockConfiguration(ctx, &backupsdk.PutBackupVaultLockConfigurationInput{
				BackupVaultName: aws.String("dst"), MinRetentionDays: aws.Int64(7), MaxRetentionDays: aws.Int64(30),
			})
			require.NoError(t, err)

			started, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("src"),
				ResourceArn:     aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/orders"),
				IamRoleArn:      aws.String(roleArn),
			})
			require.NoError(t, err)

			janitor := backup.NewJanitor(b, time.Hour, time.Hour)
			janitor.SweepOnce(ctx)
			janitor.SweepOnce(ctx)

			job, err := client.DescribeBackupJob(
				ctx,
				&backupsdk.DescribeBackupJobInput{BackupJobId: started.BackupJobId},
			)
			require.NoError(t, err)

			dst, err := client.DescribeBackupVault(
				ctx,
				&backupsdk.DescribeBackupVaultInput{BackupVaultName: aws.String("dst")},
			)
			require.NoError(t, err)

			copied, err := client.StartCopyJob(ctx, &backupsdk.StartCopyJobInput{
				RecoveryPointArn:          job.RecoveryPointArn,
				SourceBackupVaultName:     aws.String("src"),
				DestinationBackupVaultArn: dst.BackupVaultArn,
				IamRoleArn:                aws.String(roleArn),
				Lifecycle:                 tt.lifecycle,
			})
			require.NoError(t, err)

			desc, err := client.DescribeCopyJob(ctx, &backupsdk.DescribeCopyJobInput{CopyJobId: copied.CopyJobId})
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, desc.CopyJob.State)

			if tt.wantState == types.CopyJobStateFailed {
				assert.NotEmpty(t, aws.ToString(desc.CopyJob.StatusMessage))
				assert.Nil(t, desc.CopyJob.DestinationRecoveryPointArn)
			}
		})
	}
}
