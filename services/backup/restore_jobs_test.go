package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestRestoreJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("start and describe", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		job, err := b2.StartRestoreJob(
			"arn:aws:backup:us-east-1:000000000000:recovery-point:test",
			"arn:aws:iam::000000000000:role/backup",
			"EBS",
			map[string]string{"newVolumeName": "restored-vol"},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, job.RestoreJobID)
		assert.Equal(t, "COMPLETED", job.Status)
		assert.NotEmpty(t, job.CreatedResourceArn)

		found, err := b2.DescribeRestoreJob(job.RestoreJobID)
		require.NoError(t, err)
		assert.Equal(t, job.RestoreJobID, found.RestoreJobID)
	})

	t.Run("enriches from a tracked source recovery point", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		vault, err := b2.CreateBackupVault("src-vault", "", "", nil)
		require.NoError(t, err)
		rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:enrich-1"
		require.NoError(t, b2.AddRecoveryPoint("src-vault", &backup.RecoveryPoint{
			RecoveryPointArn: rpArn,
			ResourceArn:      "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
			ResourceType:     "EC2",
		}))

		job, err := b2.StartRestoreJob(
			rpArn, "arn:aws:iam::000000000000:role/backup", "", map[string]string{"k": "v"},
		)
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:ec2:us-east-1:000000000000:instance/i-abc", job.ResourceArn)
		assert.Equal(t, "EC2", job.ResourceType)
		assert.Equal(t, vault.BackupVaultArn, job.BackupVaultArn)
	})

	t.Run("list jobs", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b2.StartRestoreJob("arn:rp-1", "arn:role", "EBS", map[string]string{"k": "v"})
		require.NoError(t, err)
		_, err = b2.StartRestoreJob("arn:rp-2", "arn:role", "RDS", map[string]string{"k": "v"})
		require.NoError(t, err)
		jobs := b2.ListRestoreJobs()
		require.Len(t, jobs, 2)
	})

	t.Run("not found error", func(t *testing.T) {
		t.Parallel()
		_, err := b.DescribeRestoreJob("missing")
		require.ErrorIs(t, err, backup.ErrNotFound)
	})
}

func TestStartRestoreJob_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		metadata         map[string]string
		name             string
		recoveryPointArn string
		iamRoleArn       string
	}{
		{
			name:             "missing RecoveryPointArn",
			recoveryPointArn: "",
			iamRoleArn:       "arn:role",
			metadata:         map[string]string{"k": "v"},
		},
		{
			name:             "missing IamRoleArn",
			recoveryPointArn: "arn:rp",
			iamRoleArn:       "",
			metadata:         map[string]string{"k": "v"},
		},
		{
			name:             "missing Metadata",
			recoveryPointArn: "arn:rp",
			iamRoleArn:       "arn:role",
			metadata:         nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := backup.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.StartRestoreJob(tc.recoveryPointArn, tc.iamRoleArn, "EBS", tc.metadata)
			require.ErrorIs(t, err, backup.ErrValidation)
		})
	}
}

func TestPutRestoreValidationResult(t *testing.T) {
	t.Parallel()

	t.Run("mutates the restore job record", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		job, err := b.StartRestoreJob("arn:rp", "arn:role", "EBS", map[string]string{"k": "v"})
		require.NoError(t, err)

		err = b.PutRestoreValidationResult(job.RestoreJobID, "SUCCESSFUL", "looks good")
		require.NoError(t, err)

		got, err := b.DescribeRestoreJob(job.RestoreJobID)
		require.NoError(t, err)
		assert.Equal(t, "SUCCESSFUL", got.ValidationStatus)
		assert.Equal(t, "looks good", got.ValidationStatusMessage)
	})

	t.Run("unknown restore job is not found", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		err := b.PutRestoreValidationResult("nonexistent", "SUCCESSFUL", "")
		require.ErrorIs(t, err, backup.ErrNotFound)
	})

	t.Run("missing fields are validation errors", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		require.ErrorIs(t, b.PutRestoreValidationResult("", "SUCCESSFUL", ""), backup.ErrValidation)
		require.ErrorIs(t, b.PutRestoreValidationResult("some-id", "", ""), backup.ErrValidation)
	})
}
