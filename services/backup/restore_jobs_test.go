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
		job := b2.StartRestoreJob(
			"arn:aws:backup:us-east-1:000000000000:recovery-point:test",
			"arn:aws:iam::000000000000:role/backup",
			"EBS",
			nil,
		)
		assert.NotEmpty(t, job.RestoreJobID)
		assert.Equal(t, "COMPLETED", job.Status)

		found, err := b2.DescribeRestoreJob(job.RestoreJobID)
		require.NoError(t, err)
		assert.Equal(t, job.RestoreJobID, found.RestoreJobID)
	})

	t.Run("list jobs", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.StartRestoreJob("arn:rp-1", "arn:role", "EBS", nil)
		b2.StartRestoreJob("arn:rp-2", "arn:role", "RDS", nil)
		jobs := b2.ListRestoreJobs()
		require.Len(t, jobs, 2)
	})

	t.Run("not found error", func(t *testing.T) {
		t.Parallel()
		_, err := b.DescribeRestoreJob("missing")
		require.Error(t, err)
	})
}
