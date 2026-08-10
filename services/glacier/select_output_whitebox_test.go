package glacier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPersistenceRoundTrip_SelectOutputWritten verifies the SelectOutputWritten
// bookkeeping field (see models.go) survives a Snapshot/Restore round trip, so a
// restarted gopherstack never re-writes (and potentially duplicates) a select
// job's S3 output-location objects after already having written them once.
func TestPersistenceRoundTrip_SelectOutputWritten(t *testing.T) {
	t.Parallel()

	const (
		accountID = "000000000000"
		region    = "us-east-1"
		vaultName = "persist-select-output-vault"
		jobID     = "select-output-written-job"
	)

	b := NewInMemoryBackend()
	b.AddJobInternal(accountID, region, vaultName, &Job{
		JobID:               jobID,
		Action:              jobTypeSelect,
		Completed:           true,
		SelectOutputWritten: true,
		OutputLocation:      &outputLocationDTO{S3: &s3LocationDTO{BucketName: "bucket"}},
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	vArn := vaultARN(accountID, region, vaultName)
	j, ok := b2.jobs.Get(jobKey(vArn, jobID))
	require.True(t, ok, "job must survive restore")
	assert.True(t, j.SelectOutputWritten, "SelectOutputWritten must round-trip as true")
}

// TestPersistenceRoundTrip_SelectOutputNotWritten is the converse of the above:
// a select job that has NOT yet had its S3 output materialized must not
// spuriously become "already written" after a restore, which would silently
// suppress the real S3 write-back for that job forever.
func TestPersistenceRoundTrip_SelectOutputNotWritten(t *testing.T) {
	t.Parallel()

	const (
		accountID = "000000000000"
		region    = "us-east-1"
		vaultName = "persist-select-output-vault-2"
		jobID     = "select-output-unwritten-job"
	)

	b := NewInMemoryBackend()
	b.AddJobInternal(accountID, region, vaultName, &Job{
		JobID:               jobID,
		Action:              jobTypeSelect,
		Completed:           true,
		SelectOutputWritten: false,
		OutputLocation:      &outputLocationDTO{S3: &s3LocationDTO{BucketName: "bucket"}},
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	vArn := vaultARN(accountID, region, vaultName)
	j, ok := b2.jobs.Get(jobKey(vArn, jobID))
	require.True(t, ok, "job must survive restore")
	assert.False(t, j.SelectOutputWritten, "SelectOutputWritten must round-trip as false")
}
