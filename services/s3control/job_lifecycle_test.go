package s3control_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestJobLifecycle_StatusAdvancesWithTime(t *testing.T) {
	t.Parallel()

	const acct = "000000000000"

	tests := []struct {
		name         string
		want         string
		confirmation bool
	}{
		{name: "no confirmation ready", confirmation: false, want: "Ready"},
		{name: "confirmation suspended", confirmation: true, want: "Suspended"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := s3control.NewInMemoryBackend()
				j, err := b.CreateJob(acct, "arn:aws:iam::000000000000:role/r", 1)
				require.NoError(t, err)
				require.NoError(t, b.UpdateJobDetails(acct, j.JobID, "", "", "", "", tt.confirmation))

				got, err := b.GetJob(acct, j.JobID)
				require.NoError(t, err)
				assert.Equal(t, "New", got.Status)

				time.Sleep(5 * time.Second)

				got, err = b.GetJob(acct, j.JobID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.Status)
			})
		})
	}
}

func TestUpdateJobStatus_TerminalJobRejected(t *testing.T) {
	t.Parallel()

	const acct = "000000000000"

	b := s3control.NewInMemoryBackend()
	j, err := b.CreateJob(acct, "arn:aws:iam::000000000000:role/r", 1)
	require.NoError(t, err)

	_, err = b.UpdateJobStatusValidated(acct, j.JobID, "Cancelled", "")
	require.NoError(t, err)

	_, err = b.UpdateJobStatusValidated(acct, j.JobID, "Ready", "")
	require.Error(t, err)
	assert.Equal(t, "JobStatusException", err.Error())
}
