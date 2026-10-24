package securityhub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedExportJob(b *InMemoryBackend, id, status string) {
	b.exportJobsV2.Put(&ExportJobV2{
		ExportJobID: id, Status: status, DataType: exportDataTypeFindings,
		StartedAt: "2026-01-01T00:00:00Z", Format: exportFormatCSV,
	})
}

func TestExportJobsV2_RunningJob(t *testing.T) {
	t.Parallel()

	start := &ExportJobV2Input{
		BucketArn: "arn:aws:s3:::b", KmsKeyArn: "arn:aws:kms:us-east-1:000000000000:key/1234abcd",
		Format: exportFormatOCSFJSON,
	}

	tests := []struct {
		wantErr    error
		name       string
		status     string
		wantStatus string
	}{
		{name: "running cancels", status: exportStatusRunning, wantStatus: exportStatusCancelled},
		{name: "cancelled is idempotent", status: exportStatusCancelled, wantStatus: exportStatusCancelled},
		{name: "succeeded conflicts", status: exportStatusSucceeded, wantErr: ErrExportJobFinished},
		{name: "failed conflicts", status: exportStatusFailed, wantErr: ErrExportJobFinished},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			seedExportJob(b, "job-1", tt.status)

			got, err := b.CancelExportJobV2("job-1")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
		})
	}

	t.Run("running job blocks start", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("000000000000", "us-east-1")
		seedExportJob(b, "job-1", exportStatusRunning)

		_, err := b.StartExportJobV2(start)
		require.ErrorIs(t, err, ErrExportJobRunning)

		_, err = b.CancelExportJobV2("job-1")
		require.NoError(t, err)

		started, err := b.StartExportJobV2(start)
		require.NoError(t, err)
		assert.Equal(t, exportStatusFailed, started.Status, "no S3 wired")
	})
}
