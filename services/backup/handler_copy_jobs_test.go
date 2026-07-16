package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestStartCopyJobCreationDateEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "creation_date_is_epoch_seconds"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/src-vault", `{}`)

			resp := doBatch1Request(t, h, http.MethodPut, "/backup-jobs", `{
				"BackupVaultName": "src-vault",
				"ResourceArn": "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
				"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
			}`)
			require.Equal(t, http.StatusOK, resp.Code)
			var startData map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &startData))
			jobID := startData["BackupJobId"].(string)

			rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-copy-1"
			copyResp := doBatch1Request(t, h, http.MethodPut, "/copy-jobs", `{
				"RecoveryPointArn": "`+rpArn+`",
				"SourceBackupVaultName": "src-vault",
				"DestinationBackupVaultArn": "arn:aws:backup:us-east-1:000000000000:backup-vault:dst-vault",
				"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
			}`)
			require.Equal(t, http.StatusOK, copyResp.Code)
			_ = jobID

			var copyData map[string]any
			require.NoError(t, json.Unmarshal(copyResp.Body.Bytes(), &copyData))
			assert.NotEmpty(t, copyData["CopyJobId"], "CopyJobId must be present")

			creationDate, exists := copyData["CreationDate"]
			require.True(t, exists, "CreationDate must be present in StartCopyJob response")
			_, isFloat := creationDate.(float64)
			assert.True(t, isFloat,
				"CreationDate must be epoch seconds (float64), got %T: %v", creationDate, creationDate)
		})
	}
}

// ---- 2. DescribeGlobalSettings LastUpdateTime epoch seconds ----

func TestStartCopyJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	job := b.StartCopyJob("arn:rp", "arn:src-vault", "arn:dst-vault", "arn:role")
	assert.NotEmpty(t, job.CopyJobID)
	assert.Equal(t, "COMPLETED", job.State)

	summaries := b.ListCopyJobSummaries()
	assert.NotEmpty(t, summaries)
}

// TestStartCopyJobAndDescribeViaHTTP verifies that a copy job started via the
// backend is visible and correctly shaped when retrieved via the HTTP handler.
// This also confirms the CopyJobId is non-empty and the response wraps in "CopyJob".
func TestStartCopyJobAndDescribeViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "describe_copy_job_wraps_in_copy_job_key"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, be := newHandlerAndBackend()

			job := be.StartCopyJob(
				"arn:aws:backup:us-east-1:123456789012:recovery-point:rp-001",
				"src-vault",
				"arn:aws:backup:us-east-1:123456789012:backup-vault:dst-vault",
				"arn:aws:iam::123456789012:role/backup-role",
			)
			require.NotEmpty(t, job.CopyJobID)

			rec := doREST(t, h, http.MethodGet, "/copy-jobs/"+job.CopyJobID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)

			copyJobDoc, ok := resp["CopyJob"].(map[string]any)
			assert.True(t, ok, "response must have CopyJob wrapper key")
			if ok {
				assert.Equal(t, job.CopyJobID, copyJobDoc["CopyJobId"])
			}
		})
	}
}
