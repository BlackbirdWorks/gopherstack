package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestStopBackupJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	b.CreateBackupVault("vault-stop", "", "", nil)
	job, err := b.StartBackupJob(
		"vault-stop",
		"arn:aws:ec2:us-east-1:000000000000:volume/vol-1",
		"arn:aws:iam::000000000000:role/r",
		"EBS",
	)
	require.NoError(t, err)

	require.NoError(t, b.StopBackupJob(job.BackupJobID))
}

func TestDescribeBackupJobProgressFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantField string
	}{
		{name: "has_job_id", wantField: "BackupJobId"},
		{name: "has_vault_name", wantField: "BackupVaultName"},
		{name: "has_state", wantField: "State"},
		{name: "has_creation_date", wantField: "CreationDate"},
	}

	h, _ := newHandler(t)
	doRequest(t, h, http.MethodPut, "/backup-vaults/job-vault", `{}`)

	startResp := doRequest(t, h, http.MethodPut, "/backup-jobs", `{
		"BackupVaultName": "job-vault",
		"ResourceArn": "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
		"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
	}`)
	require.Equal(t, http.StatusOK, startResp.Code)

	var startData map[string]any
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startData))
	jobID := startData["BackupJobId"].(string)

	descResp := doRequest(t, h, http.MethodGet, "/backup-jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, descResp.Code)

	var descData map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descData))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ok := descData[tt.wantField]
			assert.True(t, ok, "field %q should be present in DescribeBackupJob response", tt.wantField)
		})
	}
}

// ---- RecoveryPoint struct field tests (item 14) ----

// TestBackupJobCRUD exercises StartBackupJob, DescribeBackupJob, and ListBackupJobs.
func TestBackupJobCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "start_and_describe",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/myvault", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "myvault",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-abc",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				startResp := parseResp(t, rec)
				jobID, ok := startResp["BackupJobId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, jobID)

				rec2 := doREST(t, h, http.MethodGet, "/backup-jobs/"+jobID, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
				descResp := parseResp(t, rec2)
				assert.Equal(t, jobID, descResp["BackupJobId"])
				assert.Equal(t, "CREATED", descResp["State"])
			},
		},
		{
			name: "list_jobs",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/vault1", nil)
				doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "vault1",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-111",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "vault1",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-222",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				rec := doREST(t, h, http.MethodGet, "/backup-jobs", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				jobs, ok := resp["BackupJobs"].([]any)
				require.True(t, ok)
				assert.Len(t, jobs, 2)
			},
		},
		{
			name: "start_job_vault_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-jobs", map[string]any{
					"BackupVaultName": "missing",
					"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-abc",
					"IamRoleArn":      "arn:aws:iam::123456789012:role/role",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_job_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/backup-jobs/no-such-job", nil)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestStartBackupJobValidation covers required-field validation.
func TestStartBackupJobValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing_resource_arn",
			body: map[string]any{
				"BackupVaultName": "vault",
				"IamRoleArn":      "arn:aws:iam::123456789012:role/r",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_iam_role_arn",
			body: map[string]any{
				"BackupVaultName": "vault",
				"ResourceArn":     "arn:aws:ec2:us-east-1:123456789012:instance/i-1",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler()
			rec := doREST(t, h, http.MethodPut, "/backup-jobs", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
