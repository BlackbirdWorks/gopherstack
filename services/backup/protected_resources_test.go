package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestProtectedResourceLastBackupTimeEpoch verifies DescribeProtectedResource
// LastBackupTime is serialized as epoch seconds, matching real AWS behavior.
func TestProtectedResourceLastBackupTimeEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		createJob  bool
	}{
		{
			name:       "seeded_resource_returns_epoch",
			createJob:  true,
			resourceID: "arn:aws:ec2:us-east-1:000000000000:instance/i-pr-test",
		},
		{
			name:       "never_backed_up_resource_not_found",
			createJob:  false,
			resourceID: "arn:aws:ec2:us-east-1:000000000000:instance/i-unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, b := newHandler(t)

			if tc.createJob {
				doRequest(t, h, http.MethodPut, "/backup-vaults/pr-vault", `{}`)
				startResp := doRequest(t, h, http.MethodPut, "/backup-jobs", `{
					"BackupVaultName": "pr-vault",
					"ResourceArn": "`+tc.resourceID+`",
					"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
				}`)
				var startData map[string]any
				require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startData))
				// Real AWS Backup jobs complete asynchronously; the emulator
				// models that via the janitor. Complete it synchronously here
				// so the resource is recorded as protected.
				require.NoError(t, b.CompleteBackupJob(startData["BackupJobId"].(string)))
			}

			resp := doRequest(t, h, http.MethodGet, "/resources/"+tc.resourceID, "")

			if !tc.createJob {
				// A resource that was never backed up is not "protected" --
				// AWS returns ResourceNotFoundException, not a fabricated record.
				assert.Equal(t, http.StatusBadRequest, resp.Code)

				return
			}

			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			lastBackup, exists := data["LastBackupTime"]
			require.True(t, exists, "LastBackupTime must be present")
			_, isFloat := lastBackup.(float64)
			assert.True(t, isFloat,
				"LastBackupTime must be epoch seconds (float64), got %T: %v", lastBackup, lastBackup)
		})
	}
}

func TestProtectedResources(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	b.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-test", "EC2", "my-vault")

	pr, err := b.DescribeProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-test")
	require.NoError(t, err)
	assert.Equal(t, "EC2", pr.ResourceType)

	all, nextToken := b.ListProtectedResources(0, "")
	require.Len(t, all, 1)
	assert.Empty(t, nextToken)

	byVault, nextToken := b.ListProtectedResourcesByBackupVault("my-vault", 0, "")
	require.Len(t, byVault, 1)
	assert.Empty(t, nextToken)
}
