package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestGlobalSettingsLastUpdateTimeEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "last_update_time_is_epoch_seconds"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			// Update settings so LastUpdateTime is set.
			doBatch1Request(t, h, http.MethodPut, "/global-settings",
				`{"GlobalSettings":{"isCrossAccountBackupEnabled":"true"}}`)

			resp := doBatch1Request(t, h, http.MethodGet, "/global-settings", "")
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			lastUpdate, exists := data["LastUpdateTime"]
			require.True(t, exists, "LastUpdateTime must be present")
			_, isFloat := lastUpdate.(float64)
			assert.True(t, isFloat,
				"LastUpdateTime must be epoch seconds (float64), got %T: %v", lastUpdate, lastUpdate)
		})
	}
}

// ---- 3. DescribeProtectedResource LastBackupTime epoch seconds ----

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
			h, b := newBatch1Handler(t)

			if tc.createJob {
				doBatch1Request(t, h, http.MethodPut, "/backup-vaults/pr-vault", `{}`)
				startResp := doBatch1Request(t, h, http.MethodPut, "/backup-jobs", `{
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

			resp := doBatch1Request(t, h, http.MethodGet, "/resources/"+tc.resourceID, "")

			if !tc.createJob {
				// A resource that was never backed up is not "protected" --
				// AWS returns ResourceNotFoundException, not a fabricated record.
				assert.Equal(t, http.StatusNotFound, resp.Code)

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

// ---- 4. UpdateBackupPlan UpdateDate in response ----

func TestGlobalSettings(t *testing.T) {
	t.Parallel()
	h, _ := newBatch1Handler(t)

	t.Run("describe returns empty initially", func(t *testing.T) {
		t.Parallel()
		h2, _ := newBatch1Handler(t)
		resp := doBatch1Request(t, h2, http.MethodGet, "/global-settings", "")
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "GlobalSettings")
	})

	t.Run("update and describe", func(t *testing.T) {
		t.Parallel()
		h2, _ := newBatch1Handler(t)
		_ = doBatch1Request(t, h2, http.MethodPut, "/global-settings",
			`{"GlobalSettings":{"isCrossAccountBackupEnabled":"true"}}`)
		resp := doBatch1Request(t, h2, http.MethodGet, "/global-settings", "")
		assert.Contains(t, resp.Body.String(), "isCrossAccountBackupEnabled")
	})
	_ = h
}

func TestRegionSettings(t *testing.T) {
	t.Parallel()
	h, _ := newBatch1Handler(t)

	t.Run("describe returns defaults", func(t *testing.T) {
		t.Parallel()
		h2, _ := newBatch1Handler(t)
		resp := doBatch1Request(t, h2, http.MethodGet, "/account-settings", "")
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "ResourceTypeOptInPreference")
	})
	_ = h
}

// ---- Backend unit tests ----

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

func TestProtectedResources(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	b.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-test", "EC2", "my-vault")

	pr, err := b.DescribeProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-test")
	require.NoError(t, err)
	assert.Equal(t, "EC2", pr.ResourceType)

	all := b.ListProtectedResources()
	require.Len(t, all, 1)

	byVault := b.ListProtectedResourcesByBackupVault("my-vault")
	require.Len(t, byVault, 1)
}

func TestTieringConfig(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	b.CreateBackupVault("tier-vault", "", "", nil)

	t.Run("create and get", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("tc-vault", "", "", nil)
		require.NoError(t, b2.CreateTieringConfiguration("tc-vault"))
		tc, err := b2.GetTieringConfiguration("tc-vault")
		require.NoError(t, err)
		assert.Equal(t, "tc-vault", tc.BackupVaultName)
	})

	t.Run("list configurations", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("v1", "", "", nil)
		b2.CreateBackupVault("v2", "", "", nil)
		_ = b2.CreateTieringConfiguration("v1")
		_ = b2.CreateTieringConfiguration("v2")
		tcs := b2.ListTieringConfigurations()
		require.Len(t, tcs, 2)
	})

	t.Run("delete removes config", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("del-vault", "", "", nil)
		_ = b2.CreateTieringConfiguration("del-vault")
		require.NoError(t, b2.DeleteTieringConfiguration("del-vault"))
		tcs := b2.ListTieringConfigurations()
		assert.Empty(t, tcs)
	})
	_ = b
}
