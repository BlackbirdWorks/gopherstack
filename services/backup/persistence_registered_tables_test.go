package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestRegisteredTablesSurviveRestore covers gopherstack-7au: recoveryPoints,
// copyJobs, vaultAccessPolicies, vaultLockConfigs, vaultNotifications,
// restoreJobs, reportJobs, scanJobs, tieringConfigs, and protectedResources
// were store.Table fields constructed via store.New but never registered on
// the backend's persisted registry, so they were silently dropped by every
// Snapshot/Restore round trip (i.e. lost across a gopherstack restart).
func TestRegisteredTablesSurviveRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "recovery_points",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "rp-vault")
				mustRP(t, b, vault.BackupVaultName, "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-1",
					"arn:aws:ec2:us-east-1:123456789012:volume/vol-1", "EC2")

				restored := restoreFresh(t, b)

				pts, err := restored.ListRecoveryPointsByBackupVault(vault.BackupVaultName)
				require.NoError(t, err)
				require.Len(t, pts, 1)
				assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:volume/vol-1", pts[0].ResourceArn)
			},
		},
		{
			name: "copy_jobs",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				src := mustVault(t, b, "copy-src")
				dst := mustVault(t, b, "copy-dst")
				mustRP(t, b, src.BackupVaultName, "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-copy",
					"arn:aws:ec2:us-east-1:123456789012:volume/vol-2", "EC2")

				job, err := b.StartCopyJob(
					"arn:aws:backup:us-east-1:123456789012:recovery-point:rp-copy",
					src.BackupVaultName, dst.BackupVaultArn, "arn:aws:iam::123456789012:role/r",
				)
				require.NoError(t, err)

				restored := restoreFresh(t, b)

				got, err := restored.DescribeCopyJob(job.CopyJobID)
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:volume/vol-2", got.ResourceArn)
			},
		},
		{
			name: "vault_access_policies",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "policy-vault")
				require.NoError(t, b.PutBackupVaultAccessPolicy(vault.BackupVaultName, `{"Version":"2012-10-17"}`))

				restored := restoreFresh(t, b)

				pol, err := restored.GetBackupVaultAccessPolicy(vault.BackupVaultName)
				require.NoError(t, err)
				assert.JSONEq(t, `{"Version":"2012-10-17"}`, pol.Policy)
			},
		},
		{
			name: "vault_lock_configs",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "lock-vault")
				require.NoError(t, b.PutBackupVaultLockConfiguration(vault.BackupVaultName, &backup.VaultLockConfig{
					MinRetentionDays: 7,
					MaxRetentionDays: 30,
				}))

				restored := restoreFresh(t, b)

				cfg, err := restored.GetBackupVaultLockConfig(vault.BackupVaultName)
				require.NoError(t, err)
				assert.Equal(t, int64(7), cfg.MinRetentionDays)
				assert.Equal(t, int64(30), cfg.MaxRetentionDays)
			},
		},
		{
			name: "vault_notifications",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "notif-vault")
				require.NoError(t, b.PutBackupVaultNotifications(vault.BackupVaultName, &backup.VaultNotificationConfig{
					SNSTopicArn:       "arn:aws:sns:us-east-1:123456789012:topic",
					BackupVaultEvents: []string{"BACKUP_JOB_STARTED"},
				}))

				restored := restoreFresh(t, b)

				cfg, err := restored.GetBackupVaultNotifications(vault.BackupVaultName)
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", cfg.SNSTopicArn)
				assert.Equal(t, []string{"BACKUP_JOB_STARTED"}, cfg.BackupVaultEvents)
			},
		},
		{
			name: "restore_jobs",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				job, err := b.StartRestoreJob(
					"arn:aws:backup:us-east-1:123456789012:recovery-point:rp-restore",
					"arn:aws:iam::123456789012:role/r", "EC2", map[string]string{"volumeType": "gp2"},
				)
				require.NoError(t, err)

				restored := restoreFresh(t, b)

				got, err := restored.DescribeRestoreJob(job.RestoreJobID)
				require.NoError(t, err)
				assert.Equal(t, "gp2", got.Metadata["volumeType"])
			},
		},
		{
			name: "report_jobs",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				_, err := b.CreateReportPlan("rp-plan", "", nil, nil)
				require.NoError(t, err)
				job := b.StartReportJob("rp-plan")

				restored := restoreFresh(t, b)

				jobs := restored.ListReportJobs("rp-plan")
				require.Len(t, jobs, 1)
				assert.Equal(t, job.ReportJobID, jobs[0].ReportJobID)
			},
		},
		{
			name: "scan_jobs",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "scan-vault")
				job := b.StartScanJob(vault.BackupVaultArn, backup.StartScanJobInput{
					IamRoleArn:       "arn:aws:iam::000000000000:role/ScanRole",
					MalwareScanner:   "GUARDDUTY",
					RecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:test",
					ScanMode:         "FULL_SCAN",
					ScannerRoleArn:   "arn:aws:iam::000000000000:role/ScannerRole",
				})

				restored := restoreFresh(t, b)

				got, err := restored.DescribeScanJob(job.ScanJobID)
				require.NoError(t, err)
				assert.Equal(t, vault.BackupVaultArn, got.BackupVaultArn)
			},
		},
		{
			name: "tiering_configs",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "tiering-vault")
				_, err := b.CreateTieringConfiguration("tiering_cfg", vault.BackupVaultName, []backup.ResourceSelection{
					{ResourceType: "EC2", Resources: []string{"*"}, TieringDownSettingsInDays: 90},
				}, "")
				require.NoError(t, err)

				restored := restoreFresh(t, b)

				got, err := restored.GetTieringConfiguration("tiering_cfg")
				require.NoError(t, err)
				assert.Equal(t, vault.BackupVaultName, got.BackupVaultName)
				require.Len(t, got.ResourceSelection, 1)
				assert.Equal(t, int64(90), got.ResourceSelection[0].TieringDownSettingsInDays)
			},
		},
		{
			name: "protected_resources",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "protected-vault")
				b.PutProtectedResource("arn:aws:ec2:us-east-1:123456789012:volume/vol-3", "EC2", vault.BackupVaultName)

				restored := restoreFresh(t, b)

				got, err := restored.DescribeProtectedResource("arn:aws:ec2:us-east-1:123456789012:volume/vol-3")
				require.NoError(t, err)
				assert.Equal(t, "EC2", got.ResourceType)
				assert.Equal(t, vault.BackupVaultName, got.BackupVaultName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// restoreFresh snapshots b and restores it into a brand-new backend, the
// same round trip a gopherstack restart performs.
func restoreFresh(t *testing.T, b *backup.InMemoryBackend) *backup.InMemoryBackend {
	t.Helper()

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := backup.NewInMemoryBackend(b.AccountID(), b.Region())
	require.NoError(t, restored.Restore(t.Context(), snap))

	return restored
}
