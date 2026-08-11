package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackendStateSurvivesRestore covers gopherstack-y5b4: mpaApprovals,
// globalSettings, recoveryPointIndexStatus, and regionSettings held real
// user state but lived in plain fields never wired into backendSnapshot, so
// they were silently lost across a restart.
func TestBackendStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "global_settings",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				b.UpdateGlobalSettings(map[string]string{"isCrossAccountBackupEnabled": "true"})
				_, wantUpdated := b.DescribeGlobalSettings()

				restored := restoreFresh(t, b)

				got, gotUpdated := restored.DescribeGlobalSettings()
				assert.Equal(t, "true", got["isCrossAccountBackupEnabled"])
				assert.True(t, wantUpdated.Equal(gotUpdated))
			},
		},
		{
			name: "region_settings",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				b.UpdateRegionSettings(
					map[string]bool{"EC2": true},
					map[string]bool{"EC2": true},
				)

				restored := restoreFresh(t, b)

				got := restored.DescribeRegionSettings()
				assert.True(t, got.ResourceTypeManagementPreference["EC2"])
				assert.True(t, got.ResourceTypeOptInPreference["EC2"])
			},
		},
		{
			name: "recovery_point_index_status",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "idx-vault")
				rpArn := "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-idx"
				mustRP(t, b, vault.BackupVaultName, rpArn,
					"arn:aws:ec2:us-east-1:123456789012:volume/vol-9", "EC2")
				require.NoError(t, b.UpdateRecoveryPointIndexSettings(vault.BackupVaultName, rpArn, "DISABLED"))

				restored := restoreFresh(t, b)

				status, err := restored.GetRecoveryPointIndexDetails(vault.BackupVaultName, rpArn)
				require.NoError(t, err)
				assert.Equal(t, "DISABLED", status)
			},
		},
		{
			name: "mpa_approvals",
			run: func(t *testing.T) {
				t.Helper()
				b := newTestBackend(t)
				vault := mustVault(t, b, "mpa-vault")
				require.NoError(t, b.AssociateBackupVaultMpaApprovalTeam(
					vault.BackupVaultName, "arn:aws:mpa::123456789012:team/t"))

				restored := restoreFresh(t, b)

				arn, ok := restored.GetVaultMpaApprovalTeamArn(vault.BackupVaultName)
				require.True(t, ok)
				assert.Equal(t, "arn:aws:mpa::123456789012:team/t", arn)
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

// TestVaultReachableByARNAfterRestore covers gopherstack-y5b4's "sharper
// bug" question: are the derived ARN/ID indexes rebuilt during restore?
// b.rebuildARNIndexes (persistence.go) already runs at the end of Restore,
// so this passes on unmodified code -- it is a regression lock, not a fix.
func TestVaultReachableByARNAfterRestore(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	vault := mustVault(t, b, "arn-reachable-vault")

	restored := restoreFresh(t, b)

	require.NoError(t, restored.TagResource(vault.BackupVaultArn, map[string]string{"k": "v"}))

	got, err := restored.ListTags(vault.BackupVaultArn)
	require.NoError(t, err)
	assert.Equal(t, "v", got["k"])
}
