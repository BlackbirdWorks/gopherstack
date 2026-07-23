package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestBackupPersistence exercises Snapshot and Restore.
func TestBackupPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "snapshot_restore",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestBackupHandler()
				doREST(t, h, http.MethodPut, "/backup-vaults/snap-vault", nil)

				snap := h.Snapshot(t.Context())
				require.NotNil(t, snap)

				h2 := newTestBackupHandler()
				require.NoError(t, h2.Restore(t.Context(), snap))

				rec := doREST(t, h2, http.MethodGet, "/backup-vaults/snap-vault", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "restore_rebuilds_arn_index_for_vault",
			run: func(t *testing.T) {
				t.Helper()
				b := backup.NewInMemoryBackend("000000000000", "us-east-1")
				vault, err := b.CreateBackupVault("my-vault", "", "", nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(t.Context(), snap))

				// Tag by ARN must succeed using the rebuilt index.
				err = fresh.TagResource(vault.BackupVaultArn, map[string]string{"env": "prod"})
				require.NoError(t, err)

				kv, err := fresh.ListTags(vault.BackupVaultArn)
				require.NoError(t, err)
				assert.Equal(t, "prod", kv["env"])
			},
		},
		{
			name: "restore_rebuilds_arn_index_for_plan",
			run: func(t *testing.T) {
				t.Helper()
				b := backup.NewInMemoryBackend("000000000000", "us-east-1")
				plan, err := b.CreateBackupPlan("my-plan", nil, nil, nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(t.Context(), snap))

				// Tag by ARN must succeed using the rebuilt index.
				err = fresh.TagResource(plan.BackupPlanArn, map[string]string{"team": "ops"})
				require.NoError(t, err)

				kv, err := fresh.ListTags(plan.BackupPlanArn)
				require.NoError(t, err)
				assert.Equal(t, "ops", kv["team"])
			},
		},
		{
			name: "restore_rebuilds_plan_id_index",
			run: func(t *testing.T) {
				t.Helper()
				b := backup.NewInMemoryBackend("000000000000", "us-east-1")
				plan, err := b.CreateBackupPlan("id-plan", nil, nil, nil)
				require.NoError(t, err)

				snap := b.Snapshot(t.Context())
				require.NotNil(t, snap)

				fresh := backup.NewInMemoryBackend("000000000000", "us-east-1")
				require.NoError(t, fresh.Restore(t.Context(), snap))

				// GetBackupPlan by plan ID must succeed using the rebuilt planIDIndex.
				got, err := fresh.GetBackupPlan(plan.BackupPlanID)
				require.NoError(t, err)
				assert.Equal(t, plan.BackupPlanName, got.BackupPlanName)
			},
		},
		{
			name: "restore_invalid_json",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestBackupHandler()
				err := h.Restore(t.Context(), []byte("not-json"))
				require.Error(t, err)
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

// TestPersistenceRoundTrip exercises Snapshot/Restore with all new resource types.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	original := backup.NewInMemoryBackend("123456789012", "us-east-1")

	// Populate original with all new resource types.
	_, err := original.CreateBackupVault("persist-vault", "", "", nil)
	require.NoError(t, err)

	_, err = original.CreateFramework("persist-fw", "test framework", nil)
	require.NoError(t, err)

	_, err = original.CreateLegalHold("hold title", "hold description", nil)
	require.NoError(t, err)

	_, err = original.CreateReportPlan("persist-rp", "test report", nil, nil)
	require.NoError(t, err)

	_, err = original.CreateRestoreTestingPlan("persist-rtp", "cron(0 12 * * ? *)", 0)
	require.NoError(t, err)

	_, err = original.CreateRestoreTestingSelection(
		"persist-rtp",
		"persist-sel",
		backup.RestoreTestingSelectionInput{
			ProtectedResourceType: "EC2",
			IAMRoleArn:            "arn:aws:iam::123456789012:role/restore-role",
		},
	)
	require.NoError(t, err)

	_, err = original.CreateLogicallyAirGappedBackupVault("persist-air", "", 1, 30, nil)
	require.NoError(t, err)

	err = original.AssociateBackupVaultMpaApprovalTeam("persist-vault", "arn:aws:mpa::123456789012:team/t")
	require.NoError(t, err)

	// Create a plan and selection.
	plan, err := original.CreateBackupPlan("persist-plan", nil, nil, nil)
	require.NoError(t, err)

	_, err = original.CreateBackupSelection(
		plan.BackupPlanID,
		"selection-1",
		"arn:aws:iam::123456789012:role/r",
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Create a restore access vault, sourced from the air-gapped vault created above.
	_, err = original.CreateRestoreAccessBackupVault(
		"arn:aws:backup:us-east-1:123456789012:backup-vault:persist-air",
		"persist-rav",
		"",
		nil,
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend.
	restored := backup.NewInMemoryBackend("", "")
	require.NoError(t, restored.Restore(t.Context(), snap))

	// Verify vaults.
	vault, err := restored.DescribeBackupVault("persist-vault")
	require.NoError(t, err)
	assert.Equal(t, "persist-vault", vault.BackupVaultName)

	// Verify frameworks.
	fw, err := restored.CreateFramework("persist-fw-2", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, fw.FrameworkArn)
	// Original framework should still conflict.
	_, err = restored.CreateFramework("persist-fw", "", nil)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	// Verify legal holds are restored.
	_, err = restored.CreateLegalHold("new title", "desc", nil)
	require.NoError(t, err)

	// Verify report plans.
	_, err = restored.CreateReportPlan("persist-rp", "dup", nil, nil)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	// Verify restore testing plans and selections.
	_, err = restored.CreateRestoreTestingPlan("persist-rtp", "", 0)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	_, err = restored.CreateRestoreTestingSelection(
		"persist-rtp",
		"persist-sel",
		backup.RestoreTestingSelectionInput{
			ProtectedResourceType: "EC2",
			IAMRoleArn:            "arn:aws:iam::123456789012:role/restore-role",
		},
	)
	require.ErrorIs(t, err, backup.ErrAlreadyExists)

	// Verify the restore testing selection's own fields (not just the
	// duplicate-create conflict above) round-tripped through the
	// composite-key store.Table + secondary store.Index that replaced the
	// old map[string]map[string]*RestoreTestingSelection.
	rtSels, err := restored.ListRestoreTestingSelections("persist-rtp")
	require.NoError(t, err)
	require.Len(t, rtSels, 1)
	assert.Equal(t, "persist-sel", rtSels[0].RestoreTestingSelectionName)
	assert.Equal(t, "EC2", rtSels[0].ProtectedResourceType)

	// Verify plan selection.
	_, err = restored.GetBackupPlan(plan.BackupPlanID)
	require.NoError(t, err)

	// Verify the backup selection's own fields round-tripped through the
	// composite-key store.Table + secondary store.Index that replaced the
	// old map[string]map[string]*Selection.
	sels, err := restored.ListBackupSelections(plan.BackupPlanID)
	require.NoError(t, err)
	require.Len(t, sels, 1)
	assert.Equal(t, "selection-1", sels[0].SelectionName)
	assert.Equal(t, plan.BackupPlanID, sels[0].BackupPlanID)
}
