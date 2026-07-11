package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// Test_DeleteDBInstance_FinalSnapshotContract verifies AWS's
// SkipFinalSnapshot/FinalDBSnapshotIdentifier/DeleteAutomatedBackups contract
// for DeleteDBInstance: a final snapshot must be requested unless explicitly
// skipped, the two snapshot parameters are mutually exclusive, and a real
// manual snapshot is created and persisted before the instance is removed.
func Test_DeleteDBInstance_FinalSnapshotContract(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		finalSnapshotID   string
		wantErrContains   string
		skipFinalSnapshot bool
	}{
		{
			name:            "missing both SkipFinalSnapshot and FinalDBSnapshotIdentifier is rejected",
			wantErrContains: "FinalDBSnapshotIdentifier",
		},
		{
			name:              "SkipFinalSnapshot with FinalDBSnapshotIdentifier is rejected",
			skipFinalSnapshot: true,
			finalSnapshotID:   "final-snap",
			wantErrContains:   "FinalDBSnapshotIdentifier",
		},
		{
			name:              "SkipFinalSnapshot alone deletes without a snapshot",
			skipFinalSnapshot: true,
		},
		{
			name:            "FinalDBSnapshotIdentifier alone takes a final snapshot",
			finalSnapshotID: "final-snap",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDBInstance(
				"del-inst", "postgres", "db.t3.micro", "", "admin", "",
				20, rds.DBInstanceOptions{},
			)
			require.NoError(t, err)

			_, err = b.DeleteDBInstanceWithOptions("del-inst", tt.skipFinalSnapshot, tt.finalSnapshotID, true)

			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.Contains(t, err.Error(), "InvalidParameterCombination")
				// The instance must NOT have been deleted on a rejected request.
				_, describeErr := b.DescribeDBInstances("del-inst")
				assert.NoError(t, describeErr, "instance should still exist after a rejected delete")

				return
			}

			require.NoError(t, err)

			// Instance is gone either way.
			_, describeErr := b.DescribeDBInstances("del-inst")
			require.Error(t, describeErr, "instance should be removed after delete")

			snaps, _ := b.DescribeDBSnapshots("", "del-inst")
			if tt.finalSnapshotID == "" {
				assert.Empty(t, snaps, "no final snapshot should be created when SkipFinalSnapshot is set")

				return
			}

			require.Len(t, snaps, 1, "exactly one final snapshot should be created")
			snap := snaps[0]
			assert.Equal(t, tt.finalSnapshotID, snap.DBSnapshotIdentifier)
			assert.Equal(t, "del-inst", snap.DBInstanceIdentifier)
			assert.Equal(t, "manual", snap.SnapshotType)
			assert.Equal(t, "postgres", snap.Engine)
		})
	}
}

// Test_DeleteDBInstance_DeleteAutomatedBackups verifies that
// DeleteAutomatedBackups=false retains the instance's automated backup record
// after deletion, while the AWS default (true) removes it.
func Test_DeleteDBInstance_DeleteAutomatedBackups(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                   string
		deleteAutomatedBackups bool
		wantBackupsRemain      bool
	}{
		{name: "default true removes the automated backup", deleteAutomatedBackups: true},
		{name: "false retains the automated backup", deleteAutomatedBackups: false, wantBackupsRemain: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDBInstance(
				"del-inst-backup", "postgres", "db.t3.micro", "", "admin", "",
				20, rds.DBInstanceOptions{BackupRetentionPeriod: 7},
			)
			require.NoError(t, err)
			require.Len(t, b.DescribeDBInstanceAutomatedBackups("del-inst-backup"), 1)

			_, err = b.DeleteDBInstanceWithOptions("del-inst-backup", true, "", tt.deleteAutomatedBackups)
			require.NoError(t, err)

			backups := b.DescribeDBInstanceAutomatedBackups("del-inst-backup")
			if tt.wantBackupsRemain {
				assert.Len(t, backups, 1)
			} else {
				assert.Empty(t, backups)
			}
		})
	}
}

// Test_DeleteDBInstance_NotFoundBeforeParamValidation verifies that a
// nonexistent instance yields DBInstanceNotFound even when the
// SkipFinalSnapshot/FinalDBSnapshotIdentifier combination is also invalid —
// matching AWS's behavior of resolving the target resource first.
func Test_DeleteDBInstance_NotFoundBeforeParamValidation(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=missing-inst")

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
	assert.NotContains(t, rec.Body.String(), "InvalidParameterCombination")
}
