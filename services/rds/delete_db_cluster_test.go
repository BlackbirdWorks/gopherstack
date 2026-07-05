package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// Test_DeleteDBCluster_FinalSnapshotContract verifies AWS's
// SkipFinalSnapshot/FinalDBSnapshotIdentifier contract for DeleteDBCluster:
// a final snapshot must be requested unless explicitly skipped, the two
// parameters are mutually exclusive, and a real manual cluster snapshot is
// created and persisted before the cluster is removed.
func Test_DeleteDBCluster_FinalSnapshotContract(t *testing.T) {
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
			finalSnapshotID:   "final-csnap",
			wantErrContains:   "FinalDBSnapshotIdentifier",
		},
		{
			name:              "SkipFinalSnapshot alone deletes without a snapshot",
			skipFinalSnapshot: true,
		},
		{
			name:            "FinalDBSnapshotIdentifier alone takes a final snapshot",
			finalSnapshotID: "final-csnap",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDBCluster(
				"del-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{},
			)
			require.NoError(t, err)

			_, err = b.DeleteDBClusterWithOptions("del-cluster", tt.skipFinalSnapshot, tt.finalSnapshotID)

			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.Contains(t, err.Error(), "InvalidParameterCombination")
				// The cluster must NOT have been deleted on a rejected request.
				_, describeErr := b.DescribeDBClusters("del-cluster")
				assert.NoError(t, describeErr, "cluster should still exist after a rejected delete")

				return
			}

			require.NoError(t, err)

			_, describeErr := b.DescribeDBClusters("del-cluster")
			require.Error(t, describeErr, "cluster should be removed after delete")

			snaps, _ := b.DescribeDBClusterSnapshots("", "del-cluster")
			if tt.finalSnapshotID == "" {
				assert.Empty(t, snaps, "no final snapshot should be created when SkipFinalSnapshot is set")

				return
			}

			require.Len(t, snaps, 1, "exactly one final cluster snapshot should be created")
			snap := snaps[0]
			assert.Equal(t, tt.finalSnapshotID, snap.DBClusterSnapshotIdentifier)
			assert.Equal(t, "del-cluster", snap.DBClusterIdentifier)
			assert.Equal(t, "aurora-postgresql", snap.Engine)
		})
	}
}

// Test_DeleteDBCluster_NotFoundBeforeParamValidation verifies that a
// nonexistent cluster yields DBClusterNotFound even when the
// SkipFinalSnapshot/FinalDBSnapshotIdentifier combination is also invalid.
func Test_DeleteDBCluster_NotFoundBeforeParamValidation(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	rec := postRDSForm(t, h, "Action=DeleteDBCluster&Version=2014-10-31&DBClusterIdentifier=missing-cluster")

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBClusterNotFound")
	assert.NotContains(t, rec.Body.String(), "InvalidParameterCombination")
}
