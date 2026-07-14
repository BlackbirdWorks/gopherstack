package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch2_DBCluster_ModifyFields(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"mod-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	updated, err := b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		DeletionProtection:    true,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	assert.True(t, updated.DeletionProtection)

	updated, err = b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		DeletionProtection:    false,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	assert.False(t, updated.DeletionProtection)
}

func TestBatch2_DBCluster_FailoverUpdatesState(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"failover-cluster",
		"aurora-mysql",
		"admin",
		"",
		"",
		3306,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.FailoverDBCluster("failover-cluster", "")
	require.NoError(t, err)
	assert.Equal(t, "failover-cluster", result.DBClusterIdentifier)
}

func TestBatch2_DBCluster_RebootTransitions(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBCluster(
		"reboot-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.RebootDBCluster("reboot-cluster")
	require.NoError(t, err)
	assert.Equal(t, "reboot-cluster", result.DBClusterIdentifier)
}

func TestBatch2_DBCluster_HTTP_Modify(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"http-mod-cluster"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"http-mod-cluster"},
		"DeletionProtection":  {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"http-mod-cluster"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

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

// TestRefinement1_DeleteDBClusterCascadeClusterRoles verifies that deleting a cluster
// also removes its IAM role associations.
func TestRefinement1_DeleteDBClusterCascadeClusterRoles(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("my-cluster", "aurora-mysql")

	err := b.AddRoleToDBCluster("my-cluster", "arn:aws:iam::000:role/R1")
	require.NoError(t, err)
	require.Equal(t, 1, rds.ClusterRoleCount(b, "my-cluster"))

	_, err = b.DeleteDBCluster("my-cluster")
	require.NoError(t, err)

	assert.Equal(t, 0, rds.ClusterRoleCount(b, "my-cluster"))
}

// TestRefinement1_BacktrackDBCluster_UniqueIDs verifies each call returns a different BacktrackIdentifier.
func TestRefinement1_BacktrackDBCluster_UniqueIDs(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("aurora-cluster", "aurora-mysql")

	bt1, err := b.BacktrackDBCluster("aurora-cluster", "2024-01-01T00:00:00Z")
	require.NoError(t, err)

	bt2, err := b.BacktrackDBCluster("aurora-cluster", "2024-01-02T00:00:00Z")
	require.NoError(t, err)

	assert.NotEqual(t, bt1.BacktrackIdentifier, bt2.BacktrackIdentifier,
		"each BacktrackDBCluster call should return a unique BacktrackIdentifier")
	assert.True(t, strings.HasPrefix(bt1.BacktrackIdentifier, "backtrack-"))
}

// TestRefinement1_BacktrackDBCluster_EmptyBacktrackTo validates that an empty BacktrackTo returns error.
func TestRefinement1_BacktrackDBCluster_EmptyBacktrackTo(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("aurora-cluster", "aurora-mysql")

	_, err := b.BacktrackDBCluster("aurora-cluster", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrInvalidParameter)
}

// TestRefinement1_HTTP_BacktrackDBCluster_EmptyBacktrackTo validates HTTP error for missing BacktrackTo.
func TestRefinement1_HTTP_BacktrackDBCluster_EmptyBacktrackTo(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("aurora-cluster", "aurora-mysql")
	h := rds.NewHandler(b)

	rec := postRDSForm(t, h, "Action=BacktrackDBCluster&Version=2014-10-31&DBClusterIdentifier=aurora-cluster")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// TestRDSBackend_BacktrackDBCluster tests BacktrackDBCluster.
func TestRDSBackend_BacktrackDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		clusterID   string
		backtrackTo string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
			},
			clusterID:   "my-cluster",
			backtrackTo: "2024-01-01T00:00:00Z",
		},
		{
			name:        "cluster_not_found",
			setup:       func(_ *rds.InMemoryBackend) {},
			clusterID:   "no-such-cluster",
			backtrackTo: "2024-01-01T00:00:00Z",
			wantErr:     true,
			wantErrIs:   rds.ErrClusterNotFound,
		},
		{
			name:        "empty_cluster_id",
			setup:       func(_ *rds.InMemoryBackend) {},
			clusterID:   "",
			backtrackTo: "2024-01-01T00:00:00Z",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			bt, err := b.BacktrackDBCluster(tt.clusterID, tt.backtrackTo)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, bt.DBClusterIdentifier)
			assert.Equal(t, "applying", bt.Status)
		})
	}
}

// TestRDSBackend_DeletionProtection tests that resources with deletion protection cannot be deleted.
func TestRDSBackend_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(b *rds.InMemoryBackend)
		action  func(b *rds.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name: "delete_protected_instance_blocked",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"prot-db", "postgres", "", "", "", "", 20,
					rds.DBInstanceOptions{DeletionProtection: true},
				)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteDBInstance("prot-db")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrInvalidDBInstanceState,
		},
		{
			name: "delete_unprotected_instance_allowed",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"unprot-db", "postgres", "", "", "", "", 20,
					rds.DBInstanceOptions{DeletionProtection: false},
				)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteDBInstance("unprot-db")

				return err
			},
			wantErr: false,
		},
		{
			name: "delete_protected_global_cluster_blocked",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc-prot", "aurora-postgresql", "14.3", false, true)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteGlobalCluster("gc-prot")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrInvalidGlobalClusterState,
		},
		{
			name: "delete_unprotected_global_cluster_allowed",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc-unprot", "aurora-postgresql", "14.3", false, false)
			},
			action: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteGlobalCluster("gc-unprot")

				return err
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")

			if tt.setup != nil {
				tt.setup(b)
			}

			err := tt.action(b)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestParity_CreateDBCluster_BackupRetentionPeriodBounds verifies that
// CreateDBCluster validates BackupRetentionPeriod within the AWS-allowed
// range [1, 35]. Real AWS returns InvalidParameterValue for out-of-range values
// and defaults to 1 when the parameter is omitted.
func TestParity_CreateDBCluster_BackupRetentionPeriodBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retention string
		wantCode  int
	}{
		{
			name:      "zero_rejected",
			retention: "0",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "above_maximum_rejected",
			retention: "36",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "minimum_boundary_accepted",
			retention: "1",
			wantCode:  http.StatusOK,
		},
		{
			name:      "maximum_boundary_accepted",
			retention: "35",
			wantCode:  http.StatusOK,
		},
		{
			name:      "mid_range_accepted",
			retention: "7",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			body := "Action=CreateDBCluster" +
				"&DBClusterIdentifier=test-cluster-" + tt.name +
				"&Engine=aurora-mysql" +
				"&BackupRetentionPeriod=" + tt.retention

			rec := postRDSForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateDBCluster BackupRetentionPeriod=%s", tt.retention)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "Error",
					"expected error response for BackupRetentionPeriod=%s", tt.retention)
			}
		})
	}
}

// TestParity_CreateDBCluster_BackupRetentionPeriodDefault verifies that
// CreateDBCluster defaults BackupRetentionPeriod to 1 when omitted.
// Real AWS documents this default and includes it in DescribeDBClusters output.
func TestParity_CreateDBCluster_BackupRetentionPeriodDefault(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	body := "Action=CreateDBCluster" +
		"&DBClusterIdentifier=default-retention-cluster" +
		"&Engine=aurora-postgresql"

	rec := postRDSForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<BackupRetentionPeriod>1</BackupRetentionPeriod>",
		"default BackupRetentionPeriod should be 1")
}

// TestParity_CreateDBCluster_BackupRetentionPeriodPersisted verifies that an
// explicitly set BackupRetentionPeriod is round-tripped through DescribeDBClusters.
func TestParity_CreateDBCluster_BackupRetentionPeriodPersisted(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	createBody := "Action=CreateDBCluster" +
		"&DBClusterIdentifier=ret-cluster" +
		"&Engine=aurora-mysql" +
		"&BackupRetentionPeriod=14"

	createRec := postRDSForm(t, h, createBody)
	require.Equal(t, http.StatusOK, createRec.Code)

	describeBody := "Action=DescribeDBClusters&DBClusterIdentifier=ret-cluster"
	describeRec := postRDSForm(t, h, describeBody)
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<BackupRetentionPeriod>14</BackupRetentionPeriod>",
		"BackupRetentionPeriod=14 should be returned by DescribeDBClusters")
}

func TestFailoverDBCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{name: "success", clusterID: "my-cluster"},
		{name: "not found", clusterID: "missing", wantErr: true, wantErrIs: rds.ErrClusterNotFound},
		{
			name:      "wrong status",
			clusterID: "stopped-cluster",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidDBClusterStateFault,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "success" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			if tt.name == "wrong status" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
				_, err = b.StopDBCluster(tt.clusterID)
				require.NoError(t, err)
			}
			got, err := b.FailoverDBCluster(tt.clusterID, "")
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
		})
	}
}

func TestRebootDBCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{name: "success", clusterID: "my-cluster"},
		{name: "not found", clusterID: "missing", wantErr: true, wantErrIs: rds.ErrClusterNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.RebootDBCluster(tt.clusterID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
		})
	}
}

func TestPromoteReadReplicaDBCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{
			name:      "success",
			clusterID: "my-cluster",
		},
		{
			name:      "not found",
			clusterID: "missing",
			wantErr:   true,
			wantErrIs: rds.ErrClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.PromoteReadReplicaDBCluster(tt.clusterID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
			assert.Equal(t, "available", got.Status)
		})
	}
}

func TestDescribeDBClusterBacktracks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{name: "success returns empty", clusterID: "my-cluster"},
		{name: "not found", clusterID: "missing", wantErr: true, wantErrIs: rds.ErrClusterNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.DescribeDBClusterBacktracks(tt.clusterID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, got)
		})
	}
}

func TestModifyCurrentDBClusterCapacity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		capacity  int
		wantErr   bool
	}{
		{name: "success", clusterID: "my-cluster", capacity: 8},
		{name: "not found", clusterID: "missing", capacity: 8, wantErr: true, wantErrIs: rds.ErrClusterNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.ModifyCurrentDBClusterCapacity(tt.clusterID, tt.capacity)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.capacity, got.ServerlessCapacity)
		})
	}
}

func TestRestoreDBClusterFromS3(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs      error
		name           string
		clusterID      string
		engine         string
		masterUsername string
		s3Bucket       string
		wantErr        bool
	}{
		{
			name:           "success",
			clusterID:      "restored-cluster",
			engine:         "aurora-mysql",
			masterUsername: "admin",
			s3Bucket:       "my-backup-bucket",
		},
		{
			name:      "empty bucket",
			clusterID: "restored-cluster",
			engine:    "aurora-mysql",
			s3Bucket:  "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty id",
			clusterID: "",
			engine:    "aurora-mysql",
			s3Bucket:  "my-bucket",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "already exists",
			clusterID: "existing-cluster",
			engine:    "aurora-mysql",
			s3Bucket:  "my-bucket",
			wantErr:   true,
			wantErrIs: rds.ErrClusterAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					tt.engine,
					tt.masterUsername,
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.RestoreDBClusterFromS3(tt.clusterID, tt.engine, tt.masterUsername, tt.s3Bucket)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
			assert.Equal(t, tt.engine, got.Engine)
		})
	}
}

// TestClusterCreateTime verifies ClusterCreateTime is set on CreateDBCluster.
func TestClusterCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	before := time.Now().UTC()
	c, err := b.CreateDBCluster("cluster-ts", "aurora-postgresql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, c.ClusterCreateTime.IsZero(), "ClusterCreateTime should be set")
	assert.False(t, c.ClusterCreateTime.Before(before))
	assert.False(t, c.ClusterCreateTime.After(after))
}

// TestDescribeDBClustersSorted verifies deterministic sort order.
func TestDescribeDBClustersSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, id := range []string{"cluster-z", "cluster-a", "cluster-m"} {
		_, err := b.CreateDBCluster(id, "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeDBClusters("")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "cluster-a", got[0].DBClusterIdentifier)
	assert.Equal(t, "cluster-m", got[1].DBClusterIdentifier)
	assert.Equal(t, "cluster-z", got[2].DBClusterIdentifier)
}

// TestDeleteDBClusterClearsMemberInstances verifies that deleting a cluster clears member DBClusterIdentifier.
func TestDeleteDBClusterClearsMemberInstances(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("cluster-del", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBInstance("member-inst", "aurora-postgresql", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{DBClusterIdentifier: "cluster-del"})
	require.NoError(t, err)
	// Instance should be a member.
	inst, err := b.DescribeDBInstances("member-inst")
	require.NoError(t, err)
	assert.Equal(t, "cluster-del", inst[0].DBClusterIdentifier)
	// Delete cluster.
	_, err = b.DeleteDBCluster("cluster-del")
	require.NoError(t, err)
	// Instance's cluster identifier should be cleared.
	inst, err = b.DescribeDBInstances("member-inst")
	require.NoError(t, err)
	assert.Empty(
		t, inst[0].DBClusterIdentifier, "member instance DBClusterIdentifier should be cleared on cluster delete",
	)
}

// TestRebootDBClusterTransitionsState verifies RebootDBCluster requires available state and transitions to rebooting.
func TestRebootDBClusterTransitionsState(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("cluster-reboot", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	// Rebooting an available cluster should return it in "rebooting" status.
	got, err := b.RebootDBCluster("cluster-reboot")
	require.NoError(t, err)
	assert.Equal(t, "rebooting", got.Status)
}

// TestRebootDBClusterNotFound verifies error when cluster does not exist.
func TestRebootDBClusterNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.RebootDBCluster("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrClusterNotFound)
}

// TestDeletionProtectionCanBeDisabled verifies that ModifyDBInstance can disable DeletionProtection.
func TestDeletionProtectionCanBeDisabled(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-dp", "postgres", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{DeletionProtection: true})
	require.NoError(t, err)
	inst, err := b.DescribeDBInstances("db-dp")
	require.NoError(t, err)
	require.True(t, inst[0].DeletionProtection)
	// Disable deletion protection via ModifyDBInstance with DeletionProtectionSet.
	_, err = b.ModifyDBInstance("db-dp", "", 0, rds.DBInstanceOptions{
		DeletionProtection:    false,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	inst, err = b.DescribeDBInstances("db-dp")
	require.NoError(t, err)
	assert.False(t, inst[0].DeletionProtection, "DeletionProtection should be disabled after modify")
}

// instanceTransitionDelay in the backend; mirror as a local lower bound for
// timing assertions. The backend uses 250ms.
const transitionDelay = 250 * time.Millisecond

// TestRebootDBClusterDelayedTransition exercises the delayed lifecycle goroutine
// scheduled by RebootDBCluster via runDelayed. It verifies both that the
// transition still fires after the delay and that Close cancels in-flight
// transitions promptly without mutating state after shutdown (the leak fix).
func TestRebootDBClusterDelayedTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantStatus    string
		closeEarly    bool
		wantFastClose bool
	}{
		{
			name:       "transition fires after delay",
			closeEarly: false,
			wantStatus: "available",
		},
		{
			name:          "close cancels in-flight transition",
			closeEarly:    true,
			wantStatus:    "rebooting",
			wantFastClose: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("123456789012", "us-east-1")

			_, err := b.CreateDBCluster(
				"my-cluster",
				"aurora-mysql",
				"admin",
				"",
				"",
				0,
				nil,
				rds.DBClusterOptions{},
			)
			require.NoError(t, err)

			_, err = b.RebootDBCluster("my-cluster")
			require.NoError(t, err)

			if tt.closeEarly {
				// Close immediately, before the transition delay elapses. The
				// delayed goroutine must observe stopCh and return without
				// mutating state. Close must block only briefly on b.wg.Wait().
				start := time.Now()
				b.Close()
				elapsed := time.Since(start)

				if tt.wantFastClose {
					require.Less(t, elapsed, transitionDelay,
						"Close should not wait out the full transition delay")
				}

				clusters, derr := b.DescribeDBClusters("my-cluster")
				require.NoError(t, derr)
				require.Equal(t, tt.wantStatus, clusters[0].Status)

				return
			}

			// Wait for the delayed transition to fire, then verify the status
			// and a clean Close afterward.
			require.Eventually(t, func() bool {
				clusters, derr := b.DescribeDBClusters("my-cluster")
				if derr != nil || len(clusters) == 0 {
					return false
				}

				return clusters[0].Status == tt.wantStatus
			}, 2*time.Second, 10*time.Millisecond)

			b.Close()
		})
	}
}

// TestDBClusterMembersEmitted verifies DBClusterMembers are populated and returned.
func TestDBClusterMembersEmitted(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBCluster("test-cluster", "aurora-postgresql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{
		EngineVersion:              "15.4",
		BacktrackWindow:            86400,
		PreferredBackupWindow:      "02:00-03:00",
		PreferredMaintenanceWindow: "sun:04:00-sun:05:00",
		MultiAZ:                    true,
		CopyTagsToSnapshot:         true,
	})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("test-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	c := clusters[0]
	assert.Equal(t, "15.4", c.EngineVersion)
	assert.Equal(t, int64(86400), c.BacktrackWindow)
	assert.Equal(t, "02:00-03:00", c.PreferredBackupWindow)
	assert.Equal(t, "sun:04:00-sun:05:00", c.PreferredMaintenanceWindow)
	assert.True(t, c.MultiAZ)
	assert.True(t, c.CopyTagsToSnapshot)
}

// TestModifyDBClusterPersistsNewFields verifies ModifyDBCluster persists the new fields.
func TestModifyDBClusterPersistsNewFields(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBCluster("mod-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	_, err = b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		EngineVersion:              "15.5",
		BacktrackWindow:            3600,
		PreferredBackupWindow:      "03:00-04:00",
		PreferredMaintenanceWindow: "wed:05:00-wed:06:00",
		MultiAZ:                    true,
	})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("mod-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	c := clusters[0]
	assert.Equal(t, "15.5", c.EngineVersion)
	assert.Equal(t, int64(3600), c.BacktrackWindow)
	assert.Equal(t, "03:00-04:00", c.PreferredBackupWindow)
	assert.True(t, c.MultiAZ)
}

// TestCreateDBClusterViaHandler verifies the handler passes new cluster opts.
func TestCreateDBClusterViaHandler(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"handler-cluster"},
		"Engine":                     {"aurora-postgresql"},
		"MasterUsername":             {"admin"},
		"EngineVersion":              {"15.4"},
		"BacktrackWindow":            {"86400"},
		"PreferredBackupWindow":      {"01:00-02:00"},
		"PreferredMaintenanceWindow": {"sat:03:00-sat:04:00"},
		"MultiAZ":                    {"true"},
		"StorageEncrypted":           {"true"},
		"CopyTagsToSnapshot":         {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			DBCluster struct {
				EngineVersion              string `xml:"EngineVersion"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
				BacktrackWindow            int64  `xml:"BacktrackWindow"`
				MultiAZ                    bool   `xml:"MultiAZ"`
				StorageEncrypted           bool   `xml:"StorageEncrypted"`
				CopyTagsToSnapshot         bool   `xml:"CopyTagsToSnapshot"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	c := resp.Result.DBCluster
	assert.Equal(t, "15.4", c.EngineVersion)
	assert.Equal(t, int64(86400), c.BacktrackWindow)
	assert.Equal(t, "01:00-02:00", c.PreferredBackupWindow)
	assert.True(t, c.MultiAZ)
	assert.True(t, c.StorageEncrypted)
	assert.True(t, c.CopyTagsToSnapshot)
}

// TestModifyDBClusterViaHandler verifies ModifyDBCluster handler passes new fields.
func TestModifyDBClusterViaHandler(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create cluster first.
	createRec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modify-handler-cluster"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Modify it.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"ModifyDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"modify-handler-cluster"},
		"EngineVersion":              {"15.5"},
		"PreferredBackupWindow":      {"04:00-05:00"},
		"PreferredMaintenanceWindow": {"fri:06:00-fri:07:00"},
		"BacktrackWindow":            {"7200"},
		"CopyTagsToSnapshot":         {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			DBCluster struct {
				EngineVersion              string `xml:"EngineVersion"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
				BacktrackWindow            int64  `xml:"BacktrackWindow"`
				CopyTagsToSnapshot         bool   `xml:"CopyTagsToSnapshot"`
			} `xml:"DBCluster"`
		} `xml:"ModifyDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	c := resp.Result.DBCluster
	assert.Equal(t, "15.5", c.EngineVersion)
	assert.Equal(t, int64(7200), c.BacktrackWindow)
	assert.Equal(t, "04:00-05:00", c.PreferredBackupWindow)
	assert.True(t, c.CopyTagsToSnapshot)
}

// TestEnabledCloudwatchLogsExportsInClusterXML verifies EnabledCloudwatchLogsExports is emitted for clusters.
func TestEnabledCloudwatchLogsExportsInClusterXML(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                               {"CreateDBCluster"},
		"Version":                              {"2014-10-31"},
		"DBClusterIdentifier":                  {"logs-cluster"},
		"Engine":                               {"aurora-mysql"},
		"MasterUsername":                       {"admin"},
		"EnableCloudwatchLogsExports.member.1": {"audit"},
		"EnableCloudwatchLogsExports.member.2": {"error"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				EnabledCloudwatchLogsExports struct {
					Members []string `xml:"member"`
				} `xml:"EnabledCloudwatchLogsExports"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"audit", "error"}, resp.Result.DBCluster.EnabledCloudwatchLogsExports.Members)
}

// TestDBClusterIdentifierPersisted verifies DBClusterIdentifier is stored and returned on instance.
func TestDBClusterIdentifierPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create the cluster first.
	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"my-aurora"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"cluster-member"},
		"DBInstanceClass":      {"db.r6g.large"},
		"Engine":               {"aurora-postgresql"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"0"},
		"DBClusterIdentifier":  {"my-aurora"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				DBClusterIdentifier string `xml:"DBClusterIdentifier"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-aurora", resp.Result.DBInstance.DBClusterIdentifier)
}

// TestDBClusterDeletionProtection verifies DeletionProtection is stored for clusters.
func TestDBClusterDeletionProtection(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"dp-cluster"},
		"Engine":              {"aurora-mysql"},
		"MasterUsername":      {"admin"},
		"DeletionProtection":  {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				DeletionProtection bool `xml:"DeletionProtection"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBCluster.DeletionProtection)
}

// TestDBClusterAvailabilityZones verifies AvailabilityZones are stored and returned for clusters.
func TestDBClusterAvailabilityZones(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                               {"CreateDBCluster"},
		"Version":                              {"2014-10-31"},
		"DBClusterIdentifier":                  {"az-cluster"},
		"Engine":                               {"aurora-postgresql"},
		"MasterUsername":                       {"admin"},
		"AvailabilityZones.AvailabilityZone.1": {"us-east-1a"},
		"AvailabilityZones.AvailabilityZone.2": {"us-east-1b"},
		"AvailabilityZones.AvailabilityZone.3": {"us-east-1c"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				AvailabilityZones struct {
					Members []string `xml:"AvailabilityZone"`
				} `xml:"AvailabilityZones"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(
		t,
		[]string{"us-east-1a", "us-east-1b", "us-east-1c"},
		resp.Result.DBCluster.AvailabilityZones.Members,
	)
}

// TestModifyDBClusterStorageEncryptedChanged verifies StorageEncrypted can be set via ModifyDBCluster.
func TestModifyDBClusterStorageEncryptedChanged(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"enc-cluster"},
		"Engine":              {"aurora-mysql"},
		"MasterUsername":      {"admin"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"enc-cluster"},
		"StorageEncrypted":    {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				StorageEncrypted bool `xml:"StorageEncrypted"`
			} `xml:"DBCluster"`
		} `xml:"ModifyDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBCluster.StorageEncrypted)
}

// TestDBInstanceJoinsClusterMemberList verifies creating an instance with DBClusterIdentifier
// adds it to cluster members.
func TestDBInstanceJoinsClusterMemberList(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"member-cluster"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	})

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"member-inst"},
		"DBInstanceClass":      {"db.r6g.large"},
		"Engine":               {"aurora-postgresql"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"0"},
		"DBClusterIdentifier":  {"member-cluster"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"member-cluster"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBClusters struct {
				Members []struct {
					DBClusterMembers struct {
						Members []struct {
							DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
						} `xml:"DBClusterMember"`
					} `xml:"DBClusterMembers"`
				} `xml:"DBCluster"`
			} `xml:"DBClusters"`
		} `xml:"DescribeDBClustersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	clusters := resp.Result.DBClusters.Members
	require.Len(t, clusters, 1)
	members := clusters[0].DBClusterMembers.Members
	require.Len(t, members, 1)
	assert.Equal(t, "member-inst", members[0].DBInstanceIdentifier)
}

// TestModifyDBClusterDeletionProtection verifies DeletionProtection can be set via ModifyDBCluster.
func TestModifyDBClusterDeletionProtection(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modifydp-cluster"},
		"Engine":              {"aurora-mysql"},
		"MasterUsername":      {"admin"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modifydp-cluster"},
		"DeletionProtection":  {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				DeletionProtection bool `xml:"DeletionProtection"`
			} `xml:"DBCluster"`
		} `xml:"ModifyDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBCluster.DeletionProtection)
}

func TestDBCluster_ReaderEndpointGenerated(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("my-cluster", "aurora-mysql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	assert.NotEmpty(t, c.ReaderEndpoint, "ReaderEndpoint should be set on cluster creation")
	assert.Contains(t, c.ReaderEndpoint, "my-cluster")
	assert.Contains(t, c.ReaderEndpoint, "-ro")
}

func TestDBCluster_NetworkTypeIPV4Default(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("net-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	assert.Equal(t, "IPV4", c.NetworkType)
}

func TestDBCluster_NetworkTypeDual(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("dual-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{NetworkType: "DUAL"})
	require.NoError(t, err)

	assert.Equal(t, "DUAL", c.NetworkType)
}

func TestDBCluster_StorageTypeAuroraIOOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("iopt-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{StorageType: "aurora-iopt1"})
	require.NoError(t, err)

	assert.Equal(t, "aurora-iopt1", c.StorageType)
}

func TestDBCluster_EngineLifecycleSupport(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("els-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{
			EngineLifecycleSupport: "open-source-rds-extended-support",
		})
	require.NoError(t, err)

	assert.Equal(t, "open-source-rds-extended-support", c.EngineLifecycleSupport)
}

func TestDBCluster_OptimizedWritesEnabled(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("ow-cluster", "aurora-mysql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{OptimizedWrites: true})
	require.NoError(t, err)

	assert.True(t, c.OptimizedWrites)
}

func TestDBCluster_ModifyStorageTypeAndNetworkType(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("mod-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	modified, err := b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		StorageType: "aurora-iopt1",
		NetworkType: "DUAL",
	})
	require.NoError(t, err)

	assert.Equal(t, "aurora-iopt1", modified.StorageType)
	assert.Equal(t, "DUAL", modified.NetworkType)
}

func TestDBCluster_NewFieldsViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBCluster&Version=2014-10-31"+
			"&DBClusterIdentifier=handler-cluster&Engine=aurora-mysql&MasterUsername=admin"+
			"&StorageType=aurora-iopt1&NetworkType=DUAL"+
			"&EngineLifecycleSupport=open-source-rds-extended-support"+
			"&EnableOptimizedWrites=true")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "aurora-iopt1", "StorageType should be in response")
	assert.Contains(t, respStr, "DUAL", "NetworkType should be in response")
	assert.Contains(t, respStr, "open-source-rds-extended-support")
	assert.Contains(t, respStr, "ReaderEndpoint")
}

func TestValidateStorageTypeForCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"empty", "", false},
		{"aurora", "aurora", false},
		{"aurora-iopt1", "aurora-iopt1", false},
		{"invalid-io1", "io1", true},
		{"invalid-gp3", "gp3", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := rds.ValidateStorageTypeForCluster(tt.val)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDescribeDBClusters_ReaderEndpointInResponse(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("re-cluster", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=DescribeDBClusters&Version=2014-10-31&DBClusterIdentifier=re-cluster")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "ReaderEndpoint")
	assert.Contains(t, respStr, "re-cluster")
	assert.Contains(t, respStr, "-ro")
}

func TestDBCluster_ReaderEndpointPersistedAndDescribed(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	created, err := b.CreateDBCluster("persist-re", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, created.ReaderEndpoint)

	clusters, err := b.DescribeDBClusters("persist-re")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	assert.Equal(t, created.ReaderEndpoint, clusters[0].ReaderEndpoint)
}

func TestBatch3_Persistence_ClusterNewFields(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("cls-snap", "aurora-mysql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{
			StorageType:            "aurora-iopt1",
			NetworkType:            "DUAL",
			EngineLifecycleSupport: "open-source-rds-extended-support",
			OptimizedWrites:        true,
		})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	clusters, err := b2.DescribeDBClusters("cls-snap")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	c := clusters[0]
	assert.Equal(t, "aurora-iopt1", c.StorageType, "StorageType should survive round-trip")
	assert.Equal(t, "DUAL", c.NetworkType, "NetworkType should survive round-trip")
	assert.Equal(t, "open-source-rds-extended-support", c.EngineLifecycleSupport)
	assert.True(t, c.OptimizedWrites)
	assert.NotEmpty(t, c.ReaderEndpoint, "ReaderEndpoint should survive round-trip")
}

func TestDBCluster_MultiAZEndpoints(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("rwg-cluster", "aurora-postgresql", "admin", "prod", "", 0, nil,
		rds.DBClusterOptions{MultiAZ: true})
	require.NoError(t, err)

	assert.NotEmpty(t, c.Endpoint, "writer endpoint should be set")
	assert.NotEmpty(t, c.ReaderEndpoint, "reader endpoint should be set for Multi-AZ")
	assert.True(t, c.MultiAZ)

	assert.NotEqual(t, c.Endpoint, c.ReaderEndpoint,
		"writer and reader endpoints should be different")
}

func TestDBCluster_ServerlessV2WithIOOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	serverlessCfg := &rds.ServerlessV2ScalingConfiguration{
		MinCapacity: 0.5,
		MaxCapacity: 128.0,
	}

	c, err := b.CreateDBCluster("sl2-iopt", "aurora-postgresql", "admin", "", "", 0, serverlessCfg,
		rds.DBClusterOptions{
			StorageType:            "aurora-iopt1",
			EngineLifecycleSupport: "open-source-rds-extended-support-disabled",
		})
	require.NoError(t, err)

	assert.NotNil(t, c.ServerlessV2ScalingConfig)
	assert.Equal(t, "aurora-iopt1", c.StorageType)
	assert.Equal(t, "open-source-rds-extended-support-disabled", c.EngineLifecycleSupport)
}

func TestDBCluster_ModifyEngineLifecycleSupportViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("els-mod", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=ModifyDBCluster&Version=2014-10-31&DBClusterIdentifier=els-mod"+
			"&EngineLifecycleSupport=open-source-rds-extended-support")
	require.Equal(t, http.StatusOK, rec.Code)

	clusters, err := b.DescribeDBClusters("els-mod")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "open-source-rds-extended-support", clusters[0].EngineLifecycleSupport)
}

func TestDBCluster_ModifyOptimizedWritesViaBackend(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("ow-mod", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("ow-mod")
	require.NoError(t, err)
	assert.False(t, clusters[0].OptimizedWrites, "OptimizedWrites should be false by default")

	_, err = b.ModifyDBCluster("ow-mod", "", rds.DBClusterOptions{OptimizedWrites: true})
	require.NoError(t, err)

	clusters, err = b.DescribeDBClusters("ow-mod")
	require.NoError(t, err)
	assert.True(t, clusters[0].OptimizedWrites, "OptimizedWrites should be set after modify")
}
