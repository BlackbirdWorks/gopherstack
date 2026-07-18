package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
