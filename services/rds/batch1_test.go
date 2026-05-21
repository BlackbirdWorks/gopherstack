package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// ---- DB Shard Group backend tests ----

func TestCreateDBShardGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		id        string
		clusterID string
		wantErr   bool
	}{
		{name: "success", id: "sg-1", clusterID: "cluster-1"},
		{
			name:      "empty id",
			id:        "",
			clusterID: "cluster-1",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty cluster",
			id:        "sg-2",
			clusterID: "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "duplicate",
			id:        "sg-dup",
			clusterID: "cluster-1",
			wantErr:   true,
			wantErrIs: rds.ErrDBShardGroupAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)

			if tt.name == "duplicate" {
				_, err := b.CreateDBShardGroup(tt.id, tt.clusterID, 128, 0, 0, false)
				require.NoError(t, err)
			}

			sg, err := b.CreateDBShardGroup(tt.id, tt.clusterID, 128, 0, 0, false)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.id, sg.DBShardGroupIdentifier)
			assert.Equal(t, tt.clusterID, sg.DBClusterIdentifier)
			assert.Equal(t, "available", sg.Status)
		})
	}
}

func TestDeleteDBShardGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-del", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)

		sg, err := b.DeleteDBShardGroup("sg-del")
		require.NoError(t, err)
		assert.Equal(t, "sg-del", sg.DBShardGroupIdentifier)
		assert.Equal(t, "deleting", sg.Status)

		// Confirm removed
		groups, err := b.DescribeDBShardGroups("sg-del")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
		assert.Empty(t, groups)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteDBShardGroup("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestDescribeDBShardGroups(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		groups, err := b.DescribeDBShardGroups("")
		require.NoError(t, err)
		assert.Empty(t, groups)
	})

	t.Run("lists all", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, id := range []string{"sg-z", "sg-a", "sg-m"} {
			_, err := b.CreateDBShardGroup(id, "cluster-1", 64, 0, 0, false)
			require.NoError(t, err)
		}
		groups, err := b.DescribeDBShardGroups("")
		require.NoError(t, err)
		require.Len(t, groups, 3)
		// verify sorted
		assert.Equal(t, "sg-a", groups[0].DBShardGroupIdentifier)
		assert.Equal(t, "sg-m", groups[1].DBShardGroupIdentifier)
		assert.Equal(t, "sg-z", groups[2].DBShardGroupIdentifier)
	})

	t.Run("filtered", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-x", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)
		_, err = b.CreateDBShardGroup("sg-y", "cluster-2", 128, 0, 0, false)
		require.NoError(t, err)

		groups, err := b.DescribeDBShardGroups("sg-x")
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, "sg-x", groups[0].DBShardGroupIdentifier)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DescribeDBShardGroups("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestModifyDBShardGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-mod", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)

		sg, err := b.ModifyDBShardGroup("sg-mod", 256, 1)
		require.NoError(t, err)
		assert.InEpsilon(t, 256.0, sg.MaxACU, 0.001)
		assert.Equal(t, 1, sg.ComputeRedundancy)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.ModifyDBShardGroup("missing", 128, 0)
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestRebootDBShardGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-reboot", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)

		sg, err := b.RebootDBShardGroup("sg-reboot")
		require.NoError(t, err)
		assert.Equal(t, "rebooting", sg.Status)

		// After reboot the stored state should be available
		groups, err := b.DescribeDBShardGroups("sg-reboot")
		require.NoError(t, err)
		assert.Equal(t, "available", groups[0].Status)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.RebootDBShardGroup("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

// ---- Integration backend tests ----

func TestCreateIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		intgName  string
		srcARN    string
		tgtARN    string
		wantErr   bool
	}{
		{
			name:     "success",
			intgName: "intg-1",
			srcARN:   "arn:aws:rds:us-east-1:123:db:src",
			tgtARN:   "arn:aws:redshift:us-east-1:123:namespace:tgt",
		},
		{name: "empty name", intgName: "", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
		{
			name:     "duplicate",
			intgName: "intg-dup",
			srcARN:   "arn:aws:rds:us-east-1:123:db:src",
			tgtARN:   "arn:aws:redshift:us-east-1:123:namespace:tgt",
			wantErr:  true, wantErrIs: rds.ErrIntegrationAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)

			if tt.name == "duplicate" {
				_, err := b.CreateIntegration(tt.intgName, tt.srcARN, tt.tgtARN, "")
				require.NoError(t, err)
			}

			intg, err := b.CreateIntegration(tt.intgName, tt.srcARN, tt.tgtARN, "")
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.intgName, intg.IntegrationName)
			assert.Equal(t, tt.srcARN, intg.SourceArn)
			assert.Equal(t, tt.tgtARN, intg.TargetArn)
			assert.Equal(t, "active", intg.Status)
			assert.NotEmpty(t, intg.IntegrationArn)
		})
	}
}

func TestDeleteIntegration(t *testing.T) {
	t.Parallel()

	t.Run("success by name", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateIntegration("intg-del", "src", "tgt", "")
		require.NoError(t, err)

		intg, err := b.DeleteIntegration("intg-del")
		require.NoError(t, err)
		assert.Equal(t, "intg-del", intg.IntegrationName)
		assert.Equal(t, "deleting", intg.Status)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteIntegration("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrIntegrationNotFound)
	})
}

func TestDescribeIntegrations(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		integrations, err := b.DescribeIntegrations("")
		require.NoError(t, err)
		assert.Empty(t, integrations)
	})

	t.Run("lists all sorted", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, nm := range []string{"intg-z", "intg-a", "intg-m"} {
			_, err := b.CreateIntegration(nm, "src", "tgt", "")
			require.NoError(t, err)
		}
		integrations, err := b.DescribeIntegrations("")
		require.NoError(t, err)
		require.Len(t, integrations, 3)
		assert.Equal(t, "intg-a", integrations[0].IntegrationName)
		assert.Equal(t, "intg-m", integrations[1].IntegrationName)
		assert.Equal(t, "intg-z", integrations[2].IntegrationName)
	})

	t.Run("filtered", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateIntegration("intg-x", "src", "tgt", "")
		require.NoError(t, err)
		_, err = b.CreateIntegration("intg-y", "src", "tgt", "")
		require.NoError(t, err)

		integrations, err := b.DescribeIntegrations("intg-x")
		require.NoError(t, err)
		require.Len(t, integrations, 1)
		assert.Equal(t, "intg-x", integrations[0].IntegrationName)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DescribeIntegrations("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrIntegrationNotFound)
	})
}

func TestModifyIntegration(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateIntegration("intg-mod", "src", "tgt", "")
		require.NoError(t, err)

		intg, err := b.ModifyIntegration("intg-mod")
		require.NoError(t, err)
		assert.Equal(t, "intg-mod", intg.IntegrationName)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.ModifyIntegration("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrIntegrationNotFound)
	})
}

// ---- Tenant Database backend tests ----

func TestCreateTenantDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs    error
		name         string
		instanceID   string
		tenantDBName string
		wantErr      bool
	}{
		{name: "success", instanceID: "db-1", tenantDBName: "tenantdb"},
		{
			name:         "empty instanceID",
			instanceID:   "",
			tenantDBName: "t1",
			wantErr:      true,
			wantErrIs:    rds.ErrInvalidParameter,
		},
		{
			name:         "empty tenantDBName",
			instanceID:   "db-1",
			tenantDBName: "",
			wantErr:      true,
			wantErrIs:    rds.ErrInvalidParameter,
		},
		{
			name:         "duplicate",
			instanceID:   "db-dup",
			tenantDBName: "tdup",
			wantErr:      true,
			wantErrIs:    rds.ErrTenantDatabaseAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)

			if tt.name == "duplicate" {
				_, err := b.CreateTenantDatabase(tt.instanceID, tt.tenantDBName, "admin")
				require.NoError(t, err)
			}

			tdb, err := b.CreateTenantDatabase(tt.instanceID, tt.tenantDBName, "admin")
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, tdb.DBInstanceIdentifier)
			assert.Equal(t, tt.tenantDBName, tdb.TenantDBName)
			assert.Equal(t, "admin", tdb.MasterUsername)
			assert.Equal(t, "available", tdb.Status)
			assert.NotEmpty(t, tdb.TenantDatabaseARN)
		})
	}
}

func TestDeleteTenantDatabase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateTenantDatabase("db-1", "tdb-del", "admin")
		require.NoError(t, err)

		tdb, err := b.DeleteTenantDatabase("db-1", "tdb-del")
		require.NoError(t, err)
		assert.Equal(t, "deleting", tdb.Status)

		tdbs, err := b.DescribeTenantDatabases("db-1", "tdb-del")
		require.NoError(t, err)
		assert.Empty(t, tdbs)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteTenantDatabase("db-1", "missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrTenantDatabaseNotFound)
	})
}

func TestDescribeTenantDatabases(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		tdbs, err := b.DescribeTenantDatabases("", "")
		require.NoError(t, err)
		assert.Empty(t, tdbs)
	})

	t.Run("filter by instance", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, tname := range []string{"t1", "t2"} {
			_, err := b.CreateTenantDatabase("db-1", tname, "admin")
			require.NoError(t, err)
		}
		_, err := b.CreateTenantDatabase("db-2", "t3", "admin")
		require.NoError(t, err)

		tdbs, err := b.DescribeTenantDatabases("db-1", "")
		require.NoError(t, err)
		assert.Len(t, tdbs, 2)
		for _, tdb := range tdbs {
			assert.Equal(t, "db-1", tdb.DBInstanceIdentifier)
		}
	})

	t.Run("filter by name", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateTenantDatabase("db-1", "ta", "admin")
		require.NoError(t, err)
		_, err = b.CreateTenantDatabase("db-1", "tb", "admin")
		require.NoError(t, err)

		tdbs, err := b.DescribeTenantDatabases("", "ta")
		require.NoError(t, err)
		require.Len(t, tdbs, 1)
		assert.Equal(t, "ta", tdbs[0].TenantDBName)
	})
}

func TestModifyTenantDatabase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateTenantDatabase("db-1", "tdb-mod", "admin")
		require.NoError(t, err)

		tdb, err := b.ModifyTenantDatabase("db-1", "tdb-mod")
		require.NoError(t, err)
		assert.Equal(t, "tdb-mod", tdb.TenantDBName)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.ModifyTenantDatabase("db-1", "missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrTenantDatabaseNotFound)
	})
}

// ---- DB Cluster Automated Backup backend tests ----

func TestDeleteDBClusterAutomatedBackup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		// Create a cluster to populate the backup
		_, err := b.CreateDBCluster(
			"cluster-bk",
			"aurora-mysql",
			"admin",
			"db",
			"",
			3306,
			nil,
			rds.DBClusterOptions{},
		)
		require.NoError(t, err)
		b.CreateDBClusterAutomatedBackup("cluster-bk")

		backups := b.DescribeDBClusterAutomatedBackups("cluster-bk")
		require.Len(t, backups, 1)

		deleted, err := b.DeleteDBClusterAutomatedBackup("cluster-bk")
		require.NoError(t, err)
		assert.Equal(t, "deleted", deleted.Status)

		backups = b.DescribeDBClusterAutomatedBackups("cluster-bk")
		assert.Empty(t, backups)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteDBClusterAutomatedBackup("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBClusterAutomatedBackupNotFound)
	})
}

func TestDescribeDBClusterAutomatedBackups(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		backups := b.DescribeDBClusterAutomatedBackups("")
		assert.Empty(t, backups)
	})

	t.Run("lists backups", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBCluster(
			"cluster-1",
			"aurora-mysql",
			"admin",
			"db",
			"",
			3306,
			nil,
			rds.DBClusterOptions{},
		)
		require.NoError(t, err)
		b.CreateDBClusterAutomatedBackup("cluster-1")

		backups := b.DescribeDBClusterAutomatedBackups("")
		require.Len(t, backups, 1)
		assert.Equal(t, "cluster-1", backups[0].DBClusterIdentifier)
		assert.Equal(t, "available", backups[0].Status)
	})

	t.Run("filter by cluster", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, id := range []string{"c1", "c2"} {
			_, err := b.CreateDBCluster(
				id,
				"aurora-mysql",
				"admin",
				"db",
				"",
				3306,
				nil,
				rds.DBClusterOptions{},
			)
			require.NoError(t, err)
			b.CreateDBClusterAutomatedBackup(id)
		}

		backups := b.DescribeDBClusterAutomatedBackups("c1")
		require.Len(t, backups, 1)
		assert.Equal(t, "c1", backups[0].DBClusterIdentifier)
	})
}

// ---- DB Instance Automated Backup enhanced tests ----

func TestDeleteDBInstanceAutomatedBackup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		// Start replication to create an automated backup record
		_, err := b.StartDBInstanceAutomatedBackupsReplication(
			"arn:aws:rds:us-east-1:123:db:src", 7,
		)
		require.NoError(t, err)

		backups := b.DescribeDBInstanceAutomatedBackups("")
		require.NotEmpty(t, backups)

		deleted, err := b.DeleteDBInstanceAutomatedBackup(backups[0].DBInstanceIdentifier)
		require.NoError(t, err)
		assert.Equal(t, "deleted", deleted.Status)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteDBInstanceAutomatedBackup("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBInstanceAutomatedBackupNotFound)
	})
}

func TestStartDBInstanceAutomatedBackupsReplication(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		backup, err := b.StartDBInstanceAutomatedBackupsReplication(
			"arn:aws:rds:us-east-1:123:db:src", 7,
		)
		require.NoError(t, err)
		assert.Equal(t, "replicating", backup.Status)
		assert.Equal(t, 7, backup.BackupRetentionPeriod)
	})

	t.Run("empty ARN", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.StartDBInstanceAutomatedBackupsReplication("", 7)
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrInvalidParameter)
	})
}

func TestStopDBInstanceAutomatedBackupsReplication(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		src := "arn:aws:rds:us-east-1:123:db:src"
		_, err := b.StartDBInstanceAutomatedBackupsReplication(src, 7)
		require.NoError(t, err)

		stopped, err := b.StopDBInstanceAutomatedBackupsReplication(src)
		require.NoError(t, err)
		assert.Equal(t, "stopped", stopped.Status)
	})

	t.Run("not running returns graceful", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		// Stop without start - should not error per AWS behavior
		stopped, err := b.StopDBInstanceAutomatedBackupsReplication(
			"arn:aws:rds:us-east-1:123:db:notfound",
		)
		require.NoError(t, err)
		assert.Equal(t, "stopped", stopped.Status)
	})
}

// ---- DB Snapshot Tenant Database backend tests ----

func TestDescribeDBSnapshotTenantDatabases(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		entries := b.DescribeDBSnapshotTenantDatabases("", "")
		assert.Empty(t, entries)
	})

	t.Run("filter by snapshot", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		b.AddDBSnapshotTenantDatabase("snap-1", "db-1", "tdb-a", "postgres")
		b.AddDBSnapshotTenantDatabase("snap-1", "db-1", "tdb-b", "postgres")
		b.AddDBSnapshotTenantDatabase("snap-2", "db-2", "tdb-c", "postgres")

		entries := b.DescribeDBSnapshotTenantDatabases("snap-1", "")
		require.Len(t, entries, 2)
		for _, e := range entries {
			assert.Equal(t, "snap-1", e.DBSnapshotIdentifier)
		}
	})

	t.Run("filter by instance", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		b.AddDBSnapshotTenantDatabase("snap-1", "db-1", "tdb-a", "postgres")
		b.AddDBSnapshotTenantDatabase("snap-2", "db-2", "tdb-b", "postgres")

		entries := b.DescribeDBSnapshotTenantDatabases("", "db-1")
		require.Len(t, entries, 1)
		assert.Equal(t, "snap-1", entries[0].DBSnapshotIdentifier)
	})
}

// ---- Handler integration tests (HTTP level) ----

func TestHandler_DBShardGroupCRUD(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create
	rec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-1&DBClusterIdentifier=cluster-1&MaxACU=128")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-1")

	// Describe all
	rec = postRDSForm(t, h, "Action=DescribeDBShardGroups&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-1")

	// Describe filtered
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeDBShardGroups&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-1")

	// Modify
	rec = postRDSForm(
		t,
		h,
		"Action=ModifyDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=sg-1&MaxACU=256",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Reboot
	rec = postRDSForm(
		t,
		h,
		"Action=RebootDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm gone
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeDBShardGroups&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_IntegrationCRUD(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create
	rec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=intg-1"+
			"&SourceArn=arn:aws:rds:us-east-1:123:db:src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123:namespace:tgt")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "intg-1")

	// Describe all
	rec = postRDSForm(t, h, "Action=DescribeIntegrations&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "intg-1")

	// Modify
	rec = postRDSForm(
		t,
		h,
		"Action=ModifyIntegration&Version=2014-10-31&IntegrationIdentifier=intg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteIntegration&Version=2014-10-31&IntegrationIdentifier=intg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm gone
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeIntegrations&Version=2014-10-31&IntegrationIdentifier=intg-1",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_TenantDatabaseCRUD(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create
	rec := postRDSForm(t, h,
		"Action=CreateTenantDatabase&Version=2014-10-31"+
			"&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb&MasterUsername=admin")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mytenantdb")

	// Describe all
	rec = postRDSForm(t, h, "Action=DescribeTenantDatabases&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mytenantdb")

	// Describe filtered
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeTenantDatabases&Version=2014-10-31&DBInstanceIdentifier=db-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mytenantdb")

	// Modify
	rec = postRDSForm(
		t,
		h,
		"Action=ModifyTenantDatabase&Version=2014-10-31&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteTenantDatabase&Version=2014-10-31&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm gone
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteTenantDatabase&Version=2014-10-31&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_AutomatedBackupOps(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Start replication
	rec := postRDSForm(t, h,
		"Action=StartDBInstanceAutomatedBackupsReplication&Version=2014-10-31"+
			"&SourceDBInstanceArn=arn:aws:rds:us-east-1:123:db:src"+
			"&BackupRetentionPeriod=7")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "replicating")

	// Describe instance automated backups
	rec = postRDSForm(t, h, "Action=DescribeDBInstanceAutomatedBackups&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Stop replication
	rec = postRDSForm(t, h,
		"Action=StopDBInstanceAutomatedBackupsReplication&Version=2014-10-31"+
			"&SourceDBInstanceArn=arn:aws:rds:us-east-1:123:db:src")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "stopped")

	// Describe cluster automated backups (empty)
	rec = postRDSForm(t, h, "Action=DescribeDBClusterAutomatedBackups&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DBSnapshotTenantDatabases(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Describe empty
	rec := postRDSForm(t, h, "Action=DescribeDBSnapshotTenantDatabases&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe with filter
	rec = postRDSForm(t, h,
		"Action=DescribeDBSnapshotTenantDatabases&Version=2014-10-31&DBSnapshotIdentifier=snap-1")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DBShardGroup_DuplicateError(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	rec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-dup&DBClusterIdentifier=cluster-1&MaxACU=64")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-dup&DBClusterIdentifier=cluster-1&MaxACU=64")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBShardGroupAlreadyExists")
}

func TestHandler_Integration_DuplicateError(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	rec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=dup-intg&SourceArn=src&TargetArn=tgt")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=dup-intg&SourceArn=src&TargetArn=tgt")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "IntegrationAlreadyExists")
}

func TestHandler_TenantDatabase_DuplicateError(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	rec := postRDSForm(t, h,
		"Action=CreateTenantDatabase&Version=2014-10-31"+
			"&DBInstanceIdentifier=db-1&TenantDBName=tdup&MasterUsername=admin")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=CreateTenantDatabase&Version=2014-10-31"+
			"&DBInstanceIdentifier=db-1&TenantDBName=tdup&MasterUsername=admin")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TenantDatabaseAlreadyExists")
}
