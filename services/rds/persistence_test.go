package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *rds.InMemoryBackend) string
		verify func(t *testing.T, b *rds.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *rds.InMemoryBackend) string {
				inst, err := b.CreateDBInstance(
					"test-db",
					"mysql",
					"db.t3.micro",
					"testdb",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				if err != nil {
					return ""
				}

				return inst.DBInstanceIdentifier
			},
			verify: func(t *testing.T, b *rds.InMemoryBackend, id string) {
				t.Helper()

				instances, err := b.DescribeDBInstances(id)
				require.NoError(t, err)
				require.Len(t, instances, 1)
				assert.Equal(t, id, instances[0].DBInstanceIdentifier)
				assert.Equal(t, "mysql", instances[0].Engine)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *rds.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *rds.InMemoryBackend, _ string) {
				t.Helper()

				instances, err := b.DescribeDBInstances("")
				require.NoError(t, err)
				assert.Empty(t, instances)
			},
		},
		{
			name: "round_trip_preserves_parameter_group",
			setup: func(b *rds.InMemoryBackend) string {
				pg, err := b.CreateDBParameterGroup("test-pg", "postgres14", "Test PG")
				if err != nil {
					return ""
				}

				return pg.DBParameterGroupName
			},
			verify: func(t *testing.T, b *rds.InMemoryBackend, id string) {
				t.Helper()
				groups, err := b.DescribeDBParameterGroups(id)
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, id, groups[0].DBParameterGroupName)
				assert.Equal(t, "postgres14", groups[0].DBParameterGroupFamily)
			},
		},
		{
			name: "round_trip_preserves_option_group",
			setup: func(b *rds.InMemoryBackend) string {
				og, err := b.CreateOptionGroup("test-og", "mysql", "8.0", "Test OG")
				if err != nil {
					return ""
				}

				return og.OptionGroupName
			},
			verify: func(t *testing.T, b *rds.InMemoryBackend, id string) {
				t.Helper()
				groups, err := b.DescribeOptionGroups(id)
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, id, groups[0].OptionGroupName)
				assert.Equal(t, "mysql", groups[0].EngineName)
			},
		},
		{
			name: "round_trip_preserves_cluster",
			setup: func(b *rds.InMemoryBackend) string {
				cluster, err := b.CreateDBCluster(
					"test-cluster",
					"aurora-postgresql",
					"admin",
					"mydb",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				if err != nil {
					return ""
				}

				return cluster.DBClusterIdentifier
			},
			verify: func(t *testing.T, b *rds.InMemoryBackend, id string) {
				t.Helper()
				clusters, err := b.DescribeDBClusters(id)
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, id, clusters[0].DBClusterIdentifier)
				assert.Equal(t, "aurora-postgresql", clusters[0].Engine)
			},
		},
		{
			name: "round_trip_preserves_cluster_snapshot",
			setup: func(b *rds.InMemoryBackend) string {
				_, err := b.CreateDBCluster(
					"snap-cluster",
					"aurora-postgresql",
					"admin",
					"mydb",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				if err != nil {
					return ""
				}
				snap, err := b.CreateDBClusterSnapshot("test-csnap", "snap-cluster")
				if err != nil {
					return ""
				}

				return snap.DBClusterSnapshotIdentifier
			},
			verify: func(t *testing.T, b *rds.InMemoryBackend, id string) {
				t.Helper()
				snaps, err := b.DescribeDBClusterSnapshots(id, "")
				require.NoError(t, err)
				require.Len(t, snaps, 1)
				assert.Equal(t, id, snaps[0].DBClusterSnapshotIdentifier)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := rds.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := rds.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip exercises the Phase
// 3.3 pkgs/store conversion end-to-end: it populates one entry in every
// store.Table-backed resource collection (see registerAllTables in
// store_setup.go) plus a representative sample of the plain maps that were
// deliberately left unconverted, snapshots the backend, restores into a fresh
// one, and asserts every resource is still there. This is the constraint-4
// full-state round-trip test called for by the datalayer refactor.
func TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	// instances / subnetGroups / parameterGroups / optionGroups
	_, err := b.CreateDBSubnetGroup("sn1", "desc", "vpc1", []string{"subnet-1"})
	require.NoError(t, err)
	_, err = b.CreateDBParameterGroup("pg1", "postgres14", "desc")
	require.NoError(t, err)
	_, err = b.CreateOptionGroup("og1", "mysql", "8.0", "desc")
	require.NoError(t, err)
	inst, err := b.CreateDBInstance("inst1", "mysql", "db.t3.micro", "db", "admin", "pg1", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)

	// snapshots
	_, err = b.CreateDBSnapshot("snap1", inst.DBInstanceIdentifier)
	require.NoError(t, err)
	_, err = b.ModifyDBSnapshotAttribute("snap1", "restore", []string{"all"}, nil)
	require.NoError(t, err)

	// clusters / clusterParameterGroups / clusterSnapshots / clusterEndpoints / clusterAutomatedBackups
	_, err = b.CreateDBClusterParameterGroup("cpg1", "aurora-postgresql14", "desc")
	require.NoError(t, err)
	cluster, err := b.CreateDBCluster("cl1", "aurora-postgresql", "admin", "db", "", 1, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBClusterSnapshot("csnap1", cluster.DBClusterIdentifier)
	require.NoError(t, err)
	_, err = b.ModifyDBClusterSnapshotAttribute("csnap1", "restore", []string{"all"}, nil)
	require.NoError(t, err)
	_, err = b.CreateDBClusterEndpoint("cep1", cluster.DBClusterIdentifier, "READER")
	require.NoError(t, err)
	require.NotNil(t, b.CreateDBClusterAutomatedBackup(cluster.DBClusterIdentifier))

	// exportTasks / globalClusters / eventSubscriptions / dbSecurityGroups / blueGreenDeployments
	_, err = b.StartExportTask("exp1", "arn:aws:rds:us-east-1:000000000000:snapshot:snap1", "bucket1")
	require.NoError(t, err)
	_, err = b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.6", false, false)
	require.NoError(t, err)
	_, err = b.CreateEventSubscription("evsub1", "arn:sns:topic", "db-instance", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDBSecurityGroup("sg1", "desc")
	require.NoError(t, err)
	_, err = b.CreateBlueGreenDeployment("bgd1", "inst1")
	require.NoError(t, err)

	// reservedInstances / recommendations / proxies / proxyTargetGroups / proxyEndpoints / customEngineVersions
	_, err = b.PurchaseReservedDBInstancesOffering("offering1", "ri1", 1)
	require.NoError(t, err)
	_, err = b.CreateDBProxy("proxy1", "MYSQL", "arn:aws:iam::000000000000:role/proxy", nil)
	require.NoError(t, err)
	_, err = b.CreateDBProxyEndpoint("proxy1", "proxyep1", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateCustomDBEngineVersion("custom-mysql", "8.0.35.custom", "desc")
	require.NoError(t, err)

	// shardGroups / integrations / tenantDatabases
	_, err = b.CreateDBShardGroup("shard1", cluster.DBClusterIdentifier, 16, 2, 0, false)
	require.NoError(t, err)
	_, err = b.CreateIntegration("intg1", "arn:src", "arn:dst", "", "", "")
	require.NoError(t, err)
	_, err = b.CreateTenantDatabase("inst1", "tenant1", "admin")
	require.NoError(t, err)

	// representative sample of maps deliberately left raw (see store_setup.go)
	require.NoError(t, b.AddRoleToDBCluster(cluster.DBClusterIdentifier, "arn:aws:iam::000000000000:role/cluster"))
	require.NoError(t, b.AddRoleToDBInstance("inst1", "arn:aws:iam::000000000000:role/instance"))
	b.AddDBSnapshotTenantDatabase("snap1", "inst1", "tenant1", "mysql")

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	instances, err := fresh.DescribeDBInstances("")
	require.NoError(t, err)
	assert.Len(t, instances, 1)

	subnetGroups, err := fresh.DescribeDBSubnetGroups("")
	require.NoError(t, err)
	assert.Len(t, subnetGroups, 1)

	paramGroups, err := fresh.DescribeDBParameterGroups("")
	require.NoError(t, err)
	assert.Len(t, paramGroups, 1)

	optionGroups, err := fresh.DescribeOptionGroups("")
	require.NoError(t, err)
	assert.Len(t, optionGroups, 1)

	snapshots, err := fresh.DescribeDBSnapshots("", "")
	require.NoError(t, err)
	assert.Len(t, snapshots, 1)

	snapAttrs, err := fresh.DescribeDBSnapshotAttributes("snap1")
	require.NoError(t, err)
	assert.Len(t, snapAttrs.DBSnapshotAttributes, 1)

	clusterParamGroups, err := fresh.DescribeDBClusterParameterGroups("")
	require.NoError(t, err)
	assert.Len(t, clusterParamGroups, 1)

	clusters, err := fresh.DescribeDBClusters("")
	require.NoError(t, err)
	assert.Len(t, clusters, 1)

	clusterSnaps, err := fresh.DescribeDBClusterSnapshots("", "")
	require.NoError(t, err)
	assert.Len(t, clusterSnaps, 1)

	clusterSnapAttrs, err := fresh.DescribeDBClusterSnapshotAttributes("csnap1")
	require.NoError(t, err)
	assert.Len(t, clusterSnapAttrs.DBClusterSnapshotAttributes, 1)

	clusterEndpoints, err := fresh.DescribeDBClusterEndpoints("", "")
	require.NoError(t, err)
	assert.Len(t, clusterEndpoints, 1)

	clusterBackups := fresh.DescribeDBClusterAutomatedBackups("")
	assert.Len(t, clusterBackups, 1)

	exportTasks, err := fresh.DescribeExportTasks("")
	require.NoError(t, err)
	assert.Len(t, exportTasks, 1)

	globalClusters, err := fresh.DescribeGlobalClusters("")
	require.NoError(t, err)
	assert.Len(t, globalClusters, 1)

	eventSubs, err := fresh.DescribeEventSubscriptions("")
	require.NoError(t, err)
	assert.Len(t, eventSubs, 1)

	secGroups, err := fresh.DescribeDBSecurityGroups("")
	require.NoError(t, err)
	assert.Len(t, secGroups, 1)

	bgds, err := fresh.DescribeBlueGreenDeployments("")
	require.NoError(t, err)
	assert.Len(t, bgds, 1)

	reserved := fresh.DescribeReservedDBInstances("", "")
	assert.Len(t, reserved, 1)

	proxies, err := fresh.DescribeDBProxies("")
	require.NoError(t, err)
	assert.Len(t, proxies, 1)

	targetGroups, err := fresh.DescribeDBProxyTargetGroups("proxy1", "")
	require.NoError(t, err)
	assert.Len(t, targetGroups, 1)

	proxyEndpoints, err := fresh.DescribeDBProxyEndpoints("", "")
	require.NoError(t, err)
	assert.Len(t, proxyEndpoints, 1)

	customVersions := fresh.DescribeCustomDBEngineVersions("", "")
	assert.Len(t, customVersions, 1)

	shardGroups, err := fresh.DescribeDBShardGroups("")
	require.NoError(t, err)
	assert.Len(t, shardGroups, 1)

	integrations, err := fresh.DescribeIntegrations("")
	require.NoError(t, err)
	assert.Len(t, integrations, 1)

	tenants, err := fresh.DescribeTenantDatabases("", "")
	require.NoError(t, err)
	assert.Len(t, tenants, 1)

	// representative raw-map sample (see store_setup.go's exclusion list)
	assert.Equal(t, 1, rds.ClusterRoleCount(fresh, cluster.DBClusterIdentifier))
	assert.Equal(t, 1, rds.InstanceRoleCount(fresh, "inst1"))
	snapTenants := fresh.DescribeDBSnapshotTenantDatabases("snap1", "")
	assert.Len(t, snapTenants, 1)
}
