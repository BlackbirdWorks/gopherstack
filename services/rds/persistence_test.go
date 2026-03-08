package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				inst, err := b.CreateDBInstance("test-db", "mysql", "db.t3.micro", "testdb", "admin", "", 20)
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
				cluster, err := b.CreateDBCluster("test-cluster", "aurora-postgresql", "admin", "mydb", "", 0)
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
				_, err := b.CreateDBCluster("snap-cluster", "aurora-postgresql", "admin", "mydb", "", 0)
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
				snaps, err := b.DescribeDBClusterSnapshots(id)
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

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := rds.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
