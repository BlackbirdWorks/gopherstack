package elasticache_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *elasticache.InMemoryBackend) string
		verify func(t *testing.T, b *elasticache.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *elasticache.InMemoryBackend) string {
				cluster, err := b.CreateCluster(context.Background(), "test-cluster", "redis", "cache.t3.micro", 6379)
				if err != nil {
					return ""
				}

				return cluster.ClusterID
			},
			verify: func(t *testing.T, b *elasticache.InMemoryBackend, id string) {
				t.Helper()

				p, err := b.DescribeClusters(context.Background(), id, "", 0, false)
				clusters := p.Data
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, id, clusters[0].ClusterID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *elasticache.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *elasticache.InMemoryBackend, _ string) {
				t.Helper()

				clusters := b.ListAll()
				assert.Empty(t, clusters)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_FullState seeds one instance of every
// resource type Phase 3.3 converted to pkgs/store, snapshots the backend,
// restores into a fresh backend, and checks every resource survived with its
// identity/fields intact. This guards against a table being silently dropped
// during the map -> store.Table conversion. Region-nested per-region table
// round-tripping (a second region within the same resource) is covered
// separately by the internal-package test TestSnapshotRestore_CrossRegion,
// which needs the unexported region-context key not reachable from here.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) { //nolint:maintidx // exhaustive seed+verify
	t.Parallel()

	ctx := t.Context()

	original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)

	_, err := original.CreateCluster(ctx, "fs-cluster", "redis", "cache.t3.micro", 6379)
	require.NoError(t, err)

	rg, err := original.CreateReplicationGroup(ctx, "fs-rg", "full state rg")
	require.NoError(t, err)

	_, err = original.CreateParameterGroup(ctx, "fs-pg", "redis7", "full state pg")
	require.NoError(t, err)

	_, err = original.CreateSubnetGroup(ctx, "fs-sng", "full state subnet group", []string{"subnet-1"})
	require.NoError(t, err)

	_, err = original.CreateSnapshot(ctx, "fs-snap", "fs-cluster", "")
	require.NoError(t, err)

	_, err = original.CreateCacheSecurityGroup(ctx, "fs-sg", "full state security group")
	require.NoError(t, err)
	_, err = original.AuthorizeCacheSecurityGroupIngress(ctx, "fs-sg", "some-ec2-sg", "000000000000")
	require.NoError(t, err)

	_, err = original.CreateGlobalReplicationGroup(ctx, "fsgrg", "full state global rg", rg.ReplicationGroupID)
	require.NoError(t, err)

	_, err = original.CreateServerlessCache(ctx, "fs-serverless", "full state serverless", "redis")
	require.NoError(t, err)
	_, err = original.CreateServerlessCacheSnapshot(ctx, "fs-serverless-snap", "fs-serverless")
	require.NoError(t, err)

	_, err = original.CreateUser(ctx, "fs-user", "fs-user-name", "on ~* &* +@all", "redis", true)
	require.NoError(t, err)
	_, err = original.CreateUserGroup(ctx, "fs-usergroup", "full state user group", "redis", []string{"fs-user"})
	require.NoError(t, err)

	// Hardcoded to match the builtin offering ID in backend_ops2.go's
	// builtinReservedOfferings (unexported, so not reachable from this
	// external test package by name).
	const builtinOfferingID = "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"
	_, err = original.PurchaseReservedCacheNodesOffering(ctx, builtinOfferingID, "fs-reserved-node", 1)
	require.NoError(t, err)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	require.NoError(t, fresh.Restore(ctx, snap))

	clusters, err := fresh.DescribeClusters(ctx, "fs-cluster", "", 0, false)
	require.NoError(t, err)
	require.Len(t, clusters.Data, 1)

	rgs, err := fresh.DescribeReplicationGroups(ctx, "fs-rg", "", 0)
	require.NoError(t, err)
	require.Len(t, rgs.Data, 1)

	pgs, err := fresh.DescribeParameterGroups(ctx, "fs-pg", "", 0)
	require.NoError(t, err)
	require.Len(t, pgs.Data, 1)

	sngs, err := fresh.DescribeSubnetGroups(ctx, "fs-sng", "", 0)
	require.NoError(t, err)
	require.Len(t, sngs.Data, 1)

	snaps, err := fresh.DescribeSnapshots(ctx, "fs-snap", "", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, snaps.Data, 1)

	sgs, err := fresh.DescribeCacheSecurityGroups(ctx, "fs-sg", "", 0)
	require.NoError(t, err)
	require.Len(t, sgs.Data, 1)

	grgs, err := fresh.DescribeGlobalReplicationGroups(ctx, "ldgnf-fsgrg", "", 0)
	require.NoError(t, err)
	require.Len(t, grgs.Data, 1)

	scs, err := fresh.DescribeServerlessCaches(ctx, "fs-serverless", "", 0)
	require.NoError(t, err)
	require.Len(t, scs.Data, 1)

	scSnaps, err := fresh.DescribeServerlessCacheSnapshots(ctx, "fs-serverless", "fs-serverless-snap", "", 0)
	require.NoError(t, err)
	require.Len(t, scSnaps.Data, 1)

	users, err := fresh.DescribeUsers(ctx, "fs-user", "", 0)
	require.NoError(t, err)
	require.Len(t, users.Data, 1)

	userGroups, err := fresh.DescribeUserGroups(ctx, "fs-usergroup", "", 0)
	require.NoError(t, err)
	require.Len(t, userGroups.Data, 1)

	reservedNodes, err := fresh.DescribeReservedCacheNodes(ctx, "fs-reserved-node", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, reservedNodes.Data, 1)
}

// TestInMemoryBackend_Restore_IncompatibleVersion feeds Restore a
// well-formed-but-wrong-version snapshot (simulating an older/incompatible
// on-disk format) and checks it is discarded cleanly -- resetting the backend
// to empty rather than erroring or partially decoding the mismatched shape.
func TestInMemoryBackend_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	_, err := original.CreateCluster(ctx, "versioned-cluster", "redis", "cache.t3.micro", 6379)
	require.NoError(t, err)

	// Craft a snapshot with an incompatible version by round-tripping through
	// a generic map: bump "version" to a value that can never match the
	// current elasticacheSnapshotVersion constant.
	var raw map[string]any

	require.NoError(t, json.Unmarshal(original.Snapshot(ctx), &raw))
	raw["version"] = -1

	badSnap, err := json.Marshal(raw)
	require.NoError(t, err)

	fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	_, err = fresh.CreateCluster(ctx, "pre-existing", "redis", "cache.t3.micro", 6379)
	require.NoError(t, err)

	require.NoError(t, fresh.Restore(ctx, badSnap))

	all := fresh.ListAll()
	assert.Empty(t, all,
		"incompatible-version restore must reset to empty, not keep pre-existing or partially decoded state")
}
