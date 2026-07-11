package memorydb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *memorydb.InMemoryBackend) string
		verify func(t *testing.T, b *memorydb.InMemoryBackend, name string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *memorydb.InMemoryBackend) string {
				c, err := b.CreateCluster(t.Context(), &memorydb.ExportedCreateClusterRequest{
					ClusterName: "test-cluster",
					NodeType:    "db.r6g.large",
				})
				if err != nil {
					return ""
				}

				return c.Name
			},
			verify: func(t *testing.T, b *memorydb.InMemoryBackend, name string) {
				t.Helper()

				clusters, err := b.DescribeClusters(t.Context(), name)
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, name, clusters[0].Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *memorydb.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *memorydb.InMemoryBackend, _ string) {
				t.Helper()

				assert.Equal(t, 0, memorydb.ClusterCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := memorydb.NewInMemoryBackend("123456789012", "us-east-1")
			name := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := memorydb.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, name)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_FullState seeds one instance of every
// resource type Phase 3.3 converted to pkgs/store, snapshots the backend,
// restores into a fresh backend, and checks every resource survived with its
// identity/fields intact. This guards against a table being silently dropped
// during the map -> store.Table conversion. Region-nested per-region table
// round-tripping (a second region within the same resource) and the two maps
// deliberately left un-converted (arnToResource and events) are covered
// separately by the internal-package tests in isolation_test.go, which need
// the unexported region-context key not reachable from here.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	original := memorydb.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := original.CreateCluster(ctx, &memorydb.ExportedCreateClusterRequest{
		ClusterName: "fs-cluster",
		NodeType:    "db.r6g.large",
	})
	require.NoError(t, err)

	_, err = original.CreateACL(ctx, &memorydb.ExportedCreateACLRequest{ACLName: "fs-acl"})
	require.NoError(t, err)

	_, err = original.CreateSubnetGroup(ctx, &memorydb.ExportedCreateSubnetGroupRequest{
		SubnetGroupName: "fs-sng",
		SubnetIDs:       []string{"subnet-1"},
	})
	require.NoError(t, err)

	_, err = original.CreateUser(ctx, &memorydb.ExportedCreateUserRequest{
		UserName:     "fs-user",
		AccessString: "on ~* &* +@all",
		AuthenticationMode: memorydb.ExportedAuthModeReq{
			Type: "no-password-required",
		},
	})
	require.NoError(t, err)

	_, err = original.CreateParameterGroup(ctx, &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "fs-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	_, err = original.CreateSnapshot(ctx, &memorydb.ExportedCreateSnapshotRequest{
		ClusterName:  "fs-cluster",
		SnapshotName: "fs-snap",
	})
	require.NoError(t, err)

	_, err = original.CreateMultiRegionCluster(ctx, &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "fs-mrc",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	// Hardcoded to match one of the builtin offering IDs in backend.go's
	// defaultReservedNodesOfferings (unexported, so not reachable from this
	// external test package by name).
	const builtinOfferingID = "aaa00000-1111-2222-3333-444444444444"
	_, err = original.PurchaseReservedNodesOffering(ctx, &memorydb.ExportedPurchaseReservedNodesOfferingRequest{
		ReservedNodesOfferingID: builtinOfferingID,
		ReservationID:           "fs-reserved-node",
	})
	require.NoError(t, err)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := memorydb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	clusters, err := fresh.DescribeClusters(ctx, "fs-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	acls, err := fresh.DescribeACLs(ctx, "fs-acl")
	require.NoError(t, err)
	require.Len(t, acls, 1)

	sngs, err := fresh.DescribeSubnetGroups(ctx, "fs-sng")
	require.NoError(t, err)
	require.Len(t, sngs, 1)

	users, err := fresh.DescribeUsers(ctx, "fs-user")
	require.NoError(t, err)
	require.Len(t, users, 1)

	pgs, err := fresh.DescribeParameterGroups(ctx, "fs-pg")
	require.NoError(t, err)
	require.Len(t, pgs, 1)

	snaps, err := fresh.DescribeSnapshots(ctx, "fs-snap", "", "", "")
	require.NoError(t, err)
	require.Len(t, snaps, 1)

	mrcs, err := fresh.DescribeMultiRegionClusters(ctx, "virv-fs-mrc")
	require.NoError(t, err)
	require.Len(t, mrcs, 1)

	reservedNodes, err := fresh.DescribeReservedNodes(ctx, &memorydb.ExportedDescribeReservedNodesRequest{
		ReservedNodeID: "fs-reserved-node",
	})
	require.NoError(t, err)
	require.Len(t, reservedNodes, 1)

	// The default open-access ACL and the four builtin default parameter
	// groups are re-seeded by NewInMemoryBackend and survive restore as part
	// of the acls/parameterGroups tables, so ACLCount/ParameterGroupCount
	// exceed 1 -- assert at-least rather than exact counts for those.
	assert.GreaterOrEqual(t, memorydb.ACLCount(fresh), 2)
	assert.GreaterOrEqual(t, memorydb.ParameterGroupCount(fresh), 1)
	assert.Equal(t, 1, memorydb.ClusterCount(fresh))
	assert.Equal(t, 1, memorydb.SubnetGroupCount(fresh))
	assert.Equal(t, 1, memorydb.UserCount(fresh))
	assert.Equal(t, 1, memorydb.SnapshotCount(fresh))
	assert.Equal(t, 1, memorydb.MultiRegionClusterCount(fresh))
	assert.Positive(t, memorydb.EventCount(fresh))
	assert.Positive(t, memorydb.ARNIndexSize(fresh))
}

// TestInMemoryBackend_Restore_IncompatibleVersion feeds Restore a
// well-formed-but-wrong-version snapshot (simulating an older/incompatible
// on-disk format) and checks it is discarded cleanly -- resetting the backend
// to empty rather than erroring or partially decoding the mismatched shape.
func TestInMemoryBackend_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	original := memorydb.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := original.CreateCluster(ctx, &memorydb.ExportedCreateClusterRequest{
		ClusterName: "versioned-cluster",
		NodeType:    "db.r6g.large",
	})
	require.NoError(t, err)

	// Craft a snapshot with an incompatible version by round-tripping through
	// a generic map: bump "version" to a value that can never match the
	// current memorydbSnapshotVersion constant.
	var raw map[string]any

	require.NoError(t, json.Unmarshal(original.Snapshot(ctx), &raw))
	raw["version"] = -1

	badSnap, err := json.Marshal(raw)
	require.NoError(t, err)

	fresh := memorydb.NewInMemoryBackend("123456789012", "us-east-1")
	_, err = fresh.CreateCluster(ctx, &memorydb.ExportedCreateClusterRequest{
		ClusterName: "pre-existing",
		NodeType:    "db.r6g.large",
	})
	require.NoError(t, err)

	require.NoError(t, fresh.Restore(ctx, badSnap))

	assert.Equal(t, 0, memorydb.ClusterCount(fresh),
		"incompatible-version restore must reset to empty, not keep pre-existing or partially decoded state")
}
