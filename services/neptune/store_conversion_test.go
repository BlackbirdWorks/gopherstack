package neptune //nolint:testpackage // needs ctxRegion (isolation_test.go) and unexported region field verification.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullStateSnapshotRestore is the Phase 3.3 full-state round-trip test: it
// exercises every store.Table the region-qualified-table conversion touches --
// two of every region-nested resource type (clusters, instances, subnet
// groups, cluster parameter groups, cluster snapshots, DB parameter groups,
// cluster endpoints, event subscriptions), all sharing the SAME name across
// TWO regions, to prove the composite "region|id" key survives a round trip
// and keeps same-named resources in different regions fully isolated. It also
// covers the partition-scoped (non-composite-key) global cluster table and
// the two raw (non-store.Table) maps clusterRoles and tags, to guard against
// a silent persistence regression.
func TestFullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	const sharedName = "shared-name"

	// us-east-1: create one of every region-nested resource under sharedName.
	_, err := original.CreateDBCluster(ctxEast, sharedName, "", 0, DBClusterCreateOptions{EngineVersion: "1.9.9.9"})
	require.NoError(t, err)
	_, err = original.CreateDBInstance(ctxEast, sharedName, sharedName, "", DBInstanceCreateOptions{})
	require.NoError(t, err)
	_, err = original.CreateDBSubnetGroup(ctxEast, sharedName, "desc-east", "vpc-1", []string{"subnet-1"})
	require.NoError(t, err)
	_, err = original.CreateDBClusterParameterGroup(ctxEast, sharedName, "neptune1.3", "cpg-east")
	require.NoError(t, err)
	_, err = original.CreateDBClusterSnapshot(ctxEast, sharedName, sharedName)
	require.NoError(t, err)
	_, err = original.CreateDBParameterGroup(ctxEast, sharedName, "neptune1.3", "dpg-east")
	require.NoError(t, err)
	_, err = original.CreateDBClusterEndpoint(ctxEast, sharedName, sharedName, "")
	require.NoError(t, err)
	_, err = original.CreateEventSubscription(
		ctxEast, sharedName, "arn:aws:sns:us-east-1:000000000000:topic", "", nil, true,
	)
	require.NoError(t, err)
	require.NoError(t, original.AddRoleToDBCluster(ctxEast, sharedName, "arn:aws:iam::000000000000:role/east"))

	// us-west-2: create the SAME-NAMED resources; only a region-qualified key
	// (not a bare name) can keep these distinct from the east set above.
	_, err = original.CreateDBCluster(ctxWest, sharedName, "", 0, DBClusterCreateOptions{EngineVersion: "1.8.8.8"})
	require.NoError(t, err)
	_, err = original.CreateDBInstance(ctxWest, sharedName, sharedName, "", DBInstanceCreateOptions{})
	require.NoError(t, err)
	_, err = original.CreateDBSubnetGroup(ctxWest, sharedName, "desc-west", "vpc-2", []string{"subnet-2"})
	require.NoError(t, err)
	_, err = original.CreateDBClusterParameterGroup(ctxWest, sharedName, "neptune1.3", "cpg-west")
	require.NoError(t, err)
	_, err = original.CreateDBClusterSnapshot(ctxWest, sharedName, sharedName)
	require.NoError(t, err)
	_, err = original.CreateDBParameterGroup(ctxWest, sharedName, "neptune1.3", "dpg-west")
	require.NoError(t, err)
	_, err = original.CreateDBClusterEndpoint(ctxWest, sharedName, sharedName, "")
	require.NoError(t, err)
	_, err = original.CreateEventSubscription(
		ctxWest, sharedName, "arn:aws:sns:us-west-2:000000000000:topic", "", nil, true,
	)
	require.NoError(t, err)
	require.NoError(t, original.AddRoleToDBCluster(ctxWest, sharedName, "arn:aws:iam::000000000000:role/west"))

	// A global cluster: partition-scoped, must survive without region nesting.
	_, err = original.CreateGlobalCluster(ctxEast, "global-shared", sharedName, "")
	require.NoError(t, err)

	// Tags on the west cluster's ARN (raw nested map, left unconverted).
	westClusters, err := original.DescribeDBClusters(ctxWest, sharedName, DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, westClusters, 1)
	require.NoError(
		t,
		original.AddTagsToResource(ctxWest, westClusters[0].DBClusterArn, []Tag{{Key: "env", Value: "west"}}),
	)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Every region-qualified table has exactly 2 entries (one per region) --
	// proves the composite key kept same-named east/west resources distinct
	// rather than one overwriting the other.
	assert.Equal(t, 2, ClusterCount(fresh))
	assert.Equal(t, 2, InstanceCount(fresh))
	assert.Equal(t, 2, SubnetGroupCount(fresh))
	assert.Equal(t, 2, ClusterParameterGroupCount(fresh))
	assert.Equal(t, 2, ClusterSnapshotCount(fresh))
	assert.Equal(t, 2, ParameterGroupCount(fresh))
	assert.Equal(t, 2, ClusterEndpointCount(fresh))
	assert.Equal(t, 2, EventSubscriptionCount(fresh))

	// Global cluster: partition-scoped, exactly 1 (not doubled by region).
	assert.Equal(t, 1, GlobalClusterCount(fresh))
	globals := fresh.DescribeGlobalClusters(ctxEast)
	require.Len(t, globals, 1)
	assert.Equal(t, "global-shared", globals[0].GlobalClusterIdentifier)

	// Per-region content and isolation: east and west each see only their own
	// version/description/engine.
	eastList, err := fresh.DescribeDBClusters(ctxEast, sharedName, DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "1.9.9.9", eastList[0].EngineVersion)
	assert.Contains(t, eastList[0].DBClusterArn, "us-east-1")

	westList, err := fresh.DescribeDBClusters(ctxWest, sharedName, DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "1.8.8.8", westList[0].EngineVersion)
	assert.Contains(t, westList[0].DBClusterArn, "us-west-2")

	eastSG, err := fresh.DescribeDBSubnetGroups(ctxEast, sharedName)
	require.NoError(t, err)
	require.Len(t, eastSG, 1)
	assert.Equal(t, "desc-east", eastSG[0].DBSubnetGroupDescription)

	westSG, err := fresh.DescribeDBSubnetGroups(ctxWest, sharedName)
	require.NoError(t, err)
	require.Len(t, westSG, 1)
	assert.Equal(t, "desc-west", westSG[0].DBSubnetGroupDescription)

	// clusterRoles (raw map, left unconverted): each region's cluster kept its
	// own role association.
	assert.Equal(t, 1, ClusterRoleCount(fresh, sharedName))

	// tags (raw map, left unconverted): tag on the west cluster's ARN survived
	// and stayed off the east cluster.
	westTags, err := fresh.ListTagsForResource(ctxWest, westList[0].DBClusterArn)
	require.NoError(t, err)
	require.Len(t, westTags, 1)
	assert.Equal(t, "west", westTags[0].Value)

	eastTags, err := fresh.ListTagsForResource(ctxEast, eastList[0].DBClusterArn)
	require.NoError(t, err)
	assert.Empty(t, eastTags)
}
