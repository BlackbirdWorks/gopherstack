package docdb //nolint:testpackage // needs docdbCtxRegion (isolation_test.go) and unexported region field verification.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullStateSnapshotRestore is the Phase 3.3 full-state round-trip test: it
// exercises every store.Table the region-qualified-table conversion touches --
// two of every region-nested resource type (clusters, instances, subnet
// groups, cluster parameter groups, cluster snapshots, event subscriptions,
// snapshot attributes), all sharing the SAME name across TWO regions, to
// prove the composite "region|id" key survives a round trip and keeps
// same-named resources in different regions fully isolated. It also covers
// the partition-scoped (non-composite-key) global cluster table and the one
// raw (non-store.Table) map tags, to guard against a silent persistence
// regression.
func TestFullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := docdbCtxRegion("us-east-1")
	ctxWest := docdbCtxRegion("us-west-2")

	const sharedName = "shared-name"

	// us-east-1: create one of every region-nested resource under sharedName.
	_, err := original.CreateDBCluster(
		ctxEast, sharedName, "docdb", "3.6.0", "admin", "", "", "", "",
		0, false, false, 1, "", "", nil, nil, nil,
	)
	require.NoError(t, err)
	_, err = original.CreateDBInstance(ctxEast, sharedName, sharedName, "", "", 0, nil, nil)
	require.NoError(t, err)
	_, err = original.CreateDBSubnetGroup(ctxEast, sharedName, "desc-east", "vpc-1", []string{"subnet-1"}, nil)
	require.NoError(t, err)
	_, err = original.CreateDBClusterParameterGroup(ctxEast, sharedName, "docdb4.0", "cpg-east", nil)
	require.NoError(t, err)
	_, err = original.CreateDBClusterSnapshot(ctxEast, sharedName, sharedName, nil)
	require.NoError(t, err)
	_, err = original.CreateEventSubscription(
		ctxEast, sharedName, "arn:aws:sns:us-east-1:000000000000:topic", "", nil, nil, nil, nil,
	)
	require.NoError(t, err)
	_, err = original.ModifyDBClusterSnapshotAttribute(
		ctxEast, sharedName, "restore", []string{"111111111111"}, nil,
	)
	require.NoError(t, err)

	// us-west-2: create the SAME-NAMED resources; only a region-qualified key
	// (not a bare name) can keep these distinct from the east set above.
	_, err = original.CreateDBCluster(
		ctxWest, sharedName, "docdb", "5.0.0", "admin", "", "", "", "",
		0, false, false, 1, "", "", nil, nil, nil,
	)
	require.NoError(t, err)
	_, err = original.CreateDBInstance(ctxWest, sharedName, sharedName, "", "", 0, nil, nil)
	require.NoError(t, err)
	_, err = original.CreateDBSubnetGroup(ctxWest, sharedName, "desc-west", "vpc-2", []string{"subnet-2"}, nil)
	require.NoError(t, err)
	_, err = original.CreateDBClusterParameterGroup(ctxWest, sharedName, "docdb4.0", "cpg-west", nil)
	require.NoError(t, err)
	_, err = original.CreateDBClusterSnapshot(ctxWest, sharedName, sharedName, nil)
	require.NoError(t, err)
	_, err = original.CreateEventSubscription(
		ctxWest, sharedName, "arn:aws:sns:us-west-2:000000000000:topic", "", nil, nil, nil, nil,
	)
	require.NoError(t, err)
	_, err = original.ModifyDBClusterSnapshotAttribute(
		ctxWest, sharedName, "restore", []string{"222222222222"}, nil,
	)
	require.NoError(t, err)

	// A global cluster: partition-scoped, must survive without region nesting.
	_, err = original.CreateGlobalCluster(ctxEast, "global-shared", sharedName, "", "")
	require.NoError(t, err)

	// Tags on the west cluster's ARN (raw nested map, left unconverted).
	westClusters, err := original.DescribeDBClusters(ctxWest, sharedName)
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
	assert.Equal(t, 2, fresh.ClusterCount())
	assert.Equal(t, 2, fresh.InstanceCount())
	assert.Equal(t, 2, fresh.SubnetGroupCount())
	assert.Equal(t, 2, fresh.ParameterGroupCount())
	assert.Equal(t, 2, fresh.SnapshotCount())
	assert.Equal(t, 2, fresh.EventSubscriptionCount())
	assert.Equal(t, 2, fresh.snapshotAttributes.Len())

	// Global cluster: partition-scoped, exactly 1 (not doubled by region).
	assert.Equal(t, 1, fresh.GlobalClusterCount())
	globals := fresh.DescribeGlobalClusters(ctxEast, "")
	require.Len(t, globals, 1)
	assert.Equal(t, "global-shared", globals[0].GlobalClusterIdentifier)

	// Per-region content and isolation: east and west each see only their own
	// version/description/engine.
	eastList, err := fresh.DescribeDBClusters(ctxEast, sharedName)
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "3.6.0", eastList[0].EngineVersion)
	assert.Contains(t, eastList[0].DBClusterArn, "us-east-1")

	westList, err := fresh.DescribeDBClusters(ctxWest, sharedName)
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "5.0.0", westList[0].EngineVersion)
	assert.Contains(t, westList[0].DBClusterArn, "us-west-2")

	eastSG, err := fresh.DescribeDBSubnetGroups(ctxEast, sharedName)
	require.NoError(t, err)
	require.Len(t, eastSG, 1)
	assert.Equal(t, "desc-east", eastSG[0].DBSubnetGroupDescription)

	westSG, err := fresh.DescribeDBSubnetGroups(ctxWest, sharedName)
	require.NoError(t, err)
	require.Len(t, westSG, 1)
	assert.Equal(t, "desc-west", westSG[0].DBSubnetGroupDescription)

	// snapshotAttributes (region-qualified, no byRegion index): east and west
	// each kept their own attribute values under the same snapshot name.
	eastAttrs, err := fresh.DescribeDBClusterSnapshotAttributes(ctxEast, sharedName)
	require.NoError(t, err)
	require.Len(t, eastAttrs.Attributes, 1)
	assert.Equal(t, []string{"111111111111"}, eastAttrs.Attributes[0].AttributeValues)

	westAttrs, err := fresh.DescribeDBClusterSnapshotAttributes(ctxWest, sharedName)
	require.NoError(t, err)
	require.Len(t, westAttrs.Attributes, 1)
	assert.Equal(t, []string{"222222222222"}, westAttrs.Attributes[0].AttributeValues)

	// tags (raw map, left unconverted): tag on the west cluster's ARN survived
	// and stayed off the east cluster.
	westTags := fresh.ListTagsForResource(ctxWest, westList[0].DBClusterArn)
	require.Len(t, westTags, 1)
	assert.Equal(t, "west", westTags[0].Value)

	eastTags := fresh.ListTagsForResource(ctxEast, eastList[0].DBClusterArn)
	assert.Empty(t, eastTags)
}
