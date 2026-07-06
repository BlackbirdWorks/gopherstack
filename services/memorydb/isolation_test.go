package memorydb //nolint:testpackage // internal tests need access to unexported backend methods

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func memdbCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func TestMemoryDBRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := memdbCtxRegion("us-east-1")
	ctxWest := memdbCtxRegion("us-west-2")

	eastCluster, err := backend.CreateCluster(ctxEast, &createClusterRequest{
		ClusterName: "shared-cluster",
		NodeType:    "db.r6g.large",
	})
	require.NoError(t, err)
	assert.Contains(t, eastCluster.ARN, "us-east-1")
	assert.Equal(t, "us-east-1", eastCluster.Region)

	westCluster, err := backend.CreateCluster(ctxWest, &createClusterRequest{
		ClusterName: "shared-cluster",
		NodeType:    "db.r6g.xlarge",
	})
	require.NoError(t, err)
	assert.Contains(t, westCluster.ARN, "us-west-2")
	assert.Equal(t, "us-west-2", westCluster.Region)

	assert.NotEqual(t, eastCluster.ARN, westCluster.ARN)

	eastList, err := backend.DescribeClusters(ctxEast, "shared-cluster")
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "db.r6g.large", eastList[0].NodeType)

	westList, err := backend.DescribeClusters(ctxWest, "shared-cluster")
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "db.r6g.xlarge", westList[0].NodeType)

	_, err = backend.DeleteCluster(ctxEast, "shared-cluster")
	require.NoError(t, err)

	eastGone, err := backend.DescribeClusters(ctxEast, "shared-cluster")
	require.Error(t, err)
	assert.Nil(t, eastGone)

	westStill, err := backend.DescribeClusters(ctxWest, "shared-cluster")
	require.NoError(t, err)
	require.Len(t, westStill, 1)
	assert.Equal(t, "db.r6g.xlarge", westStill[0].NodeType)
}

func TestMemoryDBACLRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := memdbCtxRegion("us-east-1")
	ctxWest := memdbCtxRegion("us-west-2")

	eastACL, err := backend.CreateACL(ctxEast, &createACLRequest{ACLName: "shared-acl"})
	require.NoError(t, err)
	assert.Contains(t, eastACL.ARN, "us-east-1")

	westACLs, err := backend.DescribeACLs(ctxWest, "shared-acl")
	require.Error(t, err)
	assert.Nil(t, westACLs)

	eastACLs, err := backend.DescribeACLs(ctxEast, "shared-acl")
	require.NoError(t, err)
	require.Len(t, eastACLs, 1)
}

func TestMemoryDBDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	_, err := backend.CreateCluster(context.Background(), &createClusterRequest{
		ClusterName: "def-cluster",
		NodeType:    "db.r6g.large",
	})
	require.NoError(t, err)

	list, err := backend.DescribeClusters(memdbCtxRegion("eu-central-1"), "def-cluster")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "eu-central-1", list[0].Region)

	other, err := backend.DescribeClusters(memdbCtxRegion("ap-south-1"), "def-cluster")
	require.Error(t, err)
	assert.Nil(t, other)
}

// TestSnapshotRestore_CrossRegion verifies per-region store.Table state
// (which is deliberately NOT registered on b.registry -- see
// store_setup.go's doc comment) round-trips through Snapshot/Restore for
// more than one region. This complements the exported-API full-state test in
// persistence_test.go, which only exercises the backend's default region.
func TestSnapshotRestore_CrossRegion(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("123456789012", "us-east-1")

	ctxEast := memdbCtxRegion("us-east-1")
	ctxWest := memdbCtxRegion("us-west-2")

	_, err := original.CreateCluster(ctxEast, &createClusterRequest{
		ClusterName: "cr-cluster-east",
		NodeType:    "db.r6g.large",
	})
	require.NoError(t, err)
	_, err = original.CreateCluster(ctxWest, &createClusterRequest{
		ClusterName: "cr-cluster-west",
		NodeType:    "db.r6g.xlarge",
	})
	require.NoError(t, err)

	snap := original.Snapshot(context.Background())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(context.Background(), snap))

	eastAfter, err := fresh.DescribeClusters(ctxEast, "")
	require.NoError(t, err)
	require.Len(t, eastAfter, 1)
	assert.Equal(t, "cr-cluster-east", eastAfter[0].Name)

	westAfter, err := fresh.DescribeClusters(ctxWest, "")
	require.NoError(t, err)
	require.Len(t, westAfter, 1)
	assert.Equal(t, "cr-cluster-west", westAfter[0].Name)
}

// TestReset_RegisteredTableSurvivesRepeatedCalls guards against a
// regression where Reset accidentally re-registers a registry-backed table
// (multiRegionClusters, multiRegionParameterGroups, or serviceUpdates --
// store.Register panics on a duplicate name) instead of clearing it in place
// via b.registry.ResetAll().
func TestReset_RegisteredTableSurvivesRepeatedCalls(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")

	ctx := context.Background()

	_, err := b.CreateMultiRegionCluster(ctx, &createMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "reset-mrc",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		b.Reset()
		b.Reset()
	})

	mrcs, err := b.DescribeMultiRegionClusters(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, mrcs)

	_, err = b.CreateMultiRegionCluster(ctx, &createMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "post-reset-mrc",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)
}
