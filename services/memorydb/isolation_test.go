package memorydb //nolint:testpackage

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
