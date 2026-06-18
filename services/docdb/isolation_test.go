package docdb //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func docdbCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestDocDBRegionIsolation proves that same-named DocDB resources created in two
// different regions are fully isolated: each region sees only its own resources,
// ARNs embed the correct region, and deleting in one region leaves the other
// untouched.
func TestDocDBRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := docdbCtxRegion("us-east-1")
	ctxWest := docdbCtxRegion("us-west-2")

	// 1. Create a cluster with the SAME identifier in both regions.
	eastCluster, err := backend.CreateDBCluster(
		ctxEast,
		"shared-cluster", "docdb", "", "", "", "", "", "",
		0, false, false, 1, "", "", nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastCluster.DBClusterArn, "us-east-1")

	westCluster, err := backend.CreateDBCluster(
		ctxWest,
		"shared-cluster", "docdb", "", "", "", "", "", "",
		0, false, false, 1, "", "", nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westCluster.DBClusterArn, "us-west-2")

	// ARNs must differ (region-qualified) even though identifiers match.
	assert.NotEqual(t, eastCluster.DBClusterArn, westCluster.DBClusterArn)

	// 2. Each region reads back its own cluster.
	eastList, err := backend.DescribeDBClusters(ctxEast, "shared-cluster")
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Contains(t, eastList[0].DBClusterArn, "us-east-1")

	westList, err := backend.DescribeDBClusters(ctxWest, "shared-cluster")
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Contains(t, westList[0].DBClusterArn, "us-west-2")

	// 3. Listing without a filter returns exactly one cluster per region.
	eastAll, err := backend.DescribeDBClusters(ctxEast, "")
	require.NoError(t, err)
	require.Len(t, eastAll, 1)

	westAll, err := backend.DescribeDBClusters(ctxWest, "")
	require.NoError(t, err)
	require.Len(t, westAll, 1)

	// 4. Instances with the same identifier are isolated too.
	_, err = backend.CreateDBInstance(ctxEast, "shared-inst", "shared-cluster", "", "", 0, nil, nil)
	require.NoError(t, err)
	_, err = backend.CreateDBInstance(ctxWest, "shared-inst", "shared-cluster", "", "", 0, nil, nil)
	require.NoError(t, err)

	eastInst, err := backend.DescribeDBInstances(ctxEast, "shared-inst", "")
	require.NoError(t, err)
	require.Len(t, eastInst, 1)
	assert.Contains(t, eastInst[0].DBInstanceArn, "us-east-1")

	westInst, err := backend.DescribeDBInstances(ctxWest, "shared-inst", "")
	require.NoError(t, err)
	require.Len(t, westInst, 1)
	assert.Contains(t, westInst[0].DBInstanceArn, "us-west-2")

	// 5. Deleting the instance then the cluster in us-east-1 must not affect us-west-2.
	_, err = backend.DeleteDBInstance(ctxEast, "shared-inst")
	require.NoError(t, err)

	_, err = backend.DeleteDBCluster(ctxEast, "shared-cluster", &DeleteDBClusterOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)

	eastGone, err := backend.DescribeDBClusters(ctxEast, "shared-cluster")
	require.ErrorIs(t, err, ErrClusterNotFound)
	assert.Empty(t, eastGone)

	westStill, err := backend.DescribeDBClusters(ctxWest, "shared-cluster")
	require.NoError(t, err)
	require.Len(t, westStill, 1)
	assert.Contains(t, westStill[0].DBClusterArn, "us-west-2")
}

// TestDocDBDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region.
func TestDocDBDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreateDBCluster(
		context.Background(),
		"def-cluster", "docdb", "", "", "", "", "", "",
		0, false, false, 1, "", "", nil, nil, nil,
	)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	list, err := backend.DescribeDBClusters(docdbCtxRegion("eu-central-1"), "def-cluster")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Contains(t, list[0].DBClusterArn, "eu-central-1")

	// A different region sees nothing.
	other, err := backend.DescribeDBClusters(docdbCtxRegion("ap-south-1"), "def-cluster")
	require.ErrorIs(t, err, ErrClusterNotFound)
	assert.Empty(t, other)
}
