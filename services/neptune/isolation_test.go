package neptune //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region under regionContextKey.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestNeptuneClusterRegionIsolation proves that same-named clusters in two regions
// are fully isolated: each region sees only its own cluster (with its own ARN and
// engine version), and deleting in one region leaves the other intact.
func TestNeptuneClusterRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	const (
		eastVersion = "1.3.1.0"
		westVersion = "1.2.1.0"
	)

	// 1. Create a cluster named "graph1" in us-east-1.
	eastCluster, err := backend.CreateDBCluster(
		ctxEast, "graph1", "", 0,
		DBClusterCreateOptions{EngineVersion: eastVersion},
	)
	require.NoError(t, err)
	assert.Contains(t, eastCluster.DBClusterArn, "us-east-1")
	assert.Equal(t, eastVersion, eastCluster.EngineVersion)

	// 2. Create a cluster with the SAME NAME in us-west-2 with a different version.
	westCluster, err := backend.CreateDBCluster(
		ctxWest, "graph1", "", 0,
		DBClusterCreateOptions{EngineVersion: westVersion},
	)
	require.NoError(t, err)
	assert.Contains(t, westCluster.DBClusterArn, "us-west-2")
	assert.Equal(t, westVersion, westCluster.EngineVersion)

	// 3. us-east-1 sees only its own cluster with its own ARN and version.
	eastList, err := backend.DescribeDBClusters(ctxEast, "", DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "graph1", eastList[0].DBClusterIdentifier)
	assert.Equal(t, eastVersion, eastList[0].EngineVersion)
	assert.Contains(t, eastList[0].DBClusterArn, "us-east-1")

	// 4. us-west-2 sees only its own cluster with its own ARN and version.
	westList, err := backend.DescribeDBClusters(ctxWest, "", DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "graph1", westList[0].DBClusterIdentifier)
	assert.Equal(t, westVersion, westList[0].EngineVersion)
	assert.Contains(t, westList[0].DBClusterArn, "us-west-2")

	// 5. Delete in us-east-1; us-west-2 still has its cluster.
	_, err = backend.DeleteDBCluster(ctxEast, "graph1", DBClusterDeleteOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)

	_, err = backend.DescribeDBClusters(ctxEast, "graph1", DBClusterFilters{})
	require.ErrorIs(t, err, ErrClusterNotFound)

	westAfter, err := backend.DescribeDBClusters(ctxWest, "graph1", DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, westAfter, 1)
	assert.Contains(t, westAfter[0].DBClusterArn, "us-west-2")
}

// TestNeptuneInstanceAndTagRegionIsolation proves DB instances and tags are
// region-isolated, including ARN-addressed tag operations resolving region from ARN.
func TestNeptuneInstanceAndTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// Same-named cluster + instance in both regions.
	for _, c := range []context.Context{ctxEast, ctxWest} {
		_, err := backend.CreateDBCluster(c, "c1", "", 0, DBClusterCreateOptions{})
		require.NoError(t, err)
		_, err = backend.CreateDBInstance(c, "i1", "c1", "", DBInstanceCreateOptions{})
		require.NoError(t, err)
	}

	// Each region sees exactly one instance.
	eastInsts, err := backend.DescribeDBInstances(ctxEast, "", "")
	require.NoError(t, err)
	require.Len(t, eastInsts, 1)
	assert.Contains(t, eastInsts[0].DBInstanceArn, "us-east-1")

	westInsts, err := backend.DescribeDBInstances(ctxWest, "", "")
	require.NoError(t, err)
	require.Len(t, westInsts, 1)
	assert.Contains(t, westInsts[0].DBInstanceArn, "us-west-2")

	// Tag the us-west-2 instance via its ARN; region is resolved from the ARN itself.
	require.NoError(
		t,
		backend.AddTagsToResource(ctxEast, westInsts[0].DBInstanceArn, []Tag{{Key: "env", Value: "west"}}),
	)

	// The tag must land on the us-west-2 instance, not us-east-1's.
	westTags, err := backend.ListTagsForResource(ctxEast, westInsts[0].DBInstanceArn)
	require.NoError(t, err)
	require.Len(t, westTags, 1)
	assert.Equal(t, "west", westTags[0].Value)

	eastTags, err := backend.ListTagsForResource(ctxEast, eastInsts[0].DBInstanceArn)
	require.NoError(t, err)
	assert.Empty(t, eastTags)
}

// TestNeptuneGlobalClusterIsNotRegionIsolated proves global clusters are
// partition-scoped: one created via a us-east-1 context is visible from us-west-2.
func TestNeptuneGlobalClusterIsNotRegionIsolated(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	_, err := backend.CreateGlobalCluster(ctxEast, "global1", "", "")
	require.NoError(t, err)

	// Visible regardless of the request region (global/partition-scoped).
	eastGlobals := backend.DescribeGlobalClusters(ctxEast)
	require.Len(t, eastGlobals, 1)

	westGlobals := backend.DescribeGlobalClusters(ctxWest)
	require.Len(t, westGlobals, 1)
	assert.Equal(t, "global1", westGlobals[0].GlobalClusterIdentifier)

	// Deleting from a different region context still removes the single global cluster.
	_, err = backend.DeleteGlobalCluster(ctxWest, "global1")
	require.NoError(t, err)
	assert.Empty(t, backend.DescribeGlobalClusters(ctxEast))
}
