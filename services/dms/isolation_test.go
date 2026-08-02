package dms //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dmsCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestDMSRegionIsolation proves that same-named DMS resources created in two
// different regions are fully isolated: each region sees only its own
// resources, ARNs embed the correct region, and deleting in one region leaves
// the other untouched.
func TestDMSRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := dmsCtxRegion("us-east-1")
	ctxWest := dmsCtxRegion("us-west-2")

	// 1. Create a replication instance with the SAME identifier in both regions.
	eastRI, err := backend.CreateReplicationInstance(
		ctxEast, "shared-ri", "dms.t3.medium", "", "", 0, false, false, false, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastRI.ReplicationInstanceArn, "us-east-1")

	westRI, err := backend.CreateReplicationInstance(
		ctxWest, "shared-ri", "dms.r5.large", "", "", 0, false, false, false, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westRI.ReplicationInstanceArn, "us-west-2")

	// ARNs must differ (region-qualified) even though identifiers match.
	assert.NotEqual(t, eastRI.ReplicationInstanceArn, westRI.ReplicationInstanceArn)

	// 2. Each region reads back its own instance class.
	eastList, err := backend.DescribeReplicationInstances(ctxEast, "shared-ri")
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "dms.t3.medium", eastList[0].ReplicationInstanceClass)
	assert.Equal(t, "us-east-1", eastList[0].Region)

	westList, err := backend.DescribeReplicationInstances(ctxWest, "shared-ri")
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "dms.r5.large", westList[0].ReplicationInstanceClass)
	assert.Equal(t, "us-west-2", westList[0].Region)

	// 3. Listing without a filter returns exactly one instance per region.
	eastAll, err := backend.DescribeReplicationInstances(ctxEast, "")
	require.NoError(t, err)
	require.Len(t, eastAll, 1)

	westAll, err := backend.DescribeReplicationInstances(ctxWest, "")
	require.NoError(t, err)
	require.Len(t, westAll, 1)

	// 4. Endpoints with the same identifier are isolated too.
	_, err = backend.CreateEndpoint(ctxEast, "shared-ep", "source", "mysql", "", "", "", "", 0, nil)
	require.NoError(t, err)
	_, err = backend.CreateEndpoint(ctxWest, "shared-ep", "target", "postgres", "", "", "", "", 0, nil)
	require.NoError(t, err)

	eastEP, err := backend.DescribeEndpoints(ctxEast, "shared-ep")
	require.NoError(t, err)
	require.Len(t, eastEP, 1)
	assert.Equal(t, "source", eastEP[0].EndpointType)
	assert.Equal(t, "mysql", eastEP[0].EngineName)

	westEP, err := backend.DescribeEndpoints(ctxWest, "shared-ep")
	require.NoError(t, err)
	require.Len(t, westEP, 1)
	assert.Equal(t, "target", westEP[0].EndpointType)
	assert.Equal(t, "postgres", westEP[0].EngineName)

	// 5. Deleting the replication instance in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteReplicationInstance(ctxEast, "shared-ri"))

	eastGone, err := backend.DescribeReplicationInstances(ctxEast, "shared-ri")
	require.NoError(t, err)
	assert.Empty(t, eastGone)

	westStill, err := backend.DescribeReplicationInstances(ctxWest, "shared-ri")
	require.NoError(t, err)
	require.Len(t, westStill, 1)
	assert.Equal(t, "dms.r5.large", westStill[0].ReplicationInstanceClass)
}

// TestDMSTagAndConnectionRegionIsolation proves tag and connection operations
// are scoped to the request region: a connection tested in one region is not
// visible in another, and tags resolved by ARN only match within the region.
func TestDMSTagAndConnectionRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := dmsCtxRegion("us-east-1")
	ctxWest := dmsCtxRegion("us-west-2")

	eastRI, err := backend.CreateReplicationInstance(
		ctxEast, "conn-ri", "dms.t3.medium", "", "", 0, false, false, false, nil,
	)
	require.NoError(t, err)
	eastEP, err := backend.CreateEndpoint(ctxEast, "conn-ep", "source", "mysql", "", "", "", "", 0, nil)
	require.NoError(t, err)

	// TestConnection in us-east-1 succeeds.
	_, err = backend.TestConnection(ctxEast, eastRI.ReplicationInstanceArn, eastEP.EndpointArn)
	require.NoError(t, err)

	eastConns, err := backend.DescribeConnections(ctxEast, "", "")
	require.NoError(t, err)
	require.Len(t, eastConns, 1)

	// us-west-2 sees no connections.
	westConns, err := backend.DescribeConnections(ctxWest, "", "")
	require.NoError(t, err)
	assert.Empty(t, westConns)

	// The east instance ARN does not resolve for tags in the west region.
	require.NoError(
		t,
		backend.AddTagsToResource(ctxEast, eastRI.ReplicationInstanceArn, map[string]string{"env": "prod"}),
	)

	eastTags, err := backend.ListTagsForResource(ctxEast, eastRI.ReplicationInstanceArn)
	require.NoError(t, err)
	assert.Equal(t, "prod", eastTags["env"])

	_, err = backend.ListTagsForResource(ctxWest, eastRI.ReplicationInstanceArn)
	require.Error(t, err, "east ARN must not be tag-resolvable from the west region")
}

// TestDMSDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region.
func TestDMSDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreateReplicationInstance(
		context.Background(), "def-ri", "dms.t3.medium", "", "", 0, false, false, false, nil,
	)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	list, err := backend.DescribeReplicationInstances(dmsCtxRegion("eu-central-1"), "def-ri")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "eu-central-1", list[0].Region)

	// A different region sees nothing.
	other, err := backend.DescribeReplicationInstances(dmsCtxRegion("ap-south-1"), "def-ri")
	require.NoError(t, err)
	assert.Empty(t, other)
}
