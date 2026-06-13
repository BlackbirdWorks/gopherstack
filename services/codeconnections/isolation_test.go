package codeconnections //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ccCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestCodeConnectionsRegionIsolation proves that same-named resources created in two
// different regions are fully isolated: each region sees only its own resources,
// ARNs embed the correct region, and deleting in one region leaves the other untouched.
func TestCodeConnectionsRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ccCtxRegion("us-east-1")
	ctxWest := ccCtxRegion("us-west-2")

	// 1. Create a connection with the SAME name in both regions.
	eastConn, err := backend.CreateConnection(ctxEast, "shared-conn", "GitHub", "", nil)
	require.NoError(t, err)
	assert.Contains(t, eastConn.ConnectionArn, "us-east-1")

	westConn, err := backend.CreateConnection(ctxWest, "shared-conn", "GitLab", "", nil)
	require.NoError(t, err)
	assert.Contains(t, westConn.ConnectionArn, "us-west-2")

	// ARNs must differ even though names match.
	assert.NotEqual(t, eastConn.ConnectionArn, westConn.ConnectionArn)

	// 2. Each region reads back its own provider type.
	eastList := backend.ListConnections(ctxEast, "", "")
	require.Len(t, eastList, 1)
	assert.Equal(t, "GitHub", eastList[0].ProviderType)

	westList := backend.ListConnections(ctxWest, "", "")
	require.Len(t, westList, 1)
	assert.Equal(t, "GitLab", westList[0].ProviderType)

	// 3. GetConnection resolves within the request region only.
	got, err := backend.GetConnection(ctxEast, eastConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "GitHub", got.ProviderType)

	_, err = backend.GetConnection(ctxWest, eastConn.ConnectionArn)
	require.Error(t, err, "east ARN must not resolve from the west region")

	// 4. Deleting in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteConnection(ctxEast, eastConn.ConnectionArn))

	eastGone := backend.ListConnections(ctxEast, "", "")
	assert.Empty(t, eastGone)

	westStill := backend.ListConnections(ctxWest, "", "")
	require.Len(t, westStill, 1)
	assert.Equal(t, "GitLab", westStill[0].ProviderType)
}

// TestCodeConnectionsHostRegionIsolation proves host resources are isolated per region.
func TestCodeConnectionsHostRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ccCtxRegion("us-east-1")
	ctxWest := ccCtxRegion("us-west-2")

	eastHost, err := backend.CreateHost(
		ctxEast,
		"shared-host",
		"GitHubEnterpriseServer",
		"https://ghe-east.example.com",
		nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastHost.HostArn, "us-east-1")

	westHost, err := backend.CreateHost(
		ctxWest,
		"shared-host",
		"GitHubEnterpriseServer",
		"https://ghe-west.example.com",
		nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westHost.HostArn, "us-west-2")

	assert.NotEqual(t, eastHost.HostArn, westHost.HostArn)

	eastHosts := backend.ListHosts(ctxEast)
	require.Len(t, eastHosts, 1)
	assert.Equal(t, "https://ghe-east.example.com", eastHosts[0].ProviderEndpoint)

	westHosts := backend.ListHosts(ctxWest)
	require.Len(t, westHosts, 1)
	assert.Equal(t, "https://ghe-west.example.com", westHosts[0].ProviderEndpoint)

	// Deleting in east must not touch west.
	require.NoError(t, backend.DeleteHost(ctxEast, eastHost.HostArn))
	assert.Empty(t, backend.ListHosts(ctxEast))
	require.Len(t, backend.ListHosts(ctxWest), 1)
}

// TestCodeConnectionsTagRegionIsolation proves tag operations are scoped to the request region.
func TestCodeConnectionsTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ccCtxRegion("us-east-1")
	ctxWest := ccCtxRegion("us-west-2")

	eastConn, err := backend.CreateConnection(ctxEast, "tag-conn", "GitHub", "", nil)
	require.NoError(t, err)

	require.NoError(
		t,
		backend.TagResource(ctxEast, eastConn.ConnectionArn, map[string]string{"env": "prod"}),
	)

	eastTags, err := backend.ListTagsForResource(ctxEast, eastConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "prod", eastTags["env"])

	_, err = backend.ListTagsForResource(ctxWest, eastConn.ConnectionArn)
	require.Error(t, err, "east ARN must not be tag-resolvable from the west region")
}

// TestCodeConnectionsDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestCodeConnectionsDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	_, err := backend.CreateConnection(context.Background(), "def-conn", "GitHub", "", nil)
	require.NoError(t, err)

	// The explicit default region sees it.
	list := backend.ListConnections(ccCtxRegion("eu-central-1"), "", "")
	require.Len(t, list, 1)

	// A different region sees nothing.
	other := backend.ListConnections(ccCtxRegion("ap-south-1"), "", "")
	assert.Empty(t, other)
}
