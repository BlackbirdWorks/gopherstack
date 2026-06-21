package codestarconnections //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cscCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestCSCRegionIsolation proves that same-named connections and hosts created in
// two different regions are fully isolated: each region sees only its own
// resources, ARNs embed the correct region, and deleting in one region leaves
// the other untouched.
func TestCSCRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := cscCtxRegion("us-east-1")
	ctxWest := cscCtxRegion("us-west-2")

	// 1. Create a connection with the SAME name in both regions.
	eastConn, err := backend.CreateConnection(ctxEast, "shared-conn", "GitHub", "", nil)
	require.NoError(t, err)
	assert.Contains(t, eastConn.ConnectionArn, "us-east-1")

	westConn, err := backend.CreateConnection(ctxWest, "shared-conn", "Bitbucket", "", nil)
	require.NoError(t, err)
	assert.Contains(t, westConn.ConnectionArn, "us-west-2")

	// ARNs must differ (region-qualified) even though names match.
	assert.NotEqual(t, eastConn.ConnectionArn, westConn.ConnectionArn)

	// 2. Each region reads back its own provider type.
	eastList := backend.ListConnections(ctxEast, "", "")
	require.Len(t, eastList, 1)
	assert.Equal(t, "GitHub", eastList[0].ProviderType)

	westList := backend.ListConnections(ctxWest, "", "")
	require.Len(t, westList, 1)
	assert.Equal(t, "Bitbucket", westList[0].ProviderType)

	// 3. Deleting in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteConnection(ctxEast, eastConn.ConnectionArn))

	eastAfterDel := backend.ListConnections(ctxEast, "", "")
	assert.Empty(t, eastAfterDel)

	westAfterDel := backend.ListConnections(ctxWest, "", "")
	require.Len(t, westAfterDel, 1)
	assert.Equal(t, "Bitbucket", westAfterDel[0].ProviderType)

	// 4. Hosts with the same name are isolated too.
	eastHost, err := backend.CreateHost(
		ctxEast,
		"shared-host",
		"GitHubEnterpriseServer",
		"https://east.example.com",
		nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastHost.HostArn, "us-east-1")

	westHost, err := backend.CreateHost(
		ctxWest,
		"shared-host",
		"GitHubEnterpriseServer",
		"https://west.example.com",
		nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westHost.HostArn, "us-west-2")

	eastHosts := backend.ListHosts(ctxEast)
	require.Len(t, eastHosts, 1)
	assert.Equal(t, "https://east.example.com", eastHosts[0].ProviderEndpoint)

	westHosts := backend.ListHosts(ctxWest)
	require.Len(t, westHosts, 1)
	assert.Equal(t, "https://west.example.com", westHosts[0].ProviderEndpoint)
}

// TestCSCTagRegionIsolation proves that tag operations resolve the region from
// the ARN (not the context), so same-named connections in two regions carry
// independent tag sets.
func TestCSCTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := cscCtxRegion("us-east-1")
	ctxWest := cscCtxRegion("us-west-2")

	eastConn, err := backend.CreateConnection(ctxEast, "tag-conn", "GitHub", "", map[string]string{"env": "prod"})
	require.NoError(t, err)

	westConn, err := backend.CreateConnection(ctxWest, "tag-conn", "GitHub", "", map[string]string{"env": "staging"})
	require.NoError(t, err)

	// ARN-based lookup resolves to the region encoded in the ARN.
	eastTags, err := backend.ListTagsForResource(ctxEast, eastConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "prod", eastTags["env"])

	westTags, err := backend.ListTagsForResource(ctxWest, westConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "staging", westTags["env"])

	// Cross-region tag writes stay isolated: tagging the east ARN does not
	// affect the west connection.
	require.NoError(t, backend.TagResource(ctxEast, eastConn.ConnectionArn, map[string]string{"team": "infra"}))

	eastTagsAfter, err := backend.ListTagsForResource(ctxEast, eastConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "infra", eastTagsAfter["team"])

	westTagsAfter, err := backend.ListTagsForResource(ctxWest, westConn.ConnectionArn)
	require.NoError(t, err)
	assert.Empty(t, westTagsAfter["team"], "tagging east must not affect west")
}

// TestCSCDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region.
func TestCSCDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreateConnection(context.Background(), "def-conn", "GitHub", "", nil)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	list := backend.ListConnections(cscCtxRegion("eu-central-1"), "", "")
	require.Len(t, list, 1)

	// A different region sees nothing.
	other := backend.ListConnections(cscCtxRegion("ap-south-1"), "", "")
	assert.Empty(t, other)
}

// TestCSCRepositoryLinkRegionIsolation proves repository links and sync
// configurations are isolated per region.
func TestCSCRepositoryLinkRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := cscCtxRegion("us-east-1")
	ctxWest := cscCtxRegion("us-west-2")

	// Create repository links in both regions.
	eastLink, err := backend.CreateRepositoryLink(ctxEast, "conn-arn", "east-owner", "east-repo", "")
	require.NoError(t, err)

	westLink, err := backend.CreateRepositoryLink(ctxWest, "conn-arn", "west-owner", "west-repo", "")
	require.NoError(t, err)

	assert.NotEqual(t, eastLink.RepositoryLinkID, westLink.RepositoryLinkID)

	eastLinks := backend.ListRepositoryLinks(ctxEast)
	require.Len(t, eastLinks, 1)
	assert.Equal(t, "east-owner", eastLinks[0].OwnerID)

	westLinks := backend.ListRepositoryLinks(ctxWest)
	require.Len(t, westLinks, 1)
	assert.Equal(t, "west-owner", westLinks[0].OwnerID)

	// Create sync configurations in both regions.
	_, err = backend.CreateSyncConfiguration(
		ctxEast,
		"main",
		"cfg.yaml",
		eastLink.RepositoryLinkID,
		"east-stack",
		"arn:role",
		"CFN_STACK_SYNC",
	)
	require.NoError(t, err)

	_, err = backend.CreateSyncConfiguration(
		ctxWest,
		"main",
		"cfg.yaml",
		westLink.RepositoryLinkID,
		"east-stack",
		"arn:role",
		"CFN_STACK_SYNC",
	)
	require.NoError(t, err)

	// Each region sees only its own sync config.
	eastCfg, err := backend.GetSyncConfiguration(ctxEast, "east-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, eastLink.RepositoryLinkID, eastCfg.RepositoryLinkID)

	westCfg, err := backend.GetSyncConfiguration(ctxWest, "east-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, westLink.RepositoryLinkID, westCfg.RepositoryLinkID)

	// Must delete sync config before deleting the link (AWS ResourceInUse semantics).
	require.NoError(t, backend.DeleteSyncConfiguration(ctxEast, "east-stack", "CFN_STACK_SYNC"))

	// Deleting the east link leaves the west link intact.
	require.NoError(t, backend.DeleteRepositoryLink(ctxEast, eastLink.RepositoryLinkID))

	eastLinksAfter := backend.ListRepositoryLinks(ctxEast)
	assert.Empty(t, eastLinksAfter)

	westLinksAfter := backend.ListRepositoryLinks(ctxWest)
	require.Len(t, westLinksAfter, 1)
}
