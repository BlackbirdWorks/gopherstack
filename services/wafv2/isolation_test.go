package wafv2 //nolint:testpackage // needs access to unexported regionContextKey.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wafv2CtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func TestWAFv2RegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := wafv2CtxRegion("us-east-1")
	ctxWest := wafv2CtxRegion("us-west-2")

	// Create same-named WebACL in both regions.
	eastACL, err := backend.CreateWebACL(ctxEast, "shared-acl", ScopeRegional, "",
		[]byte(`{"Allow":{}}`), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastACL.ARN, "us-east-1")

	westACL, err := backend.CreateWebACL(ctxWest, "shared-acl", ScopeRegional, "",
		[]byte(`{"Allow":{}}`), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westACL.ARN, "us-west-2")

	assert.NotEqual(t, eastACL.ARN, westACL.ARN)

	// Each region lists only its own WebACLs.
	eastList := backend.ListWebACLs(ctxEast)
	require.Len(t, eastList, 1)

	westList := backend.ListWebACLs(ctxWest)
	require.Len(t, westList, 1)

	// Delete in east does not affect west.
	require.NoError(t, backend.DeleteWebACL(ctxEast, eastACL.ID, ""))
	assert.Empty(t, backend.ListWebACLs(ctxEast))
	assert.Len(t, backend.ListWebACLs(ctxWest), 1)

	// IPSet isolation.
	eastIP, err := backend.CreateIPSet(ctxEast, "shared-ip", ScopeRegional, "", IPVersionIPv4, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastIP.ARN, "us-east-1")

	_, err = backend.CreateIPSet(ctxWest, "shared-ip", ScopeRegional, "", IPVersionIPv4, nil, nil)
	require.NoError(t, err)

	assert.Len(t, backend.ListIPSets(ctxEast), 1)
	assert.Len(t, backend.ListIPSets(ctxWest), 1)
}

func TestWAFv2DefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context → default region.
	acl, err := backend.CreateWebACL(context.Background(), "default-acl", ScopeRegional, "",
		[]byte(`{"Allow":{}}`), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Explicit default region sees it.
	list := backend.ListWebACLs(wafv2CtxRegion("eu-central-1"))
	require.Len(t, list, 1)
	assert.Contains(t, acl.ARN, "eu-central-1")

	// Other region sees nothing.
	assert.Empty(t, backend.ListWebACLs(wafv2CtxRegion("ap-south-1")))
}
