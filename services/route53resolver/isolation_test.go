package route53resolver //nolint:testpackage // needs access to unexported regionContextKey.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func r53rCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestRoute53ResolverRegionIsolation proves that same-named resolver resources
// created in two different regions are fully isolated: each region sees only
// its own resources, ARNs embed the correct region, and deleting in one region
// leaves the other untouched.
func TestRoute53ResolverRegionIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := r53rCtxRegion("us-east-1")
	ctxWest := r53rCtxRegion("us-west-2")

	// 1. Create a resolver endpoint with the SAME name in both regions.
	eastEP, err := b.CreateResolverEndpoint(
		ctxEast, "shared-ep", "INBOUND", "vpc-east", nil, nil, "IPV4", nil, "", "", "",
	)
	require.NoError(t, err)
	assert.Contains(t, eastEP.ARN, "us-east-1")

	westEP, err := b.CreateResolverEndpoint(
		ctxWest, "shared-ep", "OUTBOUND", "vpc-west", nil, nil, "IPV4", nil, "", "", "",
	)
	require.NoError(t, err)
	assert.Contains(t, westEP.ARN, "us-west-2")

	// ARNs must differ (region-qualified) even though names match.
	assert.NotEqual(t, eastEP.ARN, westEP.ARN)

	// 2. Each region lists only its own endpoint.
	eastList := b.ListResolverEndpoints(ctxEast)
	require.Len(t, eastList, 1)
	assert.Equal(t, "INBOUND", eastList[0].Direction)

	westList := b.ListResolverEndpoints(ctxWest)
	require.Len(t, westList, 1)
	assert.Equal(t, "OUTBOUND", westList[0].Direction)

	// 3. GetResolverEndpoint by ID is region-scoped.
	gotEast, err := b.GetResolverEndpoint(ctxEast, eastEP.ID)
	require.NoError(t, err)
	assert.Equal(t, eastEP.ID, gotEast.ID)

	_, err = b.GetResolverEndpoint(ctxWest, eastEP.ID)
	require.Error(t, err, "east endpoint must not be visible from the west region")

	// 4. Create a resolver rule with the same name in both regions.
	eastRule, err := b.CreateResolverRule(ctxEast, "shared-rule", "example.com", "SYSTEM", "", "", nil)
	require.NoError(t, err)
	assert.Contains(t, eastRule.ARN, "us-east-1")

	westRule, err := b.CreateResolverRule(ctxWest, "shared-rule", "example.com", "SYSTEM", "", "", nil)
	require.NoError(t, err)
	assert.Contains(t, westRule.ARN, "us-west-2")

	assert.NotEqual(t, eastRule.ARN, westRule.ARN)

	eastRules := b.ListResolverRules(ctxEast)
	require.Len(t, eastRules, 1)

	westRules := b.ListResolverRules(ctxWest)
	require.Len(t, westRules, 1)

	// 5. Deleting the endpoint in us-east-1 must not affect us-west-2.
	require.NoError(t, b.DeleteResolverEndpoint(ctxEast, eastEP.ID))

	eastGone := b.ListResolverEndpoints(ctxEast)
	assert.Empty(t, eastGone)

	westStill := b.ListResolverEndpoints(ctxWest)
	require.Len(t, westStill, 1)
	assert.Equal(t, "OUTBOUND", westStill[0].Direction)
}

// TestRoute53ResolverTagRegionIsolation proves that tags and firewall resources
// are scoped to the request region: resources tagged in one region are not
// visible when queried from another region.
func TestRoute53ResolverTagRegionIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := r53rCtxRegion("us-east-1")
	ctxWest := r53rCtxRegion("us-west-2")

	// Create a firewall rule group in each region with the same name.
	eastGrp, err := b.CreateFirewallRuleGroup(ctxEast, "shared-grp", "req-east")
	require.NoError(t, err)
	assert.Contains(t, eastGrp.ARN, "us-east-1")

	westGrp, err := b.CreateFirewallRuleGroup(ctxWest, "shared-grp", "req-west")
	require.NoError(t, err)
	assert.Contains(t, westGrp.ARN, "us-west-2")

	assert.NotEqual(t, eastGrp.ARN, westGrp.ARN)

	// Tag the east group.
	require.NoError(t, b.TagResource(ctxEast, eastGrp.ARN, []svcTags.KV{{Key: "env", Value: "prod"}}))

	eastTags := b.ListTagsForResource(ctxEast, eastGrp.ARN)
	require.Len(t, eastTags, 1)
	assert.Equal(t, "env", eastTags[0].Key)

	// The east ARN is not resolvable from the west region.
	westTags := b.ListTagsForResource(ctxWest, eastGrp.ARN)
	assert.Empty(t, westTags, "east ARN must not be tag-resolvable from the west region")

	// Each region lists only its own groups.
	eastGroups := b.ListFirewallRuleGroups(ctxEast)
	require.Len(t, eastGroups, 1)

	westGroups := b.ListFirewallRuleGroups(ctxWest)
	require.Len(t, westGroups, 1)
}

// TestRoute53ResolverDefaultRegionFallback verifies that a context without a
// region falls back to the backend's configured default region.
func TestRoute53ResolverDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context → default region store.
	ep, err := b.CreateResolverEndpoint(
		context.Background(), "def-ep", "INBOUND", "vpc-def", nil, nil, "IPV4", nil, "", "", "",
	)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	list := b.ListResolverEndpoints(r53rCtxRegion("eu-central-1"))
	require.Len(t, list, 1)
	assert.Equal(t, ep.ID, list[0].ID)

	// A different region sees nothing.
	other := b.ListResolverEndpoints(r53rCtxRegion("ap-south-1"))
	assert.Empty(t, other)
}
