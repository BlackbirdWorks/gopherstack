package rolesanywhere //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func raCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestRolesAnywhereRegionIsolation proves that same-named resources created in
// two different regions are fully isolated: each region sees only its own
// resources, ARNs embed the correct region, and deleting in one region leaves
// the other untouched.
func TestRolesAnywhereRegionIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := raCtxRegion("us-east-1")
	ctxWest := raCtxRegion("us-west-2")

	src := TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

	// 1. Create a trust anchor with the SAME name in both regions.
	eastTA, err := b.CreateTrustAnchor(ctxEast, "shared-anchor", src, nil)
	require.NoError(t, err)
	assert.Contains(t, eastTA.TrustAnchorArn, "us-east-1")

	westTA, err := b.CreateTrustAnchor(ctxWest, "shared-anchor", src, nil)
	require.NoError(t, err)
	assert.Contains(t, westTA.TrustAnchorArn, "us-west-2")

	// ARNs must differ (region-qualified) even though names match.
	assert.NotEqual(t, eastTA.TrustAnchorArn, westTA.TrustAnchorArn)

	// 2. Each region reads back exactly its own anchor.
	eastList, _, err := b.ListTrustAnchors(ctxEast, "", 0)
	require.NoError(t, err)
	require.Len(t, eastList, 1)

	westList, _, err := b.ListTrustAnchors(ctxWest, "", 0)
	require.NoError(t, err)
	require.Len(t, westList, 1)

	// 3. Same-named profiles are isolated.
	eastProfile, err := b.CreateProfile(
		ctxEast,
		"shared-profile",
		[]string{"arn:aws:iam::000000000000:role/East"},
		nil,
		nil,
		nil,
		"",
		false,
	)
	require.NoError(t, err)
	assert.Contains(t, eastProfile.ProfileArn, "us-east-1")

	westProfile, err := b.CreateProfile(
		ctxWest,
		"shared-profile",
		[]string{"arn:aws:iam::000000000000:role/West"},
		nil,
		nil,
		nil,
		"",
		false,
	)
	require.NoError(t, err)
	assert.Contains(t, westProfile.ProfileArn, "us-west-2")

	assert.NotEqual(t, eastProfile.ProfileArn, westProfile.ProfileArn)

	eastProfiles, _, err := b.ListProfiles(ctxEast, "", 0)
	require.NoError(t, err)
	require.Len(t, eastProfiles, 1)
	assert.Equal(t, "arn:aws:iam::000000000000:role/East", eastProfiles[0].RoleArns[0])

	westProfiles, _, err := b.ListProfiles(ctxWest, "", 0)
	require.NoError(t, err)
	require.Len(t, westProfiles, 1)
	assert.Equal(t, "arn:aws:iam::000000000000:role/West", westProfiles[0].RoleArns[0])

	// 4. Deleting the east trust anchor must not affect west.
	require.NoError(t, b.DeleteTrustAnchor(ctxEast, eastTA.TrustAnchorID))

	eastGone, _, err := b.ListTrustAnchors(ctxEast, "", 0)
	require.NoError(t, err)
	assert.Empty(t, eastGone)

	westStill, _, err := b.ListTrustAnchors(ctxWest, "", 0)
	require.NoError(t, err)
	require.Len(t, westStill, 1)

	// 5. Same-named CRLs are isolated.
	eastCRL, err := b.ImportCrl(ctxEast, "shared-crl", []byte("crldata"), eastTA.TrustAnchorArn, true, nil)
	require.NoError(t, err)
	assert.Contains(t, eastCRL.CrlArn, "us-east-1")

	westCRL, err := b.ImportCrl(ctxWest, "shared-crl", []byte("crldata"), westTA.TrustAnchorArn, true, nil)
	require.NoError(t, err)
	assert.Contains(t, westCRL.CrlArn, "us-west-2")

	assert.NotEqual(t, eastCRL.CrlArn, westCRL.CrlArn)

	eastCrls, _, err := b.ListCrls(ctxEast, "", 0)
	require.NoError(t, err)
	require.Len(t, eastCrls, 1)

	westCrls, _, err := b.ListCrls(ctxWest, "", 0)
	require.NoError(t, err)
	require.Len(t, westCrls, 1)
}

// TestRolesAnywhereDefaultRegionFallback verifies that a context without a
// region falls back to the backend's configured default region.
func TestRolesAnywhereDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "eu-central-1")

	src := TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

	// No region in context → default region store.
	_, err := b.CreateTrustAnchor(context.Background(), "fallback-anchor", src, nil)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	list, _, err := b.ListTrustAnchors(raCtxRegion("eu-central-1"), "", 0)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Contains(t, list[0].TrustAnchorArn, "eu-central-1")

	// A different region sees nothing.
	other, _, err := b.ListTrustAnchors(raCtxRegion("ap-south-1"), "", 0)
	require.NoError(t, err)
	assert.Empty(t, other)
}

// TestRolesAnywhereTagIsolation verifies that tags on ARN-bearing resources
// are routed by the region embedded in the ARN.
func TestRolesAnywhereTagIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := raCtxRegion("us-east-1")
	ctxWest := raCtxRegion("us-west-2")

	src := TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

	eastTA, err := b.CreateTrustAnchor(ctxEast, "tag-anchor-east", src, nil)
	require.NoError(t, err)

	westTA, err := b.CreateTrustAnchor(ctxWest, "tag-anchor-west", src, nil)
	require.NoError(t, err)

	// Tag the east anchor's ARN; region is derived from the ARN, not ctx.
	require.NoError(t, b.TagResource(ctxWest, eastTA.TrustAnchorArn, []TagEntry{{Key: "env", Value: "prod"}}))

	eastTags, err := b.ListTagsForResource(ctxEast, eastTA.TrustAnchorArn)
	require.NoError(t, err)
	require.Len(t, eastTags, 1)
	assert.Equal(t, "prod", eastTags[0].Value)

	// West anchor has no tags.
	westTags, err := b.ListTagsForResource(ctxWest, westTA.TrustAnchorArn)
	require.NoError(t, err)
	assert.Empty(t, westTags)
}
