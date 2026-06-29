package elasticsearch //nolint:testpackage // needs access to the unexported region context key.

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

// TestElasticsearchDomainRegionIsolation proves that same-named domains in two
// regions are fully isolated: each region sees only its own domain (with its own
// region-scoped ARN, endpoint and version), and deleting in one region leaves the
// other intact.
func TestElasticsearchDomainRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	const (
		eastVersion = "7.10"
		westVersion = "7.1"
	)

	// 1. Create a domain named "search1" in us-east-1.
	eastDomain, err := backend.CreateDomain(
		ctxEast,
		CreateDomainInput{Name: "search1", ElasticsearchVersion: eastVersion},
	)
	require.NoError(t, err)
	assert.Contains(t, eastDomain.ARN, "us-east-1")
	assert.Contains(t, eastDomain.Endpoint, "us-east-1")
	assert.Equal(t, eastVersion, eastDomain.ElasticsearchVersion)

	// 2. Create a domain with the SAME NAME in us-west-2 with a different version.
	westDomain, err := backend.CreateDomain(
		ctxWest,
		CreateDomainInput{Name: "search1", ElasticsearchVersion: westVersion},
	)
	require.NoError(t, err)
	assert.Contains(t, westDomain.ARN, "us-west-2")
	assert.Contains(t, westDomain.Endpoint, "us-west-2")
	assert.Equal(t, westVersion, westDomain.ElasticsearchVersion)

	// 3. us-east-1 sees only its own domain with its own ARN and version.
	eastNames := backend.ListDomainNames(ctxEast)
	require.Len(t, eastNames, 1)
	assert.Equal(t, "search1", eastNames[0])

	eastGet, err := backend.DescribeDomain(ctxEast, "search1")
	require.NoError(t, err)
	assert.Equal(t, eastVersion, eastGet.ElasticsearchVersion)
	assert.Contains(t, eastGet.ARN, "us-east-1")

	// 4. us-west-2 sees only its own domain with its own ARN and version.
	westNames := backend.ListDomainNames(ctxWest)
	require.Len(t, westNames, 1)
	assert.Equal(t, "search1", westNames[0])

	westGet, err := backend.DescribeDomain(ctxWest, "search1")
	require.NoError(t, err)
	assert.Equal(t, westVersion, westGet.ElasticsearchVersion)
	assert.Contains(t, westGet.ARN, "us-west-2")

	// 5. Delete in us-east-1; us-west-2 still has its domain.
	_, err = backend.DeleteDomain(ctxEast, "search1")
	require.NoError(t, err)

	_, err = backend.DescribeDomain(ctxEast, "search1")
	require.ErrorIs(t, err, ErrDomainNotFound)

	westAfter, err := backend.DescribeDomain(ctxWest, "search1")
	require.NoError(t, err)
	assert.Contains(t, westAfter.ARN, "us-west-2")
}

// TestElasticsearchTagRegionIsolation proves that ARN-addressed tag operations
// resolve the region from the ARN itself, so a tag written via a us-east-1 request
// context lands on the us-west-2 domain when the ARN names us-west-2.
func TestElasticsearchTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// Same-named domain in both regions.
	eastDomain, err := backend.CreateDomain(ctxEast, CreateDomainInput{Name: "shared-dom"})
	require.NoError(t, err)

	westDomain, err := backend.CreateDomain(ctxWest, CreateDomainInput{Name: "shared-dom"})
	require.NoError(t, err)
	require.NotEqual(t, eastDomain.ARN, westDomain.ARN)

	// Tag the us-west-2 domain via its ARN, using a us-east-1 request context.
	// The region is resolved from the ARN, so the tag must land in us-west-2.
	require.NoError(t, backend.AddTags(ctxEast, westDomain.ARN, map[string]string{"env": "west"}))

	westTags, err := backend.ListTags(ctxEast, westDomain.ARN)
	require.NoError(t, err)
	assert.Equal(t, "west", westTags["env"])

	// The us-east-1 domain (different ARN) must remain untagged.
	eastTags, err := backend.ListTags(ctxEast, eastDomain.ARN)
	require.NoError(t, err)
	assert.Empty(t, eastTags)
}

// TestElasticsearchPackageRegionIsolation proves packages are region-isolated:
// a package created in one region is invisible to another region.
func TestElasticsearchPackageRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	pkg, err := backend.CreatePackage(ctxEast, "dict", "TXT-DICTIONARY", "east dictionary")
	require.NoError(t, err)

	// us-east-1 sees the package.
	eastPkgs := backend.DescribePackages(ctxEast, nil)
	require.Len(t, eastPkgs, 1)
	assert.Equal(t, pkg.ID, eastPkgs[0].ID)

	// us-west-2 sees no packages.
	westPkgs := backend.DescribePackages(ctxWest, nil)
	assert.Empty(t, westPkgs)

	// A same-named package can be created independently in us-west-2.
	_, err = backend.CreatePackage(ctxWest, "dict", "TXT-DICTIONARY", "west dictionary")
	require.NoError(t, err)
	assert.Len(t, backend.DescribePackages(ctxWest, nil), 1)
}
