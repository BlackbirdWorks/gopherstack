package codeartifact //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func caCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func TestCodeArtifactRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := caCtxRegion("us-east-1")
	ctxWest := caCtxRegion("us-west-2")

	// 1. Create a domain named "shared" in us-east-1.
	eastDomain, err := backend.CreateDomain(ctxEast, "shared", "", nil)
	require.NoError(t, err)
	assert.Contains(t, eastDomain.ARN, "us-east-1")
	assert.Equal(t, "us-east-1", eastDomain.Region)

	// 2. Create a domain with the SAME NAME in us-west-2.
	westDomain, err := backend.CreateDomain(ctxWest, "shared", "", nil)
	require.NoError(t, err)
	assert.Contains(t, westDomain.ARN, "us-west-2")
	assert.Equal(t, "us-west-2", westDomain.Region)

	// 3. us-east-1 sees only its own domain.
	eastList := backend.ListDomains(ctxEast)
	require.Len(t, eastList, 1)
	assert.Equal(t, "shared", eastList[0].Name)
	assert.Contains(t, eastList[0].ARN, "us-east-1")

	// 4. us-west-2 sees only its own domain.
	westList := backend.ListDomains(ctxWest)
	require.Len(t, westList, 1)
	assert.Equal(t, "shared", westList[0].Name)
	assert.Contains(t, westList[0].ARN, "us-west-2")

	// 5. Delete in us-east-1; us-west-2's domain remains.
	_, err = backend.DeleteDomain(ctxEast, "shared")
	require.NoError(t, err)

	_, err = backend.DescribeDomain(ctxEast, "shared")
	require.Error(t, err)

	stillThere, err := backend.DescribeDomain(ctxWest, "shared")
	require.NoError(t, err)
	assert.Equal(t, "shared", stillThere.Name)
	assert.Contains(t, stillThere.ARN, "us-west-2")
}

func TestCodeArtifactRepositoryRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := caCtxRegion("us-east-1")
	ctxWest := caCtxRegion("us-west-2")

	_, err := backend.CreateDomain(ctxEast, "d", "", nil)
	require.NoError(t, err)
	_, err = backend.CreateDomain(ctxWest, "d", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateRepository(ctxEast, "d", "repo", "east repo", nil, nil)
	require.NoError(t, err)
	_, err = backend.CreateRepository(ctxWest, "d", "repo", "west repo", nil, nil)
	require.NoError(t, err)

	eastRepo, err := backend.DescribeRepository(ctxEast, "d", "repo")
	require.NoError(t, err)
	assert.Equal(t, "east repo", eastRepo.Description)
	assert.Contains(t, eastRepo.ARN, "us-east-1")

	westRepo, err := backend.DescribeRepository(ctxWest, "d", "repo")
	require.NoError(t, err)
	assert.Equal(t, "west repo", westRepo.Description)
	assert.Contains(t, westRepo.ARN, "us-west-2")

	// Deleting the repo in us-east-1 leaves us-west-2's repo intact.
	_, err = backend.DeleteRepository(ctxEast, "d", "repo")
	require.NoError(t, err)

	_, err = backend.DescribeRepository(ctxEast, "d", "repo")
	require.Error(t, err)

	_, err = backend.DescribeRepository(ctxWest, "d", "repo")
	require.NoError(t, err)
}
