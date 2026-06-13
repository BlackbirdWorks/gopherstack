package cognitoidentity //nolint:testpackage // needs access to the unexported region context key.

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

// TestCognitoIdentityPoolRegionIsolation proves that same-named identity pools in two
// regions are fully isolated: each region sees only its own pool (with its own ARN),
// and deleting in one region leaves the other intact.
func TestCognitoIdentityPoolRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create a pool named "mypool" in us-east-1.
	eastPool, err := backend.CreateIdentityPool(ctxEast, "mypool", true, false, "", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastPool.ARN, "us-east-1")
	assert.Contains(t, eastPool.IdentityPoolID, "us-east-1")

	// 2. Create a pool with the SAME NAME in us-west-2.
	westPool, err := backend.CreateIdentityPool(ctxWest, "mypool", true, false, "", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westPool.ARN, "us-west-2")
	assert.Contains(t, westPool.IdentityPoolID, "us-west-2")

	// The two pools must have distinct IDs and ARNs.
	assert.NotEqual(t, eastPool.IdentityPoolID, westPool.IdentityPoolID)
	assert.NotEqual(t, eastPool.ARN, westPool.ARN)

	// 3. us-east-1 sees only its own pool.
	eastPools, _ := backend.ListIdentityPools(ctxEast, 0, "")
	require.Len(t, eastPools, 1)
	assert.Contains(t, eastPools[0].ARN, "us-east-1")

	// 4. us-west-2 sees only its own pool.
	westPools, _ := backend.ListIdentityPools(ctxWest, 0, "")
	require.Len(t, westPools, 1)
	assert.Contains(t, westPools[0].ARN, "us-west-2")

	// 5. Delete in us-east-1; us-west-2 still has its pool.
	require.NoError(t, backend.DeleteIdentityPool(ctxEast, eastPool.IdentityPoolID))

	_, err = backend.DescribeIdentityPool(ctxEast, eastPool.IdentityPoolID)
	require.ErrorIs(t, err, ErrIdentityPoolNotFound)

	westAfter, err := backend.DescribeIdentityPool(ctxWest, westPool.IdentityPoolID)
	require.NoError(t, err)
	assert.Contains(t, westAfter.ARN, "us-west-2")
}

// TestCognitoIdentityTagRegionIsolation proves that ARN-based tag operations
// resolve the region from the ARN, not from the request context.
func TestCognitoIdentityTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// Create pools in both regions.
	eastPool, err := backend.CreateIdentityPool(ctxEast, "pool1", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	westPool, err := backend.CreateIdentityPool(ctxWest, "pool1", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	// Tag the us-west-2 pool via its ARN using an east context; region must resolve from ARN.
	require.NoError(t, backend.TagResource(ctxEast, westPool.ARN, map[string]string{"env": "west"}))

	// The tag must land on the us-west-2 pool, not us-east-1's.
	westTags, err := backend.ListTagsForResource(ctxEast, westPool.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "west"}, westTags)

	eastTags, err := backend.ListTagsForResource(ctxEast, eastPool.ARN)
	require.NoError(t, err)
	assert.Empty(t, eastTags)
}
