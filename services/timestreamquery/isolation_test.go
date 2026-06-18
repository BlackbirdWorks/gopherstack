package timestreamquery //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tsqCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestTimestreamQueryRegionIsolation proves that same-named scheduled queries
// created in two different regions are fully isolated: each region sees only its
// own queries, ARNs embed the correct region, and deleting in one region leaves
// the other untouched.
func TestTimestreamQueryRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := tsqCtxRegion("us-east-1")
	ctxWest := tsqCtxRegion("us-west-2")

	// 1. Create a scheduled query with the SAME name in both regions.
	eastSQ, err := backend.CreateScheduledQuery(
		ctxEast, "shared-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::000000000000:role/east",
		"", "", "", "", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastSQ.Arn, "us-east-1")

	westSQ, err := backend.CreateScheduledQuery(
		ctxWest, "shared-sq", "SELECT 2", "rate(2 hours)", "arn:aws:iam::000000000000:role/west",
		"", "", "", "", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westSQ.Arn, "us-west-2")

	// ARNs must differ (region-qualified) even though the names match.
	assert.NotEqual(t, eastSQ.Arn, westSQ.Arn)

	// 2. Each region reads back its own query string.
	eastDesc, err := backend.DescribeScheduledQuery(ctxEast, eastSQ.Arn)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", eastDesc.QueryString)
	assert.Contains(t, eastDesc.Arn, "us-east-1")

	westDesc, err := backend.DescribeScheduledQuery(ctxWest, westSQ.Arn)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 2", westDesc.QueryString)
	assert.Contains(t, westDesc.Arn, "us-west-2")

	// 3. Listing returns exactly one query per region.
	eastList := backend.ListScheduledQueriesFull(ctxEast)
	require.Len(t, eastList, 1)
	assert.Equal(t, "shared-sq", eastList[0].Name)
	assert.Contains(t, eastList[0].Arn, "us-east-1")

	westList := backend.ListScheduledQueriesFull(ctxWest)
	require.Len(t, westList, 1)
	assert.Equal(t, "shared-sq", westList[0].Name)
	assert.Contains(t, westList[0].Arn, "us-west-2")

	// 4. Delete in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteScheduledQuery(ctxEast, eastSQ.Arn))

	_, err = backend.DescribeScheduledQuery(ctxEast, eastSQ.Arn)
	require.Error(t, err, "east query must be gone after deletion")

	westStill, err := backend.DescribeScheduledQuery(ctxWest, westSQ.Arn)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 2", westStill.QueryString)
}

// TestTimestreamQueryTagRegionIsolation proves that tag operations on ARNs resolve
// the region from the ARN itself: tagging via a west-region ARN while holding an
// east-region context lands on the correct resource.
func TestTimestreamQueryTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := tsqCtxRegion("us-east-1")
	ctxWest := tsqCtxRegion("us-west-2")

	// Create one query in each region.
	eastSQ, err := backend.CreateScheduledQuery(
		ctxEast, "tag-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::000000000000:role/r",
		"", "", "", "", nil,
	)
	require.NoError(t, err)

	westSQ, err := backend.CreateScheduledQuery(
		ctxWest, "tag-sq", "SELECT 2", "rate(1 hour)", "arn:aws:iam::000000000000:role/r",
		"", "", "", "", nil,
	)
	require.NoError(t, err)

	// Tag the west query using the east context — region must be resolved from ARN.
	require.NoError(t, backend.TagResource(ctxEast, westSQ.Arn, map[string]string{"env": "west"}))

	// The tag lands on the west resource.
	westTags, err := backend.ListTagsForResource(ctxEast, westSQ.Arn)
	require.NoError(t, err)
	require.Len(t, westTags, 1)
	assert.Equal(t, "west", westTags[0]["Value"])

	// The east resource remains untagged.
	eastTags, err := backend.ListTagsForResource(ctxEast, eastSQ.Arn)
	require.NoError(t, err)
	assert.Empty(t, eastTags)
}

// TestTimestreamQueryDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestTimestreamQueryDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context → default region store.
	sq, err := backend.CreateScheduledQuery(
		context.Background(), "def-sq", "SELECT 1", "rate(1 hour)",
		"arn:aws:iam::000000000000:role/r", "", "", "", "", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, sq.Arn, "eu-central-1")

	// Reading via the explicit default region sees it.
	list := backend.ListScheduledQueriesFull(tsqCtxRegion("eu-central-1"))
	require.Len(t, list, 1)
	assert.Equal(t, "def-sq", list[0].Name)

	// A different region sees nothing.
	other := backend.ListScheduledQueriesFull(tsqCtxRegion("ap-south-1"))
	assert.Empty(t, other)
}
