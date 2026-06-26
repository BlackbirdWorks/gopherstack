package pipes //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pipesCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestPipesRegionIsolation proves that same-named pipes created in two different
// regions are fully isolated: each region sees only its own pipes, ARNs embed the
// correct region, and deleting in one region leaves the other untouched.
func TestPipesRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := pipesCtxRegion("us-east-1")
	ctxWest := pipesCtxRegion("us-west-2")

	// 1. Create a pipe with the SAME name in both regions.
	eastPipe, err := backend.CreatePipe(ctxEast, CreatePipeInput{
		Name:    "shared-pipe",
		Source:  "arn:aws:sqs:us-east-1:000000000000:east-q",
		Target:  "arn:aws:lambda:us-east-1:000000000000:function:east-fn",
		RoleARN: "arn:aws:iam::000000000000:role/r",
	})
	require.NoError(t, err)
	assert.Contains(t, eastPipe.ARN, "us-east-1")

	westPipe, err := backend.CreatePipe(ctxWest, CreatePipeInput{
		Name:    "shared-pipe",
		Source:  "arn:aws:sqs:us-west-2:000000000000:west-q",
		Target:  "arn:aws:lambda:us-west-2:000000000000:function:west-fn",
		RoleARN: "arn:aws:iam::000000000000:role/r",
	})
	require.NoError(t, err)
	assert.Contains(t, westPipe.ARN, "us-west-2")

	// ARNs must differ even though names match.
	assert.NotEqual(t, eastPipe.ARN, westPipe.ARN)

	// 2. Each region reads back its own source.
	eastGet, err := backend.GetPipe(ctxEast, "shared-pipe")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:east-q", eastGet.Source)
	assert.Equal(t, "us-east-1", eastGet.Region)

	westGet, err := backend.GetPipe(ctxWest, "shared-pipe")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sqs:us-west-2:000000000000:west-q", westGet.Source)
	assert.Equal(t, "us-west-2", westGet.Region)

	// 3. ListPipes for each region returns exactly one pipe.
	eastList, err := backend.ListPipes(ctxEast, ListPipesFilter{})
	require.NoError(t, err)
	require.Len(t, eastList.Pipes, 1)
	assert.Equal(t, "shared-pipe", eastList.Pipes[0].Name)
	assert.Equal(t, "us-east-1", eastList.Pipes[0].Region)

	westList, err := backend.ListPipes(ctxWest, ListPipesFilter{})
	require.NoError(t, err)
	require.Len(t, westList.Pipes, 1)
	assert.Equal(t, "shared-pipe", westList.Pipes[0].Name)
	assert.Equal(t, "us-west-2", westList.Pipes[0].Region)

	// 4. Deleting in us-east-1 must not affect us-west-2.
	_, err = backend.DeletePipe(ctxEast, "shared-pipe")
	require.NoError(t, err)

	// Wait for the async deletion to complete.
	require.Eventually(t, func() bool {
		_, getErr := backend.GetPipe(ctxEast, "shared-pipe")

		return getErr != nil
	}, 500*time.Millisecond, 5*time.Millisecond, "east pipe should be deleted")

	_, err = backend.GetPipe(ctxWest, "shared-pipe")
	require.NoError(t, err, "west pipe must survive east deletion")
}

// TestPipesTagRegionIsolation proves that tag operations resolve the pipe from
// the region embedded in the ARN, so an east ARN cannot be tagged from a west
// context.
func TestPipesTagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := pipesCtxRegion("us-east-1")
	ctxWest := pipesCtxRegion("us-west-2")

	eastPipe, err := backend.CreatePipe(ctxEast, CreatePipeInput{
		Name:    "tag-pipe",
		Source:  "arn:aws:sqs:us-east-1:000000000000:q",
		Target:  "arn:aws:lambda:us-east-1:000000000000:function:fn",
		RoleARN: "arn:aws:iam::000000000000:role/r",
	})
	require.NoError(t, err)

	err = backend.TagResource(ctxEast, eastPipe.ARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	tags, err := backend.ListTagsForResource(ctxEast, eastPipe.ARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])

	// West context cannot resolve the east ARN.
	_, err = backend.ListTagsForResource(ctxWest, eastPipe.ARN)
	require.Error(t, err, "east ARN must not be tag-resolvable from the west region")
}

// TestPipesDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region.
func TestPipesDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreatePipe(context.Background(), CreatePipeInput{
		Name:    "default-pipe",
		Source:  "arn:aws:sqs:eu-central-1:000000000000:q",
		Target:  "arn:aws:lambda:eu-central-1:000000000000:function:fn",
		RoleARN: "arn:aws:iam::000000000000:role/r",
	})
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	p, err := backend.GetPipe(pipesCtxRegion("eu-central-1"), "default-pipe")
	require.NoError(t, err)
	assert.Equal(t, "eu-central-1", p.Region)

	// A different region sees nothing.
	_, err = backend.GetPipe(pipesCtxRegion("ap-south-1"), "default-pipe")
	require.Error(t, err, "different region must not see default-region pipe")
}
