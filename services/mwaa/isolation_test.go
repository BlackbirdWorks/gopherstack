package mwaa //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isoCtxRegion returns a context carrying the given AWS region under the
// unexported region context key, mirroring what the handler injects per request.
func isoCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// newIsoCreateReq returns a minimal valid create request for isolation tests.
func newIsoCreateReq() *createEnvironmentRequest {
	return &createEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::000000000000:role/mwaa",
		SourceBucketArn:  "arn:aws:s3:::mwaa-bucket",
	}
}

func TestMWAAEnvironmentRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("us-east-1", "000000000000")

	ctxEast := isoCtxRegion("us-east-1")
	ctxWest := isoCtxRegion("us-west-2")

	// 1. Create an environment named "env1" in us-east-1.
	eastEnv, err := backend.CreateEnvironment(ctxEast, "env1", newIsoCreateReq())
	require.NoError(t, err)
	assert.Contains(t, eastEnv.ARN, "us-east-1")

	// 2. Create an environment with the SAME NAME in us-west-2.
	westReq := newIsoCreateReq()
	westReq.EnvironmentClass = "mw1.large"
	westEnv, err := backend.CreateEnvironment(ctxWest, "env1", westReq)
	require.NoError(t, err)
	assert.Contains(t, westEnv.ARN, "us-west-2")

	// 3. Each region sees only its own environment.
	eastList, err := backend.ListEnvironments(ctxEast)
	require.NoError(t, err)
	require.Equal(t, []string{"env1"}, eastList)

	westList, err := backend.ListEnvironments(ctxWest)
	require.NoError(t, err)
	require.Equal(t, []string{"env1"}, westList)

	// 4. Get returns the region-specific environment (distinct ARN + class).
	gotEast, err := backend.GetEnvironment(ctxEast, "env1")
	require.NoError(t, err)
	assert.Contains(t, gotEast.ARN, "us-east-1")
	assert.Equal(t, defaultEnvironmentClass, gotEast.EnvironmentClass)

	gotWest, err := backend.GetEnvironment(ctxWest, "env1")
	require.NoError(t, err)
	assert.Contains(t, gotWest.ARN, "us-west-2")
	assert.Equal(t, "mw1.large", gotWest.EnvironmentClass)

	// 5. Deleting in us-east-1 leaves us-west-2's environment intact.
	_, err = backend.DeleteEnvironment(ctxEast, "env1")
	require.NoError(t, err)

	_, err = backend.GetEnvironment(ctxEast, "env1")
	require.ErrorIs(t, err, ErrEnvironmentNotFound)

	stillWest, err := backend.GetEnvironment(ctxWest, "env1")
	require.NoError(t, err)
	assert.Contains(t, stillWest.ARN, "us-west-2")
}

func TestMWAATagsAndMetricsRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("us-east-1", "000000000000")

	ctxEast := isoCtxRegion("us-east-1")
	ctxWest := isoCtxRegion("us-west-2")

	eastEnv, err := backend.CreateEnvironment(ctxEast, "shared", newIsoCreateReq())
	require.NoError(t, err)

	westEnv, err := backend.CreateEnvironment(ctxWest, "shared", newIsoCreateReq())
	require.NoError(t, err)

	// Tag the us-east-1 environment only.
	require.NoError(t, backend.TagResource(ctxEast, eastEnv.ARN, map[string]string{"team": "east"}))

	eastTags, err := backend.ListTagsForResource(ctxEast, eastEnv.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "east"}, eastTags)

	// us-west-2 environment has no tags and its ARN is unknown to the us-east-1 store.
	westTags, err := backend.ListTagsForResource(ctxWest, westEnv.ARN)
	require.NoError(t, err)
	assert.Empty(t, westTags)

	// The us-east-1 ARN is not resolvable from the us-west-2 region store.
	_, err = backend.ListTagsForResource(ctxWest, eastEnv.ARN)
	require.ErrorIs(t, err, ErrEnvironmentNotFound)

	// Publish metrics only into us-west-2; us-east-1 must not see them.
	require.NoError(t, backend.PublishMetrics(ctxWest, "shared", &publishMetricsRequest{
		MetricData: []MetricDatum{{MetricName: "m1"}},
	}))

	westMetrics, err := backend.GetMetrics(ctxWest, "shared")
	require.NoError(t, err)
	require.Len(t, westMetrics, 1)

	eastMetrics, err := backend.GetMetrics(ctxEast, "shared")
	require.NoError(t, err)
	assert.Empty(t, eastMetrics)
}
