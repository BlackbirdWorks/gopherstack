package sagemaker //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sagemakrCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestSageMakerRegionIsolation proves that same-named SageMaker resources
// created in two different regions are fully isolated: each region sees only
// its own resources, ARNs embed the correct region, and deleting in one region
// leaves the other untouched.
func TestSageMakerRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := sagemakrCtxRegion("us-east-1")
	ctxWest := sagemakrCtxRegion("us-west-2")

	// 1. Create a Model with the SAME name in both regions.
	eastModel, err := backend.CreateModel(ctxEast, "shared-model", "arn:aws:iam::000000000000:role/role", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastModel.ModelARN, "us-east-1")

	westModel, err := backend.CreateModel(ctxWest, "shared-model", "arn:aws:iam::000000000000:role/role", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westModel.ModelARN, "us-west-2")

	// ARNs must differ (region-qualified) even though names match.
	assert.NotEqual(t, eastModel.ModelARN, westModel.ModelARN)

	// 2. Each region reads back its own model.
	eastDescribed, err := backend.DescribeModel(ctxEast, "shared-model")
	require.NoError(t, err)
	assert.Contains(t, eastDescribed.ModelARN, "us-east-1")

	westDescribed, err := backend.DescribeModel(ctxWest, "shared-model")
	require.NoError(t, err)
	assert.Contains(t, westDescribed.ModelARN, "us-west-2")

	// 3. Create an EndpointConfig with the same name in both regions.
	eastEC, err := backend.CreateEndpointConfig(ctxEast, "shared-ec", nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastEC.EndpointConfigARN, "us-east-1")

	westEC, err := backend.CreateEndpointConfig(ctxWest, "shared-ec", nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westEC.EndpointConfigARN, "us-west-2")

	// ARNs must differ.
	assert.NotEqual(t, eastEC.EndpointConfigARN, westEC.EndpointConfigARN)

	// Each region sees its own endpoint config.
	eastECDescribed, err := backend.DescribeEndpointConfig(ctxEast, "shared-ec")
	require.NoError(t, err)
	assert.Contains(t, eastECDescribed.EndpointConfigARN, "us-east-1")

	westECDescribed, err := backend.DescribeEndpointConfig(ctxWest, "shared-ec")
	require.NoError(t, err)
	assert.Contains(t, westECDescribed.EndpointConfigARN, "us-west-2")

	// 4. Deleting the model in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteModel(ctxEast, "shared-model"))

	_, err = backend.DescribeModel(ctxEast, "shared-model")
	require.Error(t, err, "east model should be gone after deletion")

	westStill, err := backend.DescribeModel(ctxWest, "shared-model")
	require.NoError(t, err)
	assert.Contains(t, westStill.ModelARN, "us-west-2")

	// 5. AddTags to a model ARN in us-east-1 context — after deletion east model
	// is gone, use west model ARN to verify us-east-1 cannot find it.
	err = backend.AddTags(ctxEast, westModel.ModelARN, map[string]string{"env": "staging"})
	require.Error(t, err, "east context must not resolve a west-region ARN")
}

// TestSageMakerDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestSageMakerDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreateModel(
		context.Background(),
		"def-model",
		"arn:aws:iam::000000000000:role/role",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	m, err := backend.DescribeModel(sagemakrCtxRegion("eu-central-1"), "def-model")
	require.NoError(t, err)
	assert.Contains(t, m.ModelARN, "eu-central-1")

	// A different region sees nothing.
	_, err = backend.DescribeModel(sagemakrCtxRegion("ap-south-1"), "def-model")
	require.Error(t, err, "ap-south-1 should not find a resource created in eu-central-1")
}
