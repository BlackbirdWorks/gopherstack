package ssm //nolint:testpackage // existing issue.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSMRegionIsolation(t *testing.T) { //nolint:paralleltest // existing issue.
	backend := NewInMemoryBackend()

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	// 1. Create parameter in us-east-1
	_, err := backend.PutParameter(ctxEast, &PutParameterInput{
		Name:  "param1",
		Value: "value-east",
		Type:  "String",
	})
	require.NoError(t, err)

	// 2. Create parameter with SAME NAME in us-west-2
	_, err = backend.PutParameter(ctxWest, &PutParameterInput{
		Name:  "param1",
		Value: "value-west",
		Type:  "String",
	})
	require.NoError(t, err)

	// 3. Verify us-east-1 has its value
	outEast, err := backend.GetParameter(ctxEast, &GetParameterInput{
		Name: "param1",
	})
	require.NoError(t, err)
	assert.Equal(t, "value-east", outEast.Parameter.Value)

	// 4. Verify us-west-2 has its value
	outWest, err := backend.GetParameter(ctxWest, &GetParameterInput{
		Name: "param1",
	})
	require.NoError(t, err)
	assert.Equal(t, "value-west", outWest.Parameter.Value)

	// 5. Delete in us-east-1 and verify still exists in us-west-2
	_, err = backend.DeleteParameter(ctxEast, &DeleteParameterInput{
		Name: "param1",
	})
	require.NoError(t, err)

	_, err = backend.GetParameter(ctxEast, &GetParameterInput{
		Name: "param1",
	})
	require.Error(t, err)

	outWest2, err := backend.GetParameter(ctxWest, &GetParameterInput{
		Name: "param1",
	})
	require.NoError(t, err)
	assert.Equal(t, "value-west", outWest2.Parameter.Value)
}
