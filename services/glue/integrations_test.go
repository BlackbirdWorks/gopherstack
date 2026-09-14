package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func Test_IntegrationResourceProperty_GetReturnsIndependentCopy(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer b.Close()

	const resourceArn = "arn:aws:glue:us-east-1:000000000000:integration/test"

	_, err := b.CreateIntegrationResourceProperty(
		resourceArn,
		map[string]any{"k": "v1"},
		nil,
	)
	require.NoError(t, err)

	got, err := b.GetIntegrationResourceProperty(resourceArn)
	require.NoError(t, err)
	require.Equal(t, "v1", got.SourceProcessingProperties["k"])

	// UpdateIntegrationResourceProperty must not mutate a map already handed back
	// to a caller from Get/CreateIntegrationResourceProperty.
	_, err = b.UpdateIntegrationResourceProperty(resourceArn, map[string]any{"k": "v2"}, nil)
	require.NoError(t, err)
	assert.Equal(
		t, "v1", got.SourceProcessingProperties["k"], "GetIntegrationResourceProperty must return an independent copy",
	)

	fresh, err := b.GetIntegrationResourceProperty(resourceArn)
	require.NoError(t, err)
	assert.Equal(t, "v2", fresh.SourceProcessingProperties["k"])
}
