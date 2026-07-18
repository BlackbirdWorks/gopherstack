package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestBatchGetDevEndpoints_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddDevEndpointInternal(&glue.DevEndpoint{EndpointName: "ep1", Status: "READY"})

	found, missing := b.BatchGetDevEndpoints([]string{"ep1", "ep2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "ep1", found[0].EndpointName)
	assert.Len(t, missing, 1)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("db1", glue.TableInput{Name: "tbl1"})
	require.NoError(t, err)
	b.AddBlueprintInternal(&glue.Blueprint{Name: "bp1"})
	b.AddDevEndpointInternal(&glue.DevEndpoint{EndpointName: "ep1", Status: "READY"})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := glue.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.DatabaseCount(b2))
	assert.Equal(t, 1, glue.TableCount(b2))
	assert.Equal(t, 1, glue.BlueprintCount(b2))
	assert.Equal(t, 1, glue.DevEndpointCount(b2))
}
