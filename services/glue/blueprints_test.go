package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestBatchGetBlueprints_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddBlueprintInternal(&glue.Blueprint{Name: "bp1", Status: "ACTIVE"})

	found, missing := b.BatchGetBlueprints([]string{"bp1", "bp2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "bp1", found[0].Name)
	assert.Len(t, missing, 1)
	assert.Contains(t, missing, "bp2")
}

func TestBatchGetBlueprints_NonNilSlices(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	found, missing := b.BatchGetBlueprints([]string{})

	assert.NotNil(t, found)
	assert.NotNil(t, missing)
}

func TestPersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := glue.NewInMemoryBackend("000000000000", "us-east-1")
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 0, glue.DatabaseCount(b2))
	assert.Equal(t, 0, glue.BlueprintCount(b2))
}
