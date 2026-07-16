package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("111111111111", "us-west-2")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, glue.DatabaseCount(b))

	b.Reset()

	assert.Equal(t, 0, glue.DatabaseCount(b))
	assert.Equal(t, 0, glue.TableCount(b))
	assert.Equal(t, 0, glue.CrawlerCount(b))
	assert.Equal(t, 0, glue.JobCount(b))
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.ConnectionCount(b))
	assert.Equal(t, 0, glue.BlueprintCount(b))
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("111111111111", "us-west-2")

	for range 3 {
		_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, glue.DatabaseCount(b))

		b.Reset()

		assert.Equal(t, 0, glue.DatabaseCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("111111111111", "us-east-1")
	h := glue.NewHandler(backend)

	_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, glue.DatabaseCount(backend))
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, glue.DatabaseCount(b))
	assert.Equal(t, 0, glue.TableCount(b))
	assert.Equal(t, 0, glue.CrawlerCount(b))
	assert.Equal(t, 0, glue.JobCount(b))
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.TableVersionCount(b))
	assert.Equal(t, 0, glue.ConnectionCount(b))
	assert.Equal(t, 0, glue.BlueprintCount(b))
	assert.Equal(t, 0, glue.CustomEntityTypeCount(b))
	assert.Equal(t, 0, glue.DataQualityResultCount(b))
	assert.Equal(t, 0, glue.DevEndpointCount(b))

	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.DatabaseCount(b))
}

func TestRefinement1_SortedGetDatabases(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"gamma", "alpha", "beta"} {
		_, err := b.CreateDatabase(glue.DatabaseInput{Name: name}, nil)
		require.NoError(t, err)
	}

	dbs := b.GetDatabases()
	require.Len(t, dbs, 3)

	assert.Equal(t, "alpha", dbs[0].Name)
	assert.Equal(t, "beta", dbs[1].Name)
	assert.Equal(t, "gamma", dbs[2].Name)
}

func TestRefinement1_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)

	_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	backend2 := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := glue.NewHandler(backend2)

	err = h2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.DatabaseCount(backend2))
}
