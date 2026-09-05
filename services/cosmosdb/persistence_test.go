package cosmosdb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func TestBackend_SnapshotRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	_, err := b.CreateDocument("mydb", "mycoll", map[string]any{"id": "1", "pk": "a", "name": "hi"}, false)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cosmosdb.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	pk, err := cosmosdb.CanonicalPartitionKeyJSON("a")
	require.NoError(t, err)

	info, err := b2.GetDocument("mydb", "mycoll", pk, "1")
	require.NoError(t, err)
	assert.Equal(t, "hi", info.Body["name"])
}

// TestBackend_SnapshotRestore_Int64PrecisionRoundTrip is a regression test
// for the exact bug class AZURE.md's process rules call out: a document
// body's json.Number values must round-trip a save/restore cycle without
// losing precision above 2^53.
func TestBackend_SnapshotRestore_Int64PrecisionRoundTrip(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"beyond_float64_mantissa": "9007199254740993",
		"max_int64":               "9223372036854775807",
		"min_int64":               "-9223372036854775808",
		"negative_one":            "-1",
		"zero":                    "0",
	}

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	for name, raw := range values {
		body := decodeJSONBody(t, `{"id":"`+name+`","pk":"a","big":`+raw+`}`)
		_, err := b.CreateDocument("mydb", "mycoll", body, false)
		require.NoError(t, err, name)
	}

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cosmosdb.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	pk, err := cosmosdb.CanonicalPartitionKeyJSON("a")
	require.NoError(t, err)

	for name, want := range values {
		info, getErr := b2.GetDocument("mydb", "mycoll", pk, name)
		require.NoError(t, getErr, name)

		gotNum, ok := info.Body["big"].(json.Number)
		require.True(t, ok, name)
		assert.Equal(t, want, gotNum.String(), name)
	}
}

func TestBackend_Restore_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	err := b.Restore(t.Context(), []byte(`{"databases":{},"version":999}`))
	require.NoError(t, err)
	assert.Empty(t, b.ListDatabases())
}

func TestBackend_Restore_NullDatabaseRejected(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte(`{"databases":{"d":null},"version":1}`))
	require.ErrorIs(t, err, cosmosdb.ErrSnapshotDatabaseNull)
}

func TestBackend_Restore_NullContainerRejected(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	err := b.Restore(t.Context(),
		[]byte(`{"databases":{"d":{"ID":"d","Containers":{"c":null}}},"version":1}`))
	require.ErrorIs(t, err, cosmosdb.ErrSnapshotContainerNull)
}

// TestBackend_Restore_NullDocumentRejected covers a snapshot whose
// container's "Documents" map holds a JSON null value under a
// documentCompositeKey-shaped key (a JSON string array
// [PartitionKeyJSON, ID], itself JSON-string-escaped as an object key --
// mirrors services/azuretable's identical entityCompositeKey encoding).
func TestBackend_Restore_NullDocumentRejected(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte(
		`{"databases":{"d":{"ID":"d","Containers":{"c":{"ID":"c","PartitionKeyPath":"/pk",`+
			`"Documents":{"[\"pkval\",\"id1\"]":null}}}}},"version":1}`))
	require.ErrorIs(t, err, cosmosdb.ErrSnapshotDocumentNull)
}

// TestBackend_Restore_NilNestedMapsAreInitialized covers snapshots whose
// "Containers"/"Documents" maps are legal JSON null (decodes to a nil Go
// map, not a nil pointer, so it isn't caught by the null-entry checks
// above): a nil map is safe to range/read but panics on insertion.
func TestBackend_Restore_NilNestedMapsAreInitialized(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(),
		[]byte(`{"databases":{"d":{"ID":"d","Containers":null}},"version":1}`)))

	assert.NotPanics(t, func() {
		_, err := b.CreateContainer("d", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"})
		require.NoError(t, err)
	})

	require.NoError(t, b.Restore(t.Context(), []byte(
		`{"databases":{"d":{"ID":"d","Containers":{"c":{"ID":"c",`+
			`"PartitionKeyPath":"/pk","Documents":null}}}}, "version":1}`,
	)))

	assert.NotPanics(t, func() {
		_, err := b.CreateDocument("d", "c", map[string]any{"id": "1", "pk": "a"}, false)
		require.NoError(t, err)
	})
}

func TestBackend_Restore_MalformedJSON(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte(`not json`))
	require.Error(t, err)
}

func TestHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	backend := cosmosdb.NewInMemoryBackend()
	h := cosmosdb.NewHandler(backend)
	_, err := backend.CreateDatabase("d")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))
}

func TestHandler_Restore_WrapsBackendError(t *testing.T) {
	t.Parallel()

	h := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
	err := h.Restore(t.Context(), []byte(`{"databases":{"d":null},"version":1}`))
	require.Error(t, err)
	assert.ErrorIs(t, err, cosmosdb.ErrSnapshotDatabaseNull)
}
