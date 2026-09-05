package azuretable_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func TestBackend_SnapshotRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	require.NoError(t, b.CreateTable("t"))
	_, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
		"Name": {Type: azuretable.EdmString, Value: "hi"},
		"Age":  {Type: azuretable.EdmInt32, Value: int32(42)},
		"Big":  {Type: azuretable.EdmInt64, Value: int64(9223372036854775807)},
		"Blob": {Type: azuretable.EdmBinary, Value: []byte("hello")},
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := azuretable.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	info, err := b2.GetEntity("t", "p", "r")
	require.NoError(t, err)
	assert.Equal(t, "hi", info.Properties["Name"].Value)
	assert.Equal(t, int32(42), info.Properties["Age"].Value)
	assert.Equal(t, int64(9223372036854775807), info.Properties["Big"].Value)
	assert.Equal(t, []byte("hello"), info.Properties["Blob"].Value)
}

// TestBackend_SnapshotRestore_Int64PrecisionRoundTrip is a regression test
// for a real data-corruption bug: an earlier EntityProperty.MarshalJSON
// encoded Edm.Int64 as a bare JSON number (float64(n)), which silently loses
// precision above 2^53 (float64's mantissa width) -- 9007199254740993
// (2^53+1) round-tripped to 9007199254740992. Int64 is now encoded as a
// decimal string in the snapshot, matching the OData wire format's own
// Edm.Int64 encoding. Covers the boundary values that matter: the smallest
// value a float64 can't represent exactly, both int64 extremes, -1, and 0.
func TestBackend_SnapshotRestore_Int64PrecisionRoundTrip(t *testing.T) {
	t.Parallel()

	values := map[string]int64{
		"beyond_float64_mantissa": 1<<53 + 1, // 9007199254740993
		"max_int64":               math.MaxInt64,
		"min_int64":               math.MinInt64,
		"negative_one":            -1,
		"zero":                    0,
	}

	b := azuretable.NewInMemoryBackend()
	require.NoError(t, b.CreateTable("t"))

	for name, v := range values {
		_, err := b.InsertEntity("t", "p", name, map[string]azuretable.EntityProperty{
			"Big": {Type: azuretable.EdmInt64, Value: v},
		})
		require.NoError(t, err, name)
	}

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := azuretable.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	for name, want := range values {
		info, err := b2.GetEntity("t", "p", name)
		require.NoError(t, err, name)
		assert.Equal(t, want, info.Properties["Big"].Value, name)
	}
}

func TestBackend_Restore_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	require.NoError(t, b.CreateTable("t"))

	err := b.Restore(t.Context(), []byte(`{"tables":{},"version":999}`))
	require.NoError(t, err)
	assert.Empty(t, b.ListTables())
}

func TestBackend_Restore_NullTableRejected(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte(`{"tables":{"t":null},"version":2}`))
	require.ErrorIs(t, err, azuretable.ErrSnapshotTableNull)
}

func TestBackend_Restore_NullEntityRejected(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	// The entity map's key is entityCompositeKey (see models.go), which
	// marshals itself as a JSON string array ["PartitionKey","RowKey"] --
	// so, as an object key, it appears here JSON-string-escaped:
	// ["p","r"] -> the literal object key "[\"p\",\"r\"]".
	err := b.Restore(t.Context(),
		[]byte(`{"tables":{"t":{"Name":"t","Entities":{"[\"p\",\"r\"]":null}}},"version":2}`))
	require.ErrorIs(t, err, azuretable.ErrSnapshotEntityNull)
}

// TestBackend_Restore_NilEntitiesMapIsInitialized covers a snapshot whose
// table has "Entities": null (legal JSON -- decodes to a nil Go map, not a
// nil *storedTable, so it isn't caught by the null-table/null-entity checks
// above): a nil map is safe to range/read, but InsertEntity assigning into
// it directly afterward would panic ("assignment to entry in nil map").
// Restore must leave the table usable.
func TestBackend_Restore_NilEntitiesMapIsInitialized(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(),
		[]byte(`{"tables":{"t":{"Name":"t","Entities":null}},"version":2}`)))

	assert.NotPanics(t, func() {
		_, err := b.InsertEntity("t", "p", "r", nil)
		require.NoError(t, err)
	})

	info, err := b.GetEntity("t", "p", "r")
	require.NoError(t, err)
	assert.Equal(t, "p", info.PartitionKey)
}

func TestBackend_Restore_TableNameMismatchRejected(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	err := b.Restore(t.Context(),
		[]byte(`{"tables":{"t":{"Name":"other","Entities":{}}},"version":2}`))
	require.ErrorIs(t, err, azuretable.ErrSnapshotTableNameMismatch)
}

func TestBackend_Restore_MalformedJSON(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte(`not json`))
	require.Error(t, err)
}

func TestHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	backend := azuretable.NewInMemoryBackend()
	h := azuretable.NewHandler(backend)
	require.NoError(t, backend.CreateTable("t"))

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := azuretable.NewHandler(azuretable.NewInMemoryBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))
}

func TestHandler_Restore_WrapsBackendError(t *testing.T) {
	t.Parallel()

	h := azuretable.NewHandler(azuretable.NewInMemoryBackend())
	err := h.Restore(t.Context(), []byte(`{"tables":{"t":null},"version":2}`))
	require.Error(t, err)
	assert.ErrorIs(t, err, azuretable.ErrSnapshotTableNull)
}
