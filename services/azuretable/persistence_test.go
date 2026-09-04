package azuretable_test

import (
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
	err := b.Restore(t.Context(), []byte(`{"tables":{"t":null},"version":1}`))
	require.ErrorIs(t, err, azuretable.ErrSnapshotTableNull)
}

func TestBackend_Restore_NullEntityRejected(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	err := b.Restore(t.Context(),
		[]byte(`{"tables":{"t":{"Name":"t","Entities":{"k":null}}},"version":1}`))
	require.ErrorIs(t, err, azuretable.ErrSnapshotEntityNull)
}

func TestBackend_Restore_TableNameMismatchRejected(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	err := b.Restore(t.Context(),
		[]byte(`{"tables":{"t":{"Name":"other","Entities":{}}},"version":1}`))
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
	err := h.Restore(t.Context(), []byte(`{"tables":{"t":null},"version":1}`))
	require.Error(t, err)
	assert.ErrorIs(t, err, azuretable.ErrSnapshotTableNull)
}
