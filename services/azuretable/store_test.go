package azuretable_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func newTestBackend(t *testing.T) *azuretable.InMemoryBackend {
	t.Helper()

	return azuretable.NewInMemoryBackend()
}

func TestInMemoryBackend_CreateTable(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	require.NoError(t, b.CreateTable("foo"))
	err := b.CreateTable("foo")
	require.ErrorIs(t, err, azuretable.ErrTableAlreadyExists)
}

func TestInMemoryBackend_DeleteTable(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("foo"))
	require.NoError(t, b.DeleteTable("foo"))

	err := b.DeleteTable("foo")
	require.ErrorIs(t, err, azuretable.ErrTableNotFound)
}

func TestInMemoryBackend_ListTables_SortedByName(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("zebra"))
	require.NoError(t, b.CreateTable("alpha"))

	infos := b.ListTables()
	require.Len(t, infos, 2)
	assert.Equal(t, "alpha", infos[0].Name)
	assert.Equal(t, "zebra", infos[1].Name)
}

func TestInMemoryBackend_InsertEntity(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))

		info, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"Name": {Type: azuretable.EdmString, Value: "hi"},
		})
		require.NoError(t, err)
		assert.Equal(t, "p", info.PartitionKey)
		assert.Equal(t, "r", info.RowKey)
		assert.NotEmpty(t, info.ETag)
	})

	t.Run("table_not_found", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.InsertEntity("nosuch", "p", "r", nil)
		require.ErrorIs(t, err, azuretable.ErrTableNotFound)
	})

	t.Run("already_exists", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))
		_, err := b.InsertEntity("t", "p", "r", nil)
		require.NoError(t, err)

		_, err = b.InsertEntity("t", "p", "r", nil)
		require.ErrorIs(t, err, azuretable.ErrEntityAlreadyExists)
	})
}

func TestInMemoryBackend_GetEntity(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))

	_, err := b.GetEntity("nosuch", "p", "r")
	require.ErrorIs(t, err, azuretable.ErrTableNotFound)

	_, err = b.GetEntity("t", "p", "r")
	require.ErrorIs(t, err, azuretable.ErrEntityNotFound)

	_, err = b.InsertEntity("t", "p", "r", nil)
	require.NoError(t, err)

	info, err := b.GetEntity("t", "p", "r")
	require.NoError(t, err)
	assert.Equal(t, "p", info.PartitionKey)
}

func TestInMemoryBackend_QueryEntities_OrderingAndTop(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))

	_, err := b.InsertEntity("t", "p2", "r1", nil)
	require.NoError(t, err)
	_, err = b.InsertEntity("t", "p1", "r2", nil)
	require.NoError(t, err)
	_, err = b.InsertEntity("t", "p1", "r1", nil)
	require.NoError(t, err)

	infos, err := b.QueryEntities("t", nil, 0)
	require.NoError(t, err)
	require.Len(t, infos, 3)
	assert.Equal(t, [2]string{"p1", "r1"}, [2]string{infos[0].PartitionKey, infos[0].RowKey})
	assert.Equal(t, [2]string{"p1", "r2"}, [2]string{infos[1].PartitionKey, infos[1].RowKey})
	assert.Equal(t, [2]string{"p2", "r1"}, [2]string{infos[2].PartitionKey, infos[2].RowKey})

	capped, err := b.QueryEntities("t", nil, 2)
	require.NoError(t, err)
	assert.Len(t, capped, 2)

	_, err = b.QueryEntities("nosuch", nil, 0)
	require.ErrorIs(t, err, azuretable.ErrTableNotFound)
}

func TestInMemoryBackend_QueryEntities_Filter(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))

	_, err := b.InsertEntity("t", "p1", "r1", map[string]azuretable.EntityProperty{
		"Age": {Type: azuretable.EdmInt32, Value: int32(10)},
	})
	require.NoError(t, err)
	_, err = b.InsertEntity("t", "p2", "r1", map[string]azuretable.EntityProperty{
		"Age": {Type: azuretable.EdmInt32, Value: int32(20)},
	})
	require.NoError(t, err)

	node, parseErr := azuretable.ParseFilter("Age gt 15")
	require.NoError(t, parseErr)

	infos, err := b.QueryEntities("t", node, 0)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Equal(t, "p2", infos[0].PartitionKey)
}

func TestInMemoryBackend_ReplaceEntity(t *testing.T) {
	t.Parallel()

	t.Run("upsert_no_ifmatch_creates", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))

		info, err := b.ReplaceEntity("t", "p", "r", nil, "")
		require.NoError(t, err)
		assert.NotEmpty(t, info.ETag)
	})

	t.Run("ifmatch_star_requires_existing", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))

		_, err := b.ReplaceEntity("t", "p", "r", nil, azuretable.IfMatchAny)
		require.ErrorIs(t, err, azuretable.ErrEntityNotFound)
	})

	t.Run("ifmatch_specific_mismatch", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))
		_, err := b.InsertEntity("t", "p", "r", nil)
		require.NoError(t, err)

		_, err = b.ReplaceEntity("t", "p", "r", nil, `W/"datetime'bogus'"`)
		require.ErrorIs(t, err, azuretable.ErrETagMismatch)
	})

	t.Run("replace_drops_old_properties", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))
		_, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"A": {Type: azuretable.EdmString, Value: "x"},
		})
		require.NoError(t, err)

		info, err := b.ReplaceEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"B": {Type: azuretable.EdmString, Value: "y"},
		}, azuretable.IfMatchAny)
		require.NoError(t, err)
		_, hasA := info.Properties["A"]
		assert.False(t, hasA)
		_, hasB := info.Properties["B"]
		assert.True(t, hasB)
	})

	t.Run("table_not_found", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.ReplaceEntity("nosuch", "p", "r", nil, "")
		require.ErrorIs(t, err, azuretable.ErrTableNotFound)
	})
}

func TestInMemoryBackend_MergeEntity_KeepsUnlistedProperties(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))
	_, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
		"A": {Type: azuretable.EdmString, Value: "x"},
		"B": {Type: azuretable.EdmString, Value: "y"},
	})
	require.NoError(t, err)

	info, err := b.MergeEntity("t", "p", "r", map[string]azuretable.EntityProperty{
		"A": {Type: azuretable.EdmString, Value: "z"},
	}, azuretable.IfMatchAny)
	require.NoError(t, err)
	assert.Equal(t, "z", info.Properties["A"].Value)
	assert.Equal(t, "y", info.Properties["B"].Value)
}

func TestInMemoryBackend_DeleteEntity(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))
	_, err := b.InsertEntity("t", "p", "r", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteEntity("t", "p", "r", azuretable.IfMatchAny))

	err = b.DeleteEntity("t", "p", "r", azuretable.IfMatchAny)
	require.ErrorIs(t, err, azuretable.ErrEntityNotFound)

	err = b.DeleteEntity("nosuch", "p", "r", azuretable.IfMatchAny)
	require.ErrorIs(t, err, azuretable.ErrTableNotFound)
}

// TestInMemoryBackend_ETagChangesOnEveryWrite is a regression test for
// exactly the class of bug M1's review bots caught: two mutations landing
// within the same injected clock tick must still produce different ETags.
func TestInMemoryBackend_ETagChangesOnEveryWrite(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))

	fixed := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	azuretable.SetNowFunc(b, func() time.Time { return fixed })

	info1, err := b.InsertEntity("t", "p", "r", nil)
	require.NoError(t, err)

	info2, err := b.ReplaceEntity("t", "p", "r", nil, azuretable.IfMatchAny)
	require.NoError(t, err)

	info3, err := b.MergeEntity("t", "p", "r", nil, azuretable.IfMatchAny)
	require.NoError(t, err)

	assert.NotEqual(t, info1.ETag, info2.ETag)
	assert.NotEqual(t, info2.ETag, info3.ETag)
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))

	b.Reset()

	assert.Empty(t, b.ListTables())
}

func TestSetETagFunc(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	azuretable.SetETagFunc(b, func(time.Time) string { return "fixed-etag" })
	require.NoError(t, b.CreateTable("t"))

	info, err := b.InsertEntity("t", "p", "r", nil)
	require.NoError(t, err)
	assert.Equal(t, "fixed-etag", info.ETag)
}

func TestEtagFor(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 1, 2, 3, 4, 5, 123456700, time.UTC)
	got := azuretable.EtagFor(ts)

	assert.Contains(t, got, "datetime")
	assert.Contains(t, got, "%3A") // url-encoded colon
}

// TestInMemoryBackend_EntityKeys_NoNULDelimiterCollision is a regression
// test for a real bug: entityKey used to build its map key by concatenating
// PartitionKey + "\x00" + RowKey, but JSON permits a literal NUL byte inside
// a string property, so two different (PartitionKey, RowKey) pairs could
// collide onto the same delimited string
// (partitionKey="a\x00b",rowKey="c" vs. partitionKey="a",rowKey="b\x00c").
// The map key is now a comparable struct (entityCompositeKey) instead, so
// this must no longer collide: both entities must insert successfully and
// remain independently addressable.
func TestInMemoryBackend_EntityKeys_NoNULDelimiterCollision(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	require.NoError(t, b.CreateTable("t"))

	_, err := b.InsertEntity("t", "a\x00b", "c", map[string]azuretable.EntityProperty{
		"Which": {Type: azuretable.EdmString, Value: "first"},
	})
	require.NoError(t, err)

	// If the two pairs collided, this second insert would incorrectly fail
	// with ErrEntityAlreadyExists.
	_, err = b.InsertEntity("t", "a", "b\x00c", map[string]azuretable.EntityProperty{
		"Which": {Type: azuretable.EdmString, Value: "second"},
	})
	require.NoError(t, err)

	first, err := b.GetEntity("t", "a\x00b", "c")
	require.NoError(t, err)
	assert.Equal(t, "first", first.Properties["Which"].Value)

	second, err := b.GetEntity("t", "a", "b\x00c")
	require.NoError(t, err)
	assert.Equal(t, "second", second.Properties["Which"].Value)

	infos, err := b.QueryEntities("t", nil, 0)
	require.NoError(t, err)
	assert.Len(t, infos, 2, "both entities must coexist, not collide into one")
}

// TestInMemoryBackend_EdmBinary_NoAliasing is a regression test for a real
// data-corruption bug: cloneProps/MergeEntity's map copy was shallow, so the
// []byte backing an EdmBinary property's Value was shared between the
// caller and the stored entity (and again between the stored entity and
// whatever GetEntity/QueryEntities hands back). Mutating any one of those
// slices in place silently corrupted the "stored" value with no Timestamp
// bump and no ETag change -- exactly the kind of change optimistic
// concurrency exists to detect and can't if it never happens explicitly.
func TestInMemoryBackend_EdmBinary_NoAliasing(t *testing.T) {
	t.Parallel()

	t.Run("mutating_the_inserted_slice_after_insert_does_not_corrupt_storage", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))

		original := []byte("original")
		_, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"Blob": {Type: azuretable.EdmBinary, Value: original},
		})
		require.NoError(t, err)

		original[0] = 'X' // mutate the caller's own slice after the call returns

		info, err := b.GetEntity("t", "p", "r")
		require.NoError(t, err)
		assert.Equal(t, []byte("original"), info.Properties["Blob"].Value)
	})

	t.Run("mutating_a_returned_slice_does_not_corrupt_storage", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))
		_, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"Blob": {Type: azuretable.EdmBinary, Value: []byte("stored")},
		})
		require.NoError(t, err)

		info, err := b.GetEntity("t", "p", "r")
		require.NoError(t, err)

		returned, ok := info.Properties["Blob"].Value.([]byte)
		require.True(t, ok)
		returned[0] = 'X' // mutate the slice the backend handed back

		info2, err := b.GetEntity("t", "p", "r")
		require.NoError(t, err)
		assert.Equal(t, []byte("stored"), info2.Properties["Blob"].Value)
	})

	t.Run("merge_deep_copies_incoming_binary_values_too", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))
		_, err := b.InsertEntity("t", "p", "r", nil)
		require.NoError(t, err)

		merged := []byte("merged")
		_, err = b.MergeEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"Blob": {Type: azuretable.EdmBinary, Value: merged},
		}, azuretable.IfMatchAny)
		require.NoError(t, err)

		merged[0] = 'X' // mutate the caller's own slice after the call returns

		info, err := b.GetEntity("t", "p", "r")
		require.NoError(t, err)
		assert.Equal(t, []byte("merged"), info.Properties["Blob"].Value)
	})

	t.Run("query_results_do_not_alias_storage", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		require.NoError(t, b.CreateTable("t"))
		_, err := b.InsertEntity("t", "p", "r", map[string]azuretable.EntityProperty{
			"Blob": {Type: azuretable.EdmBinary, Value: []byte("queried")},
		})
		require.NoError(t, err)

		infos, err := b.QueryEntities("t", nil, 0)
		require.NoError(t, err)
		require.Len(t, infos, 1)

		returned, ok := infos[0].Properties["Blob"].Value.([]byte)
		require.True(t, ok)
		returned[0] = 'X'

		info2, err := b.GetEntity("t", "p", "r")
		require.NoError(t, err)
		assert.Equal(t, []byte("queried"), info2.Properties["Blob"].Value)
	})
}
