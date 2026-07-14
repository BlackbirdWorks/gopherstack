// This file is package mediastoredata (not mediastoredata_test) so it can
// reach regionContextKey and reuse msdCtxRegion, matching
// isolation_test.go's existing precedent.
package mediastoredata //nolint:testpackage // needs access to the unexported region context key.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := msdCtxRegion("us-east-1")

	b := NewInMemoryBackend("us-east-1")
	_, err := b.PutObject(ctx, "/seed.mp4", []byte("seed"), "video/mp4", "", "TEMPORAL", "")
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"states":{}}`))
	require.NoError(t, err)

	stats := b.Stats(ctx)
	assert.Equal(t, 0, stats.ObjectCount)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches mediastoredataSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape: MediaStore Data had no persistence at all
// before this conversion, so there is no legacy shape to actually collide
// with -- any input lacking "version" hits this same path.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	ctx := msdCtxRegion("us-east-1")

	b := NewInMemoryBackend("us-east-1")
	_, err := b.PutObject(ctx, "/seed.mp4", []byte("seed"), "video/mp4", "", "TEMPORAL", "")
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"states":{}}`))
	require.NoError(t, err)

	stats := b.Stats(ctx)
	assert.Equal(t, 0, stats.ObjectCount)
}

// TestInMemoryBackend_SnapshotRestore_EmptyBackend verifies that snapshotting
// and restoring a backend with no objects at all round-trips cleanly to an
// empty backend, rather than erroring or leaving stale state.
func TestInMemoryBackend_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	ctx := msdCtxRegion("us-east-1")

	b := NewInMemoryBackend("us-east-1")
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	restored := NewInMemoryBackend("us-east-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	stats := restored.Stats(ctx)
	assert.Equal(t, 0, stats.ObjectCount)
}

// TestInMemoryBackend_SnapshotRestore_FullState is a full-state round-trip:
// it seeds several objects (with distinct bodies, content-types, storage
// classes and upload-availability values, exercising every Object field) in
// two distinct regions, snapshots the backend, restores the snapshot into a
// fresh backend, and asserts every field -- including the raw object
// bytes -- survived, that region isolation is preserved (see store_setup.go
// for why states is region-nested), and that ListItems output order is
// unaffected by the round-trip.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	const (
		regionEast = "us-east-1"
		regionWest = "us-west-2"
	)

	ctxEast := msdCtxRegion(regionEast)
	ctxWest := msdCtxRegion(regionWest)

	b := NewInMemoryBackend(regionEast)

	eastPaths := []string{"/videos/clip1.mp4", "/videos/clip2.mp4", "/root.mp4"}
	for i, p := range eastPaths {
		_, err := b.PutObject(
			ctxEast, p, fmt.Appendf(nil, "east-body-%d", i), "video/mp4", "no-cache", "TEMPORAL", "STREAMING",
		)
		require.NoError(t, err)
	}

	westPaths := []string{"/west-only.mp4"}
	for i, p := range westPaths {
		_, err := b.PutObject(
			ctxWest, p, fmt.Appendf(nil, "west-body-%d", i), "video/mp4", "", "TEMPORAL", "",
		)
		require.NoError(t, err)
	}

	preRestoreEastList := b.ListItems(ctxEast, ListItemsInput{FolderPath: ""})
	preRestoreEastNames := itemNames(preRestoreEastList.Items)

	data := b.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restored := NewInMemoryBackend(regionEast)
	require.NoError(t, restored.Restore(t.Context(), data))

	// Every field of every east object survived, including raw bytes.
	for i, p := range eastPaths {
		obj, err := restored.GetObject(ctxEast, p)
		require.NoError(t, err)
		assert.Equal(t, fmt.Appendf(nil, "east-body-%d", i), obj.Body)
		assert.Equal(t, "video/mp4", obj.ContentType)
		assert.Equal(t, "no-cache", obj.CacheControl)
		assert.Equal(t, "TEMPORAL", obj.StorageClass)
		assert.Equal(t, "STREAMING", obj.UploadAvailability)
		assert.NotEmpty(t, obj.ETag)
		assert.NotEmpty(t, obj.SHA256)
	}

	westObj, err := restored.GetObject(ctxWest, "/west-only.mp4")
	require.NoError(t, err)
	assert.Equal(t, []byte("west-body-0"), westObj.Body)

	// Region isolation must survive the round-trip: east must not see west's
	// object and vice versa.
	_, err = restored.GetObject(ctxWest, "/videos/clip1.mp4")
	require.Error(t, err)

	_, err = restored.GetObject(ctxEast, "/west-only.mp4")
	require.Error(t, err)

	// ListItems ordering (sorted by Name, per backend.go) must be identical
	// before and after the round-trip -- Table.Snapshot's sorted-key order
	// must not silently change the observable listing order, since it's
	// backend.go's explicit sort.Slice, not table/map order, that determines
	// it either way.
	postRestoreEastList := restored.ListItems(ctxEast, ListItemsInput{FolderPath: ""})
	assert.Equal(t, preRestoreEastNames, itemNames(postRestoreEastList.Items))
}

// itemNames extracts the Name field from a slice of *Item in order, for
// order-sensitive assertions.
func itemNames(items []*Item) []string {
	names := make([]string, len(items))
	for i, it := range items {
		names[i] = it.Name
	}

	return names
}

// TestHandler_SnapshotRestore verifies Handler.Snapshot/Restore delegate to
// the backend -- this is the dead-wiring fix: before Phase 3.3, MediaStore
// Data had no persistence at all, so cli.go's generic setupPersistence
// (which type-asserts the registered service.Registerable for a
// Snapshot/Restore pair) never picked MediaStore Data up.
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := msdCtxRegion("us-east-1")

	backend := NewInMemoryBackend("us-east-1")
	h := NewHandler(backend)

	_, err := backend.PutObject(ctx, "/handler/object.mp4", []byte("handler data"), "video/mp4", "", "TEMPORAL", "")
	require.NoError(t, err)

	data := h.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restoredBackend := NewInMemoryBackend("us-east-1")
	restoredHandler := NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(t.Context(), data))

	got, err := restoredBackend.GetObject(ctx, "/handler/object.mp4")
	require.NoError(t, err)
	assert.Equal(t, []byte("handler data"), got.Body)
}
