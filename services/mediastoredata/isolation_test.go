package mediastoredata //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msdCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestMediaStoreDataRegionIsolation proves that same-named objects created in
// two different regions are fully isolated: each region sees only its own
// objects, and deleting in one region leaves the other untouched.
func TestMediaStoreDataRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("us-east-1")

	ctxEast := msdCtxRegion("us-east-1")
	ctxWest := msdCtxRegion("us-west-2")

	// 1. Store the same path in both regions with distinct bodies.
	_, err := backend.PutObject(ctxEast, "/videos/clip.mp4", []byte("east content"), "video/mp4", "", "TEMPORAL", "")
	require.NoError(t, err)

	_, err = backend.PutObject(ctxWest, "/videos/clip.mp4", []byte("west content"), "video/mp4", "", "TEMPORAL", "")
	require.NoError(t, err)

	// 2. Each region reads back its own body.
	eastObj, err := backend.GetObject(ctxEast, "/videos/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, []byte("east content"), eastObj.Body)

	westObj, err := backend.GetObject(ctxWest, "/videos/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, []byte("west content"), westObj.Body)

	// 3. ListItems returns exactly one item per region.
	eastList := backend.ListItems(ctxEast, ListItemsInput{FolderPath: "videos"})
	require.Len(t, eastList.Items, 1)
	assert.Equal(t, "clip.mp4", eastList.Items[0].Name)

	westList := backend.ListItems(ctxWest, ListItemsInput{FolderPath: "videos"})
	require.Len(t, westList.Items, 1)
	assert.Equal(t, "clip.mp4", westList.Items[0].Name)

	// 4. Stats are region-scoped.
	eastStats := backend.Stats(ctxEast)
	assert.Equal(t, 1, eastStats.ObjectCount)
	assert.Equal(t, int64(12), eastStats.TotalBytes)

	westStats := backend.Stats(ctxWest)
	assert.Equal(t, 1, westStats.ObjectCount)
	assert.Equal(t, int64(12), westStats.TotalBytes)

	// 5. Deleting in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteObject(ctxEast, "/videos/clip.mp4"))

	_, err = backend.GetObject(ctxEast, "/videos/clip.mp4")
	require.Error(t, err, "east object should be gone")

	stillWest, err := backend.GetObject(ctxWest, "/videos/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, []byte("west content"), stillWest.Body)
}

// TestMediaStoreDataDefaultRegionFallback verifies that a context without a
// region falls back to the backend's configured default region.
func TestMediaStoreDataDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("eu-central-1")

	// No region in context → default region store.
	_, err := backend.PutObject(
		context.Background(), "/default/file.mp4", []byte("data"), "video/mp4", "", "TEMPORAL", "",
	)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	obj, err := backend.GetObject(msdCtxRegion("eu-central-1"), "/default/file.mp4")
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), obj.Body)

	// A different region sees nothing.
	_, err = backend.GetObject(msdCtxRegion("ap-south-1"), "/default/file.mp4")
	require.Error(t, err, "object must not be visible in a different region")
}
