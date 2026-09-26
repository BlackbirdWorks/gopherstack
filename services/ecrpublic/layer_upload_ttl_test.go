package ecrpublic_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecrpublic"
)

// pastLayerUploadTTL is strictly greater than the modeled 24h
// layerUploadTTL, so the lazy prune always evicts a stale session by the
// time it fires.
const pastLayerUploadTTL = 24*time.Hour + time.Second

// TestInitiateLayerUpload_KeptWithinTTL proves an abandoned upload session
// survives up to layerUploadTTL, matching a real (if slow) docker push.
func TestInitiateLayerUpload_KeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		backend := ecrpublic.NewInMemoryBackend(testAccountID, testRegion)

		_, err := backend.CreateRepository("kept-repo", nil, nil)
		require.NoError(t, err)

		uploadID, _, err := backend.InitiateLayerUpload("", "kept-repo")
		require.NoError(t, err)

		time.Sleep(24*time.Hour - time.Second)

		_, err = backend.UploadLayerPart("", "kept-repo", uploadID, 0, 3, []byte("data"))
		require.NoError(t, err)
	})
}

// TestInitiateLayerUpload_EvictedAfterTTL locks in the fix for the
// InitiateLayerUpload session leak: sessions that never reach
// CompleteLayerUpload used to be retained forever, growing the backend's
// memory unbounded in a long-running emulator. They are now pruned
// lazily, on the next InitiateLayerUpload call, once layerUploadTTL has
// elapsed.
func TestInitiateLayerUpload_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		backend := ecrpublic.NewInMemoryBackend(testAccountID, testRegion)

		_, err := backend.CreateRepository("evict-repo", nil, nil)
		require.NoError(t, err)

		uploadID, _, err := backend.InitiateLayerUpload("", "evict-repo")
		require.NoError(t, err)

		time.Sleep(pastLayerUploadTTL)

		// The prune runs lazily on the next InitiateLayerUpload call.
		_, _, err = backend.InitiateLayerUpload("", "evict-repo")
		require.NoError(t, err)

		_, err = backend.UploadLayerPart("", "evict-repo", uploadID, 0, 3, []byte("data"))
		require.ErrorIs(t, err, ecrpublic.ErrUploadNotFound)
	})
}
