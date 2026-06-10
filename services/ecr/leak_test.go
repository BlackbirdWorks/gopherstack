package ecr_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// TestInitiateLayerUpload_PrunesAbandonedUploads verifies that initiating a new
// layer upload prunes uploads that were started but never completed and have
// aged past the TTL, bounding the layerUploads map on a long-lived registry.
func TestInitiateLayerUpload_PrunesAbandonedUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		abandoned int
	}{
		{name: "one abandoned", abandoned: 1},
		{name: "many abandoned", abandoned: 6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")

			_, err := b.CreateRepository(ctx, "repo", "MUTABLE", false, "", "")
			require.NoError(t, err)

			for range tc.abandoned {
				_, err = b.InitiateLayerUpload(ctx, "repo")
				require.NoError(t, err)
			}
			require.Equal(t, tc.abandoned, b.LayerUploadCount())

			// Age the abandoned uploads past the TTL, then start a new one. The
			// stale uploads must be pruned, leaving only the fresh session.
			b.AgeAllLayerUploadsForTest(ecr.LayerUploadTTLForTest + 1)

			_, err = b.InitiateLayerUpload(ctx, "repo")
			require.NoError(t, err)

			require.Equal(t, 1, b.LayerUploadCount(),
				"abandoned layer uploads must be pruned on InitiateLayerUpload")
		})
	}
}

// TestUploadLayerPart_KeepsActiveUploadAlive ensures an in-progress multi-part
// upload is not pruned as abandoned while parts are still arriving.
func TestUploadLayerPart_KeepsActiveUploadAlive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")

	_, err := b.CreateRepository(ctx, "repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	init, err := b.InitiateLayerUpload(ctx, "repo")
	require.NoError(t, err)

	// Age the upload, but then send a part — activity must refresh CreatedAt.
	b.AgeAllLayerUploadsForTest(ecr.LayerUploadTTLForTest + 1)
	_, err = b.UploadLayerPart(ctx, "repo", init.UploadID, 0, -1, []byte("data"))
	require.NoError(t, err)

	// A subsequent initiate must NOT prune the refreshed upload.
	_, err = b.InitiateLayerUpload(ctx, "repo")
	require.NoError(t, err)

	require.Equal(t, 2, b.LayerUploadCount(),
		"an actively-uploading session must survive pruning")
}
