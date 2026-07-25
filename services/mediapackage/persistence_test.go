package mediapackage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// Test_SnapshotRestore exercises a Snapshot->Restore round trip across every
// resource family the Phase 3.3 pkgs/store conversion touched: the three
// store.Table-backed collections (channels, originEndpoints -- including its
// byChannel secondary index, harvestJobs) plus the plain tags map left
// un-converted. Each subtest seeds and verifies its own backend instance so
// subtests can run in parallel without shared state.
func Test_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed   func(t *testing.T, b *mediapackage.InMemoryBackend)
		verify func(t *testing.T, b *mediapackage.InMemoryBackend)
		name   string
	}{
		{
			name: "channels",
			seed: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateChannel("chan1", "desc1", map[string]string{"env": "test"})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, mediapackage.ChannelCount(b))

				ch, err := b.DescribeChannel("chan1")
				require.NoError(t, err)
				assert.Equal(t, "desc1", ch.Description)
				assert.Equal(t, "test", ch.Tags["env"])
				require.Len(t, ch.HlsIngest.IngestEndpoints, 2)
			},
		},
		{
			name: "originEndpoints_byChannelIndex",
			seed: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateChannel("chanA", "", nil)
				require.NoError(t, err)
				_, err = b.CreateChannel("chanB", "", nil)
				require.NoError(t, err)

				_, err = b.CreateOriginEndpoint(
					"chanA", "epA1", "", "", 0, 0, "", nil, nil, mediapackage.PackagingConfig{},
				)
				require.NoError(t, err)
				_, err = b.CreateOriginEndpoint("chanA", "epA2", "", "", 0, 0, "", nil,
					map[string]string{"k": "v"}, mediapackage.PackagingConfig{})
				require.NoError(t, err)
				_, err = b.CreateOriginEndpoint(
					"chanB", "epB1", "", "", 0, 0, "", nil, nil, mediapackage.PackagingConfig{},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 3, mediapackage.OriginEndpointCount(b))

				epsA, _, err := b.ListOriginEndpoints("chanA", 0, "")
				require.NoError(t, err)
				require.Len(t, epsA, 2)
				assert.Equal(t, "epA1", epsA[0].ID)
				assert.Equal(t, "epA2", epsA[1].ID)
				assert.Equal(t, "v", epsA[1].Tags["k"])

				epsB, _, err := b.ListOriginEndpoints("chanB", 0, "")
				require.NoError(t, err)
				require.Len(t, epsB, 1)
				assert.Equal(t, "epB1", epsB[0].ID)
			},
		},
		{
			name: "harvestJobs",
			seed: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateChannel("chan1", "", nil)
				require.NoError(t, err)
				_, err = b.CreateOriginEndpoint(
					"chan1", "ep1", "", "", 0, 0, "", nil, nil, mediapackage.PackagingConfig{},
				)
				require.NoError(t, err)
				_, err = b.CreateHarvestJob("job1", "ep1", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z",
					mediapackage.S3Destination{BucketName: "bucket", ManifestKey: "key", RoleArn: "role"})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, mediapackage.HarvestJobCount(b))

				job, err := b.DescribeHarvestJob("job1")
				require.NoError(t, err)
				assert.Equal(t, "chan1", job.ChannelID)
				assert.Equal(t, "ep1", job.OriginEndpointID)
				require.NotNil(t, job.S3Destination)
				assert.Equal(t, "bucket", job.S3Destination.BucketName)
			},
		},
		{
			name: "tags_rawMap",
			seed: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				ch, err := b.CreateChannel("chan1", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.TagResource(ch.ARN, map[string]string{"team": "video"}))
			},
			verify: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				ch, err := b.DescribeChannel("chan1")
				require.NoError(t, err)

				tags, err := b.ListTagsForResource(ch.ARN)
				require.NoError(t, err)
				assert.Equal(t, "video", tags["team"])
				assert.Equal(t, "video", ch.Tags["team"])
			},
		},
		{
			name: "accountAndRegion",
			seed: func(t *testing.T, _ *mediapackage.InMemoryBackend) {
				t.Helper()
			},
			verify: func(t *testing.T, b *mediapackage.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, "111122223333", b.AccountID())
				assert.Equal(t, "us-east-1", b.Region())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := mediapackage.NewInMemoryBackend("111122223333", "us-east-1")
			tt.seed(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := mediapackage.NewInMemoryBackend("000000000000", "us-west-2")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed snapshot JSON is
// rejected with an error rather than silently accepted.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("111122223333", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("111122223333", "us-east-1")
	_, err := b.CreateChannel("seed", "", nil)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, mediapackage.ChannelCount(b))
}

// TestHandler_SnapshotRestoreDelegate verifies the dead-wiring fix: Handler
// now delegates Snapshot/Restore to the backend, which previously implemented
// both but was never invoked because Handler did not expose them.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := mediapackage.NewHandler(mediapackage.NewInMemoryBackend("111122223333", "us-east-1"))
	_, err := h.Backend.CreateChannel("delegate", "desc", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := mediapackage.NewHandler(mediapackage.NewInMemoryBackend("000000000000", "us-west-2"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	ch, err := h2.Backend.DescribeChannel("delegate")
	require.NoError(t, err)
	assert.Equal(t, "desc", ch.Description)
}
