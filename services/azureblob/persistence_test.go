package azureblob_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "roundtrip_container_and_blob"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("c1"))
			_, err := b.PutBlob("c1", "blob1", []byte("payload"), "text/plain")
			require.NoError(t, err)

			data := b.Snapshot(ctx)
			require.NotEmpty(t, data, tt.name)

			restored := azureblob.NewInMemoryBackend()
			require.NoError(t, restored.Restore(ctx, data))

			containers := restored.ListContainers()
			require.Len(t, containers, 1, tt.name)
			assert.Equal(t, "c1", containers[0].Name, tt.name)

			_, blobData, err := restored.GetBlob("c1", "blob1")
			require.NoError(t, err, tt.name)
			assert.Equal(t, "payload", string(blobData), tt.name)
		})
	}
}

func TestRestore_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "garbage_bytes_discarded", data: []byte(`{"version":999,"containers":{}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("preexisting"))

			require.NoError(t, b.Restore(ctx, tt.data))

			assert.Empty(t, b.ListContainers(), tt.name)
		})
	}
}

func TestHandlerSnapshotRestore_Delegates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_delegates_to_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			backend := azureblob.NewInMemoryBackend()
			h := azureblob.NewHandler(backend)
			require.NoError(t, backend.CreateContainer("c1"))

			data := h.Snapshot(ctx)
			require.NotEmpty(t, data, tt.name)

			restoredBackend := azureblob.NewInMemoryBackend()
			restoredHandler := azureblob.NewHandler(restoredBackend)
			require.NoError(t, restoredHandler.Restore(ctx, data))

			assert.Len(t, restoredBackend.ListContainers(), 1, tt.name)
		})
	}
}
