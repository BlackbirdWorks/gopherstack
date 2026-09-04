package azurequeue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "roundtrip_queue_and_message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "payload", 0, 0)
			require.NoError(t, err)

			data := b.Snapshot(ctx)
			require.NotEmpty(t, data, tt.name)

			restored := azurequeue.NewInMemoryBackend()
			require.NoError(t, restored.Restore(ctx, data))

			queues := restored.ListQueues()
			require.Len(t, queues, 1, tt.name)
			assert.Equal(t, "q1", queues[0].Name, tt.name)

			peeked, err := restored.PeekMessages("q1", 10)
			require.NoError(t, err, tt.name)
			require.Len(t, peeked, 1, tt.name)
			assert.Equal(t, "payload", peeked[0].Text, tt.name)
		})
	}
}

func TestRestore_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "garbage_bytes_discarded", data: []byte(`{"version":999,"queues":{}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("preexisting")
			require.NoError(t, err)

			require.NoError(t, b.Restore(ctx, tt.data))

			assert.Empty(t, b.ListQueues(), tt.name)
		})
	}
}

// TestRestore_RejectsNullEntries is a regression test: a JSON `null` value
// inside "queues" or a queue's "Messages" decodes to a nil pointer without a
// JSON-unmarshal error, and previously nothing checked for that before
// storing it -- the first thing to dereference it later would panic.
// Restore must reject the whole snapshot instead.
func TestRestore_RejectsNullEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		data    []byte
	}{
		{
			name:    "null_queue",
			data:    []byte(`{"version":1,"queues":{"q1":null}}`),
			wantErr: azurequeue.ErrSnapshotQueueNull,
		},
		{
			name:    "null_message",
			data:    []byte(`{"version":1,"queues":{"q1":{"Name":"q1","Messages":[null]}}}`),
			wantErr: azurequeue.ErrSnapshotMessageNull,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("preexisting")
			require.NoError(t, err)

			err = b.Restore(ctx, tt.data)
			require.ErrorIs(t, err, tt.wantErr, tt.name)

			// A rejected snapshot must not have partially mutated state.
			queues := b.ListQueues()
			require.Len(t, queues, 1, tt.name)
			assert.Equal(t, "preexisting", queues[0].Name, tt.name)
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

			backend := azurequeue.NewInMemoryBackend()
			h := azurequeue.NewHandler(backend)
			_, err := backend.CreateQueue("q1")
			require.NoError(t, err)

			data := h.Snapshot(ctx)
			require.NotEmpty(t, data, tt.name)

			restoredBackend := azurequeue.NewInMemoryBackend()
			restoredHandler := azurequeue.NewHandler(restoredBackend)
			require.NoError(t, restoredHandler.Restore(ctx, data))

			assert.Len(t, restoredBackend.ListQueues(), 1, tt.name)
		})
	}
}
