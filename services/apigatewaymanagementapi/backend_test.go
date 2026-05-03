package apigatewaymanagementapi_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

// TestInMemoryBackend_RingBufferRotation verifies the ring buffer caps stored
// messages at the configured maximum, drops the oldest entries first, and
// preserves chronological ordering on read.
func TestInMemoryBackend_RingBufferRotation(t *testing.T) {
	t.Parallel()

	cap := apigatewaymanagementapi.MaxMessagesPerConnection

	tests := []struct {
		name       string
		writeCount int
		wantSize   int
		wantFirst  string
		wantLast   string
	}{
		{
			name:       "below_cap_keeps_all",
			writeCount: 3,
			wantSize:   3,
			wantFirst:  "msg-0",
			wantLast:   "msg-2",
		},
		{
			name:       "at_cap_keeps_all",
			writeCount: cap,
			wantSize:   cap,
			wantFirst:  "msg-0",
			wantLast:   fmt.Sprintf("msg-%d", cap-1),
		},
		{
			name:       "over_cap_drops_oldest",
			writeCount: cap + 5,
			wantSize:   cap,
			wantFirst:  "msg-5",
			wantLast:   fmt.Sprintf("msg-%d", cap+4),
		},
		{
			name:       "double_cap_keeps_only_recent_window",
			writeCount: cap * 2,
			wantSize:   cap,
			wantFirst:  fmt.Sprintf("msg-%d", cap),
			wantLast:   fmt.Sprintf("msg-%d", cap*2-1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewaymanagementapi.NewInMemoryBackend()
			_, err := b.CreateConnection("conn", "1.1.1.1", "ring-test")
			require.NoError(t, err)

			for i := range tt.writeCount {
				require.NoError(t, b.PostToConnection("conn", fmt.Appendf(nil, "msg-%d", i)))
			}

			msgs := b.GetMessages("conn")
			assert.Len(t, msgs, tt.wantSize)
			assert.Equal(t, tt.wantFirst, string(msgs[0].Data))
			assert.Equal(t, tt.wantLast, string(msgs[len(msgs)-1].Data))

			conn, err := b.GetConnection("conn")
			require.NoError(t, err)
			assert.Equal(t, tt.writeCount, conn.PostedMessages)
		})
	}
}

// TestInMemoryBackend_RingBufferSnapshotRoundTrip verifies that messages
// rotated through the ring buffer survive Snapshot+Restore in the same
// chronological window the live buffer would expose.
func TestInMemoryBackend_RingBufferSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	cap := apigatewaymanagementapi.MaxMessagesPerConnection

	tests := []struct {
		name       string
		writeCount int
	}{
		{name: "no_overflow", writeCount: 5},
		{name: "exact_cap", writeCount: cap},
		{name: "over_cap_one_wrap", writeCount: cap + 25},
		{name: "over_cap_many_wraps", writeCount: cap * 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := apigatewaymanagementapi.NewInMemoryBackend()
			_, err := original.CreateConnection("conn", "1.1.1.1", "snap-test")
			require.NoError(t, err)

			for i := range tt.writeCount {
				require.NoError(t, original.PostToConnection("conn", fmt.Appendf(nil, "m-%d", i)))
			}

			before := original.GetMessages("conn")

			restored := apigatewaymanagementapi.NewInMemoryBackend()
			require.NoError(t, restored.Restore(original.Snapshot()))

			after := restored.GetMessages("conn")
			require.Len(t, after, len(before))

			for i := range before {
				assert.Equal(t, before[i].Data, after[i].Data, "msg index %d", i)
			}
		})
	}
}

// TestInMemoryBackend_RingBufferDeleteClears verifies that deleting a
// connection releases its message buffer (no GetMessages results, no leaked
// state on subsequent ListConnections).
func TestInMemoryBackend_RingBufferDeleteClears(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		writeCount int
	}{
		{name: "few_messages", writeCount: 3},
		{name: "over_cap", writeCount: apigatewaymanagementapi.MaxMessagesPerConnection + 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewaymanagementapi.NewInMemoryBackend()
			_, err := b.CreateConnection("conn", "1.1.1.1", "del-test")
			require.NoError(t, err)

			for i := range tt.writeCount {
				require.NoError(t, b.PostToConnection("conn", fmt.Appendf(nil, "x-%d", i)))
			}

			require.NoError(t, b.DeleteConnection("conn"))
			assert.Empty(t, b.GetMessages("conn"))
			assert.Empty(t, b.ListConnections())
		})
	}
}
