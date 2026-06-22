package apigatewaymanagementapi_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

func TestBackend_RingBuffer_OverwriteAndOrder(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("ring", "1.1.1.1", "ua", nil)
	require.NoError(t, err)

	total := apigatewaymanagementapi.MaxMessagesPerConnection * 2
	for i := range total {
		require.NoError(t, b.PostToConnection("ring", fmt.Appendf(nil, "%d", i)))
	}

	msgs := b.GetMessages("ring")
	assert.Len(t, msgs, apigatewaymanagementapi.MaxMessagesPerConnection)

	// Oldest message retained should be the one at offset (total - cap).
	expectFirst := fmt.Appendf(nil, "%d", total-apigatewaymanagementapi.MaxMessagesPerConnection)
	assert.Equal(t, expectFirst, msgs[0].Data)

	// Newest message retained should be the last write.
	expectLast := fmt.Appendf(nil, "%d", total-1)
	assert.Equal(t, expectLast, msgs[len(msgs)-1].Data)
}

func TestBackend_BroadcastDelivers(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	for _, id := range []string{"a", "b", "c"} {
		_, err := b.CreateConnection(id, "1.1.1.1", "ua", nil)
		require.NoError(t, err)
	}

	delivered, err := b.Broadcast([]byte("ping"))
	require.NoError(t, err)
	assert.Equal(t, 3, delivered)

	for _, id := range []string{"a", "b", "c"} {
		msgs := b.GetMessages(id)
		require.Len(t, msgs, 1)
		assert.Equal(t, []byte("ping"), msgs[0].Data)
	}

	stats := b.Stats()
	assert.Equal(t, int64(1), stats.TotalBroadcasts)
	assert.Equal(t, int64(3), stats.TotalMessages)
}

func TestBackend_PingUpdatesActivity(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	conn, err := b.CreateConnection("p", "1.1.1.1", "ua", nil)
	require.NoError(t, err)

	original := conn.LastActiveAt
	require.NoError(t, b.PingConnection("p"))

	updated, err := b.GetConnection("p")
	require.NoError(t, err)
	assert.False(t, updated.LastActiveAt.Before(original))

	tl := b.GetTimeline("p")
	require.Len(t, tl, 2)
	assert.Equal(t, apigatewaymanagementapi.EventConnected, tl[0].Type)
	assert.Equal(t, apigatewaymanagementapi.EventPing, tl[1].Type)
}

func TestBackend_ClearMessagesPreservesConnection(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()
	_, err := b.CreateConnection("clr", "1.1.1.1", "ua", nil)
	require.NoError(t, err)
	require.NoError(t, b.PostToConnection("clr", []byte("x")))

	require.NoError(t, b.ClearMessages("clr"))
	assert.Empty(t, b.GetMessages("clr"))

	conn, err := b.GetConnection("clr")
	require.NoError(t, err)
	assert.Equal(t, "clr", conn.ConnectionID)
}

func TestBackend_FilterConnectionsCaseInsensitive(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()
	_, err := b.CreateConnection("WS-Alpha", "10.0.0.1", "Chrome", nil)
	require.NoError(t, err)
	_, err = b.CreateConnection("ws-beta", "10.0.0.2", "Firefox", nil)
	require.NoError(t, err)

	out := b.FilterConnections("ALPHA")
	require.Len(t, out, 1)
	assert.Equal(t, "WS-Alpha", out[0].ConnectionID)

	out = b.FilterConnections("firefox")
	require.Len(t, out, 1)
	assert.Equal(t, "ws-beta", out[0].ConnectionID)
}

func TestBackend_StatsTracksRejected(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	err := b.PostToConnection("missing", []byte("x"))
	require.Error(t, err)

	stats := b.Stats()
	assert.Equal(t, int64(1), stats.TotalRejected)
}

func TestBackend_SnapshotPreservesEverything(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()
	_, err := b.CreateConnection("snap", "1.1.1.1", "ua", nil)
	require.NoError(t, err)
	require.NoError(t, b.PostToConnection("snap", []byte("hello")))
	require.NoError(t, b.PingConnection("snap"))

	data := b.Snapshot()
	require.NotNil(t, data)

	// Verify well-formed JSON
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	fresh := apigatewaymanagementapi.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(data))

	conn, err := fresh.GetConnection("snap")
	require.NoError(t, err)
	assert.Equal(t, "snap", conn.ConnectionID)

	msgs := fresh.GetMessages("snap")
	require.Len(t, msgs, 1)
	assert.Equal(t, []byte("hello"), msgs[0].Data)

	tl := fresh.GetTimeline("snap")
	require.Len(t, tl, 3)
}
