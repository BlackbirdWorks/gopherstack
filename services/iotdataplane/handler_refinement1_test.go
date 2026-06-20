package iotdataplane_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// --- infrastructure tests ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *iotdataplane.InMemoryBackend)
		name  string
	}{
		{
			name: "empty_backend",
		},
		{
			name: "populated_backend_clears_all",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("thing1", "", []byte(`{"state":{}}`))
				b.AddConnectionInternal("client-1")
				require.NoError(t, b.StoreRetainedMessage("t/1", []byte("x"), 0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			b.Reset()

			assert.Equal(t, 0, iotdataplane.ShadowCount(b))
			assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
			assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
		})
	}
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for range 3 {
		b.AddShadowInternal("thing", "shadow", []byte(`{}`))
		require.NoError(t, b.StoreRetainedMessage("t/1", []byte("x"), 0))
		b.Reset()
		assert.Equal(t, 0, iotdataplane.ShadowCount(b))
		assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing", "", []byte(`{"state":{}}`))
	h := iotdataplane.NewHandler(b)
	assert.Equal(t, 1, iotdataplane.ShadowCount(b))

	h.Reset()

	assert.Equal(t, 0, iotdataplane.ShadowCount(b))
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &iotdataplane.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, iotdataplane.ErrNilAppContext)
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	ops := h.GetSupportedOperations()
	want := []string{
		"DeleteConnection", "DeleteThingShadow", "GetRetainedMessage",
		"GetThingShadow", "ListNamedShadowsForThing", "ListRetainedMessages",
		"Publish", "UpdateThingShadow",
	}

	for _, op := range want {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	b.AddShadowInternal("thing1", "named", []byte(`{"state":{"desired":{"k":"v2"}}}`))
	b.AddConnectionInternal("client-abc")
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 0))

	assert.Equal(t, 2, iotdataplane.ShadowCount(b))
	assert.Equal(t, 1, iotdataplane.ThingCount(b))
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	assert.Equal(t, 0, iotdataplane.ShadowCount(b))
	assert.Equal(t, 0, iotdataplane.ThingCount(b))
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))

	b.AddShadowInternal("t", "s", []byte(`{}`))
	assert.Equal(t, 1, iotdataplane.ShadowCount(b))
	assert.Equal(t, 1, iotdataplane.ThingCount(b))
}

// --- error mapping tests ---

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantBody string
		wantCode int
	}{
		{
			name:     "publish_empty_topic",
			path:     "/topics/",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_connection_empty_client",
			path:     "/_admin/connections/",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_retained_empty_topic",
			path:     "/retainedMessage/",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			method := http.MethodPost
			if tt.path != "/topics/" {
				method = http.MethodDelete
				if tt.path == "/retainedMessage/" {
					method = http.MethodGet
				}
			}

			rec := doRequest(t, h, method, tt.path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_DeleteConnection_DollarPrefixReturns400(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodDelete, "/_admin/connections/$reserved", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidRequestException")
}

// --- Publish tests ---

func TestRefinement1_Publish_NoBroker_Returns200(t *testing.T) {
	t.Parallel()

	// When no broker is configured publish should silently succeed (HTTP 200),
	// not return 500. This matches localstack behaviour where publish is fire-and-forget.
	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/topics/test/topic", []byte("hello"))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement1_Publish_QosParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		qos      string
		wantCode int
	}{
		{name: "qos_0", qos: "0", wantCode: http.StatusOK},
		{name: "qos_1", qos: "1", wantCode: http.StatusOK},
		{name: "qos_invalid", qos: "2", wantCode: http.StatusBadRequest},
		{name: "qos_negative", qos: "-1", wantCode: http.StatusBadRequest},
		{name: "qos_string", qos: "abc", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/topics/t?qos="+tt.qos, []byte("x"))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_Publish_RetainStores(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// Publish with retain=true should store a retained message.
	rec := doRequest(t, h, http.MethodPost, "/topics/sensor/temp?retain=true", []byte("42"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))

	// Publish without retain should not store.
	b.Reset()
	rec = doRequest(t, h, http.MethodPost, "/topics/sensor/temp", []byte("42"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
}

// --- DeleteThingShadow returns payload ---

func TestRefinement1_DeleteThingShadow_ReturnsPayload(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"state":{"desired":{"key":"value"}}}`))
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodDelete, "/things/thing1/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Response must be a JSON object with version and timestamp (AWS contract).
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "version")
	assert.Contains(t, resp, "timestamp")
}

// --- ListNamedShadowsForThing includes timestamp ---

func TestRefinement1_ListNamedShadows_IncludesTimestamp(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "shadow-a", []byte(`{}`))
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListNamedShadowsForThing/thing1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "results")
	assert.Contains(t, resp, "timestamp")
}

func TestRefinement1_ListNamedShadows_SortedByBackend(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "z-shadow", []byte(`{}`))
	b.AddShadowInternal("thing1", "a-shadow", []byte(`{}`))
	b.AddShadowInternal("thing1", "m-shadow", []byte(`{}`))

	names, err := b.ListNamedShadowsForThing("thing1")
	require.NoError(t, err)
	require.Len(t, names, 3)
	assert.Equal(t, "a-shadow", names[0])
	assert.Equal(t, "m-shadow", names[1])
	assert.Equal(t, "z-shadow", names[2])
}

// --- maxRetainedMessages cap ---

func TestRefinement1_MaxRetainedMessages_LRUEviction(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Fill to max (1000).
	for i := range 1000 {
		topic := fmt.Sprintf("t/%d", i)
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("x"), 0))
	}

	assert.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	// Adding a new topic at cap must succeed via LRU eviction (not fail).
	require.NoError(t, b.StoreRetainedMessage("overflow/topic", []byte("y"), 0))

	// Count stays at cap — one entry was evicted to make room.
	assert.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	// The new entry must be stored.
	msg, err := b.GetRetainedMessage("overflow/topic")
	require.NoError(t, err)
	assert.Equal(t, []byte("y"), msg.Payload)
}

func TestRefinement1_MaxRetainedMessages_UpdateExistingNotCapped(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("existing/topic", []byte("v1"), 0))

	// Updating an existing topic should never fail even at the cap.
	for range 1010 {
		require.NoError(t, b.StoreRetainedMessage("existing/topic", []byte("v2"), 0))
	}
}

// --- Persistence round-trip ---

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	b.AddConnectionInternal("client-1")
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 1))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, iotdataplane.ShadowCount(b2))
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b2))
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b2))

	// Verify retained message data integrity.
	msg, err := b2.GetRetainedMessage("sensor/temp")
	require.NoError(t, err)
	assert.Equal(t, []byte("42"), msg.Payload)
	assert.Equal(t, int32(1), msg.Qos)
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 0, iotdataplane.ShadowCount(b2))
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b2))
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b2))
}

func TestRefinement1_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"k":"v"}`))
	h := iotdataplane.NewHandler(b)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	require.NoError(t, h2.Restore(snap))
	assert.Equal(t, 1, iotdataplane.ShadowCount(b))
}

// --- ListRetainedMessages pagination ---

func TestRefinement1_ListRetainedMessages_Pagination(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for _, topic := range []string{"a/1", "b/2", "c/3", "d/4", "e/5"} {
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("x"), 0))
	}

	h := iotdataplane.NewHandler(b)

	// First page: maxResults=2.
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage?maxResults=2", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	topics1 := page1["retainedTopics"].([]any)
	assert.Len(t, topics1, 2)

	nextToken, hasNext := page1["nextToken"].(string)
	assert.True(t, hasNext, "should have nextToken when more pages exist")

	// Second page.
	rec = doRequest(t, h, http.MethodGet, "/retainedMessage?maxResults=2&nextToken="+nextToken, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	topics2 := page2["retainedTopics"].([]any)
	assert.Len(t, topics2, 2)
}

func TestRefinement1_ListRetainedMessages_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	topics, ok := resp["retainedTopics"]
	require.True(t, ok)
	// Must be an array (not null).
	_, isSlice := topics.([]any)
	assert.True(t, isSlice, "retainedTopics should be a JSON array, not null")
}

// --- StoreRetainedMessage clears with empty payload ---

func TestRefinement1_StoreRetainedMessage_EmptyPayloadRemoves(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 0))
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))

	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte{}, 0))
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
}

// --- DeleteConnection idempotent ---

func TestRefinement1_DeleteConnection_Idempotent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("c1")

	require.NoError(t, b.DeleteConnection("c1"))
	require.NoError(t, b.DeleteConnection("c1"))
	require.NoError(t, b.DeleteConnection("never-existed"))

	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}

// --- Deep copy protection ---

func TestRefinement1_DeepCopy_RetainedMessage(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/data", []byte("original"), 0))

	msg, err := b.GetRetainedMessage("sensor/data")
	require.NoError(t, err)

	// Mutate the returned payload – the stored copy must not change.
	msg.Payload[0] = 'X'

	msg2, err := b.GetRetainedMessage("sensor/data")
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), msg2.Payload)
}

func TestRefinement1_DeepCopy_ListRetainedMessages(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("t/1", []byte("hello"), 0))

	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// Mutate the slice element – the stored copy must be unchanged.
	msgs[0].Payload[0] = 'Z'

	msg, err := b.GetRetainedMessage("t/1")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), msg.Payload)
}

// --- maxShadowsPerThing cap ---

func TestRefinement1_MaxShadowsPerThing_CapEnforced(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Fill to cap.
	for i := range iotdataplane.MaxShadowsPerThing {
		_, err := b.UpdateThingShadow("thing1", fmt.Sprintf("shadow-%d", i), []byte(`{"state":{"desired":{"x":1}}}`))
		require.NoError(t, err)
	}

	assert.Equal(t, iotdataplane.MaxShadowsPerThing, iotdataplane.ShadowCount(b))

	// One more new shadow for the same thing must fail.
	_, err := b.UpdateThingShadow("thing1", "overflow-shadow", []byte(`{"state":{"desired":{"x":1}}}`))
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

func TestRefinement1_MaxShadowsPerThing_UpdateExistingNotCapped(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "existing", []byte(`{"state":{}}`))

	// Updating existing shadow must succeed regardless of cap.
	for range iotdataplane.MaxShadowsPerThing + 10 {
		_, err := b.UpdateThingShadow("thing1", "existing", []byte(`{"state":{"desired":{"k":"v"}}}`))
		require.NoError(t, err)
	}
}

func TestRefinement1_MaxShadowsPerThing_CapPerThing(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Fill thing1 to cap.
	for i := range iotdataplane.MaxShadowsPerThing {
		_, err := b.UpdateThingShadow("thing1", fmt.Sprintf("s-%d", i), []byte(`{"state":{"desired":{"x":1}}}`))
		require.NoError(t, err)
	}

	// thing2 must still accept new shadows.
	_, err := b.UpdateThingShadow("thing2", "new-shadow", []byte(`{"state":{"desired":{"x":1}}}`))
	require.NoError(t, err)
}
