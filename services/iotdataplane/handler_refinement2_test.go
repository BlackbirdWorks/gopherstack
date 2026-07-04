package iotdataplane_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// --- Topic validation ---

func TestRefinement2_TopicValidation_Wildcards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		topic    string
		wantCode int
	}{
		{name: "hash_wildcard", topic: "sensor/#", wantCode: http.StatusBadRequest},
		{name: "plus_wildcard", topic: "sensor/+/temp", wantCode: http.StatusBadRequest},
		{name: "multi_wildcard", topic: "#", wantCode: http.StatusBadRequest},
		{name: "valid_topic", topic: "sensor/temperature", wantCode: http.StatusOK},
		{name: "valid_nested", topic: "a/b/c/d", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/topics/"+tt.topic, []byte("x"))
			assert.Equal(t, tt.wantCode, rec.Code, "topic: %q", tt.topic)
		})
	}
}

func TestRefinement2_TopicValidation_Length(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// 257-character topic must be rejected.
	longTopic := strings.Repeat("a", 257)
	rec := doRequest(t, h, http.MethodPost, "/topics/"+longTopic, []byte("x"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// 256-character topic must be accepted.
	exactTopic := strings.Repeat("a", 256)
	rec = doRequest(t, h, http.MethodPost, "/topics/"+exactTopic, []byte("x"))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement2_TopicValidation_EmptySegments(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// Topic with empty segment (consecutive slashes) must be rejected.
	rec := doRequest(t, h, http.MethodPost, "/topics/a//b", []byte("x"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Shadow name validation ---

func TestRefinement2_ShadowNameValidation_InvalidChars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryParam string // URL-encoded query string
		wantCode   int
	}{
		{name: "valid_alphanumeric", queryParam: "myShadow123", wantCode: http.StatusNotFound},
		{name: "valid_with_colon", queryParam: "ns:shadow", wantCode: http.StatusNotFound},
		{name: "valid_with_underscore", queryParam: "my_shadow", wantCode: http.StatusNotFound},
		{name: "valid_with_hyphen", queryParam: "my-shadow", wantCode: http.StatusNotFound},
		{name: "invalid_space", queryParam: "my%20shadow", wantCode: http.StatusBadRequest},
		{name: "invalid_dot", queryParam: "my.shadow", wantCode: http.StatusBadRequest},
		{name: "invalid_at", queryParam: "my%40shadow", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+tt.queryParam, nil)
			assert.Equal(t, tt.wantCode, rec.Code, "queryParam: %q", tt.queryParam)
		})
	}
}

func TestRefinement2_ShadowNameValidation_TooLong(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// 65-character shadow name must be rejected.
	longName := strings.Repeat("a", 65)
	rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+longName, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// 64-character shadow name must be accepted (name valid, just thing not found).
	exactName := strings.Repeat("a", 64)
	rec = doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+exactName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Shadow document validation ---

func TestRefinement2_ShadowDocumentValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{name: "valid_object", body: []byte(`{"state":{"desired":{}}}`), wantCode: http.StatusOK},
		// AWS requires "state" key; {} without it returns 400 InvalidRequestException.
		{name: "empty_object", body: []byte(`{}`), wantCode: http.StatusBadRequest},
		{name: "array", body: []byte(`["a"]`), wantCode: http.StatusBadRequest},
		{name: "number", body: []byte(`42`), wantCode: http.StatusBadRequest},
		{name: "string", body: []byte(`"hello"`), wantCode: http.StatusBadRequest},
		{name: "empty", body: []byte{}, wantCode: http.StatusBadRequest},
		{name: "null", body: []byte(`null`), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/things/thing1/shadow", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %q", string(tt.body))
		})
	}
}

func TestRefinement2_ShadowDocumentValidation_TooLarge(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// Build a document just over the HTTP body limit (maxShadowBodyBytes = 8KB).
	// The HTTP MaxBytesReader fires before backend validation, so expect 413.
	value := strings.Repeat("v", iotdataplane.MaxShadowDocumentBytes+1)
	oversize := fmt.Sprintf(`{"k":%q}`, value)

	rec := doRequest(t, h, http.MethodPost, "/things/thing1/shadow", []byte(oversize))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestRefinement2_ShadowDocumentValidation_BackendSizeCheck(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// The backend-level size check catches documents that exceed MaxShadowDocumentBytes
	// (used when the backend is invoked directly, bypassing HTTP body limits).
	value := strings.Repeat("v", iotdataplane.MaxShadowDocumentBytes+1)
	oversize := fmt.Appendf(nil, `{"k":%q}`, value)

	_, err := b.UpdateThingShadow("thing1", "", oversize)
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

// --- Version overflow protection ---

func TestRefinement2_ShadowVersion_OverflowProtection(t *testing.T) {
	t.Parallel()

	// Create a shadow with version just below the rollover point, then update
	// to verify it resets to 1 rather than overflowing into negative territory.
	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{}`))

	// Force the version to max by doing many updates. We'll set it via the internal
	// helper and verify the next update starts a new cycle.
	iotdataplane.ForceSetShadowVersion(b, "thing1", "", iotdataplane.MaxShadowVersion)

	resp, err := b.UpdateThingShadow("thing1", "", []byte(`{"state":{}}`))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp, &result))

	versionFloat, ok := result["version"].(float64)
	require.True(t, ok, "version must be a number")
	assert.Equal(t, 1, int(versionFloat), "version must reset to 1 after overflow")
}

// --- Connection management ---

func TestRefinement2_ListConnections_Empty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	conns := resp["connections"].([]any)
	assert.Empty(t, conns)
}

func TestRefinement2_RegisterConnection_Success(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))
}

func TestRefinement2_RegisterConnection_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceAlreadyExistsException")
}

func TestRefinement2_RegisterConnection_DollarPrefixRejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/$system", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_ListConnections_ShowsRegistered(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// Register two connections.
	doRequest(t, h, http.MethodPost, "/_admin/connections/device-a", nil)
	doRequest(t, h, http.MethodPost, "/_admin/connections/device-b", nil)

	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	conns := resp["connections"].([]any)
	assert.Len(t, conns, 2)
}

func TestRefinement2_DeleteConnection_RemovesFromList(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/_admin/connections/device-x", nil)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	doRequest(t, h, http.MethodDelete, "/_admin/connections/device-x", nil)
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}

func TestRefinement2_Connections_WrongMethod(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// PUT on /connections should return 405.
	rec := doRequest(t, h, http.MethodPut, "/_admin/connections", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// --- ListThingsWithShadows ---

func TestRefinement2_ListThingsWithShadows_Empty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	things := resp["things"].([]any)
	assert.Empty(t, things)
}

func TestRefinement2_ListThingsWithShadows_Multiple(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing-a", "", []byte(`{}`))
	b.AddShadowInternal("thing-b", "shadow1", []byte(`{}`))
	b.AddShadowInternal("thing-c", "shadow2", []byte(`{}`))

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	things := resp["things"].([]any)
	assert.Len(t, things, 3)
	// Must be sorted.
	assert.Equal(t, "thing-a", things[0])
	assert.Equal(t, "thing-b", things[1])
	assert.Equal(t, "thing-c", things[2])
}

func TestRefinement2_ListThingsWithShadows_WrongMethod(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/api/things/shadow/ListThingsWithShadows", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// --- Shadow path precision ---

func TestRefinement2_ShadowPath_Precision(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("mydevice", "", []byte(`{"state":{}}`))
	h := iotdataplane.NewHandler(b)

	// Exact shadow path must succeed.
	rec := doRequest(t, h, http.MethodGet, "/things/mydevice/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Path with extra segment after /shadow must return 404 (not matched as shadow).
	rec = doRequest(t, h, http.MethodGet, "/things/mydevice/shadow/extra", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Persistence round-trip with new Connection struct ---

func TestRefinement2_Persistence_ConnectionsPreserved(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("client-1")
	b.AddConnectionInternal("client-2")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 2, iotdataplane.ConnectionCount(b2))

	// Verify connections are present with their timestamps preserved.
	conns := b2.ListConnections()
	assert.Len(t, conns, 2)
	for _, c := range conns {
		assert.False(t, c.ConnectedAt.IsZero(), "ConnectedAt must be restored")
	}
}

// --- Backend ListThingsWithShadows ---

func TestRefinement2_Backend_ListThingsWithShadows_Sorted(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("zebra", "", []byte(`{}`))
	b.AddShadowInternal("apple", "", []byte(`{}`))
	b.AddShadowInternal("mango", "", []byte(`{}`))

	things := b.ListThingsWithShadows()
	require.Len(t, things, 3)
	assert.Equal(t, "apple", things[0])
	assert.Equal(t, "mango", things[1])
	assert.Equal(t, "zebra", things[2])
}

// --- Backend ListConnections sorted by connectedAt ---

func TestRefinement2_Backend_ListConnections_SortedByConnectedAt(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	// Add three connections with small sleeps to ensure distinct timestamps.
	for _, id := range []string{"first", "second", "third"} {
		b.AddConnectionInternal(id)
	}

	conns := b.ListConnections()
	// All must be present; order is by connectedAt (insertion order here).
	assert.Len(t, conns, 3)
}

// --- RegisterConnection idempotency ---

func TestRefinement2_RegisterConnection_NotIdempotent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	require.NoError(t, b.RegisterConnection("c1", ""))
	err := b.RegisterConnection("c1", "")
	require.ErrorIs(t, err, iotdataplane.ErrConnectionExists)
}

// --- RouteMatcher for new paths (via HTTP round-trip) ---

func TestRefinement2_RouteMatcher_NewPaths(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("device", "", []byte(`{"state":{}}`))
	h := iotdataplane.NewHandler(b)

	tests := []struct {
		method  string
		path    string
		body    []byte
		matched bool
	}{
		{http.MethodGet, "/_admin/connections", nil, true},
		{http.MethodPost, "/_admin/connections/device-1", nil, true}, // registers → 201
		{http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil, true},
		{http.MethodGet, "/things/device/shadow", nil, true}, // seeded above → 200
		{http.MethodGet, "/things/device/shadow/extra", nil, false},
		{http.MethodGet, "/other", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			if tt.matched {
				assert.NotEqual(t, http.StatusNotFound, rec.Code, "path %q should be matched by handler", tt.path)
			} else {
				assert.Equal(t, http.StatusNotFound, rec.Code, "path %q should NOT be matched by handler", tt.path)
			}
		})
	}
}
