package iotdataplane_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

func newTestHandler(t *testing.T) *iotdataplane.Handler {
	t.Helper()

	return iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
}

func doRequest(t *testing.T, h *iotdataplane.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "IoTDataPlane", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "Publish")
	assert.Contains(t, ops, "GetThingShadow")
	assert.Contains(t, ops, "UpdateThingShadow")
	assert.Contains(t, ops, "DeleteThingShadow")
	assert.Contains(t, ops, "ListNamedShadowsForThing")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "publish",
			method: http.MethodPost,
			path:   "/topics/my/topic",
			wantOp: "Publish",
		},
		{
			name:   "get shadow",
			method: http.MethodGet,
			path:   "/things/myThing/shadow",
			wantOp: "GetThingShadow",
		},
		{
			name:   "update shadow",
			method: http.MethodPost,
			path:   "/things/myThing/shadow",
			wantOp: "UpdateThingShadow",
		},
		{
			name:   "delete shadow",
			method: http.MethodDelete,
			path:   "/things/myThing/shadow",
			wantOp: "DeleteThingShadow",
		},
		{
			name:   "list named shadows",
			method: http.MethodGet,
			path:   "/api/things/shadow/ListNamedShadowsForThing/myThing",
			wantOp: "ListNamedShadowsForThing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "topic", path: "/topics/foo", wantMatch: true},
		{name: "things shadow", path: "/things/foo/shadow", wantMatch: true},
		{name: "list named shadows", path: "/api/things/shadow/ListNamedShadowsForThing/foo", wantMatch: true},
		{name: "other", path: "/other", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ShadowCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shadowDoc := map[string]any{
		"state": map[string]any{
			"desired": map[string]any{"color": "red"},
		},
	}
	docBytes, err := json.Marshal(shadowDoc)
	require.NoError(t, err)

	// Step 1: shadow does not exist yet.
	rec := doRequest(t, h, http.MethodGet, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Step 2: create/update classic shadow.
	rec = doRequest(t, h, http.MethodPost, "/things/myThing/shadow", docBytes)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "color")

	// Step 3: get classic shadow.
	rec = doRequest(t, h, http.MethodGet, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "color")

	// Step 4: create named shadow.
	rec = doRequest(t, h, http.MethodPost, "/things/myThing/shadow?name=myNamedShadow", docBytes)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Step 5: get named shadow.
	rec = doRequest(t, h, http.MethodGet, "/things/myThing/shadow?name=myNamedShadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "color")

	// Step 6: list named shadows.
	rec = doRequest(t, h, http.MethodGet, "/api/things/shadow/ListNamedShadowsForThing/myThing", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "myNamedShadow")

	// Step 7: delete classic shadow.
	rec = doRequest(t, h, http.MethodDelete, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Step 8: delete non-existent shadow.
	rec = doRequest(t, h, http.MethodDelete, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_PublishMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/topics/test", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ShadowMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ListNamedShadowsMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/api/things/shadow/ListNamedShadowsForThing/myThing", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantResource string
	}{
		{
			name:         "topic",
			path:         "/topics/my/topic",
			wantResource: "my/topic",
		},
		{
			name:         "thing shadow",
			path:         "/things/myThing/shadow",
			wantResource: "myThing",
		},
		{
			name:         "list named shadows",
			path:         "/api/things/shadow/ListNamedShadowsForThing/myThing",
			wantResource: "myThing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "iotdata", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "Publish")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 88, h.MatchPriority())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &iotdataplane.Provider{}
	assert.Equal(t, "IoTDataPlane", p.Name())
}

func TestHandler_PublishEmptyTopic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/topics/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListNamedShadows_EmptyThingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListNamedShadowsForThing/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBackend_ListNamedShadows_NoShadows(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	names, err := b.ListNamedShadowsForThing("nonexistent")
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestBackend_DeleteThingShadow_ThingNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	_, err := b.DeleteThingShadow("nonexistent", "")
	require.Error(t, err)
}

func TestBackend_GetThingShadow_ThingNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	_, err := b.GetThingShadow("nonexistent", "")
	require.Error(t, err)
}

func TestBackend_ShadowVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updates     [][]byte
		wantVersion int
		wantErr     bool
	}{
		{
			name: "first_update_is_version_1",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
			},
			wantVersion: 1,
		},
		{
			name: "second_update_increments_to_2",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
				[]byte(`{"state":{"desired":{"color":"blue"}}}`),
			},
			wantVersion: 2,
		},
		{
			name: "correct_version_check_succeeds",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
				// second update supplies version=1 which matches current
				[]byte(`{"state":{"desired":{"color":"blue"}},"version":1}`),
			},
			wantVersion: 2,
		},
		{
			name: "wrong_version_check_returns_conflict",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
				// second update supplies wrong version=99
				[]byte(`{"state":{"desired":{"color":"blue"}},"version":99}`),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()

			var lastErr error

			for _, doc := range tt.updates {
				_, lastErr = b.UpdateThingShadow("thing", "", doc)
				if lastErr != nil {
					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
				require.ErrorIs(t, lastErr, iotdataplane.ErrVersionConflict)

				return
			}

			require.NoError(t, lastErr)

			resp, err := b.GetThingShadow("thing", "")
			require.NoError(t, err)

			var result map[string]any
			require.NoError(t, json.Unmarshal(resp, &result))

			gotVersion, ok := result["version"].(float64)
			require.True(t, ok, "response should contain numeric version field")
			assert.Equal(t, tt.wantVersion, int(gotVersion))

			_, hasTimestamp := result["timestamp"]
			assert.True(t, hasTimestamp, "response should contain timestamp field")
		})
	}
}

func TestHandler_ShadowVersionConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doc := []byte(`{"state":{"desired":{"color":"red"}}}`)

	// Initial update – creates shadow at version 1.
	rec := doRequest(t, h, http.MethodPost, "/things/myThing/shadow", doc)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Second update with wrong version should return HTTP 409.
	docBadVer := []byte(`{"state":{"desired":{"color":"blue"}},"version":99}`)
	rec = doRequest(t, h, http.MethodPost, "/things/myThing/shadow", docBadVer)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "VersionConflictException")
}

func TestHandler_ShadowResponseContainsVersionAndTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doc := []byte(`{"state":{"desired":{"color":"green"}}}`)

	// Update returns shadow document with version and timestamp.
	rec := doRequest(t, h, http.MethodPost, "/things/myThing/shadow", doc)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "version")
	assert.Contains(t, resp, "timestamp")

	// Get also returns shadow document with version and timestamp.
	rec = doRequest(t, h, http.MethodGet, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Contains(t, getResp, "version")
	assert.Contains(t, getResp, "timestamp")
}

func TestBackend_ShadowUpdate_NonObjectDocumentRejected(t *testing.T) {
	t.Parallel()

	// AWS IoT requires shadow documents to be JSON objects. Arrays, primitives,
	// and plain strings must be rejected with ErrValidation.
	b := iotdataplane.NewInMemoryBackend()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "json_array", payload: []byte(`["a","b","c"]`)},
		{name: "plain_string", payload: []byte(`"just a string"`)},
		{name: "number", payload: []byte(`42`)},
		{name: "empty", payload: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.UpdateThingShadow("thing-"+tt.name, "", tt.payload)
			require.ErrorIs(t, err, iotdataplane.ErrValidation, "non-object document must be rejected")
		})
	}
}

func TestBackend_ShadowUpdate_ObjectDocumentSucceeds(t *testing.T) {
	t.Parallel()

	// Valid JSON object documents must be accepted and the response must include
	// version + timestamp (verifies the response is built before state mutation).
	b := iotdataplane.NewInMemoryBackend()

	resp, err := b.UpdateThingShadow("thing1", "", []byte(`{"state":{"desired":{"key":"value"}}}`))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp, &result), "response must be valid JSON")
	assert.Contains(t, result, "version", "response must contain version")
	assert.Contains(t, result, "timestamp", "response must contain timestamp")
}

// mockMQTTPublisher implements MQTTPublisher for testing.
type mockMQTTPublisher struct {
	topic   string
	payload []byte
}

func (m *mockMQTTPublisher) Publish(topic string, payload []byte, _ bool, _ byte) error {
	m.topic = topic
	m.payload = payload

	return nil
}

func TestBackend_Publish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topic     string
		payload   []byte
		wantErr   bool
		setupMock bool
	}{
		{
			name:      "publish with broker",
			topic:     "test/topic",
			payload:   []byte("hello"),
			wantErr:   false,
			setupMock: true,
		},
		{
			name:    "publish without broker",
			topic:   "test/topic",
			payload: []byte("hello"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()

			if tt.setupMock {
				mock := &mockMQTTPublisher{}
				b.SetBroker(mock)
			}

			err := b.Publish(tt.topic, tt.payload, 0, false)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_PublishWithBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		body     []byte
		wantCode int
	}{
		{
			name:     "publish plain payload",
			path:     "/topics/test/topic",
			body:     []byte("hello"),
			wantCode: http.StatusOK,
		},
		{
			name:     "publish json payload",
			path:     "/topics/json/topic",
			body:     []byte(`{"payload":"world"}`),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			b.SetBroker(&mockMQTTPublisher{})
			h := iotdataplane.NewHandler(b)

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		clientID string
		wantCode int
	}{
		{
			name:     "delete_existing_connection",
			method:   http.MethodDelete,
			clientID: "client-001",
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_nonexistent_connection_is_idempotent",
			method:   http.MethodDelete,
			clientID: "unknown-client",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_clientId_returns_bad_request",
			method:   http.MethodDelete,
			clientID: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "wrong_method_returns_method_not_allowed",
			method:   http.MethodGet,
			clientID: "client-001",
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			b.AddConnectionInternal("client-001")
			h := iotdataplane.NewHandler(b)

			path := "/_admin/connections/" + tt.clientID
			rec := doRequest(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetRetainedMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iotdataplane.InMemoryBackend)
		name        string
		method      string
		topic       string
		wantPayload string
		wantCode    int
	}{
		{
			name: "get_existing_retained_message",
			setup: func(b *iotdataplane.InMemoryBackend) {
				require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 0))
			},
			method:      http.MethodGet,
			topic:       "sensor/temp",
			wantCode:    http.StatusOK,
			wantPayload: "sensor/temp",
		},
		{
			name:     "get_nonexistent_topic_returns_not_found",
			setup:    nil,
			method:   http.MethodGet,
			topic:    "sensor/humidity",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_topic_returns_bad_request",
			setup:    nil,
			method:   http.MethodGet,
			topic:    "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "wrong_method_returns_method_not_allowed",
			setup:    nil,
			method:   http.MethodPost,
			topic:    "sensor/temp",
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			h := iotdataplane.NewHandler(b)

			path := "/retainedMessage/" + tt.topic
			rec := doRequest(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantPayload != "" {
				assert.Contains(t, rec.Body.String(), tt.wantPayload)
			}
		})
	}
}

func TestHandler_ListRetainedMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*iotdataplane.InMemoryBackend)
		name       string
		method     string
		wantTopics []string
		wantCode   int
	}{
		{
			name: "list_with_multiple_retained_messages",
			setup: func(b *iotdataplane.InMemoryBackend) {
				require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 0))
				require.NoError(t, b.StoreRetainedMessage("sensor/humidity", []byte("70"), 1))
			},
			method:     http.MethodGet,
			wantCode:   http.StatusOK,
			wantTopics: []string{"sensor/humidity", "sensor/temp"},
		},
		{
			name:     "list_empty_returns_empty_array",
			setup:    nil,
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "wrong_method_returns_method_not_allowed",
			setup:    nil,
			method:   http.MethodPost,
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			h := iotdataplane.NewHandler(b)

			rec := doRequest(t, h, tt.method, "/retainedMessage", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, topic := range tt.wantTopics {
				assert.Contains(t, rec.Body.String(), topic)
			}
		})
	}
}

func TestHandler_PublishWithRetain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		path          string
		retainedTopic string
		body          []byte
		wantCode      int
		wantRetained  bool
	}{
		{
			name:          "publish_with_retain_true_stores_message",
			path:          "/topics/sensor/data?retain=true",
			body:          []byte(`{"temp":25}`),
			wantCode:      http.StatusOK,
			wantRetained:  true,
			retainedTopic: "sensor/data",
		},
		{
			name:          "publish_without_retain_does_not_store",
			path:          "/topics/sensor/data",
			body:          []byte(`{"temp":25}`),
			wantCode:      http.StatusOK,
			wantRetained:  false,
			retainedTopic: "sensor/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			b.SetBroker(&mockMQTTPublisher{})
			h := iotdataplane.NewHandler(b)

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			_, err := b.GetRetainedMessage(tt.retainedTopic)
			if tt.wantRetained {
				require.NoError(t, err, "retained message should be stored")
			} else {
				require.Error(t, err, "retained message should not be stored")
			}
		})
	}
}

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "DeleteConnection")
	assert.Contains(t, ops, "GetRetainedMessage")
	assert.Contains(t, ops, "ListRetainedMessages")
}

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "delete_connection",
			method: http.MethodDelete,
			path:   "/_admin/connections/client-001",
			wantOp: "DeleteConnection",
		},
		{
			name:   "get_retained_message",
			method: http.MethodGet,
			path:   "/retainedMessage/sensor/temp",
			wantOp: "GetRetainedMessage",
		},
		{
			name:   "list_retained_messages",
			method: http.MethodGet,
			path:   "/retainedMessage",
			wantOp: "ListRetainedMessages",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_RouteMatcher_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "connections", path: "/_admin/connections/client-001", wantMatch: true},
		{name: "retained_message_by_topic", path: "/retainedMessage/sensor/temp", wantMatch: true},
		{name: "list_retained_messages", path: "/retainedMessage", wantMatch: true},
		{name: "unrelated", path: "/other/path", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestBackend_RetainedMessageLifecycle(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Store two retained messages.
	require.NoError(t, b.StoreRetainedMessage("a/b", []byte("hello"), 1))
	require.NoError(t, b.StoreRetainedMessage("c/d", []byte("world"), 0))

	// GetRetainedMessage returns exact data.
	msg, err := b.GetRetainedMessage("a/b")
	require.NoError(t, err)
	assert.Equal(t, "a/b", msg.Topic)
	assert.Equal(t, []byte("hello"), msg.Payload)
	assert.Equal(t, int32(1), msg.Qos)
	assert.NotZero(t, msg.LastModifiedTime)

	// ListRetainedMessages returns sorted summaries.
	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, "a/b", msgs[0].Topic)
	assert.Equal(t, "c/d", msgs[1].Topic)

	// Storing empty payload removes the retained message.
	require.NoError(t, b.StoreRetainedMessage("a/b", []byte{}, 0))
	_, err = b.GetRetainedMessage("a/b")
	require.Error(t, err)
}

func TestBackend_DeleteConnection_Idempotent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("my-client")

	// First delete succeeds.
	require.NoError(t, b.DeleteConnection("my-client"))
	// Second delete on already-removed client is also a no-op.
	require.NoError(t, b.DeleteConnection("my-client"))
	// Deleting an unknown client is also fine.
	require.NoError(t, b.DeleteConnection("never-existed"))
}
