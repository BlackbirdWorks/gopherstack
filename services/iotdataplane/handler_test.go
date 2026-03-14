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
	err := b.DeleteThingShadow("nonexistent", "")
	require.Error(t, err)
}

func TestBackend_GetThingShadow_ThingNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	_, err := b.GetThingShadow("nonexistent", "")
	require.Error(t, err)
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

			err := b.Publish(tt.topic, tt.payload)

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

	b := iotdataplane.NewInMemoryBackend()
	mock := &mockMQTTPublisher{}
	b.SetBroker(mock)
	h := iotdataplane.NewHandler(b)

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

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
