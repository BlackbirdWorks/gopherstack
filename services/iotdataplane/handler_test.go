package iotdataplane_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
func TestHandler_GetSupportedOperations_ConnectionAndRetainedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "DeleteConnection")
	assert.Contains(t, ops, "GetRetainedMessage")
	assert.Contains(t, ops, "ListRetainedMessages")
}
func TestHandler_ExtractOperation_ConnectionAndRetainedPaths(t *testing.T) {
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
func TestHandler_RouteMatcher_ConnectionAndRetainedPaths(t *testing.T) {
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
func Test_Reset(t *testing.T) {
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
				require.NoError(t, b.StoreRetainedMessage("t/1", []byte("x"), 0, nil))
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
func Test_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for range 3 {
		b.AddShadowInternal("thing", "shadow", []byte(`{}`))
		require.NoError(t, b.StoreRetainedMessage("t/1", []byte("x"), 0, nil))
		b.Reset()
		assert.Equal(t, 0, iotdataplane.ShadowCount(b))
		assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
	}
}
func Test_HandlerReset(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing", "", []byte(`{"state":{}}`))
	h := iotdataplane.NewHandler(b)
	assert.Equal(t, 1, iotdataplane.ShadowCount(b))

	h.Reset()

	assert.Equal(t, 0, iotdataplane.ShadowCount(b))
}
func Test_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &iotdataplane.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, iotdataplane.ErrNilAppContext)
}
func Test_GetSupportedOperations_AllOps(t *testing.T) {
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
func Test_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	b.AddShadowInternal("thing1", "named", []byte(`{"state":{"desired":{"k":"v2"}}}`))
	b.AddConnectionInternal("client-abc")
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 0, nil))

	assert.Equal(t, 2, iotdataplane.ShadowCount(b))
	assert.Equal(t, 1, iotdataplane.ThingCount(b))
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))
}
func Test_ExportCountHelpers(t *testing.T) {
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
func Test_ErrValidationMapping(t *testing.T) {
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
func Test_Dispatch_ConnectionsShadowsAndThingsPaths(t *testing.T) {
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
func Test_ErrorShapes_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		wantError string
		body      []byte
		wantCode  int
	}{
		{
			name:      "shadow_not_found",
			method:    http.MethodGet,
			path:      "/things/missing-thing/shadow",
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "retained_message_not_found",
			method:    http.MethodGet,
			path:      "/retainedMessage/no/such/topic",
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "invalid_request_bad_state",
			method:    http.MethodPost,
			path:      "/things/device1/shadow",
			body:      []byte(`{}`),
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidRequestException",
		},
		{
			name:      "invalid_request_wildcard_topic",
			method:    http.MethodPost,
			path:      "/topics/bad/+/wildcard",
			body:      []byte(`{"data":"val"}`),
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidRequestException",
		},
		{
			name:      "duplicate_connection",
			method:    http.MethodPost,
			path:      "/_admin/connections/dup-client",
			wantCode:  http.StatusConflict,
			wantError: "ResourceAlreadyExistsException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

			// Pre-seed for duplicate connection test.
			if tt.name == "duplicate_connection" {
				doRequest(t, h, http.MethodPost, "/_admin/connections/dup-client", nil)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "unexpected status for %s %s", tt.method, tt.path)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantError, resp["error"],
				"unexpected error type for %s %s", tt.method, tt.path)
			_, hasMsg := resp["message"]
			assert.True(t, hasMsg, "error response must include message field")
		})
	}
}
func Test_VersionConflict_ErrorShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev/shadow", []byte(`{"state":{"desired":{"k":"v"}}}`))

	rec := doRequest(t, h, http.MethodPost, "/things/dev/shadow",
		[]byte(`{"state":{"desired":{"k":"v2"}},"version":99}`))
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ConflictException", resp["error"])
	// AWS includes the numeric code in the body.
	code, hasCode := resp["code"]
	require.True(t, hasCode, "ConflictException body must include code")
	assert.InDelta(t, float64(http.StatusConflict), code, 0)
}
