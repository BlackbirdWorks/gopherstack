package apigatewaymanagementapi_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

func newTestHandler(t *testing.T) *apigatewaymanagementapi.Handler {
	t.Helper()

	return apigatewaymanagementapi.NewHandler(apigatewaymanagementapi.NewInMemoryBackend())
}

func doRequest(
	t *testing.T,
	h *apigatewaymanagementapi.Handler,
	method, path string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "APIGatewayManagementAPI", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "PostToConnection")
	assert.Contains(t, ops, "GetConnection")
	assert.Contains(t, ops, "DeleteConnection")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		want   string
	}{
		{name: "post", method: http.MethodPost, want: "PostToConnection"},
		{name: "get", method: http.MethodGet, want: "GetConnection"},
		{name: "delete", method: http.MethodDelete, want: "DeleteConnection"},
		{name: "put", method: http.MethodPut, want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, "/@connections/conn-1", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/@connections/my-conn-id", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "my-conn-id", h.ExtractResource(c))
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "connections prefix", path: "/@connections/conn-1", want: true},
		{name: "not connections", path: "/restapis/something", want: false},
		{name: "dashboard", path: "/dashboard/apigatewaymanagementapi", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_PostToConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		payload      []byte
		wantStatus   int
		preCreate    bool
	}{
		{
			name:         "success",
			connectionID: "conn-abc",
			preCreate:    true,
			payload:      []byte(`{"message":"hello"}`),
			wantStatus:   http.StatusOK,
		},
		{
			name:         "connection not found returns 410",
			connectionID: "conn-missing",
			preCreate:    false,
			payload:      []byte(`{"message":"hello"}`),
			wantStatus:   http.StatusGone,
		},
		{
			name:         "oversized payload returns 413",
			connectionID: "conn-large",
			preCreate:    true,
			payload:      make([]byte, 129*1024),
			wantStatus:   http.StatusRequestEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.preCreate {
				_, err := h.Backend.CreateConnection(tt.connectionID, "127.0.0.1", "test-agent")
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodPost, "/@connections/"+tt.connectionID, tt.payload)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		preCreate    bool
		wantStatus   int
	}{
		{
			name:         "found",
			connectionID: "conn-get-1",
			preCreate:    true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "not found returns 410",
			connectionID: "conn-missing",
			preCreate:    false,
			wantStatus:   http.StatusGone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.preCreate {
				_, err := h.Backend.CreateConnection(tt.connectionID, "10.0.0.1", "test-agent/1.0")
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/@connections/"+tt.connectionID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var conn apigatewaymanagementapi.Connection
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &conn))
				assert.Equal(t, tt.connectionID, conn.ConnectionID)
			}
		})
	}
}

func TestHandler_DeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		preCreate    bool
		wantStatus   int
	}{
		{
			name:         "success",
			connectionID: "conn-del-1",
			preCreate:    true,
			wantStatus:   http.StatusNoContent,
		},
		{
			name:         "not found returns 410",
			connectionID: "conn-missing",
			preCreate:    false,
			wantStatus:   http.StatusGone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.preCreate {
				_, err := h.Backend.CreateConnection(tt.connectionID, "10.0.0.2", "test-agent/2.0")
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodDelete, "/@connections/"+tt.connectionID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreateConnection("conn-1", "127.0.0.1", "test")
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/@connections/conn-1", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_EmptyConnectionID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/@connections/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "apigatewaymanagementapi", h.ChaosServiceName())
	assert.Equal(t, []string{"PostToConnection", "GetConnection", "DeleteConnection"}, h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.Equal(t, 87, h.MatchPriority())
}

func TestBackend_ListConnections(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	conns := b.ListConnections()
	assert.Empty(t, conns)

	_, err := b.CreateConnection("list-conn-1", "1.2.3.4", "ua1")
	require.NoError(t, err)

	_, err = b.CreateConnection("list-conn-2", "5.6.7.8", "ua2")
	require.NoError(t, err)

	conns = b.ListConnections()
	assert.Len(t, conns, 2)
}

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := apigatewaymanagementapi.Provider{}
	assert.Equal(t, "APIGatewayManagementAPI", p.Name())

	h, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestHandler_UnknownPathPrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/restapis/something", nil)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBackend_PostToConnection_AfterDelete(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("c1", "1.2.3.4", "ua")
	require.NoError(t, err)

	require.NoError(t, b.DeleteConnection("c1"))

	err = b.PostToConnection("c1", []byte("data"))
	require.Error(t, err)
}

func TestBackend_GetMessages(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("conn-msg", "1.2.3.4", "ua")
	require.NoError(t, err)

	require.NoError(t, b.PostToConnection("conn-msg", []byte("first")))
	require.NoError(t, b.PostToConnection("conn-msg", []byte("second")))

	msgs := b.GetMessages("conn-msg")
	assert.Len(t, msgs, 2)
	assert.Equal(t, []byte("first"), msgs[0].Data)
	assert.Equal(t, []byte("second"), msgs[1].Data)
}
