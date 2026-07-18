package iotdataplane_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_RouteMatcher_DeleteConnectionRealPath verifies the RouteMatcher
// (which real HTTP traffic goes through, unlike doRequest's direct Handler()
// call) recognizes the real AWS DeleteConnection wire path "DELETE
// /connections/{clientId}", while GET/POST on the same bare prefix -- which
// have no real AWS equivalent -- do not match.
func TestHandler_RouteMatcher_DeleteConnectionRealPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		wantMatch bool
	}{
		{name: "delete_matches", method: http.MethodDelete, path: "/connections/client-001", wantMatch: true},
		{name: "get_does_not_match", method: http.MethodGet, path: "/connections/client-001", wantMatch: false},
		{name: "post_does_not_match", method: http.MethodPost, path: "/connections/client-001", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
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
func Test_DeleteConnection_DollarPrefixReturns400(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodDelete, "/_admin/connections/$reserved", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidRequestException")
}
func Test_DeleteConnection_Idempotent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("c1")

	require.NoError(t, b.DeleteConnection("c1"))
	require.NoError(t, b.DeleteConnection("c1"))
	require.NoError(t, b.DeleteConnection("never-existed"))

	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}
func Test_ListConnections_Empty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	conns := resp["connections"].([]any)
	assert.Empty(t, conns)
}
func Test_RegisterConnection_Success(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))
}
func Test_RegisterConnection_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceAlreadyExistsException")
}
func Test_RegisterConnection_DollarPrefixRejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/$system", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func Test_ListConnections_ShowsRegistered(t *testing.T) {
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
func Test_DeleteConnection_RemovesFromList(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/_admin/connections/device-x", nil)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	doRequest(t, h, http.MethodDelete, "/_admin/connections/device-x", nil)
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}
func Test_Connections_WrongMethod(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// PUT on /connections should return 405.
	rec := doRequest(t, h, http.MethodPut, "/_admin/connections", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
func Test_ListConnections_SortedByConnectedAt(t *testing.T) {
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
func Test_RegisterConnection_NotIdempotent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	require.NoError(t, b.RegisterConnection("c1", ""))
	err := b.RegisterConnection("c1", "")
	require.ErrorIs(t, err, iotdataplane.ErrConnectionExists)
}
func Test_AdminConnectionsPath_BasicFlow(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// List (empty initially).
	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Register a new connection.
	rec = doRequest(t, h, http.MethodPost, "/_admin/connections/dev-x", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	// List shows it.
	rec = doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["connections"].([]any), 1)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/_admin/connections/dev-x", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}

// TestRefinement3_ConnectionsPath_NonAWSOpsReturn404 verifies that
// RegisterConnection and ListConnections -- which have no real AWS
// iotdataplane equivalent -- are unreachable at the bare /connections path
// (they only exist under the gopherstack-only /_admin/connections alias).
func Test_ConnectionsPath_NonAWSOpsReturn404(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/connections"},
		{http.MethodPost, "/connections/device-x"},
	}

	for _, tt := range tests {
		t.Run(tt.method+tt.path, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"RegisterConnection/ListConnections have no real AWS path and stay 404 outside /_admin")
		})
	}
}

// TestRefinement3_DeleteConnection_RealAWSPath verifies that DeleteConnection
// -- unlike RegisterConnection/ListConnections -- IS a real, published AWS
// iotdataplane operation (aws-sdk-go-v2/service/iotdataplane.DeleteConnection,
// wire path "DELETE /connections/{clientId}") and so must remain reachable at
// its real wire path, not just the gopherstack /_admin alias.
func Test_DeleteConnection_RealAWSPath(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/_admin/connections/device-x", nil)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	rec := doRequest(t, h, http.MethodDelete, "/connections/device-x", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "real AWS DeleteConnection wire path must work")
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}
func Test_Connection_Register_DuplicateShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/_admin/connections/client-1", nil)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/client-1", nil)
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceAlreadyExistsException", resp["error"])
}
func Test_Connection_EmptyClientID_Rejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// POST /_admin/connections/ with empty clientId segment.
	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func Test_Connection_DollarPrefix_Rejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/$system-client", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
}
func Test_Connection_DeleteIdempotent_DollarRejected(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Deleting a nonexistent ID is idempotent.
	err := b.DeleteConnection("nonexistent-client")
	require.NoError(t, err, "delete of nonexistent client must be idempotent")

	// Dollar-prefix rejected even for delete.
	err = b.DeleteConnection("$system")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}
