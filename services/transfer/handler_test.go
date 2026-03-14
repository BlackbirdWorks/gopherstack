package transfer_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

func newTestHandler(t *testing.T) *transfer.Handler {
	t.Helper()

	return transfer.NewHandler(transfer.NewInMemoryBackend(testAccountID, testRegion))
}

func doTransferRequest(
	t *testing.T,
	h *transfer.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TransferService."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Transfer", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateServer")
	assert.Contains(t, ops, "DescribeServer")
	assert.Contains(t, ops, "ListServers")
	assert.Contains(t, ops, "StartServer")
	assert.Contains(t, ops, "StopServer")
	assert.Contains(t, ops, "DeleteServer")
	assert.Contains(t, ops, "UpdateServer")
	assert.Contains(t, ops, "CreateUser")
	assert.Contains(t, ops, "DescribeUser")
	assert.Contains(t, ops, "ListUsers")
	assert.Contains(t, ops, "DeleteUser")
	assert.Contains(t, ops, "UpdateUser")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches Transfer target",
			target: "TransferService.CreateServer",
			want:   true,
		},
		{
			name:   "does not match wrong prefix",
			target: "AWSShield_20160616.CreateProtection",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_CreateServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:     "default protocols",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			wantKey:  "ServerId",
		},
		{
			name:     "explicit SFTP",
			body:     map[string]any{"Protocols": []string{"SFTP"}},
			wantCode: http.StatusOK,
			wantKey:  "ServerId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateServer", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp[tt.wantKey])
		})
	}
}

func TestHandler_DescribeServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a server first
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "found",
			body:     map[string]any{"ServerId": serverID},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			body:     map[string]any{"ServerId": "s-doesnotexist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing server id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "DescribeServer", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListServers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTransferRequest(t, h, "ListServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.IsType(t, []any{}, resp["Servers"])
}

func TestHandler_StartStopServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*transfer.Handler) string
		name      string
		operation string
		serverID  string
		wantCode  int
	}{
		{
			name:      "stop existing server",
			operation: "StopServer",
			wantCode:  http.StatusOK,
			setup: func(h *transfer.Handler) string {
				s, _ := h.Backend.CreateServer(nil, nil)

				return s.ServerID
			},
		},
		{
			name:      "start existing server",
			operation: "StartServer",
			wantCode:  http.StatusOK,
			setup: func(h *transfer.Handler) string {
				s, _ := h.Backend.CreateServer(nil, nil)

				return s.ServerID
			},
		},
		{
			name:      "stop not found",
			operation: "StopServer",
			serverID:  "s-missing",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			serverID := tt.serverID
			if tt.setup != nil {
				serverID = tt.setup(h)
			}

			rec := doTransferRequest(t, h, tt.operation, map[string]any{"ServerId": serverID})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	rec := doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Second delete should fail
	rec = doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	rec := doTransferRequest(t, h, "UpdateServer", map[string]any{
		"ServerId":  serverID,
		"Protocols": []string{"SFTP", "FTPS"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UserCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create server
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	// Create user
	createUserRec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId":      serverID,
		"UserName":      "alice",
		"HomeDirectory": "/alice",
		"Role":          "arn:aws:iam::123456789012:role/role",
	})
	assert.Equal(t, http.StatusOK, createUserRec.Code)

	var userResp map[string]any
	require.NoError(t, json.Unmarshal(createUserRec.Body.Bytes(), &userResp))
	assert.Equal(t, "alice", userResp["UserName"])

	// Describe user
	descUserRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	})
	assert.Equal(t, http.StatusOK, descUserRec.Code)

	// List users
	listUsersRec := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId": serverID,
	})
	assert.Equal(t, http.StatusOK, listUsersRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listUsersRec.Body.Bytes(), &listResp))
	users := listResp["Users"].([]any)
	assert.Len(t, users, 1)

	// Update user
	updateUserRec := doTransferRequest(t, h, "UpdateUser", map[string]any{
		"ServerId":      serverID,
		"UserName":      "alice",
		"HomeDirectory": "/home/alice",
	})
	assert.Equal(t, http.StatusOK, updateUserRec.Code)

	// Delete user
	deleteUserRec := doTransferRequest(t, h, "DeleteUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	})
	assert.Equal(t, http.StatusOK, deleteUserRec.Code)

	// List again - should be empty
	listUsersRec2 := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId": serverID,
	})
	assert.Equal(t, http.StatusOK, listUsersRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listUsersRec2.Body.Bytes(), &listResp2))
	assert.Empty(t, listResp2["Users"])
}

func TestHandler_CreateUser_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing server id",
			body:     map[string]any{"UserName": "alice"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing username",
			body:     map[string]any{"ServerId": serverID},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "CreateUser", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "UnknownOp", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
