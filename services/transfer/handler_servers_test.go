package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// mustCreateServer creates a server and returns its ServerId.
func mustCreateServer(t *testing.T, h *transfer.Handler) string {
	t.Helper()

	rec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.ServerID
}

// TestHandler_DeleteServerCascade verifies DeleteServer cleans up in HTTP path.
func TestHandler_DeleteServerCascade(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
		h := transfer.NewHandler(b)

		createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		serverID := createResp["ServerId"].(string)

		// Create access and agreement on the server
		doTransferRequest(t, h, "CreateAccess", map[string]any{
			"ServerId":   serverID,
			"ExternalId": "S-1-5-21-9999",
		})
		doTransferRequest(t, h, "CreateAgreement", map[string]any{
			"ServerId":         serverID,
			"LocalProfileId":   "p-local",
			"PartnerProfileId": "p-partner",
			"BaseDirectory":    "/base",
			"AccessRole":       "arn:role",
		})

		assert.Equal(t, 1, transfer.AccessCount(b))
		assert.Equal(t, 1, transfer.AgreementCount(b))

		// AWS requires server to be OFFLINE before deletion.
		stopRec := doTransferRequest(t, h, "StopServer", map[string]any{"ServerId": serverID})
		require.Equal(t, http.StatusOK, stopRec.Code)
		time.Sleep(serverTransitionWait)

		descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
		var resp map[string]any
		_ = json.Unmarshal(descRec.Body.Bytes(), &resp)
		require.Equal(t, "OFFLINE", resp["Server"].(map[string]any)["State"].(string))

		deleteRec := doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
		require.Equal(t, http.StatusOK, deleteRec.Code)

		assert.Equal(t, 0, transfer.ServerCount(b))
		assert.Equal(t, 0, transfer.AccessCount(b))
		assert.Equal(t, 0, transfer.AgreementCount(b))
	})
}

// TestHandler_DeleteServerOnlineReturnsConflict verifies that DeleteServer
// returns a ConflictException (400) when the server is ONLINE.
func TestHandler_DeleteServerOnlineReturnsConflict(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newTestHandler(t)
		rec := doTransferRequest(t, h, "CreateServer", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
		serverID := createResp["ServerId"].(string)

		// Servers are created OFFLINE; start it so it is ONLINE before delete is attempted.
		startRec := doTransferRequest(t, h, "StartServer", map[string]any{"ServerId": serverID})
		require.Equal(t, http.StatusOK, startRec.Code)
		time.Sleep(serverTransitionWait)

		descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
		var resp map[string]any
		_ = json.Unmarshal(descRec.Body.Bytes(), &resp)
		require.Equal(t, "ONLINE", resp["Server"].(map[string]any)["State"].(string))

		// Server is ONLINE; delete should fail.
		delRec := doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
		assert.Equal(t, http.StatusBadRequest, delRec.Code)

		var errResp map[string]any
		require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &errResp))
		// The handler maps ErrConflict to ResourceExistsException.
		assert.Contains(t, errResp["__type"], "ResourceExistsException")
	})
}

// TestHandler_ListServersIncludesIdentityProviderType verifies ListServers
// returns IdentityProviderType, EndpointType, Domain, and UserCount per server.
func TestHandler_ListServersIncludesIdentityProviderType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a server with a specific IdentityProviderType.
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"IdentityProviderType": "SERVICE_MANAGED",
		"EndpointType":         "PUBLIC",
		"Domain":               "S3",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	// Add a user to check UserCount.
	_, err := h.Backend.CreateUser(serverID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	listRec := doTransferRequest(t, h, "ListServers", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	servers := listResp["Servers"].([]any)
	require.Len(t, servers, 1)

	item := servers[0].(map[string]any)
	assert.Equal(t, serverID, item["ServerId"])
	assert.Equal(t, "SERVICE_MANAGED", item["IdentityProviderType"])
	assert.Equal(t, "PUBLIC", item["EndpointType"])
	assert.Equal(t, "S3", item["Domain"])
	assert.EqualValues(t, 1, item["UserCount"])
}

// TestHandler_DescribeServerUserCount verifies UserCount is populated.
func TestHandler_DescribeServerUserCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// No users yet.
	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": s.ServerID})
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	assert.EqualValues(t, 0, server["UserCount"])

	// Add two users.
	_, err = h.Backend.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "bob", "/bob", "", nil)
	require.NoError(t, err)

	descRec = doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": s.ServerID})
	require.Equal(t, http.StatusOK, descRec.Code)
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server = descResp["Server"].(map[string]any)
	assert.EqualValues(t, 2, server["UserCount"])
}

func TestHandler_TestIdentityProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "TestIdentityProvider", map[string]any{
		"ServerId":     s.ServerID,
		"UserName":     "testuser",
		"UserPassword": "password",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
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

	synctest.Test(t, func(t *testing.T) {
		h := newTestHandler(t)

		createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		serverID := createResp["ServerId"].(string)

		// AWS requires server to be OFFLINE before deletion; stop it first.
		stopRec := doTransferRequest(t, h, "StopServer", map[string]any{"ServerId": serverID})
		require.Equal(t, http.StatusOK, stopRec.Code)
		time.Sleep(serverTransitionWait)

		descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
		var resp map[string]any
		_ = json.Unmarshal(descRec.Body.Bytes(), &resp)
		require.Equal(t, "OFFLINE", resp["Server"].(map[string]any)["State"].(string))

		rec := doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
		assert.Equal(t, http.StatusOK, rec.Code)

		// Second delete should fail
		rec = doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
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

func TestHandler_ListServers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 servers
	for range 3 {
		body := map[string]any{"Domain": "S3", "EndpointType": "PUBLIC", "IdentityProviderType": "SERVICE_MANAGED"}
		doTransferRequest(t, h, "CreateServer", body)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantCode      int
		wantMinCount  int
		wantNextToken bool
	}{
		{
			name:         "list all servers",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantMinCount: 3,
		},
		{
			name:          "list with maxResults=1",
			body:          map[string]any{"MaxResults": 1},
			wantCode:      http.StatusOK,
			wantMinCount:  1,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "ListServers", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			servers := resp["Servers"].([]any)
			assert.GreaterOrEqual(t, len(servers), tt.wantMinCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["NextToken"])
			}
		})
	}
}

func TestHandler_StartStopDeleteServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "stop server",
			action:   "StopServer",
			body:     map[string]any{"ServerId": "PLACEHOLDER"},
			wantCode: http.StatusOK,
		},
		{
			name:     "start server",
			action:   "StartServer",
			body:     map[string]any{"ServerId": "PLACEHOLDER"},
			wantCode: http.StatusOK,
		},
		{
			name:     "start server missing id",
			action:   "StartServer",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete server",
			action:   "DeleteServer",
			body:     map[string]any{"ServerId": "PLACEHOLDER"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newTestHandler(t)
				createRec := doTransferRequest(t, h, "CreateServer", map[string]any{
					"Domain":               "S3",
					"EndpointType":         "PUBLIC",
					"IdentityProviderType": "SERVICE_MANAGED",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				serverID := createResp["ServerId"].(string)

				body := tt.body
				if id, ok := body["ServerId"]; ok && id == "PLACEHOLDER" {
					body = map[string]any{"ServerId": serverID}
				}

				// DeleteServer requires the server to be OFFLINE first.
				if tt.action == "DeleteServer" {
					stopRec := doTransferRequest(t, h, "StopServer", map[string]any{"ServerId": serverID})
					require.Equal(t, http.StatusOK, stopRec.Code)
					time.Sleep(serverTransitionWait)

					descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": serverID})
					var resp map[string]any
					_ = json.Unmarshal(descRec.Body.Bytes(), &resp)
					require.Equal(t, "OFFLINE", resp["Server"].(map[string]any)["State"].(string))
				}

				rec := doTransferRequest(t, h, tt.action, body)
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		})
	}
}
