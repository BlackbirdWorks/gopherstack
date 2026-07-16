package cognitoidp_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserPoolClient_OAuthFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("oauth-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "oauth-client", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows:  []string{"code", "implicit"},
		AllowedOAuthScopes: []string{"openid", "profile"},
		ExplicitAuthFlows:  []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"code", "implicit"}, client.AllowedOAuthFlows)
	assert.ElementsMatch(t, []string{"openid", "profile"}, client.AllowedOAuthScopes)
	assert.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, client.ExplicitAuthFlows)

	got, err := b.DescribeUserPoolClient(pool.ID, client.ClientID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"code", "implicit"}, got.AllowedOAuthFlows)
}

func TestHandler_CreateUserPoolClient_WithOAuthFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "oauth-handler-pool")

	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":         poolID,
		"ClientName":         "oauth-client",
		"AllowedOAuthFlows":  []string{"code"},
		"AllowedOAuthScopes": []string{"openid", "email"},
		"ExplicitAuthFlows":  []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPoolClient struct {
			AllowedOAuthFlows  []string `json:"AllowedOAuthFlows,omitempty"`
			AllowedOAuthScopes []string `json:"AllowedOAuthScopes,omitempty"`
			ExplicitAuthFlows  []string `json:"ExplicitAuthFlows,omitempty"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"code"}, resp.UserPoolClient.AllowedOAuthFlows)
	assert.ElementsMatch(t, []string{"email", "openid"}, resp.UserPoolClient.AllowedOAuthScopes)
	assert.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, resp.UserPoolClient.ExplicitAuthFlows)
}

func TestHandler_DescribeUserPoolClient_IncludesOAuthFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "describe-client-pool")

	createRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":         poolID,
		"ClientName":         "full-client",
		"AllowedOAuthFlows":  []string{"code"},
		"AllowedOAuthScopes": []string{"openid"},
		"ExplicitAuthFlows":  []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId,omitempty"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   createResp.UserPoolClient.ClientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPoolClient struct {
			AllowedOAuthFlows  []string `json:"AllowedOAuthFlows,omitempty"`
			AllowedOAuthScopes []string `json:"AllowedOAuthScopes,omitempty"`
			ExplicitAuthFlows  []string `json:"ExplicitAuthFlows,omitempty"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"code"}, resp.UserPoolClient.AllowedOAuthFlows)
	assert.Equal(t, []string{"openid"}, resp.UserPoolClient.AllowedOAuthScopes)
	assert.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, resp.UserPoolClient.ExplicitAuthFlows)
}

func TestAppClient_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "client-crud-pool")

	// Create a named client.
	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "my-app-client",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		UserPoolClient struct {
			ClientID   string `json:"ClientId"`
			ClientName string `json:"ClientName"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Equal(t, "my-app-client", createResp.UserPoolClient.ClientName)
	clientID := createResp.UserPoolClient.ClientID
	assert.NotEmpty(t, clientID)

	// List clients.
	listRec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		UserPoolClients []struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClients"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	var found bool
	for _, c := range listResp.UserPoolClients {
		if c.ClientID == clientID {
			found = true
		}
	}
	assert.True(t, found, "created client not in list")

	// Delete.
	delRec := doCognitoRequest(t, h, "DeleteUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)
}

func TestInMemoryBackend_CreateUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		poolIDFunc func(b *cognitoidp.InMemoryBackend) string
		name       string
		clientName string
		wantErr    bool
	}{
		{
			name:       "success",
			clientName: "my-client",
			poolIDFunc: func(b *cognitoidp.InMemoryBackend) string {
				pool, err := b.CreateUserPool("test-pool")
				if err != nil {
					return ""
				}

				return pool.ID
			},
		},
		{
			name:       "pool_not_found",
			clientName: "my-client",
			poolIDFunc: func(_ *cognitoidp.InMemoryBackend) string {
				return "us-east-1_nonexistent"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID := tt.poolIDFunc(b)

			client, err := b.CreateUserPoolClient(poolID, tt.clientName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, client.ClientID)
			assert.Equal(t, tt.clientName, client.ClientName)
			assert.Equal(t, poolID, client.UserPoolID)
		})
	}
}

func TestInMemoryBackend_DescribeUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
		badPoolID bool
	}{
		{
			name: "success",
		},
		{
			name:      "not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
		},
		{
			name:      "wrong_pool",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
			badPoolID: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("test-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "test-client")
			require.NoError(t, err)

			clientID := client.ClientID
			poolID := pool.ID

			if tt.name == "not_found" {
				clientID = "nonexistent"
			}

			if tt.badPoolID {
				poolID = "us-east-1_wrong"
			}

			got, descErr := b.DescribeUserPoolClient(poolID, clientID)

			if tt.wantErr {
				require.Error(t, descErr)
				assert.ErrorIs(t, descErr, tt.errTarget)

				return
			}

			require.NoError(t, descErr)
			assert.Equal(t, client.ClientID, got.ClientID)
		})
	}
}

func TestInMemoryBackend_ListUserPoolClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		primaryNames     []string
		secondaryNames   []string
		wantNames        []string
		deletePrimaryIdx int
	}{
		{
			name:             "list_only_primary_pool_clients",
			primaryNames:     []string{"web", "ios"},
			secondaryNames:   []string{"android"},
			deletePrimaryIdx: -1,
			wantNames:        []string{"ios", "web"},
		},
		{
			name:             "deleted_client_removed_from_indexed_listing",
			primaryNames:     []string{"web", "ios"},
			secondaryNames:   []string{"android"},
			deletePrimaryIdx: 0,
			wantNames:        []string{"ios"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			primaryPool, err := b.CreateUserPool("primary-pool")
			require.NoError(t, err)

			secondaryPool, err := b.CreateUserPool("secondary-pool")
			require.NoError(t, err)

			primaryIDs := make([]string, 0, len(tt.primaryNames))
			for _, clientName := range tt.primaryNames {
				client, createErr := b.CreateUserPoolClient(primaryPool.ID, clientName)
				require.NoError(t, createErr)
				primaryIDs = append(primaryIDs, client.ClientID)
			}

			for _, clientName := range tt.secondaryNames {
				_, createErr := b.CreateUserPoolClient(secondaryPool.ID, clientName)
				require.NoError(t, createErr)
			}

			if tt.deletePrimaryIdx >= 0 {
				require.NoError(t, b.DeleteUserPoolClient(primaryPool.ID, primaryIDs[tt.deletePrimaryIdx]))
			}

			clients, listErr := b.ListUserPoolClients(primaryPool.ID)
			require.NoError(t, listErr)
			require.Len(t, clients, len(tt.wantNames))

			gotNames := make([]string, 0, len(clients))
			for _, client := range clients {
				assert.Equal(t, primaryPool.ID, client.UserPoolID)
				gotNames = append(gotNames, client.ClientName)
			}

			assert.Equal(t, tt.wantNames, gotNames)
		})
	}
}

func TestInMemoryBackend_UpdateUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (string, string)
		name      string
		newName   string
		wantErr   bool
	}{
		{
			name: "update_client_name",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				p, _ := b.CreateUserPool("pool")
				c, _ := b.CreateUserPoolClient(p.ID, "old-name")

				return p.ID, c.ClientID
			},
			newName: "new-name",
		},
		{
			name: "client_not_found",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				p, _ := b.CreateUserPool("pool")

				return p.ID, "nonexistent"
			},
			newName:   "x",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID, clientID := tt.setup(b)

			client, err := b.UpdateUserPoolClient(poolID, clientID, tt.newName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.newName, client.ClientName)
		})
	}
}

func TestClientSecrets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "secrets-pool")

	// List before adding — empty
	rec := doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Secrets []string `json:"Secrets,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Secrets)

	// Add a secret
	rec = doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after adding — one secret
	rec = doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Secrets, 1)

	// Delete the secret
	rec = doCognitoRequest(t, h, "DeleteUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — empty again
	rec = doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Secrets)
}

func TestBackend_UpdateUserPoolClientWithOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		errTarget  error
		poolName   string
		clientName string
		newName    string
		flows      []string
		scopes     []string
		wantErr    bool
	}{
		{
			name:       "update_name_and_scopes",
			poolName:   "pool-ucwo",
			clientName: "orig-client",
			newName:    "updated-client",
			flows:      []string{"implicit"},
			scopes:     []string{"openid", "email"},
		},
		{
			name:       "update_empty_name_preserves_existing",
			poolName:   "pool-ucwo2",
			clientName: "keep-name",
			newName:    "",
		},
		{
			name:      "pool_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolName == "" {
				// pool_not_found case — call with bogus IDs
				_, err := b.UpdateUserPoolClientWithOpts(
					"nonexistent-pool", "nonexistent-client", "", cognitoidp.UserPoolClientOptions{},
				)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool(tt.poolName)
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, tt.clientName)
			require.NoError(t, err)

			opts := cognitoidp.UserPoolClientOptions{
				AllowedOAuthFlows:  tt.flows,
				AllowedOAuthScopes: tt.scopes,
			}

			updated, err := b.UpdateUserPoolClientWithOpts(pool.ID, client.ClientID, tt.newName, opts)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, updated)

			if tt.newName != "" {
				assert.Equal(t, tt.newName, updated.ClientName)
			} else {
				assert.Equal(t, tt.clientName, updated.ClientName)
			}
		})
	}
}

func TestHandler_UpdateUserPoolClient_WithOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update_scopes", wantCode: http.StatusOK},
		{name: "client_not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupHandlerPoolAndClient(t, h, "upc-pool")

			if tt.name == "client_not_found" {
				rec := doCognitoRequest(t, h, "UpdateUserPoolClient", map[string]any{
					"UserPoolId":         poolID,
					"ClientId":           "nonexistent-client",
					"AllowedOAuthScopes": []string{"openid"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rec := doCognitoRequest(t, h, "UpdateUserPoolClient", map[string]any{
				"UserPoolId":         poolID,
				"ClientId":           clientID,
				"ClientName":         "renamed-client",
				"AllowedOAuthFlows":  []string{"implicit"},
				"AllowedOAuthScopes": []string{"openid", "email"},
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				UserPoolClient struct {
					ClientName         string   `json:"ClientName,omitempty"`
					AllowedOAuthScopes []string `json:"AllowedOAuthScopes,omitempty"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "renamed-client", resp.UserPoolClient.ClientName)
		})
	}
}

func TestHandler_CreateUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "my-client",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (poolID, clientID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string) {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var poolResp map[string]map[string]any
				_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
				poolID := poolResp["UserPool"]["Id"].(string)

				clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": "c",
				})
				var clientResp map[string]map[string]any
				_ = json.Unmarshal(clientRec.Body.Bytes(), &clientResp)

				return poolID, clientResp["UserPoolClient"]["ClientId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string), "nonexistent-client"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestCognitoIDP_DeleteUserPoolClient_CleansRefreshTokens(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("my-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "my-client")
	require.NoError(t, err)

	u, err := b.SignUp(client.ClientID, "bob", "Password456!", nil)
	require.NoError(t, err)

	require.NoError(t, b.ConfirmSignUp(client.ClientID, "bob", u.ConfirmCode))

	tokens, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "bob", "Password456!")
	require.NoError(t, err)
	require.NotNil(t, tokens.Tokens)
	require.NotEmpty(t, tokens.Tokens.RefreshToken)

	// Deleting the client should clean up the refresh token.
	require.NoError(t, b.DeleteUserPoolClient(pool.ID, client.ClientID))

	// Attempting to use the refresh token should fail now (token cleaned up).
	_, err = b.InitiateAuthRefreshToken(client.ClientID, tokens.Tokens.RefreshToken)
	require.Error(t, err, "refresh token should have been cleaned up on client deletion")
}

func TestHandler_DeleteUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (poolID, clientID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string) {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var poolResp map[string]map[string]any
				_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
				poolID := poolResp["UserPool"]["Id"].(string)

				clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": "c",
				})
				var clientResp map[string]map[string]any
				_ = json.Unmarshal(clientRec.Body.Bytes(), &clientResp)

				return poolID, clientResp["UserPoolClient"]["ClientId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "client_not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string), "nonexistent-client"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DeleteUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListUserPoolClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numClients int
		wantCode   int
	}{
		{
			name:       "empty",
			numClients: 0,
			wantCode:   http.StatusOK,
		},
		{
			name:       "with_clients",
			numClients: 2,
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			for i := range tt.numClients {
				rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": fmt.Sprintf("client-%d", i),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clients := resp["UserPoolClients"].([]any)
			assert.Len(t, clients, tt.numClients)
		})
	}
}

func TestHandler_AddUserPoolClientSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantCode     int
		poolExists   bool
		clientExists bool
	}{
		{
			name:         "success",
			poolExists:   true,
			clientExists: true,
			wantCode:     http.StatusOK,
			wantContains: "ClientSecret",
		},
		{
			name:       "pool_not_found",
			poolExists: false,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "client_not_found",
			poolExists: true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := "us-east-1_NOTEXIST"
			clientID := "nonexistent-client"

			if tt.poolExists {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sec-pool"})
				var poolResp map[string]any
				require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
				poolID = poolResp["UserPool"].(map[string]any)["Id"].(string)
			}

			if tt.clientExists {
				clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": "test-client",
				})
				var clientResp map[string]any
				require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
				clientID = clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)
			}

			rec := doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestSortedListUserPoolClients(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-clients-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, name := range []string{"web-client", "android-client", "ios-client"} {
		doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
			"UserPoolId": poolID,
			"ClientName": name,
		})
	}

	rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	clients := listResp["UserPoolClients"].([]any)
	require.Len(t, clients, 3)
	assert.Equal(t, "android-client", clients[0].(map[string]any)["ClientName"])
	assert.Equal(t, "ios-client", clients[1].(map[string]any)["ClientName"])
	assert.Equal(t, "web-client", clients[2].(map[string]any)["ClientName"])
}

func TestDescribeUserPoolClient_IncludesClientSecret(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "secret-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "sec-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	// No secret yet.
	descRec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Empty(t, descResp["UserPoolClient"].(map[string]any)["ClientSecret"])

	// Add secret.
	doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})

	// Secret now present.
	descRec2 := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var descResp2 map[string]any
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	assert.NotEmpty(t, descResp2["UserPoolClient"].(map[string]any)["ClientSecret"])
}

func TestListUserPoolClients_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "empty-clients-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	clients := listResp["UserPoolClients"].([]any)
	assert.Empty(t, clients)
}

// TestParityB_ClientLastModifiedDate verifies LastModifiedDate appears and updates.
func TestClientLastModifiedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupPoolAndClientNamed(t, h, "client-lmd-pool", "client-lmd-client")

	rec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	client := descResp["UserPoolClient"].(map[string]any)
	creationDate := client["CreationDate"].(float64)
	lastModBefore := client["LastModifiedDate"].(float64)
	assert.InDelta(t, creationDate, lastModBefore, 0, "LastModifiedDate should equal CreationDate initially")

	// Update the client.
	upRec := doCognitoRequest(t, h, "UpdateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"ClientName": "client-lmd-client-updated",
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	var upResp map[string]any
	require.NoError(t, json.Unmarshal(upRec.Body.Bytes(), &upResp))
	updatedClient := upResp["UserPoolClient"].(map[string]any)
	lastModAfter := updatedClient["LastModifiedDate"].(float64)
	assert.GreaterOrEqual(t, lastModAfter, creationDate, "LastModifiedDate must be >= CreationDate after update")
}

// TestParityB_ClientOAuthRedirectFields verifies CallbackURLs, LogoutURLs, AllowedOAuthFlowsUserPoolClient round-trip.
func TestClientOAuthRedirectFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupPoolAndClientNamed(t, h, "oauth-pool", "stub-client")

	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":                      poolID,
		"ClientName":                      "oauth-client",
		"CallbackURLs":                    []string{"https://app.example.com/callback"},
		"LogoutURLs":                      []string{"https://app.example.com/logout"},
		"AllowedOAuthFlows":               []string{"code"},
		"AllowedOAuthScopes":              []string{"openid", "email"},
		"AllowedOAuthFlowsUserPoolClient": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	client := resp["UserPoolClient"].(map[string]any)

	callbackURLs := client["CallbackURLs"].([]any)
	require.Len(t, callbackURLs, 1)
	assert.Equal(t, "https://app.example.com/callback", callbackURLs[0])

	logoutURLs := client["LogoutURLs"].([]any)
	require.Len(t, logoutURLs, 1)
	assert.Equal(t, "https://app.example.com/logout", logoutURLs[0])

	assert.Equal(t, true, client["AllowedOAuthFlowsUserPoolClient"])
}
