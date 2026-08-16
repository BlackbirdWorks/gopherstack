package cognitoidp_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

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

	// AddUserPoolClientSecret creates an independently ClientSecretId-keyed
	// secret (real AWS's UserPoolClientType has no field for it, so
	// DescribeUserPoolClient's top-level ClientSecret is untouched -- the new
	// secret's value is returned once, on AddUserPoolClientSecret's own
	// response, and its metadata thereafter only via ListUserPoolClientSecrets).
	addRec := doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var addResp map[string]any
	require.NoError(t, json.Unmarshal(addRec.Body.Bytes(), &addResp))
	descriptor, ok := addResp["ClientSecretDescriptor"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, descriptor["ClientSecretId"])
	assert.NotEmpty(t, descriptor["ClientSecretValue"])

	descRec2 := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var descResp2 map[string]any
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	assert.Empty(t, descResp2["UserPoolClient"].(map[string]any)["ClientSecret"])

	listRec := doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	secrets, ok := listResp["ClientSecrets"].([]any)
	require.True(t, ok)
	require.Len(t, secrets, 1)
	assert.Equal(t, descriptor["ClientSecretId"], secrets[0].(map[string]any)["ClientSecretId"])
	assert.Empty(t, secrets[0].(map[string]any)["ClientSecretValue"], "list must never reveal the secret value")
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
