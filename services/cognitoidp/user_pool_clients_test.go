package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
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

// clientSecretsListResp mirrors ListUserPoolClientSecretsOutput's real shape
// (aws-sdk-go-v2/service/cognitoidentityprovider): a ClientSecrets list of
// descriptors, not the flat []string the pre-fix response fabricated.
type clientSecretsListResp struct {
	ClientSecrets []struct {
		ClientSecretID    string `json:"ClientSecretId"`
		ClientSecretValue string `json:"ClientSecretValue,omitempty"`
	} `json:"ClientSecrets,omitempty"`
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

	var listResp clientSecretsListResp
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.ClientSecrets)

	// Add a secret; its ClientSecretId is required to delete it later
	// (gopherstack-h910 -- DeleteUserPoolClientSecret used to ignore
	// ClientSecretId entirely, so this round trip couldn't be modeled).
	rec = doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var addResp struct {
		ClientSecretDescriptor struct {
			ClientSecretID    string `json:"ClientSecretId"`
			ClientSecretValue string `json:"ClientSecretValue"`
		} `json:"ClientSecretDescriptor"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &addResp))
	secretID := addResp.ClientSecretDescriptor.ClientSecretID
	require.NotEmpty(t, secretID)
	require.NotEmpty(t, addResp.ClientSecretDescriptor.ClientSecretValue)

	// List after adding — one secret, value never revealed by List
	rec = doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.ClientSecrets, 1)
	assert.Equal(t, secretID, listResp.ClientSecrets[0].ClientSecretID)
	assert.Empty(t, listResp.ClientSecrets[0].ClientSecretValue)

	// Deleting with the wrong ClientSecretId must not remove the real one.
	rec = doCognitoRequest(t, h, "DeleteUserPoolClientSecret", map[string]any{
		"UserPoolId":     poolID,
		"ClientId":       clientID,
		"ClientSecretId": "not-the-real-id",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// Delete the secret by its real ClientSecretId
	rec = doCognitoRequest(t, h, "DeleteUserPoolClientSecret", map[string]any{
		"UserPoolId":     poolID,
		"ClientId":       clientID,
		"ClientSecretId": secretID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — empty again
	rec = doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.ClientSecrets)
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

// TestListUserPoolClients_Pagination proves the op pages through every app
// client exactly once instead of returning them all on a single page with
// no cursor.
func TestListUserPoolClients_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// setupHandlerPoolAndClient already creates one default client.
	poolID, _ := setupHandlerPoolAndClient(t, h, "clients-pagination-pool")

	extra := []string{"client-a", "client-b", "client-c"}
	for _, n := range extra {
		rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
			"UserPoolId": poolID,
			"ClientName": n,
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)
	}

	const total = 4 // default client + extra

	type listOut struct {
		NextToken       string           `json:"NextToken,omitempty"`
		UserPoolClients []map[string]any `json:"UserPoolClients"`
	}

	rec1 := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{
		"UserPoolId": poolID,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code, "body: %s", rec1.Body)

	var page1 listOut
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.UserPoolClients, 2)
	require.NotEmpty(t, page1.NextToken, "first page must return a cursor when more clients remain")

	rec2 := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{
		"UserPoolId": poolID,
		"MaxResults": 2,
		"NextToken":  page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code, "body: %s", rec2.Body)

	var page2 listOut
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.UserPoolClients, 2)
	require.Empty(t, page2.NextToken)

	seen := map[string]bool{}
	for _, c := range page1.UserPoolClients {
		seen[c["ClientId"].(string)] = true
	}

	for _, c := range page2.UserPoolClients {
		id := c["ClientId"].(string)
		require.False(t, seen[id], "client %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, total)
}
