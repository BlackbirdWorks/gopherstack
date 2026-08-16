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

func TestHandler_AdminGetUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (poolID, username string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)
				poolID := resp["UserPool"]["Id"].(string)

				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          "getusertest",
					"TemporaryPassword": "Temp123!",
				})

				return poolID, "getusertest"
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string), "nobody"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminGetUser", map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
		wantHTTP  int
	}{
		{
			name:      "empty_pool",
			wantCount: 0,
			wantHTTP:  http.StatusOK,
		},
		{
			name:      "pool_with_user",
			wantCount: 1,
			wantHTTP:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "test-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "test-client",
			})
			var clientResp map[string]any
			require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
			clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

			if tt.wantCount > 0 {
				doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "testuser",
					"Password": "Password123!",
				})
			}

			rec := doCognitoRequest(t, h, "ListUsers", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantHTTP, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			users := resp["Users"].([]any)
			assert.Len(t, users, tt.wantCount)
		})
	}
}

func TestHandler_AdminDeleteUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
		wantHTTP int
	}{
		{
			name:     "delete_existing",
			username: "deleteuser",
			wantHTTP: http.StatusOK,
		},
		{
			name:     "delete_missing",
			username: "nonexistent",
			wantHTTP: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "test-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "test-client",
			})
			var clientResp map[string]any
			require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
			clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

			// Create the user to delete in the first case.
			doCognitoRequest(t, h, "SignUp", map[string]any{
				"ClientId": clientID,
				"Username": "deleteuser",
				"Password": "Password123!",
			})

			rec := doCognitoRequest(t, h, "AdminDeleteUser", map[string]any{
				"UserPoolId": poolID,
				"Username":   tt.username,
			})
			assert.Equal(t, tt.wantHTTP, rec.Code)
		})
	}
}

func TestHandler_AdminDisableEnableUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		setupUser bool
		wantCode  int
	}{
		{
			name:      "disable_success",
			operation: "AdminDisableUser",
			setupUser: true,
			wantCode:  http.StatusOK,
		},
		{
			name:      "enable_success",
			operation: "AdminEnableUser",
			setupUser: true,
			wantCode:  http.StatusOK,
		},
		{
			name:      "disable_user_not_found",
			operation: "AdminDisableUser",
			setupUser: false,
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "enable_user_not_found",
			operation: "AdminEnableUser",
			setupUser: false,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "dis-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			username := "dis-user"
			if tt.setupUser {
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "TempPass123!",
				})
			}

			rec := doCognitoRequest(t, h, tt.operation, map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AdminDisableUser_BlocksAuth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "block-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "block-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "blockuser",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "blockuser",
		"Password":   "FinalPass123!",
		"Permanent":  true,
	})

	// Disable the user.
	disableRec := doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "blockuser",
	})
	assert.Equal(t, http.StatusOK, disableRec.Code)

	// Attempt to authenticate with disabled user must fail.
	authRec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]any{
			"USERNAME": "blockuser",
			"PASSWORD": "FinalPass123!",
		},
	})
	assert.Equal(t, http.StatusBadRequest, authRec.Code)

	// Re-enable the user.
	enableRec := doCognitoRequest(t, h, "AdminEnableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "blockuser",
	})
	assert.Equal(t, http.StatusOK, enableRec.Code)

	// Authentication must succeed again.
	authRec2 := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]any{
			"USERNAME": "blockuser",
			"PASSWORD": "FinalPass123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec2.Code)
}

func TestHandler_ListUsers_ReturnsCorrectEnabledState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		disableUser bool
		wantEnabled bool
	}{
		{
			name:        "user_initially_enabled",
			disableUser: false,
			wantEnabled: true,
		},
		{
			name:        "user_disabled_shows_false",
			disableUser: true,
			wantEnabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "enabled-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			const username = "state-user"
			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          username,
				"TemporaryPassword": "TempPass123!",
			})

			if tt.disableUser {
				doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   username,
				})
			}

			listRec := doCognitoRequest(t, h, "ListUsers", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			users := listResp["Users"].([]any)
			require.Len(t, users, 1)
			assert.Equal(t, tt.wantEnabled, users[0].(map[string]any)["Enabled"].(bool))
		})
	}
}

func TestSortedListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-users-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, username := range []string{"zeus", "alice", "bob"} {
		doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
			"UserPoolId":        poolID,
			"Username":          username,
			"TemporaryPassword": "TempPass123!",
		})
	}

	rec := doCognitoRequest(t, h, "ListUsers", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	users := listResp["Users"].([]any)
	require.Len(t, users, 3)
	assert.Equal(t, "alice", users[0].(map[string]any)["Username"])
	assert.Equal(t, "bob", users[1].(map[string]any)["Username"])
	assert.Equal(t, "zeus", users[2].(map[string]any)["Username"])
}

func TestAdminGetUser_IncludesEnabledAndModifiedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "agu-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "agu-user",
		"TemporaryPassword": "TempPass123!",
	})

	rec := doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "agu-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp["Enabled"].(bool))
	assert.NotZero(t, resp["UserCreateDate"])
	assert.NotZero(t, resp["UserLastModifiedDate"])
}

func TestAdminCreateUser_IncludesEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "acu-enabled-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "new-user",
		"TemporaryPassword": "TempPass123!",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp["User"].(map[string]any)["Enabled"].(bool))
}

func TestRefreshToken_DisabledUserBlocked(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "rt-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "rt-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "rt-user",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "rt-user",
		"Password":   "FinalPass123!",
		"Permanent":  true,
	})

	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"USERNAME": "rt-user",
			"PASSWORD": "FinalPass123!",
		},
	})
	require.Equal(t, http.StatusOK, authRec.Code)

	var authResp map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	refreshToken := authResp["AuthenticationResult"].(map[string]any)["RefreshToken"].(string)

	// Disable the user.
	doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "rt-user",
	})

	// Refresh token must now be rejected.
	refreshRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	assert.Equal(t, http.StatusBadRequest, refreshRec.Code)
}

// TestAdminSetUserPassword_PolicyEnforced verifies that the (non-"Full")
// AdminSetUserPassword backend entry point — the one used by the JSON handler —
// rejects a password that violates the pool's password policy, matching
// ConfirmForgotPassword and AWS's InvalidPasswordException behavior.
func TestListUsers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "users-page-pool"})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp struct {
		UserPool struct {
			ID string `json:"Id"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp.UserPool.ID

	for i := range 5 {
		rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
			"UserPoolId": poolID,
			"Username":   fmt.Sprintf("user-%02d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	type listResp struct {
		PaginationToken string           `json:"PaginationToken"`
		Users           []map[string]any `json:"Users"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := map[string]any{"UserPoolId": poolID, "Limit": 2}
		if token != "" {
			body["PaginationToken"] = token
		}

		rec := doCognitoRequest(t, h, "ListUsers", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.Users), 2)

		for _, u := range resp.Users {
			name := u["Username"].(string)
			assert.False(t, seen[name], "user %s returned twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		token = resp.PaginationToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, 5)
}

// TestParity_GetUser_RejectsIDToken verifies that access-token operations reject
// an ID token presented in place of an access token (token_use enforcement).
func TestGetUser_RejectsIDToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		useID     bool
		wantErr   bool
	}{
		{name: "access_token_accepted", useID: false, wantErr: false},
		{name: "id_token_rejected", useID: true, wantErr: true, errTarget: cognitoidp.ErrNotAuthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, client := setupTestPoolAndClient(t)
			tokens := signUpConfirmAndLogin(t, b, client.ClientID, "tokuser")

			tok := tokens.AccessToken
			if tt.useID {
				tok = tokens.IDToken
			}

			_, err := b.GetUser(tok)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}
