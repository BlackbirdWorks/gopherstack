package cognitoidp_test

// coverage_gaps_test.go covers previously-untested backend and handler operations
// to bring total statement coverage above 85%.

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// ---- UpdateUserPoolClientWithOpts ----

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

// ---- AdminSetUserMFASetting ----

func TestBackend_AdminSetUserMFASetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		errTarget       error
		preferredMFA    string
		softwareEnabled bool
		smsEnabled      bool
		wantErr         bool
		poolNotFound    bool
		userNotFound    bool
	}{
		{
			name:            "enable_software_token",
			softwareEnabled: true,
			preferredMFA:    "SOFTWARE_TOKEN_MFA",
		},
		{
			name:         "enable_sms",
			smsEnabled:   true,
			preferredMFA: "SMS_MFA",
		},
		{
			name: "disable_all",
		},
		{
			name:         "pool_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrUserPoolNotFound,
			poolNotFound: true,
		},
		{
			name:         "user_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrUserNotFound,
			userNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("mfa-pool")
			require.NoError(t, err)

			if tt.poolNotFound {
				err = b.AdminSetUserMFASetting("bad-pool", "user", false, false, "")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			if tt.userNotFound {
				err = b.AdminSetUserMFASetting(pool.ID, "no-user", false, false, "")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.AdminCreateUser(pool.ID, "mfa-user", "TempPass1!", map[string]string{})
			require.NoError(t, err)
			require.NoError(t, b.AdminSetUserPassword(pool.ID, "mfa-user", "PermPass1!", true))

			err = b.AdminSetUserMFASetting(pool.ID, "mfa-user", tt.smsEnabled, tt.softwareEnabled, tt.preferredMFA)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- UpdateResourceServer ----

func TestBackend_UpdateResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		wantErr     bool
		poolMissing bool
		rsMissing   bool
	}{
		{
			name: "update_name_and_scopes",
		},
		{
			name:        "pool_not_found",
			wantErr:     true,
			errTarget:   cognitoidp.ErrUserPoolNotFound,
			poolMissing: true,
		},
		{
			name:      "resource_server_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
			rsMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolMissing {
				_, err := b.UpdateResourceServer("bad-pool", "https://api.example.com", "new-name", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool("rs-update-pool")
			require.NoError(t, err)

			if tt.rsMissing {
				_, err = b.UpdateResourceServer(pool.ID, "https://nonexistent.example.com", "x", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.CreateResourceServer(
				pool.ID,
				"https://api.example.com",
				"My API",
				[]cognitoidp.ResourceServerScope{
					{ScopeName: "read", ScopeDescription: "Read access"},
				},
			)
			require.NoError(t, err)

			updated, err := b.UpdateResourceServer(
				pool.ID,
				"https://api.example.com",
				"Updated API",
				[]cognitoidp.ResourceServerScope{{ScopeName: "write", ScopeDescription: "Write access"}},
			)

			require.NoError(t, err)
			require.NotNil(t, updated)
			assert.Equal(t, "Updated API", updated.Name)
			require.Len(t, updated.Scopes, 1)
			assert.Equal(t, "write", updated.Scopes[0].ScopeName)
		})
	}
}

// ---- DeleteResourceServer ----

func TestBackend_DeleteResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		wantErr     bool
		poolMissing bool
		rsMissing   bool
	}{
		{name: "success"},
		{
			name:        "pool_not_found",
			wantErr:     true,
			errTarget:   cognitoidp.ErrUserPoolNotFound,
			poolMissing: true,
		},
		{
			name:      "resource_server_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
			rsMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolMissing {
				err := b.DeleteResourceServer("bad-pool", "https://api.example.com")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool("rs-del-pool")
			require.NoError(t, err)

			if tt.rsMissing {
				err = b.DeleteResourceServer(pool.ID, "https://nonexistent.example.com")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.CreateResourceServer(pool.ID, "https://del.example.com", "Del API", nil)
			require.NoError(t, err)

			err = b.DeleteResourceServer(pool.ID, "https://del.example.com")
			require.NoError(t, err)

			// Second delete must fail.
			err = b.DeleteResourceServer(pool.ID, "https://del.example.com")
			require.Error(t, err)
		})
	}
}

// ---- AdminUserGlobalSignOut ----

func TestBackend_AdminUserGlobalSignOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget    error
		name         string
		wantErr      bool
		poolNotFound bool
		userNotFound bool
	}{
		{name: "success"},
		{
			name:         "pool_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrUserPoolNotFound,
			poolNotFound: true,
		},
		{
			name:         "user_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrUserNotFound,
			userNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("signout-pool")
			require.NoError(t, err)

			if tt.poolNotFound {
				err = b.AdminUserGlobalSignOut("bad-pool", "any-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			if tt.userNotFound {
				err = b.AdminUserGlobalSignOut(pool.ID, "no-such-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.AdminCreateUser(pool.ID, "signout-user", "TempPass1!", map[string]string{})
			require.NoError(t, err)
			require.NoError(t, b.AdminSetUserPassword(pool.ID, "signout-user", "PermPass1!", true))

			client, err := b.CreateUserPoolClient(pool.ID, "sc")
			require.NoError(t, err)

			result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "signout-user", "PermPass1!")
			require.NoError(t, err)
			require.NotNil(t, result.Tokens)

			// Before sign-out refresh token count > 0.
			assert.Positive(t, b.RefreshTokenCount())

			err = b.AdminUserGlobalSignOut(pool.ID, "signout-user")
			require.NoError(t, err)

			// After sign-out refresh tokens are gone.
			assert.Equal(t, 0, b.RefreshTokenCount())
		})
	}
}

// ---- ResendConfirmationCode ----

func TestBackend_ResendConfirmationCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget        error
		name             string
		wantErr          bool
		clientBad        bool
		poolBad          bool
		userBad          bool
		alreadyConfirmed bool
	}{
		{name: "success"},
		{
			name:      "client_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
			clientBad: true,
		},
		{
			name:      "user_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserNotFound,
			userBad:   true,
		},
		{
			name:             "already_confirmed",
			wantErr:          true,
			errTarget:        cognitoidp.ErrCodeMismatch,
			alreadyConfirmed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("resend-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "resend-client")
			require.NoError(t, err)

			if tt.clientBad {
				_, err = b.ResendConfirmationCode("bad-client-id", "user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			if tt.userBad {
				_, err = b.ResendConfirmationCode(client.ClientID, "no-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			user, err := b.SignUp(client.ClientID, "resend-user", "Pass1234!", map[string]string{})
			require.NoError(t, err)

			if tt.alreadyConfirmed {
				require.NoError(t, b.ConfirmSignUp(client.ClientID, "resend-user", user.ConfirmCode))

				_, err = b.ResendConfirmationCode(client.ClientID, "resend-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			newCode, err := b.ResendConfirmationCode(client.ClientID, "resend-user")
			require.NoError(t, err)
			assert.Len(t, newCode, 6)

			// New code must work to confirm.
			err = b.ConfirmSignUp(client.ClientID, "resend-user", newCode)
			require.NoError(t, err)
		})
	}
}

// ---- UpdateGroup ----

func TestBackend_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget    error
		name         string
		wantErr      bool
		poolMissing  bool
		groupMissing bool
	}{
		{name: "success"},
		{
			name:        "pool_not_found",
			wantErr:     true,
			errTarget:   cognitoidp.ErrUserPoolNotFound,
			poolMissing: true,
		},
		{
			name:         "group_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrGroupNotFound,
			groupMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolMissing {
				_, err := b.UpdateGroup("bad-pool", "grp", "desc", 0)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool("grp-pool")
			require.NoError(t, err)

			if tt.groupMissing {
				_, err = b.UpdateGroup(pool.ID, "no-grp", "desc", 0)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.CreateGroup(pool.ID, "my-group", "original desc", 1)
			require.NoError(t, err)

			updated, err := b.UpdateGroup(pool.ID, "my-group", "new desc", 5)
			require.NoError(t, err)
			require.NotNil(t, updated)
			assert.Equal(t, "new desc", updated.Description)
			assert.Equal(t, int32(5), updated.Precedence)
		})
	}
}

// ---- Handler: UpdateUserPool ----

func TestHandler_UpdateUserPool_WithOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		setup    func(t *testing.T, h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name:     "update_mfa_config",
			wantCode: http.StatusOK,
			setup: func(t *testing.T, h *cognitoidp.Handler) string {
				t.Helper()
				id, _ := setupHandlerPoolAndClient(t, h, "uu-pool")

				return id
			},
			body: map[string]any{
				"MfaConfiguration": "OPTIONAL",
			},
		},
		{
			name:     "update_password_policy",
			wantCode: http.StatusOK,
			setup: func(t *testing.T, h *cognitoidp.Handler) string {
				t.Helper()
				id, _ := setupHandlerPoolAndClient(t, h, "uu-pool2")

				return id
			},
			body: map[string]any{
				"Policies": map[string]any{
					"PasswordPolicy": map[string]any{
						"MinimumLength":    10,
						"RequireUppercase": true,
						"RequireLowercase": true,
						"RequireNumbers":   true,
						"RequireSymbols":   false,
					},
				},
			},
		},
		{
			name:     "pool_not_found",
			wantCode: http.StatusBadRequest,
			setup:    func(_ *testing.T, _ *cognitoidp.Handler) string { return "invalid-pool" },
			body:     map[string]any{"MfaConfiguration": "OFF"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(t, h)

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)

			body["UserPoolId"] = poolID

			rec := doCognitoRequest(t, h, "UpdateUserPool", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Handler: UpdateUserPoolClient (with opts) ----

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

// ---- Handler: AdminSetUserMFASetting ----

func TestHandler_AdminSetUserMFASetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "enable_software_token",
			wantCode: http.StatusOK,
			body: map[string]any{
				"SoftwareTokenMfaSettings": map[string]any{
					"Enabled":      true,
					"PreferredMfa": true,
				},
			},
		},
		{
			name:     "user_not_found",
			wantCode: http.StatusBadRequest,
			body: map[string]any{
				"Username": "no-such-user",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "admin-mfa-pool")

			username := "mfa-target"
			if u, ok := tt.body["Username"]; ok {
				username = u.(string)
			} else {
				// Create the user.
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "TempPass1!",
				})
				doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
					"UserPoolId": poolID,
					"Username":   username,
					"Password":   "PermPass1!",
					"Permanent":  true,
				})
			}

			body := make(map[string]any, len(tt.body)+2)
			maps.Copy(body, tt.body)

			body["UserPoolId"] = poolID
			body["Username"] = username

			rec := doCognitoRequest(t, h, "AdminSetUserMFASetting", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Handler: ListResourceServers ----

func TestHandler_ListResourceServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		count    int
	}{
		{name: "empty_list", wantCode: http.StatusOK, count: 0},
		{name: "one_server", wantCode: http.StatusOK, count: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "list-rs-pool")

			for i := range tt.count {
				doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
					"UserPoolId": poolID,
					"Identifier": "https://api" + string(rune('0'+i)) + ".example.com",
					"Name":       "API " + string(rune('0'+i)),
				})
			}

			rec := doCognitoRequest(t, h, "ListResourceServers", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp struct {
				ResourceServers []map[string]any `json:"ResourceServers,omitempty"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceServers, tt.count)
		})
	}
}

// ---- Handler: UpdateResourceServer ----

func TestHandler_UpdateResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		badPool  bool
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "pool_not_found", wantCode: http.StatusBadRequest, badPool: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "upd-rs-pool")

			identifier := "https://update.example.com"

			doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
				"UserPoolId": poolID,
				"Identifier": identifier,
				"Name":       "Original",
			})

			reqPoolID := poolID
			if tt.badPool {
				reqPoolID = "bad-pool-id"
			}

			rec := doCognitoRequest(t, h, "UpdateResourceServer", map[string]any{
				"UserPoolId": reqPoolID,
				"Identifier": identifier,
				"Name":       "Updated",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Handler: DeleteResourceServer ----

func TestHandler_DeleteResourceServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		badPool  bool
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", wantCode: http.StatusBadRequest, badPool: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "del-rs-pool")

			identifier := "https://delete.example.com"

			doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
				"UserPoolId": poolID,
				"Identifier": identifier,
				"Name":       "ToDelete",
			})

			reqPoolID := poolID
			if tt.badPool {
				reqPoolID = "bad-pool"
			}

			rec := doCognitoRequest(t, h, "DeleteResourceServer", map[string]any{
				"UserPoolId": reqPoolID,
				"Identifier": identifier,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Handler: AdminRespondToAuthChallenge ----

func TestHandler_AdminRespondToAuthChallenge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		challengeName string
		wantCode      int
		badSession    bool
	}{
		{
			name:          "new_password_required",
			wantCode:      http.StatusOK,
			challengeName: "NEW_PASSWORD_REQUIRED",
		},
		{
			name:          "bad_session",
			wantCode:      http.StatusBadRequest,
			challengeName: "NEW_PASSWORD_REQUIRED",
			badSession:    true,
		},
		{
			name:          "unknown_challenge_no_error",
			wantCode:      http.StatusOK,
			challengeName: "UNKNOWN_CHALLENGE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupHandlerPoolAndClient(t, h, "admin-challenge-pool")

			// Create a user requiring password change.
			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          "challenge-user",
				"TemporaryPassword": "TempPass1!",
			})

			session := "bad-session-value"

			if !tt.badSession && tt.challengeName == "NEW_PASSWORD_REQUIRED" {
				// Trigger the NEW_PASSWORD_REQUIRED challenge.
				authRec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
					"UserPoolId": poolID,
					"ClientId":   clientID,
					"AuthFlow":   "USER_PASSWORD_AUTH",
					"AuthParameters": map[string]string{
						"USERNAME": "challenge-user",
						"PASSWORD": "TempPass1!",
					},
				})

				var authResp struct {
					Session string `json:"Session,omitempty"`
				}
				require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
				session = authResp.Session
			}

			body := map[string]any{
				"ClientId":      clientID,
				"ChallengeName": tt.challengeName,
				"Session":       session,
				"ChallengeResponses": map[string]string{
					"NEW_PASSWORD": "NewPass1!",
					"USERNAME":     "challenge-user",
				},
			}

			rec := doCognitoRequest(t, h, "AdminRespondToAuthChallenge", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Persistence: new fields survive Snapshot/Restore ----

func TestPersistence_NewFieldsSurviveSnapshot(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateUserPool("persist-pool")
	require.NoError(t, err)

	// Set password policy (MFA not enabled so auth still issues tokens directly).
	require.NoError(t, b.UpdateUserPoolWithOpts(pool.ID, "", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{MinimumLength: 12, RequireUppercase: true},
	}))

	// Create a resource server.
	_, err = b.CreateResourceServer(pool.ID, "https://persist.example.com", "Persist API", nil)
	require.NoError(t, err)

	// Create user + sign out to populate tokenRevokedBefore.
	client, err := b.CreateUserPoolClient(pool.ID, "pc")
	require.NoError(t, err)

	// Use SignUp (no policy validation) so any password works.
	user, err := b.SignUp(client.ClientID, "persist-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "persist-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "persist-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNilf(t, result.Tokens, "expected tokens but got challenge: %s", result.ChallengeName)

	require.NoError(t, b.GlobalSignOut(result.Tokens.AccessToken))

	// Take snapshot.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into fresh backend.
	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify resource server survived.
	rs, err := b2.DescribeResourceServer(pool.ID, "https://persist.example.com")
	require.NoError(t, err)
	assert.Equal(t, "Persist API", rs.Name)

	// Verify password policy survived.
	restoredPool, err := b2.DescribeUserPool(pool.ID)
	require.NoError(t, err)
	require.NotNil(t, restoredPool.PasswordPolicy)
	assert.Equal(t, 12, restoredPool.PasswordPolicy.MinimumLength)
	assert.True(t, restoredPool.PasswordPolicy.RequireUppercase)
}

// ---- ConfirmCodeExpiresAt set on SignUp, ForgotPassword, ResendConfirmationCode ----

func TestBackend_ConfirmCodeExpirySet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "signup_sets_expiry", op: "signup"},
		{name: "forgot_password_sets_expiry", op: "forgot"},
		{name: "resend_sets_expiry", op: "resend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("expiry-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "expiry-client")
			require.NoError(t, err)

			user, err := b.SignUp(client.ClientID, "expiry-user", "Pass1234!", map[string]string{})
			require.NoError(t, err)

			// ConfirmCodeExpiresAt must be non-zero after SignUp.
			assert.False(t, user.ConfirmCodeExpiresAt.IsZero(), "ConfirmCodeExpiresAt must be set after SignUp")

			if tt.op == "signup" {
				return
			}

			// Confirm the user.
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "expiry-user", user.ConfirmCode))

			if tt.op == "forgot" {
				code, forgotErr := b.ForgotPassword(client.ClientID, "expiry-user")
				require.NoError(t, forgotErr)
				require.NotEmpty(t, code)

				// Now expire the code artificially and confirm the expiry check fires.
				b.ExpireConfirmCodeForTest(pool.ID, "expiry-user")

				confirmErr := b.ConfirmForgotPassword(client.ClientID, "expiry-user", code, "NewPass1234!")
				require.Error(t, confirmErr)
				assert.ErrorIs(t, confirmErr, cognitoidp.ErrExpiredCode)

				return
			}

			// resend case: re-sign up a fresh user since we confirmed above.
			user2, err := b.SignUp(client.ClientID, "expiry-user2", "Pass1234!", map[string]string{})
			require.NoError(t, err)
			assert.False(t, user2.ConfirmCodeExpiresAt.IsZero())

			newCode, err := b.ResendConfirmationCode(client.ClientID, "expiry-user2")
			require.NoError(t, err)
			require.NotEmpty(t, newCode)

			// Expire the code and check that confirmation fails.
			b.ExpireConfirmCodeForTest(pool.ID, "expiry-user2")

			err = b.ConfirmSignUp(client.ClientID, "expiry-user2", newCode)
			require.Error(t, err)
			assert.ErrorIs(t, err, cognitoidp.ErrExpiredCode)
		})
	}
}

// ---- Scope deduplication in access token ----

func TestAccuracy_AccessToken_ScopesSortedAndDeduped(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("scope-pool")
	require.NoError(t, err)

	opts := cognitoidp.UserPoolClientOptions{
		// Intentionally unsorted and with duplicate.
		AllowedOAuthScopes: []string{"openid", "aws.cognito.signin.user.admin", "email", "openid"},
	}
	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "scope-client", opts)
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "scope-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "scope-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "scope-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	claims := decodeJWTPayload(t, result.Tokens.AccessToken)
	scopeVal, ok := claims["scope"].(string)
	require.True(t, ok, "scope claim must be a string")

	scopes := splitScope(scopeVal)

	// No duplicates.
	seen := make(map[string]struct{})
	for _, s := range scopes {
		_, dup := seen[s]
		assert.False(t, dup, "duplicate scope %q", s)
		seen[s] = struct{}{}
	}

	// Sorted.
	for i := 1; i < len(scopes); i++ {
		assert.LessOrEqual(t, scopes[i-1], scopes[i], "scopes must be sorted")
	}
}

func splitScope(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Fields(s)
}

// ---- Persistence: refresh tokens survive Snapshot/Restore ----

func TestPersistence_RefreshTokensSurviveSnapshot(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("rt-persist-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "rpc")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "rt-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "rt-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "rt-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	// Snapshot with active refresh token.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Refresh token count must survive.
	assert.Equal(t, b.RefreshTokenCount(), b2.RefreshTokenCount())
	assert.Equal(t, 1, b2.RefreshTokenCount())
}

func TestHandler_RespondToAuthChallenge_NewPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		challengeName string
		wantCode      int
		badSession    bool
	}{
		{
			name:          "new_password_required",
			wantCode:      http.StatusOK,
			challengeName: "NEW_PASSWORD_REQUIRED",
		},
		{
			name:          "default_challenge",
			wantCode:      http.StatusOK,
			challengeName: "CUSTOM_CHALLENGE",
		},
		{
			name:          "bad_session_new_password",
			wantCode:      http.StatusBadRequest,
			challengeName: "NEW_PASSWORD_REQUIRED",
			badSession:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupHandlerPoolAndClient(t, h, "client-challenge-pool")

			// Create user requiring password change.
			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          "cc-user",
				"TemporaryPassword": "TempPass1!",
			})

			session := "bad-session"

			if !tt.badSession && tt.challengeName == "NEW_PASSWORD_REQUIRED" {
				// Trigger the challenge.
				authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
					"ClientId": clientID,
					"AuthFlow": "USER_PASSWORD_AUTH",
					"AuthParameters": map[string]string{
						"USERNAME": "cc-user",
						"PASSWORD": "TempPass1!",
					},
				})

				var authResp struct {
					Session string `json:"Session,omitempty"`
				}

				require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
				session = authResp.Session
			}

			rec := doCognitoRequest(t, h, "RespondToAuthChallenge", map[string]any{
				"ClientId":      clientID,
				"ChallengeName": tt.challengeName,
				"Session":       session,
				"ChallengeResponses": map[string]string{
					"NEW_PASSWORD": "NewPass1!",
					"USERNAME":     "cc-user",
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Token includes cognito:groups when user is in a group ----

func TestAccuracy_IDToken_IncludesGroups(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("groups-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "gc")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "groups-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "groups-user", user.ConfirmCode))

	// Create a group and add the user.
	_, err = b.CreateGroup(pool.ID, "admins", "admin group", 1)
	require.NoError(t, err)
	require.NoError(t, b.AdminAddUserToGroup(pool.ID, "groups-user", "admins"))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "groups-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	// cognito:groups appears in the ID token.
	claims := decodeJWTPayload(t, result.Tokens.IDToken)
	groups, ok := claims["cognito:groups"]
	require.True(t, ok, "cognito:groups should be in ID token when user is in a group")
	_ = groups
}

// ---- validatePassword edge cases ----

func TestBackend_SignUpWithValidation_PasswordPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{name: "valid", password: "ValidPass123!"},
		{name: "too_short", password: "Abc1!", wantErr: true},
		{name: "no_uppercase", password: "lowercase1!", wantErr: true},
		{name: "no_lowercase", password: "UPPERCASE1!", wantErr: true},
		{name: "no_number", password: "NoNumber!", wantErr: true},
		{name: "no_symbol", password: "NoSymbol123", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("policy-pool")
			require.NoError(t, err)

			require.NoError(t, b.UpdateUserPoolWithOpts(pool.ID, "", cognitoidp.UserPoolOptions{
				PasswordPolicy: &cognitoidp.PasswordPolicy{
					MinimumLength:    8,
					RequireUppercase: true,
					RequireLowercase: true,
					RequireNumbers:   true,
					RequireSymbols:   true,
				},
			}))

			client, err := b.CreateUserPoolClientWithOpts(pool.ID, "pc", cognitoidp.UserPoolClientOptions{})
			require.NoError(t, err)

			_, signUpErr := b.SignUpWithValidation(client.ClientID, "test-user", tt.password, map[string]string{})

			if tt.wantErr {
				require.Error(t, signUpErr)
			} else {
				require.NoError(t, signUpErr)
			}
		})
	}
}
