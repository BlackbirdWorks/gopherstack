package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestHandler_SignUp_PolicyEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "signup-policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{
				"MinimumLength":  8,
				"RequireNumbers": true,
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var poolResp struct {
		UserPool struct {
			ID string `json:"Id,omitempty"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &poolResp))

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolResp.UserPool.ID,
		"ClientName": "signup-client",
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId,omitempty"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp.UserPoolClient.ClientID

	rec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "policyuser",
		"Password": "NoDigitsHere",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "policyuser",
		"Password": "ValidPass1",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestSignUp_ConfirmSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy   map[string]any
		name     string
		username string
		password string
		wantOK   bool
	}{
		{name: "valid_user", username: "alice", password: "Passw0rd!", wantOK: true},
		{
			name:     "weak_password_fails_with_policy",
			username: "bob",
			password: "weak",
			wantOK:   false,
			policy: map[string]any{
				"MinimumLength":    8,
				"RequireUppercase": true,
				"RequireNumbers":   true,
				"RequireSymbols":   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "signup-pool-" + tt.name})
			require.Equal(t, http.StatusOK, poolRec.Code)
			var poolResp struct {
				UserPool struct {
					ID string `json:"Id"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp.UserPool.ID

			if tt.policy != nil {
				updRec := doCognitoRequest(t, h, "UpdateUserPool", map[string]any{
					"UserPoolId": poolID,
					"Policies":   map[string]any{"PasswordPolicy": tt.policy},
				})
				require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())
			}

			clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "test-client",
			})
			require.Equal(t, http.StatusOK, clientRec.Code)
			var clientResp struct {
				UserPoolClient struct {
					ClientID string `json:"ClientId"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
			clientID := clientResp.UserPoolClient.ClientID

			rec := doCognitoRequest(t, h, "SignUp", map[string]any{
				"ClientId": clientID,
				"Username": tt.username,
				"Password": tt.password,
			})
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			} else {
				assert.NotEqual(t, http.StatusOK, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandler_SignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) string
		body         func(clientID string) map[string]any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
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

				return clientResp["UserPoolClient"]["ClientId"].(string)
			},
			body: func(clientID string) map[string]any {
				return map[string]any{
					"ClientId": clientID,
					"Username": "testuser",
					"Password": "Password123!",
					"UserAttributes": []map[string]any{
						{"Name": "email", "Value": "test@example.com"},
					},
				}
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"UserSub", "UserConfirmed"},
		},
		{
			name: "invalid_client",
			setup: func(_ *cognitoidp.Handler) string {
				return "invalid-client-id"
			},
			body: func(clientID string) map[string]any {
				return map[string]any{
					"ClientId": clientID,
					"Username": "testuser",
					"Password": "Password123!",
				}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID := tt.setup(h)

			rec := doCognitoRequest(t, h, "SignUp", tt.body(clientID))
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_ConfirmSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		// setup returns clientID, username and the confirm code (may be empty for "any" codes).
		setup    func(h *cognitoidp.Handler) (clientID, username, confirmCode string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string, string) {
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
				clientID := clientResp["UserPoolClient"]["ClientId"].(string)

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "newuser",
					"Password": "Password123!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code)

				// Extract confirm code from the CodeDeliveryDetails in the SignUp response.
				var signupResp map[string]any
				_ = json.Unmarshal(signupRec.Body.Bytes(), &signupResp)
				code := ""
				if details, ok := signupResp["CodeDeliveryDetails"].(map[string]any); ok {
					code, _ = details["ConfirmationCode"].(string)
				}

				return clientID, "newuser", code
			},
			wantCode: http.StatusOK,
		},
		{
			name: "user_not_found",
			setup: func(h *cognitoidp.Handler) (string, string, string) {
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

				return clientResp["UserPoolClient"]["ClientId"].(string), "nobody", "123456"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID, username, confirmCode := tt.setup(h)

			rec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
				"ClientId":         clientID,
				"Username":         username,
				"ConfirmationCode": confirmCode,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_InitiateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) (clientID, username string)
		name         string
		password     string
		wantContains []string
		wantCode     int
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
				clientID := clientResp["UserPoolClient"]["ClientId"].(string)

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "authuser",
					"Password": "Password123!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code)

				// Extract the confirmation code from the SignUp response.
				var signupResp map[string]any
				_ = json.Unmarshal(signupRec.Body.Bytes(), &signupResp)
				code := ""
				if details, ok := signupResp["CodeDeliveryDetails"].(map[string]any); ok {
					code, _ = details["ConfirmationCode"].(string)
				}

				confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
					"ClientId":         clientID,
					"Username":         "authuser",
					"ConfirmationCode": code,
				})
				require.Equal(t, http.StatusOK, confirmRec.Code)

				return clientID, "authuser"
			},
			password:     "Password123!",
			wantCode:     http.StatusOK,
			wantContains: []string{"AccessToken", "IdToken", "RefreshToken"},
		},
		{
			name: "wrong_password",
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
				clientID := clientResp["UserPoolClient"]["ClientId"].(string)

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "authuser2",
					"Password": "Password123!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code)

				// Extract the confirmation code from the SignUp response.
				var signupResp map[string]any
				_ = json.Unmarshal(signupRec.Body.Bytes(), &signupResp)
				code := ""
				if details, ok := signupResp["CodeDeliveryDetails"].(map[string]any); ok {
					code, _ = details["ConfirmationCode"].(string)
				}

				confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
					"ClientId":         clientID,
					"Username":         "authuser2",
					"ConfirmationCode": code,
				})
				require.Equal(t, http.StatusOK, confirmRec.Code)

				return clientID, "authuser2"
			},
			password: "WrongPassword!",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
				"AuthFlow": "USER_PASSWORD_AUTH",
				"ClientId": clientID,
				"AuthParameters": map[string]string{
					"USERNAME": username,
					"PASSWORD": tt.password,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_AdminInitiateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) (poolID, clientID, username string)
		name         string
		password     string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string, string) {
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
				clientID := clientResp["UserPoolClient"]["ClientId"].(string)

				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          "adminauthuser",
					"TemporaryPassword": "Temp123!",
				})

				doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
					"UserPoolId": poolID,
					"Username":   "adminauthuser",
					"Password":   "Password123!",
					"Permanent":  true,
				})

				return poolID, clientID, "adminauthuser"
			},
			password:     "Password123!",
			wantCode:     http.StatusOK,
			wantContains: []string{"AccessToken", "IdToken"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
				"AuthFlow":   "USER_PASSWORD_AUTH",
				"AuthParameters": map[string]string{
					"USERNAME": username,
					"PASSWORD": tt.password,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_AdminConfirmSignUp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "admin-confirm-pool"})
	var poolData map[string]map[string]any
	_ = json.Unmarshal(poolRec.Body.Bytes(), &poolData)
	poolID := poolData["UserPool"]["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "c",
	})
	var clientData map[string]map[string]any
	_ = json.Unmarshal(clientRec.Body.Bytes(), &clientData)
	clientID := clientData["UserPoolClient"]["ClientId"].(string)

	signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "confuser",
		"Password": "Password123!",
	})
	require.Equal(t, http.StatusOK, signupRec.Code)

	// AdminConfirmSignUp should work without a confirmation code.
	rec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "confuser",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// After admin confirm, InitiateAuth should succeed.
	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"ClientId": clientID,
		"AuthFlow": "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "confuser",
			"PASSWORD": "Password123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec.Code)
}

func TestAdminNoSRPAuth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupPoolAndClientNamed(t, h, "nosrp-pool", "nosrp-client")

	signUpAndAdminConfirm(t, h, clientID, poolID, "nosrpuser")

	rec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "nosrpuser",
			"PASSWORD": "Pass1234!",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "ADMIN_NO_SRP_AUTH should succeed")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasAuth := resp["AuthenticationResult"]
	assert.True(t, hasAuth, "must have AuthenticationResult")
}

// TestParityB_ResendConfirmationCodeAlreadyConfirmed verifies InvalidParameterException, not CodeMismatch.
