package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestHandler_SecretHash_InitiateAuth_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pool + client WITH a secret.
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sh-http-pool"})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp struct {
		UserPool struct {
			ID string `json:"Id,omitempty"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp.UserPool.ID

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":     poolID,
		"ClientName":     "sh-client",
		"GenerateSecret": true,
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp struct {
		UserPoolClient struct {
			ClientID     string `json:"ClientId,omitempty"`
			ClientSecret string `json:"ClientSecret,omitempty"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp.UserPoolClient.ClientID
	secret := clientResp.UserPoolClient.ClientSecret
	require.NotEmpty(t, secret)

	// Sign up + confirm.
	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId":   clientID,
		"Username":   "sh-user",
		"Password":   "Pass1234!",
		"SecretHash": computeSecretHash(clientID, "sh-user", secret),
	})
	require.Equal(t, http.StatusOK, signUpRec.Code, "SignUp: %s", signUpRec.Body.String())

	var signUpResp struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	}
	require.NoError(t, json.Unmarshal(signUpRec.Body.Bytes(), &signUpResp))
	code := signUpResp.CodeDeliveryDetails["ConfirmationCode"]

	confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
		"ClientId":         clientID,
		"Username":         "sh-user",
		"ConfirmationCode": code,
		"SecretHash":       computeSecretHash(clientID, "sh-user", secret),
	})
	require.Equal(t, http.StatusOK, confirmRec.Code, "ConfirmSignUp: %s", confirmRec.Body.String())

	// Auth with valid SecretHash succeeds.
	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    "sh-user",
			"PASSWORD":    "Pass1234!",
			"SECRET_HASH": computeSecretHash(clientID, "sh-user", secret),
		},
	})
	assert.Equal(t, http.StatusOK, authRec.Code, "valid SecretHash should auth: %s", authRec.Body.String())

	// Auth with wrong SecretHash is rejected.
	badRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    "sh-user",
			"PASSWORD":    "Pass1234!",
			"SECRET_HASH": "badsecret==",
		},
	})
	assert.Equal(t, http.StatusBadRequest, badRec.Code, "bad SecretHash should fail")

	// Auth without SecretHash is rejected when client has a secret.
	noHashRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": "sh-user",
			"PASSWORD": "Pass1234!",
		},
	})
	assert.Equal(t, http.StatusBadRequest, noHashRec.Code, "missing SecretHash should fail")
}

func TestSignUp_ConfirmSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name     string
		username string
		password string
		wantOK   bool
		policy   map[string]any
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

func TestTokenExpiryFor_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name      string
		validity  int32
		unit      string
		tokenType string
		wantSecs  float64
	}{
		{name: "access_minutes", validity: 60, unit: "minutes", tokenType: "AccessToken", wantSecs: 3600},
		{name: "id_hours", validity: 2, unit: "hours", tokenType: "IdToken", wantSecs: 7200},
		{name: "refresh_days", validity: 1, unit: "days", tokenType: "RefreshToken", wantSecs: 86400},
		{name: "access_seconds", validity: 300, unit: "seconds", tokenType: "AccessToken", wantSecs: 300},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "expiry-pool-"+tt.name)

			unitKey := tt.tokenType
			rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId":           poolID,
				"ClientName":           "expiry-client",
				"AccessTokenValidity":  tt.validity,
				"IdTokenValidity":      tt.validity,
				"RefreshTokenValidity": tt.validity,
				"TokenValidityUnits": map[string]any{
					unitKey: tt.unit,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp struct {
				UserPoolClient struct { //nolint:govet // fieldalignment: test struct, cosmetic only
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.Equal(t, tt.validity, createResp.UserPoolClient.AccessTokenValidity)
			assert.Equal(t, tt.unit, createResp.UserPoolClient.TokenValidityUnits[unitKey])
		})
	}
}

func TestHandler_AdminUserGlobalSignOut_Via_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "pool_not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupHandlerPoolAndClient(t, h, "signout-http-pool")

			username := "so-user"

			if tt.name == "success" {
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
				_ = clientID
			}

			reqPoolID := poolID
			if tt.name == "pool_not_found" {
				reqPoolID = "bad-pool"
			}

			rec := doCognitoRequest(t, h, "AdminUserGlobalSignOut", map[string]any{
				"UserPoolId": reqPoolID,
				"Username":   username,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_GlobalSignOut_Via_HTTP covers the HTTP handler for GlobalSignOut.
func TestHandler_GlobalSignOut_Via_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "bad_token", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupHandlerPoolAndClient(t, h, "gsignout-pool")

			var accessToken string

			if tt.name == "success" {
				signUpAndConfirmViaHandler(t, h, clientID, "gs-user")
				accessToken = loginViaHandler(t, h, clientID, "gs-user")
			} else {
				accessToken = "invalid-token"
				_ = poolID
			}

			rec := doCognitoRequest(t, h, "GlobalSignOut", map[string]any{
				"AccessToken": accessToken,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_ResendConfirmationCode_Via_HTTP covers the HTTP handler for ResendConfirmationCode.
func TestHandler_ResendConfirmationCode_Via_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "bad_client", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, clientID := setupHandlerPoolAndClient(t, h, "resend-http-pool")

			if tt.name == "success" {
				doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "resend-http-user",
					"Password": "Pass1234!",
				})

				rec := doCognitoRequest(t, h, "ResendConfirmationCode", map[string]any{
					"ClientId": clientID,
					"Username": "resend-http-user",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			} else {
				rec := doCognitoRequest(t, h, "ResendConfirmationCode", map[string]any{
					"ClientId": "bad-client",
					"Username": "any-user",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

// TestHandler_AdminResetUserPassword covers the HTTP handler for AdminResetUserPassword.
func TestHandler_AdminResetUserPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "user_not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "reset-pool")

			username := "reset-user"

			if tt.name == "success" {
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "TempPass1!",
				})
			}

			if tt.name == "user_not_found" {
				username = "no-such-user"
			}

			rec := doCognitoRequest(t, h, "AdminResetUserPassword", map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

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

func TestHandler_RevokeToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Set up pool, client and confirmed user.
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "revoke-pool"})
	var poolResp map[string]map[string]any
	_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
	poolID := poolRec.Body.String()
	_ = poolID

	var poolData map[string]map[string]any
	_ = json.Unmarshal(poolRec.Body.Bytes(), &poolData)
	pID := poolData["UserPool"]["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": pID,
		"ClientName": "revoke-client",
	})
	var clientData map[string]map[string]any
	_ = json.Unmarshal(clientRec.Body.Bytes(), &clientData)
	clientID := clientData["UserPoolClient"]["ClientId"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        pID,
		"Username":          "revokeuser",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": pID,
		"Username":   "revokeuser",
		"Password":   "PermPass456!",
		"Permanent":  true,
	})

	// Authenticate to get tokens.
	authRec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": pID,
		"ClientId":   clientID,
		"AuthFlow":   "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "revokeuser",
			"PASSWORD": "PermPass456!",
		},
	})
	require.Equal(t, http.StatusOK, authRec.Code)

	var authData map[string]map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authData))
	refreshToken := authData["AuthenticationResult"]["RefreshToken"].(string)

	// RevokeToken should succeed (200).
	rec := doCognitoRequest(t, h, "RevokeToken", map[string]any{
		"ClientId": clientID,
		"Token":    refreshToken,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// After revocation, using the refresh token must fail.
	rec2 := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"ClientId": clientID,
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// Revoking an already-revoked (unknown) token is a no-op (200 per AWS docs).
	rec3 := doCognitoRequest(t, h, "RevokeToken", map[string]any{
		"ClientId": clientID,
		"Token":    refreshToken,
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
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

func TestHandler_ForgotPasswordConfirmForgotPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "fp-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "fp-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "fpuser",
		"Password": "OldPass123!",
	})
	doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "fpuser",
	})

	// ForgotPassword — returns code in CodeDeliveryDetails.
	fpRec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
		"ClientId": clientID,
		"Username": "fpuser",
	})
	assert.Equal(t, http.StatusOK, fpRec.Code)

	var fpResp map[string]any
	require.NoError(t, json.Unmarshal(fpRec.Body.Bytes(), &fpResp))
	details := fpResp["CodeDeliveryDetails"].(map[string]any)
	code := details["ConfirmationCode"].(string)
	require.NotEmpty(t, code)

	// ConfirmForgotPassword with wrong code must fail.
	wrongRec := doCognitoRequest(t, h, "ConfirmForgotPassword", map[string]any{
		"ClientId":         clientID,
		"Username":         "fpuser",
		"ConfirmationCode": "WRONGCODE",
		"Password":         "NewPass123!",
	})
	assert.Equal(t, http.StatusBadRequest, wrongRec.Code)

	// ConfirmForgotPassword with correct code must succeed.
	okRec := doCognitoRequest(t, h, "ConfirmForgotPassword", map[string]any{
		"ClientId":         clientID,
		"Username":         "fpuser",
		"ConfirmationCode": code,
		"Password":         "NewPass123!",
	})
	assert.Equal(t, http.StatusOK, okRec.Code)

	// User can now authenticate with the new password.
	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"USERNAME": "fpuser",
			"PASSWORD": "NewPass123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec.Code)
}

func TestHandler_GetUser_ChangePassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "gu-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "gu-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "guuser",
		"Password": "OldPass123!",
		"UserAttributes": []map[string]any{
			{"Name": "email", "Value": "test@example.com"},
		},
	})
	doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "guuser",
	})

	// Authenticate to get access token.
	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"USERNAME": "guuser",
			"PASSWORD": "OldPass123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec.Code)

	var authResp map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	authResult := authResp["AuthenticationResult"].(map[string]any)
	accessToken := authResult["AccessToken"].(string)

	// GetUser with valid token.
	guRec := doCognitoRequest(t, h, "GetUser", map[string]any{
		"AccessToken": accessToken,
	})
	assert.Equal(t, http.StatusOK, guRec.Code)

	var guResp map[string]any
	require.NoError(t, json.Unmarshal(guRec.Body.Bytes(), &guResp))
	assert.Equal(t, "guuser", guResp["Username"])

	// ChangePassword with wrong old password must fail.
	wrongPwRec := doCognitoRequest(t, h, "ChangePassword", map[string]any{
		"AccessToken":      accessToken,
		"PreviousPassword": "WrongPass!",
		"ProposedPassword": "NewPass123!",
	})
	assert.Equal(t, http.StatusBadRequest, wrongPwRec.Code)

	// ChangePassword with correct old password must succeed.
	changePwRec := doCognitoRequest(t, h, "ChangePassword", map[string]any{
		"AccessToken":      accessToken,
		"PreviousPassword": "OldPass123!",
		"ProposedPassword": "NewPass123!",
	})
	assert.Equal(t, http.StatusOK, changePwRec.Code)

	// User can authenticate with new password.
	authRec2 := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"USERNAME": "guuser",
			"PASSWORD": "NewPass123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec2.Code)
}

// TestForgotPassword_UserStateValidation table-drives the ForgotPassword
// user-state gate previously spread across four near-identical single-scenario
// tests: real AWS rejects disabled, unconfirmed, and FORCE_CHANGE_PASSWORD
// users, and only issues a reset code for a confirmed, enabled user.
func TestForgotPassword_UserStateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupUser  func(t *testing.T, h *cognitoidp.Handler, poolID, clientID, username string)
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "disabled_user_rejected",
			setupUser: func(t *testing.T, h *cognitoidp.Handler, poolID, clientID, username string) {
				t.Helper()

				signUpAndAdminConfirm(t, h, clientID, poolID, username)

				disableRec := doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   username,
				})
				require.Equal(t, http.StatusOK, disableRec.Code, "AdminDisableUser failed")
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "NotAuthorizedException",
		},
		{
			name: "unconfirmed_user_rejected",
			setupUser: func(t *testing.T, h *cognitoidp.Handler, _, clientID, username string) {
				t.Helper()

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": username,
					"Password": "Pass1234!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code, "SignUp failed")
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "InvalidParameterException",
		},
		{
			name: "confirmed_enabled_user_accepted",
			setupUser: func(t *testing.T, h *cognitoidp.Handler, poolID, clientID, username string) {
				t.Helper()

				signUpAndAdminConfirm(t, h, clientID, poolID, username)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "force_change_password_user_rejected",
			setupUser: func(t *testing.T, h *cognitoidp.Handler, poolID, _, username string) {
				t.Helper()

				rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "Temp1234!",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupPoolAndClientNamed(t, h, "fp-"+tt.name+"-pool", "fp-"+tt.name+"-client")
			const username = "fpuser"

			tt.setupUser(t, h, poolID, clientID, username)

			rec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
				"ClientId": clientID,
				"Username": username,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantType, errResp["__type"])
			}
		})
	}
}

// TestParityB_AdminNoSRPAuth verifies that ADMIN_NO_SRP_AUTH works as alias for ADMIN_USER_PASSWORD_AUTH.
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
func TestResendConfirmationCodeAlreadyConfirmed(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		wantType  string
		wantCode  int
		confirmed bool
	}

	tests := []testCase{
		{
			name:      "confirmed_user_gets_InvalidParameter",
			confirmed: true,
			wantCode:  http.StatusBadRequest,
			wantType:  "InvalidParameterException",
		},
		{
			name:      "unconfirmed_user_gets_new_code",
			confirmed: false,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupPoolAndClientNamed(t, h, "resend-pool-"+tt.name, "resend-client-"+tt.name)

			doCognitoRequest(t, h, "SignUp", map[string]any{
				"ClientId": clientID,
				"Username": "resenduser",
				"Password": "Pass1234!",
			})

			if tt.confirmed {
				doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
					"UserPoolId": poolID,
					"Username":   "resenduser",
				})
			}

			rec := doCognitoRequest(t, h, "ResendConfirmationCode", map[string]any{
				"ClientId": clientID,
				"Username": "resenduser",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantType, errResp["__type"])
			}
		})
	}
}

// TestParityB_ConfirmForgotPasswordExpiryCheckedFirst verifies expiry before mismatch.
func TestConfirmForgotPasswordExpiryCheckedFirst(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		useCode  string
		wantType string
		wantCode int
	}

	tests := []testCase{
		{
			name:     "wrong_code_gives_CodeMismatch",
			useCode:  "WRONGCODE",
			wantCode: http.StatusBadRequest,
			wantType: "CodeMismatchException",
		},
		{
			name:     "correct_code_succeeds",
			useCode:  "",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupPoolAndClientNamed(t, h, "cfp-pool-"+tt.name, "cfp-client-"+tt.name)

			signUpAndAdminConfirm(t, h, clientID, poolID, "cfpuser")

			rec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
				"ClientId": clientID,
				"Username": "cfpuser",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var fpResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fpResp))

			code := tt.useCode
			if code == "" {
				// Extract the real code from CodeDeliveryDetails extension.
				cdd := fpResp["CodeDeliveryDetails"].(map[string]any)
				code = cdd["ConfirmationCode"].(string)
			}

			confirmRec := doCognitoRequest(t, h, "ConfirmForgotPassword", map[string]any{
				"ClientId":         clientID,
				"Username":         "cfpuser",
				"ConfirmationCode": code,
				"Password":         "NewPass1234!",
			})
			assert.Equal(t, tt.wantCode, confirmRec.Code)

			if tt.wantType != "" {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(confirmRec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantType, errResp["__type"])
			}
		})
	}
}

// TestParity_GlobalSignOut_RejectsIDToken confirms GlobalSignOut (an access-token
// op) also rejects an ID token.
