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

func TestHandler_UserSRPAuth_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "srp-http-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "srp-user")

	initRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_SRP_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": "srp-user",
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, initRec.Code)

	var initResp struct {
		ChallengeName *string `json:"ChallengeName,omitempty"`
		Session       *string `json:"Session,omitempty"`
	}
	require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
	require.NotNil(t, initResp.ChallengeName)
	assert.Equal(t, "PASSWORD_VERIFIER", *initResp.ChallengeName)

	respRec := doCognitoRequest(t, h, "RespondToAuthChallenge", map[string]any{
		"ClientId":           clientID,
		"ChallengeName":      "PASSWORD_VERIFIER",
		"Session":            *initResp.Session,
		"ChallengeResponses": map[string]string{},
	})
	require.Equal(t, http.StatusOK, respRec.Code)

	var tokenResp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken,omitempty"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(respRec.Body.Bytes(), &tokenResp))
	require.NotNil(t, tokenResp.AuthenticationResult)
	assert.NotEmpty(t, tokenResp.AuthenticationResult.AccessToken)
}

func TestHandler_AdminCreateUser_PolicyEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "admin-create-policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{
				"MinimumLength":    10,
				"RequireUppercase": true,
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
	poolID := poolResp.UserPool.ID

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "baduser",
		"TemporaryPassword": "short",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "gooduser",
		"TemporaryPassword": "ValidPass1234",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestAuthFlow_UserPasswordAuth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "auth-pool")

	// SignUp + confirm.
	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "authuser",
		"Password": "Passw0rd!",
	})
	require.Equal(t, http.StatusOK, signUpRec.Code)

	confRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "authuser",
	})
	require.Equal(t, http.StatusOK, confRec.Code)

	tests := []struct {
		name       string
		password   string
		wantStatus int
	}{
		{name: "correct_password", password: "Passw0rd!", wantStatus: http.StatusOK},
		{name: "wrong_password", password: "WrongPass!", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
				"AuthFlow": "USER_PASSWORD_AUTH",
				"ClientId": clientID,
				"AuthParameters": map[string]string{
					"USERNAME": "authuser",
					"PASSWORD": tt.password,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				authResult, ok := resp["AuthenticationResult"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, authResult["AccessToken"])
				assert.NotEmpty(t, authResult["IdToken"])
				assert.NotEmpty(t, authResult["RefreshToken"])
			}
		})
	}
}

func TestAdminCreateUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "adminuser",
		"TemporaryPassword": "TempPass1!",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		User struct {
			Username string `json:"Username"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "adminuser", resp.User.Username)
}

func TestAdminCreateUser_Suppress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-suppress-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "suppressed-user",
		"TemporaryPassword": "Temp1234!",
		"MessageAction":     "SUPPRESS",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "suppress@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		User *struct {
			Username   string `json:"Username,omitempty"`
			UserStatus string `json:"UserStatus,omitempty"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.User)
	assert.Equal(t, "suppressed-user", out.User.Username)
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", out.User.UserStatus)
}

func TestAdminCreateUser_Resend(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-resend-pool")

	// Create user.
	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "resend-user",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Resend — should succeed and return same user.
	rec = doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "resend-user",
		"MessageAction": "RESEND",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		User *struct {
			Username   string `json:"Username,omitempty"`
			UserStatus string `json:"UserStatus,omitempty"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.User)
	assert.Equal(t, "resend-user", out.User.Username)
}

func TestAdminCreateUser_DeliveryMediums(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-delivery-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":             poolID,
		"Username":               "delivery-user",
		"TemporaryPassword":      "Temp1234!",
		"DesiredDeliveryMediums": []string{"EMAIL"},
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "delivery@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		User *struct {
			Username string `json:"Username,omitempty"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.User)
	assert.Equal(t, "delivery-user", out.User.Username)
}

func TestAdminSetUserPassword_Permanent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-pwd-perm-pool")

	// Create user in FORCE_CHANGE_PASSWORD.
	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "pwd-user",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set permanent password.
	rec = doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "pwd-user",
		"Password":   "Perm5678!",
		"Permanent":  true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// User should now be CONFIRMED.
	rec = doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "pwd-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UserStatus string `json:"UserStatus,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "CONFIRMED", out.UserStatus)
}

func TestAdminSetUserPassword_Temporary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-pwd-temp-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "temp-pwd-user",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set another temporary password.
	rec = doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "temp-pwd-user",
		"Password":   "NewTemp9999!",
		"Permanent":  false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// User should remain FORCE_CHANGE_PASSWORD.
	rec = doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "temp-pwd-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UserStatus string `json:"UserStatus,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", out.UserStatus)
}

func TestUserSettings_And_AuthFactors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "user-settings-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "settings-user")
	accessToken := loginViaHandler(t, h, clientID, "settings-user")

	// Initially, only PASSWORD is configured.
	rec := doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var factorsResp struct {
		Username                  string   `json:"Username,omitempty"`
		ConfiguredUserAuthFactors []string `json:"ConfiguredUserAuthFactors,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &factorsResp))
	assert.Equal(t, "settings-user", factorsResp.Username)
	assert.Contains(t, factorsResp.ConfiguredUserAuthFactors, "PASSWORD")
	assert.NotContains(t, factorsResp.ConfiguredUserAuthFactors, "SMS_OTP")
	assert.NotContains(t, factorsResp.ConfiguredUserAuthFactors, "WEB_AUTHN")

	// SetUserSettings persists legacy MFAOptions (SMS).
	rec = doCognitoRequest(t, h, "SetUserSettings", map[string]any{
		"AccessToken": accessToken,
		"MFAOptions": []map[string]any{
			{"DeliveryMedium": "SMS", "AttributeName": "phone_number"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &factorsResp))
	assert.Contains(t, factorsResp.ConfiguredUserAuthFactors, "SMS_OTP")

	// Register a WebAuthn credential -> WEB_AUTHN factor appears too.
	rec = doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
		"AccessToken": accessToken,
		"Credential":  map[string]any{"id": "factor-cred"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &factorsResp))
	assert.Contains(t, factorsResp.ConfiguredUserAuthFactors, "WEB_AUTHN")

	// AdminSetUserSettings — admin variant, same persisted state.
	rec = doCognitoRequest(t, h, "AdminSetUserSettings", map[string]any{
		"UserPoolId": poolID,
		"Username":   "settings-user",
		"MFAOptions": []map[string]any{
			{"DeliveryMedium": "SMS", "AttributeName": "phone_number"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// AdminSetUserSettings on unknown user fails.
	rec = doCognitoRequest(t, h, "AdminSetUserSettings", map[string]any{
		"UserPoolId": poolID,
		"Username":   "ghost",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetUserAuthFactors_InvalidAccessToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": "garbage"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_DeleteUser covers the HTTP handler for DeleteUser.
func TestHandler_DeleteUser(t *testing.T) {
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
			_, clientID := setupHandlerPoolAndClient(t, h, "del-user-pool")

			var accessToken string

			if tt.name == "success" {
				signUpAndConfirmViaHandler(t, h, clientID, "del-user")
				accessToken = loginViaHandler(t, h, clientID, "del-user")
			} else {
				accessToken = "bad-token"
			}

			rec := doCognitoRequest(t, h, "DeleteUser", map[string]any{
				"AccessToken": accessToken,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBackend_ListUsers covers the backend ListUsers function.
func TestCognito_ListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "list-pool",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, unmarshalBody(t, rec, &resp))
	poolID := resp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "listuser",
	})

	rec = doCognitoRequest(t, h, "ListUsers", map[string]any{
		"UserPoolId": poolID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_AdminCreateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) string
		name         string
		username     string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			username:     "adminuser",
			wantCode:     http.StatusOK,
			wantContains: []string{"adminuser", "FORCE_CHANGE_PASSWORD"},
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			username: "adminuser",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          tt.username,
				"TemporaryPassword": "TempPass123!",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_AdminSetUserPassword(t *testing.T) {
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
					"Username":          "setpassuser",
					"TemporaryPassword": "Temp123!",
				})

				return poolID, "setpassuser"
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
				"Password":   "NewPass123!",
				"Permanent":  true,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

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
