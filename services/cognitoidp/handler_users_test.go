package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestHandler_UserSRPAuth_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "srp-http-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "srp-user")

	srpClient := newSRPTestClient(t)

	initRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_SRP_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": "srp-user",
			"SRP_A":    srpClient.srpA(),
		},
	})
	require.Equal(t, http.StatusOK, initRec.Code)

	var initResp struct {
		ChallengeName       *string           `json:"ChallengeName,omitempty"`
		Session             *string           `json:"Session,omitempty"`
		ChallengeParameters map[string]string `json:"ChallengeParameters,omitempty"`
	}
	require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
	require.NotNil(t, initResp.ChallengeName)
	assert.Equal(t, "PASSWORD_VERIFIER", *initResp.ChallengeName)

	responses := srpClient.challengeResponses(t, poolID, "Pass1234!", initResp.ChallengeParameters)

	respRec := doCognitoRequest(t, h, "RespondToAuthChallenge", map[string]any{
		"ClientId":           clientID,
		"ChallengeName":      "PASSWORD_VERIFIER",
		"Session":            *initResp.Session,
		"ChallengeResponses": responses,
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

// TestHandler_AdminGetUserAuthFactors covers the HTTP handler for
// AdminGetUserAuthFactors, asserting the wire shape (Username,
// ConfiguredUserAuthFactors, PreferredMfaSetting, UserMFASettingList) and that
// the returned factors reflect real, independently-set user state.
func TestHandler_AdminGetUserAuthFactors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (poolID, username string)
		name     string
		wantCode int
	}{
		{
			name: "password_and_sms",
			setup: func(h *cognitoidp.Handler) (string, string) {
				poolID, clientID := setupHandlerPoolAndClient(t, h, "admin-factors-pool")
				signUpAndConfirmViaHandler(t, h, clientID, "admin-factors-user")

				rec := doCognitoRequest(t, h, "AdminSetUserSettings", map[string]any{
					"UserPoolId": poolID,
					"Username":   "admin-factors-user",
					"MFAOptions": []map[string]any{
						{"DeliveryMedium": "SMS", "AttributeName": "phone_number"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return poolID, "admin-factors-user"
			},
			wantCode: http.StatusOK,
		},
		{
			name: "user_not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				poolID, _ := setupHandlerPoolAndClient(t, h, "admin-factors-missing-pool")

				return poolID, "ghost"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) (string, string) {
				return "us-east-1_nonexistent", "someone"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminGetUserAuthFactors", map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp struct {
				Username                  string   `json:"Username,omitempty"`
				ConfiguredUserAuthFactors []string `json:"ConfiguredUserAuthFactors,omitempty"`
				PreferredMfaSetting       string   `json:"PreferredMfaSetting,omitempty"`
				UserMFASettingList        []string `json:"UserMFASettingList,omitempty"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, username, resp.Username)
			assert.Contains(t, resp.ConfiguredUserAuthFactors, "PASSWORD")
			assert.Contains(t, resp.ConfiguredUserAuthFactors, "SMS_OTP")
		})
	}
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
