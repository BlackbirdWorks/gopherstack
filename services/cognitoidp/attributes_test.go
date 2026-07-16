package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyUserAttribute_RealCodeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name        string
		useRealCode bool
		wantStatus  int
		wantErrType string
	}{
		{
			name:        "correct_code_succeeds",
			useRealCode: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "wrong_code_fails",
			useRealCode: false,
			wantStatus:  http.StatusBadRequest,
			wantErrType: "CodeMismatchException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use the backend directly so we can extract the verification code.
			b := newTestBackend()
			h := cognitoidp.NewHandler(b, "us-east-1")

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
				"PoolName": "verify-attr-pool-" + tt.name,
			})
			require.Equal(t, http.StatusOK, poolRec.Code)
			var poolResp struct {
				UserPool struct {
					ID string `json:"Id"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp.UserPool.ID

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

			// Sign up and confirm a user with email attribute.
			signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
				"ClientId": clientID,
				"Username": "verifyuser",
				"Password": "Passw0rd!",
				"UserAttributes": []map[string]string{
					{"Name": "email", "Value": "user@example.com"},
				},
			})
			require.Equal(t, http.StatusOK, signUpRec.Code, signUpRec.Body.String())

			adminConfRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
				"UserPoolId": poolID,
				"Username":   "verifyuser",
			})
			require.Equal(t, http.StatusOK, adminConfRec.Code)

			// Authenticate to get access token.
			authResp := initiateAuth(t, h, clientID, "verifyuser")
			authResult, ok := authResp["AuthenticationResult"].(map[string]any)
			require.True(t, ok, "missing AuthenticationResult")
			accessToken, ok := authResult["AccessToken"].(string)
			require.True(t, ok, "missing AccessToken")

			// Request attribute verification code — stored in backend.
			getCodeRec := doCognitoRequest(t, h, "GetUserAttributeVerificationCode", map[string]any{
				"AccessToken":   accessToken,
				"AttributeName": "email",
			})
			require.Equal(t, http.StatusOK, getCodeRec.Code, getCodeRec.Body.String())

			// Retrieve the actual code from the backend (not sent in HTTP response — simulates delivery).
			realCode := b.GetAttrVerificationCodeForTest(poolID, "verifyuser", "email")
			require.NotEmpty(t, realCode, "expected verification code stored in backend")

			code := "000000"
			if tt.useRealCode {
				code = realCode
			}

			rec := doCognitoRequest(t, h, "VerifyUserAttribute", map[string]any{
				"AccessToken":   accessToken,
				"AttributeName": "email",
				"Code":          code,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

func TestAttrVerification_GetAndVerify(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-verify-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-user", "Pass1234!", map[string]string{
		"email": "attr@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-user", "Pass1234!")
	require.NoError(t, err)
	accessToken := result.Tokens.AccessToken

	// Generate code.
	code, dest, medium, err := b.GetUserAttributeVerificationCode(accessToken, "email")
	require.NoError(t, err)
	assert.NotEmpty(t, code)
	assert.NotEmpty(t, dest)
	assert.Equal(t, "EMAIL", medium)
	assert.Contains(t, dest, "***")

	// Verify with code.
	require.NoError(t, b.VerifyUserAttributeWithCode(accessToken, "email", code))

	// User attribute should be verified.
	u, err := b.GetUser(accessToken)
	require.NoError(t, err)
	assert.Equal(t, "true", u.Attributes["email_verified"])
}

func TestAttrVerification_WrongCode(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-verify-wrong-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-client2")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-user2", "Pass1234!", map[string]string{
		"email": "wrong@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-user2", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-user2", "Pass1234!")
	require.NoError(t, err)

	_, _, _, err = b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "email")
	require.NoError(t, err)

	// Wrong code.
	err = b.VerifyUserAttributeWithCode(result.Tokens.AccessToken, "email", "WRONG!")
	require.Error(t, err)
}

func TestAttrVerification_NoCodeGenerated(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-nocode-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-nocode-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-nocode-user", "Pass1234!", map[string]string{
		"email": "nocode@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-nocode-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-nocode-user", "Pass1234!")
	require.NoError(t, err)

	// No code generated — verify should fail.
	err = b.VerifyUserAttributeWithCode(result.Tokens.AccessToken, "email", "123456")
	require.Error(t, err)
}

func TestAttrVerification_PhoneNumber(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-phone-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "phone-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "phone-user", "Pass1234!", map[string]string{
		"phone_number": "+14155551234",
		"email":        "phone@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "phone-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "phone-user", "Pass1234!")
	require.NoError(t, err)

	code, dest, medium, err := b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "phone_number")
	require.NoError(t, err)
	assert.NotEmpty(t, code)
	assert.Equal(t, "SMS", medium)
	assert.Contains(t, dest, "+*******")
	assert.Contains(t, dest, "1234")
}

func TestAttrVerification_InvalidAttribute(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-invalid-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-inv-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-inv-user", "Pass1234!", map[string]string{
		"email": "inv@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-inv-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-inv-user", "Pass1234!")
	require.NoError(t, err)

	// "name" is not verifiable.
	_, _, _, err = b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "name")
	require.Error(t, err)
}

func TestAttrVerification_Handler_GetAndVerify(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "attr-handler-pool")

	rec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "attr-handler-user",
		"Password": "Pass1234!",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "handler@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var signUpResp struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
		UserConfirmed       bool              `json:"UserConfirmed,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &signUpResp))

	if !signUpResp.UserConfirmed {
		confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
			"ClientId":         clientID,
			"Username":         "attr-handler-user",
			"ConfirmationCode": signUpResp.CodeDeliveryDetails["ConfirmationCode"],
		})
		require.Equal(t, http.StatusOK, confirmRec.Code)
	}

	accessToken := loginViaHandler(t, h, clientID, "attr-handler-user")

	// GetUserAttributeVerificationCode.
	rec = doCognitoRequest(t, h, "GetUserAttributeVerificationCode", map[string]any{
		"AccessToken":   accessToken,
		"AttributeName": "email",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var codeOut struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &codeOut))
	assert.Equal(t, "EMAIL", codeOut.CodeDeliveryDetails["DeliveryMedium"])
	assert.NotEmpty(t, codeOut.CodeDeliveryDetails["Destination"])

	// Get code from backend for test.
	code := h.Backend.GetAttrVerificationCodeForTest(poolID, "attr-handler-user", "email")
	require.NotEmpty(t, code)

	// VerifyUserAttribute with real code.
	rec = doCognitoRequest(t, h, "VerifyUserAttribute", map[string]any{
		"AccessToken":   accessToken,
		"AttributeName": "email",
		"Code":          code,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestEvictExpiredAttrVerificationCodes(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("evict-attr-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "evict-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "evict-user", "Pass1234!", map[string]string{
		"email": "evict@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "evict-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "evict-user", "Pass1234!")
	require.NoError(t, err)

	code, _, _, err := b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "email")
	require.NoError(t, err)
	assert.NotEmpty(t, code)

	// Evict — code should still be there (not expired).
	b.EvictExpiredAttrVerificationCodes()

	// Code should still work since not expired.
	storedCode := b.GetAttrVerificationCodeForTest(pool.ID, "evict-user", "email")
	assert.NotEmpty(t, storedCode)
}

func TestGetUserAttributeVerifCode_MasksEmail(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("mask-email-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "mask-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "mask-user", "Pass1234!", map[string]string{
		"email": "johndoe@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "mask-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "mask-user", "Pass1234!")
	require.NoError(t, err)

	_, dest, medium, err := b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "email")
	require.NoError(t, err)
	assert.Equal(t, "EMAIL", medium)
	// Should start with "jo***" and contain the domain.
	assert.Contains(t, dest, "jo***")
	assert.Contains(t, dest, "@example.com")
}

// TestHandler_DeleteUserAttributes covers the HTTP handler for DeleteUserAttributes.
func TestHandler_DeleteUserAttributes(t *testing.T) {
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
			_, clientID := setupHandlerPoolAndClient(t, h, "del-attr-pool")

			var accessToken string

			if tt.name == "success" {
				signUpAndConfirmViaHandler(t, h, clientID, "del-attr-user")
				accessToken = loginViaHandler(t, h, clientID, "del-attr-user")
			} else {
				accessToken = "bad-token"
			}

			rec := doCognitoRequest(t, h, "DeleteUserAttributes", map[string]any{
				"AccessToken":        accessToken,
				"UserAttributeNames": []string{"email"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_VerifyUserAttribute covers the HTTP handler for VerifyUserAttribute.
func TestHandler_VerifyUserAttribute(t *testing.T) {
	t.Parallel()

	t.Run("bad_token", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doCognitoRequest(t, h, "VerifyUserAttribute", map[string]any{
			"AccessToken":   "bad-token",
			"AttributeName": "email",
			"Code":          "123456",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("success_with_real_code", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		poolID, clientID := setupHandlerPoolAndClient(t, h, "verify-attr-pool2")

		// Sign up with email so GetUserAttributeVerificationCode works.
		rec := doCognitoRequest(t, h, "SignUp", map[string]any{
			"ClientId": clientID,
			"Username": "verify-attr-user2",
			"Password": "Pass1234!",
			"UserAttributes": []map[string]string{
				{"Name": "email", "Value": "verify@example.com"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var signUpResp struct {
			CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
			UserConfirmed       bool              `json:"UserConfirmed,omitempty"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &signUpResp))

		if !signUpResp.UserConfirmed {
			confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
				"ClientId":         clientID,
				"Username":         "verify-attr-user2",
				"ConfirmationCode": signUpResp.CodeDeliveryDetails["ConfirmationCode"],
			})
			require.Equal(t, http.StatusOK, confirmRec.Code)
		}

		accessToken := loginViaHandler(t, h, clientID, "verify-attr-user2")

		// Generate and store a verification code.
		_, _, _, err := h.Backend.GetUserAttributeVerificationCode(accessToken, "email")
		require.NoError(t, err)

		code := h.Backend.GetAttrVerificationCodeForTest(poolID, "verify-attr-user2", "email")
		require.NotEmpty(t, code)

		rec = doCognitoRequest(t, h, "VerifyUserAttribute", map[string]any{
			"AccessToken":   accessToken,
			"AttributeName": "email",
			"Code":          code,
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// TestBackend_DeleteUserAttributes covers the backend DeleteUserAttributes function.
func TestBackend_DeleteUserAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "success"},
		{name: "bad_token", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "bad_token" {
				err := b.DeleteUserAttributes("bad-token", []string{"email"})
				require.Error(t, err)

				return
			}

			pool, err := b.CreateUserPool("del-attr-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "dac")
			require.NoError(t, err)

			user, err := b.SignUp(
				client.ClientID,
				"del-attr-user",
				"Pass1234!",
				map[string]string{"email": "x@example.com"},
			)
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "del-attr-user", user.ConfirmCode))

			result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "del-attr-user", "Pass1234!")
			require.NoError(t, err)
			require.NotNil(t, result.Tokens)

			err = b.DeleteUserAttributes(result.Tokens.AccessToken, []string{"email"})
			require.NoError(t, err)
		})
	}
}

// TestBackend_VerifyUserAttribute covers the backend VerifyUserAttribute function.
func TestBackend_VerifyUserAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "success"},
		{name: "bad_token", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "bad_token" {
				err := b.VerifyUserAttribute("bad-token", "email", "123456")
				require.Error(t, err)

				return
			}

			pool, err := b.CreateUserPool("verify-attr-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "vac")
			require.NoError(t, err)

			user, err := b.SignUp(client.ClientID, "verify-attr-user", "Pass1234!", map[string]string{})
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "verify-attr-user", user.ConfirmCode))

			result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "verify-attr-user", "Pass1234!")
			require.NoError(t, err)
			require.NotNil(t, result.Tokens)

			// VerifyUserAttribute is a no-op stub in the backend; just check it doesn't error.
			err = b.VerifyUserAttribute(result.Tokens.AccessToken, "email", "123456")
			require.NoError(t, err)
		})
	}
}

func TestHandler_UpdateUserAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		badToken bool
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid_token",
			wantCode: http.StatusBadRequest,
			badToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "attr-pool"})
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
				"Username":          "attruser",
				"TemporaryPassword": "Temp123!",
			})
			doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
				"UserPoolId": poolID,
				"Username":   "attruser",
				"Password":   "PermPass456!",
				"Permanent":  true,
			})

			authRec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
				"AuthFlow":   "USER_PASSWORD_AUTH",
				"AuthParameters": map[string]string{
					"USERNAME": "attruser",
					"PASSWORD": "PermPass456!",
				},
			})
			require.Equal(t, http.StatusOK, authRec.Code)

			var authData map[string]map[string]any
			require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authData))
			accessToken := authData["AuthenticationResult"]["AccessToken"].(string)

			if tt.badToken {
				accessToken = "invalid-token"
			}

			rec := doCognitoRequest(t, h, "UpdateUserAttributes", map[string]any{
				"AccessToken": accessToken,
				"UserAttributes": []map[string]any{
					{"Name": "custom:role", "Value": "editor"},
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if !tt.badToken {
				// Verify attribute was updated via GetUser.
				guRec := doCognitoRequest(t, h, "GetUser", map[string]any{"AccessToken": accessToken})
				assert.Equal(t, http.StatusOK, guRec.Code)
				assert.Contains(t, guRec.Body.String(), "editor")
			}
		})
	}
}

func TestHandler_AdminUpdateUserAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
		wantCode int
	}{
		{
			name:     "success",
			username: "attruser",
			wantCode: http.StatusOK,
		},
		{
			name:     "user_not_found",
			username: "nobody",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "admin-attr-pool"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          "attruser",
				"TemporaryPassword": "Temp123!",
			})

			rec := doCognitoRequest(t, h, "AdminUpdateUserAttributes", map[string]any{
				"UserPoolId": poolID,
				"Username":   tt.username,
				"UserAttributes": []map[string]any{
					{"Name": "custom:role", "Value": "admin"},
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AddCustomAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		body     func(poolID string) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "test-pool"})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["UserPool"].(map[string]any)["Id"].(string)
			},
			body: func(poolID string) map[string]any {
				return map[string]any{
					"UserPoolId": poolID,
					"CustomAttributes": []map[string]any{
						{
							"Name":              "custom:department",
							"AttributeDataType": "String",
							"Mutable":           true,
						},
					},
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name:  "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string { return "us-east-1_NOTEXIST" },
			body: func(poolID string) map[string]any {
				return map[string]any{
					"UserPoolId":       poolID,
					"CustomAttributes": []map[string]any{{"Name": "custom:x"}},
				}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)
			rec := doCognitoRequest(t, h, "AddCustomAttributes", tt.body(poolID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AdminDeleteUserAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupAttrs map[string]any
		name       string
		attrNames  []string
		wantCode   int
		setupUser  bool
	}{
		{
			name:      "success",
			setupUser: true,
			setupAttrs: map[string]any{
				"email": "test@example.com",
				"phone": "+1234567890",
			},
			attrNames: []string{"phone"},
			wantCode:  http.StatusOK,
		},
		{
			name:      "user_not_found",
			setupUser: false,
			attrNames: []string{"email"},
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "attr-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			username := "deleteattr-user"
			if tt.setupUser {
				userAttrs := make([]map[string]any, 0)
				for k, v := range tt.setupAttrs {
					userAttrs = append(userAttrs, map[string]any{"Name": k, "Value": v})
				}
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "TempPass123!",
					"UserAttributes":    userAttrs,
				})
			}

			rec := doCognitoRequest(t, h, "AdminDeleteUserAttributes", map[string]any{
				"UserPoolId":         poolID,
				"Username":           username,
				"UserAttributeNames": tt.attrNames,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAddCustomAttributes_RequiresCustomPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		attrName string
		wantCode int
	}{
		{
			name:     "valid_custom_prefix",
			attrName: "custom:role",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_custom_prefix",
			attrName: "role",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "email_no_prefix_rejected",
			attrName: "email",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "prefix-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			rec := doCognitoRequest(t, h, "AddCustomAttributes", map[string]any{
				"UserPoolId": poolID,
				"CustomAttributes": []map[string]any{
					{"Name": tt.attrName, "AttributeDataType": "String"},
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSortedAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-attrs-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "attr-user",
		"TemporaryPassword": "TempPass123!",
		"UserAttributes": []map[string]any{
			{"Name": "zz_last", "Value": "z"},
			{"Name": "aa_first", "Value": "a"},
			{"Name": "mm_middle", "Value": "m"},
		},
	})

	rec := doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "attr-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	attrs := resp["UserAttributes"].([]any)
	require.GreaterOrEqual(t, len(attrs), 3)

	names := make([]string, 0, len(attrs))
	for _, a := range attrs {
		names = append(names, a.(map[string]any)["Name"].(string))
	}

	// Verify sorted order.
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "attributes should be sorted by name")
	}
}

// TestParityB_IDTokenUserAttributeClaims verifies that email and other user attrs appear in ID tokens.
func TestIDTokenUserAttributeClaims(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName":               "attr-pool",
		"AutoVerifiedAttributes": []string{"email"},
	})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "attr-client",
		"ExplicitAuthFlows": []string{
			"ALLOW_USER_PASSWORD_AUTH",
			"ALLOW_REFRESH_TOKEN_AUTH",
		},
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "attruser",
		"Password": "Pass1234!",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "attruser@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, signupRec.Code)

	var signupResp map[string]any
	require.NoError(t, json.Unmarshal(signupRec.Body.Bytes(), &signupResp))

	// Auto-confirmed because email is in AutoVerifiedAttributes.
	require.True(t, signupResp["UserConfirmed"].(bool), "user should be auto-confirmed")

	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"ClientId": clientID,
		"AuthFlow": "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "attruser",
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, authRec.Code)

	var authResp map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	idToken := authResp["AuthenticationResult"].(map[string]any)["IdToken"].(string)

	claims := decodeJWTPayload(t, idToken)
	assert.Equal(t, "attruser@example.com", claims["email"], "email must appear in ID token")
	assert.Equal(t, "true", claims["email_verified"], "email_verified must appear in ID token")
}

// TestParityB_AdminCreateUserSubInAttributes verifies sub appears in AdminCreateUser response.
func TestAdminCreateUserSubInAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupPoolAndClientNamed(t, h, "sub-pool", "sub-client")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "subuser",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	user := resp["User"].(map[string]any)
	attrs := user["UserAttributes"].([]any)

	var subAttr map[string]any

	for _, a := range attrs {
		attr := a.(map[string]any)
		if attr["Name"].(string) == "sub" {
			subAttr = attr

			break
		}
	}
	require.NotNil(t, subAttr, "sub attribute must be present in AdminCreateUser response")
	assert.NotEmpty(t, subAttr["Value"], "sub must have a value")
}
