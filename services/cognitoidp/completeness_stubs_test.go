package cognitoidp_test

// completeness_stubs_test.go exercises all stub handlers in handler_completeness.go
// that are NOT overridden by the accuracy dispatch table. Each stub returns HTTP 200
// with an empty JSON response body. One request per stub is sufficient for coverage.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompleteness_StubOperations calls every completeness stub that is not
// overridden by the accuracy dispatch table and asserts HTTP 200.
func TestCompleteness_StubOperations(t *testing.T) {
	t.Parallel()

	// Completeness stubs that are NOT overridden by accuracy dispatch.
	// Each is a no-op returning an empty 200 OK response.
	// Ops still returning HTTP 200 with arbitrary/empty inputs (no pool validation required).
	// Ops with real stateful backends (requiring valid UserPoolId) are tested in completeness_impl_test.go.
	stubs := []string{
		"AdminListDevices",
		"AdminSetUserSettings",
		"AdminUpdateAuthEventFeedback",
		"AdminUpdateDeviceStatus",
		"ConfirmDevice",
		"DeleteWebAuthnCredential",
		"DescribeUserPoolDomain",
		"ForgetDevice",
		"GetCSVHeader",
		"GetDevice",
		"GetUserAuthFactors",
		"ListDevices",
		"ListTagsForResource",
		"ListWebAuthnCredentials",
		"SetUserSettings",
		"StartWebAuthnRegistration",
		"TagResource",
		"UntagResource",
		"UpdateAuthEventFeedback",
		"UpdateDeviceStatus",
	}

	tests := make([]struct {
		name string
	}, len(stubs))
	for i, s := range stubs {
		tests[i].name = s
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doCognitoRequest(t, h, tt.name, map[string]any{
				"UserPoolId": "any",
				"Username":   "any",
			})
			assert.Equal(t, http.StatusOK, rec.Code, "action %s", tt.name)
		})
	}
}

// TestHandler_AdminUserGlobalSignOut_Via_HTTP covers the HTTP handler for AdminUserGlobalSignOut.
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

// TestHandler_UpdateGroup_Via_HTTP covers the HTTP handler for UpdateGroup.
func TestHandler_UpdateGroup_Via_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "group_not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "upd-grp-pool")

			groupName := "my-group"

			if tt.name == "success" {
				doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  groupName,
				})
			}

			rec := doCognitoRequest(t, h, "UpdateGroup", map[string]any{
				"UserPoolId":  poolID,
				"GroupName":   groupName,
				"Description": "updated",
				"Precedence":  int32(10),
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_GetSigningCertificate covers the HTTP handler for GetSigningCertificate.
func TestHandler_GetSigningCertificate(t *testing.T) {
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

			poolID := "bad-pool"
			if tt.name == "success" {
				id, _ := setupHandlerPoolAndClient(t, h, "cert-pool")
				poolID = id
			}

			rec := doCognitoRequest(t, h, "GetSigningCertificate", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_AdminGetDevice_Validation covers the HTTP handler for AdminGetDevice.
// Devices are never persisted, so a valid pool/user/device key still resolves to a
// ResourceNotFoundException, while unknown pools/users are rejected up front.
func TestHandler_AdminGetDevice_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "getdevice-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "device-user")

	tests := []struct {
		name     string
		poolID   string
		username string
		device   string
		wantCode int
	}{
		{
			name:     "pool_not_found",
			poolID:   "bad-pool",
			username: "device-user",
			device:   "dk",
			wantCode: http.StatusBadRequest,
		},
		{name: "user_not_found", poolID: poolID, username: "ghost", device: "dk", wantCode: http.StatusBadRequest},
		{
			name:     "device_not_found",
			poolID:   poolID,
			username: "device-user",
			device:   "dk",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doCognitoRequest(t, h, "AdminGetDevice", map[string]any{
				"UserPoolId": tt.poolID,
				"Username":   tt.username,
				"DeviceKey":  tt.device,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_AdminListUserAuthEvents_Validation covers the HTTP handler for
// AdminListUserAuthEvents: a valid pool/user returns an empty AuthEvents list, while
// unknown pools/users are rejected.
func TestHandler_AdminListUserAuthEvents_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "authevents-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "ae-user")

	t.Run("success_empty", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
			"UserPoolId": poolID,
			"Username":   "ae-user",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			AuthEvents []map[string]any `json:"AuthEvents"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Empty(t, resp.AuthEvents)
	})

	t.Run("user_not_found", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
			"UserPoolId": poolID,
			"Username":   "ghost",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_AdminLinkProviderForUser_Links covers the HTTP handler for
// AdminLinkProviderForUser, verifying the external identity is recorded on the
// destination user.
func TestHandler_AdminLinkProviderForUser_Links(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "link-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "native-user")

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
			"UserPoolId": poolID,
			"DestinationUser": map[string]any{
				"ProviderName":           "Cognito",
				"ProviderAttributeValue": "native-user",
			},
			"SourceUser": map[string]any{
				"ProviderName":           "Google",
				"ProviderAttributeName":  "Cognito_Subject",
				"ProviderAttributeValue": "google-12345",
			},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("destination_user_not_found", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
			"UserPoolId": poolID,
			"DestinationUser": map[string]any{
				"ProviderName":           "Cognito",
				"ProviderAttributeValue": "ghost",
			},
			"SourceUser": map[string]any{
				"ProviderName":           "Google",
				"ProviderAttributeValue": "google-99999",
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_CompleteWebAuthnRegistration_Validation covers the HTTP handler for
// CompleteWebAuthnRegistration: an invalid access token is rejected, a valid token with
// a credential payload succeeds (validation-only — no passkey is persisted).
func TestHandler_CompleteWebAuthnRegistration_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "webauthn-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "wa-user")

	initRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": "wa-user",
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, initRec.Code)

	var initResp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken,omitempty"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
	require.NotNil(t, initResp.AuthenticationResult)
	accessToken := initResp.AuthenticationResult.AccessToken
	require.NotEmpty(t, accessToken)

	t.Run("invalid_token", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
			"AccessToken": "not-a-real-token",
			"Credential":  map[string]any{"id": "abc"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
			"AccessToken": accessToken,
			"Credential":  map[string]any{"id": "abc", "type": "public-key"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
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

// TestHandler_GetGroup covers the HTTP handler for GetGroup.
func TestHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "get-grp-pool")

			groupName := "target-group"

			if tt.name == "success" {
				doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  groupName,
				})
			}

			rec := doCognitoRequest(t, h, "GetGroup", map[string]any{
				"UserPoolId": poolID,
				"GroupName":  groupName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
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

// TestBackend_ListUsers covers the backend ListUsers function.
func TestBackend_ListUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "success_empty"},
		{name: "with_users"},
		{name: "pool_not_found", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "pool_not_found" {
				_, err := b.ListUsers("bad-pool")
				require.Error(t, err)

				return
			}

			pool, err := b.CreateUserPool("list-users-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "lc")
			require.NoError(t, err)

			if tt.name == "with_users" {
				for _, name := range []string{"alice", "bob"} {
					user, err2 := b.SignUp(client.ClientID, name, "Pass1234!", map[string]string{})
					require.NoError(t, err2)
					require.NoError(t, b.ConfirmSignUp(client.ClientID, name, user.ConfirmCode))
				}
			}

			users, err := b.ListUsers(pool.ID)
			require.NoError(t, err)

			if tt.name == "with_users" {
				assert.Len(t, users, 2)
			} else {
				assert.Empty(t, users)
			}
		})
	}
}

// TestBackend_DeleteUser covers the backend DeleteUser function.
func TestBackend_DeleteUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{name: "bad_token", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "bad_token" {
				err := b.DeleteUser("bad-token")
				require.Error(t, err)

				return
			}

			pool, err := b.CreateUserPool("del-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "dc")
			require.NoError(t, err)

			user, err := b.SignUp(client.ClientID, "del-me", "Pass1234!", map[string]string{})
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "del-me", user.ConfirmCode))

			result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "del-me", "Pass1234!")
			require.NoError(t, err)
			require.NotNil(t, result.Tokens)

			err = b.DeleteUser(result.Tokens.AccessToken)
			require.NoError(t, err)

			assert.Equal(t, 0, b.UserCount())
		})
	}
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
