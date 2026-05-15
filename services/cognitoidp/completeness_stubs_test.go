package cognitoidp_test

// completeness_stubs_test.go exercises all stub handlers in handler_completeness.go
// that are NOT overridden by the accuracy dispatch table. Each stub returns HTTP 200
// with an empty JSON response body. One request per stub is sufficient for coverage.

import (
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
	stubs := []string{
		"AdminGetDevice",
		"AdminLinkProviderForUser",
		"AdminListDevices",
		"AdminListUserAuthEvents",
		"AdminSetUserSettings",
		"AdminUpdateAuthEventFeedback",
		"AdminUpdateDeviceStatus",
		"CompleteWebAuthnRegistration",
		"ConfirmDevice",
		"CreateIdentityProvider",
		"CreateManagedLoginBranding",
		"CreateTerms",
		"CreateUserImportJob",
		"CreateUserPoolDomain",
		"DeleteIdentityProvider",
		"DeleteManagedLoginBranding",
		"DeleteTerms",
		"DeleteUserPoolClientSecret",
		"DeleteUserPoolDomain",
		"DeleteWebAuthnCredential",
		"DescribeIdentityProvider",
		"DescribeManagedLoginBranding",
		"DescribeManagedLoginBrandingByClient",
		"DescribeRiskConfiguration",
		"DescribeTerms",
		"DescribeUserImportJob",
		"DescribeUserPoolDomain",
		"ForgetDevice",
		"GetCSVHeader",
		"GetDevice",
		"GetIdentityProviderByIdentifier",
		"GetLogDeliveryConfiguration",
		"GetTokensFromRefreshToken",
		"GetUICustomization",
		"GetUserAttributeVerificationCode",
		"GetUserAuthFactors",
		"ListDevices",
		"ListIdentityProviders",
		"ListTagsForResource",
		"ListTerms",
		"ListUserImportJobs",
		"ListUserPoolClientSecrets",
		"ListWebAuthnCredentials",
		"SetLogDeliveryConfiguration",
		"SetRiskConfiguration",
		"SetUICustomization",
		"SetUserSettings",
		"StartUserImportJob",
		"StartWebAuthnRegistration",
		"StopUserImportJob",
		"TagResource",
		"UntagResource",
		"UpdateAuthEventFeedback",
		"UpdateDeviceStatus",
		"UpdateIdentityProvider",
		"UpdateManagedLoginBranding",
		"UpdateTerms",
		"UpdateUserPoolDomain",
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
			_, clientID := setupHandlerPoolAndClient(t, h, "verify-attr-pool")

			var accessToken string

			if tt.name == "success" {
				signUpAndConfirmViaHandler(t, h, clientID, "verify-attr-user")
				accessToken = loginViaHandler(t, h, clientID, "verify-attr-user")
			} else {
				accessToken = "bad-token"
			}

			rec := doCognitoRequest(t, h, "VerifyUserAttribute", map[string]any{
				"AccessToken":   accessToken,
				"AttributeName": "email",
				"Code":          "123456",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
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
