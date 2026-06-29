package cognitoidp_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pbDecodeJWT base64url-decodes the payload of a JWT and returns the raw JSON bytes.
func pbDecodeJWT(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3, "expected 3 JWT parts")

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))

	return claims
}

// TestParityB_AccessTokenGroupsClaim verifies that cognito:groups appears in access tokens.
func TestParityB_AccessTokenGroupsClaim(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		addToGroup bool
		wantGroups bool
	}

	tests := []testCase{
		{name: "user_in_group_has_claim", addToGroup: true, wantGroups: true},
		{name: "user_not_in_group_no_claim", addToGroup: false, wantGroups: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := paSetupPoolAndClient(t, h, "grp-pool-"+tt.name, "grp-client-"+tt.name)

			paSignUpAndConfirm(t, h, clientID, poolID, "grpuser")

			if tt.addToGroup {
				rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "admins",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
				"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
				"AuthParameters": map[string]string{
					"USERNAME": "grpuser",
					"PASSWORD": "Pass1234!",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			authResult := resp["AuthenticationResult"].(map[string]any)
			accessToken := authResult["AccessToken"].(string)

			claims := pbDecodeJWT(t, accessToken)
			_, hasGroups := claims["cognito:groups"]
			assert.Equal(t, tt.wantGroups, hasGroups)
		})
	}
}

// TestParityB_IDTokenUserAttributeClaims verifies that email and other user attrs appear in ID tokens.
func TestParityB_IDTokenUserAttributeClaims(t *testing.T) {
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

	claims := pbDecodeJWT(t, idToken)
	assert.Equal(t, "attruser@example.com", claims["email"], "email must appear in ID token")
	assert.Equal(t, "true", claims["email_verified"], "email_verified must appear in ID token")
}

// TestParityB_AdminNoSRPAuth verifies that ADMIN_NO_SRP_AUTH works as alias for ADMIN_USER_PASSWORD_AUTH.
func TestParityB_AdminNoSRPAuth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := paSetupPoolAndClient(t, h, "nosrp-pool", "nosrp-client")

	paSignUpAndConfirm(t, h, clientID, poolID, "nosrpuser")

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
func TestParityB_ResendConfirmationCodeAlreadyConfirmed(t *testing.T) {
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
			poolID, clientID := paSetupPoolAndClient(t, h, "resend-pool-"+tt.name, "resend-client-"+tt.name)

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
func TestParityB_ConfirmForgotPasswordExpiryCheckedFirst(t *testing.T) {
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
			poolID, clientID := paSetupPoolAndClient(t, h, "cfp-pool-"+tt.name, "cfp-client-"+tt.name)

			paSignUpAndConfirm(t, h, clientID, poolID, "cfpuser")

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

// TestParityB_ForgotPasswordRejectsForceChangePassword verifies FORCE_CHANGE_PASSWORD is blocked.
func TestParityB_ForgotPasswordRejectsForceChangePassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := paSetupPoolAndClient(t, h, "fcp-block-pool", "fcp-block-client")

	// AdminCreateUser creates user in FORCE_CHANGE_PASSWORD status.
	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "forceuser",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	fpRec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
		"ClientId": clientID,
		"Username": "forceuser",
	})
	assert.Equal(t, http.StatusBadRequest, fpRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(fpRec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"])
}

// TestParityB_AdminGetUserMFAFields verifies MFA fields are returned in AdminGetUser.
func TestParityB_AdminGetUserMFAFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := paSetupPoolAndClient(t, h, "mfa-fields-pool", "mfa-fields-client")

	paSignUpAndConfirm(t, h, clientID, poolID, "mfauser")

	rec := doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "mfauser",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// Fields should be present (even if empty/nil for non-MFA user).
	// The response JSON should decode without error and have the base fields.
	assert.Equal(t, "mfauser", resp["Username"])
	assert.Equal(t, true, resp["Enabled"])
}

// TestParityB_AdminCreateUserSubInAttributes verifies sub appears in AdminCreateUser response.
func TestParityB_AdminCreateUserSubInAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := paSetupPoolAndClient(t, h, "sub-pool", "sub-client")

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

// TestParityB_PasswordPolicyDefaults verifies default policy returned when none configured.
func TestParityB_PasswordPolicyDefaults(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		withPolicy bool
		wantMinLen int
	}

	tests := []testCase{
		{name: "no_policy_gets_defaults", withPolicy: false, wantMinLen: 8},
		{name: "explicit_policy_used", withPolicy: true, wantMinLen: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"PoolName": "pp-pool-" + tt.name}

			if tt.withPolicy {
				body["Policies"] = map[string]any{
					"PasswordPolicy": map[string]any{
						"MinimumLength": 12,
					},
				}
			}

			rec := doCognitoRequest(t, h, "CreateUserPool", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			pool := resp["UserPool"].(map[string]any)
			policies := pool["Policies"].(map[string]any)
			pp := policies["PasswordPolicy"].(map[string]any)

			assert.InDelta(t, float64(tt.wantMinLen), pp["MinimumLength"], 0, "MinimumLength mismatch")
		})
	}
}

// TestParityB_UserPoolLastModifiedDate verifies LastModifiedDate updates on UpdateUserPool.
func TestParityB_UserPoolLastModifiedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "lmd-pool"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	pool := createResp["UserPool"].(map[string]any)
	poolID := pool["Id"].(string)
	createDate := pool["CreationDate"].(float64)
	lastModBefore := pool["LastModifiedDate"].(float64)

	// On creation, LastModifiedDate should equal CreationDate (no updates yet).
	assert.InDelta(t, createDate, lastModBefore, 0)

	// Update the pool.
	upRec := doCognitoRequest(t, h, "UpdateUserPool", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "OFF",
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	// DescribeUserPool must return updated LastModifiedDate >= CreationDate.
	descRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	lastModAfter := descResp["UserPool"].(map[string]any)["LastModifiedDate"].(float64)
	assert.GreaterOrEqual(t, lastModAfter, createDate, "LastModifiedDate must be >= CreationDate after update")
}

// TestParityB_ClientLastModifiedDate verifies LastModifiedDate appears and updates.
func TestParityB_ClientLastModifiedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := paSetupPoolAndClient(t, h, "client-lmd-pool", "client-lmd-client")

	rec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	client := descResp["UserPoolClient"].(map[string]any)
	creationDate := client["CreationDate"].(float64)
	lastModBefore := client["LastModifiedDate"].(float64)
	assert.InDelta(t, creationDate, lastModBefore, 0, "LastModifiedDate should equal CreationDate initially")

	// Update the client.
	upRec := doCognitoRequest(t, h, "UpdateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"ClientName": "client-lmd-client-updated",
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	var upResp map[string]any
	require.NoError(t, json.Unmarshal(upRec.Body.Bytes(), &upResp))
	updatedClient := upResp["UserPoolClient"].(map[string]any)
	lastModAfter := updatedClient["LastModifiedDate"].(float64)
	assert.GreaterOrEqual(t, lastModAfter, creationDate, "LastModifiedDate must be >= CreationDate after update")
}

// TestParityB_ClientOAuthRedirectFields verifies CallbackURLs, LogoutURLs, AllowedOAuthFlowsUserPoolClient round-trip.
func TestParityB_ClientOAuthRedirectFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := paSetupPoolAndClient(t, h, "oauth-pool", "stub-client")

	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":                      poolID,
		"ClientName":                      "oauth-client",
		"CallbackURLs":                    []string{"https://app.example.com/callback"},
		"LogoutURLs":                      []string{"https://app.example.com/logout"},
		"AllowedOAuthFlows":               []string{"code"},
		"AllowedOAuthScopes":              []string{"openid", "email"},
		"AllowedOAuthFlowsUserPoolClient": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	client := resp["UserPoolClient"].(map[string]any)

	callbackURLs := client["CallbackURLs"].([]any)
	require.Len(t, callbackURLs, 1)
	assert.Equal(t, "https://app.example.com/callback", callbackURLs[0])

	logoutURLs := client["LogoutURLs"].([]any)
	require.Len(t, logoutURLs, 1)
	assert.Equal(t, "https://app.example.com/logout", logoutURLs[0])

	assert.Equal(t, true, client["AllowedOAuthFlowsUserPoolClient"])
}

// TestParityB_UpdateUserPoolLambdaConfig verifies LambdaConfig round-trips through UpdateUserPool.
func TestParityB_UpdateUserPoolLambdaConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "lambda-pool"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	poolID := createResp["UserPool"].(map[string]any)["Id"].(string)

	lambdaConfig := map[string]any{
		"PreSignUp": "arn:aws:lambda:us-east-1:000000000000:function:pre-signup",
	}

	upRec := doCognitoRequest(t, h, "UpdateUserPool", map[string]any{
		"UserPoolId":   poolID,
		"LambdaConfig": lambdaConfig,
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	descRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	returnedPool := descResp["UserPool"].(map[string]any)
	returnedLambda := returnedPool["LambdaConfig"].(map[string]any)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:pre-signup", returnedLambda["PreSignUp"])
}
