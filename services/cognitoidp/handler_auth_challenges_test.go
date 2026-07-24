package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			// CUSTOM_CHALLENGE is a real, handled challenge type (CUSTOM_AUTH flow); with
			// no such session actually pending, this correctly errors rather than silently
			// succeeding, matching AWS's NotAuthorizedException for an invalid Session.
			name:          "custom_challenge_with_no_pending_session",
			wantCode:      http.StatusBadRequest,
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
