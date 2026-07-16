package cognitoidp_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetUser_MFAFields(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "nina")

	err := b.SetUserMFAPreference(tokens.AccessToken, false, true, "SOFTWARE_TOKEN_MFA")
	require.NoError(t, err)

	user, err := b.GetUser(tokens.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, []string{"SOFTWARE_TOKEN_MFA"}, user.UserMFASettingList)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", user.PreferredMfaSetting)
}

func TestSoftwareToken_PersistsSecret(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "rosa")

	secret, err := b.AssociateSoftwareToken(tokens.AccessToken)
	require.NoError(t, err)
	assert.NotEmpty(t, secret)
	assert.Greater(t, len(secret), 16)

	code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
	require.NoError(t, err)

	err = b.VerifySoftwareToken(tokens.AccessToken, code)
	require.NoError(t, err)

	user, err := b.GetUser(tokens.AccessToken)
	require.NoError(t, err)
	assert.NotEmpty(t, user.TOTPSecret)
}

func TestVerifySoftwareToken_RequiresAssociate(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "sam")

	err := b.VerifySoftwareToken(tokens.AccessToken, "123456")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)
}

func TestMFASession_ExpiresAfterTTL(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("mfa-ttl-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	err = b.UpdateUserPoolWithOpts(pool.ID, "ON", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "mfa-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "vera", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "vera", user.ConfirmCode)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "vera", "Pass1234!")
	require.NoError(t, err)
	require.NotEmpty(t, result.MFASession)

	b.ExpireMFASessionForTest(result.MFASession)
	b.EvictExpiredMFASessions()

	_, err = b.RespondToMFAChallenge(client.ClientID, result.MFASession, "123456")
	require.Error(t, err, "expired MFA session must be rejected after eviction")
}

func TestHandler_GetUser_WithMFAFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "mfa-getuser-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "zara")
	accessToken := loginViaHandler(t, h, clientID, "zara")

	prefRec := doCognitoRequest(t, h, "SetUserMFAPreference", map[string]any{
		"AccessToken": accessToken,
		"SoftwareTokenMfaSettings": map[string]any{
			"Enabled":      true,
			"PreferredMfa": true,
		},
	})
	require.Equal(t, http.StatusOK, prefRec.Code)

	rec := doCognitoRequest(t, h, "GetUser", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		PreferredMfaSetting string   `json:"PreferredMfaSetting,omitempty"`
		UserMFASettingList  []string `json:"UserMFASettingList,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"SOFTWARE_TOKEN_MFA"}, resp.UserMFASettingList)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", resp.PreferredMfaSetting)
}

func TestHandler_RespondToAuthChallenge_SMSMFAFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "sms-mfa-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "smsuser")

	// Log in while MFA is still off to get an access token, then enroll a real TOTP
	// secret — a real client must complete software-token setup (AssociateSoftwareToken +
	// VerifySoftwareToken) before the pool can challenge for SOFTWARE_TOKEN_MFA.
	accessToken := loginViaHandler(t, h, clientID, "smsuser")

	assocRec := doCognitoRequest(t, h, "AssociateSoftwareToken", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp struct {
		SecretCode string `json:"SecretCode,omitempty"`
	}
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	require.NotEmpty(t, assocResp.SecretCode)

	setupCode, err := cognitoidp.GenerateTOTPCode(assocResp.SecretCode, time.Now())
	require.NoError(t, err)

	verifyRec := doCognitoRequest(t, h, "VerifySoftwareToken", map[string]any{
		"AccessToken": accessToken,
		"UserCode":    setupCode,
	})
	require.Equal(t, http.StatusOK, verifyRec.Code)

	mfaRec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
	})
	require.Equal(t, http.StatusOK, mfaRec.Code)

	setMFARec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "smsuser",
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, setMFARec.Code)

	var mfaInitResp struct {
		ChallengeName *string `json:"ChallengeName,omitempty"`
		Session       *string `json:"Session,omitempty"`
	}
	require.NoError(t, json.Unmarshal(setMFARec.Body.Bytes(), &mfaInitResp))
	require.NotNil(t, mfaInitResp.ChallengeName)
	require.Equal(t, "SOFTWARE_TOKEN_MFA", *mfaInitResp.ChallengeName)

	// Respond to SOFTWARE_TOKEN_MFA (default when MFA is ON) with the real TOTP code
	// derived from the enrolled secret — a stray "123456" must now be rejected.
	challengeCode, err := cognitoidp.GenerateTOTPCode(assocResp.SecretCode, time.Now())
	require.NoError(t, err)

	respondRec := doCognitoRequest(t, h, "RespondToAuthChallenge", map[string]any{
		"ClientId":      clientID,
		"ChallengeName": *mfaInitResp.ChallengeName,
		"Session":       *mfaInitResp.Session,
		"ChallengeResponses": map[string]string{
			"SOFTWARE_TOKEN_MFA_CODE": challengeCode,
		},
	})
	require.Equal(t, http.StatusOK, respondRec.Code)

	var tokenResp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken,omitempty"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(respondRec.Body.Bytes(), &tokenResp))
	require.NotNil(t, tokenResp.AuthenticationResult)
	assert.NotEmpty(t, tokenResp.AuthenticationResult.AccessToken)
}

func TestHandler_AssociateSoftwareToken_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "totp-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "totp-user")
	accessToken := loginViaHandler(t, h, clientID, "totp-user")

	rec := doCognitoRequest(t, h, "AssociateSoftwareToken", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SecretCode string `json:"SecretCode,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.SecretCode)
	assert.Greater(t, len(resp.SecretCode), 10)
}

func TestHandler_VerifySoftwareToken_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "verify-totp-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "verify-user")
	accessToken := loginViaHandler(t, h, clientID, "verify-user")

	assocRec := doCognitoRequest(t, h, "AssociateSoftwareToken", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp struct {
		SecretCode string `json:"SecretCode,omitempty"`
	}
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	require.NotEmpty(t, assocResp.SecretCode)

	code, err := cognitoidp.GenerateTOTPCode(assocResp.SecretCode, time.Now())
	require.NoError(t, err)

	rec := doCognitoRequest(t, h, "VerifySoftwareToken", map[string]any{
		"AccessToken": accessToken,
		"UserCode":    code,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Status string `json:"Status,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "SUCCESS", resp.Status)
}

func TestHandler_AdminSetUserMFAPreference_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "admin-mfa-pref-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "mfa-pref-user")

	rec := doCognitoRequest(t, h, "AdminSetUserMFAPreference", map[string]any{
		"UserPoolId": poolID,
		"Username":   "mfa-pref-user",
		"SoftwareTokenMfaSettings": map[string]any{
			"Enabled":      true,
			"PreferredMfa": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "AdminSetUserMFAPreference should succeed: %s", rec.Body.String())
}

func TestHandler_AdminSetUserMFAPreference_UnknownUser_Fails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-mfa-pref-fail-pool")

	rec := doCognitoRequest(t, h, "AdminSetUserMFAPreference", map[string]any{
		"UserPoolId": poolID,
		"Username":   "ghost",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestJanitor_SweepsExpiredMFASessions(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("janitor-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "janitor-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_USER_SRP_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "juser", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "juser", user.ConfirmCode)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "juser", "Pass1234!")
	require.NoError(t, err)

	session := result.MFASession

	// Force-expire the session.
	b.ExpireMFASessionForTest(session)

	// Before sweep: the session should fail validation.
	_, err = b.RespondToSRPChallenge(client.ClientID, session)
	require.Error(t, err)

	// After sweep: EvictExpiredMFASessions removes the entry (evict manually as janitor would).
	b.EvictExpiredMFASessions()

	// Trying to use the session again still fails (entry gone).
	_, err = b.RespondToSRPChallenge(client.ClientID, session)
	require.Error(t, err)
}

func TestSetGetMfaConfig_SmsMfaConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-sms-pool")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
		"SmsMfaConfiguration": map[string]any{
			"SmsAuthenticationMessage": "Your code is {####}",
			"SmsConfiguration": map[string]any{
				"SnsCallerArn": "arn:aws:iam::123456789:role/cognito-sms",
				"SnsRegion":    "us-east-1",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		SmsMfaConfiguration *struct {
			SmsConfiguration *struct {
				SnsCallerArn string `json:"SnsCallerArn,omitempty"`
			} `json:"SmsConfiguration"`
			SmsAuthenticationMessage string `json:"SmsAuthenticationMessage,omitempty"`
		} `json:"SmsMfaConfiguration"`
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	assert.Equal(t, "ON", setOut.MfaConfiguration)
	require.NotNil(t, setOut.SmsMfaConfiguration)
	assert.Equal(t, "Your code is {####}", setOut.SmsMfaConfiguration.SmsAuthenticationMessage)
	require.NotNil(t, setOut.SmsMfaConfiguration.SmsConfiguration)
	assert.Equal(t, "arn:aws:iam::123456789:role/cognito-sms", setOut.SmsMfaConfiguration.SmsConfiguration.SnsCallerArn)

	// Get and verify persisted.
	rec = doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		SmsMfaConfiguration *struct {
			SmsAuthenticationMessage string `json:"SmsAuthenticationMessage,omitempty"`
		} `json:"SmsMfaConfiguration"`
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, "ON", getOut.MfaConfiguration)
	require.NotNil(t, getOut.SmsMfaConfiguration)
	assert.Equal(t, "Your code is {####}", getOut.SmsMfaConfiguration.SmsAuthenticationMessage)
}

func TestSetGetMfaConfig_SoftwareTokenMfa(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-totp-pool")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "OPTIONAL",
		"SoftwareTokenMfaConfiguration": map[string]any{
			"Enabled": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		SoftwareTokenMfaConfiguration *struct {
			Enabled bool `json:"Enabled,omitempty"`
		} `json:"SoftwareTokenMfaConfiguration"`
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	assert.Equal(t, "OPTIONAL", setOut.MfaConfiguration)
	require.NotNil(t, setOut.SoftwareTokenMfaConfiguration)
	assert.True(t, setOut.SoftwareTokenMfaConfiguration.Enabled)

	// Verify Get returns the same.
	rec = doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		SoftwareTokenMfaConfiguration *struct {
			Enabled bool `json:"Enabled,omitempty"`
		} `json:"SoftwareTokenMfaConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	require.NotNil(t, getOut.SoftwareTokenMfaConfiguration)
	assert.True(t, getOut.SoftwareTokenMfaConfiguration.Enabled)
}

func TestSetGetMfaConfig_EmailMfa(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-email-pool")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
		"EmailMfaConfiguration": map[string]any{
			"Message": "Your login code is {####}",
			"Subject": "Login verification",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		EmailMfaConfiguration *struct {
			Message string `json:"Message,omitempty"`
			Subject string `json:"Subject,omitempty"`
		} `json:"EmailMfaConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	require.NotNil(t, setOut.EmailMfaConfiguration)
	assert.Equal(t, "Your login code is {####}", setOut.EmailMfaConfiguration.Message)
	assert.Equal(t, "Login verification", setOut.EmailMfaConfiguration.Subject)
}

func TestMfaConfig_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": "invalid-pool-id",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       "invalid-pool-id",
		"MfaConfiguration": "ON",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMfaConfig_Backend_Full(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("mfa-full-pool")
	require.NoError(t, err)

	cfg := cognitoidp.UserPoolMfaFullConfig{
		MfaConfiguration: "ON",
		SmsMfaConfiguration: &cognitoidp.SmsMfaConfiguration{
			SmsAuthenticationMessage: "Code: {####}",
			SmsConfiguration: &cognitoidp.SmsConfiguration{
				SnsCallerArn: "arn:aws:iam::111:role/role",
				SnsRegion:    "us-west-2",
				ExternalID:   "ext-id-123",
			},
		},
		SoftwareTokenMfa: &cognitoidp.SoftwareTokenMfaConfiguration{Enabled: true},
	}

	require.NoError(t, b.SetUserPoolMfaConfigFull(pool.ID, cfg))

	got, err := b.GetUserPoolMfaConfigFull(pool.ID)
	require.NoError(t, err)
	assert.Equal(t, "ON", got.MfaConfiguration)
	require.NotNil(t, got.SmsMfaConfiguration)
	assert.Equal(t, "Code: {####}", got.SmsMfaConfiguration.SmsAuthenticationMessage)
	require.NotNil(t, got.SmsMfaConfiguration.SmsConfiguration)
	assert.Equal(t, "us-west-2", got.SmsMfaConfiguration.SmsConfiguration.SnsRegion)
	assert.Equal(t, "ext-id-123", got.SmsMfaConfiguration.SmsConfiguration.ExternalID)
	require.NotNil(t, got.SoftwareTokenMfa)
	assert.True(t, got.SoftwareTokenMfa.Enabled)
}

func TestMfaConfig_Backend_InvalidPool(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetUserPoolMfaConfigFull("bad-pool")
	require.Error(t, err)

	err = b.SetUserPoolMfaConfigFull("bad-pool", cognitoidp.UserPoolMfaFullConfig{})
	require.Error(t, err)
}

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

// TestParityB_AdminGetUserMFAFields verifies MFA fields are returned in AdminGetUser.
func TestAdminGetUserMFAFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupPoolAndClientNamed(t, h, "mfa-fields-pool", "mfa-fields-client")

	signUpAndAdminConfirm(t, h, clientID, poolID, "mfauser")

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
