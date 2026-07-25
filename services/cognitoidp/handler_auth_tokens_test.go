package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestTokenExpiryFor_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		unit      string
		tokenType string
		wantSecs  float64
		validity  int32
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
				UserPoolClient struct {
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
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
