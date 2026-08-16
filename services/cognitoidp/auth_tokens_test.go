package cognitoidp_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestAccessToken_ScopeDerived(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("scope-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "scoped-client", cognitoidp.UserPoolClientOptions{
		AllowedOAuthScopes: []string{"email", "openid"},
	})
	require.NoError(t, err)

	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "carol")

	claims := decodeJWTPayload(t, tokens.AccessToken)
	scope, _ := claims["scope"].(string)
	assert.Equal(t, "email openid", scope, "scope must be sorted space-joined AllowedOAuthScopes")
}

func TestAccessToken_DefaultScopeWhenNoneConfigured(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "eve")

	claims := decodeJWTPayload(t, tokens.AccessToken)
	scope, _ := claims["scope"].(string)
	assert.Equal(t, "aws.cognito.signin.user.admin", scope)
}

func TestIDToken_HasAuthTime(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "judy")

	claims := decodeJWTPayload(t, tokens.IDToken)
	_, ok := claims["auth_time"].(float64)
	assert.True(t, ok, "auth_time must be present in ID token")
}

func TestAccessToken_HasAuthTime(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "kate")

	claims := decodeJWTPayload(t, tokens.AccessToken)
	_, ok := claims["auth_time"].(float64)
	assert.True(t, ok, "auth_time must be present in access token")
}

func TestRefreshToken_Rotates(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "leo")

	first := tokens.RefreshToken

	tokens2, err := b.InitiateAuthRefreshToken(client.ClientID, first)
	require.NoError(t, err)
	assert.NotEqual(t, first, tokens2.RefreshToken, "refresh token must rotate")

	_, err = b.InitiateAuthRefreshToken(client.ClientID, first)
	require.Error(t, err, "old refresh token must be invalidated after rotation")
}

func TestTokenClaims_Shape(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "mary")

	idClaims := decodeJWTPayload(t, tokens.IDToken)
	assert.Equal(t, "id", idClaims["token_use"])
	assert.Equal(t, client.ClientID, idClaims["aud"])
	assert.NotEmpty(t, idClaims["sub"])
	assert.NotEmpty(t, idClaims["iss"])
	_, hasClientID := idClaims["client_id"]
	assert.False(t, hasClientID, "ID token must not include client_id")

	accClaims := decodeJWTPayload(t, tokens.AccessToken)
	assert.Equal(t, "access", accClaims["token_use"])
	assert.Equal(t, client.ClientID, accClaims["client_id"])
	_, hasAud := accClaims["aud"]
	assert.False(t, hasAud, "access token must not include aud")
}

// testPassword is the standard password used in audit tests.
const testPassword = "Passw0rd!"

// initiateAuth does USER_PASSWORD_AUTH with testPassword and returns the TokenResult.
func initiateAuth(t *testing.T, h *cognitoidp.Handler, clientID, username string) map[string]any {
	t.Helper()

	rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": username,
			"PASSWORD": testPassword,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "InitiateAuth: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// jwtClaims base64-decodes the payload section of a JWT.
func jwtClaims(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3, "JWT must have 3 parts")

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err, "base64 decode JWT payload")

	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))

	return claims
}

func TestTokenValidity_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		units                map[string]any
		name                 string
		accessTokenValidity  int32
		idTokenValidity      int32
		refreshTokenValidity int32
	}{
		{
			name:                 "defaults_zero",
			accessTokenValidity:  0,
			idTokenValidity:      0,
			refreshTokenValidity: 0,
		},
		{
			name:                 "hours_unit",
			accessTokenValidity:  2,
			idTokenValidity:      2,
			refreshTokenValidity: 7,
			units: map[string]any{
				"AccessToken":  "hours",
				"IdToken":      "hours",
				"RefreshToken": "days",
			},
		},
		{
			name:                 "minutes_unit",
			accessTokenValidity:  30,
			idTokenValidity:      60,
			refreshTokenValidity: 1,
			units: map[string]any{
				"AccessToken":  "minutes",
				"IdToken":      "minutes",
				"RefreshToken": "days",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "validity-pool-"+tt.name)

			createBody := map[string]any{
				"UserPoolId":           poolID,
				"ClientName":           "validity-client-" + tt.name,
				"AccessTokenValidity":  tt.accessTokenValidity,
				"IdTokenValidity":      tt.idTokenValidity,
				"RefreshTokenValidity": tt.refreshTokenValidity,
			}
			if tt.units != nil {
				createBody["TokenValidityUnits"] = tt.units
			}

			rec := doCognitoRequest(t, h, "CreateUserPoolClient", createBody)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var createResp struct {
				UserPoolClient struct {
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
					ClientID             string         `json:"ClientId"`
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.Equal(t, tt.accessTokenValidity, createResp.UserPoolClient.AccessTokenValidity)
			assert.Equal(t, tt.idTokenValidity, createResp.UserPoolClient.IDTokenValidity)
			assert.Equal(t, tt.refreshTokenValidity, createResp.UserPoolClient.RefreshTokenValidity)
			if tt.units != nil {
				for k, v := range tt.units {
					assert.Equal(t, v, createResp.UserPoolClient.TokenValidityUnits[k])
				}
			}

			// Verify DescribeUserPoolClient returns same values.
			descRec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   createResp.UserPoolClient.ClientID,
			})
			require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

			var descResp struct {
				UserPoolClient struct {
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.accessTokenValidity, descResp.UserPoolClient.AccessTokenValidity)
			assert.Equal(t, tt.idTokenValidity, descResp.UserPoolClient.IDTokenValidity)
			assert.Equal(t, tt.refreshTokenValidity, descResp.UserPoolClient.RefreshTokenValidity)
		})
	}
}

func TestTokenValidity_UpdateClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "upd-validity-pool")

	// Create client with no validity settings.
	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "upd-client",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	clientID := createResp.UserPoolClient.ClientID

	// Update with validity settings.
	updRec := doCognitoRequest(t, h, "UpdateUserPoolClient", map[string]any{
		"UserPoolId":          poolID,
		"ClientId":            clientID,
		"AccessTokenValidity": int32(5),
		"IdTokenValidity":     int32(5),
		"TokenValidityUnits": map[string]any{
			"AccessToken": "minutes",
			"IdToken":     "minutes",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())

	var updResp struct {
		UserPoolClient struct {
			TokenValidityUnits  map[string]any `json:"TokenValidityUnits"`
			AccessTokenValidity int32          `json:"AccessTokenValidity"`
			IDTokenValidity     int32          `json:"IdTokenValidity"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updResp))
	assert.Equal(t, int32(5), updResp.UserPoolClient.AccessTokenValidity)
	assert.Equal(t, int32(5), updResp.UserPoolClient.IDTokenValidity)
	assert.Equal(t, "minutes", updResp.UserPoolClient.TokenValidityUnits["AccessToken"])
}

func TestTokenValidity_HonoredInJWT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		unit                string
		wantExpiresIn       float64
		wantMinExpDelta     time.Duration
		wantMaxExpDelta     time.Duration
		accessTokenValidity int32
	}{
		{
			name:                "2_hours",
			accessTokenValidity: 2,
			unit:                "hours",
			wantExpiresIn:       7200,
			wantMinExpDelta:     7190 * time.Second,
			wantMaxExpDelta:     7210 * time.Second,
		},
		{
			name:                "30_minutes",
			accessTokenValidity: 30,
			unit:                "minutes",
			wantExpiresIn:       1800,
			wantMinExpDelta:     1790 * time.Second,
			wantMaxExpDelta:     1810 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "jwt-validity-"+tt.name)

			// Create client with specific access token validity.
			rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId":          poolID,
				"ClientName":          "jwt-client",
				"AccessTokenValidity": tt.accessTokenValidity,
				"TokenValidityUnits": map[string]any{
					"AccessToken": tt.unit,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp struct {
				UserPoolClient struct {
					ClientID string `json:"ClientId"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			clientID := createResp.UserPoolClient.ClientID

			// Create and confirm a user, then authenticate.
			b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
			h2 := cognitoidp.NewHandler(b, "us-east-1")

			pool2ID, client2ID := setupHandlerPoolAndClient(t, h2, "jwt-validity-inner-"+tt.name)

			// Override with validity client.
			rec2 := doCognitoRequest(t, h2, "CreateUserPoolClient", map[string]any{
				"UserPoolId":          pool2ID,
				"ClientName":          "validity-client",
				"AccessTokenValidity": tt.accessTokenValidity,
				"TokenValidityUnits": map[string]any{
					"AccessToken": tt.unit,
				},
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var cr struct {
				UserPoolClient struct {
					ClientID string `json:"ClientId"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &cr))
			validClientID := cr.UserPoolClient.ClientID

			// Sign up user with the validity client.
			signUpRec := doCognitoRequest(t, h2, "SignUp", map[string]any{
				"ClientId": validClientID,
				"Username": "jwtuser",
				"Password": "Passw0rd!",
			})
			require.Equal(t, http.StatusOK, signUpRec.Code)

			adminConfRec := doCognitoRequest(t, h2, "AdminConfirmSignUp", map[string]any{
				"UserPoolId": pool2ID,
				"Username":   "jwtuser",
			})
			require.Equal(t, http.StatusOK, adminConfRec.Code)

			// Initiate auth.
			before := time.Now()
			authRec := doCognitoRequest(t, h2, "InitiateAuth", map[string]any{
				"AuthFlow": "USER_PASSWORD_AUTH",
				"ClientId": validClientID,
				"AuthParameters": map[string]string{
					"USERNAME": "jwtuser",
					"PASSWORD": "Passw0rd!",
				},
			})
			require.Equal(t, http.StatusOK, authRec.Code, authRec.Body.String())

			var authResp struct {
				AuthenticationResult struct {
					AccessToken string  `json:"AccessToken"`
					ExpiresIn   float64 `json:"ExpiresIn"`
				} `json:"AuthenticationResult"`
			}
			require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))

			assert.InDelta(t, tt.wantExpiresIn, authResp.AuthenticationResult.ExpiresIn, 1, "ExpiresIn mismatch")

			claims := jwtClaims(t, authResp.AuthenticationResult.AccessToken)
			exp := time.Unix(int64(claims["exp"].(float64)), 0)
			delta := exp.Sub(before)
			assert.GreaterOrEqual(t, delta, tt.wantMinExpDelta, "JWT exp too small")
			assert.LessOrEqual(t, delta, tt.wantMaxExpDelta, "JWT exp too large")

			// Suppress unused variable warning.
			_ = clientID
			_ = client2ID
		})
	}
}

func TestAuthFlow_RefreshTokenAuth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "refresh-pool")

	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "refreshuser",
		"Password": "Passw0rd!",
	})
	require.Equal(t, http.StatusOK, signUpRec.Code)

	confRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "refreshuser",
	})
	require.Equal(t, http.StatusOK, confRec.Code)

	// Authenticate.
	authResp := initiateAuth(t, h, clientID, "refreshuser")
	authResult, ok := authResp["AuthenticationResult"].(map[string]any)
	require.True(t, ok)
	refreshToken, ok := authResult["RefreshToken"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, refreshToken)

	// Refresh.
	refreshRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	require.Equal(t, http.StatusOK, refreshRec.Code, refreshRec.Body.String())

	var refreshResp map[string]any
	require.NoError(t, json.Unmarshal(refreshRec.Body.Bytes(), &refreshResp))
	result, ok := refreshResp["AuthenticationResult"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, result["AccessToken"])
	assert.NotEmpty(t, result["IdToken"])
}

func TestJWT_Claims(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "jwt-claims-pool")

	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "jwtclaims",
		"Password": "Passw0rd!",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "jwtclaims@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, signUpRec.Code)

	confRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "jwtclaims",
	})
	require.Equal(t, http.StatusOK, confRec.Code)

	authResp := initiateAuth(t, h, clientID, "jwtclaims")
	authResult, ok := authResp["AuthenticationResult"].(map[string]any)
	require.True(t, ok)

	// Access token claims.
	accessClaims := jwtClaims(t, authResult["AccessToken"].(string))
	assert.Equal(t, "access", accessClaims["token_use"])
	assert.Equal(t, clientID, accessClaims["client_id"])
	assert.Equal(t, "jwtclaims", accessClaims["username"])
	assert.NotEmpty(t, accessClaims["sub"])
	assert.NotEmpty(t, accessClaims["iss"])
	assert.NotZero(t, accessClaims["exp"])
	assert.NotZero(t, accessClaims["iat"])

	// ID token claims.
	idClaims := jwtClaims(t, authResult["IdToken"].(string))
	assert.Equal(t, "id", idClaims["token_use"])
	assert.Equal(t, clientID, idClaims["aud"])
	assert.Equal(t, "jwtclaims", idClaims["cognito:username"])
	assert.Equal(t, "jwtclaims@example.com", idClaims["email"])
	assert.NotEmpty(t, idClaims["sub"])
}

func TestTokenRevocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "revoke-pool")

	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "revokeuser",
		"Password": "Passw0rd!",
	})
	require.Equal(t, http.StatusOK, signUpRec.Code)

	confRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "revokeuser",
	})
	require.Equal(t, http.StatusOK, confRec.Code)

	authResp := initiateAuth(t, h, clientID, "revokeuser")
	authResult, ok := authResp["AuthenticationResult"].(map[string]any)
	require.True(t, ok)
	refreshToken := authResult["RefreshToken"].(string)

	// Revoke the refresh token.
	revokeRec := doCognitoRequest(t, h, "RevokeToken", map[string]any{
		"ClientId": clientID,
		"Token":    refreshToken,
	})
	require.Equal(t, http.StatusOK, revokeRec.Code, revokeRec.Body.String())

	// Using the revoked token should fail.
	refreshRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	assert.Equal(t, http.StatusBadRequest, refreshRec.Code)
}

func TestGetTokensFromRefreshToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "refresh-token-pool")

	// Sign up and authenticate to get a refresh token.
	doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "refreshuser",
		"Password": "Pass1234!",
	})
	doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "refreshuser",
	})

	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"ClientId":       clientID,
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{"USERNAME": "refreshuser", "PASSWORD": "Pass1234!"},
	})
	require.Equal(t, http.StatusOK, authRec.Code)

	var authResp struct {
		AuthenticationResult struct {
			RefreshToken string `json:"RefreshToken,omitempty"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	refreshToken := authResp.AuthenticationResult.RefreshToken
	require.NotEmpty(t, refreshToken)

	// GetTokensFromRefreshToken with valid refresh token
	rec := doCognitoRequest(t, h, "GetTokensFromRefreshToken", map[string]any{
		"ClientId":     clientID,
		"RefreshToken": refreshToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var tokenResp struct {
		AuthenticationResult struct {
			AccessToken string `json:"AccessToken,omitempty"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tokenResp))
	assert.NotEmpty(t, tokenResp.AuthenticationResult.AccessToken)
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

func TestAccessToken_ScopesSortedAndDeduped(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("scope-pool")
	require.NoError(t, err)

	opts := cognitoidp.UserPoolClientOptions{
		// Intentionally unsorted and with duplicate.
		AllowedOAuthScopes: []string{"openid", "aws.cognito.signin.user.admin", "email", "openid"},
	}
	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "scope-client", opts)
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "scope-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "scope-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "scope-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	claims := decodeJWTPayload(t, result.Tokens.AccessToken)
	scopeVal, ok := claims["scope"].(string)
	require.True(t, ok, "scope claim must be a string")

	scopes := splitScope(scopeVal)

	// No duplicates.
	seen := make(map[string]struct{})
	for _, s := range scopes {
		_, dup := seen[s]
		assert.False(t, dup, "duplicate scope %q", s)
		seen[s] = struct{}{}
	}

	// Sorted.
	for i := 1; i < len(scopes); i++ {
		assert.LessOrEqual(t, scopes[i-1], scopes[i], "scopes must be sorted")
	}
}

func splitScope(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Fields(s)
}

func TestHandler_RefreshTokenAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (clientID, refreshToken string)
		name     string
		wantCode int
	}{
		{
			name: "valid_refresh_token_rotates",
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

				// Admin-create a confirmed user.
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          "refreshuser",
					"TemporaryPassword": "TempPass123!",
				})
				doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
					"UserPoolId": poolID,
					"Username":   "refreshuser",
					"Password":   "PermPass456!",
					"Permanent":  true,
				})

				// Authenticate to get a refresh token.
				authRec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
					"UserPoolId": poolID,
					"ClientId":   clientID,
					"AuthFlow":   "USER_PASSWORD_AUTH",
					"AuthParameters": map[string]string{
						"USERNAME": "refreshuser",
						"PASSWORD": "PermPass456!",
					},
				})
				var authResp map[string]map[string]any
				_ = json.Unmarshal(authRec.Body.Bytes(), &authResp)
				rt := authResp["AuthenticationResult"]["RefreshToken"].(string)

				return clientID, rt
			},
			wantCode: http.StatusOK,
		},
		{
			name: "invalid_refresh_token_rejected",
			setup: func(h *cognitoidp.Handler) (string, string) {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p2"})
				var poolResp map[string]map[string]any
				_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
				poolID := poolResp["UserPool"]["Id"].(string)

				clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": "c2",
				})
				var clientResp map[string]map[string]any
				_ = json.Unmarshal(clientRec.Body.Bytes(), &clientResp)
				clientID := clientResp["UserPoolClient"]["ClientId"].(string)

				return clientID, "totally-invalid-refresh-token"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID, refreshToken := tt.setup(h)

			rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
				"ClientId": clientID,
				"AuthFlow": "REFRESH_TOKEN_AUTH",
				"AuthParameters": map[string]string{
					"REFRESH_TOKEN": refreshToken,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["AuthenticationResult"]["AccessToken"])
				assert.NotEmpty(t, resp["AuthenticationResult"]["IdToken"])
				// After rotation the old token is gone; new one must differ.
				newRT, _ := resp["AuthenticationResult"]["RefreshToken"].(string)
				assert.NotEqual(t, refreshToken, newRT, "refresh token must rotate on exchange")
			}
		})
	}
}

// TestParity_RefreshToken_PreservesAuthTime verifies that REFRESH_TOKEN_AUTH
// preserves the original auth_time rather than resetting it on each refresh.
func TestRefreshToken_PreservesAuthTime(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "authtimer")

	origClaims := decodeJWTPayload(t, tokens.AccessToken)
	origAuthTime, ok := origClaims["auth_time"].(float64)
	require.True(t, ok, "original access token must carry auth_time")

	refreshed, err := b.InitiateAuthRefreshToken(client.ClientID, tokens.RefreshToken)
	require.NoError(t, err)

	newClaims := decodeJWTPayload(t, refreshed.AccessToken)
	newAuthTime, ok := newClaims["auth_time"].(float64)
	require.True(t, ok, "refreshed access token must carry auth_time")

	assert.InDelta(t, origAuthTime, newAuthTime, 0,
		"auth_time must be preserved across refresh, not reset")
}
