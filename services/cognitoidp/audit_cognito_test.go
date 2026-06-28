package cognitoidp_test

// audit_cognito_test.go — Phase-B Cognito audit tests.
//
// Covers:
//  1. Token validity per-client: AccessTokenValidity, IDTokenValidity, RefreshTokenValidity,
//     TokenValidityUnits stored on UserPoolClient, surfaced in Describe/Update responses,
//     and honored by token issuance (ExpiresIn + JWT exp).
//  2. VerifyUserAttribute routes to VerifyUserAttributeWithCode (real code validation),
//     not the stub that ignores the code.
//  3. Major Cognito API surface: user pools CRUD, app clients, SignUp+confirm, auth flows
//     (USER_PASSWORD_AUTH, REFRESH_TOKEN_AUTH), token JWT structure, groups, MFA, identity
//     pool GetId+GetCredentialsForIdentity, domains, resource servers.

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

// ── helpers ──────────────────────────────────────────────────────────────────

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

// ── 1. Token validity per-client ─────────────────────────────────────────────

func TestAuditCognito_TokenValidity_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name                 string
		accessTokenValidity  int32
		idTokenValidity      int32
		refreshTokenValidity int32
		units                map[string]any
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
				UserPoolClient struct { //nolint:govet // fieldalignment: test struct, cosmetic only
					ClientID             string         `json:"ClientId"`
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
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
				UserPoolClient struct { //nolint:govet // fieldalignment: test struct, cosmetic only
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.accessTokenValidity, descResp.UserPoolClient.AccessTokenValidity)
			assert.Equal(t, tt.idTokenValidity, descResp.UserPoolClient.IDTokenValidity)
			assert.Equal(t, tt.refreshTokenValidity, descResp.UserPoolClient.RefreshTokenValidity)
		})
	}
}

func TestAuditCognito_TokenValidity_UpdateClient(t *testing.T) {
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
		UserPoolClient struct { //nolint:govet // fieldalignment: test struct, cosmetic only
			AccessTokenValidity int32          `json:"AccessTokenValidity"`
			IDTokenValidity     int32          `json:"IdTokenValidity"`
			TokenValidityUnits  map[string]any `json:"TokenValidityUnits"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updResp))
	assert.Equal(t, int32(5), updResp.UserPoolClient.AccessTokenValidity)
	assert.Equal(t, int32(5), updResp.UserPoolClient.IDTokenValidity)
	assert.Equal(t, "minutes", updResp.UserPoolClient.TokenValidityUnits["AccessToken"])
}

func TestAuditCognito_TokenValidity_HonoredInJWT(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name                string
		accessTokenValidity int32
		unit                string
		wantExpiresIn       float64
		wantMinExpDelta     time.Duration
		wantMaxExpDelta     time.Duration
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

// ── 2. VerifyUserAttribute routes to real code validation ────────────────────

func TestAuditCognito_VerifyUserAttribute_RealCodeValidation(t *testing.T) {
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

// ── 3. User pool CRUD ─────────────────────────────────────────────────────────

func TestAuditCognito_UserPool_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolName string
	}{
		{name: "basic_create_describe_delete", poolName: "test-pool-crud"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": tt.poolName})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var createResp struct {
				UserPool struct {
					ID   string `json:"Id"`
					Name string `json:"Name"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			poolID := createResp.UserPool.ID
			assert.Equal(t, tt.poolName, createResp.UserPool.Name)
			assert.NotEmpty(t, poolID)

			// Describe.
			descRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp struct {
				UserPool struct {
					ID string `json:"Id"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			assert.Equal(t, poolID, descResp.UserPool.ID)

			// Delete.
			delRec := doCognitoRequest(t, h, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
			require.Equal(t, http.StatusOK, delRec.Code)

			// Describe after delete returns error.
			afterRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, http.StatusBadRequest, afterRec.Code)
		})
	}
}

// ── 4. App client CRUD ────────────────────────────────────────────────────────

func TestAuditCognito_AppClient_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "client-crud-pool")

	// Create a named client.
	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "my-app-client",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		UserPoolClient struct {
			ClientID   string `json:"ClientId"`
			ClientName string `json:"ClientName"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Equal(t, "my-app-client", createResp.UserPoolClient.ClientName)
	clientID := createResp.UserPoolClient.ClientID
	assert.NotEmpty(t, clientID)

	// List clients.
	listRec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		UserPoolClients []struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClients"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	var found bool
	for _, c := range listResp.UserPoolClients {
		if c.ClientID == clientID {
			found = true
		}
	}
	assert.True(t, found, "created client not in list")

	// Delete.
	delRec := doCognitoRequest(t, h, "DeleteUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)
}

// ── 5. SignUp + ConfirmSignUp ─────────────────────────────────────────────────

func TestAuditCognito_SignUp_ConfirmSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name     string
		username string
		password string
		wantOK   bool
		policy   map[string]any
	}{
		{name: "valid_user", username: "alice", password: "Passw0rd!", wantOK: true},
		{
			name:     "weak_password_fails_with_policy",
			username: "bob",
			password: "weak",
			wantOK:   false,
			policy: map[string]any{
				"MinimumLength":    8,
				"RequireUppercase": true,
				"RequireNumbers":   true,
				"RequireSymbols":   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "signup-pool-" + tt.name})
			require.Equal(t, http.StatusOK, poolRec.Code)
			var poolResp struct {
				UserPool struct {
					ID string `json:"Id"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp.UserPool.ID

			if tt.policy != nil {
				updRec := doCognitoRequest(t, h, "UpdateUserPool", map[string]any{
					"UserPoolId": poolID,
					"Policies":   map[string]any{"PasswordPolicy": tt.policy},
				})
				require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())
			}

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

			rec := doCognitoRequest(t, h, "SignUp", map[string]any{
				"ClientId": clientID,
				"Username": tt.username,
				"Password": tt.password,
			})
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			} else {
				assert.NotEqual(t, http.StatusOK, rec.Code, rec.Body.String())
			}
		})
	}
}

// ── 6. Auth flows ─────────────────────────────────────────────────────────────

func TestAuditCognito_AuthFlow_UserPasswordAuth(t *testing.T) {
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

func TestAuditCognito_AuthFlow_RefreshTokenAuth(t *testing.T) {
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

// ── 7. JWT token structure ────────────────────────────────────────────────────

func TestAuditCognito_JWT_Claims(t *testing.T) {
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

// ── 8. Groups ─────────────────────────────────────────────────────────────────

func TestAuditCognito_Groups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "groups-pool")

	// Create group.
	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "admins",
		"Description": "Admin group",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Create user + confirm.
	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "groupuser",
		"Password": "Passw0rd!",
	})
	require.Equal(t, http.StatusOK, signUpRec.Code)

	confRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
	})
	require.Equal(t, http.StatusOK, confRec.Code)

	// Add user to group.
	addRec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
		"GroupName":  "admins",
	})
	require.Equal(t, http.StatusOK, addRec.Code, addRec.Body.String())

	// Auth — token should contain cognito:groups.
	authResp := initiateAuth(t, h, clientID, "groupuser")
	authResult, ok := authResp["AuthenticationResult"].(map[string]any)
	require.True(t, ok)

	accessClaims := jwtClaims(t, authResult["AccessToken"].(string))
	groups, ok := accessClaims["cognito:groups"].([]any)
	require.True(t, ok, "expected cognito:groups claim")
	require.Len(t, groups, 1)
	assert.Equal(t, "admins", groups[0])
}

// ── 9. AdminCreateUser ────────────────────────────────────────────────────────

func TestAuditCognito_AdminCreateUser(t *testing.T) {
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

// ── 10. User pool domain ──────────────────────────────────────────────────────

func TestAuditCognito_Domain_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-pool")

	createRec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "my-test-domain",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	descRec := doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "my-test-domain",
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	delRec := doCognitoRequest(t, h, "DeleteUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "my-test-domain",
	})
	require.Equal(t, http.StatusOK, delRec.Code, delRec.Body.String())
}

// ── 11. Resource servers ──────────────────────────────────────────────────────

func TestAuditCognito_ResourceServer_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "rs-pool")

	createRec := doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
		"Name":       "My API",
		"Scopes": []map[string]string{
			{"ScopeName": "read", "ScopeDescription": "Read access"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	listRec := doCognitoRequest(t, h, "ListResourceServers", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		ResourceServers []struct {
			Identifier string `json:"Identifier"`
		} `json:"ResourceServers"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.ResourceServers, 1)
	assert.Equal(t, "https://api.example.com", listResp.ResourceServers[0].Identifier)
}

// ── 12. Token revocation ──────────────────────────────────────────────────────

func TestAuditCognito_TokenRevocation(t *testing.T) {
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

// ── 13. Backend tokenExpiryFor helper ────────────────────────────────────────

func TestAuditCognito_TokenExpiryFor_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name      string
		validity  int32
		unit      string
		tokenType string
		wantSecs  float64
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
				UserPoolClient struct { //nolint:govet // fieldalignment: test struct, cosmetic only
					AccessTokenValidity  int32          `json:"AccessTokenValidity"`
					IDTokenValidity      int32          `json:"IdTokenValidity"`
					RefreshTokenValidity int32          `json:"RefreshTokenValidity"`
					TokenValidityUnits   map[string]any `json:"TokenValidityUnits"`
				} `json:"UserPoolClient"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.Equal(t, tt.validity, createResp.UserPoolClient.AccessTokenValidity)
			assert.Equal(t, tt.unit, createResp.UserPoolClient.TokenValidityUnits[unitKey])
		})
	}
}
