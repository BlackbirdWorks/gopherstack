package cognitoidp_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func newTestHandler(t *testing.T) *cognitoidp.Handler {
	t.Helper()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	return cognitoidp.NewHandler(backend, "us-east-1")
}

func doCognitoRequest(t *testing.T, h *cognitoidp.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func doJWKSRequest(t *testing.T, h *cognitoidp.Handler, userPoolID string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/"+userPoolID+"/.well-known/jwks.json", nil)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CognitoIDP", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateUserPool", "DescribeUserPool", "ListUserPools",
		"CreateUserPoolClient", "DescribeUserPoolClient",
		"SignUp", "ConfirmSignUp", "InitiateAuth", "AdminInitiateAuth",
		"AdminCreateUser", "AdminSetUserPassword", "AdminGetUser",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		path   string
		want   bool
	}{
		{
			name:   "matching_target",
			target: "AWSCognitoIdentityProviderService.CreateUserPool",
			path:   "/",
			want:   true,
		},
		{
			name: "matching_jwks_path",
			path: "/us-east-1_abc123/.well-known/jwks.json",
			want: true,
		},
		{
			name:   "non_matching",
			target: "AmazonSQS.SendMessage",
			path:   "/",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		path   string
		want   string
	}{
		{
			name:   "cognito_action",
			target: "AWSCognitoIdentityProviderService.CreateUserPool",
			path:   "/",
			want:   "CreateUserPool",
		},
		{
			name: "jwks_path",
			path: "/us-east-1_abc/.well-known/jwks.json",
			want: "GetJWKS",
		},
		{
			name: "unknown",
			path: "/",
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_CreateUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         map[string]any{"PoolName": "my-test-pool"},
			wantCode:     http.StatusOK,
			wantContains: []string{"my-test-pool", "Arn", "Id"},
		},
		{
			name:     "duplicate_pool",
			body:     map[string]any{"PoolName": "duplicate-pool"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_pool" {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "duplicate-pool"})
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "CreateUserPool", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_ListUserPools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numPools  int
		wantCode  int
		wantPools int
	}{
		{
			name:      "empty",
			numPools:  0,
			wantCode:  http.StatusOK,
			wantPools: 0,
		},
		{
			name:      "with_pools",
			numPools:  2,
			wantCode:  http.StatusOK,
			wantPools: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.numPools {
				rec := doCognitoRequest(
					t,
					h,
					"CreateUserPool",
					map[string]any{"PoolName": fmt.Sprintf("pool-%d", i)},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			pools, ok := resp["UserPools"].([]any)
			require.True(t, ok)
			assert.Len(t, pools, tt.wantPools)
		})
	}
}

func TestHandler_SignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) string
		body         func(clientID string) map[string]any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
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

				return clientResp["UserPoolClient"]["ClientId"].(string)
			},
			body: func(clientID string) map[string]any {
				return map[string]any{
					"ClientId": clientID,
					"Username": "testuser",
					"Password": "Password123!",
					"UserAttributes": []map[string]any{
						{"Name": "email", "Value": "test@example.com"},
					},
				}
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"UserSub", "UserConfirmed"},
		},
		{
			name: "invalid_client",
			setup: func(_ *cognitoidp.Handler) string {
				return "invalid-client-id"
			},
			body: func(clientID string) map[string]any {
				return map[string]any{
					"ClientId": clientID,
					"Username": "testuser",
					"Password": "Password123!",
				}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID := tt.setup(h)

			rec := doCognitoRequest(t, h, "SignUp", tt.body(clientID))
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_ConfirmSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		// setup returns clientID, username and the confirm code (may be empty for "any" codes).
		setup    func(h *cognitoidp.Handler) (clientID, username, confirmCode string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string, string) {
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

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "newuser",
					"Password": "Password123!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code)

				// Extract confirm code from the CodeDeliveryDetails in the SignUp response.
				var signupResp map[string]any
				_ = json.Unmarshal(signupRec.Body.Bytes(), &signupResp)
				code := ""
				if details, ok := signupResp["CodeDeliveryDetails"].(map[string]any); ok {
					code, _ = details["ConfirmationCode"].(string)
				}

				return clientID, "newuser", code
			},
			wantCode: http.StatusOK,
		},
		{
			name: "user_not_found",
			setup: func(h *cognitoidp.Handler) (string, string, string) {
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

				return clientResp["UserPoolClient"]["ClientId"].(string), "nobody", "123456"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID, username, confirmCode := tt.setup(h)

			rec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
				"ClientId":         clientID,
				"Username":         username,
				"ConfirmationCode": confirmCode,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_InitiateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) (clientID, username string)
		name         string
		password     string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
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

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "authuser",
					"Password": "Password123!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code)

				// Extract the confirmation code from the SignUp response.
				var signupResp map[string]any
				_ = json.Unmarshal(signupRec.Body.Bytes(), &signupResp)
				code := ""
				if details, ok := signupResp["CodeDeliveryDetails"].(map[string]any); ok {
					code, _ = details["ConfirmationCode"].(string)
				}

				confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
					"ClientId":         clientID,
					"Username":         "authuser",
					"ConfirmationCode": code,
				})
				require.Equal(t, http.StatusOK, confirmRec.Code)

				return clientID, "authuser"
			},
			password:     "Password123!",
			wantCode:     http.StatusOK,
			wantContains: []string{"AccessToken", "IdToken", "RefreshToken"},
		},
		{
			name: "wrong_password",
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

				signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "authuser2",
					"Password": "Password123!",
				})
				require.Equal(t, http.StatusOK, signupRec.Code)

				// Extract the confirmation code from the SignUp response.
				var signupResp map[string]any
				_ = json.Unmarshal(signupRec.Body.Bytes(), &signupResp)
				code := ""
				if details, ok := signupResp["CodeDeliveryDetails"].(map[string]any); ok {
					code, _ = details["ConfirmationCode"].(string)
				}

				confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
					"ClientId":         clientID,
					"Username":         "authuser2",
					"ConfirmationCode": code,
				})
				require.Equal(t, http.StatusOK, confirmRec.Code)

				return clientID, "authuser2"
			},
			password: "WrongPassword!",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clientID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
				"AuthFlow": "USER_PASSWORD_AUTH",
				"ClientId": clientID,
				"AuthParameters": map[string]string{
					"USERNAME": username,
					"PASSWORD": tt.password,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
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

func TestHandler_JWKS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) string
		name         string
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
			wantCode:     http.StatusOK,
			wantContains: []string{"keys", "RSA", "RS256"},
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doJWKSRequest(t, h, poolID)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "NonExistentAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnknownOperationException")
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_AdminGetUser(t *testing.T) {
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
					"Username":          "getusertest",
					"TemporaryPassword": "Temp123!",
				})

				return poolID, "getusertest"
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string), "nobody"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminGetUser", map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "my-client",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (poolID, clientID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
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

				return poolID, clientResp["UserPoolClient"]["ClientId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string), "nonexistent-client"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AdminInitiateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *cognitoidp.Handler) (poolID, clientID, username string)
		name         string
		password     string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) (string, string, string) {
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

				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          "adminauthuser",
					"TemporaryPassword": "Temp123!",
				})

				doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
					"UserPoolId": poolID,
					"Username":   "adminauthuser",
					"Password":   "Password123!",
					"Permanent":  true,
				})

				return poolID, clientID, "adminauthuser"
			},
			password:     "Password123!",
			wantCode:     http.StatusOK,
			wantContains: []string{"AccessToken", "IdToken"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID, username := tt.setup(h)

			rec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
				"AuthFlow":   "USER_PASSWORD_AUTH",
				"AuthParameters": map[string]string{
					"USERNAME": username,
					"PASSWORD": tt.password,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &cognitoidp.Provider{}
	assert.Equal(t, "CognitoIDP", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		target  string
		body    string
		wantRes string
	}{
		{
			name:    "jwks_path_extracts_pool_id",
			path:    "/us-east-1_abc123/.well-known/jwks.json",
			wantRes: "us-east-1_abc123",
		},
		{
			name:    "body_user_pool_id",
			path:    "/",
			target:  "AWSCognitoIdentityProviderService.DescribeUserPool",
			body:    `{"UserPoolId":"us-east-1_poolXYZ"}`,
			wantRes: "us-east-1_poolXYZ",
		},
		{
			name:    "body_client_id",
			path:    "/",
			target:  "AWSCognitoIdentityProviderService.InitiateAuth",
			body:    `{"ClientId":"myclient"}`,
			wantRes: "myclient",
		},
		{
			name:    "body_username",
			path:    "/",
			target:  "AWSCognitoIdentityProviderService.SignUp",
			body:    `{"Username":"alice"}`,
			wantRes: "alice",
		},
		{
			name:    "empty_body",
			path:    "/",
			target:  "AWSCognitoIdentityProviderService.ListUserPools",
			body:    `{}`,
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(http.MethodGet, tt.path, nil)
			}

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestHandler_UnmarshalTypeError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Sending a wrong type (array instead of string for PoolName) should return 400.
	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": []string{"not-a-string"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterException")
}

func TestCognitoIDP_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("my-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "my-client")
	require.NoError(t, err)

	// Use AdminCreateUser to create a confirmed user without going through SignUp.
	_, err = b.AdminCreateUser(pool.ID, "alice", "Password123!", map[string]string{"email": "alice@example.com"})
	require.NoError(t, err)

	require.NoError(t, b.AdminSetUserPassword(pool.ID, "alice", "Password123!", true))

	_ = client

	h := cognitoidp.NewHandler(b, "us-east-1")
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	h2 := cognitoidp.NewHandler(b2, "us-east-1")
	require.NoError(t, h2.Restore(t.Context(), snap))

	pools := b2.ListUserPools()
	require.Len(t, pools, 1)
	assert.Equal(t, "my-pool", pools[0].Name)

	clients, err := b2.ListUserPoolClients(pool.ID)
	require.NoError(t, err)
	require.Len(t, clients, 1)
	assert.Equal(t, "my-client", clients[0].ClientName)
}

func TestCognitoIDP_DeleteUserPool_CleansRefreshTokens(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("my-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "my-client")
	require.NoError(t, err)

	u, err := b.SignUp(client.ClientID, "alice", "Password123!", nil)
	require.NoError(t, err)

	require.NoError(t, b.ConfirmSignUp(client.ClientID, "alice", u.ConfirmCode))

	tokens, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Password123!")
	require.NoError(t, err)
	require.NotNil(t, tokens.Tokens)
	require.NotEmpty(t, tokens.Tokens.RefreshToken)

	// Deleting the pool should clean up the refresh token.
	require.NoError(t, b.DeleteUserPool(pool.ID))

	// Attempting to use the refresh token should fail now (token cleaned up).
	_, err = b.InitiateAuthRefreshToken(client.ClientID, tokens.Tokens.RefreshToken)
	require.Error(t, err, "refresh token should have been cleaned up on pool deletion")
}

func TestCognitoIDP_DeleteUserPoolClient_CleansRefreshTokens(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("my-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "my-client")
	require.NoError(t, err)

	u, err := b.SignUp(client.ClientID, "bob", "Password456!", nil)
	require.NoError(t, err)

	require.NoError(t, b.ConfirmSignUp(client.ClientID, "bob", u.ConfirmCode))

	tokens, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "bob", "Password456!")
	require.NoError(t, err)
	require.NotNil(t, tokens.Tokens)
	require.NotEmpty(t, tokens.Tokens.RefreshToken)

	// Deleting the client should clean up the refresh token.
	require.NoError(t, b.DeleteUserPoolClient(pool.ID, client.ClientID))

	// Attempting to use the refresh token should fail now (token cleaned up).
	_, err = b.InitiateAuthRefreshToken(client.ClientID, tokens.Tokens.RefreshToken)
	require.Error(t, err, "refresh token should have been cleaned up on client deletion")
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

func TestHandler_AdminConfirmSignUp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "admin-confirm-pool"})
	var poolData map[string]map[string]any
	_ = json.Unmarshal(poolRec.Body.Bytes(), &poolData)
	poolID := poolData["UserPool"]["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "c",
	})
	var clientData map[string]map[string]any
	_ = json.Unmarshal(clientRec.Body.Bytes(), &clientData)
	clientID := clientData["UserPoolClient"]["ClientId"].(string)

	signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "confuser",
		"Password": "Password123!",
	})
	require.Equal(t, http.StatusOK, signupRec.Code)

	// AdminConfirmSignUp should work without a confirmation code.
	rec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "confuser",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// After admin confirm, InitiateAuth should succeed.
	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"ClientId": clientID,
		"AuthFlow": "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "confuser",
			"PASSWORD": "Password123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec.Code)
}

func TestHandler_ListUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
		wantHTTP  int
	}{
		{
			name:      "empty_pool",
			wantCount: 0,
			wantHTTP:  http.StatusOK,
		},
		{
			name:      "pool_with_user",
			wantCount: 1,
			wantHTTP:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "test-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "test-client",
			})
			var clientResp map[string]any
			require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
			clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

			if tt.wantCount > 0 {
				doCognitoRequest(t, h, "SignUp", map[string]any{
					"ClientId": clientID,
					"Username": "testuser",
					"Password": "Password123!",
				})
			}

			rec := doCognitoRequest(t, h, "ListUsers", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantHTTP, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			users := resp["Users"].([]any)
			assert.Len(t, users, tt.wantCount)
		})
	}
}

func TestHandler_AdminDeleteUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
		wantHTTP int
	}{
		{
			name:     "delete_existing",
			username: "deleteuser",
			wantHTTP: http.StatusOK,
		},
		{
			name:     "delete_missing",
			username: "nonexistent",
			wantHTTP: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "test-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientName": "test-client",
			})
			var clientResp map[string]any
			require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
			clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

			// Create the user to delete in the first case.
			doCognitoRequest(t, h, "SignUp", map[string]any{
				"ClientId": clientID,
				"Username": "deleteuser",
				"Password": "Password123!",
			})

			rec := doCognitoRequest(t, h, "AdminDeleteUser", map[string]any{
				"UserPoolId": poolID,
				"Username":   tt.username,
			})
			assert.Equal(t, tt.wantHTTP, rec.Code)
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

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "cognito-idp", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_DeleteUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "del-pool"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) (poolID, clientID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
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

				return poolID, clientResp["UserPoolClient"]["ClientId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "client_not_found",
			setup: func(h *cognitoidp.Handler) (string, string) {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string), "nonexistent-client"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DeleteUserPoolClient", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetUserPoolMfaConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "mfa-pool"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "OFF")
			}
		})
	}
}

func TestHandler_ListUserPoolClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numClients int
		wantCode   int
	}{
		{
			name:       "empty",
			numClients: 0,
			wantCode:   http.StatusOK,
		},
		{
			name:       "with_clients",
			numClients: 2,
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			for i := range tt.numClients {
				rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": fmt.Sprintf("client-%d", i),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clients := resp["UserPoolClients"].([]any)
			assert.Len(t, clients, tt.numClients)
		})
	}
}

func TestHandler_Groups_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantCreateErr bool
		wantDeleteErr bool
	}{
		{
			name:          "create_list_delete_success",
			wantCreateErr: false,
			wantDeleteErr: false,
		},
		{
			name:          "create_duplicate_fails",
			wantCreateErr: true,
			wantDeleteErr: false,
		},
		{
			name:          "delete_nonexistent_fails",
			wantCreateErr: false,
			wantDeleteErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "grp-pool"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			// Create a group.
			createRec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
				"UserPoolId":  poolID,
				"GroupName":   "admins",
				"Description": "Admin users",
				"Precedence":  int32(1),
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			assert.Contains(t, createRec.Body.String(), "admins")

			if tt.wantCreateErr {
				// Create duplicate group — should fail.
				dupRec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				assert.Equal(t, http.StatusBadRequest, dupRec.Code)
			}

			// List groups — should contain the created group.
			listRec := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, http.StatusOK, listRec.Code)
			assert.Contains(t, listRec.Body.String(), "admins")

			if tt.wantDeleteErr {
				// Delete nonexistent group — should fail.
				delRec := doCognitoRequest(t, h, "DeleteGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, delRec.Code)
			} else {
				// Delete the group successfully.
				delRec := doCognitoRequest(t, h, "DeleteGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				assert.Equal(t, http.StatusOK, delRec.Code)

				// List groups — should now be empty.
				listRec2 := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
				assert.Equal(t, http.StatusOK, listRec2.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
				groups := listResp["Groups"].([]any)
				assert.Empty(t, groups)
			}
		})
	}
}

func TestHandler_AdminGroupMembership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errCase  string
		wantCode int
	}{
		{
			name:     "add_remove_list",
			wantCode: http.StatusOK,
		},
		{
			name:     "add_unknown_group",
			wantCode: http.StatusBadRequest,
			errCase:  "unknown_group",
		},
		{
			name:     "add_unknown_user",
			wantCode: http.StatusBadRequest,
			errCase:  "unknown_user",
		},
		{
			name:     "remove_unknown_group",
			wantCode: http.StatusBadRequest,
			errCase:  "remove_unknown_group",
		},
		{
			name:     "list_groups_unknown_user",
			wantCode: http.StatusBadRequest,
			errCase:  "list_unknown_user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Set up pool, user, and group.
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "grp-pool"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          "grpuser",
				"TemporaryPassword": "Temp123!",
			})

			doCognitoRequest(t, h, "CreateGroup", map[string]any{
				"UserPoolId": poolID,
				"GroupName":  "mygroup",
			})

			switch tt.errCase {
			case "unknown_group":
				rec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "nogroup",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			case "unknown_user":
				rec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "nobody",
					"GroupName":  "mygroup",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			case "remove_unknown_group":
				rec := doCognitoRequest(t, h, "AdminRemoveUserFromGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "nogroup",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			case "list_unknown_user":
				rec := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   "nobody",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			default:
				// Happy path: add user to group, list groups for user, remove from group.
				addRec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "mygroup",
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				assert.Contains(t, listRec.Body.String(), "mygroup")

				removeRec := doCognitoRequest(t, h, "AdminRemoveUserFromGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "mygroup",
				})
				assert.Equal(t, http.StatusOK, removeRec.Code)

				listRec2 := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
				})
				assert.Equal(t, http.StatusOK, listRec2.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
				groups := listResp["Groups"].([]any)
				assert.Empty(t, groups)
			}
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

func TestHandler_AddUserPoolClientSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantCode     int
		poolExists   bool
		clientExists bool
	}{
		{
			name:         "success",
			poolExists:   true,
			clientExists: true,
			wantCode:     http.StatusOK,
			wantContains: "ClientSecret",
		},
		{
			name:       "pool_not_found",
			poolExists: false,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "client_not_found",
			poolExists: true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := "us-east-1_NOTEXIST"
			clientID := "nonexistent-client"

			if tt.poolExists {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sec-pool"})
				var poolResp map[string]any
				require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
				poolID = poolResp["UserPool"].(map[string]any)["Id"].(string)
			}

			if tt.clientExists {
				clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
					"UserPoolId": poolID,
					"ClientName": "test-client",
				})
				var clientResp map[string]any
				require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
				clientID = clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)
			}

			rec := doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
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

func TestHandler_AdminDisableProviderForUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolID   string
		wantCode int
	}{
		{
			name:     "success_existing_pool",
			wantCode: http.StatusOK,
		},
		{
			name:     "pool_not_found",
			poolID:   "us-east-1_NOTEXIST",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.poolID
			if poolID == "" {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "provider-pool"})
				var poolResp map[string]any
				require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
				poolID = poolResp["UserPool"].(map[string]any)["Id"].(string)
			}

			rec := doCognitoRequest(t, h, "AdminDisableProviderForUser", map[string]any{
				"UserPoolId": poolID,
				"User": map[string]any{
					"ProviderName":           "Google",
					"ProviderAttributeName":  "Cognito_Subject",
					"ProviderAttributeValue": "google-123",
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AdminDisableEnableUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		setupUser bool
		wantCode  int
	}{
		{
			name:      "disable_success",
			operation: "AdminDisableUser",
			setupUser: true,
			wantCode:  http.StatusOK,
		},
		{
			name:      "enable_success",
			operation: "AdminEnableUser",
			setupUser: true,
			wantCode:  http.StatusOK,
		},
		{
			name:      "disable_user_not_found",
			operation: "AdminDisableUser",
			setupUser: false,
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "enable_user_not_found",
			operation: "AdminEnableUser",
			setupUser: false,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "dis-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			username := "dis-user"
			if tt.setupUser {
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "TempPass123!",
				})
			}

			rec := doCognitoRequest(t, h, tt.operation, map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AdminDisableUser_BlocksAuth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "block-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "block-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "blockuser",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "blockuser",
		"Password":   "FinalPass123!",
		"Permanent":  true,
	})

	// Disable the user.
	disableRec := doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "blockuser",
	})
	assert.Equal(t, http.StatusOK, disableRec.Code)

	// Attempt to authenticate with disabled user must fail.
	authRec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]any{
			"USERNAME": "blockuser",
			"PASSWORD": "FinalPass123!",
		},
	})
	assert.Equal(t, http.StatusBadRequest, authRec.Code)

	// Re-enable the user.
	enableRec := doCognitoRequest(t, h, "AdminEnableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "blockuser",
	})
	assert.Equal(t, http.StatusOK, enableRec.Code)

	// Authentication must succeed again.
	authRec2 := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]any{
			"USERNAME": "blockuser",
			"PASSWORD": "FinalPass123!",
		},
	})
	assert.Equal(t, http.StatusOK, authRec2.Code)
}

func TestHandler_AdminForgetDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupUser bool
		wantCode  int
	}{
		{
			name:      "success",
			setupUser: true,
			wantCode:  http.StatusOK,
		},
		{
			name:      "user_not_found",
			setupUser: false,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "device-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			username := "device-user"
			if tt.setupUser {
				doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
					"UserPoolId":        poolID,
					"Username":          username,
					"TemporaryPassword": "TempPass123!",
				})
			}

			rec := doCognitoRequest(t, h, "AdminForgetDevice", map[string]any{
				"UserPoolId": poolID,
				"Username":   username,
				"DeviceKey":  "device-abc123",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListUsers_ReturnsCorrectEnabledState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		disableUser bool
		wantEnabled bool
	}{
		{
			name:        "user_initially_enabled",
			disableUser: false,
			wantEnabled: true,
		},
		{
			name:        "user_disabled_shows_false",
			disableUser: true,
			wantEnabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "enabled-pool"})
			var poolResp map[string]any
			require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
			poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

			const username = "state-user"
			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          username,
				"TemporaryPassword": "TempPass123!",
			})

			if tt.disableUser {
				doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   username,
				})
			}

			listRec := doCognitoRequest(t, h, "ListUsers", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			users := listResp["Users"].([]any)
			require.Len(t, users, 1)
			assert.Equal(t, tt.wantEnabled, users[0].(map[string]any)["Enabled"].(bool))
		})
	}
}

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pool then reset.
	doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "reset-pool"})
	assert.Equal(t, 1, h.Backend.UserPoolCount())

	h.Reset()
	assert.Equal(t, 0, h.Backend.UserPoolCount())
	assert.Equal(t, 0, h.Backend.UserCount())
	assert.Equal(t, 0, h.Backend.ClientCount())
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		_ = i
		doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "cycle-pool"})
		h.Reset()
		assert.Equal(t, 0, h.Backend.UserPoolCount())
	}
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &cognitoidp.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidp.ErrNilAppContext)
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	// Ensure the handler works correctly with the cached dispatch table.
	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "ops-pool"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement1_SortedListUserPools(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra-pool", "alpha-pool", "mango-pool"} {
		doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": name})
	}

	rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{"MaxResults": 10})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	pools := resp["UserPools"].([]any)
	require.Len(t, pools, 3)
	assert.Equal(t, "alpha-pool", pools[0].(map[string]any)["Name"])
	assert.Equal(t, "mango-pool", pools[1].(map[string]any)["Name"])
	assert.Equal(t, "zebra-pool", pools[2].(map[string]any)["Name"])
}

func TestRefinement1_SortedListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-users-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, username := range []string{"zeus", "alice", "bob"} {
		doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
			"UserPoolId":        poolID,
			"Username":          username,
			"TemporaryPassword": "TempPass123!",
		})
	}

	rec := doCognitoRequest(t, h, "ListUsers", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	users := listResp["Users"].([]any)
	require.Len(t, users, 3)
	assert.Equal(t, "alice", users[0].(map[string]any)["Username"])
	assert.Equal(t, "bob", users[1].(map[string]any)["Username"])
	assert.Equal(t, "zeus", users[2].(map[string]any)["Username"])
}

func TestRefinement1_SortedListGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-groups-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, name := range []string{"zeta", "admin", "moderator"} {
		doCognitoRequest(t, h, "CreateGroup", map[string]any{
			"UserPoolId": poolID,
			"GroupName":  name,
		})
	}

	rec := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	groups := listResp["Groups"].([]any)
	require.Len(t, groups, 3)
	assert.Equal(t, "admin", groups[0].(map[string]any)["GroupName"])
	assert.Equal(t, "moderator", groups[1].(map[string]any)["GroupName"])
	assert.Equal(t, "zeta", groups[2].(map[string]any)["GroupName"])
}

func TestRefinement1_SortedListUserPoolClients(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-clients-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, name := range []string{"web-client", "android-client", "ios-client"} {
		doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
			"UserPoolId": poolID,
			"ClientName": name,
		})
	}

	rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	clients := listResp["UserPoolClients"].([]any)
	require.Len(t, clients, 3)
	assert.Equal(t, "android-client", clients[0].(map[string]any)["ClientName"])
	assert.Equal(t, "ios-client", clients[1].(map[string]any)["ClientName"])
	assert.Equal(t, "web-client", clients[2].(map[string]any)["ClientName"])
}

func TestRefinement1_AdminGetUser_IncludesEnabledAndModifiedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "agu-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "agu-user",
		"TemporaryPassword": "TempPass123!",
	})

	rec := doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "agu-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp["Enabled"].(bool))
	assert.NotZero(t, resp["UserCreateDate"])
	assert.NotZero(t, resp["UserLastModifiedDate"])
}

func TestRefinement1_AdminCreateUser_IncludesEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "acu-enabled-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "new-user",
		"TemporaryPassword": "TempPass123!",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp["User"].(map[string]any)["Enabled"].(bool))
}

func TestRefinement1_DescribeUserPoolClient_IncludesClientSecret(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "secret-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "sec-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	// No secret yet.
	descRec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Empty(t, descResp["UserPoolClient"].(map[string]any)["ClientSecret"])

	// Add secret.
	doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})

	// Secret now present.
	descRec2 := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	var descResp2 map[string]any
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	assert.NotEmpty(t, descResp2["UserPoolClient"].(map[string]any)["ClientSecret"])
}

func TestRefinement1_DescribeUserPool_IncludesSchemaAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "schema-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AddCustomAttributes", map[string]any{
		"UserPoolId": poolID,
		"CustomAttributes": []map[string]any{
			{"Name": "custom:department", "AttributeDataType": "String"},
		},
	})

	rec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	schema := resp["UserPool"].(map[string]any)["SchemaAttributes"]
	assert.NotNil(t, schema)
	schemaList := schema.([]any)
	require.Len(t, schemaList, 1)
	assert.Equal(t, "custom:department", schemaList[0].(map[string]any)["Name"])
}

func TestRefinement1_RefreshToken_DisabledUserBlocked(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "rt-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "rt-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "rt-user",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "rt-user",
		"Password":   "FinalPass123!",
		"Permanent":  true,
	})

	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"USERNAME": "rt-user",
			"PASSWORD": "FinalPass123!",
		},
	})
	require.Equal(t, http.StatusOK, authRec.Code)

	var authResp map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	refreshToken := authResp["AuthenticationResult"].(map[string]any)["RefreshToken"].(string)

	// Disable the user.
	doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "rt-user",
	})

	// Refresh token must now be rejected.
	refreshRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]any{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	assert.Equal(t, http.StatusBadRequest, refreshRec.Code)
}

func TestRefinement1_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "nil-slices-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	// Empty ListUsers must return [] not null.
	listRec := doCognitoRequest(t, h, "ListUsers", map[string]any{"UserPoolId": poolID})
	assert.Contains(t, listRec.Body.String(), `"Users":[]`)

	// Empty ListGroups must return [] not null.
	groupsRec := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
	assert.Contains(t, groupsRec.Body.String(), `"Groups":[]`)

	// Empty ListUserPoolClients must return [] not null.
	clientsRec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	assert.Contains(t, clientsRec.Body.String(), `"UserPoolClients":[]`)

	// Empty ListUserPools must return [] not null.
	h2 := newTestHandler(t)
	poolsRec := doCognitoRequest(t, h2, "ListUserPools", map[string]any{"MaxResults": 10})
	assert.Contains(t, poolsRec.Body.String(), `"UserPools":[]`)
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	assert.Equal(t, 0, backend.UserPoolCount())

	backend.AddUserPoolInternal(&cognitoidp.UserPool{
		ID:   "us-east-1_TEST01",
		Name: "seed-pool",
		ARN:  "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_TEST01",
	})
	assert.Equal(t, 1, backend.UserPoolCount())

	backend.AddUserInternal(&cognitoidp.User{
		Sub:        "sub-123",
		Username:   "seed-user",
		UserPoolID: "us-east-1_TEST01",
		Status:     "CONFIRMED",
		Enabled:    true,
	})
	assert.Equal(t, 1, backend.UserCount())

	backend.AddUserPoolClientInternal(&cognitoidp.UserPoolClient{
		ClientID:   "client-123",
		ClientName: "seed-client",
		UserPoolID: "us-east-1_TEST01",
	})
	assert.Equal(t, 1, backend.ClientCount())
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 0, h.Backend.UserPoolCount())
	assert.Equal(t, 0, h.Backend.UserCount())
	assert.Equal(t, 0, h.Backend.ClientCount())

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "count-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)
	assert.Equal(t, 1, h.Backend.UserPoolCount())

	doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "c1",
	})
	assert.Equal(t, 1, h.Backend.ClientCount())

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "u1",
		"TemporaryPassword": "TempPass123!",
	})
	assert.Equal(t, 1, h.Backend.UserCount())
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "persist-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "persist-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "persist-user",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "persist-user",
	})

	snap := h.Backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.UserPoolCount())
	assert.Equal(t, 1, h2.Backend.UserCount())
	assert.Equal(t, 1, h2.Backend.ClientCount())

	// Disabled state should survive round-trip.
	getUserRec := doCognitoRequest(t, h2, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "persist-user",
	})
	var getUserResp map[string]any
	require.NoError(t, json.Unmarshal(getUserRec.Body.Bytes(), &getUserResp))
	assert.False(t, getUserResp["Enabled"].(bool))
}

func TestRefinement1_AddCustomAttributes_RequiresCustomPrefix(t *testing.T) {
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

func TestRefinement1_SortedAttributes(t *testing.T) {
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

func TestRefinement1_SortedAdminListGroupsForUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "algfu-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, g := range []string{"zeta", "alpha", "beta"} {
		doCognitoRequest(t, h, "CreateGroup", map[string]any{
			"UserPoolId": poolID,
			"GroupName":  g,
		})
	}

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "group-user",
		"TemporaryPassword": "TempPass123!",
	})

	for _, g := range []string{"zeta", "alpha", "beta"} {
		doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
			"UserPoolId": poolID,
			"Username":   "group-user",
			"GroupName":  g,
		})
	}

	rec := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "group-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups := resp["Groups"].([]any)
	require.Len(t, groups, 3)
	assert.Equal(t, "alpha", groups[0].(map[string]any)["GroupName"])
	assert.Equal(t, "beta", groups[1].(map[string]any)["GroupName"])
	assert.Equal(t, "zeta", groups[2].(map[string]any)["GroupName"])
}

func TestRefinement1_ListUserPoolClients_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "empty-clients-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	clients := listResp["UserPoolClients"].([]any)
	assert.Empty(t, clients)
}

func TestRefinement1_ListUserPools_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{"MaxResults": 10})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	pools, ok := listResp["UserPools"].([]any)
	assert.True(t, ok, "UserPools should be an array, not nil")
	assert.Empty(t, pools)
}
