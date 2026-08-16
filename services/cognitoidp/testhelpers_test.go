package cognitoidp_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// decodeJWTPayload base64url-decodes the payload segment of a JWT without verifying.
func decodeJWTPayload(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3, "expected JWT with 3 parts")

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))

	return claims
}

func setupTestPoolAndClient(
	t *testing.T,
) (*cognitoidp.InMemoryBackend, *cognitoidp.UserPool, *cognitoidp.UserPoolClient) {
	t.Helper()

	b := newTestBackend()
	pool, err := b.CreateUserPool("test-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "test-client")
	require.NoError(t, err)

	return b, pool, client
}

func signUpConfirmAndLogin(
	t *testing.T,
	b *cognitoidp.InMemoryBackend,
	clientID, username string,
) *cognitoidp.TokenResult {
	t.Helper()

	const password = "Pass1234!"

	user, err := b.SignUp(clientID, username, password, map[string]string{"email": username + "@x.com"})
	require.NoError(t, err)

	err = b.ConfirmSignUp(clientID, username, user.ConfirmCode)
	require.NoError(t, err)

	result, err := b.InitiateAuth(clientID, "USER_PASSWORD_AUTH", username, password)
	require.NoError(t, err)

	return result.Tokens
}

func setupHandlerPoolAndClient(t *testing.T, h *cognitoidp.Handler, poolName string) (string, string) {
	t.Helper()

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": poolName})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp struct {
		UserPool struct {
			ID string `json:"Id,omitempty"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp.UserPool.ID

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": poolName + "-client",
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId,omitempty"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))

	return poolID, clientResp.UserPoolClient.ClientID
}

func signUpAndConfirmViaHandler(t *testing.T, h *cognitoidp.Handler, clientID, username string) {
	t.Helper()

	const password = "Pass1234!"

	rec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": username,
		"Password": password,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var signUpResp struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
		UserConfirmed       bool              `json:"UserConfirmed,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &signUpResp))

	if signUpResp.UserConfirmed {
		return
	}

	code := signUpResp.CodeDeliveryDetails["ConfirmationCode"]
	confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
		"ClientId":         clientID,
		"Username":         username,
		"ConfirmationCode": code,
	})
	require.Equal(t, http.StatusOK, confirmRec.Code)
}

func loginViaHandler(t *testing.T, h *cognitoidp.Handler, clientID, username string) string {
	t.Helper()

	rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": username,
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken,omitempty"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.AuthenticationResult)

	return resp.AuthenticationResult.AccessToken
}

func computeSecretHash(clientID, username, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(username + clientID))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newTestBackend() *cognitoidp.InMemoryBackend {
	return cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
}

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

func setupPoolAndClientNamed(t *testing.T, h *cognitoidp.Handler, poolName, clientName string) (string, string) {
	t.Helper()

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": poolName})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": clientName,
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	return poolID, clientID
}

func signUpAndAdminConfirm(t *testing.T, h *cognitoidp.Handler, clientID, poolID, username string) {
	t.Helper()

	signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": username,
		"Password": "Pass1234!",
	})
	require.Equal(t, http.StatusOK, signupRec.Code, "SignUp failed for %s", username)

	confirmRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   username,
	})
	require.Equal(t, http.StatusOK, confirmRec.Code, "AdminConfirmSignUp failed for %s", username)
}
