package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebAuthn_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "webauthn-crud-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "webauthn-user")
	accessToken := loginViaHandler(t, h, clientID, "webauthn-user")

	// StartWebAuthnRegistration — real, non-empty CredentialCreationOptions.
	rec := doCognitoRequest(t, h, "StartWebAuthnRegistration", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp struct {
		CredentialCreationOptions map[string]any `json:"CredentialCreationOptions,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	require.NotEmpty(t, startResp.CredentialCreationOptions)
	assert.NotEmpty(t, startResp.CredentialCreationOptions["challenge"])
	rp, ok := startResp.CredentialCreationOptions["rp"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, rp["id"])
	user, ok := startResp.CredentialCreationOptions["user"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "webauthn-user", user["name"])

	// CompleteWebAuthnRegistration
	rec = doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
		"AccessToken": accessToken,
		"Credential": map[string]any{
			"id":                      "cred-id-1",
			"authenticatorAttachment": "platform",
			"response": map[string]any{
				"transports": []any{"internal", "hybrid"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// ListWebAuthnCredentials
	rec = doCognitoRequest(t, h, "ListWebAuthnCredentials", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Credentials []struct {
			CredentialID            string   `json:"CredentialId,omitempty"`
			FriendlyCredentialName  string   `json:"FriendlyCredentialName,omitempty"`
			AuthenticatorAttachment string   `json:"AuthenticatorAttachment,omitempty"`
			AuthenticatorTransports []string `json:"AuthenticatorTransports"`
		} `json:"Credentials,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Credentials, 1)
	assert.Equal(t, "cred-id-1", listResp.Credentials[0].CredentialID)
	assert.Equal(t, "platform", listResp.Credentials[0].AuthenticatorAttachment)
	assert.NotEmpty(t, listResp.Credentials[0].FriendlyCredentialName,
		"the wire key is FriendlyCredentialName, not FriendlyName -- a real SDK client reads this field",
	)
	assert.Equal(t, []string{"internal", "hybrid"}, listResp.Credentials[0].AuthenticatorTransports,
		"transports from the browser's credential.response.transports must be threaded through",
	)

	// DeleteWebAuthnCredential
	rec = doCognitoRequest(t, h, "DeleteWebAuthnCredential", map[string]any{
		"AccessToken":  accessToken,
		"CredentialId": "cred-id-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — empty. Decode into a fresh struct: the response
	// omits "Credentials" entirely when empty (omitempty), and
	// json.Unmarshal does not clear fields absent from the input.
	rec = doCognitoRequest(t, h, "ListWebAuthnCredentials", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp2 struct {
		Credentials []struct {
			CredentialID string `json:"CredentialId,omitempty"`
		} `json:"Credentials,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp2))
	assert.Empty(t, listResp2.Credentials)

	// Delete again — not found.
	rec = doCognitoRequest(t, h, "DeleteWebAuthnCredential", map[string]any{
		"AccessToken":  accessToken,
		"CredentialId": "cred-id-1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWebAuthn_InvalidAccessToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "StartWebAuthnRegistration", map[string]any{"AccessToken": "garbage"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
		"AccessToken": "garbage",
		"Credential":  map[string]any{"id": "x"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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
