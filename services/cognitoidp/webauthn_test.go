package cognitoidp_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestWebAuthn_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "webauthn-crud-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "webauthn-user")
	accessToken := loginViaHandler(t, h, clientID, "webauthn-user")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":            poolID,
		"WebAuthnConfiguration": map[string]any{"RelyingPartyId": passkeyRPID},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// StartWebAuthnRegistration — real, non-empty CredentialCreationOptions.
	rec = doCognitoRequest(t, h, "StartWebAuthnRegistration", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp struct {
		CredentialCreationOptions map[string]any `json:"CredentialCreationOptions,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp, jsonx.V1()))
	require.NotEmpty(t, startResp.CredentialCreationOptions)
	assert.NotEmpty(t, startResp.CredentialCreationOptions["challenge"])
	rp, ok := startResp.CredentialCreationOptions["rp"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, passkeyRPID, rp["id"])
	user, ok := startResp.CredentialCreationOptions["user"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "webauthn-user", user["name"])

	// CompleteWebAuthnRegistration
	pk := newSoftPasskey(t)
	rec = doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
		"AccessToken": accessToken,
		"Credential":  pk.register(t, startResp.CredentialCreationOptions),
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
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp, jsonx.V1()))
	require.Len(t, listResp.Credentials, 1)
	assert.Equal(t, b64u(pk.credID), listResp.Credentials[0].CredentialID)
	assert.Equal(t, "platform", listResp.Credentials[0].AuthenticatorAttachment)
	assert.NotEmpty(t, listResp.Credentials[0].FriendlyCredentialName,
		"the wire key is FriendlyCredentialName, not FriendlyName -- a real SDK client reads this field",
	)
	assert.Equal(t, []string{"internal"}, listResp.Credentials[0].AuthenticatorTransports,
		"transports from the browser's credential.response.transports must be threaded through",
	)

	// DeleteWebAuthnCredential
	rec = doCognitoRequest(t, h, "DeleteWebAuthnCredential", map[string]any{
		"AccessToken":  accessToken,
		"CredentialId": b64u(pk.credID),
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
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp2, jsonx.V1()))
	assert.Empty(t, listResp2.Credentials)

	// Delete again — not found.
	rec = doCognitoRequest(t, h, "DeleteWebAuthnCredential", map[string]any{
		"AccessToken":  accessToken,
		"CredentialId": b64u(pk.credID),
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

// TestHandler_CompleteWebAuthnRegistration_Validation covers the handler's rejections: an invalid
// access token, a credential with no pending challenge, and a malformed credential.
func TestHandler_CompleteWebAuthnRegistration_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "webauthn-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "wa-user")
	accessToken := loginViaHandler(t, h, clientID, "wa-user")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":            poolID,
		"WebAuthnConfiguration": map[string]any{"RelyingPartyId": passkeyRPID},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	tests := []struct {
		name       string
		token      string
		credential map[string]any
		wantCode   string
	}{
		{
			name:       "invalid_token",
			token:      "not-a-real-token",
			credential: map[string]any{"id": "abc"},
			wantCode:   "NotAuthorizedException",
		},
		{
			name:       "no_pending_challenge",
			token:      accessToken,
			credential: map[string]any{"id": "abc"},
			wantCode:   "WebAuthnChallengeNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
				"AccessToken": tt.token, "Credential": tt.credential,
			})
			assert.Equal(t, http.StatusBadRequest, resp.Code)
			assert.Contains(t, resp.Body.String(), tt.wantCode)
		})
	}

	t.Run("malformed_credential", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler(t)
		poolID2, clientID2 := setupHandlerPoolAndClient(t, h2, "webauthn-pool-2")
		signUpAndConfirmViaHandler(t, h2, clientID2, "wa-user")
		token := loginViaHandler(t, h2, clientID2, "wa-user")
		require.Equal(t, http.StatusOK, doCognitoRequest(t, h2, "SetUserPoolMfaConfig", map[string]any{
			"UserPoolId": poolID2, "WebAuthnConfiguration": map[string]any{"RelyingPartyId": passkeyRPID},
		}).Code)
		require.Equal(
			t,
			http.StatusOK,
			doCognitoRequest(t, h2, "StartWebAuthnRegistration", map[string]any{"AccessToken": token}).Code,
		)

		resp := doCognitoRequest(t, h2, "CompleteWebAuthnRegistration", map[string]any{
			"AccessToken": token, "Credential": map[string]any{"id": "abc", "type": "public-key"},
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "InvalidParameterException")
	})
}
