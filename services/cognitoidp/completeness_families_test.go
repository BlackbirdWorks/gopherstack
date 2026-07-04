package cognitoidp_test

// completeness_families_test.go tests the real, stateful implementations of
// three previously-stubbed op families in handler_completeness.go: device
// tracking, WebAuthn/passkey credentials, and misc auth (user settings,
// provider linking, auth event feedback, auth factors).

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// ---------------------------------------------------------------------------
// Device tracking
// ---------------------------------------------------------------------------

type deviceWire struct {
	DeviceKey    string `json:"DeviceKey,omitempty"`
	DeviceStatus string `json:"DeviceStatus,omitempty"`
}

func TestCompleteness_Devices_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "devices-crud-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "device-user")
	accessToken := loginViaHandler(t, h, clientID, "device-user")

	// ConfirmDevice with no DeviceKey — one is generated server-side.
	rec := doCognitoRequest(t, h, "ConfirmDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceName":  "my-laptop",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var confirmResp struct {
		UserConfirmationNecessary bool `json:"UserConfirmationNecessary,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &confirmResp))
	assert.False(t, confirmResp.UserConfirmationNecessary)

	// ListDevices — discover the generated device key.
	rec = doCognitoRequest(t, h, "ListDevices", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Devices []deviceWire `json:"Devices,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Devices, 1)
	assert.Equal(t, "not_remembered", listResp.Devices[0].DeviceStatus)
	deviceKey := listResp.Devices[0].DeviceKey
	require.NotEmpty(t, deviceKey)

	// GetDevice
	rec = doCognitoRequest(t, h, "GetDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   deviceKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		Device deviceWire `json:"Device"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, deviceKey, getResp.Device.DeviceKey)

	// UpdateDeviceStatus -> remembered
	rec = doCognitoRequest(t, h, "UpdateDeviceStatus", map[string]any{
		"AccessToken":            accessToken,
		"DeviceKey":              deviceKey,
		"DeviceRememberedStatus": "remembered",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doCognitoRequest(t, h, "GetDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   deviceKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "remembered", getResp.Device.DeviceStatus)

	// ForgetDevice
	rec = doCognitoRequest(t, h, "ForgetDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   deviceKey,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// GetDevice after forgetting — not found.
	rec = doCognitoRequest(t, h, "GetDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   deviceKey,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// ListDevices after forgetting — empty. Decode into a fresh struct: the
	// response omits "Devices" entirely when empty (omitempty), and
	// json.Unmarshal does not clear fields absent from the input.
	rec = doCognitoRequest(t, h, "ListDevices", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp2 struct {
		Devices []deviceWire `json:"Devices,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp2))
	assert.Empty(t, listResp2.Devices)
}

func TestCompleteness_Devices_NotFoundAndValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "devices-errors-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "device-user2")
	accessToken := loginViaHandler(t, h, clientID, "device-user2")

	// GetDevice: unknown device key.
	rec := doCognitoRequest(t, h, "GetDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   "no-such-device",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// GetDevice: invalid access token.
	rec = doCognitoRequest(t, h, "GetDevice", map[string]any{
		"AccessToken": "garbage",
		"DeviceKey":   "no-such-device",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// ForgetDevice: unknown device is a real error (non-admin, strict semantics).
	rec = doCognitoRequest(t, h, "ForgetDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   "no-such-device",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// UpdateDeviceStatus: invalid status value.
	rec = doCognitoRequest(t, h, "ConfirmDevice", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "ListDevices", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Devices []deviceWire `json:"Devices,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Devices, 1)

	rec = doCognitoRequest(t, h, "UpdateDeviceStatus", map[string]any{
		"AccessToken":            accessToken,
		"DeviceKey":              listResp.Devices[0].DeviceKey,
		"DeviceRememberedStatus": "bogus_status",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCompleteness_AdminDevices_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "admin-devices-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "admin-device-user")
	accessToken := loginViaHandler(t, h, clientID, "admin-device-user")

	rec := doCognitoRequest(t, h, "ConfirmDevice", map[string]any{
		"AccessToken": accessToken,
		"DeviceKey":   "device-abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// AdminListDevices
	rec = doCognitoRequest(t, h, "AdminListDevices", map[string]any{
		"UserPoolId": poolID,
		"Username":   "admin-device-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Devices []deviceWire `json:"Devices,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Devices, 1)
	assert.Equal(t, "device-abc", listResp.Devices[0].DeviceKey)

	// AdminGetDevice
	rec = doCognitoRequest(t, h, "AdminGetDevice", map[string]any{
		"UserPoolId": poolID,
		"Username":   "admin-device-user",
		"DeviceKey":  "device-abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// AdminUpdateDeviceStatus
	rec = doCognitoRequest(t, h, "AdminUpdateDeviceStatus", map[string]any{
		"UserPoolId":             poolID,
		"Username":               "admin-device-user",
		"DeviceKey":              "device-abc",
		"DeviceRememberedStatus": "remembered",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doCognitoRequest(t, h, "AdminGetDevice", map[string]any{
		"UserPoolId": poolID,
		"Username":   "admin-device-user",
		"DeviceKey":  "device-abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		Device deviceWire `json:"Device"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "remembered", getResp.Device.DeviceStatus)

	// AdminForgetDevice — real deletion when the device exists.
	rec = doCognitoRequest(t, h, "AdminForgetDevice", map[string]any{
		"UserPoolId": poolID,
		"Username":   "admin-device-user",
		"DeviceKey":  "device-abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "AdminGetDevice", map[string]any{
		"UserPoolId": poolID,
		"Username":   "admin-device-user",
		"DeviceKey":  "device-abc",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCompleteness_AdminDevices_InvalidPoolOrUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-devices-invalid-pool")

	rec := doCognitoRequest(t, h, "AdminGetDevice", map[string]any{
		"UserPoolId": "nonexistent-pool",
		"Username":   "someone",
		"DeviceKey":  "k",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doCognitoRequest(t, h, "AdminListDevices", map[string]any{
		"UserPoolId": poolID,
		"Username":   "no-such-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// WebAuthn / passkeys
// ---------------------------------------------------------------------------

func TestCompleteness_WebAuthn_CRUD(t *testing.T) {
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
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// ListWebAuthnCredentials
	rec = doCognitoRequest(t, h, "ListWebAuthnCredentials", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Credentials []struct {
			CredentialID            string `json:"CredentialId,omitempty"`
			FriendlyName            string `json:"FriendlyName,omitempty"`
			AuthenticatorAttachment string `json:"AuthenticatorAttachment,omitempty"`
		} `json:"Credentials,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Credentials, 1)
	assert.Equal(t, "cred-id-1", listResp.Credentials[0].CredentialID)
	assert.Equal(t, "platform", listResp.Credentials[0].AuthenticatorAttachment)
	assert.NotEmpty(t, listResp.Credentials[0].FriendlyName)

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

func TestCompleteness_WebAuthn_InvalidAccessToken(t *testing.T) {
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

// ---------------------------------------------------------------------------
// User settings (legacy MFAOptions) + auth factors
// ---------------------------------------------------------------------------

func TestCompleteness_UserSettings_And_AuthFactors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "user-settings-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "settings-user")
	accessToken := loginViaHandler(t, h, clientID, "settings-user")

	// Initially, only PASSWORD is configured.
	rec := doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var factorsResp struct {
		Username                  string   `json:"Username,omitempty"`
		ConfiguredUserAuthFactors []string `json:"ConfiguredUserAuthFactors,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &factorsResp))
	assert.Equal(t, "settings-user", factorsResp.Username)
	assert.Contains(t, factorsResp.ConfiguredUserAuthFactors, "PASSWORD")
	assert.NotContains(t, factorsResp.ConfiguredUserAuthFactors, "SMS_OTP")
	assert.NotContains(t, factorsResp.ConfiguredUserAuthFactors, "WEB_AUTHN")

	// SetUserSettings persists legacy MFAOptions (SMS).
	rec = doCognitoRequest(t, h, "SetUserSettings", map[string]any{
		"AccessToken": accessToken,
		"MFAOptions": []map[string]any{
			{"DeliveryMedium": "SMS", "AttributeName": "phone_number"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &factorsResp))
	assert.Contains(t, factorsResp.ConfiguredUserAuthFactors, "SMS_OTP")

	// Register a WebAuthn credential -> WEB_AUTHN factor appears too.
	rec = doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
		"AccessToken": accessToken,
		"Credential":  map[string]any{"id": "factor-cred"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &factorsResp))
	assert.Contains(t, factorsResp.ConfiguredUserAuthFactors, "WEB_AUTHN")

	// AdminSetUserSettings — admin variant, same persisted state.
	rec = doCognitoRequest(t, h, "AdminSetUserSettings", map[string]any{
		"UserPoolId": poolID,
		"Username":   "settings-user",
		"MFAOptions": []map[string]any{
			{"DeliveryMedium": "SMS", "AttributeName": "phone_number"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// AdminSetUserSettings on unknown user fails.
	rec = doCognitoRequest(t, h, "AdminSetUserSettings", map[string]any{
		"UserPoolId": poolID,
		"Username":   "ghost",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCompleteness_GetUserAuthFactors_InvalidAccessToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "GetUserAuthFactors", map[string]any{"AccessToken": "garbage"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// AdminLinkProviderForUser
// ---------------------------------------------------------------------------

func TestCompleteness_AdminLinkProviderForUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "link-provider-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "link-user")

	rec := doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
		"DestinationUser": map[string]any{
			"ProviderName":           "Cognito",
			"ProviderAttributeValue": "link-user",
		},
		"SourceUser": map[string]any{
			"ProviderName":           "Facebook",
			"ProviderAttributeName":  "Cognito_Subject",
			"ProviderAttributeValue": "fb-12345",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Unknown destination user.
	rec = doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
		"DestinationUser": map[string]any{
			"ProviderName":           "Cognito",
			"ProviderAttributeValue": "ghost",
		},
		"SourceUser": map[string]any{
			"ProviderName":           "Facebook",
			"ProviderAttributeName":  "Cognito_Subject",
			"ProviderAttributeValue": "fb-999",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing SourceUser fields.
	rec = doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
		"DestinationUser": map[string]any{
			"ProviderName":           "Cognito",
			"ProviderAttributeValue": "link-user",
		},
		"SourceUser": map[string]any{
			"ProviderName": "Facebook",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing DestinationUser/SourceUser entirely.
	rec = doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Auth events + feedback
// ---------------------------------------------------------------------------

func TestCompleteness_AdminListUserAuthEvents_EmptyButValid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "auth-events-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "events-user")

	rec := doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
		"UserPoolId": poolID,
		"Username":   "events-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken  string           `json:"NextToken,omitempty"`
		AuthEvents []map[string]any `json:"AuthEvents,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.AuthEvents)
	assert.Empty(t, resp.NextToken)

	// Invalid pool.
	rec = doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
		"UserPoolId": "nonexistent-pool",
		"Username":   "events-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Unknown user.
	rec = doCognitoRequest(t, h, "AdminListUserAuthEvents", map[string]any{
		"UserPoolId": poolID,
		"Username":   "ghost",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCompleteness_AuthEventFeedback_NotFoundAndValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "auth-event-feedback-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "feedback-user")

	// AdminUpdateAuthEventFeedback: no such event exists (no sign-in-event
	// producer is modeled), so this is a real, correct ResourceNotFoundException.
	rec := doCognitoRequest(t, h, "AdminUpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Invalid FeedbackValue is rejected before the not-found check would apply.
	rec = doCognitoRequest(t, h, "AdminUpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackValue": "Bogus",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Unknown user.
	rec = doCognitoRequest(t, h, "AdminUpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "ghost",
		"EventId":       "evt-1",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Non-admin UpdateAuthEventFeedback requires a FeedbackToken.
	rec = doCognitoRequest(t, h, "UpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// With a FeedbackToken present, it still 404s since no event is on record.
	rec = doCognitoRequest(t, h, "UpdateAuthEventFeedback", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "feedback-user",
		"EventId":       "evt-1",
		"FeedbackToken": "tok-123",
		"FeedbackValue": "Valid",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

func TestPersistence_DeviceWebAuthnAndUserSettingsSurviveSnapshot(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateUserPool("families-persist-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "families-persist-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "persist-families-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "persist-families-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "persist-families-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	accessToken := result.Tokens.AccessToken

	// Populate device, WebAuthn credential, and legacy MFAOptions state.
	deviceKey, _, err := b.ConfirmDevice(accessToken, "persist-device", "persist-laptop")
	require.NoError(t, err)
	require.NoError(t, b.UpdateDeviceStatus(accessToken, deviceKey, "remembered"))

	_, err = b.CompleteWebAuthnRegistration(accessToken, "persist-cred", "platform")
	require.NoError(t, err)

	require.NoError(t, b.SetUserSettings(accessToken, []cognitoidp.MFAOptionType{
		{DeliveryMedium: "SMS", AttributeName: "phone_number"},
	}))

	require.NoError(t, b.AdminLinkProviderForUser(
		pool.ID, "persist-families-user", "Facebook", "Cognito_Subject", "fb-persist-1",
	))

	// Snapshot and restore into a fresh backend.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Device survives with its remembered status.
	dev, err := b2.AdminGetDevice(pool.ID, "persist-families-user", "persist-device")
	require.NoError(t, err)
	assert.Equal(t, "remembered", dev.Status)
	assert.Equal(t, "persist-laptop", dev.Attributes["device_name"])

	// WebAuthn credential survives.
	creds, _, err := b2.ListWebAuthnCredentials(accessToken, 0, "")
	require.NoError(t, err)
	require.Len(t, creds, 1)
	assert.Equal(t, "persist-cred", creds[0].CredentialID)
	assert.Equal(t, "platform", creds[0].AuthenticatorAttachment)

	// Legacy MFAOptions and linked provider survive on the restored user.
	restoredUser, err := b2.AdminGetUser(pool.ID, "persist-families-user")
	require.NoError(t, err)
	require.Len(t, restoredUser.MFAOptions, 1)
	assert.Equal(t, "SMS", restoredUser.MFAOptions[0].DeliveryMedium)
	require.Len(t, restoredUser.LinkedProviders, 1)
	assert.Equal(t, "Facebook", restoredUser.LinkedProviders[0].ProviderName)
	assert.Equal(t, "fb-persist-1", restoredUser.LinkedProviders[0].ProviderAttributeValue)
}
