package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deviceWire struct {
	DeviceKey    string `json:"DeviceKey,omitempty"`
	DeviceStatus string `json:"DeviceStatus,omitempty"`
}

func TestDevices_CRUD(t *testing.T) {
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

func TestDevices_NotFoundAndValidation(t *testing.T) {
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

func TestAdminDevices_CRUD(t *testing.T) {
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

func TestAdminDevices_InvalidPoolOrUser(t *testing.T) {
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

// TestHandler_AdminGetDevice_Validation covers the HTTP handler for AdminGetDevice.
// Devices are never persisted, so a valid pool/user/device key still resolves to a
// ResourceNotFoundException, while unknown pools/users are rejected up front.
func TestHandler_AdminGetDevice_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "getdevice-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "device-user")

	tests := []struct {
		name     string
		poolID   string
		username string
		device   string
		wantCode int
	}{
		{
			name:     "pool_not_found",
			poolID:   "bad-pool",
			username: "device-user",
			device:   "dk",
			wantCode: http.StatusBadRequest,
		},
		{name: "user_not_found", poolID: poolID, username: "ghost", device: "dk", wantCode: http.StatusBadRequest},
		{
			name:     "device_not_found",
			poolID:   poolID,
			username: "device-user",
			device:   "dk",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doCognitoRequest(t, h, "AdminGetDevice", map[string]any{
				"UserPoolId": tt.poolID,
				"Username":   tt.username,
				"DeviceKey":  tt.device,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
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
