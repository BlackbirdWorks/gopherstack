package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannel_APNS_CertificateAuth(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-apns-cert-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"BundleId":    "com.example.app",
			"Certificate": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
			"PrivateKey":  "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
			"Enabled":     true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"], "certificate auth sets HasCredential")
	assert.Nil(t, ch["HasTokenKey"], "no token key provided — field absent")
	assert.Equal(t, "apns", ch["ChannelType"])
}

func TestChannel_APNS_TokenAuth(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-apns-token-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"BundleId":   "com.example.app",
			"TokenKey":   "MFkwEwYH...",
			"TokenKeyId": "ABC123DEF4",
			"TeamId":     "XYZ123456",
			"Enabled":    true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, true, ch["Enabled"])
	// BundleId sets HasCredential; TokenKey sets HasTokenKey
	assert.Equal(t, true, ch["HasCredential"])
	assert.Equal(t, true, ch["HasTokenKey"])
}

func TestChannel_APNS_Sandbox(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-apns-sandbox-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns_sandbox",
		map[string]any{
			"BundleId":    "com.example.app",
			"Certificate": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
			"PrivateKey":  "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
			"Enabled":     true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns_sandbox", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "apns_sandbox", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestChannel_ADM_HasCredential(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-adm-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/adm",
		map[string]any{
			"ClientId":     "amzn1.application-oa2-client.abc123",
			"ClientSecret": "supersecret",
			"Enabled":      true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/adm", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "adm", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestChannel_Baidu_HasCredential(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-baidu-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/baidu",
		map[string]any{
			"ApiKey":    "baidu-api-key-12345",
			"SecretKey": "baidu-secret-key",
			"Enabled":   true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/baidu", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "baidu", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestChannel_GCM_ServiceJSON(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-gcm-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/gcm",
		map[string]any{
			"ApiKey":                      "AAAA-gcm-server-key",
			"DefaultAuthenticationMethod": "KEY",
			"Enabled":                     true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/gcm", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "gcm", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestChannel_Voice_EnableDisable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-voice-app")

	// Enable
	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/voice",
		map[string]any{"Enabled": true})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/voice", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "voice", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.NotEmpty(t, ch["CreationDate"])
	assert.NotEmpty(t, ch["LastModifiedDate"])

	// Disable
	putRec2 := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/voice",
		map[string]any{"Enabled": false})
	require.Equal(t, http.StatusOK, putRec2.Code)

	getRec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/voice", nil)
	require.Equal(t, http.StatusOK, getRec2.Code)

	var ch2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &ch2))

	assert.Equal(t, false, ch2["Enabled"])
	assert.EqualValues(t, 2, ch2["Version"])
}

func TestChannel_InApp_EnableDisable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-inapp-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/in-app",
		map[string]any{"Enabled": true})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/in-app", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, true, ch["Enabled"])
}

func TestChannel_SMS_ShortCode(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-sms-sc-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/sms",
		map[string]any{
			"SenderId":  "MyBrand",
			"ShortCode": "12345",
			"Enabled":   true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/sms", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "sms", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
}

// TestSMSChannel_PerTypeFields locks SMSChannelRequest's real wire shape:
// only Enabled/SenderId/ShortCode are request fields (confirmed against
// aws-sdk-go-v2/service/pinpoint/types.SMSChannelRequest).
// PromotionalMessagesPerSecond/TransactionalMessagesPerSecond exist only on
// SMSChannelResponse (AWS-computed account throughput) and must NOT be
// settable via PUT -- a prior pass had invented request-side support for
// them; removed.
func TestSMSChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantSenderID  string
		wantShortCode string
		wantEnabled   bool
	}{
		{
			name: "sms_full_fields",
			body: map[string]any{
				"Enabled":   true,
				"SenderId":  "MYBRAND",
				"ShortCode": "55555",
			},
			wantEnabled:   true,
			wantSenderID:  "MYBRAND",
			wantShortCode: "55555",
		},
		{
			name: "sms_sender_only",
			body: map[string]any{
				"Enabled":  true,
				"SenderId": "BRAND2",
			},
			wantEnabled:  true,
			wantSenderID: "BRAND2",
		},
		{
			name: "sms_short_code_only",
			body: map[string]any{
				"Enabled":   false,
				"ShortCode": "12345",
			},
			wantShortCode: "12345",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "sms-fields-app-"+tc.name)

			putRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/channels/sms", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

			assert.Equal(t, tc.wantEnabled, putResp["Enabled"])
			assert.NotContains(t, putResp, "PromotionalMessagesPerSecond")
			assert.NotContains(t, putResp, "TransactionalMessagesPerSecond")

			if tc.wantSenderID != "" {
				assert.Equal(t, tc.wantSenderID, putResp["SenderId"])
			}

			if tc.wantShortCode != "" {
				assert.Equal(t, tc.wantShortCode, putResp["ShortCode"])
			}

			// GET must round-trip the same values.
			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/channels/sms", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

			assert.Equal(t, tc.wantEnabled, getResp["Enabled"])

			if tc.wantSenderID != "" {
				assert.Equal(t, tc.wantSenderID, getResp["SenderId"],
					"GET must persist SenderId")
			}

			if tc.wantShortCode != "" {
				assert.Equal(t, tc.wantShortCode, getResp["ShortCode"],
					"GET must persist ShortCode")
			}
		})
	}
}

func TestGCMChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantAuthMethod string
		wantAPIKey     bool
		wantServiceJ   bool
	}{
		{
			name: "gcm_api_key",
			body: map[string]any{
				"Enabled":                     true,
				"ApiKey":                      "AIzaSy_test_key_abc123",
				"DefaultAuthenticationMethod": "KEY",
			},
			wantAPIKey:     true,
			wantAuthMethod: "KEY",
		},
		{
			name: "gcm_service_json",
			body: map[string]any{
				"Enabled":                     true,
				"ServiceJson":                 `{"type":"service_account","project_id":"my-project"}`,
				"DefaultAuthenticationMethod": "TOKEN",
			},
			wantServiceJ:   true,
			wantAuthMethod: "TOKEN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "gcm-fields-app-"+tc.name)

			putRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/channels/gcm", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

			if tc.wantAuthMethod != "" {
				assert.Equal(t, tc.wantAuthMethod, resp["DefaultAuthenticationMethod"])
			}

			if tc.wantAPIKey {
				// GCMChannelResponse's real member is "Credential", not "ApiKey"
				// (the request field name) -- raw ApiKey must never be echoed.
				assert.NotEmpty(t, resp["Credential"])
				assert.NotContains(t, resp, "ApiKey")
				assert.Equal(t, true, resp["HasCredential"])
			}

			if tc.wantServiceJ {
				// GCMChannelResponse has no ServiceJson member, only the
				// boolean HasFcmServiceCredentials -- the raw JSON must never
				// be echoed back.
				assert.NotContains(t, resp, "ServiceJson")
				assert.Equal(t, true, resp["HasFcmServiceCredentials"])
			}
		})
	}
}

func TestAPNSChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "apns-fields-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"Enabled":     true,
			"BundleId":    "com.example.app",
			"Certificate": "CERT_CONTENTS",
			"TeamId":      "TEAM123",
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

	// APNSChannelResponse has no BundleId/Certificate/TeamId members -- only
	// HasCredential/HasTokenKey booleans -- so raw request secrets must never
	// be echoed back on the wire.
	assert.NotContains(t, resp, "BundleId")
	assert.NotContains(t, resp, "Certificate")
	assert.NotContains(t, resp, "TeamId")
	assert.Equal(t, true, resp["HasCredential"])
}

// TestEmailChannel_PerTypeFields also covers OrchestrationSendingRoleArn,
// field-diffed against EmailChannelRequest/EmailChannelResponse
// (aws-sdk-go-v2/service/pinpoint/types) -- a prior pass omitted it entirely.
func TestEmailChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "email-fields-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/email",
		map[string]any{
			"Enabled":                     true,
			"FromAddress":                 "noreply@example.com",
			"Identity":                    "arn:aws:ses:us-east-1:123456789012:identity/example.com",
			"RoleArn":                     "arn:aws:iam::123456789012:role/email-role",
			"ConfigurationSet":            "my-config-set",
			"OrchestrationSendingRoleArn": "arn:aws:iam::123456789012:role/orchestration-role",
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

	assert.Equal(t, "noreply@example.com", resp["FromAddress"])
	assert.Equal(t, "arn:aws:ses:us-east-1:123456789012:identity/example.com", resp["Identity"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/email-role", resp["RoleArn"])
	assert.Equal(t, "my-config-set", resp["ConfigurationSet"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/orchestration-role", resp["OrchestrationSendingRoleArn"])
	assert.Equal(t, true, resp["HasCredential"])

	// GET must round-trip.
	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/channels/email", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	assert.Equal(t, "noreply@example.com", getResp["FromAddress"])
	assert.Equal(t, "my-config-set", getResp["ConfigurationSet"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/orchestration-role", getResp["OrchestrationSendingRoleArn"],
		"GET must persist OrchestrationSendingRoleArn")
}

func TestBaiduChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "baidu-fields-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/baidu",
		map[string]any{
			"Enabled":   true,
			"ApiKey":    "baidu_api_key_abc",
			"SecretKey": "baidu_secret_abc",
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

	// BaiduChannelResponse's credential member is "Credential" (the API key,
	// required), not "ApiKey"; SecretKey has no response member at all.
	assert.Equal(t, "baidu_api_key_abc", resp["Credential"])
	assert.NotContains(t, resp, "ApiKey")
	assert.NotContains(t, resp, "SecretKey")
	assert.Equal(t, true, resp["HasCredential"])
}

func TestADMChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "adm-fields-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/adm",
		map[string]any{
			"Enabled":      true,
			"ClientId":     "adm_client_id",
			"ClientSecret": "adm_client_secret",
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

	// ADMChannelResponse has no ClientId/ClientSecret members -- only
	// HasCredential -- so neither must be echoed back on the wire.
	assert.NotContains(t, resp, "ClientId")
	assert.NotContains(t, resp, "ClientSecret")
	assert.Equal(t, true, resp["HasCredential"])
}

func TestVoiceChannel_BasicResponse(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "voice-channel-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/voice",
		map[string]any{"Enabled": true})
	require.Equal(t, http.StatusOK, putRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

	assert.Equal(t, true, resp["Enabled"])
	assert.NotEmpty(t, resp["ChannelType"])
}

// TestAudit6_ApnsChannel_DefaultAuthenticationMethod verifies the APNS
// channel family round-trips DefaultAuthenticationMethod under its real
// aws-sdk-go-v2 field name, not a shortened "DefaultAuthMethod".
func TestApnsChannel_DefaultAuthenticationMethod(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "apns-auth-app")

	rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"BundleId":                    "com.example.app",
			"TokenKey":                    "-----BEGIN PRIVATE KEY-----",
			"TokenKeyId":                  "key123",
			"TeamId":                      "TEAM123",
			"DefaultAuthenticationMethod": "TOKEN",
			"Enabled":                     true,
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TOKEN", resp["DefaultAuthenticationMethod"])
}
