package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────
// Finding #16: per-type channel responses include channel-specific fields
// ──────────────────────────────────────────────────

func TestAudit6_SMSChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantSenderID  string
		wantShortCode string
		wantEnabled   bool
		wantPromoRate int
		wantTransRate int
	}{
		{
			name: "sms_full_fields",
			body: map[string]any{
				"Enabled":                        true,
				"SenderId":                       "MYBRAND",
				"ShortCode":                      "55555",
				"PromotionalMessagesPerSecond":   10,
				"TransactionalMessagesPerSecond": 20,
			},
			wantEnabled:   true,
			wantSenderID:  "MYBRAND",
			wantShortCode: "55555",
			wantPromoRate: 10,
			wantTransRate: 20,
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

			if tc.wantSenderID != "" {
				assert.Equal(t, tc.wantSenderID, putResp["SenderId"])
			}

			if tc.wantShortCode != "" {
				assert.Equal(t, tc.wantShortCode, putResp["ShortCode"])
			}

			if tc.wantPromoRate > 0 {
				assert.EqualValues(t, tc.wantPromoRate, putResp["PromotionalMessagesPerSecond"])
			}

			if tc.wantTransRate > 0 {
				assert.EqualValues(t, tc.wantTransRate, putResp["TransactionalMessagesPerSecond"])
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

			if tc.wantPromoRate > 0 {
				assert.EqualValues(t, tc.wantPromoRate, getResp["PromotionalMessagesPerSecond"],
					"GET must persist PromotionalMessagesPerSecond")
			}

			if tc.wantTransRate > 0 {
				assert.EqualValues(t, tc.wantTransRate, getResp["TransactionalMessagesPerSecond"],
					"GET must persist TransactionalMessagesPerSecond")
			}
		})
	}
}

func TestAudit6_GCMChannel_PerTypeFields(t *testing.T) {
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
				assert.NotEmpty(t, resp["ApiKey"])
				assert.Equal(t, true, resp["HasCredential"])
			}

			if tc.wantServiceJ {
				assert.NotEmpty(t, resp["ServiceJson"])
			}
		})
	}
}

func TestAudit6_APNSChannel_PerTypeFields(t *testing.T) {
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

	assert.Equal(t, "com.example.app", resp["BundleId"])
	assert.Equal(t, "CERT_CONTENTS", resp["Certificate"])
	assert.Equal(t, "TEAM123", resp["TeamId"])
	assert.Equal(t, true, resp["HasCredential"])
}

func TestAudit6_EmailChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "email-fields-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/email",
		map[string]any{
			"Enabled":          true,
			"FromAddress":      "noreply@example.com",
			"Identity":         "arn:aws:ses:us-east-1:123456789012:identity/example.com",
			"RoleArn":          "arn:aws:iam::123456789012:role/email-role",
			"ConfigurationSet": "my-config-set",
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &resp))

	assert.Equal(t, "noreply@example.com", resp["FromAddress"])
	assert.Equal(t, "arn:aws:ses:us-east-1:123456789012:identity/example.com", resp["Identity"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/email-role", resp["RoleArn"])
	assert.Equal(t, "my-config-set", resp["ConfigurationSet"])
	assert.Equal(t, true, resp["HasCredential"])

	// GET must round-trip.
	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/channels/email", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	assert.Equal(t, "noreply@example.com", getResp["FromAddress"])
	assert.Equal(t, "my-config-set", getResp["ConfigurationSet"])
}

func TestAudit6_BaiduChannel_PerTypeFields(t *testing.T) {
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

	assert.Equal(t, "baidu_api_key_abc", resp["ApiKey"])
	assert.Equal(t, "baidu_secret_abc", resp["SecretKey"])
	assert.Equal(t, true, resp["HasCredential"])
}

func TestAudit6_ADMChannel_PerTypeFields(t *testing.T) {
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

	assert.Equal(t, "adm_client_id", resp["ClientId"])
	assert.Equal(t, "adm_client_secret", resp["ClientSecret"])
	assert.Equal(t, true, resp["HasCredential"])
}

func TestAudit6_VoiceChannel_BasicResponse(t *testing.T) {
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

func TestAudit6_GetChannels_IncludesExtraFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "get-channels-app")

	// Set up SMS and GCM channels.
	doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/sms",
		map[string]any{"Enabled": true, "SenderId": "MYSENDER"})
	doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/gcm",
		map[string]any{"Enabled": true, "ApiKey": "AIzaSy_test"})

	rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/channels", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	channels, ok := resp["Channels"].(map[string]any)
	require.True(t, ok, "Channels must be a map")

	sms, ok := channels["sms"].(map[string]any)
	require.True(t, ok, "SMS channel must be present")
	assert.Equal(t, "MYSENDER", sms["SenderId"],
		"GetChannels must include per-type SenderId")

	gcm, ok := channels["gcm"].(map[string]any)
	require.True(t, ok, "GCM channel must be present")
	assert.Equal(t, "AIzaSy_test", gcm["ApiKey"],
		"GetChannels must include per-type ApiKey")
}

// ──────────────────────────────────────────────────
// Finding #18: PutEvents returns per-endpoint per-event EventsResponse
// ──────────────────────────────────────────────────

func TestAudit6_PutEvents_ReturnsEventResults(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "put-events-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/events",
		map[string]any{
			"EventsRequest": map[string]any{
				"BatchItem": map[string]any{
					"ep-001": map[string]any{
						"Endpoint": map[string]any{"ChannelType": "EMAIL"},
						"Events": map[string]any{
							"ev-1": map[string]any{
								"EventType": "_session.start",
								"Timestamp": "2026-01-01T00:00:00Z",
							},
							"ev-2": map[string]any{
								"EventType": "custom.button.click",
								"Timestamp": "2026-01-01T00:00:01Z",
							},
						},
					},
				},
			},
		})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["Results"].(map[string]any)
	require.True(t, ok, "EventsResponse must have Results")

	epResult, ok := results["ep-001"].(map[string]any)
	require.True(t, ok, "Results must contain ep-001")

	evResults, ok := epResult["EventsItemResponse"].(map[string]any)
	require.True(t, ok, "ep-001 must have EventsItemResponse")

	ev1, ok := evResults["ev-1"].(map[string]any)
	require.True(t, ok, "EventsItemResponse must contain ev-1")
	assert.EqualValues(t, http.StatusAccepted, ev1["StatusCode"])
	assert.Equal(t, "Accepted", ev1["Message"])

	ev2, ok := evResults["ev-2"].(map[string]any)
	require.True(t, ok, "EventsItemResponse must contain ev-2")
	assert.EqualValues(t, http.StatusAccepted, ev2["StatusCode"])
}

func TestAudit6_PutEvents_MultipleEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "put-events-multi-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/events",
		map[string]any{
			"EventsRequest": map[string]any{
				"BatchItem": map[string]any{
					"ep-a": map[string]any{
						"Endpoint": map[string]any{},
						"Events": map[string]any{
							"ev-a1": map[string]any{"EventType": "login"},
						},
					},
					"ep-b": map[string]any{
						"Endpoint": map[string]any{},
						"Events": map[string]any{
							"ev-b1": map[string]any{"EventType": "purchase"},
							"ev-b2": map[string]any{"EventType": "view"},
						},
					},
				},
			},
		})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results := resp["Results"].(map[string]any)

	epA := results["ep-a"].(map[string]any)
	evA := epA["EventsItemResponse"].(map[string]any)
	assert.Len(t, evA, 1, "ep-a has 1 event")

	epB := results["ep-b"].(map[string]any)
	evB := epB["EventsItemResponse"].(map[string]any)
	assert.Len(t, evB, 2, "ep-b has 2 events")
}

func TestAudit6_PutEvents_EmptyBatch(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "put-events-empty-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/events",
		map[string]any{
			"EventsRequest": map[string]any{
				"BatchItem": map[string]any{},
			},
		})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results := resp["Results"].(map[string]any)
	assert.Empty(t, results, "empty BatchItem → empty Results")
}

func TestAudit6_PutEvents_UnknownApp(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/events",
		map[string]any{
			"EventsRequest": map[string]any{
				"BatchItem": map[string]any{},
			},
		})

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// ──────────────────────────────────────────────────
// Finding #23: PhoneNumberValidate returns country info
// ──────────────────────────────────────────────────

func TestAudit6_PhoneNumberValidate_E164Input(t *testing.T) {
	t.Parallel()

	tests := []struct {
		phoneNumber        string
		name               string
		wantCountryIso2    string
		wantCountry        string
		wantTimezone       string
		wantCountryNumeric string
	}{
		{
			name:               "us_e164",
			phoneNumber:        "+12125551234",
			wantCountryIso2:    "US",
			wantCountry:        "United States",
			wantTimezone:       "America/New_York",
			wantCountryNumeric: "1",
		},
		{
			name:               "gb_e164",
			phoneNumber:        "+442071234567",
			wantCountryIso2:    "GB",
			wantCountry:        "United Kingdom",
			wantTimezone:       "Europe/London",
			wantCountryNumeric: "44",
		},
		{
			name:               "de_e164",
			phoneNumber:        "+493012345678",
			wantCountryIso2:    "DE",
			wantCountry:        "Germany",
			wantTimezone:       "Europe/Berlin",
			wantCountryNumeric: "49",
		},
		{
			name:               "jp_e164",
			phoneNumber:        "+81312345678",
			wantCountryIso2:    "JP",
			wantCountry:        "Japan",
			wantTimezone:       "Asia/Tokyo",
			wantCountryNumeric: "81",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
				map[string]any{
					"NumberValidateRequest": map[string]any{
						"PhoneNumber": tc.phoneNumber,
					},
				})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			inner, ok := resp["NumberValidateResponse"].(map[string]any)
			require.True(t, ok, "NumberValidateResponse must be present")

			assert.Equal(t, tc.wantCountryIso2, inner["CountryCodeIso2"],
				"CountryCodeIso2 must match")
			assert.Equal(t, tc.wantCountryIso2, inner["OriginalCountryCodeIso2"],
				"OriginalCountryCodeIso2 must match")
			assert.Equal(t, tc.wantCountry, inner["Country"],
				"Country name must match")
			assert.Equal(t, tc.wantTimezone, inner["Timezone"],
				"Timezone must match")
			assert.Equal(t, tc.wantCountryNumeric, inner["CountryCodeNumeric"],
				"CountryCodeNumeric must match")
			assert.Equal(t, tc.phoneNumber, inner["OriginalPhoneNumber"],
				"OriginalPhoneNumber must echo input")
			assert.NotEmpty(t, inner["CleansedPhoneNumberE164"],
				"CleansedPhoneNumberE164 must be set")
		})
	}
}

func TestAudit6_PhoneNumberValidate_UnknownCountry(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
		map[string]any{
			"NumberValidateRequest": map[string]any{
				"PhoneNumber": "+99912345678",
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	inner := resp["NumberValidateResponse"].(map[string]any)
	assert.Equal(t, "ZZ", inner["CountryCodeIso2"], "unknown country returns ZZ")
	assert.Equal(t, "+99912345678", inner["OriginalPhoneNumber"])
	assert.Equal(t, "+99912345678", inner["CleansedPhoneNumberE164"])
}

func TestAudit6_PhoneNumberValidate_NationalFormat(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// 10-digit US number without + prefix should be normalized.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
		map[string]any{
			"NumberValidateRequest": map[string]any{
				"PhoneNumber": "2125551234",
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	inner := resp["NumberValidateResponse"].(map[string]any)
	assert.Equal(t, "+12125551234", inner["CleansedPhoneNumberE164"],
		"10-digit US number must be normalized to +1 prefix")
	assert.Equal(t, "US", inner["CountryCodeIso2"])
}

func TestAudit6_PhoneNumberValidate_PhoneTypeAndCarrier(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
		map[string]any{
			"NumberValidateRequest": map[string]any{
				"PhoneNumber": "+12125551234",
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	inner := resp["NumberValidateResponse"].(map[string]any)
	assert.NotEmpty(t, inner["PhoneType"], "PhoneType must be set")
	assert.NotNil(t, inner["PhoneTypeCode"], "PhoneTypeCode must be present")
}

// ──────────────────────────────────────────────────
// Parity Phase 4: SendMessages/SendUsersMessages response envelope
// ──────────────────────────────────────────────────

// TestAudit6_SendMessages_ResponseEnvelope verifies SendMessages nests its
// result under a top-level "MessageResponse" key (matching
// SendMessagesOutput.MessageResponse in aws-sdk-go-v2), not a bare envelope.
func TestAudit6_SendMessages_ResponseEnvelope(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "send-messages-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/messages",
		map[string]any{
			"MessageRequest": map[string]any{
				"Addresses": map[string]any{
					"+15555550100": map[string]any{"ChannelType": "SMS"},
				},
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	envelope, ok := resp["MessageResponse"].(map[string]any)
	require.True(t, ok, "response must be nested under a MessageResponse key")
	assert.Equal(t, appID, envelope["ApplicationId"])

	result, ok := envelope["Result"].(map[string]any)
	require.True(t, ok)

	entry, ok := result["+15555550100"].(map[string]any)
	require.True(t, ok, "result must be keyed by address")
	assert.Equal(t, "SUCCESSFUL", entry["DeliveryStatus"])
	assert.NotEmpty(t, entry["MessageId"])
}

// ──────────────────────────────────────────────────
// Parity Phase 4: RemoveAttributes honors the Blacklist and AttributeType
// ──────────────────────────────────────────────────

// TestAudit6_RemoveAttributes_RemovesBlacklistedNamesOnly verifies
// RemoveAttributes parses the UpdateAttributesRequest.Blacklist from the
// request body and only deletes the named custom attributes -- not the
// whole endpoint-custom-attributes bucket, and not unrelated attributes.
func TestAudit6_RemoveAttributes_RemovesBlacklistedNamesOnly(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "remove-attrs-app")

	rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/endpoints/ep-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"Attributes": map[string]any{
				"Interests": []string{"Music"},
				"Plan":      []string{"Gold"},
			},
		})
	require.Equal(t, http.StatusAccepted, rec.Code)

	rec = doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/attributes/endpoint-custom-attributes",
		map[string]any{"Blacklist": []string{"Interests"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "endpoint-custom-attributes", resp["AttributeType"])

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/endpoints/ep-1", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ep map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ep))

	attrs, _ := ep["Attributes"].(map[string]any)
	_, hasInterests := attrs["Interests"]
	assert.False(t, hasInterests, "blacklisted attribute must be removed")

	_, hasPlan := attrs["Plan"]
	assert.True(t, hasPlan, "non-blacklisted attribute must survive")
}

// ──────────────────────────────────────────────────
// Parity Phase 4: VoiceTemplate tagging via ARN
// ──────────────────────────────────────────────────

// TestAudit6_VoiceTemplate_TagRoundTrip verifies a voice template is
// registered in the ARN index on creation so TagResource/ListTagsForResource
// work for it like every other template type.
func TestAudit6_VoiceTemplate_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-voice-template/voice",
		map[string]any{"Body": "Hello"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	templateARN, _ := createResp["Arn"].(string)
	require.NotEmpty(t, templateARN)

	escapedARN := url.PathEscape(templateARN)

	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+escapedARN,
		map[string]any{"tags": map[string]string{"env": "prod"}})
	require.Equal(t, http.StatusNoContent, tagRec.Code,
		"TagResource must find the voice template by ARN")

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+escapedARN, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))

	tags, _ := tagsResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
}

// ──────────────────────────────────────────────────
// Parity Phase 4: Campaign/Segment version history is app-scoped
// ──────────────────────────────────────────────────

// TestAudit6_GetCampaignVersions_CrossAppIsolation verifies a campaign
// belonging to one app is not visible through another app's versions path,
// even when the campaign ID happens to be known.
func TestAudit6_GetCampaignVersions_CrossAppIsolation(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	ownerAppID := createTestApp(t, h, "owner-app")
	otherAppID := createTestApp(t, h, "other-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+ownerAppID+"/campaigns",
		map[string]any{"Name": "campaign-a"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	campaignID, _ := createResp["Id"].(string)
	require.NotEmpty(t, campaignID)

	rec = doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+otherAppID+"/campaigns/"+campaignID+"/versions", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"a campaign from another app must not be reachable through this app's versions path")

	rec = doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+ownerAppID+"/campaigns/"+campaignID+"/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "the owning app must still see its own campaign versions")
}

// ──────────────────────────────────────────────────
// Parity Phase 4: APNS channel DefaultAuthenticationMethod wire field
// ──────────────────────────────────────────────────

// TestAudit6_ApnsChannel_DefaultAuthenticationMethod verifies the APNS
// channel family round-trips DefaultAuthenticationMethod under its real
// aws-sdk-go-v2 field name, not a shortened "DefaultAuthMethod".
func TestAudit6_ApnsChannel_DefaultAuthenticationMethod(t *testing.T) {
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
