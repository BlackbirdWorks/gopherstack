package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func TestOTP_SendAndVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		otpBody    map[string]any
		verifyBody map[string]any
		name       string
		wantSend   int
		wantVerify int
	}{
		{
			name: "send_otp_accepted",
			otpBody: map[string]any{
				"SendOTPMessageRequestParameters": map[string]any{
					"Channel":             "SMS",
					"DestinationIdentity": "+15555550100",
					"OriginationIdentity": "+15555550199",
					"ReferenceID":         "ref-001",
					"BrandName":           "MyApp",
					"CodeLength":          6,
					"ValidityPeriod":      5,
				},
			},
			wantSend: http.StatusOK,
		},
		{
			name: "send_otp_email_channel",
			otpBody: map[string]any{
				"SendOTPMessageRequestParameters": map[string]any{
					"Channel":             "EMAIL",
					"DestinationIdentity": "user@example.com",
					"OriginationIdentity": "noreply@example.com",
					"ReferenceID":         "ref-002",
					"BrandName":           "MyService",
					"CodeLength":          8,
				},
			},
			wantSend: http.StatusOK,
		},
		{
			name: "verify_otp",
			otpBody: map[string]any{
				"SendOTPMessageRequestParameters": map[string]any{
					"Channel":             "SMS",
					"DestinationIdentity": "+15555550101",
					"OriginationIdentity": "+15555550199",
					"ReferenceID":         "ref-003",
					"BrandName":           "MyApp",
					"CodeLength":          6,
				},
			},
			verifyBody: map[string]any{
				"DestinationIdentity": "+15555550101",
				"ReferenceID":         "ref-003",
				"Otp":                 "123456",
			},
			wantSend:   http.StatusOK,
			wantVerify: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "otp-app")

			sendRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/otp", tc.otpBody)
			assert.Equal(t, tc.wantSend, sendRec.Code,
				"send body: %s", sendRec.Body.String())

			if tc.verifyBody != nil {
				verifyRec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/apps/"+appID+"/verify-otp", tc.verifyBody)
				assert.Equal(t, tc.wantVerify, verifyRec.Code,
					"verify body: %s", verifyRec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Valid")
			}
		})
	}
}

// ──────────────────────────────────────────────────
// SMSChannel attributes: SenderId, ShortCode
// ──────────────────────────────────────────────────

func TestOTP_SendAndVerify_CodeMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantValid bool
		hasSent   bool
	}{
		{
			name:      "verify_after_send_is_valid",
			hasSent:   true,
			wantValid: true,
		},
		{
			name:      "verify_without_send_is_invalid",
			hasSent:   false,
			wantValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "otp-test-app")

			if tc.hasSent {
				sendRec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/apps/"+appID+"/otp", map[string]any{})
				require.Equal(t, http.StatusOK, sendRec.Code)

				var sendResp map[string]any
				require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &sendResp))

				results, _ := sendResp["Result"].(map[string]any)
				assert.NotEmpty(t, results, "SendOTP should return results")
			}

			verifyRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/verify-otp", map[string]any{})
			require.Equal(t, http.StatusOK, verifyRec.Code)

			var verifyResp map[string]any
			require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &verifyResp))
			assert.Equal(t, tc.wantValid, verifyResp["Valid"], "Valid field")
		})
	}
}

// ──────────────────────────────────────────────────
// Item 27: Basic pagination on template list
// ──────────────────────────────────────────────────

func TestOTP_CodeMatch_CorrectCode(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "otp-code-match-app")

	// Send OTP — backend stores the code internally.
	sendRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/otp",
		map[string]any{
			"SendOTPMessageRequestParameters": map[string]any{
				"Channel":             "SMS",
				"BrandName":           "TestBrand",
				"DestinationIdentity": "+15555550100",
				"OriginationIdentity": "+15555550199",
				"ReferenceId":         "ref-abc",
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	// Verify with no code provided → falls back to "was OTP sent?" → Valid=true.
	verifyNoCode := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/verify-otp",
		map[string]any{})
	require.Equal(t, http.StatusOK, verifyNoCode.Code)

	var noCodeResp map[string]any
	require.NoError(t, json.Unmarshal(verifyNoCode.Body.Bytes(), &noCodeResp))
	assert.True(t, noCodeResp["Valid"].(bool), "empty code falls back to has-pending-OTP check")
}

func TestOTP_CodeMatch_WrongCode(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "otp-wrong-code-app")

	// Send OTP.
	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/otp", map[string]any{})

	// Verify with a wrong code → Valid=false.
	verifyRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/verify-otp",
		map[string]any{
			"DestinationIdentity": "+15555550100",
			"Otp":                 "999999",
			"ReferenceId":         "ref-xyz",
		})
	require.Equal(t, http.StatusOK, verifyRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
	assert.False(t, resp["Valid"].(bool), "wrong OTP code must return Valid=false")
}

func TestOTP_CodeMatch_NoOTPSent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "otp-no-send-app")

	// Verify without ever sending an OTP → Valid=false even with a code.
	verifyRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/verify-otp",
		map[string]any{
			"Otp": "123456",
		})
	require.Equal(t, http.StatusOK, verifyRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
	assert.False(t, resp["Valid"].(bool), "no OTP sent → Valid=false")
}

// ──────────────────────────────────────────────────
// Finding #15: ChannelType validation on UpdateEndpoint
// ──────────────────────────────────────────────────

func TestSendUsersMessages_WithEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-app")

	// Register two endpoints for user "alice".
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-alice-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "alice@example.com",
			"User":        map[string]any{"UserId": "alice"},
		})
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-alice-2",
		map[string]any{
			"ChannelType": "SMS",
			"Address":     "+15555550100",
			"User":        map[string]any{"UserId": "alice"},
		})

	// Register one endpoint for user "bob".
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-bob-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "bob@example.com",
			"User":        map[string]any{"UserId": "bob"},
		})

	sendRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{
			"Users": map[string]any{
				"alice": map[string]any{},
				"bob":   map[string]any{},
			},
			"MessageConfiguration": map[string]any{
				"EmailMessage": map[string]any{"FromAddress": "noreply@example.com"},
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &resp))

	result, _ := resp["Result"].(map[string]any)
	require.NotNil(t, result)

	aliceResults, _ := result["alice"].(map[string]any)
	require.NotNil(t, aliceResults, "alice must have per-endpoint results")
	// Alice has two endpoints → two result entries.
	assert.Len(t, aliceResults, 2, "alice has 2 endpoints → 2 result entries")

	for epID, v := range aliceResults {
		r := v.(map[string]any)
		assert.Equal(t, "SUCCESSFUL", r["DeliveryStatus"],
			"endpoint %s should be SUCCESSFUL", epID)
		assert.NotEmpty(t, r["MessageId"])
	}

	bobResults, _ := result["bob"].(map[string]any)
	require.NotNil(t, bobResults, "bob must have per-endpoint results")
	assert.Len(t, bobResults, 1, "bob has 1 endpoint → 1 result entry")
}

func TestSendUsersMessages_UnknownUser(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-noendpoint-app")

	// User "ghost" has no registered endpoints.
	sendRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{
			"Users": map[string]any{
				"ghost": map[string]any{},
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &resp))

	result, _ := resp["Result"].(map[string]any)
	require.NotNil(t, result)

	ghostResults, _ := result["ghost"].(map[string]any)
	require.NotNil(t, ghostResults, "unknown user still gets a result entry")
}

func TestSendUsersMessages_EmptyBody(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-empty-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	result, _ := resp["Result"].(map[string]any)
	assert.Empty(t, result, "no users in request → empty result")
}

// ──────────────────────────────────────────────────
// Finding #27: Pagination for list operations
// ──────────────────────────────────────────────────

func TestPhoneNumberValidate_E164Input(t *testing.T) {
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
					"PhoneNumber": tc.phoneNumber,
				})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			_, wrapped := resp["NumberValidateResponse"]
			assert.False(t, wrapped, "NumberValidateResponse must not appear as a wrapper key")

			inner := resp
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

func TestPhoneNumberValidate_UnknownCountry(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
		map[string]any{
			"PhoneNumber": "+99912345678",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	inner := resp
	assert.Equal(t, "ZZ", inner["CountryCodeIso2"], "unknown country returns ZZ")
	assert.Equal(t, "+99912345678", inner["OriginalPhoneNumber"])
	assert.Equal(t, "+99912345678", inner["CleansedPhoneNumberE164"])
}

func TestPhoneNumberValidate_NationalFormat(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// 10-digit US number without + prefix should be normalized.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
		map[string]any{
			"PhoneNumber": "2125551234",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	inner := resp
	assert.Equal(t, "+12125551234", inner["CleansedPhoneNumberE164"],
		"10-digit US number must be normalized to +1 prefix")
	assert.Equal(t, "US", inner["CountryCodeIso2"])
}

func TestPhoneNumberValidate_PhoneTypeAndCarrier(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/phone/number/validate",
		map[string]any{
			"PhoneNumber": "+12125551234",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	inner := resp
	assert.NotEmpty(t, inner["PhoneType"], "PhoneType must be set")
	assert.NotNil(t, inner["PhoneTypeCode"], "PhoneTypeCode must be present")
}

// ──────────────────────────────────────────────────
// Parity Phase 4: SendMessages response body
// ──────────────────────────────────────────────────

// TestSendMessages_ResponseFlat verifies SendMessages returns MessageResponse's
// fields at the top level of the body, not nested under a "MessageResponse"
// key. gopherstack-lffs (re-audit of gopherstack-6flj): pinpoint@v1.42.4's
// awsRestjson1_deserializeOpSendMessages decodes the whole HTTP body directly
// into output.MessageResponse (deserializers.go) -- there is no wrapper key on
// the wire, and a real client reading a "MessageResponse"-nested body would
// see the required ApplicationId/Result fields as absent. See also
// TestSendMessages_RealClient in messages_wrapper_fix_test.go for the
// real-SDK-client version of this coverage.
func TestSendMessages_ResponseFlat(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "send-messages-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/messages",
		map[string]any{
			"Addresses": map[string]any{
				"+15555550100": map[string]any{"ChannelType": "SMS"},
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, wrapped := resp["MessageResponse"]
	assert.False(t, wrapped, "MessageResponse must not appear as a wrapper key")
	assert.Equal(t, appID, resp["ApplicationId"])

	result, ok := resp["Result"].(map[string]any)
	require.True(t, ok)

	entry, ok := result["+15555550100"].(map[string]any)
	require.True(t, ok, "result must be keyed by address")
	assert.Equal(t, "SUCCESSFUL", entry["DeliveryStatus"])
	assert.NotEmpty(t, entry["MessageId"])
}

// createTestInAppCampaign creates an in-app template plus a campaign that
// targets it via TemplateConfiguration.InAppTemplate.Name -- the real way
// AWS associates a campaign with an in-app template (pinpoint@v1.42.4
// types/types.go's TemplateConfiguration.InAppTemplate). Both requests are
// sent in their real FLAT wire shape (no "InAppTemplateRequest"/
// "WriteCampaignRequest" wrapper key -- those types ARE the request body
// directly; see messages_wrapper_fix_test.go's package doc comment).
// Returns the created campaign's id.
func createTestInAppCampaign(
	t *testing.T, h *pinpoint.Handler, appID, templateName string, priority int, paused bool,
) string {
	t.Helper()

	tmplRec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/"+templateName+"/inapp", map[string]any{
		"Layout": "BOTTOM_BANNER",
		"Content": []map[string]any{
			{"BackgroundColor": "#FFFFFF"},
		},
	})
	require.Equal(t, http.StatusCreated, tmplRec.Code, tmplRec.Body.String())

	campRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", map[string]any{
		"Name":      templateName + "-campaign",
		"SegmentId": "seg-001",
		"Priority":  priority,
		"IsPaused":  paused,
		"TemplateConfiguration": map[string]any{
			"InAppTemplate": map[string]any{"Name": templateName},
		},
	})
	require.Equal(t, http.StatusCreated, campRec.Code, campRec.Body.String())

	var camp map[string]any
	require.NoError(t, json.Unmarshal(campRec.Body.Bytes(), &camp))
	campaignID, _ := camp["Id"].(string)
	require.NotEmpty(t, campaignID)

	return campaignID
}

// TestGetInAppMessages tests retrieval of in-app messages for an endpoint.
// gopherstack has no segment/dimension matching engine, so every non-paused
// campaign in the app that targets an in-app template is returned
// regardless of endpointID (see in_app_messages.go's package doc comment) --
// these cases exercise what IS implemented rather than asserting a specific
// endpoint is excluded.
func TestGetInAppMessages(t *testing.T) {
	t.Parallel()

	t.Run("no_campaigns_returns_empty_not_null_list", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		appID := createTestApp(t, h, "inapp-empty-app")

		rec := doPinpointRequest(t, h, http.MethodGet,
			"/v1/apps/"+appID+"/endpoints/ep-1/inappmessages", nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		campaigns, ok := resp["InAppMessageCampaigns"].([]any)
		require.True(t, ok, "InAppMessageCampaigns must be present and be an array")
		assert.Empty(t, campaigns)
	})

	t.Run("matching_in_app_campaign_returns_template_content", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		appID := createTestApp(t, h, "inapp-with-campaign-app")
		campaignID := createTestInAppCampaign(t, h, appID, "gim-template", 5, false)

		rec := doPinpointRequest(t, h, http.MethodGet,
			"/v1/apps/"+appID+"/endpoints/ep-2/inappmessages", nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		campaigns, ok := resp["InAppMessageCampaigns"].([]any)
		require.True(t, ok)
		require.Len(t, campaigns, 1)

		got, ok := campaigns[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, campaignID, got["CampaignId"])
		assert.InDelta(t, 5, got["Priority"], 0)

		msg, ok := got["InAppMessage"].(map[string]any)
		require.True(t, ok, "InAppMessage must carry the template's real content, not be absent")
		assert.Equal(t, "BOTTOM_BANNER", msg["Layout"])

		content, ok := msg["Content"].([]any)
		require.True(t, ok)
		require.Len(t, content, 1)
		first, ok := content[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "#FFFFFF", first["BackgroundColor"])
	})

	t.Run("paused_campaign_excluded", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		appID := createTestApp(t, h, "inapp-paused-app")
		createTestInAppCampaign(t, h, appID, "paused-template", 1, true)

		rec := doPinpointRequest(t, h, http.MethodGet,
			"/v1/apps/"+appID+"/endpoints/ep-3/inappmessages", nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		campaigns, ok := resp["InAppMessageCampaigns"].([]any)
		require.True(t, ok)
		assert.Empty(t, campaigns, "a paused campaign doesn't run, per real AWS's IsPaused semantics")
	})

	t.Run("app_not_found", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)

		rec := doPinpointRequest(t, h, http.MethodGet,
			"/v1/apps/non-existent-app-id/endpoints/ep-4/inappmessages", nil)
		require.Equal(t, http.StatusNotFound, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "NotFoundException", resp["__type"])
	})
}
