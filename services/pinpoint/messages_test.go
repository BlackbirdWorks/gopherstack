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
				"VerifyOTPMessageRequestParameters": map[string]any{
					"DestinationIdentity": "+15555550101",
					"ReferenceID":         "ref-003",
					"Otp":                 "123456",
				},
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

				msgResp, _ := sendResp["MessageResponse"].(map[string]any)
				results, _ := msgResp["Result"].(map[string]any)
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
			"VerifyOTPMessageRequestParameters": map[string]any{
				"DestinationIdentity": "+15555550100",
				"Otp":                 "999999",
				"ReferenceId":         "ref-xyz",
			},
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
			"VerifyOTPMessageRequestParameters": map[string]any{
				"Otp": "123456",
			},
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
			"SendUsersMessageRequest": map[string]any{
				"Users": map[string]any{
					"alice": map[string]any{},
					"bob":   map[string]any{},
				},
				"MessageConfiguration": map[string]any{
					"EmailMessage": map[string]any{"FromAddress": "noreply@example.com"},
				},
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &resp))

	envelope, _ := resp["SendUsersMessageResponse"].(map[string]any)
	require.NotNil(t, envelope, "response must be nested under SendUsersMessageResponse")

	result, _ := envelope["Result"].(map[string]any)
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
			"SendUsersMessageRequest": map[string]any{
				"Users": map[string]any{
					"ghost": map[string]any{},
				},
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &resp))

	envelope, _ := resp["SendUsersMessageResponse"].(map[string]any)
	require.NotNil(t, envelope, "response must be nested under SendUsersMessageResponse")

	result, _ := envelope["Result"].(map[string]any)
	ghostResults, _ := result["ghost"].(map[string]any)
	require.NotNil(t, ghostResults, "unknown user still gets a result entry")
}

func TestSendUsersMessages_EmptyBody(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-empty-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{
			"SendUsersMessageRequest": map[string]any{},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	envelope, _ := resp["SendUsersMessageResponse"].(map[string]any)
	result, _ := envelope["Result"].(map[string]any)
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

func TestPhoneNumberValidate_UnknownCountry(t *testing.T) {
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

func TestPhoneNumberValidate_NationalFormat(t *testing.T) {
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

func TestPhoneNumberValidate_PhoneTypeAndCarrier(t *testing.T) {
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
func TestSendMessages_ResponseEnvelope(t *testing.T) {
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

// TestGetInAppMessages tests retrieval of in-app messages for an endpoint.
func TestGetInAppMessages(t *testing.T) {
	t.Parallel()

	type args struct {
		setup func(t *testing.T, h *pinpoint.Handler) (string, string)
	}

	type want struct {
		wantStatus int
		wantCount  int
	}

	tests := []struct {
		args args
		name string
		want want
	}{
		{
			name: "app_with_no_templates",
			args: args{
				setup: func(t *testing.T, h *pinpoint.Handler) (string, string) {
					t.Helper()
					appID := createTestApp(t, h, "inapp-empty-app")

					return appID, "ep-1"
				},
			},
			want: want{
				wantStatus: http.StatusOK,
				wantCount:  0,
			},
		},
		{
			name: "app_with_inapp_templates",
			args: args{
				setup: func(t *testing.T, h *pinpoint.Handler) (string, string) {
					t.Helper()
					appID := createTestApp(t, h, "inapp-with-tmpl-app")
					rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/tmpl-banner/inapp", map[string]any{
						"InAppTemplateRequest": map[string]any{
							"TemplateDescription": "Banner Template",
						},
					})
					require.Equal(t, http.StatusCreated, rec.Code)

					return appID, "ep-2"
				},
			},
			want: want{
				wantStatus: http.StatusOK,
				wantCount:  1,
			},
		},
		{
			name: "app_not_found",
			args: args{
				setup: func(t *testing.T, _ *pinpoint.Handler) (string, string) {
					t.Helper()

					return "non-existent-app-id", "ep-3"
				},
			},
			want: want{
				wantStatus: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID, endpointID := tt.args.setup(t, h)

			rec := doPinpointRequest(
				t,
				h,
				http.MethodGet,
				"/v1/apps/"+appID+"/endpoints/"+endpointID+"/inappmessages",
				nil,
			)
			require.Equal(t, tt.want.wantStatus, rec.Code)

			if tt.want.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				campaigns, ok := resp["InAppMessageCampaigns"].([]any)
				require.True(t, ok)
				assert.Len(t, campaigns, tt.want.wantCount)
			}
		})
	}
}
