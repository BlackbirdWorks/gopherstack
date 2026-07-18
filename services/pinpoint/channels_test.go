package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannel_PerTypeFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body              map[string]any
		name              string
		channelType       string
		wantStatus        int
		wantHasCredential bool
		wantHasTokenKey   bool
		wantEnabled       bool
	}{
		{
			name:        "gcm_with_api_key",
			channelType: "gcm",
			body: map[string]any{
				"ApiKey":  "AIzaSy-real-key",
				"Enabled": true,
			},
			wantStatus:        http.StatusOK,
			wantHasCredential: true,
			wantEnabled:       true,
		},
		{
			name:        "apns_with_certificate",
			channelType: "apns",
			body: map[string]any{
				"BundleId":                    "com.example.app",
				"Certificate":                 "-----BEGIN CERTIFICATE-----",
				"DefaultAuthenticationMethod": "CERTIFICATE",
				"Enabled":                     true,
			},
			wantStatus:        http.StatusOK,
			wantHasCredential: true,
			wantEnabled:       true,
		},
		{
			name:        "apns_with_token_key",
			channelType: "apns",
			body: map[string]any{
				"BundleId":                    "com.example.app",
				"TokenKey":                    "-----BEGIN PRIVATE KEY-----",
				"TokenKeyId":                  "key123",
				"TeamId":                      "TEAM123",
				"DefaultAuthenticationMethod": "TOKEN",
				"Enabled":                     true,
			},
			wantStatus:      http.StatusOK,
			wantHasTokenKey: true,
			wantEnabled:     true,
		},
		{
			name:        "email_with_from_address",
			channelType: "email",
			body: map[string]any{
				"FromAddress": "noreply@example.com",
				"Identity":    "arn:aws:ses:us-east-1:123:identity/example.com",
				"RoleArn":     "arn:aws:iam::123:role/PinpointRole",
				"Enabled":     true,
			},
			wantStatus:        http.StatusOK,
			wantHasCredential: true,
			wantEnabled:       true,
		},
		{
			name:        "sms_with_sender_id",
			channelType: "sms",
			body: map[string]any{
				"SenderId":  "MyBrand",
				"ShortCode": "12345",
				"Enabled":   true,
			},
			wantStatus:  http.StatusOK,
			wantEnabled: true,
		},
		{
			name:        "adm_with_client_id",
			channelType: "adm",
			body: map[string]any{
				"ClientId":     "amzn1.application.123",
				"ClientSecret": "secret",
				"Enabled":      true,
			},
			wantStatus:        http.StatusOK,
			wantHasCredential: true,
			wantEnabled:       true,
		},
		{
			name:        "baidu_with_api_key",
			channelType: "baidu",
			body: map[string]any{
				"ApiKey":    "baidu-api-key",
				"SecretKey": "baidu-secret",
				"Enabled":   true,
			},
			wantStatus:        http.StatusOK,
			wantHasCredential: true,
			wantEnabled:       true,
		},
		{
			name:        "channel_version_increments",
			channelType: "gcm",
			body: map[string]any{
				"ApiKey":  "key-v1",
				"Enabled": true,
			},
			wantStatus:  http.StatusOK,
			wantEnabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
			require.Equal(t, http.StatusCreated, appRec.Code)
			var appResp map[string]any
			require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
			appID := appResp["Id"].(string)

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/channels/"+tc.channelType, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if rec.Code != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, tc.wantEnabled, resp["Enabled"])
			assert.NotEmpty(t, resp["CreationDate"])
			assert.InDelta(t, float64(1), resp["Version"], 0.001)

			if tc.wantHasCredential {
				assert.Equal(t, true, resp["HasCredential"])
			}

			if tc.wantHasTokenKey {
				assert.Equal(t, true, resp["HasTokenKey"])
			}

			// GET the channel back.
			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/channels/"+tc.channelType, nil)
			require.Equal(t, http.StatusOK, getRec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, resp["Enabled"], getResp["Enabled"])
			assert.Equal(t, resp["HasCredential"], getResp["HasCredential"])
		})
	}
}

func TestChannel_VersionIncrements(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	for i := 1; i <= 3; i++ {
		rec := doPinpointRequest(t, h, http.MethodPut,
			"/v1/apps/"+appID+"/channels/gcm",
			map[string]any{"ApiKey": "key", "Enabled": true})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.InDelta(t, float64(i), resp["Version"], 0.001, "version should increment on each put")
	}
}

func TestChannel_CreationDatePreserved(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	rec1 := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/gcm",
		map[string]any{"ApiKey": "key", "Enabled": true})
	require.Equal(t, http.StatusOK, rec1.Code)
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
	creationDate1 := r1["CreationDate"].(string)

	rec2 := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/channels/gcm",
		map[string]any{"ApiKey": "key-updated", "Enabled": true})
	require.Equal(t, http.StatusOK, rec2.Code)
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
	creationDate2 := r2["CreationDate"].(string)

	assert.Equal(t, creationDate1, creationDate2, "CreationDate should be preserved on update")
}

// ──────────────────────────────────────────────────
// Recommender validation: IdType enum, conditional LastModifiedDate
// ──────────────────────────────────────────────────

func TestChannel_AllChannelsListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	channelTypes := []string{"gcm", "email", "sms"}
	bodies := []map[string]any{
		{"ApiKey": "gcm-key", "Enabled": true},
		{"FromAddress": "noreply@example.com", "Enabled": true},
		{"SenderId": "MySender", "Enabled": true},
	}

	for i, ct := range channelTypes {
		rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/"+ct, bodies[i])
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	channels, _ := listResp["Channels"].(map[string]any)
	assert.Len(t, channels, 3)
}

func TestSMSChannel_Attributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantEnabled   bool
		wantSenderID  bool
		wantShortCode bool
	}{
		{
			name: "basic_sms_channel_enable",
			body: map[string]any{
				"Enabled": true,
			},
			wantEnabled: true,
		},
		{
			name: "sms_with_sender_id",
			body: map[string]any{
				"Enabled":  true,
				"SenderId": "MYAPP",
			},
			wantEnabled:  true,
			wantSenderID: true,
		},
		{
			name: "sms_with_short_code",
			body: map[string]any{
				"Enabled":   true,
				"ShortCode": "55555",
			},
			wantEnabled:   true,
			wantShortCode: true,
		},
		{
			name: "sms_with_sender_and_shortcode",
			body: map[string]any{
				"Enabled":   true,
				"SenderId":  "BRAND",
				"ShortCode": "99999",
			},
			wantEnabled:   true,
			wantSenderID:  true,
			wantShortCode: true,
		},
		{
			name: "sms_channel_disable",
			body: map[string]any{
				"Enabled": false,
			},
			wantEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "sms-ch-app")

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/channels/sms", tc.body)
			require.Equal(t, http.StatusOK, rec.Code,
				"body: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantEnabled, resp["Enabled"])

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/channels/sms", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var gr map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gr))
			chType, _ := gr["ChannelType"].(string)
			assert.True(t, chType == "SMS" || chType == "sms", "expected SMS channel type, got %q", chType)
		})
	}
}

// ──────────────────────────────────────────────────
// EmailIdentities (verify): Email channel Identity field
// ──────────────────────────────────────────────────

func TestEmailChannel_Identity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantEnabled bool
	}{
		{
			name: "email_channel_with_ses_identity",
			body: map[string]any{
				"Enabled":     true,
				"FromAddress": "noreply@example.com",
				"Identity":    "arn:aws:ses:us-east-1:123456789012:identity/example.com",
				"RoleArn":     "arn:aws:iam::123456789012:role/PinpointEmailRole",
			},
			wantEnabled: true,
		},
		{
			name: "email_channel_with_email_identity",
			body: map[string]any{
				"Enabled":     true,
				"FromAddress": "sender@corp.com",
				"Identity":    "arn:aws:ses:us-east-1:123456789012:identity/sender@corp.com",
			},
			wantEnabled: true,
		},
		{
			name: "email_channel_with_configuration_set",
			body: map[string]any{
				"Enabled":          true,
				"FromAddress":      "noreply@brand.com",
				"Identity":         "arn:aws:ses:us-east-1:123456789012:identity/brand.com",
				"ConfigurationSet": "my-config-set",
			},
			wantEnabled: true,
		},
		{
			name: "disable_email_channel",
			body: map[string]any{
				"Enabled":  false,
				"Identity": "arn:aws:ses:us-east-1:123456789012:identity/example.com",
			},
			wantEnabled: false,
		},
		{
			name: "email_channel_update_from_address",
			body: map[string]any{
				"Enabled":     true,
				"FromAddress": "updated@example.com",
				"Identity":    "arn:aws:ses:us-east-1:123456789012:identity/example.com",
			},
			wantEnabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "email-ch-app")

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/channels/email", tc.body)
			require.Equal(t, http.StatusOK, rec.Code,
				"body: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantEnabled, resp["Enabled"])
			chType, _ := resp["ChannelType"].(string)
			assert.True(t, chType == "EMAIL" || chType == "email", "expected EMAIL channel type, got %q", chType)

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/channels/email", nil)
			require.Equal(t, http.StatusOK, getRec.Code)
		})
	}
}

// ──────────────────────────────────────────────────
// Backend: RecommenderConfiguration via direct API
// ──────────────────────────────────────────────────

func TestChannel_Delete_ReturnsChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody  map[string]any
		channelPath string
		channelType string
	}{
		{
			channelPath: "adm",
			channelType: "adm",
			updateBody:  map[string]any{"ClientId": "adm-id", "ClientSecret": "secret", "Enabled": true},
		},
		{
			channelPath: "baidu",
			channelType: "baidu",
			updateBody:  map[string]any{"ApiKey": "key", "SecretKey": "secret", "Enabled": true},
		},
		{
			channelPath: "gcm",
			channelType: "gcm",
			updateBody:  map[string]any{"ApiKey": "gcm-key", "Enabled": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.channelType, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "channel-del-"+tc.channelPath+"-app")

			doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/channels/"+tc.channelPath, tc.updateBody)

			delRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/apps/"+appID+"/channels/"+tc.channelPath, nil)
			require.Equal(t, http.StatusOK, delRec.Code)

			var ch map[string]any
			require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &ch))
			assert.Equal(t, tc.channelType, ch["ChannelType"])
		})
	}
}

// ──────────────────────────────────────────────────
// Endpoint user attribute updates
// ──────────────────────────────────────────────────

func TestGetChannels_IncludesExtraFields(t *testing.T) {
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

// TestCoverage_ChannelOperations covers GetChannel, GetChannels, UpdateChannel, DeleteChannel.
func TestChannelOperations(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-ops-app")

	// GetChannels.
	rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateChannel (email).
	rec = doPinpointRequest(
		t,
		h,
		http.MethodPut,
		"/v1/apps/"+appID+"/channels/email",
		map[string]any{
			"Enabled":     true,
			"FromAddress": "test@example.com",
			"Identity":    "arn:aws:ses:us-east-1:123:identity/test@example.com",
		},
	)
	assert.True(t, rec.Code == http.StatusOK || rec.Code == http.StatusCreated, "expected 200 or 201, got %d", rec.Code)

	// GetChannel (email).
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/email", nil)
	assert.True(
		t,
		rec.Code == http.StatusOK || rec.Code == http.StatusNotFound,
		"expected 200 or 404, got %d",
		rec.Code,
	)

	// DeleteChannel (email).
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/channels/email", nil)
	assert.True(
		t,
		rec.Code == http.StatusOK || rec.Code == http.StatusNotFound,
		"expected 200 or 404, got %d",
		rec.Code,
	)
}
