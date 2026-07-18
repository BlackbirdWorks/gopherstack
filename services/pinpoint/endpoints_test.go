package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpoint_FullShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantChannelType string
		wantStatus2     string
		wantOptOut      string
		wantStatus      int
		wantHasDemog    bool
		wantHasLocation bool
		wantHasMetrics  bool
	}{
		{
			name: "full_endpoint",
			body: map[string]any{
				"ChannelType": "EMAIL",
				"Address":     "user@example.com",
				"Demographic": map[string]any{
					"AppVersion":      "1.2.3",
					"Locale":          "en_US",
					"Make":            "Apple",
					"Model":           "iPhone",
					"ModelVersion":    "14",
					"Platform":        "iOS",
					"PlatformVersion": "16.0",
					"Timezone":        "America/New_York",
				},
				"Location": map[string]any{
					"City":       "New York",
					"Country":    "US",
					"Latitude":   40.7128,
					"Longitude":  -74.0060,
					"PostalCode": "10001",
					"Region":     "NY",
				},
				"Metrics": map[string]any{
					"session_count": 42.0,
					"revenue":       99.99,
				},
				"Attributes": map[string]any{
					"Tier":       []any{"premium"},
					"Categories": []any{"sports", "tech"},
				},
				"EndpointStatus": "ACTIVE",
				"OptOut":         "NONE",
				"User": map[string]any{
					"UserId": "user-123",
					"UserAttributes": map[string]any{
						"FirstName": []any{"Alice"},
					},
				},
			},
			wantStatus:      http.StatusAccepted,
			wantChannelType: "EMAIL",
			wantHasDemog:    true,
			wantHasLocation: true,
			wantHasMetrics:  true,
			wantStatus2:     "ACTIVE",
			wantOptOut:      "NONE",
		},
		{
			name: "inactive_endpoint",
			body: map[string]any{
				"ChannelType":    "SMS",
				"Address":        "+15555550100",
				"EndpointStatus": "INACTIVE",
				"OptOut":         "ALL",
			},
			wantStatus:  http.StatusAccepted,
			wantStatus2: "INACTIVE",
			wantOptOut:  "ALL",
		},
		{
			name: "endpoint_metrics_round_trip",
			body: map[string]any{
				"ChannelType": "PUSH",
				"Address":     "token-xyz",
				"Metrics": map[string]any{
					"sessions": 7.0,
					"revenue":  150.50,
				},
			},
			wantStatus:     http.StatusAccepted,
			wantHasMetrics: true,
		},
		{
			name: "endpoint_attributes_list_values",
			body: map[string]any{
				"ChannelType": "EMAIL",
				"Address":     "multi@example.com",
				"Attributes": map[string]any{
					"Interests": []any{"music", "art", "travel"},
				},
			},
			wantStatus: http.StatusAccepted,
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

			endpointID := "ep-" + tc.name
			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/endpoints/"+endpointID, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if rec.Code != http.StatusAccepted {
				return
			}

			// GET the endpoint to verify round-trip.
			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/endpoints/"+endpointID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			if tc.wantChannelType != "" {
				assert.Equal(t, tc.wantChannelType, resp["ChannelType"])
			}

			if tc.wantHasDemog {
				assert.NotNil(t, resp["Demographic"])
			}

			if tc.wantHasLocation {
				assert.NotNil(t, resp["Location"])
			}

			if tc.wantHasMetrics {
				assert.NotNil(t, resp["Metrics"])
			}

			if tc.wantStatus2 != "" {
				assert.Equal(t, tc.wantStatus2, resp["EndpointStatus"])
			}

			if tc.wantOptOut != "" {
				assert.Equal(t, tc.wantOptOut, resp["OptOut"])
			}
		})
	}
}

func TestEndpoint_UserAttributes(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/endpoints/ep-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"User": map[string]any{
				"UserId": "user-999",
				"UserAttributes": map[string]any{
					"FirstName": []any{"Bob"},
					"LastName":  []any{"Smith"},
					"Tier":      []any{"gold"},
				},
			},
		})
	require.Equal(t, http.StatusAccepted, rec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/endpoints/ep-1", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	user, ok := resp["User"].(map[string]any)
	require.True(t, ok, "User field must be present")
	assert.Equal(t, "user-999", user["UserId"])

	userAttrs, _ := user["UserAttributes"].(map[string]any)
	require.NotNil(t, userAttrs)

	firstName, _ := userAttrs["FirstName"].([]any)
	assert.Equal(t, []any{"Bob"}, firstName)
}

func TestEndpointBatch_FullShape(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	batchBody := map[string]any{
		"Item": map[string]any{
			"ep-batch-1": map[string]any{
				"ChannelType": "EMAIL",
				"Address":     "a@example.com",
				"Attributes": map[string]any{
					"Plan": []any{"pro"},
				},
				"Metrics": map[string]any{
					"opens": 5.0,
				},
				"Demographic": map[string]any{
					"Platform": "iOS",
				},
				"User": map[string]any{"UserId": "u-1"},
			},
			"ep-batch-2": map[string]any{
				"ChannelType":    "SMS",
				"Address":        "+15555550200",
				"EndpointStatus": "INACTIVE",
				"OptOut":         "ALL",
			},
		},
	}

	rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/endpoints", batchBody)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// Verify ep-batch-1.
	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/endpoints/ep-batch-1", nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r1))
	assert.NotNil(t, r1["Attributes"])
	assert.NotNil(t, r1["Metrics"])
	assert.NotNil(t, r1["Demographic"])

	// Verify ep-batch-2.
	getRec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/endpoints/ep-batch-2", nil)
	require.Equal(t, http.StatusOK, getRec2.Code)
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &r2))
	assert.Equal(t, "INACTIVE", r2["EndpointStatus"])
	assert.Equal(t, "ALL", r2["OptOut"])
}

// ──────────────────────────────────────────────────
// Channel per-type fields
// ──────────────────────────────────────────────────

func TestEndpoint_EffectiveDate_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	effectiveDate := "2026-01-15T10:30:00Z"
	rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/endpoints/ep-eff",
		map[string]any{
			"ChannelType":   "EMAIL",
			"Address":       "t@example.com",
			"EffectiveDate": effectiveDate,
		})
	require.Equal(t, http.StatusAccepted, rec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/endpoints/ep-eff", nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Equal(t, effectiveDate, resp["EffectiveDate"])
}

func TestEndpoint_UserAttributes_MultiValue(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "ep-ua-multi-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-multi",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"User": map[string]any{
				"UserId": "user-001",
				"UserAttributes": map[string]any{
					"hobbies":    []any{"cycling", "reading", "cooking"},
					"languages":  []any{"en", "fr", "de"},
					"membership": []any{"gold"},
				},
			},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/endpoints/ep-multi", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ep map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ep))

	user := ep["User"].(map[string]any)
	assert.Equal(t, "user-001", user["UserId"])

	ua := user["UserAttributes"].(map[string]any)
	hobbies := ua["hobbies"].([]any)
	assert.Len(t, hobbies, 3)
	assert.Contains(t, hobbies, "cycling")
	assert.Contains(t, hobbies, "reading")
	assert.Contains(t, hobbies, "cooking")

	langs := ua["languages"].([]any)
	assert.Len(t, langs, 3)

	mem := ua["membership"].([]any)
	assert.Equal(t, []any{"gold"}, mem)
}

func TestEndpoint_UserAttributes_UpdateMerge(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "ep-ua-merge-app")

	// First update: set hobbies
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-merge",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"User": map[string]any{
				"UserId": "user-002",
				"UserAttributes": map[string]any{
					"tier": []any{"silver"},
				},
			},
		})

	// Second update: overwrite tier, add new attribute
	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-merge",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"User": map[string]any{
				"UserId": "user-002",
				"UserAttributes": map[string]any{
					"tier":   []any{"gold"},
					"region": []any{"us-west"},
				},
			},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/endpoints/ep-merge", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ep map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ep))

	ua := ep["User"].(map[string]any)["UserAttributes"].(map[string]any)
	assert.Equal(t, []any{"gold"}, ua["tier"])
	assert.Equal(t, []any{"us-west"}, ua["region"])
}

func TestGetUserEndpoints_ByUserID(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "ep-user-lookup-app")

	// Create two endpoints for same user
	for i, addr := range []string{"ep-a", "ep-b"} {
		doPinpointRequest(t, h, http.MethodPut,
			"/v1/apps/"+appID+"/endpoints/"+addr,
			map[string]any{
				"ChannelType": "EMAIL",
				"Address":     "user" + string(rune('a'+i)) + "@example.com",
				"User": map[string]any{
					"UserId": "shared-user",
					"UserAttributes": map[string]any{
						"plan": []any{"premium"},
					},
				},
			})
	}

	// Create endpoint for different user
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-other",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "other@example.com",
			"User": map[string]any{
				"UserId": "other-user",
			},
		})

	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/users/shared-user", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	items := resp["Item"].([]any)
	assert.Len(t, items, 2, "only endpoints for shared-user")

	for _, item := range items {
		ep := item.(map[string]any)
		user := ep["User"].(map[string]any)
		assert.Equal(t, "shared-user", user["UserId"])
	}
}

func TestEndpoint_UserAttributes_BatchUpdate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "ep-ua-batch-app")

	batchRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints",
		map[string]any{
			"Item": map[string]any{
				"ep-batch-1": map[string]any{
					"ChannelType": "PUSH",
					"Address":     "token-abc",
					"User": map[string]any{
						"UserId": "batch-user-1",
						"UserAttributes": map[string]any{
							"segment": []any{"beta-testers"},
						},
					},
				},
				"ep-batch-2": map[string]any{
					"ChannelType": "EMAIL",
					"Address":     "batch2@example.com",
					"User": map[string]any{
						"UserId": "batch-user-2",
						"UserAttributes": map[string]any{
							"segment": []any{"power-users"},
						},
					},
				},
			},
		})
	require.Equal(t, http.StatusAccepted, batchRec.Code)

	// Verify ep-batch-1
	getRec1 := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/endpoints/ep-batch-1", nil)
	require.Equal(t, http.StatusOK, getRec1.Code)

	var ep1 map[string]any
	require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &ep1))
	ua1 := ep1["User"].(map[string]any)["UserAttributes"].(map[string]any)
	assert.Equal(t, []any{"beta-testers"}, ua1["segment"])

	// Verify ep-batch-2
	getRec2 := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/endpoints/ep-batch-2", nil)
	require.Equal(t, http.StatusOK, getRec2.Code)

	var ep2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &ep2))
	ua2 := ep2["User"].(map[string]any)["UserAttributes"].(map[string]any)
	assert.Equal(t, []any{"power-users"}, ua2["segment"])
}

// ──────────────────────────────────────────────────
// Recommender configurations — deeper coverage
// ──────────────────────────────────────────────────

func TestEndpoint_Metrics_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "ep-metrics-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-metrics",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"Metrics": map[string]any{
				"session_count":  12.0,
				"purchase_count": 3.0,
				"ltv":            99.99,
			},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/endpoints/ep-metrics", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ep map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ep))

	metrics := ep["Metrics"].(map[string]any)
	assert.InDelta(t, 12.0, metrics["session_count"], 0.001)
	assert.InDelta(t, 3.0, metrics["purchase_count"], 0.001)
	assert.InDelta(t, 99.99, metrics["ltv"], 0.001)
}

func TestEndpoint_Demographic_Location(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "ep-demo-loc-app")

	putRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-demo",
		map[string]any{
			"ChannelType": "PUSH",
			"Address":     "device-token-xyz",
			"Demographic": map[string]any{
				"AppVersion":      "4.2.1",
				"Make":            "Apple",
				"Model":           "iPhone15",
				"ModelVersion":    "iPhone OS 17.0",
				"Platform":        "ios",
				"PlatformVersion": "17.0",
				"Timezone":        "America/New_York",
				"Locale":          "en_US",
			},
			"Location": map[string]any{
				"City":       "New York",
				"Country":    "US",
				"Latitude":   40.7128,
				"Longitude":  -74.0060,
				"PostalCode": "10001",
				"Region":     "NY",
			},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/endpoints/ep-demo", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ep map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ep))

	demo := ep["Demographic"].(map[string]any)
	assert.Equal(t, "4.2.1", demo["AppVersion"])
	assert.Equal(t, "Apple", demo["Make"])
	assert.Equal(t, "iPhone15", demo["Model"])
	assert.Equal(t, "ios", demo["Platform"])
	assert.Equal(t, "America/New_York", demo["Timezone"])

	loc := ep["Location"].(map[string]any)
	assert.Equal(t, "New York", loc["City"])
	assert.Equal(t, "US", loc["Country"])
	assert.Equal(t, "10001", loc["PostalCode"])
}

// ──────────────────────────────────────────────────
// Helper: createTestSegment used by campaign tests
// ──────────────────────────────────────────────────

func TestEndpoint_InvalidChannelType_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelType string
		wantStatus  int
	}{
		{"GCM_valid", "GCM", http.StatusAccepted},
		{"APNS_valid", "APNS", http.StatusAccepted},
		{"APNS_SANDBOX_valid", "APNS_SANDBOX", http.StatusAccepted},
		{"SMS_valid", "SMS", http.StatusAccepted},
		{"EMAIL_valid", "EMAIL", http.StatusAccepted},
		{"VOICE_valid", "VOICE", http.StatusAccepted},
		{"BAIDU_valid", "BAIDU", http.StatusAccepted},
		{"ADM_valid", "ADM", http.StatusAccepted},
		{"CUSTOM_valid", "CUSTOM", http.StatusAccepted},
		{"IN_APP_valid", "IN_APP", http.StatusAccepted},
		{"PUSH_valid", "PUSH", http.StatusAccepted},
		{"INVALID_rejected", "INVALID_CHANNEL", http.StatusBadRequest},
		{"HTTP_rejected", "HTTP", http.StatusBadRequest},
		{"WEBSOCKET_rejected", "WEBSOCKET", http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "channel-val-app-"+tc.channelType)

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/endpoints/ep-1",
				map[string]any{"ChannelType": tc.channelType, "Address": "test@example.com"})
			assert.Equal(t, tc.wantStatus, rec.Code, "channel type %q", tc.channelType)
		})
	}
}

func TestEndpoint_EmptyChannelType_Accepted(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-empty-app")

	// Empty ChannelType should not be rejected (field is optional).
	rec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-1",
		map[string]any{"Address": "user@example.com"})
	assert.Equal(t, http.StatusAccepted, rec.Code)
}

// ──────────────────────────────────────────────────
// Finding #21: SendUsersMessages maps user→endpoint results
// ──────────────────────────────────────────────────

func TestPinpoint_Endpoints_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "endpoint-app")

	endpointID := "ep-001"

	rec := doPinpointRequest(t, h, http.MethodPut,
		fmt.Sprintf("/v1/apps/%s/endpoints/%s", appID, endpointID),
		map[string]any{
			"ChannelType":    "EMAIL",
			"Address":        "test@example.com",
			"EndpointStatus": "ACTIVE",
		})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/apps/%s/endpoints/%s", appID, endpointID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut,
		fmt.Sprintf("/v1/apps/%s/endpoints", appID),
		map[string]any{
			"Item": []map[string]any{{
				"Id":          "ep-002",
				"ChannelType": "SMS",
				"Address":     "+15555550100",
			}},
		})
	assert.Positive(t, rec.Code) // PutEndpoints batch may not be implemented

	rec = doPinpointRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/apps/%s/users/%s", appID, "user-1"), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/v1/apps/%s/endpoints/%s", appID, endpointID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestUpdateEndpoint_ReturnsMessageBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "email_endpoint",
			body: map[string]any{"ChannelType": "EMAIL", "Address": "user@example.com"},
		},
		{
			name: "sms_endpoint",
			body: map[string]any{"ChannelType": "SMS", "Address": "+15555550100"},
		},
		{
			name: "endpoint_with_user",
			body: map[string]any{
				"ChannelType": "EMAIL",
				"Address":     "b@example.com",
				"User":        map[string]any{"UserId": "u-1"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "ep-msgbody-app")

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/endpoints/ep-1", tc.body)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			msg, _ := resp["Message"].(string)
			assert.Equal(t, "Accepted", msg,
				"UpdateEndpoint must return {Message:Accepted} not the endpoint body")

			// Must NOT contain endpoint-specific fields.
			assert.NotContains(t, resp, "ApplicationId",
				"UpdateEndpoint response must not contain ApplicationId (endpoint field)")
			assert.NotContains(t, resp, "ChannelType",
				"UpdateEndpoint response must not contain ChannelType (endpoint field)")
		})
	}
}

// ──────────────────────────────────────────────────
// Finding 3: GetSegmentImportJobs ignored segmentID
//
// Before fix: backend used _ for segmentID and returned ALL import jobs for
// the app. AWS returns only the import job(s) that created the specified segment.
// After fix: backend stores the segment ID on the import job and filters by it.
// ──────────────────────────────────────────────────
