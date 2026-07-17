package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationSettings_BodyPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantCampaignHook bool
		wantLimits       bool
		wantQuietTime    bool
	}{
		{
			name: "persist_campaign_hook",
			body: map[string]any{
				"CampaignHook": map[string]any{
					"LambdaFunctionName": "arn:aws:lambda:us-east-1:123:function:hook",
					"Mode":               "FILTER",
					"WebUrl":             "https://example.com/hook",
				},
			},
			wantCampaignHook: true,
		},
		{
			name: "persist_limits",
			body: map[string]any{
				"Limits": map[string]any{
					"Daily":             200,
					"MaximumDuration":   900,
					"MessagesPerSecond": 20,
					"Total":             10000,
				},
			},
			wantLimits: true,
		},
		{
			name: "persist_quiet_time",
			body: map[string]any{
				"QuietTime": map[string]any{
					"Start": "22:00",
					"End":   "08:00",
				},
			},
			wantQuietTime: true,
		},
		{
			name: "persist_all_settings",
			body: map[string]any{
				"CampaignHook": map[string]any{
					"Mode": "DELIVERY",
				},
				"Limits": map[string]any{
					"Daily": 100,
				},
				"QuietTime": map[string]any{
					"Start": "21:00",
					"End":   "09:00",
				},
				"CloudWatchMetricsEnabled": true,
			},
			wantCampaignHook: true,
			wantLimits:       true,
			wantQuietTime:    true,
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

			// Update settings.
			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

			// GET settings back.
			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

			if tc.wantCampaignHook {
				hook, ok := getResp["CampaignHook"].(map[string]any)
				assert.True(t, ok)
				assert.NotEmpty(t, hook)
			}

			if tc.wantLimits {
				limits, ok := getResp["Limits"].(map[string]any)
				assert.True(t, ok)
				assert.NotEmpty(t, limits)
			}

			if tc.wantQuietTime {
				qt, ok := getResp["QuietTime"].(map[string]any)
				assert.True(t, ok)
				assert.NotEmpty(t, qt)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Journey update round-trip: Activities, Schedule, Limits
// ──────────────────────────────────────────────────

func TestApplicationSettings_CloudWatchMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantFlag bool
	}{
		{
			name:     "enable_cloudwatch",
			body:     map[string]any{"CloudWatchMetricsEnabled": true},
			wantFlag: true,
		},
		{
			name:     "disable_cloudwatch",
			body:     map[string]any{"CloudWatchMetricsEnabled": false},
			wantFlag: false,
		},
		{
			name: "cloudwatch_with_limits",
			body: map[string]any{
				"CloudWatchMetricsEnabled": true,
				"Limits": map[string]any{
					"Daily": 500,
					"Total": 10000,
				},
			},
			wantFlag: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "cw-metrics-app")

			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantFlag, resp["CloudWatchMetricsEnabled"])
		})
	}
}

// ──────────────────────────────────────────────────
// Application settings: EventTaggingEnabled
// ──────────────────────────────────────────────────

func TestApplicationSettings_EventTaggingCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantFlag bool
	}{
		{
			name:     "enable_event_tagging",
			body:     map[string]any{"EventTaggingEnabled": true},
			wantFlag: true,
		},
		{
			name:     "disable_event_tagging",
			body:     map[string]any{"EventTaggingEnabled": false},
			wantFlag: false,
		},
		{
			name: "event_tagging_with_cloudwatch",
			body: map[string]any{
				"EventTaggingEnabled":      true,
				"CloudWatchMetricsEnabled": true,
			},
			wantFlag: true,
		},
		{
			name: "event_tagging_with_quiet_time",
			body: map[string]any{
				"EventTaggingEnabled": true,
				"QuietTime": map[string]any{
					"Start": "22:00",
					"End":   "06:00",
				},
			},
			wantFlag: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "event-tag-app")

			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
			assert.Equal(
				t,
				tc.wantFlag,
				putResp["EventTaggingEnabled"],
				"PUT response should include EventTaggingEnabled",
			)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(
				t,
				tc.wantFlag,
				getResp["EventTaggingEnabled"],
				"GET response should persist EventTaggingEnabled",
			)
		})
	}
}

// ──────────────────────────────────────────────────
// Application settings: Limits field deep coverage
// ──────────────────────────────────────────────────

func TestApplicationSettings_LimitsCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		limits   map[string]any
		checkKey string
		name     string
		checkVal float64
	}{
		{
			name:     "daily_limit",
			limits:   map[string]any{"Daily": 200.0},
			checkKey: "Daily",
			checkVal: 200.0,
		},
		{
			name:     "messages_per_second",
			limits:   map[string]any{"MessagesPerSecond": 50.0},
			checkKey: "MessagesPerSecond",
			checkVal: 50.0,
		},
		{
			name:     "maximum_duration",
			limits:   map[string]any{"MaximumDuration": 600.0},
			checkKey: "MaximumDuration",
			checkVal: 600.0,
		},
		{
			name:     "total_limit",
			limits:   map[string]any{"Total": 5000.0},
			checkKey: "Total",
			checkVal: 5000.0,
		},
		{
			name: "all_limits",
			limits: map[string]any{
				"Daily":             100.0,
				"MaximumDuration":   900.0,
				"MessagesPerSecond": 25.0,
				"Total":             50000.0,
			},
			checkKey: "Daily",
			checkVal: 100.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "limits-app")

			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
				map[string]any{"Limits": tc.limits})
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
			limits, ok := resp["Limits"].(map[string]any)
			require.True(t, ok, "Limits must be an object")
			assert.InDelta(t, tc.checkVal, limits[tc.checkKey], 0.001)
		})
	}
}

// ──────────────────────────────────────────────────
// ApplicationDateRangeKpi
// ──────────────────────────────────────────────────

func TestApplicationDateRangeKpi(t *testing.T) {
	t.Parallel()

	kpiNames := []struct {
		name    string
		kpiName string
	}{
		{name: "successful_endpoint_deliveries", kpiName: "successful-endpoint-deliveries"},
		{name: "unique_deliveries_grouped_by_date", kpiName: "unique-deliveries-grouped-by-date"},
		{name: "successful_deliveries_grouped_by_application", kpiName: "successful-deliveries-grouped-by-application"},
		{name: "unique_endpoint_deliveries", kpiName: "unique-endpoint-deliveries"},
		{name: "failed_deliveries", kpiName: "failed-deliveries"},
	}

	for _, tc := range kpiNames {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "kpi-app")

			rec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/kpis/daterange/"+tc.kpiName, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, appID, resp["ApplicationId"])
			assert.Equal(t, tc.kpiName, resp["KpiName"])
			assert.NotNil(t, resp["KpiResult"])
		})
	}
}

// ──────────────────────────────────────────────────
// AppLifeCycleEvent via PutEvents
// ──────────────────────────────────────────────────

func TestApplicationSettings_MissingApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_settings_missing_app",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "put_settings_missing_app",
			method:     http.MethodPut,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			rec := doPinpointRequest(t, h, tc.method,
				"/v1/apps/nonexistent-app-id/settings",
				map[string]any{"CloudWatchMetricsEnabled": true})
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

// ──────────────────────────────────────────────────
// VoiceTemplate: duplicate create rejected
// ──────────────────────────────────────────────────

func TestApplicationSettings_FullRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		wantCW bool
		wantET bool
	}{
		{
			name: "both_enabled",
			body: map[string]any{
				"CloudWatchMetricsEnabled": true,
				"EventTaggingEnabled":      true,
				"CampaignHook": map[string]any{
					"Mode": "DELIVERY",
				},
				"Limits": map[string]any{
					"Daily": 1000,
				},
				"QuietTime": map[string]any{
					"Start": "23:00",
					"End":   "07:00",
				},
			},
			wantCW: true,
			wantET: true,
		},
		{
			name: "cw_only",
			body: map[string]any{
				"CloudWatchMetricsEnabled": true,
				"EventTaggingEnabled":      false,
			},
			wantCW: true,
			wantET: false,
		},
		{
			name: "et_only",
			body: map[string]any{
				"CloudWatchMetricsEnabled": false,
				"EventTaggingEnabled":      true,
			},
			wantCW: false,
			wantET: true,
		},
		{
			name: "both_disabled",
			body: map[string]any{
				"CloudWatchMetricsEnabled": false,
				"EventTaggingEnabled":      false,
			},
			wantCW: false,
			wantET: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "full-settings-app")

			putRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/settings", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
			assert.Equal(t, tc.wantCW, putResp["CloudWatchMetricsEnabled"])
			assert.Equal(t, tc.wantET, putResp["EventTaggingEnabled"])
			assert.NotNil(t, putResp["CampaignHook"])
			assert.NotNil(t, putResp["Limits"])
			assert.NotNil(t, putResp["QuietTime"])

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, tc.wantCW, getResp["CloudWatchMetricsEnabled"])
			assert.Equal(t, tc.wantET, getResp["EventTaggingEnabled"])
		})
	}
}

func TestApplicationSettings_CampaignHook(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "settings-hook-app")

	hook := map[string]any{
		"LambdaFunctionName": "arn:aws:lambda:us-east-1:123456789012:function:SegmentHook",
		"Mode":               "FILTER",
	}

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{
			"CampaignHook":             hook,
			"CloudWatchMetricsEnabled": true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var settings map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &settings))

	gotHook := settings["CampaignHook"].(map[string]any)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:SegmentHook", gotHook["LambdaFunctionName"])
	assert.Equal(t, "FILTER", gotHook["Mode"])
	assert.Equal(t, true, settings["CloudWatchMetricsEnabled"])
}

func TestApplicationSettings_QuietTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		start     string
		end       string
		wantStart string
		wantEnd   string
	}{
		{name: "overnight_quiet", start: "22:00", end: "08:00", wantStart: "22:00", wantEnd: "08:00"},
		{name: "midday_quiet", start: "12:00", end: "13:00", wantStart: "12:00", wantEnd: "13:00"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "settings-qt-app")

			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
				map[string]any{
					"QuietTime": map[string]any{
						"Start": tc.start,
						"End":   tc.end,
					},
				})
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var settings map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &settings))

			qt := settings["QuietTime"].(map[string]any)
			assert.Equal(t, tc.wantStart, qt["Start"])
			assert.Equal(t, tc.wantEnd, qt["End"])
		})
	}
}

func TestApplicationSettings_Limits(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "settings-limits-app")

	limits := map[string]any{
		"Daily":             500,
		"MaximumDuration":   600,
		"MessagesPerSecond": 200,
		"Total":             0,
	}

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{"Limits": limits})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var settings map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &settings))

	got := settings["Limits"].(map[string]any)
	assert.EqualValues(t, 500, got["Daily"])
	assert.EqualValues(t, 600, got["MaximumDuration"])
	assert.EqualValues(t, 200, got["MessagesPerSecond"])
}

func TestApplicationSettings_MultipleUpdates(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "settings-multi-app")

	// First update: CloudWatch + QuietTime
	doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{
			"CloudWatchMetricsEnabled": true,
			"QuietTime": map[string]any{
				"Start": "23:00",
				"End":   "07:00",
			},
		})

	// Second update: add Limits without disturbing other fields
	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{
			"CloudWatchMetricsEnabled": true,
			"QuietTime": map[string]any{
				"Start": "23:00",
				"End":   "07:00",
			},
			"Limits": map[string]any{
				"Daily":             100,
				"MessagesPerSecond": 20,
			},
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var settings map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &settings))

	assert.Equal(t, true, settings["CloudWatchMetricsEnabled"])

	qt := settings["QuietTime"].(map[string]any)
	assert.Equal(t, "23:00", qt["Start"])

	limits := settings["Limits"].(map[string]any)
	assert.EqualValues(t, 100, limits["Daily"])
}

func TestApplicationSettings_EventTagging(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "settings-evt-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{
			"EventTaggingEnabled":      true,
			"CloudWatchMetricsEnabled": false,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var settings map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &settings))

	assert.Equal(t, true, settings["EventTaggingEnabled"])
	assert.Equal(t, false, settings["CloudWatchMetricsEnabled"])
	assert.Equal(t, appID, settings["ApplicationId"])
	assert.NotEmpty(t, settings["LastModifiedDate"])
}

// ──────────────────────────────────────────────────
// Journey — RefreshOnSegmentUpdate + schedule
// ──────────────────────────────────────────────────

func TestHandler_ApplicationSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_settings",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "put_settings",
			method:     http.MethodPut,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "settings-app"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var appResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&appResp))

			appID, _ := appResp["Id"].(string)
			require.NotEmpty(t, appID)

			settingsPath := "/v1/apps/" + appID + "/settings"

			var body any
			if tt.method == http.MethodPut {
				body = map[string]any{}
			}

			rec2 := doPinpointRequest(t, h, tt.method, settingsPath, body)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			var settingsResp map[string]any
			require.NoError(t, json.NewDecoder(rec2.Body).Decode(&settingsResp))
			assert.Equal(t, appID, settingsResp["ApplicationId"])
			// CampaignHook, Limits, and QuietTime must be non-nil empty objects so
			// the Terraform provider flatten helpers don't panic on nil dereferences.
			assert.NotNil(t, settingsResp["CampaignHook"])
			assert.NotNil(t, settingsResp["Limits"])
			assert.NotNil(t, settingsResp["QuietTime"])
		})
	}
}

func TestAppSettings_LastModifiedDate_ConsistentAfterUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "cloudwatch_enabled",
			body: map[string]any{"CloudWatchMetricsEnabled": true},
		},
		{
			name: "quiet_time_set",
			body: map[string]any{
				"QuietTime": map[string]any{"Start": "22:00", "End": "06:00"},
			},
		},
		{
			name: "limits_set",
			body: map[string]any{
				"Limits": map[string]any{"Daily": 500},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "settings-lmd-app")

			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
			putDate, _ := putResp["LastModifiedDate"].(string)
			require.NotEmpty(t, putDate, "PUT must return non-empty LastModifiedDate")

			getRec1 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec1.Code)
			var getResp1 map[string]any
			require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &getResp1))

			getRec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec2.Code)
			var getResp2 map[string]any
			require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))

			date1, _ := getResp1["LastModifiedDate"].(string)
			date2, _ := getResp2["LastModifiedDate"].(string)

			assert.Equal(t, putDate, date1, "GET must return same LastModifiedDate as PUT")
			assert.Equal(t, date1, date2, "consecutive GETs must return identical LastModifiedDate")
		})
	}
}

func TestAppSettings_LastModifiedDate_UpdatesOnSecondPut(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "settings-lmd-update-app")

	put1 := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{"CloudWatchMetricsEnabled": true})
	require.Equal(t, http.StatusOK, put1.Code)
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(put1.Body.Bytes(), &r1))
	date1, _ := r1["LastModifiedDate"].(string)
	require.NotEmpty(t, date1)

	// Second PUT with different settings.
	put2 := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{"EventTaggingEnabled": true})
	require.Equal(t, http.StatusOK, put2.Code)
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(put2.Body.Bytes(), &r2))
	date2, _ := r2["LastModifiedDate"].(string)
	require.NotEmpty(t, date2)

	// GET must reflect the second PUT's date, not the first.
	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	getDate, _ := getResp["LastModifiedDate"].(string)

	assert.Equal(t, date2, getDate, "GET must return the most recent PUT's LastModifiedDate")
}

// ──────────────────────────────────────────────────
// Finding 2: UpdateEndpoint returned full endpoint instead of MessageBody
//
// Before fix: handler returned toEndpointResponse(e) (HTTP 202 + endpoint JSON).
// AWS returns HTTP 202 + {"Message":"Accepted"} (MessageBody).
// After fix: handler returns messageBodyResponse{Message: "Accepted"}.
// ──────────────────────────────────────────────────
