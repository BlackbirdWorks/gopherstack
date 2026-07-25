package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaignFullDTO_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantState        string
		wantSegID        string
		wantStatus       int
		wantPriority     float64
		wantIsPaused     bool
		wantHasSchedule  bool
		wantHasHook      bool
		wantHasLimits    bool
		wantHasMsgConfig bool
	}{
		{
			name: "minimal_campaign",
			body: map[string]any{
				"Name":      "basic",
				"SegmentId": "seg-001",
			},
			wantStatus: http.StatusCreated,
			wantState:  "SCHEDULED",
			wantSegID:  "seg-001",
		},
		{
			name: "campaign_with_schedule",
			body: map[string]any{
				"Name":      "scheduled",
				"SegmentId": "seg-001",
				"Schedule": map[string]any{
					"StartTime": "2026-01-01T00:00:00Z",
					"Frequency": "DAILY",
					"Timezone":  "UTC",
				},
			},
			wantStatus:      http.StatusCreated,
			wantState:       "SCHEDULED",
			wantHasSchedule: true,
		},
		{
			name: "campaign_with_hook",
			body: map[string]any{
				"Name":      "hooked",
				"SegmentId": "seg-001",
				"Hook": map[string]any{
					"LambdaFunctionName": "arn:aws:lambda:us-east-1:123:function:my-fn",
					"Mode":               "FILTER",
				},
			},
			wantStatus:  http.StatusCreated,
			wantHasHook: true,
		},
		{
			name: "campaign_with_limits",
			body: map[string]any{
				"Name":      "limited",
				"SegmentId": "seg-001",
				"Limits": map[string]any{
					"Daily":             100.0,
					"MessagesPerSecond": 10.0,
					"Total":             1000.0,
				},
			},
			wantStatus:    http.StatusCreated,
			wantHasLimits: true,
		},
		{
			name: "campaign_with_message_config",
			body: map[string]any{
				"Name":      "msg-config",
				"SegmentId": "seg-001",
				"MessageConfiguration": map[string]any{
					"DefaultMessage": map[string]any{
						"Body": "Hello {{user.FirstName}}",
					},
					"SMSMessage": map[string]any{
						"Body":        "SMS body",
						"MessageType": "PROMOTIONAL",
					},
				},
			},
			wantStatus:       http.StatusCreated,
			wantHasMsgConfig: true,
		},
		{
			name: "paused_campaign",
			body: map[string]any{
				"Name":      "paused",
				"SegmentId": "seg-001",
				"IsPaused":  true,
			},
			wantStatus:   http.StatusCreated,
			wantState:    "PAUSED",
			wantIsPaused: true,
		},
		{
			name: "campaign_with_priority",
			body: map[string]any{
				"Name":      "high-priority",
				"SegmentId": "seg-001",
				"Priority":  5,
			},
			wantStatus:   http.StatusCreated,
			wantPriority: 5,
		},
		{
			name: "campaign_with_treatment",
			body: map[string]any{
				"Name":                 "treatment-test",
				"SegmentId":            "seg-001",
				"TreatmentDescription": "Control group",
				"TreatmentName":        "Control",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "campaign_with_additional_treatments",
			body: map[string]any{
				"Name":      "ab-test",
				"SegmentId": "seg-001",
				"AdditionalTreatments": []any{
					map[string]any{
						"SizePercent":   50,
						"TreatmentName": "Variant A",
						"SegmentId":     "seg-002",
					},
				},
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "campaign_with_template_config",
			body: map[string]any{
				"Name":      "templated",
				"SegmentId": "seg-001",
				"TemplateConfiguration": map[string]any{
					"EmailTemplate": map[string]any{
						"Name":    "welcome-email",
						"Version": "1",
					},
				},
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			// Create an app first.
			appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "test-app"})
			require.Equal(t, http.StatusCreated, appRec.Code)

			var appResp map[string]any
			require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
			appID := appResp["Id"].(string)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())

			if rec.Code != http.StatusCreated {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.NotEmpty(t, resp["Id"])
			assert.Equal(t, appID, resp["ApplicationId"])

			state, _ := resp["State"].(map[string]any)
			require.NotNil(t, state, "State must be present")

			wantState := tc.wantState
			if wantState == "" {
				wantState = "SCHEDULED"
			}

			assert.Equal(t, wantState, state["CampaignStatus"])

			if tc.wantSegID != "" {
				assert.Equal(t, tc.wantSegID, resp["SegmentId"])
			}

			if tc.wantPriority != 0 {
				assert.InDelta(t, tc.wantPriority, resp["Priority"], 0.001)
			}

			if tc.wantIsPaused {
				assert.Equal(t, true, resp["IsPaused"])
			}

			if tc.wantHasSchedule {
				assert.NotNil(t, resp["Schedule"])
			}

			if tc.wantHasHook {
				assert.NotNil(t, resp["Hook"])
			}

			if tc.wantHasLimits {
				assert.NotNil(t, resp["Limits"])
			}

			if tc.wantHasMsgConfig {
				assert.NotNil(t, resp["MessageConfiguration"])
			}
		})
	}
}

func TestCampaignStateMachine_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody   map[string]any
		updateBody   map[string]any
		name         string
		wantState    string
		wantStatus   int
		wantVersion  float64
		wantIsPaused bool
	}{
		{
			name:        "update_name_keeps_state",
			createBody:  map[string]any{"Name": "original", "SegmentId": "seg-1"},
			updateBody:  map[string]any{"Name": "updated"},
			wantStatus:  http.StatusOK,
			wantState:   "SCHEDULED",
			wantVersion: 2,
		},
		{
			name:         "pause_campaign",
			createBody:   map[string]any{"Name": "pausable", "SegmentId": "seg-1"},
			updateBody:   map[string]any{"IsPaused": true},
			wantStatus:   http.StatusOK,
			wantState:    "PAUSED",
			wantIsPaused: true,
			wantVersion:  2,
		},
		{
			name: "unpause_restores_scheduled",
			createBody: map[string]any{
				"Name": "unpausable", "SegmentId": "seg-1", "IsPaused": true,
			},
			updateBody:   map[string]any{"IsPaused": false},
			wantStatus:   http.StatusOK,
			wantState:    "SCHEDULED",
			wantIsPaused: false,
			wantVersion:  2,
		},
		{
			name:       "update_schedule_round_trips",
			createBody: map[string]any{"Name": "sched-update", "SegmentId": "seg-1"},
			updateBody: map[string]any{
				"Schedule": map[string]any{
					"Frequency": "WEEKLY",
					"StartTime": "2026-06-01T08:00:00Z",
				},
			},
			wantStatus:  http.StatusOK,
			wantState:   "SCHEDULED",
			wantVersion: 2,
		},
		{
			name:       "update_limits_round_trips",
			createBody: map[string]any{"Name": "limits-update", "SegmentId": "seg-1"},
			updateBody: map[string]any{
				"Limits": map[string]any{"Daily": 200.0, "Total": 5000.0},
			},
			wantStatus:  http.StatusOK,
			wantVersion: 2,
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

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			campaignID := created["Id"].(string)

			rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campaignID, tc.updateBody)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if rec.Code != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tc.wantState != "" {
				state, _ := resp["State"].(map[string]any)
				assert.Equal(t, tc.wantState, state["CampaignStatus"])
			}

			if tc.wantVersion != 0 {
				assert.InDelta(t, tc.wantVersion, resp["Version"], 0.001)
			}

			if tc.wantIsPaused {
				assert.Equal(t, true, resp["IsPaused"])
			}
		})
	}
}

func TestCampaignVersioning(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "versioned", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	campaignID := created["Id"].(string)

	// Do 3 updates.
	for i := range 3 {
		updateRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campaignID,
			map[string]any{"Name": "versioned-v" + string(rune('A'+i))})
		require.Equal(t, http.StatusOK, updateRec.Code)
	}

	// GetCampaignVersions should have 4 versions (1 create + 3 updates).
	verRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions", nil)
	require.Equal(t, http.StatusOK, verRec.Code)
	var verResp map[string]any
	require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verResp))
	items, _ := verResp["Item"].([]any)
	assert.Len(t, items, 4, "should have 4 versions")

	// GetCampaignVersion by number.
	v1Rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions/1", nil)
	require.Equal(t, http.StatusOK, v1Rec.Code)
	var v1Resp map[string]any
	require.NoError(t, json.Unmarshal(v1Rec.Body.Bytes(), &v1Resp))
	assert.InDelta(t, float64(1), v1Resp["Version"], 0.001)
}

// ──────────────────────────────────────────────────
// Segment DTO: Dimensions, SegmentGroups, ImportDefinition
// ──────────────────────────────────────────────────

func TestCampaignPauseUnpauseCycle(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "cycle", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	campaignID := created["Id"].(string)

	// Check initial state.
	state1, _ := created["State"].(map[string]any)
	assert.Equal(t, "SCHEDULED", state1["CampaignStatus"])

	// Pause.
	pauseRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campaignID,
		map[string]any{"IsPaused": true})
	require.Equal(t, http.StatusOK, pauseRec.Code)
	var paused map[string]any
	require.NoError(t, json.Unmarshal(pauseRec.Body.Bytes(), &paused))
	pausedState, _ := paused["State"].(map[string]any)
	assert.Equal(t, "PAUSED", pausedState["CampaignStatus"])

	// Unpause.
	unpauseRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campaignID,
		map[string]any{"IsPaused": false})
	require.Equal(t, http.StatusOK, unpauseRec.Code)
	var unpaused map[string]any
	require.NoError(t, json.Unmarshal(unpauseRec.Body.Bytes(), &unpaused))
	unpausedState, _ := unpaused["State"].(map[string]any)
	assert.Equal(t, "SCHEDULED", unpausedState["CampaignStatus"])
}

// ──────────────────────────────────────────────────
// Edge cases
// ──────────────────────────────────────────────────

func TestCampaign_ScheduleFrequencyRoundTrip(t *testing.T) {
	t.Parallel()

	frequencies := []string{
		"IN_APP", "ONCE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "EVENT",
	}

	for _, freq := range frequencies {
		t.Run(freq, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
			require.Equal(t, http.StatusCreated, appRec.Code)
			var appResp map[string]any
			require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
			appID := appResp["Id"].(string)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
				map[string]any{
					"Name":      "freq-test",
					"SegmentId": "seg-1",
					"Schedule": map[string]any{
						"Frequency": freq,
						"StartTime": "2026-01-01T00:00:00Z",
					},
				})
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			schedule, _ := resp["Schedule"].(map[string]any)
			assert.Equal(t, freq, schedule["Frequency"])
		})
	}
}

func TestCampaign_AdditionalTreatments_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{
			"Name":      "ab-campaign",
			"SegmentId": "seg-1",
			"AdditionalTreatments": []any{
				map[string]any{
					"SizePercent":   30,
					"TreatmentName": "Variant A",
					"MessageConfiguration": map[string]any{
						"DefaultMessage": map[string]any{"Body": "Variant A message"},
					},
				},
				map[string]any{
					"SizePercent":   20,
					"TreatmentName": "Variant B",
				},
			},
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	treatments, _ := resp["AdditionalTreatments"].([]any)
	assert.Len(t, treatments, 2)
}

// TestCoverage_CampaignCRUD covers GetCampaign, GetCampaigns, UpdateCampaign,
// DeleteCampaign, GetCampaignActivities, GetCampaignVersions, GetCampaignVersion,
// GetCampaignDateRangeKpi.
func TestCampaignCRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "campaign-crud-app")

	// Create campaign.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "test-campaign", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	campaignID, _ := createResp["Id"].(string)
	require.NotEmpty(t, campaignID)

	// GetCampaign.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaigns.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateCampaign.
	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campaignID,
		map[string]any{"Name": "updated-campaign"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignActivities.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/activities", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignVersions.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignVersion.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions/1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignDateRangeKpi.
	rec = doPinpointRequest(
		t,
		h,
		http.MethodGet,
		"/v1/apps/"+appID+"/campaigns/"+campaignID+"/kpis/daterange/test-kpi",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteCampaign.
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/campaigns/"+campaignID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaign not found after delete.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateCampaignAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/campaigns",
		map[string]any{"Name": "orphan-campaign"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateCampaignInvalidJSON(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "invalid-json-app")

	rec := doRawPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", []byte("bad-body"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateCampaign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_campaign",
			body:       map[string]any{"Name": "my-campaign", "SegmentId": "seg-1"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "creates_campaign_with_tags",
			body:       map[string]any{"Name": "tagged-campaign", "tags": map[string]string{"env": "prod"}},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "campaign-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, appID, resp["ApplicationId"])
			}
		})
	}
}

// TestGetCampaignVersion_UnknownVersionNotFound locks that GetCampaignVersion
// 404s for a version number absent from the campaign's history, matching the
// documented NotFoundException response on the real
// /v1/apps/{appId}/campaigns/{campaignId}/versions/{version} resource,
// instead of silently substituting the current campaign under the wrong
// Version number.
func TestGetCampaignVersion_UnknownVersionNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "campaign-version-404-app")

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "c1", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	campaignID, _ := created["Id"].(string)
	require.NotEmpty(t, campaignID)

	// Version 1 exists (created by CreateCampaign) -- confirm it's reachable.
	v1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions/1", nil)
	require.Equal(t, http.StatusOK, v1Rec.Code)

	// Version 999 was never created -- must 404, not fall back to version 1's
	// (or the current campaign's) content.
	missingRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions/999", nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(missingRec.Body).Decode(&errResp))
	assert.Equal(t, "NotFoundException", errResp["__type"])
}
