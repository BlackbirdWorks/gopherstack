package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// ──────────────────────────────────────────────────
// Campaign full DTO + State machine
// ──────────────────────────────────────────────────

func TestAudit1_CampaignFullDTO_Create(t *testing.T) {
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

func TestAudit1_CampaignStateMachine_Update(t *testing.T) {
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

func TestAudit1_CampaignVersioning(t *testing.T) {
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

func TestAudit1_SegmentFullDTO_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body              map[string]any
		name              string
		wantSegmentType   string
		wantStatus        int
		wantHasDimensions bool
		wantHasImport     bool
	}{
		{
			name:            "minimal_segment",
			body:            map[string]any{"Name": "plain"},
			wantStatus:      http.StatusCreated,
			wantSegmentType: "DIMENSIONAL",
		},
		{
			name: "segment_with_dimensions",
			body: map[string]any{
				"Name": "with-dims",
				"Dimensions": map[string]any{
					"Attributes": map[string]any{
						"Plan": map[string]any{
							"AttributeType": "INCLUSIVE",
							"Values":        []any{"premium"},
						},
					},
					"Demographic": map[string]any{
						"Platform": map[string]any{
							"DimensionType": "INCLUSIVE",
							"Values":        []any{"ios", "android"},
						},
					},
				},
			},
			wantStatus:        http.StatusCreated,
			wantSegmentType:   "DIMENSIONAL",
			wantHasDimensions: true,
		},
		{
			name: "segment_with_location_dimensions",
			body: map[string]any{
				"Name": "geo-segment",
				"Dimensions": map[string]any{
					"Location": map[string]any{
						"Country": map[string]any{
							"DimensionType": "INCLUSIVE",
							"Values":        []any{"US", "CA"},
						},
					},
				},
			},
			wantStatus:        http.StatusCreated,
			wantSegmentType:   "DIMENSIONAL",
			wantHasDimensions: true,
		},
		{
			name: "segment_with_import_definition",
			body: map[string]any{
				"Name": "imported",
				"ImportDefinition": map[string]any{
					"S3Url":   "s3://my-bucket/endpoints.csv",
					"RoleArn": "arn:aws:iam::123456789012:role/PinpointRole",
					"Format":  "CSV",
				},
			},
			wantStatus:      http.StatusCreated,
			wantSegmentType: "IMPORT",
			wantHasImport:   true,
		},
		{
			name: "segment_with_segment_groups",
			body: map[string]any{
				"Name": "grouped",
				"SegmentGroups": map[string]any{
					"Groups": []any{
						map[string]any{
							"Type":       "ALL",
							"SourceType": "ANY",
							"SourceSegments": []any{
								map[string]any{"Id": "seg-a", "Version": 1},
							},
						},
					},
					"Include": "ALL",
				},
			},
			wantStatus:      http.StatusCreated,
			wantSegmentType: "DIMENSIONAL",
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

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments", tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if rec.Code != http.StatusCreated {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, tc.wantSegmentType, resp["SegmentType"])
			assert.NotEmpty(t, resp["Id"])
			assert.NotEmpty(t, resp["CreationDate"])
			assert.NotEmpty(t, resp["LastModifiedDate"])
			assert.InDelta(t, float64(1), resp["Version"], 0.001)

			if tc.wantHasDimensions {
				assert.NotNil(t, resp["Dimensions"])
			}

			if tc.wantHasImport {
				assert.NotNil(t, resp["ImportDefinition"])
			}
		})
	}
}

func TestAudit1_SegmentUpdate_DimensionsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		updateBody      map[string]any
		wantSegmentType string
		wantVersion     float64
	}{
		{
			name: "update_dimensions",
			updateBody: map[string]any{
				"Dimensions": map[string]any{
					"Attributes": map[string]any{
						"Tier": map[string]any{
							"AttributeType": "INCLUSIVE",
							"Values":        []any{"gold"},
						},
					},
				},
			},
			wantSegmentType: "DIMENSIONAL",
			wantVersion:     2,
		},
		{
			name: "update_adds_import_definition",
			updateBody: map[string]any{
				"ImportDefinition": map[string]any{
					"S3Url":   "s3://bucket/data.json",
					"RoleArn": "arn:aws:iam::123:role/Role",
					"Format":  "JSON",
				},
			},
			wantSegmentType: "IMPORT",
			wantVersion:     2,
		},
		{
			name: "update_segment_groups",
			updateBody: map[string]any{
				"SegmentGroups": map[string]any{
					"Include": "ALL",
					"Groups":  []any{},
				},
			},
			wantSegmentType: "DIMENSIONAL",
			wantVersion:     2,
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

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
				map[string]any{"Name": "seg"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			segID := created["Id"].(string)

			rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segID, tc.updateBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, tc.wantSegmentType, resp["SegmentType"])
			assert.InDelta(t, tc.wantVersion, resp["Version"], 0.001)
			assert.NotEmpty(t, resp["LastModifiedDate"])
		})
	}
}

// ──────────────────────────────────────────────────
// Journey: Activities, StartCondition, Schedule, Limits, State validation
// ──────────────────────────────────────────────────

func TestAudit1_JourneyFullDTO_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body              map[string]any
		name              string
		wantState         string
		wantRefreshFreq   string
		wantStatus        int
		wantHasActivities bool
		wantHasStartCond  bool
		wantHasSchedule   bool
		wantHasLimits     bool
	}{
		{
			name:       "minimal_journey",
			body:       map[string]any{"Name": "journey"},
			wantStatus: http.StatusCreated,
			wantState:  "DRAFT",
		},
		{
			name: "journey_with_activities",
			body: map[string]any{
				"Name": "active-journey",
				"Activities": map[string]any{
					"activity-1": map[string]any{
						"Wait": map[string]any{
							"WaitTime":     map[string]any{"WaitFor": "PT1H"},
							"NextActivity": "activity-2",
						},
					},
					"activity-2": map[string]any{
						"EMAIL": map[string]any{
							"MessageConfig": map[string]any{"FromAddress": "noreply@example.com"},
							"NextActivity":  "",
						},
					},
				},
				"StartActivity": "activity-1",
			},
			wantStatus:        http.StatusCreated,
			wantHasActivities: true,
		},
		{
			name: "journey_with_start_condition",
			body: map[string]any{
				"Name": "conditional-start",
				"StartCondition": map[string]any{
					"Description": "Start when event fires",
					"EventStartCondition": map[string]any{
						"EventFilter": map[string]any{
							"Dimensions": map[string]any{
								"EventType": map[string]any{
									"DimensionType": "INCLUSIVE",
									"Values":        []any{"_session.start"},
								},
							},
							"FilterType": "SYSTEM",
						},
					},
				},
			},
			wantStatus:       http.StatusCreated,
			wantHasStartCond: true,
		},
		{
			name: "journey_with_schedule",
			body: map[string]any{
				"Name": "scheduled-journey",
				"Schedule": map[string]any{
					"StartTime": "2026-01-01T00:00:00Z",
					"EndTime":   "2026-12-31T23:59:59Z",
					"Timezone":  "UTC",
				},
			},
			wantStatus:      http.StatusCreated,
			wantHasSchedule: true,
		},
		{
			name: "journey_with_limits",
			body: map[string]any{
				"Name": "limited-journey",
				"Limits": map[string]any{
					"DailyCap":                100,
					"EndpointReentryCap":      3,
					"EndpointReentryInterval": "P1D",
					"MessagesPerSecond":       50,
				},
			},
			wantStatus:    http.StatusCreated,
			wantHasLimits: true,
		},
		{
			name: "journey_with_refresh_frequency",
			body: map[string]any{
				"Name":             "refresh-journey",
				"RefreshFrequency": "PT1H",
			},
			wantStatus:      http.StatusCreated,
			wantRefreshFreq: "PT1H",
		},
		{
			name: "journey_with_quiet_time",
			body: map[string]any{
				"Name": "quiet-journey",
				"QuietTime": map[string]any{
					"Start": "22:00",
					"End":   "06:00",
				},
				"WaitForQuietTime": true,
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "journey_with_local_time",
			body: map[string]any{
				"Name":      "local-time-journey",
				"LocalTime": true,
			},
			wantStatus: http.StatusCreated,
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

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys", tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if rec.Code != http.StatusCreated {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.NotEmpty(t, resp["Id"])
			assert.Equal(t, appID, resp["ApplicationId"])

			wantState := tc.wantState
			if wantState == "" {
				wantState = "DRAFT"
			}

			assert.Equal(t, wantState, resp["State"])

			if tc.wantHasActivities {
				assert.NotNil(t, resp["Activities"])
			}

			if tc.wantHasStartCond {
				assert.NotNil(t, resp["StartCondition"])
			}

			if tc.wantHasSchedule {
				assert.NotNil(t, resp["Schedule"])
			}

			if tc.wantHasLimits {
				assert.NotNil(t, resp["Limits"])
			}

			if tc.wantRefreshFreq != "" {
				assert.Equal(t, tc.wantRefreshFreq, resp["RefreshFrequency"])
			}
		})
	}
}

func TestAudit1_JourneyStateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fromState        string
		toState          string
		setupTransitions []string
		wantStatus       int
	}{
		{
			name:       "draft_to_active",
			fromState:  "DRAFT",
			toState:    "ACTIVE",
			wantStatus: http.StatusOK,
		},
		{
			name:       "draft_to_cancelled",
			fromState:  "DRAFT",
			toState:    "CANCELLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "draft_to_paused_invalid",
			fromState:  "DRAFT",
			toState:    "PAUSED",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "draft_to_completed_invalid",
			fromState:  "DRAFT",
			toState:    "COMPLETED",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "active_to_paused",
			fromState:  "DRAFT",
			toState:    "PAUSED",
			wantStatus: http.StatusBadRequest, // from DRAFT directly
		},
		{
			name:             "active_to_cancelled",
			setupTransitions: []string{"ACTIVE"},
			toState:          "CANCELLED",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "active_to_paused_valid",
			setupTransitions: []string{"ACTIVE"},
			toState:          "PAUSED",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "active_to_completed",
			setupTransitions: []string{"ACTIVE"},
			toState:          "COMPLETED",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "paused_to_active",
			setupTransitions: []string{"ACTIVE", "PAUSED"},
			toState:          "ACTIVE",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "paused_to_cancelled",
			setupTransitions: []string{"ACTIVE", "PAUSED"},
			toState:          "CANCELLED",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "cancelled_no_transitions",
			setupTransitions: []string{"CANCELLED"},
			toState:          "ACTIVE",
			wantStatus:       http.StatusBadRequest,
		},
		{
			name:             "completed_no_transitions",
			setupTransitions: []string{"ACTIVE", "COMPLETED"},
			toState:          "PAUSED",
			wantStatus:       http.StatusBadRequest,
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

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
				map[string]any{"Name": "j"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			journeyID := created["Id"].(string)

			for _, state := range tc.setupTransitions {
				r := doPinpointRequest(t, h, http.MethodPut,
					"/v1/apps/"+appID+"/journeys/"+journeyID+"/state",
					map[string]any{"State": state})
				require.Equal(t, http.StatusOK, r.Code, "setup transition to %s failed: %s", state, r.Body.String())
			}

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/journeys/"+journeyID+"/state",
				map[string]any{"State": tc.toState})
			assert.Equal(t, tc.wantStatus, rec.Code, "transition to %s: %s", tc.toState, rec.Body.String())
		})
	}
}

func TestAudit1_JourneyUpdate_BlockedWhenActive(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{"Name": "j"})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	journeyID := created["Id"].(string)

	// Activate the journey.
	r := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/journeys/"+journeyID+"/state",
		map[string]any{"State": "ACTIVE"})
	require.Equal(t, http.StatusOK, r.Code)

	// UpdateJourney (activities mutation) must be rejected.
	updateRec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/journeys/"+journeyID,
		map[string]any{
			"Name":       "mutated",
			"Activities": map[string]any{"x": map[string]any{}},
		})
	assert.Equal(t, http.StatusBadRequest, updateRec.Code)
}

// ──────────────────────────────────────────────────
// Endpoint: full shape with Demographic, Location, Metrics, Status
// ──────────────────────────────────────────────────

func TestAudit1_Endpoint_FullShape(t *testing.T) {
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

func TestAudit1_Endpoint_UserAttributes(t *testing.T) {
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

func TestAudit1_EndpointBatch_FullShape(t *testing.T) {
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

func TestAudit1_Channel_PerTypeFields(t *testing.T) {
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
				"BundleId":          "com.example.app",
				"Certificate":       "-----BEGIN CERTIFICATE-----",
				"DefaultAuthMethod": "CERTIFICATE",
				"Enabled":           true,
			},
			wantStatus:        http.StatusOK,
			wantHasCredential: true,
			wantEnabled:       true,
		},
		{
			name:        "apns_with_token_key",
			channelType: "apns",
			body: map[string]any{
				"BundleId":          "com.example.app",
				"TokenKey":          "-----BEGIN PRIVATE KEY-----",
				"TokenKeyId":        "key123",
				"TeamId":            "TEAM123",
				"DefaultAuthMethod": "TOKEN",
				"Enabled":           true,
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

func TestAudit1_Channel_VersionIncrements(t *testing.T) {
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

func TestAudit1_Channel_CreationDatePreserved(t *testing.T) {
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

func TestAudit1_Recommender_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		idType         string
		wantCreateFail bool
	}{
		{
			name:   "valid_endpoint_id_type",
			idType: "PINPOINT_ENDPOINT_ID",
		},
		{
			name:   "valid_user_id_type",
			idType: "PINPOINT_USER_ID",
		},
		{
			name:   "empty_id_type_ok",
			idType: "",
		},
		{
			name:           "invalid_id_type",
			idType:         "INVALID_TYPE",
			wantCreateFail: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
			req := pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                         "test-recommender",
				RecommendationProviderIDType: tc.idType,
				RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/rec",
			}

			r, err := b.CreateRecommenderConfiguration(req)
			if tc.wantCreateFail {
				require.Error(t, err)
				assert.Nil(t, r)
			} else {
				require.NoError(t, err)
				require.NotNil(t, r)

				if tc.idType != "" {
					assert.Equal(t, tc.idType, r.RecommendationProviderIDType)
				}
			}
		})
	}
}

func TestAudit1_Recommender_ConditionalLastModifiedDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantFieldUpdated  string
		wantFieldValue    string
		initialReq        pinpoint.ExportedCreateRecommenderConfigRequest
		updateReq         pinpoint.ExportedCreateRecommenderConfigRequest
		wantDateUnchanged bool
	}{
		{
			name: "same_name_no_date_change",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "original",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "original",
			},
			wantDateUnchanged: true,
		},
		{
			name: "update_description_stored",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "rec",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Description: "a new description",
			},
			wantFieldUpdated: "Description",
			wantFieldValue:   "a new description",
		},
		{
			name: "update_uri_stored",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                      "rec",
				RecommendationProviderURI: "arn:aws:personalize:us-east-1:123:campaign/orig",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				RecommendationProviderURI: "arn:aws:personalize:us-east-1:123:campaign/new",
			},
			wantFieldUpdated: "URI",
			wantFieldValue:   "arn:aws:personalize:us-east-1:123:campaign/new",
		},
		{
			name: "update_id_type_stored",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "rec",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				RecommendationProviderIDType: "PINPOINT_USER_ID",
			},
			wantFieldUpdated: "IDType",
			wantFieldValue:   "PINPOINT_USER_ID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

			created, err := b.CreateRecommenderConfiguration(tc.initialReq)
			require.NoError(t, err)
			originalDate := created.LastModifiedDate

			updated, err := b.UpdateRecommenderConfiguration(created.ID, tc.updateReq)
			require.NoError(t, err)
			require.NotNil(t, updated)

			if tc.wantDateUnchanged {
				assert.Equal(t, originalDate, updated.LastModifiedDate,
					"LastModifiedDate should not update when nothing changed")
			}

			switch tc.wantFieldUpdated {
			case "Description":
				assert.Equal(t, tc.wantFieldValue, updated.Description)
			case "URI":
				assert.Equal(t, tc.wantFieldValue, updated.RecommendationProviderURI)
			case "IDType":
				assert.Equal(t, tc.wantFieldValue, updated.RecommendationProviderIDType)
			}
		})
	}
}

func TestAudit1_Recommender_InvalidIDTypeOnUpdate(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	created, err := b.CreateRecommenderConfiguration(
		pinpoint.ExportedCreateRecommenderConfigRequest{Name: "rec"},
	)
	require.NoError(t, err)

	_, err = b.UpdateRecommenderConfiguration(created.ID, pinpoint.ExportedCreateRecommenderConfigRequest{
		RecommendationProviderIDType: "INVALID_TYPE",
	})
	require.Error(t, err)
}

// ──────────────────────────────────────────────────
// ApplicationSettings: body persistence
// ──────────────────────────────────────────────────

func TestAudit1_ApplicationSettings_BodyPersistence(t *testing.T) {
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

func TestAudit1_JourneyUpdate_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updateBody map[string]any
		checkField string
	}{
		{
			name: "update_activities",
			updateBody: map[string]any{
				"Activities": map[string]any{
					"step-1": map[string]any{
						"EMAIL": map[string]any{
							"MessageConfig": map[string]any{},
							"NextActivity":  "",
						},
					},
				},
				"StartActivity": "step-1",
			},
			checkField: "Activities",
		},
		{
			name: "update_schedule",
			updateBody: map[string]any{
				"Schedule": map[string]any{
					"StartTime": "2026-06-01T00:00:00Z",
					"Timezone":  "UTC",
				},
			},
			checkField: "Schedule",
		},
		{
			name: "update_limits",
			updateBody: map[string]any{
				"Limits": map[string]any{
					"DailyCap":          200,
					"MessagesPerSecond": 100,
				},
			},
			checkField: "Limits",
		},
		{
			name: "update_quiet_time",
			updateBody: map[string]any{
				"QuietTime": map[string]any{
					"Start": "23:00",
					"End":   "07:00",
				},
				"WaitForQuietTime": true,
			},
			checkField: "QuietTime",
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

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
				map[string]any{"Name": "j"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			journeyID := created["Id"].(string)

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/journeys/"+journeyID, tc.updateBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.NotNil(t, resp[tc.checkField], "%s field must be non-nil after update", tc.checkField)
		})
	}
}

// ──────────────────────────────────────────────────
// Campaign + Segment: GetVersions return correct count
// ──────────────────────────────────────────────────

func TestAudit1_SegmentVersioning(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "versioned-seg"})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	segID := created["Id"].(string)

	// 2 updates.
	for i := range 2 {
		updateRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segID,
			map[string]any{"Name": "seg-v" + string(rune('A'+i))})
		require.Equal(t, http.StatusOK, updateRec.Code)
	}

	verRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segID+"/versions", nil)
	require.Equal(t, http.StatusOK, verRec.Code)
	var verResp map[string]any
	require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verResp))
	items, _ := verResp["Item"].([]any)
	assert.Len(t, items, 3, "3 versions: 1 create + 2 updates")
}

// ──────────────────────────────────────────────────
// Campaign: IsPaused false on paused campaign restores SCHEDULED
// ──────────────────────────────────────────────────

func TestAudit1_CampaignPauseUnpauseCycle(t *testing.T) {
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

func TestAudit1_Endpoint_EffectiveDate_RoundTrip(t *testing.T) {
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

func TestAudit1_Journey_MultipleActivities_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	activities := map[string]any{
		"start": map[string]any{
			"Wait": map[string]any{
				"WaitTime":     map[string]any{"WaitFor": "PT24H"},
				"NextActivity": "email-step",
			},
		},
		"email-step": map[string]any{
			"EMAIL": map[string]any{
				"MessageConfig":   map[string]any{"FromAddress": "no-reply@example.com"},
				"TemplateName":    "welcome",
				"TemplateVersion": "1",
				"NextActivity":    "holdout",
			},
		},
		"holdout": map[string]any{
			"Holdout": map[string]any{
				"NextActivity": "",
				"Percentage":   15,
			},
		},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name":          "multi-step",
			"StartActivity": "start",
			"Activities":    activities,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &resp))

	returnedActivities, _ := resp["Activities"].(map[string]any)
	assert.Len(t, returnedActivities, 3, "all 3 activities round-trip")
	assert.Contains(t, returnedActivities, "start")
	assert.Contains(t, returnedActivities, "email-step")
	assert.Contains(t, returnedActivities, "holdout")
	assert.Equal(t, "start", resp["StartActivity"])
}

func TestAudit1_Campaign_ScheduleFrequencyRoundTrip(t *testing.T) {
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

func TestAudit1_Segment_VersionRetrieval(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	appRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "app"})
	require.Equal(t, http.StatusCreated, appRec.Code)
	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appID := appResp["Id"].(string)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "seg-v"})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	segID := created["Id"].(string)

	updateRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segID,
		map[string]any{"Name": "seg-v-updated"})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// GetSegmentVersion v1.
	v1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+segID+"/versions/1", nil)
	require.Equal(t, http.StatusOK, v1Rec.Code)
	var v1 map[string]any
	require.NoError(t, json.Unmarshal(v1Rec.Body.Bytes(), &v1))
	assert.InDelta(t, float64(1), v1["Version"], 0.001)
	assert.Equal(t, "seg-v", v1["Name"])
}

func TestAudit1_Campaign_AdditionalTreatments_RoundTrip(t *testing.T) {
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

func TestAudit1_Channel_AllChannelsListed(t *testing.T) {
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
