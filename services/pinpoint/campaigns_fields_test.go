package pinpoint_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaign_AdditionalTreatments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantTreatCount   int
		wantHasTreatment bool
	}{
		{
			name: "single_additional_treatment",
			body: map[string]any{
				"Name":      "ab-test-1",
				"SegmentId": "seg-001",
				"AdditionalTreatments": []any{
					map[string]any{
						"SizePercent":          50,
						"TreatmentName":        "Variant A",
						"TreatmentDescription": "Blue button variant",
						"Schedule": map[string]any{
							"StartTime": "2026-06-01T00:00:00Z",
						},
					},
				},
			},
			wantHasTreatment: true,
			wantTreatCount:   1,
		},
		{
			name: "multiple_additional_treatments",
			body: map[string]any{
				"Name":      "ab-test-2",
				"SegmentId": "seg-002",
				"AdditionalTreatments": []any{
					map[string]any{
						"SizePercent":   33,
						"TreatmentName": "Variant A",
					},
					map[string]any{
						"SizePercent":   33,
						"TreatmentName": "Variant B",
					},
				},
			},
			wantHasTreatment: true,
			wantTreatCount:   2,
		},
		{
			name: "treatment_with_message_config",
			body: map[string]any{
				"Name":      "msg-ab-test",
				"SegmentId": "seg-003",
				"AdditionalTreatments": []any{
					map[string]any{
						"SizePercent":   40,
						"TreatmentName": "Email Variant",
						"MessageConfiguration": map[string]any{
							"EmailMessage": map[string]any{
								"FromAddress": "test@example.com",
							},
						},
					},
				},
			},
			wantHasTreatment: true,
			wantTreatCount:   1,
		},
		{
			name: "treatment_round_trips_on_update",
			body: map[string]any{
				"Name":      "update-treatment",
				"SegmentId": "seg-004",
				"AdditionalTreatments": []any{
					map[string]any{
						"SizePercent":   25,
						"TreatmentName": "Original Variant",
					},
				},
			},
			wantHasTreatment: true,
			wantTreatCount:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "treatment-app")

			rec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/campaigns", tc.body)
			require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tc.wantHasTreatment {
				treatments, _ := resp["AdditionalTreatments"].([]any)
				assert.Len(t, treatments, tc.wantTreatCount)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Campaign state machine: detailed transitions
// ──────────────────────────────────────────────────

func TestCampaign_StateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		updateBody map[string]any
		name       string
		wantState  string
		wantStatus int
	}{
		{
			name:       "create_scheduled_is_default",
			createBody: map[string]any{"Name": "c1", "SegmentId": "seg-1"},
			updateBody: nil,
			wantState:  "SCHEDULED",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "pause_transitions_to_paused",
			createBody: map[string]any{"Name": "c2", "SegmentId": "seg-1"},
			updateBody: map[string]any{"IsPaused": true},
			wantState:  "PAUSED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_paused_direct",
			createBody: map[string]any{"Name": "c3", "SegmentId": "seg-1", "IsPaused": true},
			updateBody: nil,
			wantState:  "PAUSED",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "unpause_returns_to_scheduled",
			createBody: map[string]any{"Name": "c4", "SegmentId": "seg-1", "IsPaused": true},
			updateBody: map[string]any{"IsPaused": false},
			wantState:  "SCHEDULED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_segment_keeps_scheduled",
			createBody: map[string]any{"Name": "c5", "SegmentId": "seg-1"},
			updateBody: map[string]any{
				"SegmentId":      "seg-2",
				"SegmentVersion": 2,
			},
			wantState:  "SCHEDULED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_limits_keeps_scheduled",
			createBody: map[string]any{"Name": "c6", "SegmentId": "seg-1"},
			updateBody: map[string]any{"Limits": map[string]any{"Daily": 100}},
			wantState:  "SCHEDULED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_hook_keeps_scheduled",
			createBody: map[string]any{"Name": "c7", "SegmentId": "seg-1"},
			updateBody: map[string]any{
				"Hook": map[string]any{
					"LambdaFunctionName": "arn:aws:lambda:us-east-1:123:function:fn",
					"Mode":               "FILTER",
				},
			},
			wantState:  "SCHEDULED",
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "sm-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/campaigns", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			campaignID := createResp["Id"].(string)

			if tc.updateBody == nil {
				state, _ := createResp["State"].(map[string]any)
				require.NotNil(t, state)
				assert.Equal(t, tc.wantState, state["CampaignStatus"])

				return
			}

			updateRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/campaigns/"+campaignID, tc.updateBody)
			assert.Equal(t, tc.wantStatus, updateRec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))

			state, _ := updateResp["State"].(map[string]any)
			require.NotNil(t, state)
			assert.Equal(t, tc.wantState, state["CampaignStatus"])
		})
	}
}

// ──────────────────────────────────────────────────
// Campaign date range KPIs
// ──────────────────────────────────────────────────

func TestCampaign_DateRangeKpi(t *testing.T) {
	t.Parallel()

	kpiCases := []struct {
		name    string
		kpiName string
	}{
		{name: "successful_deliveries", kpiName: "successful-deliveries"},
		{name: "unique_deliveries", kpiName: "unique-deliveries"},
		{name: "failed_deliveries", kpiName: "failed-deliveries"},
		{name: "open_rate", kpiName: "email-open-rate"},
		{name: "click_rate", kpiName: "email-click-rate"},
	}

	for _, tc := range kpiCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "kpi-campaign-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/campaigns",
				map[string]any{"Name": "kpi-camp", "SegmentId": "seg-1"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			campaignID := cr["Id"].(string)

			kpiRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/campaigns/"+campaignID+"/kpis/daterange/"+tc.kpiName, nil)
			require.Equal(t, http.StatusOK, kpiRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(kpiRec.Body.Bytes(), &resp))
			assert.Equal(t, appID, resp["ApplicationId"])
			assert.Equal(t, campaignID, resp["CampaignId"])
			assert.Equal(t, tc.kpiName, resp["KpiName"])
			assert.NotNil(t, resp["KpiResult"])
		})
	}
}

// ──────────────────────────────────────────────────
// Journey state machine: full transitions
// ──────────────────────────────────────────────────

func TestCampaign_GetActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		campaignBody map[string]any
		name         string
	}{
		{
			name:         "activities_for_basic_campaign",
			campaignBody: map[string]any{"Name": "act-camp-1", "SegmentId": "seg-1"},
		},
		{
			name: "activities_for_campaign_with_schedule",
			campaignBody: map[string]any{
				"Name":      "act-camp-2",
				"SegmentId": "seg-1",
				"Schedule": map[string]any{
					"StartTime": "2026-06-01T00:00:00Z",
					"Frequency": "DAILY",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "camp-act-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/campaigns", tc.campaignBody)
			require.Equal(t, http.StatusCreated, createRec.Code)
			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			campaignID := cr["Id"].(string)

			actRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/campaigns/"+campaignID+"/activities", nil)
			require.Equal(t, http.StatusOK, actRec.Code)

			var actResp map[string]any
			require.NoError(t, json.Unmarshal(actRec.Body.Bytes(), &actResp))
			assert.Contains(t, actResp, "Item")
		})
	}
}

// ──────────────────────────────────────────────────
// ListTemplates: all template types
// ──────────────────────────────────────────────────

func TestCampaign_SegmentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantSegVersion float64
	}{
		{
			name: "segment_version_one",
			body: map[string]any{
				"Name":           "sv-campaign-1",
				"SegmentId":      "seg-001",
				"SegmentVersion": 1,
			},
			wantSegVersion: 1,
		},
		{
			name: "segment_version_five",
			body: map[string]any{
				"Name":           "sv-campaign-2",
				"SegmentId":      "seg-001",
				"SegmentVersion": 5,
			},
			wantSegVersion: 5,
		},
		{
			name: "default_segment_version_zero",
			body: map[string]any{
				"Name":      "sv-campaign-3",
				"SegmentId": "seg-001",
			},
			wantSegVersion: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "segver-app")

			rec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/campaigns", tc.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			if tc.wantSegVersion != 0 {
				assert.InDelta(t, tc.wantSegVersion, resp["SegmentVersion"], 0.001)
			} else {
				sv, _ := resp["SegmentVersion"].(float64)
				assert.InDelta(t, float64(0), sv, 0.001)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Journey: runs and execution metrics after activation
// ──────────────────────────────────────────────────

func TestCampaign_TemplateConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		templateConfig map[string]any
		name           string
	}{
		{
			name: "email_template",
			templateConfig: map[string]any{
				"EmailTemplate": map[string]any{
					"Name":    "welcome-email",
					"Version": "1",
				},
			},
		},
		{
			name: "push_template",
			templateConfig: map[string]any{
				"PushTemplate": map[string]any{
					"Name":    "promo-push",
					"Version": "2",
				},
			},
		},
		{
			name: "sms_template",
			templateConfig: map[string]any{
				"SMSTemplate": map[string]any{
					"Name":    "reminder-sms",
					"Version": "1",
				},
			},
		},
		{
			name: "voice_template",
			templateConfig: map[string]any{
				"VoiceTemplate": map[string]any{
					"Name":    "alert-voice",
					"Version": "1",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "camp-templ-app")
			segID := createTestSegment(t, h, appID, "seg-1")

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
				map[string]any{
					"Name":                  "tmpl-campaign",
					"SegmentId":             segID,
					"TemplateConfiguration": tc.templateConfig,
				})
			require.Equal(t, http.StatusCreated, createRec.Code)

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			campID := cr["Id"].(string)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var c map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &c))

			tmplCfg, ok := c["TemplateConfiguration"].(map[string]any)
			assert.True(t, ok, "TemplateConfiguration must be present")

			// Verify at least one template key preserved
			for k, v := range tc.templateConfig {
				got, exists := tmplCfg[k]
				assert.True(t, exists, "key %q must be in TemplateConfiguration", k)
				assert.Equal(t, v, got)
			}
		})
	}
}

func TestCampaign_Limits_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "camp-limits-app")
	segID := createTestSegment(t, h, appID, "seg-limits")

	limits := map[string]any{
		"Daily":             200,
		"Total":             5000,
		"MessagesPerSecond": 50,
		"Session":           3,
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{
			"Name":      "limits-campaign",
			"SegmentId": segID,
			"Limits":    limits,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	campID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var c map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &c))

	got := c["Limits"].(map[string]any)
	assert.EqualValues(t, 200, got["Daily"])
	assert.EqualValues(t, 5000, got["Total"])
	assert.EqualValues(t, 50, got["MessagesPerSecond"])
	assert.EqualValues(t, 3, got["Session"])
}

func TestCampaign_Hook_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "camp-hook-app")
	segID := createTestSegment(t, h, appID, "seg-hook")

	hook := map[string]any{
		"LambdaFunctionName": "arn:aws:lambda:us-east-1:123456789012:function:SegmentFilter",
		"Mode":               "FILTER",
		"WebUrl":             "",
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{
			"Name":      "hook-campaign",
			"SegmentId": segID,
			"Hook":      hook,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	campID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var c map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &c))

	got := c["Hook"].(map[string]any)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:SegmentFilter", got["LambdaFunctionName"])
	assert.Equal(t, "FILTER", got["Mode"])
}

func TestCampaign_TemplateConfiguration_OnUpdate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "camp-tmpl-update-app")
	segID := createTestSegment(t, h, appID, "seg-x")

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "tmpl-upd-campaign", "SegmentId": segID})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	campID := cr["Id"].(string)

	// Update to set template configuration
	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campID,
		map[string]any{
			"Name":      "tmpl-upd-campaign",
			"SegmentId": segID,
			"TemplateConfiguration": map[string]any{
				"EmailTemplate": map[string]any{"Name": "v2-email", "Version": "2"},
			},
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var c map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &c))

	tmpl := c["TemplateConfiguration"].(map[string]any)
	email := tmpl["EmailTemplate"].(map[string]any)
	assert.Equal(t, "v2-email", email["Name"])
	assert.Equal(t, "2", email["Version"])
}

// ──────────────────────────────────────────────────
// Segments — dynamic (SegmentGroups) + IMPORT
// ──────────────────────────────────────────────────

func TestCampaign_CustomDeliveryConfiguration(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "camp-custom-delivery-app")
	segID := createTestSegment(t, h, appID, "seg-custom")

	customDelivery := map[string]any{
		"DeliveryUri":   "arn:aws:lambda:us-east-1:123456789012:function:CustomDelivery",
		"EndpointTypes": []any{"CUSTOM"},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{
			"Name":                        "custom-delivery-campaign",
			"SegmentId":                   segID,
			"CustomDeliveryConfiguration": customDelivery,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	campID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var c map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &c))

	got := c["CustomDeliveryConfiguration"].(map[string]any)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:CustomDelivery", got["DeliveryUri"])
	ep := got["EndpointTypes"].([]any)
	assert.Contains(t, ep, "CUSTOM")
}

// ──────────────────────────────────────────────────
// Segment — SegmentGroups update replaces existing
// ──────────────────────────────────────────────────

func TestPagination_Campaigns_NextToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "paged-campaigns-app")

	// Create 5 campaigns.
	for i := range 5 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/campaigns",
			map[string]any{"Name": fmt.Sprintf("camp-%02d", i)})
	}

	// Page 1: page-size=2 → 2 items, NextToken set.
	p1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=2", nil)
	require.Equal(t, http.StatusOK, p1Rec.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(p1Rec.Body.Bytes(), &p1))

	items1, _ := p1["Item"].([]any)
	assert.Len(t, items1, 2, "page 1 should contain 2 items")
	nextToken, ok := p1["NextToken"].(string)
	require.True(t, ok, "NextToken must be set when more items exist")
	assert.NotEmpty(t, nextToken)

	// Page 2: use the token.
	p2Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=2&token="+nextToken, nil)
	require.Equal(t, http.StatusOK, p2Rec.Code)

	var p2 map[string]any
	require.NoError(t, json.Unmarshal(p2Rec.Body.Bytes(), &p2))

	items2, _ := p2["Item"].([]any)
	assert.Len(t, items2, 2, "page 2 should contain 2 items")
	nextToken2, ok2 := p2["NextToken"].(string)
	require.True(t, ok2, "NextToken must be set for page 2")

	// Page 3: final page has 1 item, no NextToken.
	p3Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=2&token="+nextToken2, nil)
	require.Equal(t, http.StatusOK, p3Rec.Code)

	var p3 map[string]any
	require.NoError(t, json.Unmarshal(p3Rec.Body.Bytes(), &p3))

	items3, _ := p3["Item"].([]any)
	assert.Len(t, items3, 1, "page 3 should contain the remaining 1 item")
	_, hasMore := p3["NextToken"].(string)
	assert.False(t, hasMore, "no NextToken on the last page")
}

func TestPagination_Campaigns_NoToken_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "no-token-app")

	for i := range 3 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/campaigns",
			map[string]any{"Name": fmt.Sprintf("c-%d", i)})
	}

	// No page-size → default 100 → all 3 returned without NextToken.
	rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, _ := resp["Item"].([]any)
	assert.Len(t, items, 3)
	assert.Nil(t, resp["NextToken"], "no NextToken when all items fit in one page")
}

func TestPagination_TokenEncoding(t *testing.T) {
	t.Parallel()

	// Verify that NextToken is a base64-encoded integer offset.
	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "token-encoding-app")

	for i := range 5 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/campaigns",
			map[string]any{"Name": fmt.Sprintf("c-%d", i)})
	}

	rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tok, _ := resp["NextToken"].(string)
	require.NotEmpty(t, tok)

	raw, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err, "NextToken must be valid base64")

	offset, err := strconv.Atoi(string(raw))
	require.NoError(t, err, "decoded token must be a numeric offset")
	assert.Equal(t, 3, offset, "offset after first page of 3 must be 3")
}

func TestPagination_EmptyList_NoToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "empty-list-app")

	rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=10", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, _ := resp["Item"].([]any)
	assert.Empty(t, items)
	assert.Nil(t, resp["NextToken"])
}

// TestAudit6_GetCampaignVersions_CrossAppIsolation verifies a campaign
// belonging to one app is not visible through another app's versions path,
// even when the campaign ID happens to be known.
func TestGetCampaignVersions_CrossAppIsolation(t *testing.T) {
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
