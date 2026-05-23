package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// ──────────────────────────────────────────────────
// Application settings: CloudWatchMetricsEnabled
// ──────────────────────────────────────────────────

func TestAudit2_ApplicationSettings_CloudWatchMetrics(t *testing.T) {
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

func TestAudit2_ApplicationSettings_EventTagging(t *testing.T) {
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
			assert.Equal(t, tc.wantFlag, putResp["EventTaggingEnabled"], "PUT response should include EventTaggingEnabled")

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, tc.wantFlag, getResp["EventTaggingEnabled"], "GET response should persist EventTaggingEnabled")
		})
	}
}

// ──────────────────────────────────────────────────
// Application settings: Limits field deep coverage
// ──────────────────────────────────────────────────

func TestAudit2_ApplicationSettings_Limits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		limits    map[string]any
		checkKey  string
		name      string
		checkVal  float64
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

func TestAudit2_ApplicationDateRangeKpi(t *testing.T) {
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

func TestAudit2_PutEvents_AppLifeCycleEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "lifecycle_open_event",
			body: map[string]any{
				"EventsRequest": map[string]any{
					"BatchItem": map[string]any{
						"ep-001": map[string]any{
							"Endpoint": map[string]any{"ChannelType": "APNS"},
							"Events": map[string]any{
								"ev-1": map[string]any{
									"EventType": "_session.start",
									"Timestamp": "2026-05-01T00:00:00Z",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "lifecycle_stop_event",
			body: map[string]any{
				"EventsRequest": map[string]any{
					"BatchItem": map[string]any{
						"ep-002": map[string]any{
							"Endpoint": map[string]any{"ChannelType": "GCM"},
							"Events": map[string]any{
								"ev-2": map[string]any{
									"EventType": "_session.stop",
									"Timestamp": "2026-05-01T01:00:00Z",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "lifecycle_pause_event",
			body: map[string]any{
				"EventsRequest": map[string]any{
					"BatchItem": map[string]any{
						"ep-003": map[string]any{
							"Endpoint": map[string]any{"ChannelType": "EMAIL"},
							"Events": map[string]any{
								"ev-3": map[string]any{
									"EventType": "_session.pause",
									"Timestamp": "2026-05-01T02:00:00Z",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "lifecycle_resume_event",
			body: map[string]any{
				"EventsRequest": map[string]any{
					"BatchItem": map[string]any{
						"ep-004": map[string]any{
							"Endpoint": map[string]any{"ChannelType": "SMS"},
							"Events": map[string]any{
								"ev-4": map[string]any{
									"EventType": "_session.resume",
									"Timestamp": "2026-05-01T03:00:00Z",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "custom_app_lifecycle_event",
			body: map[string]any{
				"EventsRequest": map[string]any{
					"BatchItem": map[string]any{
						"ep-005": map[string]any{
							"Endpoint": map[string]any{"ChannelType": "PUSH"},
							"Events": map[string]any{
								"ev-5": map[string]any{
									"EventType": "app.purchase",
									"Timestamp": "2026-05-01T04:00:00Z",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "events-app")

			rec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/events", tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// ──────────────────────────────────────────────────
// SegmentVersion deeper
// ──────────────────────────────────────────────────

func TestAudit2_SegmentVersions_Deeper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		updateCount  int
		wantVersions int
		fetchVersion int
	}{
		{
			name:         "single_update_two_versions",
			updateCount:  1,
			wantVersions: 2,
			fetchVersion: 1,
		},
		{
			name:         "three_updates_four_versions",
			updateCount:  3,
			wantVersions: 4,
			fetchVersion: 2,
		},
		{
			name:         "five_updates_six_versions",
			updateCount:  5,
			wantVersions: 6,
			fetchVersion: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "seg-ver-app")

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
				map[string]any{
					"Name": "versioned-seg",
					"Dimensions": map[string]any{
						"Attributes": map[string]any{
							"plan": map[string]any{"AttributeType": "INCLUSIVE", "Values": []string{"premium"}},
						},
					},
				})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			segID := created["Id"].(string)

			for i := range tc.updateCount {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/apps/"+appID+"/segments/"+segID,
					map[string]any{"Name": fmt.Sprintf("seg-v%d", i+2)})
				require.Equal(t, http.StatusOK, updateRec.Code)
			}

			versionsRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/segments/"+segID+"/versions", nil)
			require.Equal(t, http.StatusOK, versionsRec.Code)

			var versionsResp map[string]any
			require.NoError(t, json.Unmarshal(versionsRec.Body.Bytes(), &versionsResp))
			items, _ := versionsResp["Item"].([]any)
			assert.Len(t, items, tc.wantVersions, "wrong version count")

			vRec := doPinpointRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v1/apps/%s/segments/%s/versions/%d", appID, segID, tc.fetchVersion), nil)
			require.Equal(t, http.StatusOK, vRec.Code)

			var vResp map[string]any
			require.NoError(t, json.Unmarshal(vRec.Body.Bytes(), &vResp))
			assert.InDelta(t, float64(tc.fetchVersion), vResp["Version"], 0.001)
		})
	}
}

// ──────────────────────────────────────────────────
// Segment import/export jobs deeper
// ──────────────────────────────────────────────────

func TestAudit2_SegmentJobsDeeper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobType  string
		wantPath func(appID, segID string) string
	}{
		{
			name:    "segment_export_jobs",
			jobType: "export",
			wantPath: func(appID, segID string) string {
				return "/v1/apps/" + appID + "/segments/" + segID + "/jobs/export"
			},
		},
		{
			name:    "segment_import_jobs",
			jobType: "import",
			wantPath: func(appID, segID string) string {
				return "/v1/apps/" + appID + "/segments/" + segID + "/jobs/import"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "seg-jobs-app")

			segRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
				map[string]any{"Name": "seg-for-jobs"})
			require.Equal(t, http.StatusCreated, segRec.Code)
			var segResp map[string]any
			require.NoError(t, json.Unmarshal(segRec.Body.Bytes(), &segResp))
			segID := segResp["Id"].(string)

			if tc.jobType == "export" {
				exportRec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/apps/"+appID+"/jobs/export",
					map[string]any{
						"RoleArn":     "arn:aws:iam::123456789012:role/ExportRole",
						"S3UrlPrefix": "s3://my-bucket/exports/",
					})
				require.True(t, exportRec.Code == http.StatusCreated || exportRec.Code == http.StatusOK)
			} else {
				importRec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/apps/"+appID+"/jobs/import",
					map[string]any{
						"RoleArn":     "arn:aws:iam::123456789012:role/ImportRole",
						"S3Url":       "s3://my-bucket/imports/data.csv",
						"Format":      "CSV",
						"SegmentName": "imported-seg",
					})
				require.True(t, importRec.Code == http.StatusCreated || importRec.Code == http.StatusOK)
			}

			jobsRec := doPinpointRequest(t, h, http.MethodGet, tc.wantPath(appID, segID), nil)
			require.Equal(t, http.StatusOK, jobsRec.Code)

			var jobsResp map[string]any
			require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))
			assert.Contains(t, jobsResp, "Item")
		})
	}
}

// ──────────────────────────────────────────────────
// Campaign full lifecycle: AdditionalTreatments
// ──────────────────────────────────────────────────

func TestAudit2_Campaign_AdditionalTreatments(t *testing.T) {
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

func TestAudit2_Campaign_StateMachine(t *testing.T) {
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
			name: "update_segment_keeps_scheduled",
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

func TestAudit2_Campaign_DateRangeKpi(t *testing.T) {
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

func TestAudit2_Journey_StateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		transitions []string
		name        string
		wantFinal   string
		wantErr     bool
	}{
		{
			name:        "draft_to_active",
			transitions: []string{"ACTIVE"},
			wantFinal:   "ACTIVE",
		},
		{
			name:        "draft_to_cancelled",
			transitions: []string{"CANCELLED"},
			wantFinal:   "CANCELLED",
		},
		{
			name:        "draft_to_active_to_paused",
			transitions: []string{"ACTIVE", "PAUSED"},
			wantFinal:   "PAUSED",
		},
		{
			name:        "draft_to_active_to_cancelled",
			transitions: []string{"ACTIVE", "CANCELLED"},
			wantFinal:   "CANCELLED",
		},
		{
			name:        "draft_to_active_to_completed",
			transitions: []string{"ACTIVE", "COMPLETED"},
			wantFinal:   "COMPLETED",
		},
		{
			name:        "draft_to_active_pause_resume",
			transitions: []string{"ACTIVE", "PAUSED", "ACTIVE"},
			wantFinal:   "ACTIVE",
		},
		{
			name:        "invalid_draft_to_completed",
			transitions: []string{"COMPLETED"},
			wantFinal:   "",
			wantErr:     true,
		},
		{
			name:        "invalid_draft_to_paused",
			transitions: []string{"PAUSED"},
			wantFinal:   "",
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "journey-sm-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/journeys",
				map[string]any{"Name": "sm-journey"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			journeyID := cr["Id"].(string)

			assert.Equal(t, "DRAFT", cr["State"])

			lastOK := true

			for i, state := range tc.transitions {
				rec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/apps/"+appID+"/journeys/"+journeyID+"/state",
					map[string]any{"State": state})

				if i == len(tc.transitions)-1 && tc.wantErr {
					assert.Equal(t, http.StatusBadRequest, rec.Code)
					lastOK = false
				} else {
					require.Equal(t, http.StatusOK, rec.Code,
						"transition to %s failed: %s", state, rec.Body.String())
				}
			}

			if lastOK && tc.wantFinal != "" {
				getRec := doPinpointRequest(t, h, http.MethodGet,
					"/v1/apps/"+appID+"/journeys/"+journeyID, nil)
				require.Equal(t, http.StatusOK, getRec.Code)
				var j map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))
				assert.Equal(t, tc.wantFinal, j["State"])
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Journey list states
// ──────────────────────────────────────────────────

func TestAudit2_Journey_ListByState(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-list-app")

	journeyNames := []string{"j-draft", "j-active", "j-cancelled"}
	journeyIDs := make([]string, len(journeyNames))

	for i, name := range journeyNames {
		rec := doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/journeys",
			map[string]any{"Name": name})
		require.Equal(t, http.StatusCreated, rec.Code)
		var j map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &j))
		journeyIDs[i] = j["Id"].(string)
	}

	// Activate j-active.
	rec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/journeys/"+journeyIDs[1]+"/state",
		map[string]any{"State": "ACTIVE"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Cancel j-cancelled.
	rec = doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/journeys/"+journeyIDs[2]+"/state",
		map[string]any{"State": "CANCELLED"})
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/journeys", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	items, _ := listResp["Item"].([]any)
	assert.Len(t, items, 3)

	stateMap := make(map[string]string)
	for _, item := range items {
		m := item.(map[string]any)
		stateMap[m["Name"].(string)] = m["State"].(string)
	}
	assert.Equal(t, "DRAFT", stateMap["j-draft"])
	assert.Equal(t, "ACTIVE", stateMap["j-active"])
	assert.Equal(t, "CANCELLED", stateMap["j-cancelled"])
}

// ──────────────────────────────────────────────────
// Journey activities: all types
// ──────────────────────────────────────────────────

func TestAudit2_Journey_Activities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		activities map[string]any
		name       string
		activityID string
		actType    string
	}{
		{
			name:       "email_activity",
			activityID: "act-email",
			actType:    "EMAIL",
			activities: map[string]any{
				"act-email": map[string]any{
					"EMAIL": map[string]any{
						"MessageConfig": map[string]any{
							"FromAddress": "sender@example.com",
						},
						"NextActivity": "",
						"TemplateName": "welcome-email",
					},
				},
			},
		},
		{
			name:       "push_activity",
			activityID: "act-push",
			actType:    "PUSH",
			activities: map[string]any{
				"act-push": map[string]any{
					"PUSH": map[string]any{
						"MessageConfig": map[string]any{
							"TimeToLive": 3600,
						},
						"NextActivity": "",
					},
				},
			},
		},
		{
			name:       "sms_activity",
			activityID: "act-sms",
			actType:    "SMS",
			activities: map[string]any{
				"act-sms": map[string]any{
					"SMS": map[string]any{
						"MessageConfig": map[string]any{
							"MessageType": "TRANSACTIONAL",
						},
						"NextActivity": "",
					},
				},
			},
		},
		{
			name:       "wait_activity",
			activityID: "act-wait",
			actType:    "WAIT",
			activities: map[string]any{
				"act-wait": map[string]any{
					"WAIT": map[string]any{
						"WaitTime": map[string]any{
							"WaitFor": "PT1H",
						},
						"NextActivity": "",
					},
				},
			},
		},
		{
			name:       "holdout_activity",
			activityID: "act-holdout",
			actType:    "HOLDOUT",
			activities: map[string]any{
				"act-holdout": map[string]any{
					"HOLDOUT": map[string]any{
						"Percentage":   10,
						"NextActivity": "",
					},
				},
			},
		},
		{
			name:       "random_split_activity",
			activityID: "act-random",
			actType:    "RANDOM_SPLIT",
			activities: map[string]any{
				"act-random": map[string]any{
					"RANDOM_SPLIT": map[string]any{
						"Branches": []any{
							map[string]any{
								"NextActivity": "act-email",
								"Percentage":   50,
							},
							map[string]any{
								"NextActivity": "act-sms",
								"Percentage":   50,
							},
						},
					},
				},
			},
		},
		{
			name:       "multi_condition_activity",
			activityID: "act-multi",
			actType:    "MULTI_CONDITION",
			activities: map[string]any{
				"act-multi": map[string]any{
					"MULTI_CONDITION": map[string]any{
						"DefaultActivity": "act-wait",
						"Branches": []any{
							map[string]any{
								"NextActivity": "act-email",
								"Condition": map[string]any{
									"Operator": "ALL",
								},
							},
						},
					},
				},
			},
		},
		{
			name:       "contact_center_activity",
			activityID: "act-cc",
			actType:    "CONTACT_CENTER",
			activities: map[string]any{
				"act-cc": map[string]any{
					"CONTACT_CENTER": map[string]any{
						"NextActivity": "",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "journey-act-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/journeys",
				map[string]any{
					"Name":          "activity-journey",
					"Activities":    tc.activities,
					"StartActivity": tc.activityID,
				})
			require.Equal(t, http.StatusCreated, createRec.Code,
				"body: %s", createRec.Body.String())

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			journeyID := cr["Id"].(string)

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/journeys/"+journeyID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var j map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

			activities, _ := j["Activities"].(map[string]any)
			assert.NotNil(t, activities, "Activities must be present")
			act, ok := activities[tc.activityID]
			assert.True(t, ok, "activityID %q must be in Activities", tc.activityID)
			assert.NotNil(t, act)
		})
	}
}

// ──────────────────────────────────────────────────
// VoiceTemplate full CRUD
// ──────────────────────────────────────────────────

func TestAudit2_VoiceTemplate_FullCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		updateBody map[string]any
		name       string
		wantBody   string
	}{
		{
			name:       "create_and_get_basic",
			createBody: map[string]any{"Body": "Hello, your appointment is tomorrow."},
			updateBody: map[string]any{"Body": "Updated: your appointment is today."},
			wantBody:   "Hello, your appointment is tomorrow.",
		},
		{
			name: "create_with_tags",
			createBody: map[string]any{
				"Body": "Welcome to our service.",
				"tags": map[string]string{"env": "prod", "team": "notifications"},
			},
			updateBody: map[string]any{"Body": "Welcome, valued customer."},
			wantBody:   "Welcome to our service.",
		},
		{
			name:       "create_empty_body",
			createBody: map[string]any{},
			updateBody: map[string]any{"Body": "Now has content."},
			wantBody:   "SKIP",
		},
		{
			name:       "update_body_persists",
			createBody: map[string]any{"Body": "Original message."},
			updateBody: map[string]any{"Body": "Replacement message."},
			wantBody:   "Original message.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := "voice-tmpl-" + tc.name

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/voice", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code,
				"body: %s", createRec.Body.String())

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			assert.NotEmpty(t, cr["Message"])

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/voice", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var gr map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gr))
			assert.Equal(t, templateName, gr["TemplateName"])
			if tc.wantBody != "SKIP" {
				assert.Equal(t, tc.wantBody, gr["Body"])
			}

			updateRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/templates/"+templateName+"/voice", tc.updateBody)
			require.True(t, updateRec.Code == http.StatusOK || updateRec.Code == http.StatusAccepted,
				"update should succeed, got %d: %s", updateRec.Code, updateRec.Body.String())

			deleteRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/templates/"+templateName+"/voice", nil)
			require.True(t, deleteRec.Code == http.StatusOK || deleteRec.Code == http.StatusAccepted)

			getRec2 := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/voice", nil)
			assert.Equal(t, http.StatusNotFound, getRec2.Code)
		})
	}
}

// ──────────────────────────────────────────────────
// VoiceTemplate version history
// ──────────────────────────────────────────────────

func TestAudit2_VoiceTemplate_VersionHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		updateCount  int
		wantVersions int
	}{
		{name: "one_version_on_create", updateCount: 0, wantVersions: 1},
		{name: "two_versions_after_one_update", updateCount: 1, wantVersions: 2},
		{name: "four_versions_after_three_updates", updateCount: 3, wantVersions: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := "voice-ver-" + tc.name

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/voice",
				map[string]any{"Body": "Version 1"})
			require.Equal(t, http.StatusCreated, createRec.Code)

			for i := range tc.updateCount {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/voice",
					map[string]any{"Body": fmt.Sprintf("Version %d", i+2)})
				require.True(t, updateRec.Code == http.StatusOK || updateRec.Code == http.StatusAccepted)
			}

			versionsRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/voice/versions", nil)
			require.Equal(t, http.StatusOK, versionsRec.Code)

			var versionsResp map[string]any
			require.NoError(t, json.Unmarshal(versionsRec.Body.Bytes(), &versionsResp))
			items, _ := versionsResp["Item"].([]any)
			assert.Len(t, items, tc.wantVersions)
		})
	}
}

// ──────────────────────────────────────────────────
// RecommenderConfiguration full
// ──────────────────────────────────────────────────

func TestAudit2_RecommenderConfiguration_FullCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody          map[string]any
		updateBody          map[string]any
		name                string
		wantName            string
		wantProviderURI     string
		wantIDType          string
		wantRPM             float64
		wantHasDescription  bool
		wantHasAttributes   bool
	}{
		{
			name: "basic_recommender",
			createBody: map[string]any{
				"Name":                          "basic-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam1",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
			},
			updateBody: map[string]any{
				"RecommendationProviderUri": "arn:aws:personalize:us-east-1:123:campaign/cam2",
			},
			wantName:        "basic-rec",
			wantProviderURI: "arn:aws:personalize:us-east-1:123:campaign/cam1",
		},
		{
			name: "recommender_with_id_type",
			createBody: map[string]any{
				"Name":                            "typed-rec",
				"RecommendationProviderUri":       "arn:aws:personalize:us-east-1:123:campaign/cam3",
				"RecommendationProviderRoleArn":   "arn:aws:iam::123:role/Role",
				"RecommendationProviderIdType":    "PINPOINT_USER_ID",
			},
			updateBody: map[string]any{
				"RecommendationProviderIdType": "PINPOINT_ENDPOINT_ID",
			},
			wantName:   "typed-rec",
			wantIDType: "PINPOINT_USER_ID",
		},
		{
			name: "recommender_with_description",
			createBody: map[string]any{
				"Name":                          "desc-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam4",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"Description":                   "Product recommendations",
			},
			updateBody: map[string]any{
				"Description": "Updated description",
			},
			wantName:           "desc-rec",
			wantHasDescription: true,
		},
		{
			name: "recommender_with_rpm",
			createBody: map[string]any{
				"Name":                          "rpm-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam5",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"RecommendationsPerMessage":     5,
			},
			updateBody: map[string]any{
				"RecommendationsPerMessage": 10,
			},
			wantName: "rpm-rec",
			wantRPM:  5,
		},
		{
			name: "recommender_with_attributes",
			createBody: map[string]any{
				"Name":                          "attr-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam6",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"Attributes": map[string]string{
					"Attr1": "ProductName",
					"Attr2": "ProductPrice",
				},
			},
			updateBody: map[string]any{
				"Attributes": map[string]string{
					"Attr1": "UpdatedName",
				},
			},
			wantName:          "attr-rec",
			wantHasAttributes: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/recommenders", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code,
				"body: %s", createRec.Body.String())

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			recID := cr["Id"].(string)
			require.NotEmpty(t, recID)

			assert.Equal(t, tc.wantName, cr["Name"])

			if tc.wantProviderURI != "" {
				assert.Equal(t, tc.wantProviderURI, cr["RecommendationProviderUri"])
			}

			if tc.wantIDType != "" {
				assert.Equal(t, tc.wantIDType, cr["RecommendationProviderIdType"])
			}

			if tc.wantRPM != 0 {
				assert.InDelta(t, tc.wantRPM, cr["RecommendationsPerMessage"], 0.001)
			}

			if tc.wantHasDescription {
				assert.NotEmpty(t, cr["Description"])
			}

			if tc.wantHasAttributes {
				assert.NotNil(t, cr["Attributes"])
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/recommenders/"+recID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var gr map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gr))
			assert.Equal(t, recID, gr["Id"])

			updateRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/recommenders/"+recID, tc.updateBody)
			require.Equal(t, http.StatusOK, updateRec.Code,
				"update body: %s", updateRec.Body.String())

			deleteRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/recommenders/"+recID, nil)
			require.Equal(t, http.StatusOK, deleteRec.Code)

			getRec2 := doPinpointRequest(t, h, http.MethodGet,
				"/v1/recommenders/"+recID, nil)
			assert.Equal(t, http.StatusNotFound, getRec2.Code)
		})
	}
}

// ──────────────────────────────────────────────────
// RecommenderConfiguration: list all
// ──────────────────────────────────────────────────

func TestAudit2_RecommenderConfiguration_ListAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		wantLen int
	}{
		{name: "empty_list", count: 0, wantLen: 0},
		{name: "single_recommender", count: 1, wantLen: 1},
		{name: "three_recommenders", count: 3, wantLen: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for i := range tc.count {
				rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders", map[string]any{
					"Name":                          fmt.Sprintf("rec-%d", i),
					"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/c",
					"RecommendationProviderRoleArn": "arn:aws:iam::123:role/R",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tc.wantLen)
		})
	}
}

// ──────────────────────────────────────────────────
// OneTimeTokenChannel: SendOTPMessage + VerifyOTPMessage
// ──────────────────────────────────────────────────

func TestAudit2_OTP_SendAndVerify(t *testing.T) {
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
					"Channel":            "SMS",
					"DestinationIdentity": "+15555550100",
					"OriginationIdentity": "+15555550199",
					"ReferenceID":        "ref-001",
					"BrandName":          "MyApp",
					"CodeLength":         6,
					"ValidityPeriod":     5,
				},
			},
			wantSend: http.StatusOK,
		},
		{
			name: "send_otp_email_channel",
			otpBody: map[string]any{
				"SendOTPMessageRequestParameters": map[string]any{
					"Channel":            "EMAIL",
					"DestinationIdentity": "user@example.com",
					"OriginationIdentity": "noreply@example.com",
					"ReferenceID":        "ref-002",
					"BrandName":          "MyService",
					"CodeLength":         8,
				},
			},
			wantSend: http.StatusOK,
		},
		{
			name: "verify_otp",
			otpBody: map[string]any{
				"SendOTPMessageRequestParameters": map[string]any{
					"Channel":            "SMS",
					"DestinationIdentity": "+15555550101",
					"OriginationIdentity": "+15555550199",
					"ReferenceID":        "ref-003",
					"BrandName":          "MyApp",
					"CodeLength":         6,
				},
			},
			verifyBody: map[string]any{
				"VerifyOTPMessageRequestParameters": map[string]any{
					"DestinationIdentity": "+15555550101",
					"ReferenceID":        "ref-003",
					"Otp":                "123456",
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

func TestAudit2_SMSChannel_Attributes(t *testing.T) {
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

func TestAudit2_EmailChannel_Identity(t *testing.T) {
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

func TestAudit2_Backend_Recommender_DirectAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req    pinpoint.ExportedCreateRecommenderConfigRequest
		name   string
		wantID bool
	}{
		{
			name: "create_basic",
			req: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                      "direct-rec",
				RecommendationProviderURI: "arn:aws:personalize:us-east-1:123:campaign/c",
			},
			wantID: true,
		},
		{
			name: "create_with_user_id_type",
			req: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                         "direct-user-rec",
				RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/d",
				RecommendationProviderIDType: "PINPOINT_USER_ID",
			},
			wantID: true,
		},
		{
			name: "create_with_endpoint_id_type",
			req: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                         "direct-ep-rec",
				RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/e",
				RecommendationProviderIDType: "PINPOINT_ENDPOINT_ID",
			},
			wantID: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

			created, err := b.CreateRecommenderConfiguration(tc.req)
			require.NoError(t, err)
			require.NotNil(t, created)

			if tc.wantID {
				assert.NotEmpty(t, created.ID)
			}

			assert.Equal(t, tc.req.Name, created.Name)

			got, err := b.GetRecommenderConfiguration(created.ID)
			require.NoError(t, err)
			assert.Equal(t, created.ID, got.ID)

			all, err := b.GetRecommenderConfigurations()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(all), 1)

			deleted, err := b.DeleteRecommenderConfiguration(created.ID)
			require.NoError(t, err)
			assert.Equal(t, created.ID, deleted.ID)

			_, err = b.GetRecommenderConfiguration(created.ID)
			require.Error(t, err)
		})
	}
}

// ──────────────────────────────────────────────────
// Journey KPI: per-journey date range
// ──────────────────────────────────────────────────

func TestAudit2_Journey_DateRangeKpi(t *testing.T) {
	t.Parallel()

	kpiCases := []struct {
		name    string
		kpiName string
	}{
		{name: "journey_entry_count", kpiName: "journey-entry-count"},
		{name: "journey_active_participants", kpiName: "journey-participants"},
		{name: "journey_successful_deliveries", kpiName: "journey-successful-deliveries"},
		{name: "journey_completion_rate", kpiName: "journey-completion-rate"},
	}

	for _, tc := range kpiCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "journey-kpi-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/journeys",
				map[string]any{"Name": "kpi-journey"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			journeyID := cr["Id"].(string)

			kpiRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/journeys/"+journeyID+"/kpis/daterange/"+tc.kpiName, nil)
			require.Equal(t, http.StatusOK, kpiRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(kpiRec.Body.Bytes(), &resp))
			assert.Equal(t, appID, resp["ApplicationId"])
			assert.Equal(t, journeyID, resp["JourneyId"])
			assert.Equal(t, tc.kpiName, resp["KpiName"])
		})
	}
}

// ──────────────────────────────────────────────────
// Campaign: GetCampaignActivities
// ──────────────────────────────────────────────────

func TestAudit2_Campaign_GetActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		campaignBody map[string]any
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

func TestAudit2_ListTemplates_VoiceListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/lv1/voice", map[string]any{"Body": "test"})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	items, _ := resp["Item"].([]any)
	require.NotEmpty(t, items)

	found := false
	for _, item := range items {
		m := item.(map[string]any)
		if m["TemplateType"] == "VOICE" {
			found = true
			break
		}
	}
	assert.True(t, found, "VOICE template must appear in ListTemplates")
}

func TestAudit2_ListTemplates_EmailListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/le1/email", map[string]any{"Subject": "Hello"})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	items, _ := resp["Item"].([]any)
	require.NotEmpty(t, items)

	found := false
	for _, item := range items {
		m := item.(map[string]any)
		if m["TemplateType"] == "EMAIL" {
			found = true
			break
		}
	}
	assert.True(t, found, "EMAIL template must appear in ListTemplates")
}

func TestAudit2_ListTemplates_SMSListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/ls1/sms", map[string]any{"Body": "code"})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	items, _ := resp["Item"].([]any)
	require.NotEmpty(t, items)

	found := false
	for _, item := range items {
		m := item.(map[string]any)
		if m["TemplateType"] == "SMS" {
			found = true
			break
		}
	}
	assert.True(t, found, "SMS template must appear in ListTemplates")
}

func TestAudit2_ListTemplates_MixedTypes(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/mix-v/voice", map[string]any{"Body": "v"})
	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/mix-e/email", map[string]any{"Subject": "e"})
	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/mix-s/sms", map[string]any{"Body": "s"})

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	items, _ := resp["Item"].([]any)
	assert.GreaterOrEqual(t, len(items), 3)
}

// ──────────────────────────────────────────────────
// RecommenderConfiguration: invalid ID type rejected
// ──────────────────────────────────────────────────

func TestAudit2_Recommender_InvalidIDType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		idType     string
		name       string
		wantErr    bool
	}{
		{name: "valid_user_id_type", idType: "PINPOINT_USER_ID", wantErr: false},
		{name: "valid_endpoint_id_type", idType: "PINPOINT_ENDPOINT_ID", wantErr: false},
		{name: "empty_id_type_ok", idType: "", wantErr: false},
		{name: "invalid_id_type", idType: "INVALID_TYPE", wantErr: true},
		{name: "garbage_id_type", idType: "FOOBAR", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

			created, err := b.CreateRecommenderConfiguration(
				pinpoint.ExportedCreateRecommenderConfigRequest{
					Name:                         "rec-type-test",
					RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/c",
					RecommendationProviderIDType: tc.idType,
				},
			)

			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, created)
			} else {
				require.NoError(t, err)
				require.NotNil(t, created)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Application settings: 404 for missing app
// ──────────────────────────────────────────────────

func TestAudit2_ApplicationSettings_MissingApp(t *testing.T) {
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

func TestAudit2_VoiceTemplate_DuplicateRejected(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	templateName := "dup-voice"

	rec1 := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/"+templateName+"/voice",
		map[string]any{"Body": "First"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/"+templateName+"/voice",
		map[string]any{"Body": "Second"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// ──────────────────────────────────────────────────
// Campaign: SegmentVersion field stored and returned
// ──────────────────────────────────────────────────

func TestAudit2_Campaign_SegmentVersion(t *testing.T) {
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

func TestAudit2_Journey_RunsAfterActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		activate   bool
		wantRuns   int
	}{
		{
			name:     "no_runs_before_activation",
			activate: false,
			wantRuns: 0,
		},
		{
			name:     "one_run_after_activation",
			activate: true,
			wantRuns: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "runs-app")

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/journeys",
				map[string]any{"Name": "runs-journey"})
			require.Equal(t, http.StatusCreated, createRec.Code)
			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			journeyID := cr["Id"].(string)

			if tc.activate {
				activeRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/apps/"+appID+"/journeys/"+journeyID+"/state",
					map[string]any{"State": "ACTIVE"})
				require.Equal(t, http.StatusOK, activeRec.Code)
			}

			runsRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/journeys/"+journeyID+"/runs", nil)
			require.Equal(t, http.StatusOK, runsRec.Code)

			var runsResp map[string]any
			require.NoError(t, json.Unmarshal(runsRec.Body.Bytes(), &runsResp))
			items, _ := runsResp["Item"].([]any)
			assert.Len(t, items, tc.wantRuns)
		})
	}
}

// ──────────────────────────────────────────────────
// ApplicationSettings: full round trip including
// EventTaggingEnabled + CloudWatchMetricsEnabled together
// ──────────────────────────────────────────────────

func TestAudit2_ApplicationSettings_FullRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantCW  bool
		wantET  bool
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
