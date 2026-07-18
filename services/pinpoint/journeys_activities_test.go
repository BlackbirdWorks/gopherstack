package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJourney_StateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantFinal   string
		transitions []string
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

func TestJourney_ListByState(t *testing.T) {
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

func TestJourney_Activities(t *testing.T) {
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

func TestJourney_DateRangeKpi(t *testing.T) {
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

func TestJourney_RunsAfterActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		activate bool
		wantRuns int
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

func TestJourney_LinkedActivityGraph(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-graph-app")

	// Three-node chain: wait → email → sms
	activities := map[string]any{
		"act-wait": map[string]any{
			"WAIT": map[string]any{
				"WaitTime":     map[string]any{"WaitFor": "PT30M"},
				"NextActivity": "act-email",
			},
		},
		"act-email": map[string]any{
			"EMAIL": map[string]any{
				"MessageConfig": map[string]any{"FromAddress": "no-reply@example.com"},
				"TemplateName":  "welcome",
				"NextActivity":  "act-sms",
			},
		},
		"act-sms": map[string]any{
			"SMS": map[string]any{
				"MessageConfig": map[string]any{"MessageType": "PROMOTIONAL"},
				"NextActivity":  "",
			},
		},
	}

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name":          "linked-journey",
			"StartActivity": "act-wait",
			"Activities":    activities,
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	assert.Equal(t, "act-wait", j["StartActivity"])

	acts := j["Activities"].(map[string]any)
	assert.Len(t, acts, 3)

	waitAct := acts["act-wait"].(map[string]any)["WAIT"].(map[string]any)
	assert.Equal(t, "act-email", waitAct["NextActivity"])

	emailAct := acts["act-email"].(map[string]any)["EMAIL"].(map[string]any)
	assert.Equal(t, "act-sms", emailAct["NextActivity"])
	assert.Equal(t, "welcome", emailAct["TemplateName"])
}

func TestJourney_QuietTimeAndOpenHours(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-quiet-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name": "quiet-journey",
			"QuietTime": map[string]any{
				"End":   "09:00",
				"Start": "21:00",
			},
			"OpenHours": map[string]any{
				"EMAIL": []any{
					map[string]any{"Start": "08:00", "End": "20:00"},
				},
			},
			"ClosedDays": map[string]any{
				"EMAIL": []any{
					map[string]any{
						"Name":          "New Year",
						"StartDateTime": "2025-01-01T00:00:00Z",
						"EndDateTime":   "2025-01-01T23:59:59Z",
					},
				},
			},
			"WaitForQuietTime": true,
			"LocalTime":        true,
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	qt := j["QuietTime"].(map[string]any)
	assert.Equal(t, "21:00", qt["Start"])
	assert.Equal(t, "09:00", qt["End"])

	oh := j["OpenHours"].(map[string]any)
	assert.NotNil(t, oh["EMAIL"])

	cd := j["ClosedDays"].(map[string]any)
	assert.NotNil(t, cd["EMAIL"])

	assert.Equal(t, true, j["WaitForQuietTime"])
	assert.Equal(t, true, j["LocalTime"])
}

func TestJourney_LimitsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-limits-app")

	limits := map[string]any{
		"DailyCap":                100,
		"EndpointReentryCap":      2,
		"MessagesPerSecond":       50,
		"EndpointReentryInterval": "P7D",
	}

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name":   "limits-journey",
			"Limits": limits,
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	got := j["Limits"].(map[string]any)
	assert.EqualValues(t, 100, got["DailyCap"])
	assert.EqualValues(t, 2, got["EndpointReentryCap"])
	assert.EqualValues(t, 50, got["MessagesPerSecond"])
	assert.Equal(t, "P7D", got["EndpointReentryInterval"])
}

func TestJourney_RandomSplitAndHoldout(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-split-app")

	activities := map[string]any{
		"act-holdout": map[string]any{
			"HOLDOUT": map[string]any{
				"Percentage":   15,
				"NextActivity": "act-email",
			},
		},
		"act-email": map[string]any{
			"EMAIL": map[string]any{
				"TemplateName": "promo",
				"NextActivity": "act-split",
			},
		},
		"act-split": map[string]any{
			"RANDOM_SPLIT": map[string]any{
				"Branches": []any{
					map[string]any{"NextActivity": "act-sms-a", "Percentage": 60},
					map[string]any{"NextActivity": "act-sms-b", "Percentage": 40},
				},
			},
		},
		"act-sms-a": map[string]any{
			"SMS": map[string]any{"MessageConfig": map[string]any{"MessageType": "PROMOTIONAL"}},
		},
		"act-sms-b": map[string]any{
			"SMS": map[string]any{"MessageConfig": map[string]any{"MessageType": "TRANSACTIONAL"}},
		},
	}

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name":          "split-journey",
			"StartActivity": "act-holdout",
			"Activities":    activities,
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	acts := j["Activities"].(map[string]any)
	holdout := acts["act-holdout"].(map[string]any)["HOLDOUT"].(map[string]any)
	assert.EqualValues(t, 15, holdout["Percentage"])

	split := acts["act-split"].(map[string]any)["RANDOM_SPLIT"].(map[string]any)
	branches := split["Branches"].([]any)
	assert.Len(t, branches, 2)
}

func TestJourney_UpdateActivities(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-update-act-app")

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name": "update-act-journey",
			"Activities": map[string]any{
				"act-1": map[string]any{
					"WAIT": map[string]any{"WaitTime": map[string]any{"WaitFor": "PT1H"}},
				},
			},
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	// Add a second activity by updating the journey
	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/journeys/"+jID,
		map[string]any{
			"Name": "update-act-journey",
			"Activities": map[string]any{
				"act-1": map[string]any{
					"WAIT": map[string]any{
						"WaitTime":     map[string]any{"WaitFor": "PT2H"},
						"NextActivity": "act-2",
					},
				},
				"act-2": map[string]any{
					"EMAIL": map[string]any{"TemplateName": "follow-up"},
				},
			},
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	acts := j["Activities"].(map[string]any)
	assert.Len(t, acts, 2)

	wait := acts["act-1"].(map[string]any)["WAIT"].(map[string]any)
	wt := wait["WaitTime"].(map[string]any)
	assert.Equal(t, "PT2H", wt["WaitFor"])
	assert.Equal(t, "act-2", wait["NextActivity"])
}

// ──────────────────────────────────────────────────
// Campaigns — template, holdout, limits
// ──────────────────────────────────────────────────

func TestJourney_RefreshOnSegmentUpdate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-refresh-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name":                   "refresh-journey",
			"RefreshOnSegmentUpdate": true,
			"RefreshFrequency":       "PT1H",
			"Schedule": map[string]any{
				"StartTime": "2025-01-01T08:00:00Z",
				"EndTime":   "2025-12-31T23:59:59Z",
				"Timezone":  "UTC",
			},
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	assert.Equal(t, true, j["RefreshOnSegmentUpdate"])
	assert.Equal(t, "PT1H", j["RefreshFrequency"])

	sched := j["Schedule"].(map[string]any)
	assert.Equal(t, "2025-01-01T08:00:00Z", sched["StartTime"])
	assert.Equal(t, "UTC", sched["Timezone"])
}

func TestJourney_StartCondition(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-startcond-app")

	startCondition := map[string]any{
		"Description": "entry from segment",
		"SegmentStartCondition": map[string]any{
			"SegmentId": "seg-entry",
		},
	}

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{
			"Name":           "startcond-journey",
			"StartCondition": startCondition,
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	jID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+jID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var j map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &j))

	sc := j["StartCondition"].(map[string]any)
	assert.Equal(t, "entry from segment", sc["Description"])
	ssc := sc["SegmentStartCondition"].(map[string]any)
	assert.Equal(t, "seg-entry", ssc["SegmentId"])
}

// ──────────────────────────────────────────────────
// Campaign — CustomDeliveryConfiguration
// ──────────────────────────────────────────────────
