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
// Journey + Activities — batch-3
// ──────────────────────────────────────────────────

func TestAudit4_Journey_LinkedActivityGraph(t *testing.T) {
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

func TestAudit4_Journey_QuietTimeAndOpenHours(t *testing.T) {
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
					map[string]any{"Name": "New Year", "StartDateTime": "2025-01-01T00:00:00Z", "EndDateTime": "2025-01-01T23:59:59Z"},
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

func TestAudit4_Journey_LimitsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-limits-app")

	limits := map[string]any{
		"DailyCap":              100,
		"EndpointReentryCap":    2,
		"MessagesPerSecond":     50,
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

func TestAudit4_Journey_RandomSplitAndHoldout(t *testing.T) {
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

func TestAudit4_Journey_UpdateActivities(t *testing.T) {
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

func TestAudit4_Campaign_TemplateConfiguration_RoundTrip(t *testing.T) {
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
					"Name":                "tmpl-campaign",
					"SegmentId":           segID,
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

func TestAudit4_Campaign_Limits_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "camp-limits-app")
	segID := createTestSegment(t, h, appID, "seg-limits")

	limits := map[string]any{
		"Daily":              200,
		"Total":              5000,
		"MessagesPerSecond":  50,
		"Session":            3,
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

func TestAudit4_Campaign_Hook_RoundTrip(t *testing.T) {
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

func TestAudit4_Campaign_TemplateConfiguration_OnUpdate(t *testing.T) {
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

func TestAudit4_Segment_ImportType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		importDef  map[string]any
		name       string
		wantFormat string
		wantS3Url  string
	}{
		{
			name: "csv_import",
			importDef: map[string]any{
				"RoleArn": "arn:aws:iam::123456789012:role/S3ImportRole",
				"S3Url":   "s3://my-bucket/segments/users.csv",
				"Format":  "CSV",
			},
			wantFormat: "CSV",
			wantS3Url:  "s3://my-bucket/segments/users.csv",
		},
		{
			name: "json_import",
			importDef: map[string]any{
				"RoleArn": "arn:aws:iam::123456789012:role/S3ImportRole",
				"S3Url":   "s3://my-bucket/segments/users.json",
				"Format":  "JSON",
			},
			wantFormat: "JSON",
			wantS3Url:  "s3://my-bucket/segments/users.json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "seg-import-app")

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
				map[string]any{
					"Name":             "import-segment",
					"ImportDefinition": tc.importDef,
				})
			require.Equal(t, http.StatusCreated, createRec.Code)

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

			assert.Equal(t, "IMPORT", cr["SegmentType"])

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/segments/"+cr["Id"].(string), nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var s map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &s))

			assert.Equal(t, "IMPORT", s["SegmentType"])

			importDef := s["ImportDefinition"].(map[string]any)
			assert.Equal(t, tc.wantFormat, importDef["Format"])
			assert.Equal(t, tc.wantS3Url, importDef["S3Url"])
		})
	}
}

func TestAudit4_Segment_DynamicWithSegmentGroups(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-dynamic-app")

	segGroups := map[string]any{
		"Groups": []any{
			map[string]any{
				"Dimensions": []any{
					map[string]any{
						"Attributes": map[string]any{
							"plan": map[string]any{
								"AttributeType": "INCLUSIVE",
								"Values":        []any{"premium", "enterprise"},
							},
						},
					},
				},
				"SourceType": "ALL",
				"Type":       "ALL",
			},
		},
		"Include": "ALL",
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{
			"Name":          "dynamic-group-segment",
			"SegmentGroups": segGroups,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

	// No ImportDefinition → DIMENSIONAL type
	assert.Equal(t, "DIMENSIONAL", cr["SegmentType"])

	segID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var s map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &s))

	assert.Equal(t, "DIMENSIONAL", s["SegmentType"])

	sg := s["SegmentGroups"].(map[string]any)
	assert.Equal(t, "ALL", sg["Include"])
	groups := sg["Groups"].([]any)
	assert.Len(t, groups, 1)
}

func TestAudit4_Segment_DimensionsAttributes(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-dims-app")

	dims := map[string]any{
		"Attributes": map[string]any{
			"country": map[string]any{
				"AttributeType": "INCLUSIVE",
				"Values":        []any{"US", "CA"},
			},
		},
		"UserAttributes": map[string]any{
			"membership": map[string]any{
				"AttributeType": "INCLUSIVE",
				"Values":        []any{"gold"},
			},
		},
		"Demographic": map[string]any{
			"AppVersion": map[string]any{
				"DimensionType": "INCLUSIVE",
				"Values":        []any{"3.0"},
			},
		},
		"Location": map[string]any{
			"Country": map[string]any{
				"DimensionType": "INCLUSIVE",
				"Values":        []any{"US"},
			},
		},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{
			"Name":       "dims-segment",
			"Dimensions": dims,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	segID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var s map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &s))

	d := s["Dimensions"].(map[string]any)

	attrs := d["Attributes"].(map[string]any)
	country := attrs["country"].(map[string]any)
	assert.Equal(t, "INCLUSIVE", country["AttributeType"])
	vals := country["Values"].([]any)
	assert.Contains(t, vals, "US")
	assert.Contains(t, vals, "CA")

	ua := d["UserAttributes"].(map[string]any)
	assert.NotNil(t, ua["membership"])

	demo := d["Demographic"].(map[string]any)
	assert.NotNil(t, demo["AppVersion"])

	loc := d["Location"].(map[string]any)
	assert.NotNil(t, loc["Country"])
}

func TestAudit4_Segment_UpdatePreservesType(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-type-preserve-app")

	// Create import segment
	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{
			"Name": "type-preserve-seg",
			"ImportDefinition": map[string]any{
				"RoleArn": "arn:aws:iam::123456789012:role/R",
				"S3Url":   "s3://bucket/file.csv",
				"Format":  "CSV",
			},
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	segID := cr["Id"].(string)
	assert.Equal(t, "IMPORT", cr["SegmentType"])

	// Update name only — type should remain IMPORT
	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segID,
		map[string]any{"Name": "type-preserve-seg-v2"})
	require.Equal(t, http.StatusOK, putRec.Code)

	var ur map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &ur))
	assert.Equal(t, "IMPORT", ur["SegmentType"])
	assert.Equal(t, "type-preserve-seg-v2", ur["Name"])

	// ImportDefinition should still be there
	impDef := ur["ImportDefinition"].(map[string]any)
	assert.Equal(t, "CSV", impDef["Format"])
}

// ──────────────────────────────────────────────────
// Message templates — push, SMS, InApp deeper
// ──────────────────────────────────────────────────

func TestAudit4_PushTemplate_PerPlatformOverrides(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/promo-push/push",
		map[string]any{
			"Body":  "Default body",
			"Title": "Default title",
			"APNS": map[string]any{
				"Body":  "iOS promo",
				"Title": "iOS title",
				"Sound": "default",
				"Badge": 1,
			},
			"GCM": map[string]any{
				"Body":  "Android promo",
				"Title": "Android title",
				"Sound": "notification.mp3",
				"IconReference": "ic_notification",
			},
			"TemplateDescription": "Cross-platform promo push",
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/promo-push/push", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "Default body", tmpl["Body"])
	assert.Equal(t, "Default title", tmpl["Title"])

	apns := tmpl["APNS"].(map[string]any)
	assert.Equal(t, "iOS promo", apns["Body"])
	assert.Equal(t, "iOS title", apns["Title"])
	assert.Equal(t, "default", apns["Sound"])

	gcm := tmpl["GCM"].(map[string]any)
	assert.Equal(t, "Android promo", gcm["Body"])
	assert.Equal(t, "Android title", gcm["Title"])
	assert.Equal(t, "notification.mp3", gcm["Sound"])
}

func TestAudit4_PushTemplate_UpdatePerPlatform(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/push-upd/push",
		map[string]any{
			"Body":  "v1 body",
			"APNS": map[string]any{"Body": "v1 ios"},
		})

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/push-upd/push",
		map[string]any{
			"Body":  "v2 body",
			"APNS": map[string]any{"Body": "v2 ios", "Sound": "ding"},
			"GCM":  map[string]any{"Body": "v2 android"},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/push-upd/push", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "v2 body", tmpl["Body"])
	apns := tmpl["APNS"].(map[string]any)
	assert.Equal(t, "v2 ios", apns["Body"])
	assert.Equal(t, "ding", apns["Sound"])

	gcm := tmpl["GCM"].(map[string]any)
	assert.Equal(t, "v2 android", gcm["Body"])
}

func TestAudit4_InAppTemplate_ContentStructure(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	content := []any{
		map[string]any{
			"BodyConfig": map[string]any{
				"Alignment": "CENTER",
				"Body":      "Upgrade to Premium today!",
				"TextColor": "#FFFFFF",
			},
			"HeaderConfig": map[string]any{
				"Alignment": "CENTER",
				"Header":    "Special Offer",
				"TextColor": "#FFFF00",
			},
			"BackgroundColor": "#0000FF",
			"PrimaryBtn": map[string]any{
				"DefaultConfig": map[string]any{
					"BackgroundColor": "#FFFFFF",
					"BorderRadius":    4,
					"ButtonAction":    "LINK",
					"Link":            "https://example.com/upgrade",
					"Text":            "Upgrade Now",
					"TextColor":       "#000000",
				},
			},
			"SecondaryBtn": map[string]any{
				"DefaultConfig": map[string]any{
					"ButtonAction": "CLOSE",
					"Text":         "Maybe Later",
					"TextColor":    "#888888",
				},
			},
		},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/promo-inapp/inapp",
		map[string]any{
			"Layout":              "BOTTOM_BANNER",
			"TemplateDescription": "Upgrade promo",
			"Content":             content,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/promo-inapp/inapp", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "BOTTOM_BANNER", tmpl["Layout"])
	assert.Equal(t, "Upgrade promo", tmpl["TemplateDescription"])

	gotContent := tmpl["Content"].([]any)
	require.Len(t, gotContent, 1)

	c0 := gotContent[0].(map[string]any)
	assert.Equal(t, "#0000FF", c0["BackgroundColor"])

	header := c0["HeaderConfig"].(map[string]any)
	assert.Equal(t, "Special Offer", header["Header"])
	assert.Equal(t, "#FFFF00", header["TextColor"])

	body := c0["BodyConfig"].(map[string]any)
	assert.Equal(t, "Upgrade to Premium today!", body["Body"])

	primary := c0["PrimaryBtn"].(map[string]any)
	defaultCfg := primary["DefaultConfig"].(map[string]any)
	assert.Equal(t, "LINK", defaultCfg["ButtonAction"])
	assert.Equal(t, "https://example.com/upgrade", defaultCfg["Link"])
}

func TestAudit4_InAppTemplate_MultipleContentItems(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	content := []any{
		map[string]any{
			"BodyConfig":      map[string]any{"Body": "Slide 1"},
			"BackgroundColor": "#FF0000",
		},
		map[string]any{
			"BodyConfig":      map[string]any{"Body": "Slide 2"},
			"BackgroundColor": "#00FF00",
		},
		map[string]any{
			"BodyConfig":      map[string]any{"Body": "Slide 3"},
			"BackgroundColor": "#0000FF",
		},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/multi-slide/inapp",
		map[string]any{
			"Layout":  "CAROUSEL",
			"Content": content,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/multi-slide/inapp", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	gotContent := tmpl["Content"].([]any)
	require.Len(t, gotContent, 3)
	assert.Equal(t, "#FF0000", gotContent[0].(map[string]any)["BackgroundColor"])
	assert.Equal(t, "#00FF00", gotContent[1].(map[string]any)["BackgroundColor"])
	assert.Equal(t, "#0000FF", gotContent[2].(map[string]any)["BackgroundColor"])
}

func TestAudit4_SMSTemplate_SenderID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		senderID string
	}{
		{name: "with_sender_id", senderID: "MyBrand"},
		{name: "empty_sender_id", senderID: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			tmplName := "sms-sender-" + tc.name

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+tmplName+"/sms",
				map[string]any{
					"Body":     "Hello from {{sender}}",
					"SenderId": tc.senderID,
				})
			require.Equal(t, http.StatusCreated, createRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/"+tmplName+"/sms", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var tmpl map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

			assert.Equal(t, "Hello from {{sender}}", tmpl["Body"])

			if tc.senderID != "" {
				assert.Equal(t, tc.senderID, tmpl["SenderId"])
			}
		})
	}
}

func TestAudit4_SMSTemplate_UpdateSenderID(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/sms-update/sms",
		map[string]any{"Body": "Hello", "SenderId": "OldBrand"})

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/sms-update/sms",
		map[string]any{"Body": "Hello v2", "SenderId": "NewBrand"})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/sms-update/sms", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "Hello v2", tmpl["Body"])
	assert.Equal(t, "NewBrand", tmpl["SenderId"])
}

// ──────────────────────────────────────────────────
// Event streams — Kinesis
// ──────────────────────────────────────────────────

func TestAudit4_EventStream_PutAndGet_Fields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "evtstream-app")

	destArn := "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
	roleArn := "arn:aws:iam::123456789012:role/PinpointKinesisRole"

	putRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{
			"DestinationStreamArn": destArn,
			"RoleArn":              roleArn,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var es map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &es))

	assert.Equal(t, destArn, es["DestinationStreamArn"])
	assert.Equal(t, roleArn, es["RoleArn"])
	assert.Equal(t, appID, es["ApplicationId"])
	assert.NotEmpty(t, es["LastModifiedDate"], "LastModifiedDate must be set")
}

func TestAudit4_EventStream_Replace(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "evtstream-replace-app")

	stream1 := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-1"
	stream2 := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-2"
	roleArn := "arn:aws:iam::123456789012:role/Role"

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{"DestinationStreamArn": stream1, "RoleArn": roleArn})

	putRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{"DestinationStreamArn": stream2, "RoleArn": roleArn})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var es map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &es))

	assert.Equal(t, stream2, es["DestinationStreamArn"], "second PUT must replace first")
}

func TestAudit4_EventStream_Delete(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "evtstream-del-app")

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{
			"DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
			"RoleArn":              "arn:aws:iam::123456789012:role/R",
		})

	delRec := doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/eventstream", nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/eventstream", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestAudit4_EventStream_AppIsolation(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appA := createTestApp(t, h, "evtstream-app-a")
	appB := createTestApp(t, h, "evtstream-app-b")

	streamA := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-a"
	streamB := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-b"

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appA+"/eventstream",
		map[string]any{"DestinationStreamArn": streamA, "RoleArn": "arn:aws:iam::123:role/r"})
	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appB+"/eventstream",
		map[string]any{"DestinationStreamArn": streamB, "RoleArn": "arn:aws:iam::123:role/r"})

	getA := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appA+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getA.Code)
	var esA map[string]any
	require.NoError(t, json.Unmarshal(getA.Body.Bytes(), &esA))
	assert.Equal(t, streamA, esA["DestinationStreamArn"])

	getB := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appB+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getB.Code)
	var esB map[string]any
	require.NoError(t, json.Unmarshal(getB.Body.Bytes(), &esB))
	assert.Equal(t, streamB, esB["DestinationStreamArn"])
}

// ──────────────────────────────────────────────────
// Channel settings — credential flags per type
// ──────────────────────────────────────────────────

func TestAudit4_Channel_APNS_CertificateAuth(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-apns-cert-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"BundleId":    "com.example.app",
			"Certificate": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
			"PrivateKey":  "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
			"Enabled":     true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"], "certificate auth sets HasCredential")
	assert.Nil(t, ch["HasTokenKey"], "no token key provided — field absent")
	assert.Equal(t, "apns", ch["ChannelType"])
}

func TestAudit4_Channel_APNS_TokenAuth(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-apns-token-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"BundleId":   "com.example.app",
			"TokenKey":   "MFkwEwYH...",
			"TokenKeyId": "ABC123DEF4",
			"TeamId":     "XYZ123456",
			"Enabled":    true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, true, ch["Enabled"])
	// BundleId sets HasCredential; TokenKey sets HasTokenKey
	assert.Equal(t, true, ch["HasCredential"])
	assert.Equal(t, true, ch["HasTokenKey"])
}

func TestAudit4_Channel_APNS_Sandbox(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-apns-sandbox-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns_sandbox",
		map[string]any{
			"BundleId":    "com.example.app",
			"Certificate": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
			"PrivateKey":  "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
			"Enabled":     true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns_sandbox", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "apns_sandbox", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestAudit4_Channel_ADM_HasCredential(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-adm-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/adm",
		map[string]any{
			"ClientId":     "amzn1.application-oa2-client.abc123",
			"ClientSecret": "supersecret",
			"Enabled":      true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/adm", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "adm", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestAudit4_Channel_Baidu_HasCredential(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-baidu-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/baidu",
		map[string]any{
			"ApiKey":    "baidu-api-key-12345",
			"SecretKey": "baidu-secret-key",
			"Enabled":   true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/baidu", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "baidu", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestAudit4_Channel_GCM_ServiceJSON(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-gcm-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/gcm",
		map[string]any{
			"ApiKey":                      "AAAA-gcm-server-key",
			"DefaultAuthenticationMethod": "KEY",
			"Enabled":                     true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/gcm", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "gcm", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.Equal(t, true, ch["HasCredential"])
}

func TestAudit4_Channel_Voice_EnableDisable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-voice-app")

	// Enable
	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/voice",
		map[string]any{"Enabled": true})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/voice", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "voice", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
	assert.NotEmpty(t, ch["CreationDate"])
	assert.NotEmpty(t, ch["LastModifiedDate"])

	// Disable
	putRec2 := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/voice",
		map[string]any{"Enabled": false})
	require.Equal(t, http.StatusOK, putRec2.Code)

	getRec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/voice", nil)
	require.Equal(t, http.StatusOK, getRec2.Code)

	var ch2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &ch2))

	assert.Equal(t, false, ch2["Enabled"])
	assert.EqualValues(t, 2, ch2["Version"])
}

func TestAudit4_Channel_InApp_EnableDisable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-inapp-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/in-app",
		map[string]any{"Enabled": true})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/in-app", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, true, ch["Enabled"])
}

func TestAudit4_Channel_SMS_ShortCode(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-sms-sc-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/sms",
		map[string]any{
			"SenderId":  "MyBrand",
			"ShortCode": "12345",
			"Enabled":   true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/sms", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ch map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ch))

	assert.Equal(t, "sms", ch["ChannelType"])
	assert.Equal(t, true, ch["Enabled"])
}

func TestAudit4_Channel_Delete_ReturnsChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		channelPath string
		channelType string
		updateBody  map[string]any
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

func TestAudit4_Endpoint_UserAttributes_MultiValue(t *testing.T) {
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

func TestAudit4_Endpoint_UserAttributes_UpdateMerge(t *testing.T) {
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

func TestAudit4_GetUserEndpoints_ByUserID(t *testing.T) {
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

func TestAudit4_Endpoint_UserAttributes_BatchUpdate(t *testing.T) {
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

func TestAudit4_Recommender_Attributes_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	attrs := map[string]any{
		"Recommendations.ProductName": "Product Name",
		"Recommendations.Price":       "Price",
		"Recommendations.Category":    "Category",
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
		map[string]any{
			"Name":                          "attr-recommender",
			"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/PinpointRec",
			"RecommendationProviderUri":     "arn:aws:personalize:::campaign/my-campaign",
			"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			"RecommendationsPerMessage":     5,
			"Attributes":                    attrs,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	recID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var r map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r))

	assert.Equal(t, "attr-recommender", r["Name"])
	assert.EqualValues(t, 5, r["RecommendationsPerMessage"])

	gotAttrs := r["Attributes"].(map[string]any)
	assert.Equal(t, "Product Name", gotAttrs["Recommendations.ProductName"])
	assert.Equal(t, "Price", gotAttrs["Recommendations.Price"])
	assert.Equal(t, "Category", gotAttrs["Recommendations.Category"])
}

func TestAudit4_Recommender_RecommendationsPerMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		recommendationsPerMsg int
	}{
		{name: "one", recommendationsPerMsg: 1},
		{name: "five", recommendationsPerMsg: 5},
		{name: "ten", recommendationsPerMsg: 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
				map[string]any{
					"Name":                          "rpm-rec-" + tc.name,
					"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
					"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c",
					"RecommendationProviderIdType":  "PINPOINT_USER_ID",
					"RecommendationsPerMessage":     tc.recommendationsPerMsg,
				})
			require.Equal(t, http.StatusCreated, createRec.Code)

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			recID := cr["Id"].(string)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var r map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r))

			assert.EqualValues(t, tc.recommendationsPerMsg, r["RecommendationsPerMessage"])
		})
	}
}

func TestAudit4_Recommender_UpdateAttributes(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
		map[string]any{
			"Name":                          "update-attr-rec",
			"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
			"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c",
			"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			"RecommendationsPerMessage":     3,
			"Attributes": map[string]any{
				"Recommendations.Name": "Name",
			},
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	recID := cr["Id"].(string)

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/recommenders/"+recID,
		map[string]any{
			"Name":                          "update-attr-rec",
			"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
			"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c",
			"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			"RecommendationsPerMessage":     7,
			"Attributes": map[string]any{
				"Recommendations.Name":  "Name",
				"Recommendations.Score": "Score",
			},
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var r map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r))

	assert.EqualValues(t, 7, r["RecommendationsPerMessage"])
	gotAttrs := r["Attributes"].(map[string]any)
	assert.Equal(t, "Name", gotAttrs["Recommendations.Name"])
	assert.Equal(t, "Score", gotAttrs["Recommendations.Score"])
}

func TestAudit4_Recommender_List_MultipleConfigs(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	names := []string{"rec-alpha", "rec-beta", "rec-gamma"}
	for _, name := range names {
		rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
			map[string]any{
				"Name":                          name,
				"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
				"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c-" + name,
				"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	items := resp["Item"].([]any)
	assert.Len(t, items, 3)

	gotNames := make([]string, 0, 3)
	for _, item := range items {
		r := item.(map[string]any)
		gotNames = append(gotNames, r["Name"].(string))
	}

	assert.ElementsMatch(t, names, gotNames)
}

// ──────────────────────────────────────────────────
// Application settings — deeper fields
// ──────────────────────────────────────────────────

func TestAudit4_ApplicationSettings_CampaignHook(t *testing.T) {
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

func TestAudit4_ApplicationSettings_QuietTime(t *testing.T) {
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

func TestAudit4_ApplicationSettings_Limits(t *testing.T) {
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

func TestAudit4_ApplicationSettings_MultipleUpdates(t *testing.T) {
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

func TestAudit4_ApplicationSettings_EventTagging(t *testing.T) {
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

func TestAudit4_Journey_RefreshOnSegmentUpdate(t *testing.T) {
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

func TestAudit4_Journey_StartCondition(t *testing.T) {
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

func TestAudit4_Campaign_CustomDeliveryConfiguration(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "camp-custom-delivery-app")
	segID := createTestSegment(t, h, appID, "seg-custom")

	customDelivery := map[string]any{
		"DeliveryUri":    "arn:aws:lambda:us-east-1:123456789012:function:CustomDelivery",
		"EndpointTypes":  []any{"CUSTOM"},
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

func TestAudit4_Segment_SegmentGroups_UpdateReplaces(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-sg-update-app")

	v1Groups := map[string]any{
		"Groups": []any{
			map[string]any{
				"SourceType": "ALL",
				"Type":       "ALL",
				"Dimensions": []any{
					map[string]any{
						"Attributes": map[string]any{
							"plan": map[string]any{
								"AttributeType": "INCLUSIVE",
								"Values":        []any{"basic"},
							},
						},
					},
				},
			},
		},
		"Include": "ALL",
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "sg-update-seg", "SegmentGroups": v1Groups})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	segID := cr["Id"].(string)

	v2Groups := map[string]any{
		"Groups": []any{
			map[string]any{
				"SourceType": "ALL",
				"Type":       "ALL",
				"Dimensions": []any{
					map[string]any{
						"Attributes": map[string]any{
							"plan": map[string]any{
								"AttributeType": "INCLUSIVE",
								"Values":        []any{"premium", "enterprise"},
							},
						},
					},
				},
			},
		},
		"Include": "ANY",
	}

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segID,
		map[string]any{"Name": "sg-update-seg", "SegmentGroups": v2Groups})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var s map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &s))

	sg := s["SegmentGroups"].(map[string]any)
	assert.Equal(t, "ANY", sg["Include"])
}

// ──────────────────────────────────────────────────
// Voice template — Tags round-trip
// ──────────────────────────────────────────────────

func TestAudit4_VoiceTemplate_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/tagged-voice/voice",
		map[string]any{
			"Body": "Hello {{user.FirstName}}, your appointment is tomorrow.",
			"tags": map[string]any{
				"env":  "production",
				"team": "comms",
			},
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/tagged-voice/voice", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "Hello {{user.FirstName}}, your appointment is tomorrow.", tmpl["Body"])
	assert.NotEmpty(t, tmpl["Arn"])
	assert.NotEmpty(t, tmpl["CreationDate"])
}

// ──────────────────────────────────────────────────
// InApp template — update replaces content slice
// ──────────────────────────────────────────────────

func TestAudit4_InAppTemplate_UpdateContent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/inapp-update/inapp",
		map[string]any{
			"Layout": "MIDDLE_BANNER",
			"Content": []any{
				map[string]any{
					"BodyConfig":      map[string]any{"Body": "v1 content"},
					"BackgroundColor": "#111111",
				},
			},
		})

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/inapp-update/inapp",
		map[string]any{
			"Layout": "TOP_BANNER",
			"Content": []any{
				map[string]any{
					"BodyConfig":      map[string]any{"Body": "v2 slide a"},
					"BackgroundColor": "#AAAAAA",
				},
				map[string]any{
					"BodyConfig":      map[string]any{"Body": "v2 slide b"},
					"BackgroundColor": "#BBBBBB",
				},
			},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/inapp-update/inapp", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "TOP_BANNER", tmpl["Layout"])
	content := tmpl["Content"].([]any)
	require.Len(t, content, 2)
	assert.Equal(t, "#AAAAAA", content[0].(map[string]any)["BackgroundColor"])
	assert.Equal(t, "#BBBBBB", content[1].(map[string]any)["BackgroundColor"])
}

// ──────────────────────────────────────────────────
// Endpoint — metrics field
// ──────────────────────────────────────────────────

func TestAudit4_Endpoint_Metrics_RoundTrip(t *testing.T) {
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

func TestAudit4_Endpoint_Demographic_Location(t *testing.T) {
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

func createTestSegment(t *testing.T, h *pinpoint.Handler, appID, name string) string {
	t.Helper()

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/segments",
		map[string]any{"Name": name})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	id, _ := resp["Id"].(string)
	require.NotEmpty(t, id)

	return id
}
