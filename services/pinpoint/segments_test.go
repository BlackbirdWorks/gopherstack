package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// createTestSegment is a helper that creates a Pinpoint segment and returns its ID.
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

func TestSegmentFullDTO_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body              map[string]any
		name              string
		wantSegmentType   string
		wantStatus        int
		wantHasDimensions bool
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
			// The real WriteSegmentRequest has no ImportDefinition member
			// (pinpoint@v1.42.4 types/types.go:7240) -- it's only ever
			// derived from CreateImportJob. CreateSegment must ignore it.
			name: "segment_with_import_definition_ignored",
			body: map[string]any{
				"Name": "imported",
				"ImportDefinition": map[string]any{
					"S3Url":   "s3://my-bucket/endpoints.csv",
					"RoleArn": "arn:aws:iam::123456789012:role/PinpointRole",
					"Format":  "CSV",
				},
			},
			wantStatus:      http.StatusCreated,
			wantSegmentType: "DIMENSIONAL",
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

			assert.Nil(t, resp["ImportDefinition"], "CreateSegment can never set ImportDefinition")
		})
	}
}

func TestSegmentUpdate_DimensionsRoundTrip(t *testing.T) {
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
			// The real WriteSegmentRequest has no ImportDefinition member
			// (pinpoint@v1.42.4 types/types.go:7240) -- UpdateSegment must
			// ignore it, not flip the segment to IMPORT type.
			name: "update_ignores_import_definition",
			updateBody: map[string]any{
				"ImportDefinition": map[string]any{
					"S3Url":   "s3://bucket/data.json",
					"RoleArn": "arn:aws:iam::123:role/Role",
					"Format":  "JSON",
				},
			},
			wantSegmentType: "DIMENSIONAL",
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

func TestSegmentVersioning(t *testing.T) {
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

func TestSegment_VersionRetrieval(t *testing.T) {
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

func TestSegmentVersions_Deeper(t *testing.T) {
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

func TestSegmentJobsDeeper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantPath func(appID, segID string) string
		name     string
		jobType  string
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

// TestSegment_ImportType drives CreateImportJob, the only real way to get an
// IMPORT-type segment with a populated ImportDefinition -- the real
// WriteSegmentRequest has no ImportDefinition member (pinpoint@v1.42.4
// types/types.go:7240), so CreateSegment/UpdateSegment can never set it
// directly.
func TestSegment_ImportType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		roleArn    string
		s3Url      string
		format     string
		wantFormat string
		wantS3Url  string
	}{
		{
			name:       "csv_import",
			roleArn:    "arn:aws:iam::123456789012:role/S3ImportRole",
			s3Url:      "s3://my-bucket/segments/users.csv",
			format:     "CSV",
			wantFormat: "CSV",
			wantS3Url:  "s3://my-bucket/segments/users.csv",
		},
		{
			name:       "json_import",
			roleArn:    "arn:aws:iam::123456789012:role/S3ImportRole",
			s3Url:      "s3://my-bucket/segments/users.json",
			format:     "JSON",
			wantFormat: "JSON",
			wantS3Url:  "s3://my-bucket/segments/users.json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "seg-import-app")

			jobRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
				map[string]any{
					"RoleArn": tc.roleArn,
					"S3Url":   tc.s3Url,
					"Format":  tc.format,
				})
			require.Equal(t, http.StatusCreated, jobRec.Code)

			var jr map[string]any
			require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &jr))
			definition := jr["Definition"].(map[string]any)
			segID := definition["SegmentId"].(string)
			require.NotEmpty(t, segID)

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/apps/"+appID+"/segments/"+segID, nil)
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

func TestSegment_DynamicWithSegmentGroups(t *testing.T) {
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

func TestSegment_DimensionsAttributes(t *testing.T) {
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

func TestSegment_UpdatePreservesType(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-type-preserve-app")

	// Create the import segment the real way: CreateImportJob, not
	// CreateSegment's ImportDefinition (the real WriteSegmentRequest has no
	// such member -- pinpoint@v1.42.4 types/types.go:7240).
	jobRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/R",
			"S3Url":   "s3://bucket/file.csv",
			"Format":  "CSV",
		})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	var jr map[string]any
	require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &jr))
	segID := jr["Definition"].(map[string]any)["SegmentId"].(string)
	require.NotEmpty(t, segID)

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

func TestSegment_SegmentGroups_UpdateReplaces(t *testing.T) {
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

func TestPagination_Segments_NextToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "paged-segments-app")

	for i := range 4 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/segments",
			map[string]any{"Name": fmt.Sprintf("seg-%02d", i)})
	}

	p1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments?page-size=2", nil)
	require.Equal(t, http.StatusOK, p1Rec.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(p1Rec.Body.Bytes(), &p1))

	items1, _ := p1["Item"].([]any)
	assert.Len(t, items1, 2)
	tok, ok := p1["NextToken"].(string)
	require.True(t, ok, "NextToken must be present")

	p2Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments?page-size=2&token="+tok, nil)
	var p2 map[string]any
	require.NoError(t, json.Unmarshal(p2Rec.Body.Bytes(), &p2))

	items2, _ := p2["Item"].([]any)
	assert.Len(t, items2, 2)
	assert.Nil(t, p2["NextToken"], "last page has no NextToken")
}

// TestCoverage_SegmentCRUD covers GetSegment, GetSegments, UpdateSegment, DeleteSegment,
// GetSegmentVersion, GetSegmentVersions, GetSegmentExportJobs, GetSegmentImportJobs.
func TestSegmentCRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "segment-crud-app")

	// Create segment.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "test-segment"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	segmentID, _ := createResp["Id"].(string)
	require.NotEmpty(t, segmentID)

	// GetSegment.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegments.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateSegment.
	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segmentID,
		map[string]any{"Name": "updated-segment"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentVersion.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/versions/1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentVersions.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentExportJobs.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/jobs/export", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentImportJobs.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/jobs/import", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteSegment.
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/segments/"+segmentID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateSegmentAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/segments",
		map[string]any{"Name": "orphan-segment"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateSegment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_segment",
			body:       map[string]any{"Name": "my-segment"},
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
			appID := createTestApp(t, h, "segment-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "DIMENSIONAL", resp["SegmentType"])
			}
		})
	}
}

// TestGetSegmentVersion_UnknownVersionNotFound locks that GetSegmentVersion
// 404s for a version number absent from the segment's history, matching the
// documented NotFoundException response on the real
// /v1/apps/{appId}/segments/{segmentId}/versions/{version} resource, instead
// of silently substituting the current segment under the wrong Version
// number. Mirrors TestGetCampaignVersion_UnknownVersionNotFound.
func TestGetSegmentVersion_UnknownVersionNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "segment-version-404-app")
	segmentID := createTestSegment(t, h, appID, "s1")

	// Version 1 exists (created by CreateSegment) -- confirm it's reachable.
	v1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+segmentID+"/versions/1", nil)
	require.Equal(t, http.StatusOK, v1Rec.Code)

	// Version 999 was never created -- must 404, not fall back to version 1's
	// (or the current segment's) content.
	missingRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+segmentID+"/versions/999", nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(missingRec.Body).Decode(&errResp))
	assert.Equal(t, "NotFoundException", errResp["__type"])
}

// TestHandler_GetSegments_DuplicateNames_NoDropOrDupAcrossPages proves GetSegments loses
// (or repeats) segments at a page boundary when several segments in the same app share a
// Name. Segment names have no uniqueness constraint (CreateSegment never checks for an
// existing Name), yet GetSegments sorts solely by Name with no secondary key, over a
// *store.Table map walk whose iteration order varies between calls; handleGetSegments
// then pages that resort with an offset cursor (applyPageParams). Looped since this
// depends on map iteration reshuffling a tie group between the calls backing page 1 and
// page 2, which does not reproduce on every run.
func TestHandler_GetSegments_DuplicateNames_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		h := newHandlerForTest(t)
		appID := createTestApp(t, h, "segment-pg-tie-app")

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for range dupCount {
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
				map[string]any{"Name": "dup-segment-name"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			created[resp["Id"].(string)] = true
		}

		seen := make(map[string]bool, dupCount)
		path := "/v1/apps/" + appID + "/segments?page-size=2"

		for range dupCount + 1 {
			rec := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			items, _ := resp["Item"].([]any)
			for _, item := range items {
				s, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[s["Id"].(string)] = true
			}

			nextToken, hasToken := resp["NextToken"].(string)
			if !hasToken {
				break
			}

			path = "/v1/apps/" + appID + "/segments?page-size=2&token=" + url.QueryEscape(nextToken)
		}

		assert.Equal(t, created, seen, "paged GetSegments dropped or duplicated same-named segments across pages")
	}
}
