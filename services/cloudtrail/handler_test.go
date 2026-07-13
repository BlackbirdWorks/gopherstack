package cloudtrail_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func newTestCloudTrailHandler() *cloudtrail.Handler {
	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return cloudtrail.NewHandler(backend)
}

func doCloudTrailOp(
	t *testing.T,
	h *cloudtrail.Handler,
	operation string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CloudTrail_20131101."+operation)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseCloudTrailResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestCloudTrailCRUD exercises CreateTrail, GetTrail, DescribeTrails,
// UpdateTrail, and DeleteTrail through the JSON handler.
func TestCloudTrailCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_trail",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "my-trail", resp["Name"])
				assert.NotEmpty(t, resp["TrailARN"])
			},
		},
		{
			name: "create_trail_already_exists",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "create_trail_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"S3BucketName": "my-bucket",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_trail_missing_bucket",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_trail",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				rec := doCloudTrailOp(t, h, "GetTrail", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				trail, ok := resp["Trail"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-trail", trail["Name"])
			},
		},
		{
			name: "get_trail_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GetTrail", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_trails",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "trail-a",
					"S3BucketName": "bucket-a",
				})
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "trail-b",
					"S3BucketName": "bucket-b",
				})
				rec := doCloudTrailOp(t, h, "DescribeTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["trailList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 2)
			},
		},
		{
			name: "describe_trails_by_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "trail-a",
					"S3BucketName": "bucket-a",
				})
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "trail-b",
					"S3BucketName": "bucket-b",
				})
				rec := doCloudTrailOp(t, h, "DescribeTrails", map[string]any{
					"trailNameList": []string{"trail-a"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["trailList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
				item := list[0].(map[string]any)
				assert.Equal(t, "trail-a", item["Name"])
			},
		},
		{
			name: "update_trail",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "old-bucket",
				})
				rec := doCloudTrailOp(t, h, "UpdateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "new-bucket",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "new-bucket", resp["S3BucketName"])
			},
		},
		{
			name: "update_trail_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "UpdateTrail", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_trail",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				rec := doCloudTrailOp(t, h, "DeleteTrail", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				// Verify deleted
				rec2 := doCloudTrailOp(t, h, "GetTrail", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "delete_trail_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteTrail", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailLogging exercises StartLogging, StopLogging, and GetTrailStatus.
func TestCloudTrailLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "start_logging",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				rec := doCloudTrailOp(t, h, "StartLogging", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				// Verify status
				statusRec := doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusOK, statusRec.Code)
				resp := parseCloudTrailResp(t, statusRec)
				assert.Equal(t, true, resp["IsLogging"])
			},
		},
		{
			name: "stop_logging",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				doCloudTrailOp(t, h, "StartLogging", map[string]any{"Name": "my-trail"})
				rec := doCloudTrailOp(t, h, "StopLogging", map[string]any{
					"Name": "my-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				// Verify status
				statusRec := doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
					"Name": "my-trail",
				})
				resp := parseCloudTrailResp(t, statusRec)
				assert.Equal(t, false, resp["IsLogging"])
			},
		},
		{
			name: "start_logging_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "StartLogging", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "stop_logging_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "StopLogging", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "get_trail_status_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailEventSelectors exercises PutEventSelectors and GetEventSelectors.
func TestCloudTrailEventSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "put_and_get_event_selectors",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
					"TrailName": "my-trail",
					"EventSelectors": []map[string]any{
						{
							"ReadWriteType":           "All",
							"IncludeManagementEvents": true,
							"DataResources":           []any{},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["TrailARN"])
				selectors, ok := resp["EventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, selectors, 1)
				// Now get event selectors
				getRec := doCloudTrailOp(t, h, "GetEventSelectors", map[string]any{
					"TrailName": "my-trail",
				})
				assert.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.NotEmpty(t, getResp["TrailARN"])
				getSelectors, ok := getResp["EventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, getSelectors, 1)
			},
		},
		{
			name: "get_event_selectors_empty",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				rec := doCloudTrailOp(t, h, "GetEventSelectors", map[string]any{
					"TrailName": "my-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				selectors, ok := resp["EventSelectors"].([]any)
				require.True(t, ok)
				assert.Empty(t, selectors)
			},
		},
		{
			name: "put_event_selectors_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
					"TrailName":      "missing-trail",
					"EventSelectors": []any{},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailTags exercises AddTags, RemoveTags, and ListTags.
func TestCloudTrailTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "add_and_list_tags",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)

				rec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": trailARN,
					"TagsList": []map[string]string{
						{"Key": "Env", "Value": "test"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				listRec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{trailARN},
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseCloudTrailResp(t, listRec)
				resourceTagList, ok := listResp["ResourceTagList"].([]any)
				require.True(t, ok)
				assert.Len(t, resourceTagList, 1)
				item := resourceTagList[0].(map[string]any)
				assert.Equal(t, trailARN, item["ResourceId"])
				tagsList := item["TagsList"].([]any)
				assert.Len(t, tagsList, 1)
				tag := tagsList[0].(map[string]any)
				assert.Equal(t, "Env", tag["Key"])
				assert.Equal(t, "test", tag["Value"])
			},
		},
		{
			name: "remove_tags",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)

				doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": trailARN,
					"TagsList": []map[string]string{
						{"Key": "Env", "Value": "test"},
						{"Key": "Project", "Value": "foo"},
					},
				})
				rec := doCloudTrailOp(t, h, "RemoveTags", map[string]any{
					"ResourceId": trailARN,
					"TagsList": []map[string]string{
						{"Key": "Env"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "add_tags_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": "arn:aws:cloudtrail:us-east-1:123456789012:trail/missing",
					"TagsList":   []map[string]string{},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailListTrails exercises the ListTrails operation.
func TestCloudTrailListTrails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "list_trails_empty",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "ListTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				trails, ok := resp["Trails"].([]any)
				require.True(t, ok)
				assert.Empty(t, trails)
			},
		},
		{
			name: "list_trails_with_data",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "trail-x",
					"S3BucketName": "bucket-x",
				})
				rec := doCloudTrailOp(t, h, "ListTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				trails, ok := resp["Trails"].([]any)
				require.True(t, ok)
				assert.Len(t, trails, 1)
				trail := trails[0].(map[string]any)
				assert.Equal(t, "trail-x", trail["Name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

func TestCloudTrailLookupEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no_input_returns_empty_list",
			body: nil,
		},
		{
			name: "with_max_results_still_returns_empty",
			body: map[string]any{"MaxResults": 10},
		},
		{
			name: "with_lookup_attribute_still_returns_empty",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "EventName",
						"AttributeValue": "CreateBucket",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestCloudTrailHandler()
			rec := doCloudTrailOp(t, h, "LookupEvents", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			resp := parseCloudTrailResp(t, rec)
			events, ok := resp["Events"].([]any)
			require.True(t, ok)
			assert.Empty(t, events)
		})
	}
}

// TestCloudTrailMetadata exercises RouteMatcher, Name, and ChaosServiceName.
func TestCloudTrailMetadata(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "name",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "CloudTrail", h.Name())
			},
		},
		{
			name: "chaos_service_name",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "cloudtrail", h.ChaosServiceName())
			},
		},
		{
			name: "supported_operations",
			fn: func(t *testing.T) {
				t.Helper()
				ops := h.GetSupportedOperations()
				assert.NotEmpty(t, ops)
				assert.Contains(t, ops, "CreateTrail")
				assert.Contains(t, ops, "DeleteTrail")
			},
		},
		{
			name: "chaos_operations",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
			},
		},
		{
			name: "chaos_regions",
			fn: func(t *testing.T) {
				t.Helper()
				regions := h.ChaosRegions()
				assert.NotEmpty(t, regions)
			},
		},
		{
			name: "match_priority",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Positive(t, h.MatchPriority())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}

// TestCloudTrailRouteMatcher verifies the route matcher accepts/rejects requests.
func TestCloudTrailRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches_CreateTrail",
			target:    "CloudTrail_20131101.CreateTrail",
			wantMatch: true,
		},
		{
			name:      "matches_DescribeTrails",
			target:    "CloudTrail_20131101.DescribeTrails",
			wantMatch: true,
		},
		{
			name:      "no_match_ssm",
			target:    "AmazonSSM.GetParameter",
			wantMatch: false,
		},
		{
			name:      "no_match_empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

// TestCloudTrailPersistence verifies Snapshot and Restore round-trip.
func TestCloudTrailPersistence(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "trail-persist",
		"S3BucketName": "bucket-persist",
	})

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec := doCloudTrailOp(t, h2, "GetTrail", map[string]any{
		"Name": "trail-persist",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	trail, ok := resp["Trail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "trail-persist", trail["Name"])
}

// TestCloudTrailUnknownOperation verifies an unknown operation returns an error.
func TestCloudTrailUnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	rec := doCloudTrailOp(t, h, "NonExistentOperation", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCloudTrailExtractOperation verifies ExtractOperation returns correct name.
func TestCloudTrailExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "create_trail",
			target: "CloudTrail_20131101.CreateTrail",
			wantOp: "CreateTrail",
		},
		{
			name:   "describe_trails",
			target: "CloudTrail_20131101.DescribeTrails",
			wantOp: "DescribeTrails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// TestCloudTrailExtractResource verifies ExtractResource always returns empty string.
func TestCloudTrailExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	e := echo.New()
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c))
}

// TestCloudTrailTrailWithAllFields creates a trail with optional fields set.
func TestCloudTrailTrailWithAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_with_tags_and_options",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":                       "full-trail",
					"S3BucketName":               "bucket",
					"S3KeyPrefix":                "logs/",
					"SnsTopicName":               "my-topic",
					"IncludeGlobalServiceEvents": true,
					"IsMultiRegionTrail":         true,
					"EnableLogFileValidation":    true,
					"TagsList": []map[string]string{
						{"Key": "Env", "Value": "prod"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "full-trail", resp["Name"])
				assert.Equal(t, "logs/", resp["S3KeyPrefix"])
				assert.Equal(t, true, resp["IsMultiRegionTrail"])
				assert.Equal(t, true, resp["LogFileValidationEnabled"])
			},
		},
		{
			name: "get_trail_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "arn-trail",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)
				rec := doCloudTrailOp(t, h, "GetTrail", map[string]any{
					"Name": trailARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				trail, ok := resp["Trail"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "arn-trail", trail["Name"])
			},
		},
		{
			name: "describe_trails_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "arn-trail-2",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)
				rec := doCloudTrailOp(t, h, "DescribeTrails", map[string]any{
					"trailNameList": []string{trailARN},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["trailList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "delete_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "arn-del-trail",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)
				rec := doCloudTrailOp(t, h, "DeleteTrail", map[string]any{
					"Name": trailARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "start_logging_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "log-arn-trail",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)
				rec := doCloudTrailOp(t, h, "StartLogging", map[string]any{
					"Name": trailARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "update_trail_boolean_fields",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "bool-trail",
					"S3BucketName": "bucket",
				})
				boolTrue := true
				boolFalse := false
				rec := doCloudTrailOp(t, h, "UpdateTrail", map[string]any{
					"Name":                       "bool-trail",
					"IncludeGlobalServiceEvents": boolTrue,
					"IsMultiRegionTrail":         boolFalse,
					"EnableLogFileValidation":    boolTrue,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "update_trail_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "UpdateTrail", map[string]any{
					"S3BucketName": "bucket",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "remove_tags_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "RemoveTags", map[string]any{
					"ResourceId": "arn:aws:cloudtrail:us-east-1:123456789012:trail/missing",
					"TagsList":   []map[string]string{{"Key": "Env"}},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "put_event_selectors_with_data_resources",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "data-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
					"TrailName": "data-trail",
					"EventSelectors": []map[string]any{
						{
							"ReadWriteType":           "All",
							"IncludeManagementEvents": true,
							"DataResources": []map[string]any{
								{"Type": "AWS::S3::Object", "Values": []string{"arn:aws:s3:::my-bucket/"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "get_event_selectors_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GetEventSelectors", map[string]any{
					"TrailName": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_tags_empty_resources",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				tagList, ok := resp["ResourceTagList"].([]any)
				require.True(t, ok)
				assert.Empty(t, tagList)
			},
		},
		{
			name: "add_tags_by_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "named-tag-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": "named-tag-trail",
					"TagsList":   []map[string]string{{"Key": "K", "Value": "V"}},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "stop_logging_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "stop-arn-trail",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)
				doCloudTrailOp(t, h, "StartLogging", map[string]any{"Name": trailARN})
				rec := doCloudTrailOp(t, h, "StopLogging", map[string]any{
					"Name": trailARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "get_trail_status_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "status-arn-trail",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)
				rec := doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
					"Name": trailARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "update_trail_optional_string_fields",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "str-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h, "UpdateTrail", map[string]any{
					"Name":                      "str-trail",
					"SnsTopicName":              "topic",
					"CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:123:log-group:test",
					"CloudWatchLogsRoleArn":     "arn:aws:iam::123:role/test",
					"KMSKeyId":                  "arn:aws:kms:us-east-1:123:key/abc",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "arn:aws:logs:us-east-1:123:log-group:test", resp["CloudWatchLogsLogGroupArn"])
				assert.Equal(t, "arn:aws:iam::123:role/test", resp["CloudWatchLogsRoleArn"])
				// The wire key is "KmsKeyId" (matching the real aws-sdk-go-v2
				// deserializer's exact case), not "KMSKeyId".
				assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", resp["KmsKeyId"])
			},
		},
		{
			name: "trail_map_with_all_optional_fields",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":                      "optional-trail",
					"S3BucketName":              "bucket",
					"S3KeyPrefix":               "prefix/",
					"SnsTopicName":              "my-sns",
					"CloudWatchLogsLogGroupArn": "arn:logs",
					"CloudWatchLogsRoleArn":     "arn:role",
					"KMSKeyId":                  "arn:kms",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "prefix/", resp["S3KeyPrefix"])
				assert.Equal(t, "my-sns", resp["SnsTopicName"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailProvider exercises the Provider methods.
func TestCloudTrailProvider(t *testing.T) {
	t.Parallel()

	p := &cloudtrail.Provider{}

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "name",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "CloudTrail", p.Name())
			},
		},
		{
			name: "init",
			fn: func(t *testing.T) {
				t.Helper()
				// Provider.Init requires service.AppContext; test basic init with nil config.
				appCtx := &service.AppContext{}
				reg, err := p.Init(appCtx)
				require.NoError(t, err)
				require.NotNil(t, reg)
				assert.Equal(t, "CloudTrail", reg.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}

// TestCloudTrailChannel exercises CreateChannel and DeleteChannel.
func TestCloudTrailChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_channel_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name":   "my-channel",
					"Source": "custom-source",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["ChannelArn"])
				assert.Equal(t, "my-channel", resp["Name"])
				assert.Equal(t, "custom-source", resp["Source"])
			},
		},
		{
			name: "create_channel_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Source": "custom-source",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_channel_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name":   "del-channel",
					"Source": "src",
				})
				createResp := parseCloudTrailResp(t, createRec)
				channelARN := createResp["ChannelArn"].(string)
				rec := doCloudTrailOp(t, h, "DeleteChannel", map[string]any{
					"Channel": channelARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_channel_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteChannel", map[string]any{
					"Channel": "channel-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailDashboard exercises CreateDashboard and DeleteDashboard.
func TestCloudTrailDashboard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_dashboard_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "my-dashboard",
					"Type": "CUSTOM",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["DashboardArn"])
				assert.Equal(t, "my-dashboard", resp["Name"])
				assert.Equal(t, "CREATED", resp["Status"])
			},
		},
		{
			name: "create_dashboard_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Type": "CUSTOM",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_dashboard_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "del-dashboard",
				})
				createResp := parseCloudTrailResp(t, createRec)
				dashboardARN := createResp["DashboardArn"].(string)
				rec := doCloudTrailOp(t, h, "DeleteDashboard", map[string]any{
					"DashboardId": dashboardARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_dashboard_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteDashboard", map[string]any{
					"DashboardId": "dashboard-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailEventDataStore exercises CreateEventDataStore and DeleteEventDataStore.
func TestCloudTrailEventDataStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_event_data_store_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name":                         "my-eds",
					"MultiRegionEnabled":           true,
					"OrganizationEnabled":          false,
					"TerminationProtectionEnabled": true,
					"RetentionPeriod":              90,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["EventDataStoreArn"])
				assert.Equal(t, "my-eds", resp["Name"])
				assert.Equal(t, "ENABLED", resp["Status"])
				assert.Equal(t, true, resp["MultiRegionEnabled"])
				assert.Equal(t, true, resp["TerminationProtectionEnabled"])
			},
		},
		{
			name: "create_event_data_store_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"RetentionPeriod": 90,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_event_data_store_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "del-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)
				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_event_data_store_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": "eds-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailResourcePolicy exercises DeleteResourcePolicy.
func TestCloudTrailResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "delete_resource_policy_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteResourcePolicy", map[string]any{
					"ResourceArn": "arn:aws:cloudtrail:us-east-1:123456789012:trail/my-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailDeregisterOrgDelegatedAdmin exercises DeregisterOrganizationDelegatedAdmin.
func TestCloudTrailDeregisterOrgDelegatedAdmin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "deregister_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeregisterOrganizationDelegatedAdmin", map[string]any{
					"DelegatedAdminAccountId": "123456789012",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "deregister_missing_account_id",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeregisterOrganizationDelegatedAdmin", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailQuery exercises CancelQuery and DescribeQuery.
func TestCloudTrailQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "cancel_query_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{
					"QueryId": "query-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_query_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{
					"QueryId": "query-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "cancel_and_describe_query",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				// Create a query via StartQuery in backend directly
				q, err := h.Backend.StartQuery("SELECT eventName, eventSource FROM events LIMIT 10", "", "")
				require.NoError(t, err)
				require.NotEmpty(t, q.QueryID)

				cancelRec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{
					"QueryId": q.QueryID,
				})
				assert.Equal(t, http.StatusOK, cancelRec.Code)
				cancelResp := parseCloudTrailResp(t, cancelRec)
				assert.Equal(t, "CANCELLED", cancelResp["QueryStatus"])

				descRec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{
					"QueryId": q.QueryID,
				})
				assert.Equal(t, http.StatusOK, descRec.Code)
				descResp := parseCloudTrailResp(t, descRec)
				assert.Equal(t, "CANCELLED", descResp["QueryStatus"])
				assert.Equal(t, "SELECT eventName, eventSource FROM events LIMIT 10", descResp["QueryString"])
			},
		},
		{
			name: "cancel_already_cancelled_query",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				q, err := h.Backend.StartQuery("SELECT 1", "", "")
				require.NoError(t, err)

				// Cancel it once
				_, err = h.Backend.CancelQuery(q.QueryID)
				require.NoError(t, err)

				// Try cancelling again - should fail with validation error
				rec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{
					"QueryId": q.QueryID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestRefinement1_Reset exercises Reset() on backend and handler.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "reset_clears_trails",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name": "trail-a", "S3BucketName": "bucket",
				})
				h.Reset()
				rec := doCloudTrailOp(t, h, "ListTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["Trails"].([]any)
				require.True(t, ok)
				assert.Empty(t, list)
			},
		},
		{
			name: "reset_clears_channels",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "chan-a", "Source": "src",
				})
				h.Reset()
				// after Reset, creating a channel with same name succeeds (uniqueness cleared)
				rec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "chan-a", "Source": "src",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "reset_clears_event_data_stores",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "eds-a",
				})
				h.Reset()
				// After reset, can create again with same name
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "eds-a",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestRefinement1_SortedOutput verifies deterministic sorted output.
func TestRefinement1_SortedOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "list_trails_sorted_by_arn",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				for _, n := range []string{"trail-z", "trail-a", "trail-m"} {
					doCloudTrailOp(t, h, "CreateTrail", map[string]any{
						"Name": n, "S3BucketName": "bucket",
					})
				}
				rec := doCloudTrailOp(t, h, "ListTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["Trails"].([]any)
				require.True(t, ok)
				require.Len(t, list, 3)
				// Verify sorted order by comparing ARNs (which contain the name)
				firstName := list[0].(map[string]any)["Name"].(string)
				secondName := list[1].(map[string]any)["Name"].(string)
				thirdName := list[2].(map[string]any)["Name"].(string)
				assert.Less(t, firstName, secondName)
				assert.Less(t, secondName, thirdName)
			},
		},
		{
			name: "describe_trails_sorted_by_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				for _, n := range []string{"trail-z", "trail-a", "trail-m"} {
					doCloudTrailOp(t, h, "CreateTrail", map[string]any{
						"Name": n, "S3BucketName": "bucket",
					})
				}
				rec := doCloudTrailOp(t, h, "DescribeTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["trailList"].([]any)
				require.True(t, ok)
				require.Len(t, list, 3)
				names := []string{
					list[0].(map[string]any)["Name"].(string),
					list[1].(map[string]any)["Name"].(string),
					list[2].(map[string]any)["Name"].(string),
				}
				assert.Equal(t, []string{"trail-a", "trail-m", "trail-z"}, names)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestRefinement1_NameUniqueness verifies duplicate-name detection for new resource types.
func TestRefinement1_NameUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "duplicate_channel_name_rejected",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec1 := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "dup-chan", "Source": "src",
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "dup-chan", "Source": "src",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
			},
		},
		{
			name: "duplicate_dashboard_name_rejected",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec1 := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "dup-dash",
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "dup-dash",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
			},
		},
		{
			name: "duplicate_event_data_store_name_rejected",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec1 := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "dup-eds",
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "dup-eds",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestRefinement1_TagsOnAllResources exercises AddTags/RemoveTags/ListTags on channels, dashboards, event data stores.
func TestRefinement1_TagsOnAllResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "tags_on_channel",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "tagged-chan", "Source": "src",
				})
				createResp := parseCloudTrailResp(t, createRec)
				chanARN := createResp["ChannelArn"].(string)

				addRec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": chanARN,
					"TagsList":   []map[string]string{{"Key": "Env", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{chanARN},
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseCloudTrailResp(t, listRec)
				tagList := listResp["ResourceTagList"].([]any)
				require.Len(t, tagList, 1)
				item := tagList[0].(map[string]any)
				assert.Equal(t, chanARN, item["ResourceId"])
				assert.NotEmpty(t, item["TagsList"])
			},
		},
		{
			name: "tags_on_dashboard",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "tagged-dash",
				})
				createResp := parseCloudTrailResp(t, createRec)
				dashARN := createResp["DashboardArn"].(string)

				addRec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": dashARN,
					"TagsList":   []map[string]string{{"Key": "Team", "Value": "platform"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				removeRec := doCloudTrailOp(t, h, "RemoveTags", map[string]any{
					"ResourceId": dashARN,
					"TagsList":   []map[string]string{{"Key": "Team"}},
				})
				assert.Equal(t, http.StatusOK, removeRec.Code)
			},
		},
		{
			name: "tags_on_event_data_store",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "tagged-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				addRec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": edsARN,
					"TagsList":   []map[string]string{{"Key": "Cost", "Value": "team-a"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{edsARN},
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseCloudTrailResp(t, listRec)
				tagList := listResp["ResourceTagList"].([]any)
				require.Len(t, tagList, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore persists all resource types.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create one of each resource type
	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name": "persist-trail", "S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "CreateChannel", map[string]any{
		"Name": "persist-chan", "Source": "src",
	})
	doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
		"Name": "persist-dash",
	})
	doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "persist-eds",
	})
	q, err := h.Backend.StartQuery("SELECT eventName FROM events LIMIT 1", "", "")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify trail restored
	rec := doCloudTrailOp(t, h2, "GetTrail", map[string]any{"Name": "persist-trail"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify channel name uniqueness restored (creating dup should fail)
	dupChanRec := doCloudTrailOp(t, h2, "CreateChannel", map[string]any{
		"Name": "persist-chan", "Source": "src",
	})
	assert.Equal(t, http.StatusConflict, dupChanRec.Code)

	// Verify dashboard name uniqueness restored
	dupDashRec := doCloudTrailOp(t, h2, "CreateDashboard", map[string]any{
		"Name": "persist-dash",
	})
	assert.Equal(t, http.StatusConflict, dupDashRec.Code)

	// Verify EDS name uniqueness restored
	dupEDSRec := doCloudTrailOp(t, h2, "CreateEventDataStore", map[string]any{
		"Name": "persist-eds",
	})
	assert.Equal(t, http.StatusConflict, dupEDSRec.Code)

	// Verify query restored
	descRec := doCloudTrailOp(t, h2, "DescribeQuery", map[string]any{
		"QueryId": q.QueryID,
	})
	assert.Equal(t, http.StatusOK, descRec.Code)
}

// TestRefinement1_Validation exercises required-field validation for newly validated ops.
func TestRefinement1_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "start_logging_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "StartLogging", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "stop_logging_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "StopLogging", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "put_event_selectors_missing_trail_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
					"EventSelectors": []any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "cancel_query_missing_query_id",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_query_missing_query_id",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "event_data_store_status_is_enabled",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "status-check-eds",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "ENABLED", resp["Status"])
			},
		},
		{
			name: "dashboard_status_is_created",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "status-check-dash",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "CREATED", resp["Status"])
			},
		},
		{
			name: "channel_not_found_gives_404",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteChannel", map[string]any{
					"Channel": "nonexistent-channel",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "event_data_store_not_found_gives_404",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": "nonexistent-eds",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestRefinement1_ProviderInitNilCtx ensures Provider.Init is nil-safe.
func TestRefinement1_ProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &cloudtrail.Provider{}
	reg, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "CloudTrail", reg.Name())
}

// TestCloudTrailUpdateChannelRename verifies UpdateChannel applies the Name
// parameter (renaming the channel), not just Destinations.
func TestCloudTrailUpdateChannelRename(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	createRec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
		"Name":   "orig-channel",
		"Source": "Custom",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	channelARN, _ := parseCloudTrailResp(t, createRec)["ChannelArn"].(string)
	require.NotEmpty(t, channelARN)

	rec := doCloudTrailOp(t, h, "UpdateChannel", map[string]any{
		"Channel": channelARN,
		"Name":    "renamed-channel",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	assert.Equal(t, "renamed-channel", resp["Name"])

	getRec := doCloudTrailOp(t, h, "GetChannel", map[string]any{"Channel": channelARN})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseCloudTrailResp(t, getRec)
	assert.Equal(t, "renamed-channel", getResp["Name"], "rename must persist and be visible via GetChannel")
}

// TestCloudTrailGenerateQuery exercises GenerateQuery: required-parameter
// validation and the AWS response shape (QueryStatement/QueryAlias/
// EventDataStoreOwnerAccountId -- not QueryId/QueryString).
func TestCloudTrailGenerateQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "gq-eds"})
				require.Equal(t, http.StatusOK, edsRec.Code)
				edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h, "GenerateQuery", map[string]any{
					"EventDataStores": []string{edsARN},
					"Prompt":          "show me all console logins",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)

				stmt, ok := resp["QueryStatement"].(string)
				require.True(t, ok, "QueryStatement must be present and a string")
				assert.NotEmpty(t, stmt)
				assert.NotEmpty(t, resp["QueryAlias"])
				assert.Equal(t, "123456789012", resp["EventDataStoreOwnerAccountId"])

				_, hasQueryID := resp["QueryId"]
				assert.False(t, hasQueryID, "GenerateQueryOutput has no QueryId field")
				_, hasQueryString := resp["QueryString"]
				assert.False(t, hasQueryString, "GenerateQueryOutput has no QueryString field")
			},
		},
		{
			name: "missing_event_data_stores",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GenerateQuery", map[string]any{
					"Prompt": "show me all console logins",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_prompt",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GenerateQuery", map[string]any{
					"EventDataStores": []string{"eds-000001"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailEventConfiguration exercises GetEventConfiguration and
// PutEventConfiguration for both trails and event data stores, verifying the
// AWS wire shape (TrailARN/EventDataStoreArn, AggregationConfigurations,
// ContextKeySelectors, MaxEventSize) and that settings persist across calls.
func TestCloudTrailEventConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "trail_round_trip",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "evtcfg-trail",
					"S3BucketName": "bucket",
				})

				putRec := doCloudTrailOp(t, h, "PutEventConfiguration", map[string]any{
					"TrailName":    "evtcfg-trail",
					"MaxEventSize": "Large",
					"ContextKeySelectors": []map[string]any{
						{"Type": "RequestContext", "Equals": []string{"authparams"}},
					},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
				putResp := parseCloudTrailResp(t, putRec)
				assert.NotEmpty(t, putResp["TrailARN"])
				assert.Equal(t, "Large", putResp["MaxEventSize"])
				_, hasEDSArn := putResp["EventDataStoreArn"]
				assert.False(t, hasEDSArn, "trail-scoped response must not include EventDataStoreArn")

				getRec := doCloudTrailOp(t, h, "GetEventConfiguration", map[string]any{
					"TrailName": "evtcfg-trail",
				})
				require.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.Equal(t, "Large", getResp["MaxEventSize"])
				selectors, ok := getResp["ContextKeySelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, selectors, 1)
			},
		},
		{
			name: "event_data_store_round_trip",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "evtcfg-eds"})
				require.Equal(t, http.StatusOK, edsRec.Code)
				edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

				putRec := doCloudTrailOp(t, h, "PutEventConfiguration", map[string]any{
					"EventDataStore": edsARN,
					"MaxEventSize":   "Standard",
				})
				require.Equal(t, http.StatusOK, putRec.Code)
				putResp := parseCloudTrailResp(t, putRec)
				assert.Equal(t, edsARN, putResp["EventDataStoreArn"])
				_, hasTrailARN := putResp["TrailARN"]
				assert.False(t, hasTrailARN, "EDS-scoped response must not include TrailARN")

				getRec := doCloudTrailOp(t, h, "GetEventConfiguration", map[string]any{
					"EventDataStore": edsARN,
				})
				require.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.Equal(t, "Standard", getResp["MaxEventSize"])
			},
		},
		{
			name: "get_defaults_when_unset",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "evtcfg-unset-trail",
					"S3BucketName": "bucket",
				})

				rec := doCloudTrailOp(t, h, "GetEventConfiguration", map[string]any{
					"TrailName": "evtcfg-unset-trail",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["TrailARN"])
				_, hasMaxSize := resp["MaxEventSize"]
				assert.False(t, hasMaxSize, "MaxEventSize omitted when never configured")
			},
		},
		{
			name: "missing_both_resource_params",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GetEventConfiguration", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "trail_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GetEventConfiguration", map[string]any{
					"TrailName": "does-not-exist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailInsightSelectorsEventDataStore verifies PutInsightSelectors
// and GetInsightSelectors work against an EventDataStore (not just
// TrailName), matching AWS's PutInsightSelectorsInput/GetInsightSelectorsInput
// which accept either parameter.
func TestCloudTrailInsightSelectorsEventDataStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "put_and_get_round_trip",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "insights-eds"})
				require.Equal(t, http.StatusOK, edsRec.Code)
				edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

				putRec := doCloudTrailOp(t, h, "PutInsightSelectors", map[string]any{
					"EventDataStore": edsARN,
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
					},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
				putResp := parseCloudTrailResp(t, putRec)
				assert.Equal(t, edsARN, putResp["EventDataStoreArn"])

				getRec := doCloudTrailOp(t, h, "GetInsightSelectors", map[string]any{
					"EventDataStore": edsARN,
				})
				require.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.Equal(t, edsARN, getResp["EventDataStoreArn"])
				sels, ok := getResp["InsightSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, sels, 1)
			},
		},
		{
			name: "get_not_enabled",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "insights-eds-none"})
				require.Equal(t, http.StatusOK, edsRec.Code)
				edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h, "GetInsightSelectors", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "put_missing_both_params",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "PutInsightSelectors", map[string]any{
					"InsightSelectors": []map[string]any{{"InsightType": "ApiCallRateInsight"}},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_missing_both_params",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GetInsightSelectors", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}
