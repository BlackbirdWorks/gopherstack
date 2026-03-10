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

	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(snap))

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
				assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", resp["KMSKeyId"])
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
