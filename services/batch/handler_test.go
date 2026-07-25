package batch_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/batch"
)

func newTestHandler(t *testing.T) *batch.Handler {
	t.Helper()

	return batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *batch.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func post(t *testing.T, h *batch.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	return doRequest(t, h, http.MethodPost, path, body)
}

func mustUnmarshal(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

// --- Handler metadata tests ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Batch", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "batch", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateComputeEnvironment",
		"DescribeComputeEnvironments",
		"UpdateComputeEnvironment",
		"DeleteComputeEnvironment",
		"CreateJobQueue",
		"DescribeJobQueues",
		"UpdateJobQueue",
		"DeleteJobQueue",
		"RegisterJobDefinition",
		"DescribeJobDefinitions",
		"DeregisterJobDefinition",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"UpdateServiceJob",
		"CreateQuotaShare",
		"DescribeQuotaShare",
		"UpdateQuotaShare",
		"DeleteQuotaShare",
		"ListQuotaShares",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "batch_path", path: "/v1/createcomputeenvironment", wantMatch: true},
		{name: "tags_path", path: "/v1/tags/some-arn", wantMatch: true},
		{
			name:      "tags_batch_arn",
			path:      "/v1/tags/arn%3Aaws%3Abatch%3Aus-east-1%3A123%3Acompute-environment%2Ftest",
			wantMatch: true,
		},
		{
			name:      "tags_kafka_arn_excluded",
			path:      "/v1/tags/arn%3Aaws%3Akafka%3Aus-east-1%3A123%3Acluster%2Ftest%2Fuuid",
			wantMatch: false,
		},
		{name: "kafka_cluster_excluded", path: "/v1/clusters", wantMatch: false},
		{name: "kafka_config_excluded", path: "/v1/configurations", wantMatch: false},
		{name: "appsync_path_excluded", path: "/v1/apis", wantMatch: false},
		{name: "appsync_path_excluded_with_id", path: "/v1/apis/abc123/datasources", wantMatch: false},
		{name: "other_prefix", path: "/v2/apis", wantMatch: false},
		{name: "root", path: "/", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.RouteMatcher()(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_ce",
			path:   "/v1/createcomputeenvironment",
			method: http.MethodPost,
			wantOp: "CreateComputeEnvironment",
		},
		{
			name:   "describe_ce",
			path:   "/v1/describecomputeenvironments",
			method: http.MethodPost,
			wantOp: "DescribeComputeEnvironments",
		},
		{name: "tags_get", path: "/v1/tags/some-arn", method: http.MethodGet, wantOp: "ListTagsForResource"},
		{name: "tags_post", path: "/v1/tags/some-arn", method: http.MethodPost, wantOp: "TagResource"},
		{name: "tags_delete", path: "/v1/tags/some-arn", method: http.MethodDelete, wantOp: "UntagResource"},
		{name: "unknown", path: "/v1/unknown", method: http.MethodPost, wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// --- Compute Environment tests ---

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/createcomputeenvironment", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/unknownoperation", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateComputeEnvironment")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.Equal(t, []string{"us-east-1"}, regions)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "tags_arn",
			path: "/v1/tags/arn:aws:batch:us-east-1:000000000000:compute-environment/my-ce",
			want: "arn:aws:batch:us-east-1:000000000000:compute-environment/my-ce",
		},
		{
			name: "non_tags_path",
			path: "/v1/createcomputeenvironment",
			want: "",
		},
		{
			name: "tags_empty",
			path: "/v1/tags/",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			h := newTestHandler(t)
			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBatch_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create some state.
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-reset",
		"type":                   "MANAGED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createconsumableresource", map[string]any{
		"consumableResourceName": "cr-reset",
		"totalQuantity":          int64(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Reset clears all state.
	h.Backend.Reset()

	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var ces map[string]any
	mustUnmarshal(t, rec, &ces)
	assert.Empty(t, ces["computeEnvironments"])

	rec2 := post(t, h, "/v1/listconsumableresources", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var list map[string]any
	mustUnmarshal(t, rec2, &list)
	assert.Empty(t, list["consumableResourceSummaryList"])
}

// --- Required field validation tests ---

func TestBatch_RequiredFieldValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
	}{
		{
			name: "CreateConsumableResource_missing_name",
			path: "/v1/createconsumableresource",
			body: map[string]any{},
		},
		{
			name: "DeleteConsumableResource_missing_resource",
			path: "/v1/deleteconsumableresource",
			body: map[string]any{},
		},
		{
			name: "DescribeConsumableResource_missing_resource",
			path: "/v1/describeconsumableresource",
			body: map[string]any{},
		},
		{
			name: "CreateSchedulingPolicy_missing_name",
			path: "/v1/createschedulingpolicy",
			body: map[string]any{},
		},
		{
			name: "DeleteSchedulingPolicy_missing_arn",
			path: "/v1/deleteschedulingpolicy",
			body: map[string]any{},
		},
		{
			name: "CreateServiceEnvironment_missing_name",
			path: "/v1/createserviceenvironment",
			body: map[string]any{"serviceEnvironmentType": "SAGEMAKER_TRAINING"},
		},
		{
			name: "DeleteServiceEnvironment_missing_resource",
			path: "/v1/deleteserviceenvironment",
			body: map[string]any{},
		},
		{
			name: "UpdateServiceJob_missing_jobId",
			path: "/v1/updateservicejob",
			body: map[string]any{"schedulingPriority": 5},
		},
		{
			name: "UpdateServiceJob_missing_schedulingPriority",
			path: "/v1/updateservicejob",
			body: map[string]any{"jobId": "some-id"},
		},
		{
			name: "CreateQuotaShare_missing_name",
			path: "/v1/createquotashare",
			body: map[string]any{"jobQueue": "some-queue"},
		},
		{
			name: "CreateQuotaShare_missing_jobQueue",
			path: "/v1/createquotashare",
			body: map[string]any{"quotaShareName": "qs-1"},
		},
		{
			name: "DescribeQuotaShare_missing_arn",
			path: "/v1/describequotashare",
			body: map[string]any{},
		},
		{
			name: "UpdateQuotaShare_missing_arn",
			path: "/v1/updatequotashare",
			body: map[string]any{},
		},
		{
			name: "DeleteQuotaShare_missing_arn",
			path: "/v1/deletequotashare",
			body: map[string]any{},
		},
		{
			name: "ListQuotaShares_missing_jobQueue",
			path: "/v1/listquotashares",
			body: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := post(t, h, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// --- UpdateConsumableResource tests ---

// assertTagsPresent checks that raw JSON has a "tags" key containing a non-null
// object (possibly empty). This mirrors the AWS guarantee that describe responses
// always include "tags": {} even when no tags are set.
func assertTagsPresent(t *testing.T, raw []byte) {
	t.Helper()

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &rawMap))

	tagBytes, hasTags := rawMap["tags"]
	assert.True(t, hasTags, "tags key must be present in describe response")

	if hasTags {
		var tags map[string]string
		require.NoError(t, json.Unmarshal(tagBytes, &tags), "tags must be a JSON object, not null")
		assert.NotNil(t, tags, "tags must be {} not null")
	}
}
