package resourcegroupstaggingapi_test

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
	"github.com/blackbirdworks/gopherstack/resourcegroupstaggingapi"
)

func newTestHandler(t *testing.T) *resourcegroupstaggingapi.Handler {
	t.Helper()

	b := resourcegroupstaggingapi.NewInMemoryBackend("000000000000", "us-east-1")

	return resourcegroupstaggingapi.NewHandler(b)
}

func newTestHandlerWithResources(
	t *testing.T,
	resources []resourcegroupstaggingapi.TaggedResource,
) *resourcegroupstaggingapi.Handler {
	t.Helper()

	b := resourcegroupstaggingapi.NewInMemoryBackend("000000000000", "us-east-1")
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource { return resources })

	return resourcegroupstaggingapi.NewHandler(b)
}

func doTaggingRequest(
	t *testing.T,
	h *resourcegroupstaggingapi.Handler,
	action string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_GetResources(t *testing.T) {
	t.Parallel()

	resources := []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:dynamodb:us-east-1:000000000000:table/t1",
			ResourceType: "dynamodb:table",
			Tags:         map[string]string{"env": "dev"},
		},
	}

	tests := []struct {
		body         any
		name         string
		wantContains string
		wantAbsent   string
		wantCode     int
	}{
		{
			name:         "no_filter_returns_all",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantContains: "ResourceTagMappingList",
		},
		{
			name:         "filter_by_type",
			body:         map[string]any{"ResourceTypeFilters": []string{"sqs:queue"}},
			wantCode:     http.StatusOK,
			wantContains: "q1",
			wantAbsent:   "table/t1",
		},
		{
			name: "filter_by_tag",
			body: map[string]any{
				"TagFilters": []map[string]any{{"Key": "env", "Values": []string{"prod"}}},
			},
			wantCode:     http.StatusOK,
			wantContains: "q1",
			wantAbsent:   "table/t1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerWithResources(t, resources)
			rec := doTaggingRequest(t, h, "GetResources", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, rec.Body.String(), tt.wantAbsent)
			}
		})
	}
}

func TestHandler_GetTagKeys(t *testing.T) {
	t.Parallel()

	resources := []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod", "team": "ops"}},
	}
	h := newTestHandlerWithResources(t, resources)

	rec := doTaggingRequest(t, h, "GetTagKeys", map[string]any{})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "env")
	assert.Contains(t, rec.Body.String(), "team")
}

func TestHandler_GetTagValues(t *testing.T) {
	t.Parallel()

	resources := []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev"}},
	}
	h := newTestHandlerWithResources(t, resources)

	rec := doTaggingRequest(t, h, "GetTagValues", map[string]any{"Key": "env"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "prod")
	assert.Contains(t, rec.Body.String(), "dev")
}

func TestHandler_TagResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "tag_unhandled_arn",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"Tags":            map[string]string{"env": "test"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTaggingRequest(t, h, "TagResources", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), "FailedResourcesMap")
		})
	}
}

func TestHandler_UntagResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTaggingRequest(t, h, "UntagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		"TagKeys":         []string{"env"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTaggingRequest(t, h, "BogusOperation", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches_tagging_target",
			target: "ResourceGroupsTaggingAPI_20170126.GetResources",
			want:   true,
		},
		{
			name:   "does_not_match_other_target",
			target: "DynamoDB_20120810.GetItem",
			want:   false,
		},
		{
			name:   "empty_target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_ServiceInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "ResourceGroupsTaggingAPI", h.Name())
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
	assert.Contains(t, h.GetSupportedOperations(), "GetResources")
	assert.Contains(t, h.GetSupportedOperations(), "GetTagKeys")
	assert.Contains(t, h.GetSupportedOperations(), "GetTagValues")
	assert.Contains(t, h.GetSupportedOperations(), "TagResources")
	assert.Contains(t, h.GetSupportedOperations(), "UntagResources")
}
