package resourcegroupstaggingapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

// ptr returns a pointer to a copy of v, for building test input literals.
func ptr[T any](v T) *T {
	p := new(T)
	*p = v

	return p
}

// newBackend creates an InMemoryBackend for the default test account/region.
func newBackend(t *testing.T) *resourcegroupstaggingapi.InMemoryBackend {
	t.Helper()

	return resourcegroupstaggingapi.NewInMemoryBackend(testAccountID, testRegion)
}

// seedResources registers a provider that returns the given resources.
func seedResources(
	b *resourcegroupstaggingapi.InMemoryBackend,
	resources []resourcegroupstaggingapi.TaggedResource,
) {
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return resources
	})
}

// makeKeys returns n distinct tag-key strings, used to exercise TagKeys count limits.
func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	return keys
}

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
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource { return resources })

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

	return doTaggingRequestRaw(t, h, action, bodyBytes)
}

func doTaggingRequestRaw(
	t *testing.T,
	h *resourcegroupstaggingapi.Handler,
	action string,
	bodyBytes []byte,
) *httptest.ResponseRecorder {
	t.Helper()

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
	assert.Contains(t, h.GetSupportedOperations(), "DescribeReportCreation")
	assert.Contains(t, h.GetSupportedOperations(), "GetComplianceSummary")
	assert.Contains(t, h.GetSupportedOperations(), "GetResources")
	assert.Contains(t, h.GetSupportedOperations(), "GetTagKeys")
	assert.Contains(t, h.GetSupportedOperations(), "GetTagValues")
	assert.Contains(t, h.GetSupportedOperations(), "ListRequiredTags")
	assert.Contains(t, h.GetSupportedOperations(), "StartReportCreation")
	assert.Contains(t, h.GetSupportedOperations(), "TagResources")
	assert.Contains(t, h.GetSupportedOperations(), "UntagResources")
}

func TestHandler_StartReportCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:     "valid_bucket",
			body:     map[string]any{"S3Bucket": "my-report-bucket"},
			wantCode: http.StatusOK,
		},
		{
			name:         "missing_bucket",
			body:         map[string]any{},
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterException",
		},
		{
			name:         "empty_bucket",
			body:         map[string]any{"S3Bucket": ""},
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTaggingRequest(t, h, "StartReportCreation", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestHandler_DescribeReportCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*resourcegroupstaggingapi.Handler)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:     "no_report_created",
			wantCode: http.StatusOK,
		},
		{
			name: "after_start_report_creation",
			setupFn: func(h *resourcegroupstaggingapi.Handler) {
				doTaggingRequest(t, h, "StartReportCreation", map[string]any{"S3Bucket": "my-bucket"})
				// Advance the backend clock so DescribeReportCreation transitions RUNNING→SUCCEEDED.
				if b, ok := h.Backend.(*resourcegroupstaggingapi.InMemoryBackend); ok {
					done := time.Now().Add(resourcegroupstaggingapi.ReportRunningDuration() + time.Second)
					resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return done })
				}
			},
			wantCode:     http.StatusOK,
			wantContains: "SUCCEEDED",
		},
		{
			name: "s3_location_present_after_start",
			setupFn: func(h *resourcegroupstaggingapi.Handler) {
				doTaggingRequest(t, h, "StartReportCreation", map[string]any{"S3Bucket": "test-bucket"})
			},
			wantCode:     http.StatusOK,
			wantContains: "test-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setupFn != nil {
				tt.setupFn(h)
			}

			rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestHandler_GetComplianceSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_request",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantContains: "SummaryList",
		},
		{
			name: "with_filters",
			body: map[string]any{
				"RegionFilters":       []string{"us-east-1"},
				"ResourceTypeFilters": []string{"ec2:instance"},
				"TagKeyFilters":       []string{"env"},
			},
			wantCode:     http.StatusOK,
			wantContains: "SummaryList",
		},
		{
			name: "with_pagination",
			body: map[string]any{
				"MaxResults": 10,
			},
			wantCode:     http.StatusOK,
			wantContains: "SummaryList",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTaggingRequest(t, h, "GetComplianceSummary", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestHandler_ListRequiredTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_request",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantContains: "RequiredTags",
		},
		{
			name: "with_max_results",
			body: map[string]any{
				"MaxResults": 50,
			},
			wantCode:     http.StatusOK,
			wantContains: "RequiredTags",
		},
		{
			name: "with_next_token",
			body: map[string]any{
				"NextToken": "some-token",
			},
			wantCode:     http.StatusOK,
			wantContains: "RequiredTags",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTaggingRequest(t, h, "ListRequiredTags", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// ------------------------------------------------------------------ wire error codes ---

// TestValidationFailures_UseInvalidParameterException verifies that resourcegroupstaggingapi
// -- which has no ValidationException shape in its error model -- reports every
// parameter-validation failure as InvalidParameterException instead.
func TestValidationFailures_UseInvalidParameterException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "GetResources_duplicate_tag_filter_keys",
			op:   "GetResources",
			body: map[string]any{
				"TagFilters": []map[string]any{{"Key": "env"}, {"Key": "env"}},
			},
		},
		{
			name: "TagResources_empty_arn_list",
			op:   "TagResources",
			body: map[string]any{
				"ResourceARNList": []string{},
				"Tags":            map[string]string{"k": "v"},
			},
		},
		{
			name: "UntagResources_empty_tag_keys",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         []string{},
			},
		},
		{
			name: "StartReportCreation_missing_bucket",
			op:   "StartReportCreation",
			body: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			rec := doTaggingRequest(t, h, tt.op, tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())

			var body map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
			assert.Equal(t, "InvalidParameterException", body["__type"],
				"real resourcegroupstaggingapi has no ValidationException shape")
			assert.NotContains(t, rec.Body.String(), "ValidationException")
		})
	}
}

// TestMalformedBody_UsesSerializationException verifies that a body the AWS JSON 1.1
// decoder cannot parse at all -- a protocol-level failure, not a modeled operation error
// -- is reported as SerializationException regardless of the specific malformation.
func TestMalformedBody_UsesSerializationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body []byte
	}{
		{name: "not_json", body: []byte("not-json")},
		{name: "unterminated_object", body: []byte("{not-json")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			rec := doTaggingRequestRaw(t, h, "GetResources", tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "SerializationException")

			var body map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
			assert.Equal(t, "SerializationException", body["__type"])
		})
	}
}

// TestHandler_ValidationErrors_Return400 covers handler-level 400 responses across every
// operation's parameter validation.
func TestHandler_ValidationErrors_Return400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body any
		name string
		op   string
		want int
	}{
		{
			name: "GetResources_exclude_compliant_without_include_details",
			op:   "GetResources",
			body: map[string]any{
				"ExcludeCompliantResources": true,
				"IncludeComplianceDetails":  false,
			},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_duplicate_tag_filter_keys",
			op:   "GetResources",
			body: map[string]any{
				"TagFilters": []map[string]any{
					{"Key": "env"},
					{"Key": "env"},
				},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_invalid_resource_type_filter",
			op:   "GetResources",
			body: map[string]any{"ResourceTypeFilters": []string{"SQS:Queue"}},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_tags_per_page_too_small",
			op:   "GetResources",
			body: map[string]any{"TagsPerPage": 50},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_tags_per_page_too_large",
			op:   "GetResources",
			body: map[string]any{"TagsPerPage": 501},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_too_many_tag_filters",
			op:   "GetResources",
			body: map[string]any{"TagFilters": func() []map[string]any {
				filters := make([]map[string]any, 51)
				for i := range filters {
					filters[i] = map[string]any{"Key": fmt.Sprintf("k%d", i)}
				}

				return filters
			}()},
			want: http.StatusBadRequest,
		},
		{
			name: "TagResources_empty_arn",
			op:   "TagResources",
			body: map[string]any{
				"ResourceARNList": []string{""},
				"Tags":            map[string]string{"k": "v"},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "TagResources_invalid_arn",
			op:   "TagResources",
			body: map[string]any{
				"ResourceARNList": []string{"not-an-arn"},
				"Tags":            map[string]string{"k": "v"},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "UntagResources_too_many_keys",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         makeKeys(51),
			},
			want: http.StatusBadRequest,
		},
		{
			name: "UntagResources_empty_key",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         []string{""},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "UntagResources_empty_tag_keys_list",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         []string{},
			},
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTaggingRequest(t, h, tt.op, tt.body)

			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

// TestHandler_ValidationError_HasInvalidParameterExceptionBody verifies the JSON error
// body's __type field, not just the HTTP status code.
func TestHandler_ValidationError_HasInvalidParameterExceptionBody(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "TagResources", map[string]any{
		"ResourceARNList": []string{},
		"Tags":            map[string]string{"k": "v"},
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	// Real resourcegroupstaggingapi has no ValidationException shape; parameter
	// validation failures are modeled as InvalidParameterException.
	assert.Equal(t, "InvalidParameterException", body["__type"])
}

// ------------------------------------------------------------------ GetTagValues (handler-level) ---

func TestHandler_GetTagValues_NilKey_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "GetTagValues", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterException")
	assert.Contains(t, rec.Body.String(), "Key is required")
}

func TestHandler_GetTagValues_EmptyKey_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "GetTagValues", map[string]any{"Key": ""})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterException")
}

// ------------------------------------------------------------------ GetComplianceSummary (handler-level) ---

func TestHandler_GetComplianceSummary_InvalidGroupBy_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groupBy []string
	}{
		{name: "unknown_value", groupBy: []string{"INVALID"}},
		{name: "lowercase_region", groupBy: []string{"region"}},
		{name: "mixed_valid_invalid", groupBy: []string{"REGION", "INVALID_VALUE"}},
		{name: "empty_string", groupBy: []string{""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			rec := doTaggingRequest(t, h, "GetComplianceSummary", map[string]any{"GroupBy": tt.groupBy})

			assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			assert.Contains(t, rec.Body.String(), "InvalidParameterException")
		})
	}
}

func TestHandler_GetComplianceSummary_ValidGroupBy_Returns200(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groupBy []string
	}{
		{name: "TARGET_ID", groupBy: []string{"TARGET_ID"}},
		{name: "REGION", groupBy: []string{"REGION"}},
		{name: "RESOURCE_TYPE", groupBy: []string{"RESOURCE_TYPE"}},
		{name: "multi", groupBy: []string{"REGION", "RESOURCE_TYPE"}},
		{name: "empty", groupBy: []string{}},
		{name: "nil", groupBy: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			body := map[string]any{}
			if tt.groupBy != nil {
				body["GroupBy"] = tt.groupBy
			}

			rec := doTaggingRequest(t, h, "GetComplianceSummary", body)

			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			assert.Contains(t, rec.Body.String(), "SummaryList")
		})
	}
}

// ------------------------------------------------------------------ StartReportCreation (handler-level) ---

func TestHandler_StartReportCreation_ConcurrentModification_Returns409(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	h := resourcegroupstaggingapi.NewHandler(b)

	// Start first report successfully.
	rec := doTaggingRequest(t, h, "StartReportCreation", map[string]any{"S3Bucket": "first-bucket"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Concurrent attempt returns 409.
	rec = doTaggingRequest(t, h, "StartReportCreation", map[string]any{"S3Bucket": "second-bucket"})
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConcurrentModificationException")
}

// ------------------------------------------------------------------ Handler lifecycle/construction ---

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource { return nil })
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bucket/path", "2025-01-01T00:00:00Z")

	h := resourcegroupstaggingapi.NewHandler(b)
	require.True(t, resourcegroupstaggingapi.HasReportState(b))
	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))

	h.Reset()

	// Only reportState is cleared; wired providers survive.
	assert.False(t, resourcegroupstaggingapi.HasReportState(b))
	assert.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b), "providers must survive Handler.Reset()")
}

func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))

	assert.Equal(t, 9, resourcegroupstaggingapi.HandlerOpsLen(h))
}

func TestHandlerAcceptsStorageBackend(t *testing.T) {
	t.Parallel()

	var backend resourcegroupstaggingapi.StorageBackend = newBackend(t)
	h := resourcegroupstaggingapi.NewHandler(backend)

	assert.NotNil(t, h)
}
