package timestreamquery_test

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
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func newTestBackendAndHandler() (*timestreamquery.InMemoryBackend, *timestreamquery.Handler) {
	backend := timestreamquery.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return backend, timestreamquery.NewHandler(backend)
}

func newTestHandler() *timestreamquery.Handler {
	_, h := newTestBackendAndHandler()

	return h
}

func doRequest(
	t *testing.T,
	h *timestreamquery.Handler,
	op string,
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
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "Timestream_20181101."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func doRequestRaw(
	t *testing.T,
	h *timestreamquery.Handler,
	op string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "Timestream_20181101."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func TestTimestreamQueryHandler_DescribeEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "returns local endpoint",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "DescribeEndpoints", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
			resp := parseResponse(t, rec)
			endpoints, ok := resp["Endpoints"].([]any)
			require.True(t, ok, "Endpoints should be a list")
			assert.NotEmpty(t, endpoints)
		})
	}
}

func TestTimestreamQueryHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "Query")
	assert.Contains(t, ops, "CreateScheduledQuery")
	assert.Contains(t, ops, "DescribeEndpoints")
}

func TestTimestreamQueryHandler_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	h := timestreamquery.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 15)
	assert.Equal(t, 15, timestreamquery.HandlerOpsLen(h))
}

func TestTimestreamQueryHandler_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	h := timestreamquery.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

func TestTimestreamQueryHandler_Metadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "TimestreamQuery", h.Name())
	assert.Equal(t, "timestream", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestTimestreamQueryHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "matches timestream prefix", target: "Timestream_20181101.Query", want: true},
		{name: "matches create", target: "Timestream_20181101.CreateScheduledQuery", want: true},
		{name: "does not match athena", target: "AmazonAthena.Query", want: false},
		{name: "does not match empty", target: "", want: false},
		// Tag ops are deferred to the TimestreamWrite handler so that a single
		// unified tag store handles all Timestream resource types.
		{name: "TagResource deferred to write service", target: "Timestream_20181101.TagResource", want: false},
		{name: "UntagResource deferred to write service", target: "Timestream_20181101.UntagResource", want: false},
		{
			name:   "ListTagsForResource deferred to write service",
			target: "Timestream_20181101.ListTagsForResource",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestTimestreamQueryHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "extracts ScheduledQueryArn with highest priority",
			body: map[string]any{
				"ScheduledQueryArn": "arn:aws:timestream:us-east-1:123:scheduled-query/sq",
				"Arn":               "arn:aws:timestream:us-east-1:123:scheduled-query/other",
			},
			want: "arn:aws:timestream:us-east-1:123:scheduled-query/sq",
		},
		{
			name: "extracts ResourceARN as second priority",
			body: map[string]any{"ResourceARN": "arn:aws:timestream:us-east-1:123:scheduled-query/resource"},
			want: "arn:aws:timestream:us-east-1:123:scheduled-query/resource",
		},
		{
			name: "extracts arn",
			body: map[string]any{"Arn": "arn:aws:timestream:us-east-1:123:scheduled-query/test"},
			want: "arn:aws:timestream:us-east-1:123:scheduled-query/test",
		},
		{
			name: "extracts name when no arn",
			body: map[string]any{"Name": "my-query"},
			want: "my-query",
		},
		{
			name: "empty body",
			body: map[string]any{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTimestreamQueryBackend_Accessors(t *testing.T) {
	t.Parallel()

	backend := timestreamquery.NewInMemoryBackend("111222333444", "eu-west-1")
	assert.Equal(t, "111222333444", backend.AccountID())
	assert.Equal(t, "eu-west-1", backend.Region())
}

func TestTimestreamQueryBackend_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ timestreamquery.StorageBackend = (*timestreamquery.InMemoryBackend)(nil)
}

func TestTimestreamQueryBackend_Reset(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	// Create a scheduled query to have some state.
	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "reset-test",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	// Reset clears all state.
	backend.Reset()
	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(backend))
}

func TestTimestreamQueryHandler_Reset(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "handler-reset-test",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	h.Reset()
	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(backend))
}

func TestTimestreamQueryHandler_ContentTypeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "delete returns empty body with content type",
			op:   "DeleteScheduledQuery",
		},
		{
			name: "query returns content type",
			op:   "Query",
			body: map[string]any{"QueryString": "SELECT 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, h := newTestBackendAndHandler()

			if tt.op == "DeleteScheduledQuery" {
				// Create first.
				rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
					"Name":                           "ct-test",
					"QueryString":                    "SELECT 1",
					"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
					"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
					"NotificationConfiguration": map[string]any{
						"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
					},
					"ErrorReportConfiguration": map[string]any{
						"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				arn := parseResponse(t, rec)["Arn"].(string)
				tt.body = map[string]any{"ScheduledQueryArn": arn}
			}

			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
		})
	}
}

func TestTimestreamQueryHandler_BadJSON(t *testing.T) {
	t.Parallel()

	ops := []string{
		"Query",
		"CancelQuery",
		"CreateScheduledQuery",
		"DeleteScheduledQuery",
		"DescribeScheduledQuery",
		"ExecuteScheduledQuery",
		"UpdateScheduledQuery",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"PrepareQuery",
		"UpdateAccountSettings",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequestRaw(t, h, op, []byte(`not-valid-json{`))
			// Bad JSON should return 5xx (parse error) or 4xx (validation)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)
		})
	}
}

func TestTimestreamQueryHandler_HandleErrorBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantType string
		wantCode int
	}{
		{
			name:     "not found returns ResourceNotFoundException",
			op:       "DescribeScheduledQuery",
			body:     map[string]any{"ScheduledQueryArn": "arn:aws:timestream:us-east-1:123:scheduled-query/nope"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceNotFoundException",
		},
		{
			name: "already exists returns ConflictException",
			op:   "CreateScheduledQuery",
			body: map[string]any{
				"Name":                           "dup",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
				},
			},
			wantCode: http.StatusConflict,
			wantType: "ConflictException",
		},
		{
			name:     "validation returns ValidationException",
			op:       "UpdateScheduledQuery",
			body:     map[string]any{"ScheduledQueryArn": "arn:...", "State": "INVALID_STATE"},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "unknown operation returns ValidationException",
			op:       "NonExistentOp",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// For duplicate test, create first.
			if tt.op == "CreateScheduledQuery" && tt.wantType == "ConflictException" {
				first := doRequest(t, h, "CreateScheduledQuery", tt.body)
				require.Equal(t, http.StatusOK, first.Code)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}
