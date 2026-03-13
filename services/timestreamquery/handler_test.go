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

func newTestHandler() *timestreamquery.Handler {
	backend := timestreamquery.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return timestreamquery.NewHandler(backend)
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

func TestTimestreamQueryHandler_Query(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantCode    int
		wantQueryID bool
	}{
		{
			name:        "success",
			body:        map[string]any{"QueryString": "SELECT * FROM my_table"},
			wantCode:    http.StatusOK,
			wantQueryID: true,
		},
		{
			name:     "missing query string",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "Query", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantQueryID {
				resp := parseResponse(t, rec)
				assert.NotEmpty(t, resp["QueryId"])
			}
		})
	}
}

func TestTimestreamQueryHandler_CancelQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *timestreamquery.Handler) string
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "cancel existing query",
			setup: func(t *testing.T, h *timestreamquery.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1"})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)

				return resp["QueryId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing query id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "query not found",
			body:     map[string]any{"QueryId": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			body := tt.body
			if tt.setup != nil {
				qid := tt.setup(t, h)
				body = map[string]any{"QueryId": qid}
			}

			rec := doRequest(t, h, "CancelQuery", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTimestreamQueryHandler_ScheduledQueryLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sqName   string
		wantCode int
	}{
		{
			name:     "create_describe_delete",
			sqName:   "test-query-1",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			createBody := map[string]any{
				"Name":                           tt.sqName,
				"QueryString":                    "SELECT * FROM my_db.my_table",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123456789012:role/test-role",
				"ScheduleConfiguration": map[string]any{
					"ScheduleExpression": "rate(1 hour)",
				},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{
						"TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
					},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{
						"BucketName": "my-error-bucket",
					},
				},
			}

			// Create
			rec := doRequest(t, h, "CreateScheduledQuery", createBody)
			require.Equal(t, tt.wantCode, rec.Code)
			resp := parseResponse(t, rec)
			arn, ok := resp["Arn"].(string)
			require.True(t, ok, "Arn should be a string")
			require.NotEmpty(t, arn)

			// Describe
			rec = doRequest(t, h, "DescribeScheduledQuery", map[string]any{"ScheduledQueryArn": arn})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			sq, ok := resp["ScheduledQuery"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.sqName, sq["Name"])

			// List
			rec = doRequest(t, h, "ListScheduledQueries", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			queries, ok := resp["ScheduledQueries"].([]any)
			require.True(t, ok)
			assert.Len(t, queries, 1)

			// Update state
			rec = doRequest(t, h, "UpdateScheduledQuery", map[string]any{
				"ScheduledQueryArn": arn,
				"State":             "DISABLED",
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Execute
			rec = doRequest(t, h, "ExecuteScheduledQuery", map[string]any{
				"ScheduledQueryArn": arn,
				"InvocationTime":    "2024-01-01T00:00:00Z",
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete
			rec = doRequest(t, h, "DeleteScheduledQuery", map[string]any{"ScheduledQueryArn": arn})
			assert.Equal(t, http.StatusOK, rec.Code)

			// List after delete - should be empty
			rec = doRequest(t, h, "ListScheduledQueries", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			queries, ok = resp["ScheduledQueries"].([]any)
			require.True(t, ok)
			assert.Empty(t, queries)
		})
	}
}

func TestTimestreamQueryHandler_CreateScheduledQuery_Duplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "duplicate name returns conflict",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			createBody := map[string]any{
				"Name":                           "dup-query",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "bucket"},
				},
			}

			rec := doRequest(t, h, "CreateScheduledQuery", createBody)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "CreateScheduledQuery", createBody)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTimestreamQueryHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		sqName   string
		wantCode int
	}{
		{
			name:     "tag resource",
			sqName:   "tagged-query",
			tags:     map[string]string{"env": "test", "team": "data"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			// Create a scheduled query to get an ARN
			createBody := map[string]any{
				"Name":                           tt.sqName,
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "bucket"},
				},
			}

			rec := doRequest(t, h, "CreateScheduledQuery", createBody)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			arn := resp["Arn"].(string)

			// TagResource
			tagItems := make([]map[string]string, 0, len(tt.tags))
			for k, v := range tt.tags {
				tagItems = append(tagItems, map[string]string{"Key": k, "Value": v})
			}

			rec = doRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tagItems,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			// ListTagsForResource
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, len(tt.tags))

			// UntagResource
			keys := make([]string, 0, len(tt.tags))
			for k := range tt.tags {
				keys = append(keys, k)
			}

			rec = doRequest(t, h, "UntagResource", map[string]any{
				"ResourceARN": arn,
				"TagKeys":     keys,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// ListTagsForResource after untag
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			tags, ok = resp["Tags"].([]any)
			require.True(t, ok)
			assert.Empty(t, tags)
		})
	}
}

func TestTimestreamQueryHandler_DescribeScheduledQuery_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantCode int
	}{
		{
			name:     "not found",
			arn:      "arn:aws:timestream:us-east-1:123456789012:scheduled-query/nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "DescribeScheduledQuery", map[string]any{"ScheduledQueryArn": tt.arn})
			assert.Equal(t, tt.wantCode, rec.Code)
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

func TestTimestreamQueryBackend_ListScheduledQueriesFull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queries   []string
		wantCount int
	}{
		{
			name:      "empty backend",
			queries:   []string{},
			wantCount: 0,
		},
		{
			name:      "single query",
			queries:   []string{"q1"},
			wantCount: 1,
		},
		{
			name:      "multiple queries sorted by name",
			queries:   []string{"zeta", "alpha", "beta"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			for _, name := range tt.queries {
				createBody := map[string]any{
					"Name":                           name,
					"QueryString":                    "SELECT 1",
					"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
					"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
					"NotificationConfiguration": map[string]any{
						"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:t"},
					},
					"ErrorReportConfiguration": map[string]any{
						"S3Configuration": map[string]any{"BucketName": "b"},
					},
				}

				rec := doRequest(t, h, "CreateScheduledQuery", createBody)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			queries := h.Backend.ListScheduledQueriesFull()
			assert.Len(t, queries, tt.wantCount)

			if len(queries) > 1 {
				for i := 1; i < len(queries); i++ {
					assert.LessOrEqual(t, queries[i-1].Name, queries[i].Name)
				}
			}
		})
	}
}

func TestTimestreamQueryHandler_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name:     "create - missing name",
			op:       "CreateScheduledQuery",
			body:     map[string]any{"QueryString": "SELECT 1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete - missing arn",
			op:       "DeleteScheduledQuery",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "describe - missing arn",
			op:       "DescribeScheduledQuery",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "execute - missing arn",
			op:       "ExecuteScheduledQuery",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update - missing arn",
			op:       "UpdateScheduledQuery",
			body:     map[string]any{"State": "ENABLED"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update - missing state",
			op:       "UpdateScheduledQuery",
			body:     map[string]any{"ScheduledQueryArn": "arn:aws:timestream:us-east-1:123:scheduled-query/q"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag - missing arn",
			op:       "TagResource",
			body:     map[string]any{"Tags": []any{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "untag - missing arn",
			op:       "UntagResource",
			body:     map[string]any{"TagKeys": []string{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list tags - missing arn",
			op:       "ListTagsForResource",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTimestreamQueryHandler_DeleteAndExecute_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		arn      string
		wantCode int
	}{
		{
			name:     "delete not found",
			op:       "DeleteScheduledQuery",
			arn:      "arn:aws:timestream:us-east-1:123:scheduled-query/nonexistent",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "execute not found",
			op:       "ExecuteScheduledQuery",
			arn:      "arn:aws:timestream:us-east-1:123:scheduled-query/nonexistent",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update not found",
			op:       "UpdateScheduledQuery",
			arn:      "arn:aws:timestream:us-east-1:123:scheduled-query/nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			body := map[string]any{"ScheduledQueryArn": tt.arn}
			if tt.op == "UpdateScheduledQuery" {
				body["State"] = "DISABLED"
			}

			rec := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
