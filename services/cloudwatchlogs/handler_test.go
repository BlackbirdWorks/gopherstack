package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const logsTarget = "Logs_20140328."

// makeLogsRequest sends a POST to the CloudWatch Logs handler with a fresh backend.
func makeLogsRequest(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	handler := cloudwatchlogs.NewHandler(backend)

	return doLogsRequest(t, handler, e, action, body)
}

// doLogsRequest sends a POST to the given handler.
func doLogsRequest(
	t *testing.T,
	handler *cloudwatchlogs.Handler,
	e *echo.Echo,
	action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}

	if action != "" {
		req.Header.Set("X-Amz-Target", logsTarget+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, handler.Handler()(c))

	return rec
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var ops []string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
	assert.Contains(t, ops, "CreateLogGroup")
	assert.Contains(t, ops, "PutLogEvents")
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodGet, "/notroot", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "InvalidTarget")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "CreateLogGroup", "not-json")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body              map[string]any
		name              string
		action            string
		wantListField     string
		wantNotEmptyField string
		wantCode          int
		wantListLen       int
	}{
		{
			name:     "UnknownOperation",
			action:   "UnknownOp",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateLogGroup",
			action:   "CreateLogGroup",
			body:     map[string]any{"logGroupName": "/my/group"},
			wantCode: http.StatusOK,
		},
		{
			name: "CreateLogGroup/AlreadyExists",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"dup"}`)
			},
			action:   "CreateLogGroup",
			body:     map[string]any{"logGroupName": "dup"},
			wantCode: http.StatusConflict,
		},
		{
			name: "DeleteLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"to-delete"}`)
			},
			action:   "DeleteLogGroup",
			body:     map[string]any{"logGroupName": "to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteLogGroup/NotFound",
			action:   "DeleteLogGroup",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "DescribeLogGroups",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/prod/app"}`)
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/dev/app"}`)
			},
			action:        "DescribeLogGroups",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "logGroups",
			wantListLen:   2,
		},
		{
			name: "DescribeLogGroups/WithPrefix",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/prod/app"}`)
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/dev/app"}`)
			},
			action:        "DescribeLogGroups",
			body:          map[string]any{"logGroupNamePrefix": "/prod"},
			wantCode:      http.StatusOK,
			wantListField: "logGroups",
			wantListLen:   1,
		},
		{
			name: "CreateLogStream",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
			},
			action:   "CreateLogStream",
			body:     map[string]any{"logGroupName": "grp", "logStreamName": "stream"},
			wantCode: http.StatusOK,
		},
		{
			name:     "CreateLogStream/GroupNotFound",
			action:   "CreateLogStream",
			body:     map[string]any{"logGroupName": "nonexistent", "logStreamName": "stream"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "CreateLogStream/AlreadyExists",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"dup"}`,
				)
			},
			action:   "CreateLogStream",
			body:     map[string]any{"logGroupName": "grp", "logStreamName": "dup"},
			wantCode: http.StatusConflict,
		},
		{
			name: "DescribeLogStreams",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s1"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s2"}`,
				)
			},
			action:        "DescribeLogStreams",
			body:          map[string]any{"logGroupName": "grp"},
			wantCode:      http.StatusOK,
			wantListField: "logStreams",
			wantListLen:   2,
		},
		{
			name:     "DescribeLogStreams/GroupNotFound",
			action:   "DescribeLogStreams",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "PutLogEvents",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s"}`,
				)
			},
			action: "PutLogEvents",
			body: map[string]any{
				"logGroupName":  "grp",
				"logStreamName": "s",
				"logEvents":     []any{map[string]any{"message": "hello", "timestamp": 1000}},
			},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "nextSequenceToken",
		},
		{
			name:   "PutLogEvents/GroupNotFound",
			action: "PutLogEvents",
			body: map[string]any{
				"logGroupName":  "nonexistent",
				"logStreamName": "s",
				"logEvents":     []any{},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "GetLogEvents",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutLogEvents",
					`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"m1","timestamp":1000}]}`,
				)
			},
			action:        "GetLogEvents",
			body:          map[string]any{"logGroupName": "grp", "logStreamName": "s"},
			wantCode:      http.StatusOK,
			wantListField: "events",
			wantListLen:   1,
		},
		{
			name:     "GetLogEvents/NotFound",
			action:   "GetLogEvents",
			body:     map[string]any{"logGroupName": "nonexistent", "logStreamName": "s"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "FilterLogEvents",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutLogEvents",
					`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"ERROR: bad","timestamp":1000}]}`,
				)
			},
			action:        "FilterLogEvents",
			body:          map[string]any{"logGroupName": "grp", "filterPattern": "ERROR"},
			wantCode:      http.StatusOK,
			wantListField: "events",
			wantListLen:   1,
		},
		{
			name:     "FilterLogEvents/GroupNotFound",
			action:   "FilterLogEvents",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "TagLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
			},
			action: "TagLogGroup",
			body: map[string]any{
				"logGroupName": "tag-grp",
				"tags":         map[string]string{"env": "prod", "team": "ops"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "ListTagsLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"TagLogGroup",
					`{"logGroupName":"tag-grp","tags":{"env":"prod","team":"ops"}}`,
				)
			},
			action:   "ListTagsLogGroup",
			body:     map[string]any{"logGroupName": "tag-grp"},
			wantCode: http.StatusOK,
		},
		{
			name: "ListTagsForResource",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"TagLogGroup",
					`{"logGroupName":"tag-grp","tags":{"env":"prod"}}`,
				)
			},
			action:   "ListTagsForResource",
			body:     map[string]any{"resourceArn": "tag-grp"},
			wantCode: http.StatusOK,
		},
		{
			name: "UntagLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"TagLogGroup",
					`{"logGroupName":"tag-grp","tags":{"env":"prod","team":"ops"}}`,
				)
			},
			action:   "UntagLogGroup",
			body:     map[string]any{"logGroupName": "tag-grp", "tags": []string{"env"}},
			wantCode: http.StatusOK,
		},
		{
			name: "PutRetentionPolicy",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"ret-grp"}`)
			},
			action:   "PutRetentionPolicy",
			body:     map[string]any{"logGroupName": "ret-grp", "retentionInDays": 30},
			wantCode: http.StatusOK,
		},
		{
			name: "DeleteRetentionPolicy",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"ret-grp"}`)
				doLogsRequest(t, h, e, "PutRetentionPolicy",
					`{"logGroupName":"ret-grp","retentionInDays":30}`)
			},
			action:   "DeleteRetentionPolicy",
			body:     map[string]any{"logGroupName": "ret-grp"},
			wantCode: http.StatusOK,
		},
		{
			name: "PutSubscriptionFilter",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
			},
			action: "PutSubscriptionFilter",
			body: map[string]any{
				"logGroupName":   "sub-grp",
				"filterName":     "my-filter",
				"filterPattern":  "",
				"destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:target",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutSubscriptionFilter/GroupNotFound",
			action: "PutSubscriptionFilter",
			body: map[string]any{
				"logGroupName":   "nonexistent",
				"filterName":     "f",
				"destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:target",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "DescribeSubscriptionFilters",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
				doLogsRequest(t, h, e, "PutSubscriptionFilter",
					`{"logGroupName":"sub-grp","filterName":"f1","filterPattern":"",`+
						`"destinationArn":"arn:aws:lambda:us-east-1:123456789012:function:a"}`,
				)
			},
			action:        "DescribeSubscriptionFilters",
			body:          map[string]any{"logGroupName": "sub-grp"},
			wantCode:      http.StatusOK,
			wantListField: "subscriptionFilters",
			wantListLen:   1,
		},
		{
			name:     "DescribeSubscriptionFilters/GroupNotFound",
			action:   "DescribeSubscriptionFilters",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "DeleteSubscriptionFilter",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
				doLogsRequest(t, h, e, "PutSubscriptionFilter",
					`{"logGroupName":"sub-grp","filterName":"f1","filterPattern":"",`+
						`"destinationArn":"arn:aws:lambda:us-east-1:123456789012:function:a"}`,
				)
			},
			action:   "DeleteSubscriptionFilter",
			body:     map[string]any{"logGroupName": "sub-grp", "filterName": "f1"},
			wantCode: http.StatusOK,
		},
		{
			name: "DeleteSubscriptionFilter/NotFound",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
			},
			action:   "DeleteSubscriptionFilter",
			body:     map[string]any{"logGroupName": "sub-grp", "filterName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "StartQuery",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"qgrp"}`)
			},
			action: "StartQuery",
			body: map[string]any{
				"logGroupName": "qgrp",
				"queryString":  "fields @timestamp, @message",
				"startTime":    0,
				"endTime":      0,
			},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "queryId",
		},
		{
			name: "StartQuery/MultipleGroups",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp1"}`)
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp2"}`)
			},
			action: "StartQuery",
			body: map[string]any{
				"logGroupNames": []string{"grp1", "grp2"},
				"queryString":   "fields @message",
				"startTime":     0,
				"endTime":       0,
			},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "queryId",
		},
		{
			name:     "StartQuery/InvalidQuery",
			action:   "StartQuery",
			body:     map[string]any{"logGroupName": "grp", "queryString": "limit notanumber"},
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "GetQueryResults/NotFound",
			action:   "GetQueryResults",
			body:     map[string]any{"queryId": "no-such-id"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "StopQuery/NotFound",
			action:   "StopQuery",
			body:     map[string]any{"queryId": "no-such-id"},
			wantCode: http.StatusNotFound,
		},
		{
			name:          "DescribeQueries/Empty",
			action:        "DescribeQueries",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "queries",
			wantListLen:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp[tt.wantListField].([]any), tt.wantListLen)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}

func TestHandler_TagRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Create a log group and tag it.
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(
		t,
		h,
		e,
		"TagLogGroup",
		`{"logGroupName":"grp","tags":{"env":"prod","team":"ops"}}`,
	)

	// ListTagsLogGroup returns both tags.
	rec := doLogsRequest(t, h, e, "ListTagsLogGroup", `{"logGroupName":"grp"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, "prod", listResp["tags"]["env"])
	assert.Equal(t, "ops", listResp["tags"]["team"])

	// ListTagsForResource also works.
	rec2 := doLogsRequest(t, h, e, "ListTagsForResource", `{"resourceArn":"grp"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp2 map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp2))
	assert.Len(t, listResp2["tags"], 2)

	// Remove one tag.
	doLogsRequest(t, h, e, "UntagLogGroup", `{"logGroupName":"grp","tags":["env"]}`)

	// Verify only "team" remains.
	rec3 := doLogsRequest(t, h, e, "ListTagsLogGroup", `{"logGroupName":"grp"}`)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp3 map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp3))
	assert.Len(t, listResp3["tags"], 1)
	assert.Equal(t, "ops", listResp3["tags"]["team"])
}

func TestHandler_InsightsWorkflow(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Set up log group, stream, and events.
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/insights/test"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/insights/test","logStreamName":"stream1"}`)
	doLogsRequest(t, h, e, "PutLogEvents", `{
"logGroupName":"/insights/test","logStreamName":"stream1",
"logEvents":[
{"message":"ERROR: disk full","timestamp":1000},
{"message":"INFO: startup","timestamp":2000},
{"message":"ERROR: connection refused","timestamp":3000}
]
}`)

	// StartQuery with filter.
	rec := doLogsRequest(t, h, e, "StartQuery", `{
"logGroupName":"/insights/test",
"queryString":"fields @timestamp, @message | filter @message like /ERROR/",
"startTime":0,"endTime":0
}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
	queryID := startOut["queryId"]
	require.NotEmpty(t, queryID)

	// GetQueryResults.
	rec2 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resultsOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resultsOut))
	assert.Equal(t, "Complete", resultsOut["status"])
	rows, ok := resultsOut["results"].([]any)
	require.True(t, ok)
	assert.Len(t, rows, 2) // only 2 ERROR events

	// DescribeQueries lists the query.
	rec3 := doLogsRequest(t, h, e, "DescribeQueries", `{"logGroupName":"/insights/test"}`)
	require.Equal(t, http.StatusOK, rec3.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &descOut))
	queries, ok := descOut["queries"].([]any)
	require.True(t, ok)
	assert.Len(t, queries, 1)

	// StopQuery cancels the query.
	rec4 := doLogsRequest(t, h, e, "StopQuery", `{"queryId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec4.Code)

	var stopOut map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &stopOut))
	assert.Equal(t, true, stopOut["success"])

	// GetQueryResults now shows Cancelled.
	rec5 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec5.Code)

	var resultsOut2 map[string]any
	require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &resultsOut2))
	assert.Equal(t, "Cancelled", resultsOut2["status"])
}

func TestHandler_InsightsQueryLanguage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		query       string
		wantResults int
	}{
		{
			name:        "fields_projection",
			query:       "fields @timestamp, @message",
			wantResults: 3,
		},
		{
			name:        "filter_regex",
			query:       "filter @message like /ERROR/",
			wantResults: 2,
		},
		{
			name:        "filter_limit",
			query:       "fields @message | limit 1",
			wantResults: 1,
		},
		{
			name:        "sort_desc",
			query:       "fields @timestamp | sort @timestamp desc",
			wantResults: 3,
		},
		{
			name:        "stats_count",
			query:       "stats count(*) by @message",
			wantResults: 3,
		},
		{
			name:        "empty_query",
			query:       "",
			wantResults: 3,
		},
		{
			name:        "unknown_command_ignored",
			query:       "parse @message \"* *\" as level, rest",
			wantResults: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/grp"}`)
			doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/grp","logStreamName":"s"}`)
			doLogsRequest(t, h, e, "PutLogEvents", `{
"logGroupName":"/grp","logStreamName":"s",
"logEvents":[
{"message":"ERROR: one","timestamp":1000},
{"message":"INFO: two","timestamp":2000},
{"message":"ERROR: three","timestamp":3000}
]
}`)

			bodyJSON, merr := json.Marshal(map[string]any{
				"logGroupName": "/grp",
				"queryString":  tt.query,
				"startTime":    0,
				"endTime":      0,
			})
			require.NoError(t, merr)

			rec := doLogsRequest(t, h, e, "StartQuery", string(bodyJSON))
			require.Equal(t, http.StatusOK, rec.Code)

			var startOut map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
			queryID := startOut["queryId"]

			rec2 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resultsOut map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resultsOut))
			rows, ok := resultsOut["results"].([]any)
			require.True(t, ok, "results should be an array")
			assert.Len(t, rows, tt.wantResults)
		})
	}
}

func TestHandler_InsightsARNIdentifier(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Create log group named "/arn-test" and add events.
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/arn-test"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/arn-test","logStreamName":"s"}`)
	doLogsRequest(t, h, e, "PutLogEvents", `{
"logGroupName":"/arn-test","logStreamName":"s",
"logEvents":[{"message":"hello","timestamp":1000}]
}`)

	tests := []struct {
		body        map[string]any
		name        string
		wantResults int
	}{
		{
			name: "arn_identifier_resolves_to_name",
			body: map[string]any{
				"logGroupIdentifiers": []string{
					"arn:aws:logs:us-east-1:123456789012:log-group:/arn-test",
				},
				"queryString": "fields @message",
				"startTime":   0,
				"endTime":     0,
			},
			wantResults: 1,
		},
		{
			name: "plain_name_identifier",
			body: map[string]any{
				"logGroupIdentifiers": []string{"/arn-test"},
				"queryString":         "fields @message",
				"startTime":           0,
				"endTime":             0,
			},
			wantResults: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, "StartQuery", string(bodyBytes))
			require.Equal(t, http.StatusOK, rec.Code)

			var startOut map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
			queryID := startOut["queryId"]
			require.NotEmpty(t, queryID)

			rec2 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resultsOut map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resultsOut))
			rows, ok := resultsOut["results"].([]any)
			require.True(t, ok)
			assert.Len(t, rows, tt.wantResults)
		})
	}
}

func TestHandler_DescribeQueries_Pagination(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/pg"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/pg","logStreamName":"s"}`)

	// Start 3 queries.
	for i := range 3 {
		bodyBytes, err := json.Marshal(map[string]any{
			"logGroupName": "/pg",
			"queryString":  "fields @message",
			"startTime":    i,
			"endTime":      0,
		})
		require.NoError(t, err)

		rec := doLogsRequest(t, h, e, "StartQuery", string(bodyBytes))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: maxResults=2.
	rec := doLogsRequest(t, h, e, "DescribeQueries", `{"logGroupName":"/pg","maxResults":2}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	page1Queries, ok := page1["queries"].([]any)
	require.True(t, ok, "page1 queries should be an array")
	assert.Len(t, page1Queries, 2)
	nextToken, _ := page1["nextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Second page.
	bodyBytes, err := json.Marshal(map[string]any{
		"logGroupName": "/pg",
		"maxResults":   2,
		"nextToken":    nextToken,
	})
	require.NoError(t, err)

	rec2 := doLogsRequest(t, h, e, "DescribeQueries", string(bodyBytes))
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	page2Queries, ok := page2["queries"].([]any)
	require.True(t, ok, "page2 queries should be an array")
	assert.Len(t, page2Queries, 1)
}

func TestHandler_DeleteLogStream(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"g"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"g","logStreamName":"s"}`)

	// Verify stream exists.
	rec := doLogsRequest(t, h, e, "DescribeLogStreams", `{"logGroupName":"g"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var desc1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc1))
	assert.Len(t, desc1["logStreams"], 1)

	// Delete it.
	rec = doLogsRequest(t, h, e, "DeleteLogStream", `{"logGroupName":"g","logStreamName":"s"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stream gone.
	rec = doLogsRequest(t, h, e, "DescribeLogStreams", `{"logGroupName":"g"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var desc2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc2))
	assert.Empty(t, desc2["logStreams"])

	// Delete non-existent stream → 404.
	rec = doLogsRequest(t, h, e, "DeleteLogStream", `{"logGroupName":"g","logStreamName":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagResource_UntagResource(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	arn := "arn:aws:logs:us-east-1:123456789012:log-group:/my-group"

	// TagResource — set two tags.
	rec := doLogsRequest(t, h, e, "TagResource",
		`{"resourceArn":"`+arn+`","tags":{"env":"prod","team":"platform"}}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource — verify tags present.
	rec = doLogsRequest(t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var listOut map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Equal(t, "prod", listOut["tags"]["env"])
	assert.Equal(t, "platform", listOut["tags"]["team"])

	// UntagResource — remove one tag.
	rec = doLogsRequest(t, h, e, "UntagResource",
		`{"resourceArn":"`+arn+`","tagKeys":["env"]}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify env tag removed, team tag remains.
	rec = doLogsRequest(t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["tags"]["env"])
	assert.Equal(t, "platform", listOut["tags"]["team"])
}

func TestHandler_Reset_ClearsTags(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Set a tag.
	rec := doLogsRequest(t, h, e, "TagResource",
		`{"resourceArn":"arn:aws:logs:us-east-1:123:log-group:/g","tags":{"k":"v"}}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Reset.
	h.Reset()

	// Tags should be gone after reset.
	rec = doLogsRequest(t, h, e, "ListTagsForResource",
		`{"resourceArn":"arn:aws:logs:us-east-1:123:log-group:/g"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var listOut map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["tags"])
}

func TestHandler_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// AssociateKmsKey
		{
			name:     "AssociateKmsKey/LogGroup",
			action:   "AssociateKmsKey",
			body:     map[string]any{"logGroupName": "/my/group", "kmsKeyId": "arn:aws:kms:us-east-1:123:key/abc"},
			wantCode: http.StatusOK,
		},
		{
			name:   "AssociateKmsKey/ResourceIdentifier",
			action: "AssociateKmsKey",
			body: map[string]any{
				"resourceIdentifier": "arn:aws:logs:us-east-1:123:query-result:*",
				"kmsKeyId":           "arn:aws:kms:us-east-1:123:key/abc",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "AssociateKmsKey/MissingKmsKeyId",
			action:   "AssociateKmsKey",
			body:     map[string]any{"logGroupName": "/my/group"},
			wantCode: http.StatusBadRequest,
		},
		// AssociateSourceToS3TableIntegration
		{
			name:   "AssociateSourceToS3TableIntegration/OK",
			action: "AssociateSourceToS3TableIntegration",
			body: map[string]any{
				"integrationArn": "arn:aws:s3tables:us-east-1:123:integration/my-int",
				"dataSource":     map[string]any{"name": "source1", "type": "CloudWatchLogs"},
			},
			wantCode: http.StatusOK,
			wantKey:  "identifier",
		},
		{
			name:     "AssociateSourceToS3TableIntegration/MissingArn",
			action:   "AssociateSourceToS3TableIntegration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		// CancelExportTask
		{
			name:   "CancelExportTask/OK",
			action: "CancelExportTask",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				rec := doLogsRequest(t, h, e, "CreateExportTask",
					`{"logGroupName":"grp","destination":"my-bucket","from":1000,"to":2000}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				t.Logf("taskId=%v", out["taskId"])
			},
			body:     map[string]any{"taskId": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CancelExportTask/NotFound",
			action:   "CancelExportTask",
			body:     map[string]any{"taskId": "nonexistent-task"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "CancelExportTask/MissingTaskId",
			action:   "CancelExportTask",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		// CancelImportTask
		{
			name:     "CancelImportTask/NotFound",
			action:   "CancelImportTask",
			body:     map[string]any{"importId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "CancelImportTask/MissingImportId",
			action:   "CancelImportTask",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		// CreateDelivery
		{
			name:   "CreateDelivery/OK",
			action: "CreateDelivery",
			body: map[string]any{
				"deliverySourceName":     "my-source",
				"deliveryDestinationArn": "arn:aws:logs:us-east-1:123:delivery-destination:dst",
			},
			wantCode: http.StatusOK,
			wantKey:  "delivery",
		},
		{
			name:     "CreateDelivery/MissingSource",
			action:   "CreateDelivery",
			body:     map[string]any{"deliveryDestinationArn": "arn:aws:logs:us-east-1:123:delivery-destination:dst"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateDelivery/MissingDestination",
			action:   "CreateDelivery",
			body:     map[string]any{"deliverySourceName": "my-source"},
			wantCode: http.StatusBadRequest,
		},
		// CreateExportTask
		{
			name:     "CreateExportTask/OK",
			action:   "CreateExportTask",
			body:     map[string]any{"logGroupName": "/my/group", "destination": "my-bucket", "from": 1000, "to": 2000},
			wantCode: http.StatusOK,
			wantKey:  "taskId",
		},
		{
			name:     "CreateExportTask/MissingLogGroup",
			action:   "CreateExportTask",
			body:     map[string]any{"destination": "my-bucket", "from": 1000, "to": 2000},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateExportTask/MissingDestination",
			action:   "CreateExportTask",
			body:     map[string]any{"logGroupName": "/my/group", "from": 1000, "to": 2000},
			wantCode: http.StatusBadRequest,
		},
		// CreateImportTask
		{
			name:   "CreateImportTask/OK",
			action: "CreateImportTask",
			body: map[string]any{
				"importRoleArn":   "arn:aws:iam::123:role/my-role",
				"importSourceArn": "arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc",
			},
			wantCode: http.StatusOK,
			wantKey:  "importId",
		},
		{
			name:     "CreateImportTask/MissingRoleArn",
			action:   "CreateImportTask",
			body:     map[string]any{"importSourceArn": "arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateImportTask/MissingSourceArn",
			action:   "CreateImportTask",
			body:     map[string]any{"importRoleArn": "arn:aws:iam::123:role/my-role"},
			wantCode: http.StatusBadRequest,
		},
		// CreateLogAnomalyDetector
		{
			name:     "CreateLogAnomalyDetector/OK",
			action:   "CreateLogAnomalyDetector",
			body:     map[string]any{"logGroupArnList": []string{"arn:aws:logs:us-east-1:123:log-group:/my/group"}},
			wantCode: http.StatusOK,
			wantKey:  "anomalyDetectorArn",
		},
		{
			name:     "CreateLogAnomalyDetector/EmptyList",
			action:   "CreateLogAnomalyDetector",
			body:     map[string]any{"logGroupArnList": []string{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateLogAnomalyDetector/MissingList",
			action:   "CreateLogAnomalyDetector",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		// CreateScheduledQuery
		{
			name:   "CreateScheduledQuery/OK",
			action: "CreateScheduledQuery",
			body: map[string]any{
				"name":               "my-query",
				"queryString":        "fields @timestamp | sort @timestamp desc",
				"scheduleExpression": "cron(0 * * * ? *)",
				"executionRoleArn":   "arn:aws:iam::123:role/role",
			},
			wantCode: http.StatusOK,
			wantKey:  "scheduledQueryArn",
		},
		{
			name:     "CreateScheduledQuery/DefaultStateEnabled",
			action:   "CreateScheduledQuery",
			body:     map[string]any{"name": "q2", "queryString": "fields @message"},
			wantCode: http.StatusOK,
			wantKey:  "state",
			wantVal:  "ENABLED",
		},
		{
			name:     "CreateScheduledQuery/MissingName",
			action:   "CreateScheduledQuery",
			body:     map[string]any{"queryString": "fields @message"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateScheduledQuery/MissingQueryString",
			action:   "CreateScheduledQuery",
			body:     map[string]any{"name": "my-query"},
			wantCode: http.StatusBadRequest,
		},
		// DeleteAccountPolicy
		{
			name:     "DeleteAccountPolicy/OK",
			action:   "DeleteAccountPolicy",
			body:     map[string]any{"policyName": "my-policy", "policyType": "DATA_PROTECTION_POLICY"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteAccountPolicy/MissingPolicyName",
			action:   "DeleteAccountPolicy",
			body:     map[string]any{"policyType": "DATA_PROTECTION_POLICY"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteAccountPolicy/MissingPolicyType",
			action:   "DeleteAccountPolicy",
			body:     map[string]any{"policyName": "my-policy"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}

func TestHandler_ExportTask_CancelRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	// Create an export task.
	rec := doLogsRequest(t, h, e, "CreateExportTask",
		`{"logGroupName":"/my/group","destination":"my-bucket","from":1000,"to":2000}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	taskID, ok := createOut["taskId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, taskID)

	// Cancel the task.
	bodyBytes, err := json.Marshal(map[string]any{"taskId": taskID})
	require.NoError(t, err)
	rec = doLogsRequest(t, h, e, "CancelExportTask", string(bodyBytes))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ImportTask_CancelRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	// Create an import task.
	createBody := `{"importRoleArn":"arn:aws:iam::123:role/my-role",` +
		`"importSourceArn":"arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc"}`
	rec := doLogsRequest(t, h, e, "CreateImportTask", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	importID, ok := createOut["importId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, importID)

	importDestARN, ok := createOut["importDestinationArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, importDestARN)

	// Cancel the task.
	bodyBytes, err := json.Marshal(map[string]any{"importId": importID})
	require.NoError(t, err)
	rec = doLogsRequest(t, h, e, "CancelImportTask", string(bodyBytes))
	require.Equal(t, http.StatusOK, rec.Code)

	var cancelOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cancelOut))
	assert.Equal(t, importID, cancelOut["importId"])
	assert.Equal(t, "CANCELLED", cancelOut["importStatus"])
}

func TestHandler_Delivery_CreateWithTags(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	deliveryBody := `{"deliverySourceName":"src",` +
		`"deliveryDestinationArn":"arn:aws:logs:us-east-1:123:delivery-destination:dst",` +
		`"tags":{"env":"prod"}}`
	rec := doLogsRequest(t, h, e, "CreateDelivery", deliveryBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	delivery, ok := out["delivery"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, delivery["id"])
	assert.NotEmpty(t, delivery["arn"])
	assert.Equal(t, "src", delivery["deliverySourceName"])
	tags, ok := delivery["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

func TestHandler_LogAnomalyDetector_Create(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	rec := doLogsRequest(
		t,
		h,
		e,
		"CreateLogAnomalyDetector",
		`{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],`+
			`"detectorName":"my-detector","evaluationFrequency":"FIVE_MIN"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	arn, ok := out["anomalyDetectorArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "log-anomaly-detector")
}

func TestHandler_ScheduledQuery_Create(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	rec := doLogsRequest(
		t,
		h,
		e,
		"CreateScheduledQuery",
		`{"name":"my-sched","queryString":"fields @message | limit 100",`+
			`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r","state":"DISABLED"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	queryARN, ok := out["scheduledQueryArn"].(string)
	require.True(t, ok)
	assert.Contains(t, queryARN, "scheduled-query")
	assert.Equal(t, "DISABLED", out["state"])
}

func TestHandler_AssociateKmsKey_Idempotent(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	const kmsKey = "arn:aws:kms:us-east-1:123:key/mykey"

	// Associate once.
	rec := doLogsRequest(t, h, e, "AssociateKmsKey",
		`{"logGroupName":"/my/group","kmsKeyId":"`+kmsKey+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Associate again (idempotent update).
	rec = doLogsRequest(t, h, e, "AssociateKmsKey",
		`{"logGroupName":"/my/group","kmsKeyId":"`+kmsKey+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteAccountPolicy_NonExistentSucceeds(t *testing.T) {
	t.Parallel()

	// DeleteAccountPolicy is idempotent: deleting a non-existent policy
	// should succeed (no error).
	rec := makeLogsRequest(t, "DeleteAccountPolicy",
		`{"policyName":"does-not-exist","policyType":"DATA_PROTECTION_POLICY"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}
