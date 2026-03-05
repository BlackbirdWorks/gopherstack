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

	"github.com/blackbirdworks/gopherstack/cloudwatchlogs"
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
			name:     "PutRetentionPolicy",
			action:   "PutRetentionPolicy",
			body:     map[string]any{"logGroupName": "grp", "retentionInDays": 30},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteRetentionPolicy",
			action:   "DeleteRetentionPolicy",
			body:     map[string]any{"logGroupName": "grp"},
			wantCode: http.StatusOK,
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
