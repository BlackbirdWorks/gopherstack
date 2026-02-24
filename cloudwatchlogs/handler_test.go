package cloudwatchlogs_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const logsTarget = "Logs_20140328."

// makeLogsRequest sends a POST to the CloudWatch Logs handler with a fresh backend.
func makeLogsRequest(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := cloudwatchlogs.NewInMemoryBackend()
	handler := cloudwatchlogs.NewHandler(backend, log)

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
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
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
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	req := httptest.NewRequest(http.MethodGet, "/notroot", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "InvalidTarget")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "UnknownOp", "{}")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "CreateLogGroup", "not-json")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_CreateLogGroup(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "CreateLogGroup", `{"logGroupName":"/my/group"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CreateLogGroup_AlreadyExists(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	rec := doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"dup"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"dup"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteLogGroup(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"to-delete"}`)
	rec := doLogsRequest(t, h, e, "DeleteLogGroup", `{"logGroupName":"to-delete"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteLogGroup_NotFound(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "DeleteLogGroup", `{"logGroupName":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DescribeLogGroups(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/prod/app"}`)
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/dev/app"}`)

	rec := doLogsRequest(t, h, e, "DescribeLogGroups", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups := resp["logGroups"].([]any)
	assert.Len(t, groups, 2)
}

func TestHandler_DescribeLogGroups_WithPrefix(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/prod/app"}`)
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/dev/app"}`)

	rec := doLogsRequest(t, h, e, "DescribeLogGroups", `{"logGroupNamePrefix":"/prod"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["logGroups"].([]any), 1)
}

func TestHandler_CreateLogStream(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	rec := doLogsRequest(t, h, e, "CreateLogStream",
		`{"logGroupName":"grp","logStreamName":"stream"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CreateLogStream_GroupNotFound(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "CreateLogStream",
		`{"logGroupName":"nonexistent","logStreamName":"stream"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateLogStream_AlreadyExists(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"dup"}`)
	rec := doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"dup"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DescribeLogStreams(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"s1"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"s2"}`)

	rec := doLogsRequest(t, h, e, "DescribeLogStreams", `{"logGroupName":"grp"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["logStreams"].([]any), 2)
}

func TestHandler_DescribeLogStreams_GroupNotFound(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "DescribeLogStreams", `{"logGroupName":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_PutLogEvents(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"s"}`)

	rec := doLogsRequest(t, h, e, "PutLogEvents",
		`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"hello","timestamp":1000}]}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["nextSequenceToken"])
}

func TestHandler_PutLogEvents_GroupNotFound(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "PutLogEvents",
		`{"logGroupName":"nonexistent","logStreamName":"s","logEvents":[]}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetLogEvents(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"s"}`)
	doLogsRequest(t, h, e, "PutLogEvents",
		`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"m1","timestamp":1000}]}`)

	rec := doLogsRequest(t, h, e, "GetLogEvents",
		`{"logGroupName":"grp","logStreamName":"s"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["events"].([]any), 1)
}

func TestHandler_GetLogEvents_NotFound(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "GetLogEvents",
		`{"logGroupName":"nonexistent","logStreamName":"s"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_FilterLogEvents(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"s"}`)
	doLogsRequest(t, h, e, "PutLogEvents",
		`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"ERROR: bad","timestamp":1000}]}`)

	rec := doLogsRequest(t, h, e, "FilterLogEvents",
		`{"logGroupName":"grp","filterPattern":"ERROR"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["events"].([]any), 1)
}

func TestHandler_FilterLogEvents_GroupNotFound(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "FilterLogEvents", `{"logGroupName":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
