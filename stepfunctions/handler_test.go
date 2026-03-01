package stepfunctions_test

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

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/stepfunctions"
)

// sfnRequest sends a POST request to the Step Functions handler.
func sfnRequest(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := stepfunctions.NewInMemoryBackend()
	handler := stepfunctions.NewHandler(backend, log)

	return sfnRequestWithHandler(t, handler, e, action, body)
}

// sfnRequestWithHandler sends a POST to a specific handler instance.
func sfnRequestWithHandler(
	t *testing.T,
	handler *stepfunctions.Handler,
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
		req.Header.Set("X-Amz-Target", "AmazonStates."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, "StepFunctions", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var ops []string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
	assert.Contains(t, ops, "CreateStateMachine")
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodGet, "/path", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "InvalidTarget")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "UnknownOp", "{}")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "CreateStateMachine", `not-json`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_CreateStateMachine(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "CreateStateMachine",
		`{"name":"test-sm","definition":"{}","roleArn":"arn:role","type":"STANDARD"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["stateMachineArn"].(string), "test-sm")
}

func TestHandler_CreateStateMachine_AlreadyExists(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"dup","definition":"{}","roleArn":"arn:role"}`)
	rec := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"dup","definition":"{}","roleArn":"arn:role"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteStateMachine(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"del-sm","definition":"{}","roleArn":"arn:role"}`)
	require.Equal(t, http.StatusOK, cr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))
	arn := created["stateMachineArn"].(string)

	rec := sfnRequestWithHandler(t, h, e, "DeleteStateMachine",
		`{"stateMachineArn":"`+arn+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteStateMachine_NotFound(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "DeleteStateMachine",
		`{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListStateMachines(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"sm-1","definition":"{}","roleArn":"arn:role"}`)
	sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"sm-2","definition":"{}","roleArn":"arn:role"}`)

	rec := sfnRequestWithHandler(t, h, e, "ListStateMachines", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["stateMachines"].([]any), 2)
}

func TestHandler_DescribeStateMachine(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"desc-sm","definition":"{}","roleArn":"arn:role","type":"EXPRESS"}`)
	require.Equal(t, http.StatusOK, cr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))

	rec := sfnRequestWithHandler(t, h, e, "DescribeStateMachine",
		`{"stateMachineArn":"`+created["stateMachineArn"].(string)+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var sm map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sm))
	assert.Equal(t, "EXPRESS", sm["type"])
}

func TestHandler_DescribeStateMachine_NotFound(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "DescribeStateMachine",
		`{"stateMachineArn":"arn:nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_StartExecution(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"start-sm","definition":"{}","roleArn":"arn:role"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))

	rec := sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+created["stateMachineArn"].(string)+`","name":"exec1","input":"{}"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["executionArn"].(string), "exec1")
}

func TestHandler_StartExecution_SMNotFound(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "StartExecution",
		`{"stateMachineArn":"arn:nonexistent","name":"exec1","input":"{}"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_StartExecution_AlreadyExists(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"dup-exec-sm","definition":"{}","roleArn":"arn:role"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))
	arn := created["stateMachineArn"].(string)

	sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+arn+`","name":"exec-dup","input":"{}"}`)
	rec := sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+arn+`","name":"exec-dup","input":"{}"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DescribeExecution(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"ex-sm","definition":"{}","roleArn":"arn:role"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))

	er := sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+created["stateMachineArn"].(string)+`","name":"myexec","input":"{}"}`)
	var execResp map[string]any
	require.NoError(t, json.Unmarshal(er.Body.Bytes(), &execResp))

	rec := sfnRequestWithHandler(t, h, e, "DescribeExecution",
		`{"executionArn":"`+execResp["executionArn"].(string)+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeExecution_NotFound(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "DescribeExecution", `{"executionArn":"arn:nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_StopExecution(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"stop-sm","definition":"{}","roleArn":"arn:role"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))

	er := sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+created["stateMachineArn"].(string)+`","name":"stop-exec","input":"{}"}`)
	var execResp map[string]any
	require.NoError(t, json.Unmarshal(er.Body.Bytes(), &execResp))

	rec := sfnRequestWithHandler(t, h, e, "StopExecution",
		`{"executionArn":"`+execResp["executionArn"].(string)+`","error":"MyErr","cause":"test stop"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_StopExecution_NotFound(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "StopExecution", `{"executionArn":"arn:nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListExecutions(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"list-exec-sm","definition":"{}","roleArn":"arn:role"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))
	arn := created["stateMachineArn"].(string)

	sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+arn+`","name":"e1","input":"{}"}`)
	sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+arn+`","name":"e2","input":"{}"}`)

	rec := sfnRequestWithHandler(t, h, e, "ListExecutions",
		`{"stateMachineArn":"`+arn+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["executions"].([]any), 2)
}

func TestHandler_GetExecutionHistory(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"hist-sm","definition":"{}","roleArn":"arn:role"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))

	er := sfnRequestWithHandler(t, h, e, "StartExecution",
		`{"stateMachineArn":"`+created["stateMachineArn"].(string)+`","name":"hist-exec","input":"{}"}`)
	var execResp map[string]any
	require.NoError(t, json.Unmarshal(er.Body.Bytes(), &execResp))

	rec := sfnRequestWithHandler(t, h, e, "GetExecutionHistory",
		`{"executionArn":"`+execResp["executionArn"].(string)+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["events"].([]any), 2)
}

func TestHandler_GetExecutionHistory_NotFound(t *testing.T) {
	t.Parallel()
	rec := sfnRequest(t, "GetExecutionHistory", `{"executionArn":"arn:nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"tag-sm","definition":"{}","roleArn":"arn:role"}`)
	require.Equal(t, http.StatusOK, cr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))
	arn := created["stateMachineArn"].(string)

	rec := sfnRequestWithHandler(t, h, e, "TagResource",
		`{"resourceArn":"`+arn+`","tags":{"env":"prod","team":"infra"}}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"list-tag-sm","definition":"{}","roleArn":"arn:role"}`)
	require.Equal(t, http.StatusOK, cr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))
	arn := created["stateMachineArn"].(string)

	// Tag the resource.
	sfnRequestWithHandler(t, h, e, "TagResource",
		`{"resourceArn":"`+arn+`","tags":{"env":"prod"}}`)

	// List and verify.
	rec := sfnRequestWithHandler(t, h, e, "ListTagsForResource",
		`{"resourceArn":"`+arn+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tags := resp["tags"].([]any)
	assert.NotEmpty(t, tags)
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk, log)

	cr := sfnRequestWithHandler(t, h, e, "CreateStateMachine",
		`{"name":"untag-sm","definition":"{}","roleArn":"arn:role"}`)
	require.Equal(t, http.StatusOK, cr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &created))
	arn := created["stateMachineArn"].(string)

	// Tag then untag.
	sfnRequestWithHandler(t, h, e, "TagResource",
		`{"resourceArn":"`+arn+`","tags":{"env":"prod","team":"infra"}}`)
	rec := sfnRequestWithHandler(t, h, e, "UntagResource",
		`{"resourceArn":"`+arn+`","tagKeys":["team"]}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify only "env" remains.
	listRec := sfnRequestWithHandler(t, h, e, "ListTagsForResource",
		`{"resourceArn":"`+arn+`"}`)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	tags := resp["tags"].([]any)
	assert.Len(t, tags, 1)

	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["key"])
	assert.Equal(t, "prod", tag["value"])
}
