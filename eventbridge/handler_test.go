package eventbridge_test

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

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// makeRequest is a helper to send a POST request to the EventBridge handler.
func makeRequest(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}

	if action != "" {
		req.Header.Set("X-Amz-Target", "AmazonEventBridge."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// makeRequestWithHandler sends a POST to a specific handler instance.
func makeRequestWithHandler(
	t *testing.T,
	handler *eventbridge.Handler,
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
		req.Header.Set("X-Amz-Target", "AmazonEventBridge."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var ops []string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
	assert.Contains(t, ops, "CreateEventBus")
	assert.Contains(t, ops, "PutEvents")
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Missing X-Amz-Target")
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPut, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	rec := makeRequest(t, "NonExistentAction", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp eventbridge.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "UnknownOperationException", errResp.Type)
}

func TestHandler_CreateEventBus(t *testing.T) {
	t.Parallel()

	rec := makeRequest(t, "CreateEventBus", `{"Name":"test-bus","Description":"my bus"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["EventBusArn"], "test-bus")
}

func TestHandler_CreateAndListEventBuses(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"bus-a"}`)
	makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"bus-b"}`)

	rec := makeRequestWithHandler(t, handler, e, "ListEventBuses", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken  string                 `json:"NextToken"`
		EventBuses []eventbridge.EventBus `json:"EventBuses"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.GreaterOrEqual(t, len(resp.EventBuses), 3) // default + bus-a + bus-b
}

func TestHandler_DeleteEventBus(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"temp-bus"}`)

	rec := makeRequestWithHandler(t, handler, e, "DeleteEventBus", `{"Name":"temp-bus"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe should now return 404.
	rec = makeRequestWithHandler(t, handler, e, "DescribeEventBus", `{"Name":"temp-bus"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteDefaultBus_Fails(t *testing.T) {
	t.Parallel()

	rec := makeRequest(t, "DeleteEventBus", `{"Name":"default"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PutRuleAndListRules(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	rec := makeRequestWithHandler(t, handler, e, "PutRule",
		`{"Name":"my-rule","EventPattern":"{\"source\":[\"my.app\"]}","State":"ENABLED"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	assert.Contains(t, putResp["RuleArn"], "my-rule")

	rec = makeRequestWithHandler(t, handler, e, "ListRules", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Rules []eventbridge.Rule `json:"Rules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Rules, 1)
	assert.Equal(t, "my-rule", listResp.Rules[0].Name)
}

func TestHandler_DescribeRule(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	makeRequestWithHandler(t, handler, e, "PutRule",
		`{"Name":"desc-rule","Description":"a description","State":"DISABLED"}`)

	rec := makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"desc-rule"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var rule eventbridge.Rule
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
	assert.Equal(t, "desc-rule", rule.Name)
	assert.Equal(t, "a description", rule.Description)
	assert.Equal(t, "DISABLED", rule.State)
}

func TestHandler_EnableDisableRule(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	makeRequestWithHandler(t, handler, e, "PutRule", `{"Name":"toggle","State":"ENABLED"}`)

	rec := makeRequestWithHandler(t, handler, e, "DisableRule", `{"Name":"toggle"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"toggle"}`)
	var rule eventbridge.Rule
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
	assert.Equal(t, "DISABLED", rule.State)

	makeRequestWithHandler(t, handler, e, "EnableRule", `{"Name":"toggle"}`)
	rec = makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"toggle"}`)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
	assert.Equal(t, "ENABLED", rule.State)
}

func TestHandler_PutTargetsListAndRemove(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	makeRequestWithHandler(t, handler, e, "PutRule", `{"Name":"rule-t"}`)

	rec := makeRequestWithHandler(
		t,
		handler,
		e,
		"PutTargets",
		`{"Rule":"rule-t","Targets":[{"Id":"t1","Arn":"arn:aws:lambda:us-east-1:123:function:fn"},{"Id":"t2","Arn":"arn:aws:sqs:us-east-1:123:q"}]}`,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var putResp struct {
		FailedEntries    []eventbridge.FailedEntry `json:"FailedEntries"`
		FailedEntryCount int                       `json:"FailedEntryCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	assert.Equal(t, 0, putResp.FailedEntryCount)

	rec = makeRequestWithHandler(t, handler, e, "ListTargetsByRule", `{"Rule":"rule-t"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Targets []eventbridge.Target `json:"Targets"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Targets, 2)

	rec = makeRequestWithHandler(t, handler, e, "RemoveTargets", `{"Rule":"rule-t","Ids":["t1"]}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = makeRequestWithHandler(t, handler, e, "ListTargetsByRule", `{"Rule":"rule-t"}`)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Targets, 1)
	assert.Equal(t, "t2", listResp.Targets[0].ID)
}

func TestHandler_PutEvents(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend, log)

	rec := makeRequestWithHandler(
		t,
		handler,
		e,
		"PutEvents",
		`{"Entries":[{"Source":"my.app","DetailType":"UserCreated","Detail":"{\"userId\":\"1\"}"},{"Source":"my.app","DetailType":"UserDeleted","Detail":"{}"}]}`,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Entries          []eventbridge.EventResultEntry `json:"Entries"`
		FailedEntryCount int                            `json:"FailedEntryCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.FailedEntryCount)
	assert.Len(t, resp.Entries, 2)
	for _, entry := range resp.Entries {
		assert.NotEmpty(t, entry.EventID)
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), log)

	matcher := handler.RouteMatcher()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonEventBridge.PutEvents")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, matcher(c))

	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, matcher(c2))
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonEventBridge.PutRule")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "PutRule", handler.ExtractOperation(c))
}
