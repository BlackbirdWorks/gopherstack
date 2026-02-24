package eventbridge_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// mockConfigProvider implements config.Provider for testing.
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()
	p := &eventbridge.Provider{}
	assert.Equal(t, "EventBridge", p.Name())
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()
	p := &eventbridge.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockConfigProvider{},
	}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "EventBridge", svc.Name())
}

func TestProvider_Init_WithoutConfig(t *testing.T) {
	t.Parallel()
	p := &eventbridge.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, "EventBridge", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcherCoverage(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), slog.Default())
	matcher := h.RouteMatcher()
	e := echo.New()

	reqMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqMatch.Header.Set("X-Amz-Target", "AmazonEventBridge.CreateEventBus")
	assert.True(t, matcher(e.NewContext(reqMatch, httptest.NewRecorder())))

	reqNoMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqNoMatch.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	assert.False(t, matcher(e.NewContext(reqNoMatch, httptest.NewRecorder())))
}

func TestHandler_ExtractOperationCoverage(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonEventBridge.PutEvents")
	assert.Equal(t, "PutEvents", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))

	reqUnknown := httptest.NewRequest(http.MethodPost, "/", nil)
	assert.Equal(t, "Unknown", h.ExtractOperation(e.NewContext(reqUnknown, httptest.NewRecorder())))
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Name":"my-bus"}`))
	assert.Equal(t, "my-bus", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))

	reqRule := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Rule":"my-rule"}`))
	assert.Equal(t, "my-rule", h.ExtractResource(e.NewContext(reqRule, httptest.NewRecorder())))

	reqEmpty := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	assert.Equal(t, "", h.ExtractResource(e.NewContext(reqEmpty, httptest.NewRecorder())))

	reqBadJSON := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`not-json`))
	assert.Equal(t, "", h.ExtractResource(e.NewContext(reqBadJSON, httptest.NewRecorder())))
}

func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "InvalidTarget")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_NotFoundErrors(t *testing.T) {
	t.Parallel()

	rec := makeRequest(t, "DeleteEventBus", `{"Name":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = makeRequest(t, "DescribeEventBus", `{"Name":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = makeRequest(t, "DeleteRule", `{"Name":"r","EventBusName":"default"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = makeRequest(t, "DescribeRule", `{"Name":"r","EventBusName":"default"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = makeRequest(t, "EnableRule", `{"Name":"r","EventBusName":"default"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = makeRequest(t, "DisableRule", `{"Name":"r","EventBusName":"default"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	// JSON parse errors are mapped to 500 InternalServerError
	rec := makeRequest(t, "CreateEventBus", `not-json`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_ListRulesWithPrefix(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(bk, log)

	// Create two rules
	makeRequestWithHandler(t, h, e, "PutRule",
		`{"Name":"alpha-rule","EventBusName":"default","EventPattern":"{}","State":"ENABLED"}`)
	makeRequestWithHandler(t, h, e, "PutRule",
		`{"Name":"beta-rule","EventBusName":"default","EventPattern":"{}","State":"ENABLED"}`)

	// List with prefix
	rec := makeRequestWithHandler(t, h, e, "ListRules",
		`{"EventBusName":"default","NamePrefix":"alpha"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListEventBusesWithPrefix(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(bk, log)

	makeRequestWithHandler(t, h, e, "CreateEventBus", `{"Name":"prod-bus"}`)
	makeRequestWithHandler(t, h, e, "CreateEventBus", `{"Name":"dev-bus"}`)

	rec := makeRequestWithHandler(t, h, e, "ListEventBuses", `{"NamePrefix":"prod"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListTargetsByRule_RuleNotFound(t *testing.T) {
	t.Parallel()
	// ListTargetsByRule with a nonexistent rule returns empty list (200), not 404
	rec := makeRequest(t, "ListTargetsByRule",
		`{"Rule":"nonexistent","EventBusName":"default"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutEvents_Empty(t *testing.T) {
	t.Parallel()
	rec := makeRequest(t, "PutEvents", `{"Entries":[]}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutRule_MissingName(t *testing.T) {
	t.Parallel()
	rec := makeRequest(t, "PutRule", `{"EventBusName":"default"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateEventBus_AlreadyExists(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	bk := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(bk, log)

	makeRequestWithHandler(t, h, e, "CreateEventBus", `{"Name":"dup-bus"}`)
	rec := makeRequestWithHandler(t, h, e, "CreateEventBus", `{"Name":"dup-bus"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteDefaultBus(t *testing.T) {
	t.Parallel()
	rec := makeRequest(t, "DeleteEventBus", `{"Name":"default"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
