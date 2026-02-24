package stepfunctions_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/stepfunctions"
)

// mockSFNConfig implements config.Provider for testing.
type mockSFNConfig struct{}

func (m *mockSFNConfig) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()
	p := &stepfunctions.Provider{}
	assert.Equal(t, "StepFunctions", p.Name())
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()
	p := &stepfunctions.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockSFNConfig{},
	}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "StepFunctions", svc.Name())
}

func TestProvider_Init_WithoutConfig(t *testing.T) {
	t.Parallel()
	p := &stepfunctions.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcherCoverage(t *testing.T) {
	t.Parallel()
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), slog.Default())
	matcher := h.RouteMatcher()
	e := echo.New()

	reqMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqMatch.Header.Set("X-Amz-Target", "AmazonStates.CreateStateMachine")
	assert.True(t, matcher(e.NewContext(reqMatch, httptest.NewRecorder())))

	reqNoMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqNoMatch.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	assert.False(t, matcher(e.NewContext(reqNoMatch, httptest.NewRecorder())))
}

func TestHandler_ExtractOperationCoverage(t *testing.T) {
	t.Parallel()
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonStates.ListStateMachines")
	assert.Equal(t, "ListStateMachines", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))

	reqUnknown := httptest.NewRequest(http.MethodPost, "/", nil)
	assert.Equal(t, "Unknown", h.ExtractOperation(e.NewContext(reqUnknown, httptest.NewRecorder())))
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend(), slog.Default())
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	_ = log

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/json")

	// name field
	reqName := httptest.NewRequest(http.MethodPost, "/",
		stringReader(`{"name":"my-sm"}`))
	assert.Equal(t, "my-sm", h.ExtractResource(e.NewContext(reqName, httptest.NewRecorder())))

	// stateMachineArn field
	reqArn := httptest.NewRequest(http.MethodPost, "/",
		stringReader(`{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:test"}`))
	assert.Equal(t, "arn:aws:states:us-east-1:123:stateMachine:test",
		h.ExtractResource(e.NewContext(reqArn, httptest.NewRecorder())))

	// executionArn field
	reqExec := httptest.NewRequest(http.MethodPost, "/",
		stringReader(`{"executionArn":"arn:aws:states:us-east-1:123:execution:test:exec1"}`))
	assert.Equal(t, "arn:aws:states:us-east-1:123:execution:test:exec1",
		h.ExtractResource(e.NewContext(reqExec, httptest.NewRecorder())))

	// empty body
	reqEmpty := httptest.NewRequest(http.MethodPost, "/", stringReader(`{}`))
	assert.Empty(t, h.ExtractResource(e.NewContext(reqEmpty, httptest.NewRecorder())))

	// bad JSON
	reqBad := httptest.NewRequest(http.MethodPost, "/", stringReader(`not-json`))
	assert.Empty(t, h.ExtractResource(e.NewContext(reqBad, httptest.NewRecorder())))
}

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
