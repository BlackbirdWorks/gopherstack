package cloudwatchlogs_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// mockLogsConfigProvider implements config.Provider for testing.
type mockLogsConfigProvider struct{}

func (m *mockLogsConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_Name_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	p := &cloudwatchlogs.Provider{}
	assert.Equal(t, "CloudWatchLogs", p.Name())
}

func TestProvider_Init_WithConfig_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	p := &cloudwatchlogs.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockLogsConfigProvider{},
	}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "CloudWatchLogs", svc.Name())
}

func TestProvider_Init_WithoutConfig_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	p := &cloudwatchlogs.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestHandler_Name_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, "CloudWatchLogs", h.Name())
}

func TestHandler_MatchPriority_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcher_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	matcher := h.RouteMatcher()
	e := echo.New()

	reqMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqMatch.Header.Set("X-Amz-Target", "Logs_20140328.CreateLogGroup")
	assert.True(t, matcher(e.NewContext(reqMatch, httptest.NewRecorder())))

	reqNoMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqNoMatch.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	assert.False(t, matcher(e.NewContext(reqNoMatch, httptest.NewRecorder())))
}

func TestHandler_ExtractOperation_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Logs_20140328.PutLogEvents")
	assert.Equal(t, "PutLogEvents", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))

	reqUnknown := httptest.NewRequest(http.MethodPost, "/", nil)
	assert.Equal(t, "Unknown", h.ExtractOperation(e.NewContext(reqUnknown, httptest.NewRecorder())))
}

func TestHandler_ExtractResource_CloudWatchLogs(t *testing.T) {
	t.Parallel()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"logGroupName":"my-group"}`))
	assert.Equal(t, "my-group", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))

	reqStream := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"logStreamName":"my-stream"}`))
	assert.Equal(t, "my-stream", h.ExtractResource(e.NewContext(reqStream, httptest.NewRecorder())))

	reqEmpty := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	assert.Empty(t, h.ExtractResource(e.NewContext(reqEmpty, httptest.NewRecorder())))

	reqBadJSON := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`not-json`))
	assert.Empty(t, h.ExtractResource(e.NewContext(reqBadJSON, httptest.NewRecorder())))
}

func TestHandler_GetSupportedOperations_Coverage(t *testing.T) {
	t.Parallel()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateLogGroup")
	assert.Contains(t, ops, "FilterLogEvents")
}

func TestHandler_DescribeLogGroups_Pagination_Coverage(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), log)

	for i := range 5 {
		doLogsRequest(t, h, e, "CreateLogGroup",
			`{"logGroupName":"/group/`+string(rune('a'+i))+`"}`)
	}

	rec := doLogsRequest(t, h, e, "DescribeLogGroups", `{"limit":2}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}
