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

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cloudwatchlogs.Provider{}
	assert.Equal(t, "CloudWatchLogs", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   config.Provider
		wantName string
	}{
		{
			name:     "WithConfig",
			config:   &mockLogsConfigProvider{},
			wantName: "CloudWatchLogs",
		},
		{
			name: "WithoutConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudwatchlogs.Provider{}
			ctx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, svc.Name())
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{"Match", "Logs_20140328.CreateLogGroup", true},
		{"NoMatch", "AmazonSQS.CreateQueue", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			assert.Equal(t, tt.want, h.RouteMatcher()(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{"WithTarget", "Logs_20140328.PutLogEvents", "PutLogEvents"},
		{"NoTarget", "", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			assert.Equal(t, tt.want, h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name string
		body string
		want string
	}{
		{"LogGroupName", `{"logGroupName":"my-group"}`, "my-group"},
		{"LogStreamName", `{"logStreamName":"my-stream"}`, "my-stream"},
		{"Empty", `{}`, ""},
		{"BadJSON", `not-json`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			assert.Equal(t, tt.want, h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_Coverage_Name(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	assert.Equal(t, "CloudWatchLogs", h.Name())
}

func TestHandler_Coverage_MatchPriority(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_Coverage_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateLogGroup")
	assert.Contains(t, ops, "FilterLogEvents")
}

func TestHandler_Coverage_DescribeLogGroups_Pagination(t *testing.T) {
	t.Parallel()

	e := echo.New()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	for i := range 5 {
		doLogsRequest(t, h, e, "CreateLogGroup",
			`{"logGroupName":"/group/`+string(rune('a'+i))+`"}`)
	}

	rec := doLogsRequest(t, h, e, "DescribeLogGroups", `{"limit":2}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}
