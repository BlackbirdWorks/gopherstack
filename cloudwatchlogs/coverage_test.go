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

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				p := &cloudwatchlogs.Provider{}
				assert.Equal(t, "CloudWatchLogs", p.Name())
			},
		},
		{
			name: "Init/WithConfig",
			run: func(t *testing.T) {
				p := &cloudwatchlogs.Provider{}
				ctx := &service.AppContext{
					Logger: slog.Default(),
					Config: &mockLogsConfigProvider{},
				}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				require.NotNil(t, svc)
				assert.Equal(t, "CloudWatchLogs", svc.Name())
			},
		},
		{
			name: "Init/WithoutConfig",
			run: func(t *testing.T) {
				p := &cloudwatchlogs.Provider{}
				ctx := &service.AppContext{Logger: slog.Default()}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				require.NotNil(t, svc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
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

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
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

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
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

func TestHandler_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
				assert.Equal(t, "CloudWatchLogs", h.Name())
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
				assert.Equal(t, 100, h.MatchPriority())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), slog.Default())
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "CreateLogGroup")
				assert.Contains(t, ops, "FilterLogEvents")
			},
		},
		{
			name: "DescribeLogGroups/Pagination",
			run: func(t *testing.T) {
				e := echo.New()
				log := logger.NewLogger(slog.LevelDebug)
				h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend(), log)

				for i := range 5 {
					doLogsRequest(t, h, e, "CreateLogGroup",
						`{"logGroupName":"/group/`+string(rune('a'+i))+`"}`)
				}

				rec := doLogsRequest(t, h, e, "DescribeLogGroups", `{"limit":2}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
