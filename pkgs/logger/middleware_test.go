package logger_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEchoMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		method         string
		path           string
		logMessage     string
		wantStatus     int
		wantBody       string
		wantLogContain []string
	}{
		{
			name:       "GET request logs debug message with key-value pair",
			method:     http.MethodGet,
			path:       "/test",
			logMessage: "test message",
			wantStatus: http.StatusOK,
			wantBody:   "OK",
			wantLogContain: []string{
				"test message",
				"key",
				"value",
				"level=DEBUG",
			},
		},
		{
			name:       "POST request logs different debug message",
			method:     http.MethodPost,
			path:       "/test",
			logMessage: "post handler called",
			wantStatus: http.StatusOK,
			wantBody:   "OK",
			wantLogContain: []string{
				"post handler called",
				"level=DEBUG",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logBuffer bytes.Buffer
			testLogger := slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			e := echo.New()
			e.Use(logger.EchoMiddleware(testLogger))

			e.GET("/test", func(c *echo.Context) error {
				ctx := c.Request().Context()
				log := logger.Load(ctx)
				log.DebugContext(ctx, tt.logMessage, "key", "value")

				return c.String(http.StatusOK, "OK")
			})
			e.POST("/test", func(c *echo.Context) error {
				ctx := c.Request().Context()
				log := logger.Load(ctx)
				log.DebugContext(ctx, tt.logMessage)

				return c.String(http.StatusOK, "OK")
			})

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantBody, rec.Body.String())

			logOutput := logBuffer.String()
			for _, want := range tt.wantLogContain {
				assert.Contains(t, logOutput, want)
			}
		})
	}
}

func TestMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		method         string
		path           string
		logMessage     string
		wantStatus     int
		wantLogContain []string
	}{
		{
			name:       "GET request logger available in handler context",
			method:     http.MethodGet,
			path:       "/test",
			logMessage: "net/http handler called",
			wantStatus: http.StatusOK,
			wantLogContain: []string{
				"net/http handler called",
			},
		},
		{
			name:       "POST request logger available in handler context",
			method:     http.MethodPost,
			path:       "/test",
			logMessage: "post request handled",
			wantStatus: http.StatusOK,
			wantLogContain: []string{
				"post request handled",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logBuffer bytes.Buffer
			testLogger := slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			mux := http.NewServeMux()
			mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
				log := logger.Load(r.Context())
				log.DebugContext(r.Context(), tt.logMessage)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			})

			handler := logger.Middleware(testLogger)(mux)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantLogContain {
				assert.Contains(t, logBuffer.String(), want)
			}
		})
	}
}
