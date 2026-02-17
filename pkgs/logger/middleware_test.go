package logger_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"Gopherstack/pkgs/logger"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

func TestEchoMiddleware(t *testing.T) {
	t.Parallel()

	// Create a buffer to capture log output
	var logBuffer bytes.Buffer
	testLogger := slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create Echo instance with logger middleware
	e := echo.New()
	e.Use(logger.EchoMiddleware(testLogger))

	// Add a test handler that uses logger from context
	e.GET("/test", func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)
		log.DebugContext(ctx, "test message", "key", "value")
		return c.String(http.StatusOK, "OK")
	})

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	// Serve request
	e.ServeHTTP(rec, req)

	// Verify response
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Verify logger was injected and used
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "test message")
	assert.Contains(t, logOutput, "key")
	assert.Contains(t, logOutput, "value")
	assert.Contains(t, logOutput, "level=DEBUG")
}
