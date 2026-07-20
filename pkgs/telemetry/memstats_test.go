package telemetry_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStatsMiddleware(t *testing.T) {
	t.Parallel()

	mw := telemetry.MemoryStatsMiddleware(func(c *echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := mw(c)
	require.NoError(t, err)

	stats := rec.Header().Get("X-Gopherstack-Memory-Stats")
	require.NotEmpty(t, stats)
	assert.Contains(t, stats, "Alloc=")
	assert.Contains(t, stats, "TotalAlloc=")
	assert.Contains(t, stats, "Sys=")
	assert.Contains(t, stats, "NumGC=")
}
