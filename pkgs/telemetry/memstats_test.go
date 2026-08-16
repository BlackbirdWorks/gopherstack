package telemetry_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

func TestMemoryStatsMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantContains []string
	}{
		{
			name:   "returns_memory_stats_in_header",
			method: http.MethodGet,
			path:   "/",
			wantContains: []string{
				"Alloc=",
				"TotalAlloc=",
				"Sys=",
				"NumGC=",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mw := telemetry.MemoryStatsMiddleware(func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := mw(c)
			require.NoError(t, err)

			stats := rec.Header().Get("X-Gopherstack-Memory-Stats")
			require.NotEmpty(t, stats)

			for _, want := range tt.wantContains {
				assert.Contains(t, stats, want)
			}
		})
	}
}
