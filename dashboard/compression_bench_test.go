package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

type discardResponse struct{ h http.Header }

func (w *discardResponse) Header() http.Header         { return w.h }
func (w *discardResponse) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardResponse) WriteHeader(int)             {}

func benchRequest(b *testing.B, h echo.HandlerFunc, e *echo.Echo, p, ae string, fresh func() echo.HandlerFunc) {
	b.Helper()
	b.ReportAllocs()

	req := httptest.NewRequest(http.MethodGet, p, nil)
	if ae != "" {
		req.Header.Set("Accept-Encoding", ae)
	}

	w := &discardResponse{h: http.Header{}}

	for b.Loop() {
		clear(w.h)

		hh := h
		if fresh != nil {
			hh = fresh()
		}

		require.NoError(b, hh(e.NewContext(req, w)))
	}
}

func BenchmarkDashboardAsset(b *testing.B) {
	e := echo.New()
	h := dashboard.NewHandler(dashboard.Config{})
	cached := dashboard.CompressionMiddleware()(h.Handler())
	asset := "/dashboard/static/app.js"

	b.Run("identity", func(b *testing.B) { benchRequest(b, cached, e, asset, "", nil) })

	for _, enc := range []string{"gzip", "br", "zstd"} {
		b.Run("first_"+enc, func(b *testing.B) {
			benchRequest(b, nil, e, asset, enc, func() echo.HandlerFunc {
				return dashboard.CompressionMiddleware()(h.Handler())
			})
		})
		b.Run("cached_"+enc, func(b *testing.B) {
			benchRequest(b, cached, e, asset, enc, nil)
		})
	}
}

func BenchmarkDashboardJSON(b *testing.B) {
	e := echo.New()
	rows := make([]map[string]string, 4000)

	for i := range rows {
		rows[i] = map[string]string{
			"name":  "resource-" + strings.Repeat("x", i%23),
			"arn":   "arn:aws:s3:::bucket",
			"state": "ACTIVE",
		}
	}

	h := dashboard.CompressionMiddleware()(func(c *echo.Context) error { return c.JSON(http.StatusOK, rows) })

	b.Run("identity", func(b *testing.B) { benchRequest(b, h, e, "/dashboard/api/x", "", nil) })

	for _, enc := range []string{"gzip", "br", "zstd"} {
		b.Run(enc, func(b *testing.B) { benchRequest(b, h, e, "/dashboard/api/x", enc, nil) })
	}
}
