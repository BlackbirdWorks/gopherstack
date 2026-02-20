package dashboard

import (
	"net/http"

	"Gopherstack/pkgs/telemetry"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterMetricsHandlers registers metrics endpoints under the dashboard group.
func RegisterMetricsHandlers(g *echo.Group, h *DashboardHandler) {
	if g == nil {
		return
	}
	// Prometheus native format (for scraping or direct consumption)
	g.GET("/metrics/raw", echo.WrapHandler(promhttp.Handler()))

	// JSON format for dashboard consumption
	g.GET("/api/metrics", getMetricsJSON)

	// UI Routes
	if h != nil {
		g.GET("/metrics", func(c *echo.Context) error {
			h.metricsIndex(c.Response(), c.Request())

			return nil
		})
		g.GET("/docs", func(c *echo.Context) error {
			h.docIndex(c.Response(), c.Request())

			return nil
		})
	}
}

// getMetricsJSON returns metrics in JSON format for dashboard charts.
func getMetricsJSON(c *echo.Context) error {
	result := telemetry.CollectMetrics()

	return c.JSON(http.StatusOK, result)
}

// metricsIndex renders the metrics dashboard page.
func (h *DashboardHandler) metricsIndex(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "Performance Metrics",
		ActiveTab: "metrics",
	}
	h.renderTemplate(w, "metrics.html", data)
}
