package dashboard

import (
	"net/http"

	"Gopherstack/pkgs/telemetry"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterMetricsHandlers registers metrics endpoints.
func RegisterMetricsHandlers(e *echo.Echo) {
	// Prometheus native format (for scraping or direct consumption)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	// JSON format for dashboard consumption
	e.GET("/api/metrics", getMetricsJSON)
}

// getMetricsJSON returns metrics in JSON format for dashboard charts.
func getMetricsJSON(c *echo.Context) error {
	result := telemetry.CollectMetrics()

	return c.JSON(http.StatusOK, result)
}

// metricsIndex renders the metrics dashboard page.
func (h *Handler) metricsIndex(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "Performance Metrics",
		ActiveTab: "metrics",
	}
	h.renderTemplate(w, "templates/metrics.html", data)
}
