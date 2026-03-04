package dashboard

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	metricsStreamInterval    = 2 * time.Second
	metricsKeepAliveInterval = 15 * time.Second
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
	g.GET("/api/metrics/stream", streamMetricsSSE)

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

// streamMetricsSSE pushes metrics securely to the dashboard ui via Server-Sent Events.
func streamMetricsSSE(c *echo.Context) error {
	c.Response().Header().Set(echo.HeaderContentType, "text/event-stream")
	c.Response().Header().Set(echo.HeaderCacheControl, "no-cache")
	c.Response().Header().Set(echo.HeaderConnection, "keep-alive")

	w := c.Response()
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	encoder := json.NewEncoder(w)

	// Stream updates every 2 seconds matching the console Live API rate

	ticker := time.NewTicker(metricsStreamInterval)
	defer ticker.Stop()

	// Keep alive ping every 15s
	keepAlive := time.NewTicker(metricsKeepAliveInterval)
	defer keepAlive.Stop()

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case <-keepAlive.C:
			if err := sendSSEKeepAlive(w); err != nil {
				return err
			}
		case <-ticker.C:
			if err := sendSSEMetrics(w, encoder); err != nil {
				return err
			}
		}
	}
}

func sendSSEMetrics(w http.ResponseWriter, encoder *json.Encoder) error {
	result := telemetry.CollectMetrics()
	if _, err := w.Write([]byte("data: ")); err != nil {
		return err
	}
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\n\n")); err != nil {
		return err
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	return nil
}

// metricsIndex renders the metrics dashboard page.
func (h *DashboardHandler) metricsIndex(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "Performance Metrics",
		ActiveTab: "metrics",
		Snippet: &SnippetData{
			ID:    "metrics-operations",
			Title: "Using Metrics",
			Cli:   "aws metrics help --endpoint-url http://localhost:8000",
			Go:    "/* Write AWS SDK v2 Code for Metrics */",
			Python: "# Write boto3 code for Metrics\nimport boto3\n" +
				"client = boto3.client('metrics', endpoint_url='http://localhost:8000')",
		},
	}
	h.renderTemplate(w, "metrics.html", data)
}
