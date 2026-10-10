package apigatewayv2

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/ApiGateway HTTP and WebSocket metrics (ApiId, Stage): developerguide/http-api-metrics.html and
// apigateway-websocket-api-logging.html.
const (
	v2MetricNamespace = "AWS/ApiGateway"
	v2UnitCount       = "Count"
	v2UnitMillis      = "Milliseconds"
	v2UnitBytes       = "Bytes"
	integrationLatKey = "apigatewayv2.integrationLatency"
	routeKeyCtxKey    = "apigatewayv2.routeKey"
)

// SetMetricEmitter sets the emitter that publishes AWS/ApiGateway metrics to CloudWatch, for h and its region peers.
func (h *Handler) SetMetricEmitter(e cwmetric.Emitter) {
	h.metrics.Set(e)

	for _, p := range h.peers.All() {
		p.metrics.Set(e)
	}
}

// v2Dims returns the dimension sets for a metric; a non-empty route adds the
// ApiId+Stage+Route set published when the route has detailed metrics enabled.
func v2Dims(apiID, stage, route string) [][]cwmetric.Dimension {
	api := cwmetric.Dimension{Name: "ApiId", Value: apiID}
	st := cwmetric.Dimension{Name: "Stage", Value: stage}
	sets := [][]cwmetric.Dimension{{api}, {api, st}}

	if route != "" {
		sets = append(sets, []cwmetric.Dimension{api, st, {Name: "Route", Value: route}})
	}

	return sets
}

// detailedRoute returns routeKey when the stage enables DetailedMetricsEnabled for it, else "".
func (h *Handler) detailedRoute(apiID, stageName, routeKey string) string {
	stage := h.lookupStage(apiID, stageName)
	if stage == nil || routeKey == "" {
		return ""
	}

	if rs := routeSettingsFor(stage, routeKey); rs != nil && rs.DetailedMetricsEnabled {
		return routeKey
	}

	return ""
}

func (h *Handler) putV2Route(apiID, stage, route, name, unit string, v float64) {
	if !h.metrics.Enabled() {
		return
	}

	for _, dims := range v2Dims(apiID, stage, route) {
		h.metrics.Put(h.region, v2MetricNamespace, name, unit, v, dims...)
	}
}

func recordedStatus(c *echo.Context, err error) int {
	if err != nil {
		if he, ok := errors.AsType[*echo.HTTPError](err); ok {
			return he.Code
		}

		return http.StatusInternalServerError
	}

	if resp, rerr := echo.UnwrapResponse(c.Response()); rerr == nil && resp.Status != 0 {
		return resp.Status
	}

	return http.StatusOK
}

func (h *Handler) emitHTTPMetrics(c *echo.Context, apiID, stage string, start time.Time, err error) {
	if !h.metrics.Enabled() {
		return
	}

	route := ""
	if rk, ok := c.Get(routeKeyCtxKey).(string); ok {
		route = h.detailedRoute(apiID, stage, rk)
	}

	put := func(name, unit string, v float64) { h.putV2Route(apiID, stage, route, name, unit, v) }

	status := recordedStatus(c, err)
	put("Count", v2UnitCount, 1)
	clientErr := status >= http.StatusBadRequest && status < http.StatusInternalServerError
	put("4xx", v2UnitCount, flag(clientErr))
	put("5xx", v2UnitCount, flag(status >= http.StatusInternalServerError))
	put("Latency", v2UnitMillis, float64(time.Since(start))/float64(time.Millisecond))
	put("DataProcessed", v2UnitBytes, float64(dataProcessed(c)))

	if d, ok := c.Get(integrationLatKey).(time.Duration); ok {
		put("IntegrationLatency", v2UnitMillis, float64(d)/float64(time.Millisecond))
	}
}

// dataProcessed is the request body plus response body size in bytes.
func dataProcessed(c *echo.Context) int64 {
	n := max(c.Request().ContentLength, 0)

	if resp, err := echo.UnwrapResponse(c.Response()); err == nil {
		n += resp.Size
	}

	return n
}

func flag(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// wsMetrics publishes the WebSocket API metrics for one API and stage.
type wsMetrics struct {
	h     *Handler
	api   string
	stage string
	route string
}

func (h *Handler) wsMetrics(apiID, stage string) wsMetrics {
	return wsMetrics{h: h, api: apiID, stage: stage}
}

// forRoute returns m scoped to routeKey, adding the Route dimension when the stage enables detailed metrics for it.
func (m wsMetrics) forRoute(routeKey string) wsMetrics {
	m.route = m.h.detailedRoute(m.api, m.stage, routeKey)

	return m
}

func (m wsMetrics) count(name string) {
	m.h.putV2Route(m.api, m.stage, m.route, name, v2UnitCount, 1)
}

func (m wsMetrics) connect() {
	m.count("ConnectCount")
	m.count("Count")
}

func (m wsMetrics) message() {
	m.count("MessageCount")
}

func (m wsMetrics) clientError() {
	m.count("ClientError")
}

func (m wsMetrics) routed(err error, start time.Time) {
	if !m.h.metrics.Enabled() {
		return
	}

	m.h.putV2Route(m.api, m.stage, m.route, "IntegrationLatency", v2UnitMillis,
		float64(time.Since(start))/float64(time.Millisecond))

	if err != nil {
		m.count("ExecutionError")
	}

	if errors.Is(err, ErrIntegrationInvoke) {
		m.count("IntegrationError")
	}
}

func (h *Handler) recordIntegrationLatency(c *echo.Context, start time.Time) {
	if h.metrics.Enabled() {
		c.Set(integrationLatKey, time.Since(start))
	}
}
