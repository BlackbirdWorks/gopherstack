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
	integrationLatKey = "apigatewayv2.integrationLatency"
)

// SetMetricEmitter sets the emitter that publishes AWS/ApiGateway metrics to CloudWatch, for h and its region peers.
func (h *Handler) SetMetricEmitter(e cwmetric.Emitter) {
	h.metrics.Set(e)

	for _, p := range h.peers.All() {
		p.metrics.Set(e)
	}
}

func v2Dims(apiID, stage string) [][]cwmetric.Dimension {
	api := cwmetric.Dimension{Name: "ApiId", Value: apiID}

	return [][]cwmetric.Dimension{{api}, {api, {Name: "Stage", Value: stage}}}
}

func (h *Handler) putV2(apiID, stage, name, unit string, v float64) {
	if !h.metrics.Enabled() {
		return
	}

	for _, dims := range v2Dims(apiID, stage) {
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

	status := recordedStatus(c, err)
	h.putV2(apiID, stage, "Count", v2UnitCount, 1)
	clientErr := status >= http.StatusBadRequest && status < http.StatusInternalServerError
	h.putV2(apiID, stage, "4xx", v2UnitCount, flag(clientErr))
	h.putV2(apiID, stage, "5xx", v2UnitCount, flag(status >= http.StatusInternalServerError))
	h.putV2(apiID, stage, "Latency", v2UnitMillis, float64(time.Since(start))/float64(time.Millisecond))

	if d, ok := c.Get(integrationLatKey).(time.Duration); ok {
		h.putV2(apiID, stage, "IntegrationLatency", v2UnitMillis, float64(d)/float64(time.Millisecond))
	}
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
}

func (h *Handler) wsMetrics(apiID, stage string) wsMetrics {
	return wsMetrics{h: h, api: apiID, stage: stage}
}

func (m wsMetrics) count(name string) {
	m.h.putV2(m.api, m.stage, name, v2UnitCount, 1)
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

	m.h.putV2(m.api, m.stage, "IntegrationLatency", v2UnitMillis, float64(time.Since(start))/float64(time.Millisecond))

	if err != nil {
		m.count("ExecutionError")
	}
}

func (h *Handler) recordIntegrationLatency(c *echo.Context, start time.Time) {
	if h.metrics.Enabled() {
		c.Set(integrationLatKey, time.Since(start))
	}
}
