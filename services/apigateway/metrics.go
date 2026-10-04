package apigateway

import (
	"context"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/ApiGateway REST metrics: developerguide/api-gateway-metrics-and-dimensions.html.
const (
	apigwMetricNamespace = "AWS/ApiGateway"
	apigwUnitCount       = "Count"
	apigwUnitMillis      = "Milliseconds"
)

// SetMetricEmitter sets the emitter that publishes AWS/ApiGateway metrics to CloudWatch, for h and its region peers.
func (h *Handler) SetMetricEmitter(e cwmetric.Emitter) {
	h.metrics.Set(e)

	for _, p := range h.peers.All() {
		p.metrics.Set(e)
	}
}

// handleProxyRequest serves a data-plane request, publishing its metrics when an emitter is installed.
func (h *Handler) handleProxyRequest(apiID, stageName string) http.HandlerFunc {
	inner := h.proxyHandler(apiID, stageName)

	return func(w http.ResponseWriter, r *http.Request) {
		if !h.metrics.Enabled() {
			inner(w, r)

			return
		}

		rec := &statusRecorder{ResponseWriter: w}
		obs := &proxyObs{start: time.Now()}
		inner(rec, r.WithContext(context.WithValue(r.Context(), obsKey{}, obs)))
		h.emitProxyMetrics(apiID, r.Method, rec.statusOrOK(), obs)
	}
}

type obsKey struct{}

func obsFrom(ctx context.Context) *proxyObs {
	o, _ := ctx.Value(obsKey{}).(*proxyObs)

	return o
}

// statusRecorder captures the response status of a proxied request.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}

	s.ResponseWriter.WriteHeader(code)
}

// statusOrOK reports the written status; a body written without WriteHeader is an implicit 200.
func (s *statusRecorder) statusOrOK() int {
	if s.status == 0 {
		return http.StatusOK
	}

	return s.status
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// proxyObs collects what a proxied request resolved, for metric dimensions and latency.
type proxyObs struct {
	start       time.Time
	stage       *Stage
	resource    string
	integration time.Duration
	integrated  bool
}

func (o *proxyObs) setStage(s *Stage) {
	if o != nil {
		o.stage = s
	}
}

func (o *proxyObs) setResource(path string) {
	if o != nil {
		o.resource = path
	}
}

func (o *proxyObs) setIntegration(d time.Duration) {
	if o != nil {
		o.integration, o.integrated = d, true
	}
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// emitProxyMetrics publishes the per-request metrics once the stage resolved; unresolved requests carry no stage.
func (h *Handler) emitProxyMetrics(apiID, httpMethod string, status int, o *proxyObs) {
	if o.stage == nil {
		return
	}

	api, err := h.Backend.GetRestAPI(apiID)
	if err != nil {
		return
	}

	if status == 0 {
		status = http.StatusOK
	}

	apiDim := cwmetric.Dimension{Name: "ApiName", Value: api.Name}
	stageDim := cwmetric.Dimension{Name: "Stage", Value: o.stage.StageName}
	sets := [][]cwmetric.Dimension{{apiDim}, {apiDim, stageDim}}

	ms, _ := stageMethodSettingFor(o.stage, o.resource, httpMethod)
	if ms != nil && ms.MetricsEnabled && o.resource != "" {
		sets = append(sets, []cwmetric.Dimension{
			apiDim, {Name: "Method", Value: httpMethod}, {Name: "Resource", Value: o.resource}, stageDim,
		})
	}

	region := h.region
	total := time.Since(o.start)
	clientErr := status >= http.StatusBadRequest && status < http.StatusInternalServerError

	for _, dims := range sets {
		put := func(name, unit string, v float64) {
			h.metrics.Put(region, apigwMetricNamespace, name, unit, v, dims...)
		}

		put("Count", apigwUnitCount, 1)
		put("4XXError", apigwUnitCount, boolFloat(clientErr))
		put("5XXError", apigwUnitCount, boolFloat(status >= http.StatusInternalServerError))
		put("Latency", apigwUnitMillis, millis(total))

		if o.integrated {
			put("IntegrationLatency", apigwUnitMillis, millis(o.integration))
		}
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}

	return 0
}
