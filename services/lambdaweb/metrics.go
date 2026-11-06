package lambdaweb

import (
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const sweepEvery = time.Second

// captureWriter records the status and body size a caller receives.
type captureWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (c *captureWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}

	c.ResponseWriter.WriteHeader(code)
}

func (c *captureWriter) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}

	n, err := c.ResponseWriter.Write(p)
	c.bytes += int64(n)

	return n, err //nolint:wrapcheck // passthrough of the client connection error
}

func (c *captureWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

func (c *captureWriter) code() int {
	if c.status == 0 {
		return http.StatusOK
	}

	return c.status
}

// requestMetrics collects what recordRequest publishes once the request completes.
type requestMetrics struct {
	start    time.Time
	cw       *captureWriter
	lease    *lease
	throttle string
	scope    []string
}

// SetMetricEmitter wires the in-process CloudWatch backend the AWS/Lambda/Web metrics are published to.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) {
	b.mu.Lock("SetMetricEmitter")
	defer b.mu.Unlock()

	b.metrics = e
}

func (b *InMemoryBackend) metricEmitter() cwmetric.Emitter {
	b.mu.RLock("metricEmitter")
	defer b.mu.RUnlock()

	return b.metrics
}

func (b *InMemoryBackend) sweepIfDue() {
	now := b.run.now().UnixNano()
	last := b.run.lastSweep.Load()

	if now-last >= int64(sweepEvery) && b.run.lastSweep.CompareAndSwap(last, now) {
		b.SweepEnvironments()
	}
}

// recordRequest publishes the request metrics and the platform.requestComplete event.
func (b *InMemoryBackend) recordRequest(ep *Endpoint, cw *captureWriter, rm *requestMetrics) {
	em := b.metricEmitter()
	status := cw.code()

	if rm.lease != nil {
		errType := cw.Header().Get(hdrErrorType)
		rec := map[string]any{
			"requestId": cw.Header().Get(hdrRequestID), "endpointName": ep.Name, "result": status,
			keyMetrics: map[string]any{"durationMs": msSince(b.run.now(), rm.start), "responseBytes": cw.bytes},
		}

		if errType != "" {
			rec["errorType"] = errType
		}

		rm.lease.inst.ship.system("platform.requestComplete", levelDebug, rec)
	}

	if em == nil {
		return
	}

	web := []cwmetric.Dimension{
		{Name: "FunctionName", Value: ep.FunctionName}, {Name: "EndpointName", Value: ep.Name},
	}
	usage := append(
		append([]cwmetric.Dimension{}, web...),
		cwmetric.Dimension{Name: "ExecutionRegion", Value: ep.Region},
	)

	switch {
	case status >= http.StatusInternalServerError:
		emitMetric(em, ep.Region, nsWeb, "HTTPResponseStatusClass5XX", unitCount, 1, web)
	case status >= http.StatusBadRequest:
		emitMetric(em, ep.Region, nsWeb, "HTTPResponseStatusClass4XX", unitCount, 1, web)
	}

	if rm.throttle != "" {
		emitMetric(em, ep.Region, nsWebUsage, rm.throttle, unitCount, 1, usage)
	}

	if rm.lease != nil {
		emitMetric(em, ep.Region, nsWeb, "ExecutedRequests", unitCount, 1, web)
		emitMetric(em, ep.Region, nsWebUsage, "ExecutedRequests", unitCount, 1, usage)
	}

	if len(rm.scope) > 0 {
		active := float64(b.run.activeEnvironments(rm.scope))
		emitMetric(em, ep.Region, nsWebUsage, "EndpointActiveEnvironments", unitCount, active, usage)
	}
}
