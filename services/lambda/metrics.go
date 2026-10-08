package lambda

import (
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/Lambda invocation metrics per docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics-types.html.
const (
	lambdaMetricNamespace = "AWS/Lambda"
	lambdaUnitCount       = "Count"
	lambdaUnitMillis      = "Milliseconds"
	millisPerSecondFloat  = 1000.0
)

// SetMetricEmitter sets the emitter that publishes AWS/Lambda metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// SetMetricEmitter installs e on the home backend and every region sibling built so far;
// siblings built later inherit it.
func (h *Handler) SetMetricEmitter(e cwmetric.Emitter) {
	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			mem.SetMetricEmitter(e)
		}
	}
}

// metricDimensionSets returns the dimension combinations Lambda publishes for an invocation:
// FunctionName always, plus Resource (name:qualifier) and, for alias invocations, ExecutedVersion.
func metricDimensionSets(functionName, qualifier, executedVersion string) [][]cwmetric.Dimension {
	fn := cwmetric.Dimension{Name: "FunctionName", Value: functionName}
	sets := [][]cwmetric.Dimension{{fn}}

	if qualifier == "" || qualifier == versionLatest {
		return sets
	}

	resource := cwmetric.Dimension{Name: "Resource", Value: functionName + ":" + qualifier}
	sets = append(sets, []cwmetric.Dimension{fn, resource})

	if _, err := strconv.Atoi(qualifier); err != nil && executedVersion != "" {
		sets = append(sets, []cwmetric.Dimension{
			fn, resource, {Name: "ExecutedVersion", Value: executedVersion},
		})
	}

	return sets
}

func (b *InMemoryBackend) putLambdaMetric(
	sets [][]cwmetric.Dimension, name, unit string, value float64,
) {
	for _, dims := range sets {
		b.metrics.Put(b.region, lambdaMetricNamespace, name, unit, value, dims...)
	}
}

// emitInvocationMetrics publishes Invocations, Errors (when failed) and Duration for one execution attempt.
func (b *InMemoryBackend) emitInvocationMetrics(
	functionName, qualifier, executedVersion string,
	elapsed time.Duration,
	failed bool,
) {
	if !b.metrics.Enabled() {
		return
	}

	sets := metricDimensionSets(functionName, qualifier, executedVersion)

	b.putLambdaMetric(sets, "Invocations", lambdaUnitCount, 1)

	if failed {
		b.putLambdaMetric(sets, "Errors", lambdaUnitCount, 1)
	}

	b.putLambdaMetric(sets, "Duration", lambdaUnitMillis, elapsed.Seconds()*millisPerSecondFloat)
}

// emitThrottleMetric publishes Throttles for an invocation rejected by a concurrency limit.
func (b *InMemoryBackend) emitThrottleMetric(functionName, qualifier, executedVersion string) {
	if !b.metrics.Enabled() {
		return
	}

	b.putLambdaMetric(metricDimensionSets(functionName, qualifier, executedVersion), "Throttles", lambdaUnitCount, 1)
}
