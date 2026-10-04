package stepfunctions

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/States execution metrics per docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
// (dimension StateMachineArn).
const (
	sfnMetricNamespace = "AWS/States"
	sfnUnitCount       = "Count"
	sfnUnitMillis      = "Milliseconds"
	sfnArnRegionIdx    = 3
)

// SetMetricEmitter sets the emitter that publishes AWS/States metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

func (b *InMemoryBackend) putExecMetric(smArn, name, unit string, v float64) {
	region := ""
	if parts := strings.Split(smArn, ":"); len(parts) > sfnArnRegionIdx {
		region = parts[sfnArnRegionIdx]
	}

	b.metrics.Put(region, sfnMetricNamespace, name, unit, v, cwmetric.Dimension{Name: "StateMachineArn", Value: smArn})
}

func (b *InMemoryBackend) emitExecutionStarted(smArn string) {
	if b.metrics.Enabled() {
		b.putExecMetric(smArn, "ExecutionsStarted", sfnUnitCount, 1)
	}
}

// emitExecutionEnded publishes the terminal-status counter and ExecutionTime; start and stop are epoch seconds.
func (b *InMemoryBackend) emitExecutionEnded(smArn, status string, start, stop float64) {
	if !b.metrics.Enabled() {
		return
	}

	var name string

	switch status {
	case statusSucceeded:
		name = "ExecutionsSucceeded"
	case statusFailed:
		name = "ExecutionsFailed"
	case statusAborted:
		name = "ExecutionsAborted"
	case statusTimedOut:
		name = "ExecutionsTimedOut"
	default:
		return
	}

	b.putExecMetric(smArn, name, sfnUnitCount, 1)

	const millisPerSecond = 1000

	b.putExecMetric(smArn, "ExecutionTime", sfnUnitMillis, max(stop-start, 0)*millisPerSecond)
}
