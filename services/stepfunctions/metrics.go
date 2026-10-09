package stepfunctions

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// AWS/States execution metrics per docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
// (dimension StateMachineArn).
const (
	sfnMetricNamespace   = "AWS/States"
	sfnUnitCount         = "Count"
	sfnUnitMillis        = "Milliseconds"
	sfnArnRegionIdx      = 3
	sfnTaskResourceParts = 6
	awsServiceLambda     = "lambda"
	errCodeStatesTimeout = "States.Timeout"
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

type taskMetricNames struct {
	dimension, scheduled, started, succeeded, failed, timedOut, runTime, scheduleTime, total string
}

// taskMetricsFor picks the AWS/States metric family for a Task Resource: activities, direct
// Lambda ARNs, or service integrations (procedure-cw-metrics.html).
func taskMetricsFor(resource string) (taskMetricNames, bool) {
	parts := strings.Split(resource, ":")
	if len(parts) < sfnTaskResourceParts || parts[0] != arnPrefix {
		return taskMetricNames{}, false
	}

	switch {
	case parts[2] == awsServiceStates && parts[3] == "" && parts[4] == "":
		return taskMetricNames{
			dimension: "ServiceIntegrationResourceArn", scheduled: "ServiceIntegrationsScheduled",
			started: "ServiceIntegrationsStarted", succeeded: "ServiceIntegrationsSucceeded",
			failed: "ServiceIntegrationsFailed", timedOut: "ServiceIntegrationsTimedOut",
			runTime: "ServiceIntegrationRunTime", scheduleTime: "ServiceIntegrationScheduleTime",
			total: "ServiceIntegrationTime",
		}, true
	case parts[2] == awsServiceStates && parts[5] == resourceSegmentActivity:
		return taskMetricNames{
			dimension: "ActivityArn", scheduled: "ActivitiesScheduled", started: "ActivitiesStarted",
			succeeded: "ActivitiesSucceeded", failed: "ActivitiesFailed", timedOut: "ActivitiesTimedOut",
			runTime: "ActivityRunTime", scheduleTime: "ActivityScheduleTime", total: "ActivityTime",
		}, true
	case parts[2] == awsServiceLambda && parts[5] == "function":
		return taskMetricNames{
			dimension: "LambdaFunctionArn", scheduled: "LambdaFunctionsScheduled", started: "LambdaFunctionsStarted",
			succeeded: "LambdaFunctionsSucceeded", failed: "LambdaFunctionsFailed", timedOut: "LambdaFunctionsTimedOut",
			runTime: "LambdaFunctionRunTime", scheduleTime: "LambdaFunctionScheduleTime", total: "LambdaFunctionTime",
		}, true
	}

	return taskMetricNames{}, false
}

func isTaskTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, asl.ErrStatesTimeout) {
		return true
	}

	var fe *asl.FailError

	return errors.As(err, &fe) && (fe.ErrCode == errCodeStatesTimeout || fe.ErrCode == "States.HeartbeatTimeout")
}

// RecordTaskAttempt publishes the activity, Lambda or service-integration metrics for one Task attempt.
func (r *historyRecorder) RecordTaskAttempt(
	execARN, resource string, scheduledAt, startedAt, endedAt time.Time, err error,
) {
	b := r.backend
	if !b.metrics.Enabled() || errors.Is(err, context.Canceled) {
		return
	}

	names, ok := taskMetricsFor(resource)
	if !ok {
		return
	}

	region := regionFromARN(execARN, b.region)
	dim := cwmetric.Dimension{Name: names.dimension, Value: resource}
	put := func(name, unit string, v float64) { b.metrics.Put(region, sfnMetricNamespace, name, unit, v, dim) }
	millis := func(d time.Duration) float64 { return max(float64(d.Milliseconds()), 0) }

	put(names.scheduled, sfnUnitCount, 1)
	put(names.started, sfnUnitCount, 1)

	switch {
	case err == nil:
		put(names.succeeded, sfnUnitCount, 1)
	case isTaskTimeout(err):
		put(names.timedOut, sfnUnitCount, 1)
	default:
		put(names.failed, sfnUnitCount, 1)
	}

	put(names.scheduleTime, sfnUnitMillis, millis(startedAt.Sub(scheduledAt)))
	put(names.runTime, sfnUnitMillis, millis(endedAt.Sub(startedAt)))
	put(names.total, sfnUnitMillis, millis(endedAt.Sub(scheduledAt)))
}
