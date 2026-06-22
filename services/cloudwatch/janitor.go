package cloudwatch

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	cwWorkerService        = "cloudwatch"
	metricSweeper          = "MetricSweeper"
	alarmEvaluator         = "AlarmEvaluator"
	defaultJanitorInterval = 5 * time.Minute
	// alarmEvalInterval is how often the janitor evaluates metric alarm states.
	alarmEvalInterval = 10 * time.Second
)

// Janitor is the CloudWatch background worker that manages metric retention
// and evaluates metric alarm states.
type Janitor struct {
	Backend           *InMemoryBackend
	Interval          time.Duration
	AlarmEvalInterval time.Duration
}

// NewJanitor creates a new CloudWatch Janitor.
func NewJanitor(backend *InMemoryBackend) *Janitor {
	return &Janitor{
		Backend:           backend,
		Interval:          defaultJanitorInterval,
		AlarmEvalInterval: alarmEvalInterval,
	}
}

// Run runs the janitor loop until ctx is cancelled. It runs two independent
// tickers: one that sweeps expired metrics at Interval, and one that evaluates
// metric alarm states at AlarmEvalInterval.
func (j *Janitor) Run(ctx context.Context) {
	evalInterval := j.AlarmEvalInterval
	if evalInterval <= 0 {
		evalInterval = alarmEvalInterval
	}

	g := worker.NewGroup(ctx, cwWorkerService)
	g.Ticker(metricSweeper, j.Interval, 0, j.sweepMetrics)
	g.Ticker(alarmEvaluator, evalInterval, 0, j.evaluateAlarms)

	<-ctx.Done()
	g.Stop()
}

// sweepMetrics evicts expired metrics.
func (j *Janitor) sweepMetrics(_ context.Context) {
	j.Backend.SweepExpiredMetrics()
	telemetry.RecordWorkerTask(cwWorkerService, metricSweeper, "success")
}

// evaluateAlarms re-evaluates metric alarm states as of now.
func (j *Janitor) evaluateAlarms(ctx context.Context) {
	j.Backend.EvaluateAlarms(ctx, time.Now())
	telemetry.RecordWorkerTask(cwWorkerService, alarmEvaluator, "success")
}
