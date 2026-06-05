package cloudwatch

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
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

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	metricTicker := time.NewTicker(j.Interval)
	defer metricTicker.Stop()

	evalInterval := j.AlarmEvalInterval
	if evalInterval <= 0 {
		evalInterval = alarmEvalInterval
	}

	alarmTicker := time.NewTicker(evalInterval)
	defer alarmTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-metricTicker.C:
			j.Backend.SweepExpiredMetrics()
			telemetry.RecordWorkerTask(cwWorkerService, metricSweeper, "success")
		case now := <-alarmTicker.C:
			j.Backend.EvaluateAlarms(ctx, now)
			telemetry.RecordWorkerTask(cwWorkerService, alarmEvaluator, "success")
		}
	}
}
