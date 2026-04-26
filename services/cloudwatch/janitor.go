package cloudwatch

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	cwWorkerService        = "cloudwatch"
	metricSweeper          = "MetricSweeper"
	defaultJanitorInterval = 5 * time.Minute
)

// Janitor is the CloudWatch background worker that manages metric retention.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new CloudWatch Janitor.
func NewJanitor(backend *InMemoryBackend) *Janitor {
	return &Janitor{
		Backend:  backend,
		Interval: defaultJanitorInterval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.Backend.SweepExpiredMetrics()
			telemetry.RecordWorkerTask(cwWorkerService, metricSweeper, "success")
		}
	}
}
