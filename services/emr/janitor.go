package emr

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultJanitorInterval = time.Minute
	defaultTerminatedTTL   = time.Hour

	clusterSweeperComponent  = "TerminatedClusterCleaner"
	janitorWorkerServiceName = "emr"
)

// Janitor is the EMR background worker that sweeps terminated clusters after
// a configurable TTL, matching the AWS behavior where terminated clusters
// remain visible for approximately one hour.
type Janitor struct {
	Backend       *InMemoryBackend
	Interval      time.Duration
	TerminatedTTL time.Duration
}

// NewJanitor creates a new EMR Janitor for the given backend.
// If interval or terminatedTTL are zero, defaults are used.
func NewJanitor(backend *InMemoryBackend, interval, terminatedTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	if terminatedTTL == 0 {
		terminatedTTL = defaultTerminatedTTL
	}

	return &Janitor{
		Backend:       backend,
		Interval:      interval,
		TerminatedTTL: terminatedTTL,
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
			j.sweepTerminatedClusters(ctx)
		}
	}
}

// sweepTerminatedClusters removes clusters that have been in the terminated
// state longer than TerminatedTTL.
func (j *Janitor) sweepTerminatedClusters(ctx context.Context) {
	cutoff := time.Now().Add(-j.TerminatedTTL)

	j.Backend.mu.Lock("sweepTerminatedClusters")

	var swept []string

	for id, c := range j.Backend.clusters {
		terminal := c.Status.State == StateTerminated || c.Status.State == StateTerminatedWithErrors
		if terminal && !c.TerminatedAt.IsZero() && c.TerminatedAt.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.clusters, id)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(janitorWorkerServiceName, clusterSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorWorkerServiceName, clusterSweeperComponent, count)

	for _, id := range swept {
		logger.Load(ctx).InfoContext(ctx, "EMR janitor: terminated cluster swept", "clusterID", id)
	}
}
