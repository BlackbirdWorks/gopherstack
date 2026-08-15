package emr

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
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
	TaskTimeout   time.Duration
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
	g := worker.NewGroup(ctx, janitorWorkerServiceName)
	g.Ticker(
		clusterSweeperComponent,
		j.Interval,
		j.TaskTimeout,
		j.sweepTerminatedClusters,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepTerminatedClusters(ctx)
}

// sweepTerminatedClusters removes clusters that have been in the terminated
// state longer than TerminatedTTL.
func (j *Janitor) sweepTerminatedClusters(ctx context.Context) {
	cutoff := time.Now().Add(-j.TerminatedTTL)

	j.Backend.mu.Lock("sweepTerminatedClusters")

	var swept []string

	// Clusters live in a single flat store.Table keyed by "region|id" (see
	// regionKey in backend.go). store.Table.Snapshot allocates a fresh slice
	// on every call, so deleting entries while ranging over it here is safe
	// -- unlike store.Index.Get, whose slice is owned by the index itself.
	for _, c := range j.Backend.clusters.Snapshot() {
		terminal := c.Status.State == StateTerminated || c.Status.State == StateTerminatedWithErrors
		if terminal && !c.terminatedAt.IsZero() && c.terminatedAt.Before(cutoff) {
			swept = append(swept, c.ID)
			j.Backend.clusterDelete(c.region, c.ID)
			if arnIndex := j.Backend.arnIndex[c.region]; arnIndex != nil {
				delete(arnIndex, c.ARN)
			}
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
