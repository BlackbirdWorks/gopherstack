package codebuild

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultCodeBuildJanitorInterval = time.Minute
	defaultCodeBuildBuildTTL        = 24 * time.Hour

	codebuildWorkerServiceName = "codebuild"
	buildSweeperComponent      = "CompletedBuildSweeper"
)

// isTerminalBuild reports whether the given build status is terminal.
func isTerminalBuild(status string) bool {
	return status == buildStatusSucceeded || status == "FAILED" || status == buildStatusStopped ||
		status == "TIMED_OUT" || status == "FAULT"
}

// Janitor is the CodeBuild background worker that evicts completed builds
// after a configurable TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	BuildTTL time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new CodeBuild Janitor for the given backend.
// Zero values for interval or buildTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, buildTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultCodeBuildJanitorInterval
	}

	if buildTTL == 0 {
		buildTTL = defaultCodeBuildBuildTTL
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
		BuildTTL: buildTTL,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, codebuildWorkerServiceName)
	g.Ticker(
		buildSweeperComponent,
		j.Interval,
		j.TaskTimeout,
		j.sweepCompletedBuilds,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepCompletedBuilds(ctx)
}

// sweepCompletedBuilds removes builds in terminal states whose EndTime is older than BuildTTL.
func (j *Janitor) sweepCompletedBuilds(ctx context.Context) {
	cutoff := float64(time.Now().Add(-j.BuildTTL).Unix())

	j.Backend.mu.Lock("CodeBuildJanitor")

	var swept []string

	// IDs are gathered first, then deleted, since store.Table.Delete mutates
	// the very index groups a live iteration would otherwise be reading.
	for _, build := range j.Backend.builds.All() {
		if isTerminalBuild(build.BuildStatus) && build.EndTime > 0 && build.EndTime < cutoff {
			swept = append(swept, build.ID)
		}
	}

	for _, id := range swept {
		j.Backend.builds.Delete(id)
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(codebuildWorkerServiceName, buildSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(codebuildWorkerServiceName, buildSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "CodeBuild janitor: completed builds evicted", "count", count)
}
