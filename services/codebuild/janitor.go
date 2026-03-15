package codebuild

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultCodeBuildJanitorInterval = time.Minute
	defaultCodeBuildBuildTTL        = 24 * time.Hour

	codebuildWorkerServiceName = "codebuild"
	buildSweeperComponent      = "CompletedBuildSweeper"
)

// isTerminalBuild reports whether the given build status is terminal.
func isTerminalBuild(status string) bool {
	return status == "SUCCEEDED" || status == "FAILED" || status == "STOPPED" ||
		status == "TIMED_OUT" || status == "FAULT"
}

// Janitor is the CodeBuild background worker that evicts completed builds
// after a configurable TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	BuildTTL time.Duration
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.sweepCompletedBuilds(ctx)
		}
	}
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

	for id, build := range j.Backend.builds {
		if isTerminalBuild(build.BuildStatus) && build.EndTime > 0 && build.EndTime < cutoff {
			swept = append(swept, id)
			delete(j.Backend.buildARNIndex, build.Arn)
			delete(j.Backend.builds, id)
			if proj := j.Backend.buildsByProject[build.ProjectName]; proj != nil {
				delete(proj, id)
				if len(proj) == 0 {
					delete(j.Backend.buildsByProject, build.ProjectName)
				}
			}
		}
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
