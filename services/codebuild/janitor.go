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
	buildAdvancerComponent     = "InProgressBuildAdvancer"
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
		j.tick,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single janitor pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.tick(ctx)
}

// tick performs one full janitor pass: evict completed builds past their TTL,
// then advance any still-IN_PROGRESS builds/build batches toward a terminal
// state (see advanceInProgressBuilds).
func (j *Janitor) tick(ctx context.Context) {
	j.sweepCompletedBuilds(ctx)
	j.advanceInProgressBuilds(ctx)
}

// sweepCompletedBuilds removes builds in terminal states whose EndTime is older than BuildTTL.
func (j *Janitor) sweepCompletedBuilds(ctx context.Context) {
	cutoff := float64(time.Now().Add(-j.BuildTTL).Unix())

	var swept []string

	func() {
		j.Backend.mu.Lock("CodeBuildJanitor")
		defer j.Backend.mu.Unlock()

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
	}()

	count := len(swept)

	telemetry.RecordWorkerTask(codebuildWorkerServiceName, buildSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(codebuildWorkerServiceName, buildSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "CodeBuild janitor: completed builds evicted", "count", count)
}

// advanceInProgressBuilds transitions builds and build batches that are still
// IN_PROGRESS to a terminal SUCCEEDED state.
//
// Real CodeBuild builds run asynchronously against a fleet and eventually
// complete on their own; StartBuild/StartBuildBatch only ever set the initial
// IN_PROGRESS status. Without this advancement, nothing in the backend ever
// mutates that status again (StopBuild/StopBuildBatch require an explicit
// caller action), so a build would sit at IN_PROGRESS forever and any client
// polling BatchGetBuilds/BatchGetBuildBatches to wait for completion would
// spin indefinitely.
func (j *Janitor) advanceInProgressBuilds(ctx context.Context) {
	now := float64(time.Now().Unix())

	buildIDs, batchIDs := j.collectInProgressIDs()

	if len(buildIDs) == 0 && len(batchIDs) == 0 {
		return
	}

	j.advanceBuildsLocked(buildIDs, batchIDs, now)

	count := len(buildIDs) + len(batchIDs)

	telemetry.RecordWorkerTask(codebuildWorkerServiceName, buildAdvancerComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(codebuildWorkerServiceName, buildAdvancerComponent, count)

	logger.Load(ctx).InfoContext(ctx, "CodeBuild janitor: in-progress builds advanced to SUCCEEDED",
		"builds", len(buildIDs), "buildBatches", len(batchIDs))
}

// collectInProgressIDs returns the IDs of builds and build batches currently
// IN_PROGRESS, gathered under the backend read lock.
func (j *Janitor) collectInProgressIDs() ([]string, []string) {
	j.Backend.mu.RLock("CodeBuildJanitorAdvanceRead")
	defer j.Backend.mu.RUnlock()

	var buildIDs, batchIDs []string

	for _, build := range j.Backend.builds.All() {
		if build.BuildStatus == buildStatusInProgress {
			buildIDs = append(buildIDs, build.ID)
		}
	}

	for _, batch := range j.Backend.buildBatches.All() {
		if batch.BuildBatchStatus == buildStatusInProgress {
			batchIDs = append(batchIDs, batch.ID)
		}
	}

	return buildIDs, batchIDs
}

// advanceBuildsLocked transitions the given builds and build batches to
// SUCCEEDED, under the backend write lock.
func (j *Janitor) advanceBuildsLocked(buildIDs, batchIDs []string, now float64) {
	j.Backend.mu.Lock("CodeBuildJanitorAdvance")
	defer j.Backend.mu.Unlock()

	for _, id := range buildIDs {
		if build, ok := j.Backend.builds.Get(id); ok && build.BuildStatus == buildStatusInProgress {
			build.BuildStatus = buildStatusSucceeded
			build.EndTime = now
			build.CurrentPhase = phaseCompleted
			build.BuildComplete = true
		}
	}

	for _, id := range batchIDs {
		if batch, ok := j.Backend.buildBatches.Get(id); ok && batch.BuildBatchStatus == buildStatusInProgress {
			batch.BuildBatchStatus = buildStatusSucceeded
			batch.EndTime = now
		}
	}
}
