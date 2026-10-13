package codepipeline

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	buildStatusInProgress = "IN_PROGRESS"
	buildStatusSucceeded  = "SUCCEEDED"
	buildPollInterval     = 500 * time.Millisecond
	buildPollComponent    = "BuildActionPoller"
)

func isS3SourceAction(a Action) bool {
	return isBuiltinAction(a, actionProviderS3) && a.ActionTypeID.Category == actionCategorySource
}

func isS3DeployAction(a Action) bool {
	return isBuiltinAction(a, actionProviderS3) && a.ActionTypeID.Category == actionCategoryDeploy
}

// pipelineOf returns the pipeline named name in region, or an empty Pipeline. Callers must hold b.mu.
func (b *InMemoryBackend) pipelineOf(region, name string) *Pipeline {
	if p, ok := b.pipelines.Get(regionKey(region, name)); ok {
		return p
	}

	return &Pipeline{}
}

// runCodeBuildAction starts a build for a Build/CodeBuild action's ProjectName. A missing
// ProjectName succeeds (nothing to call); a StartBuild error fails the action. When the
// starter is a CodeBuildRunner the action stays InProgress until reconcileBuildActions
// sees the build reach a final status.
func (b *InMemoryBackend) runCodeBuildAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	projectName := action.Configuration[configKeyProjectName]
	if projectName == "" {
		return
	}

	runner, ok := b.codeBuildBackend.(CodeBuildRunner)
	if !ok {
		if err := b.codeBuildBackend.StartBuild(ctx, projectName); err != nil {
			failActionf(ae, failureJobFailed, "%v", err)
		}

		return
	}

	req := BuildRequest{ProjectName: projectName, BuildspecOverride: action.Configuration["BuildspecOverride"]}

	if in := primaryInputArtifact(action); in != "" {
		if bucket, key, found := artifactLocation(p, action, execID, in); found {
			req.SourceLocation = bucket + "/" + key
		}
	}

	buildID, err := runner.RunBuild(ctx, req)
	if err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return
	}

	ae.ExternalExecutionID = buildID
	ae.Status = statusInProgress
}

// pendingBuild identifies an InProgress Build action execution awaiting its build.
type pendingBuild struct {
	ae               *ActionExecution
	region, pipeline string
}

func (b *InMemoryBackend) pendingBuildsLocked() []pendingBuild {
	var out []pendingBuild

	for region, byPipeline := range b.actionExecutions {
		for name, aes := range byPipeline {
			for _, ae := range aes {
				if ae.Status == statusInProgress && ae.Token == "" && ae.ExternalExecutionID != "" &&
					b.isBuildAction(region, name, ae) {
					out = append(out, pendingBuild{region: region, pipeline: name, ae: ae})
				}
			}
		}
	}

	return out
}

func (b *InMemoryBackend) isBuildAction(region, pipelineName string, ae *ActionExecution) bool {
	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return false
	}

	stage := findStage(p, ae.StageName)
	if stage == nil {
		return false
	}

	action := findAction(stage, ae.ActionName)

	return action != nil && isBuiltinAction(*action, actionProviderCodeBuild)
}

// reconcileBuildActions polls the build behind every InProgress Build action and, once a
// build is final, completes the action and resumes its pipeline execution.
func (b *InMemoryBackend) reconcileBuildActions(_ context.Context) {
	b.mu.RLock("reconcileBuildActionsScan")
	_, isRunner := b.codeBuildBackend.(CodeBuildRunner)
	pending := b.pendingBuildsLocked()
	b.mu.RUnlock()

	if !isRunner || len(pending) == 0 {
		return
	}

	b.mu.Lock("reconcileBuildActions")
	defer b.mu.Unlock()

	runner, ok := b.codeBuildBackend.(CodeBuildRunner)
	if !ok {
		return
	}

	for _, pb := range b.pendingBuildsLocked() {
		b.settleBuild(runner, pb)
	}
}

func (b *InMemoryBackend) settleBuild(runner CodeBuildRunner, pb pendingBuild) {
	ctx := b.regionContext(pb.region)
	ae := pb.ae

	status, err := runner.BuildStatus(ctx, ae.ExternalExecutionID)
	if err == nil && status == buildStatusInProgress {
		return
	}

	p, ok := b.pipelines.Get(regionKey(pb.region, pb.pipeline))
	if !ok {
		return
	}

	ae.LastUpdateTime = time.Now().UTC()

	switch {
	case err != nil:
		failActionf(ae, failureJobFailed, "Unable to read build %s: %v", ae.ExternalExecutionID, err)
	case status != buildStatusSucceeded:
		failActionf(ae, failureJobFailed, "Build %s terminated with status %s", ae.ExternalExecutionID, status)
	default:
		ae.Status = statusSucceeded

		if perr := b.passThroughArtifacts(ctx, p, ae.PipelineExecutionID, *findAction(
			findStage(p, ae.StageName), ae.ActionName)); perr != nil {
			failActionf(ae, failureJobFailed, "%v", perr)
		}
	}

	exec := findExecution(b.executionsStore(pb.region)[pb.pipeline], ae.PipelineExecutionID)
	if exec == nil || exec.Status != statusInProgress {
		return
	}

	b.runPipelineActions(pb.region, p, exec)
	exec.LastUpdateTime = ae.LastUpdateTime
}

// StartWorker polls in-flight Build actions until ctx ends. It implements service.Registerable's worker hook.
func (h *Handler) StartWorker(ctx context.Context) error {
	g := worker.NewGroup(ctx, "codepipeline")
	g.Ticker(buildPollComponent, buildPollInterval, 0, h.Backend.reconcileBuildActions)

	go func() {
		<-ctx.Done()
		g.Stop()
	}()

	return nil
}
