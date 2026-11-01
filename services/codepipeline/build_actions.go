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

// pendingAction identifies an InProgress backend-driven action execution awaiting its backend resource.
type pendingAction struct {
	action           *Action
	ae               *ActionExecution
	region, pipeline string
}

func (b *InMemoryBackend) pendingActionsLocked(includeBuilds bool) []pendingAction {
	var out []pendingAction

	for region, byPipeline := range b.actionExecutions {
		for name, aes := range byPipeline {
			for _, ae := range aes {
				if action := b.pendingDeclaration(region, name, ae, includeBuilds); action != nil {
					out = append(out, pendingAction{region: region, pipeline: name, ae: ae, action: action})
				}
			}
		}
	}

	return out
}

// pendingDeclaration returns the declaration of ae's action when ae is waiting on a backend resource.
func (b *InMemoryBackend) pendingDeclaration(
	region, pipelineName string, ae *ActionExecution, includeBuilds bool,
) *Action {
	if ae.Status != statusInProgress || ae.Token != "" {
		return nil
	}

	action := b.declaredAction(region, pipelineName, ae)
	if action == nil {
		return nil
	}

	isBuild := includeBuilds && ae.ExternalExecutionID != "" && isBuiltinAction(*action, actionProviderCodeBuild)
	if !isBuild && !isAsyncBuiltin(*action) {
		return nil
	}

	return action
}

// declaredAction returns the pipeline's declaration of the action behind ae, or nil.
func (b *InMemoryBackend) declaredAction(region, pipelineName string, ae *ActionExecution) *Action {
	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return nil
	}

	stage := findStage(p, ae.StageName)
	if stage == nil {
		return nil
	}

	return findAction(stage, ae.ActionName)
}

// reconcileBuildActions polls the backend resource behind every InProgress Build, CloudFormation and ECS action
// and, once it settles, completes the action and resumes its pipeline execution.
func (b *InMemoryBackend) reconcileBuildActions(_ context.Context) {
	b.mu.RLock("reconcileBuildActionsScan")
	_, isRunner := b.codeBuildBackend.(CodeBuildRunner)
	pending := b.pendingActionsLocked(isRunner)
	b.mu.RUnlock()

	if len(pending) == 0 {
		return
	}

	b.mu.Lock("reconcileBuildActions")
	defer b.mu.Unlock()

	runner, _ := b.codeBuildBackend.(CodeBuildRunner)

	for _, pa := range b.pendingActionsLocked(runner != nil) {
		b.settleAction(runner, pa)
	}
}

func (b *InMemoryBackend) settleAction(runner CodeBuildRunner, pa pendingAction) {
	ctx := b.regionContext(pa.region)
	ae := pa.ae

	p, ok := b.pipelines.Get(regionKey(pa.region, pa.pipeline))
	if !ok {
		return
	}

	switch {
	case isBuiltinAction((*pa.action), actionProviderCodeBuild):
		b.settleBuild(ctx, runner, p, pa)
	case isBuiltinAction((*pa.action), actionProviderCloudFormation):
		b.advanceCloudFormation(ctx, p, ae.PipelineExecutionID, (*pa.action), ae)
	case isBuiltinAction((*pa.action), actionProviderECS):
		b.advanceECS(ctx, p, ae.PipelineExecutionID, (*pa.action), ae)
	}

	if ae.Status == statusInProgress {
		return
	}

	ae.LastUpdateTime = time.Now().UTC()

	exec := findExecution(b.executionsStore(pa.region)[pa.pipeline], ae.PipelineExecutionID)
	if exec == nil || exec.Status != statusInProgress {
		return
	}

	b.runPipelineActions(pa.region, p, exec)
	exec.LastUpdateTime = ae.LastUpdateTime
}

func (b *InMemoryBackend) settleBuild(ctx context.Context, runner CodeBuildRunner, p *Pipeline, pa pendingAction) {
	ae := pa.ae

	status, err := runner.BuildStatus(ctx, ae.ExternalExecutionID)

	switch {
	case err == nil && status == buildStatusInProgress:
	case err != nil:
		failActionf(ae, failureJobFailed, "Unable to read build %s: %v", ae.ExternalExecutionID, err)
	case status != buildStatusSucceeded:
		failActionf(ae, failureJobFailed, "Build %s terminated with status %s", ae.ExternalExecutionID, status)
	default:
		ae.Status = statusSucceeded

		if perr := b.passThroughArtifacts(ctx, p, ae.PipelineExecutionID, (*pa.action)); perr != nil {
			failActionf(ae, failureJobFailed, "%v", perr)
		}
	}
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
