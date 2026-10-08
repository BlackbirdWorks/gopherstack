package codepipeline

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// GetStageTransitionState returns the disabled state for a stage transition, or nil if enabled.
func (b *InMemoryBackend) GetStageTransitionState(
	ctx context.Context,
	pipelineName, stageName, transitionType string,
) *StageTransitionState {
	b.mu.RLock("GetStageTransitionState")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, stageTransitionKey{
		PipelineName:   pipelineName,
		StageName:      stageName,
		TransitionType: transitionType,
	}.String())

	state, ok := b.stageTransitions.Get(key)
	if !ok {
		return nil
	}

	cp := *state

	return &cp
}

// DisableStageTransition disables a stage transition and records the reason.
// Returns StageNotFoundException if stageName does not exist in the pipeline.
func (b *InMemoryBackend) DisableStageTransition(
	ctx context.Context,
	pipelineName, stageName, transitionType, reason string,
) error {
	b.mu.Lock("DisableStageTransition")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	if !pipelineHasStage(p, stageName) {
		return fmt.Errorf("%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName)
	}

	b.stageTransitions.Put(&StageTransitionState{
		region:         region,
		PipelineName:   pipelineName,
		StageName:      stageName,
		TransitionType: transitionType,
		Reason:         reason,
		Disabled:       true,
	})

	return nil
}

// EnableStageTransition re-enables a stage transition and, since real AWS
// resumes any artifacts that were waiting on it without requiring a further
// client call, re-drives runPipelineActions for every InProgress execution
// of the pipeline so a run parked at this gate (see runPipelineActions in
// action_engine.go) can proceed.
func (b *InMemoryBackend) EnableStageTransition(
	ctx context.Context,
	pipelineName, stageName, transitionType string,
) error {
	b.mu.Lock("EnableStageTransition")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	key := regionKey(region, stageTransitionKey{
		PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType,
	}.String())
	b.stageTransitions.Delete(key)

	now := time.Now().UTC()

	for _, exec := range b.executionsStore(region)[pipelineName] {
		if exec.Status != statusInProgress {
			continue
		}

		b.runPipelineActions(region, p, exec)
		exec.LastUpdateTime = now
	}

	return nil
}

// stageTransitionDisabled reports whether stageName's inbound or outbound
// transition (per transitionType) is currently disabled. Callers must hold
// b.mu (RLock or Lock).
func (b *InMemoryBackend) stageTransitionDisabled(region, pipelineName, stageName, transitionType string) bool {
	key := regionKey(region, stageTransitionKey{
		PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType,
	}.String())

	state, ok := b.stageTransitions.Get(key)

	return ok && state.Disabled
}

// pipelineHasStage returns true if the pipeline contains a stage with the given name.
func pipelineHasStage(p *Pipeline, stageName string) bool {
	for _, s := range p.Declaration.Stages {
		if s.Name == stageName {
			return true
		}
	}

	return false
}

// GetPipelineState returns the current state of each stage in a pipeline.
func (b *InMemoryBackend) GetPipelineState(ctx context.Context, pipelineName string) ([]StageState, error) {
	b.mu.RLock("GetPipelineState")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return nil, ErrNotFound
	}

	states := make([]StageState, len(p.Declaration.Stages))
	for i, stage := range p.Declaration.Stages {
		inKey := regionKey(region, stageTransitionKey{
			PipelineName: pipelineName, StageName: stage.Name, TransitionType: transitionTypeInbound,
		}.String())

		var inState *StageTransitionState
		if ts, found := b.stageTransitions.Get(inKey); found {
			tsCopy := *ts
			inState = &tsCopy
		}

		actionExecs := b.actionExecutionsStoreRO(region)[pipelineName]
		revisions := b.actionRevisionsStoreRO(region)
		actionStates := make([]map[string]any, len(stage.Actions))

		for j, action := range stage.Actions {
			actionStates[j] = buildActionState(actionExecs, revisions, pipelineName, stage.Name, action.Name)
		}

		states[i] = StageState{
			StageName:              stage.Name,
			InboundTransitionState: inState,
			ActionStates:           actionStates,
			Conditions:             b.stageConditionStates(region, pipelineName, stage.Name),
		}
	}

	return states, nil
}

// buildActionState builds the ActionState wire map for a single stage/action
// pair: the most recent action-execution record (if any), and the most
// recently tracked ActionRevision (if any, from PutActionRevision).
func buildActionState(
	actionExecs []*ActionExecution,
	revisions map[string]*ActionRevisionRecord,
	pipelineName, stageName, actionName string,
) map[string]any {
	state := map[string]any{"actionName": actionName}

	// Walk backwards to find the most recent execution for this stage/action pair.
	for _, ae := range slices.Backward(actionExecs) {
		if ae.StageName != stageName || ae.ActionName != actionName {
			continue
		}

		latest := map[string]any{
			keyActionExecutionID: ae.ActionExecutionID,
			keyStatus:            ae.Status,
			"startTime":          float64(ae.StartTime.Unix()),
			"lastUpdateTime":     float64(ae.LastUpdateTime.Unix()),
			"lastStatusChange":   float64(ae.LastUpdateTime.Unix()),
		}

		if ae.Summary != "" {
			latest["summary"] = ae.Summary
		}

		if ae.Token != "" {
			latest["token"] = ae.Token
		}

		if ae.ExternalExecutionID != "" {
			latest["externalExecutionId"] = ae.ExternalExecutionID
		}

		if ae.PercentComplete != 0 {
			latest["percentComplete"] = ae.PercentComplete
		}

		if ae.ErrorMessage != "" || ae.ErrorCode != "" {
			latest["errorDetails"] = map[string]any{"code": ae.ErrorCode, "message": ae.ErrorMessage}
		}

		state["latestExecution"] = latest

		break
	}

	if rev, ok := revisions[actionRevisionKey(pipelineName, stageName, actionName)]; ok {
		state["currentRevision"] = map[string]any{
			"revisionId":       rev.RevisionID,
			"revisionChangeId": rev.RevisionChangeID,
			"created":          rev.Created,
		}
	}

	return state
}

// RetryStageExecution retries the failed actions of a stage within a
// specific pipeline execution. RetryMode FAILED_ACTIONS (the common case)
// resets only actions currently Failed/Abandoned in that stage; ALL_ACTIONS
// resets every action in the stage regardless of status. Either way, at
// least one action in the stage must currently be Failed/Abandoned for this
// execution or StageNotRetryableException is returned (matching real AWS,
// which only allows retrying a stage that actually failed). The reset
// actions are then re-run synchronously via runPipelineActions, resuming the
// SAME pipeline execution from that point -- unlike RollbackStage, retry
// never creates a new execution.
func (b *InMemoryBackend) RetryStageExecution(
	ctx context.Context,
	pipelineName, stageName, executionID, retryMode string,
) (*PipelineExecution, error) {
	b.mu.Lock("RetryStageExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	if findStage(p, stageName) == nil {
		return nil, fmt.Errorf("%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName)
	}

	// gopherstack-wlab: same undeclared-code issue as OverrideStageCondition
	// below -- PipelineExecutionNotFoundException is not in
	// RetryStageExecution's declared error set per botocore
	// codepipeline/2015-07-09/service-2.json (PipelineNotFoundException/
	// StageNotFoundException/StageNotRetryableException/
	// NotLatestPipelineExecutionException/
	// ConcurrentPipelineExecutionsLimitExceededException/ConflictException/
	// ValidationException only). Left unfixed: StageNotRetryableException's
	// doc text ("stage state might have changed ... or the stage contains
	// no failed actions") describes a stage-condition mismatch on a real
	// execution, not a nonexistent executionID -- doesn't fit. NOTE: this
	// does not break errors.As for a real caller -- see
	// undeclared_error_codes_test.go.
	exec := findExecution(b.executionsStore(region)[pipelineName], executionID)
	if exec == nil {
		return nil, fmt.Errorf("%w: pipeline %q execution %q", ErrExecutionNotFound, pipelineName, executionID)
	}

	actionExecs := b.actionExecutionsStore(region)[pipelineName]
	if !resetStageActions(actionExecs, executionID, stageName, retryMode == stageRetryModeAllActions) {
		return nil, fmt.Errorf("%w: stage %q of execution %q has no failed action to retry",
			ErrStageNotRetryable, stageName, executionID)
	}

	exec.Status = statusInProgress
	b.runPipelineActions(region, p, exec)
	exec.LastUpdateTime = time.Now().UTC()

	cp := *exec

	return &cp, nil
}

// resetStageActions resets the action executions belonging to executionID's
// stageName back to Succeeded so runPipelineActions can re-run them. It
// first requires (and reports via its bool return) that at least one such
// action is currently Failed or Abandoned -- a stage with no failed action
// is not retryable in real AWS. When allActions is true (StageRetryMode
// ALL_ACTIONS) every action in the stage is reset, not just the failed ones.
func resetStageActions(actionExecs []*ActionExecution, executionID, stageName string, allActions bool) bool {
	failedFound := false

	for _, ae := range actionExecs {
		if ae.PipelineExecutionID != executionID || ae.StageName != stageName {
			continue
		}

		if ae.Status == statusFailed || ae.Status == statusActionAbandoned {
			failedFound = true
		}
	}

	if !failedFound {
		return false
	}

	now := time.Now().UTC()

	for _, ae := range actionExecs {
		if ae.PipelineExecutionID != executionID || ae.StageName != stageName {
			continue
		}

		if allActions || ae.Status == statusFailed || ae.Status == statusActionAbandoned {
			ae.Status = statusSucceeded
			ae.StartTime = now
			ae.LastUpdateTime = now
			ae.Token = ""
			ae.Summary = ""
		}
	}

	return true
}

// RollbackStage rolls back stageName to the state it was in after
// targetExecutionID last completed it successfully, creating (and
// persisting) a new ROLLBACK-type PipelineExecution rather than mutating the
// target execution. Real AWS requires the target execution to have actually
// succeeded through that stage; this backend enforces the same precondition
// via stageSucceededInExecution and returns UnableToRollbackStageException
// otherwise.
func (b *InMemoryBackend) RollbackStage(
	ctx context.Context,
	pipelineName, stageName, targetExecutionID string,
) (*PipelineExecution, error) {
	b.mu.Lock("RollbackStage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	stage := findStage(p, stageName)
	if stage == nil {
		return nil, fmt.Errorf("%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName)
	}

	actionExecs := b.actionExecutionsStore(region)[pipelineName]
	if !stageSucceededInExecution(actionExecs, stage, targetExecutionID) {
		return nil, fmt.Errorf(
			"%w: pipeline %q execution %q did not complete stage %q successfully",
			ErrUnableToRollbackStage, pipelineName, targetExecutionID, stageName,
		)
	}

	exec := b.newRollbackExecution(region, p, stage, targetExecutionID, triggerTypeManualRollback)

	cp := *exec

	return &cp, nil
}

// stageSucceededInExecution reports whether every action in stage has a
// Succeeded action-execution record under executionID -- the real-AWS
// precondition for RollbackStage's target execution.
func stageSucceededInExecution(actionExecs []*ActionExecution, stage *Stage, executionID string) bool {
	if len(stage.Actions) == 0 {
		return false
	}

	succeeded := make(map[string]bool, len(stage.Actions))

	for _, ae := range actionExecs {
		if ae.PipelineExecutionID == executionID && ae.StageName == stage.Name && ae.Status == statusSucceeded {
			succeeded[ae.ActionName] = true
		}
	}

	for _, action := range stage.Actions {
		if !succeeded[action.Name] {
			return false
		}
	}

	return true
}

// OverrideStageCondition marks the stage's failed BEFORE_ENTRY or ON_SUCCESS condition Overridden and
// resumes the execution past it. A condition that has not failed is ConditionNotOverridableException.
func (b *InMemoryBackend) OverrideStageCondition(
	ctx context.Context,
	pipelineName, stageName, executionID, conditionType string,
) error {
	b.mu.Lock("OverrideStageCondition")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	if findStage(p, stageName) == nil {
		return fmt.Errorf("%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName)
	}

	exec := findExecution(b.executionsStoreRO(region)[pipelineName], executionID)
	if exec == nil {
		return fmt.Errorf("%w: pipeline %q execution %q", ErrExecutionNotFound, pipelineName, executionID)
	}

	run := findConditionRun(b.conditionRunsStoreRO(region)[pipelineName], executionID, stageName, conditionType)
	if run == nil || run.Status != conditionStatusFailed {
		return fmt.Errorf("%w: %s condition of stage %q is not failed",
			ErrConditionNotOverridable, conditionType, stageName)
	}

	now := time.Now().UTC()
	caller := awsmeta.CallerArn(ctx)
	run.Status = conditionStatusOverridden
	run.Summary = "Overridden"
	run.LastStatusChange = now

	for _, ce := range run.Conditions {
		for _, rr := range ce.Rules {
			rr.UpdatedBy = caller
		}
	}

	exec.Status = statusInProgress
	exec.StatusSummary = ""
	b.runPipelineActions(region, p, exec)
	exec.LastUpdateTime = now

	return nil
}

// newRollbackExecution records a succeeded ROLLBACK-type execution that restores stage to targetExecutionID.
// Callers hold b.mu.Lock.
func (b *InMemoryBackend) newRollbackExecution(
	region string, p *Pipeline, stage *Stage, targetExecutionID, trigger string,
) *PipelineExecution {
	pipelineName := p.Declaration.Name
	now := time.Now().UTC()
	exec := &PipelineExecution{
		PipelineName:              pipelineName,
		PipelineExecutionID:       uuid.NewString(),
		Status:                    statusSucceeded,
		PipelineVersion:           p.Declaration.Version,
		ExecutionMode:             p.Declaration.ExecutionMode,
		ExecutionType:             executionTypeRollback,
		Trigger:                   trigger,
		RollbackTargetExecutionID: targetExecutionID,
		StartTime:                 now,
		LastUpdateTime:            now,
	}

	execs := b.executionsStore(region)
	execs[pipelineName] = append(execs[pipelineName], exec)

	actionExecStore := b.actionExecutionsStore(region)
	for _, action := range stage.Actions {
		actionExecStore[pipelineName] = append(actionExecStore[pipelineName], &ActionExecution{
			PipelineExecutionID: exec.PipelineExecutionID,
			ActionExecutionID:   uuid.NewString(),
			StageName:           stage.Name,
			ActionName:          action.Name,
			Status:              statusSucceeded,
			StartTime:           now,
			LastUpdateTime:      now,
		})
	}

	return exec
}
