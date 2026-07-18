package codepipeline

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
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

// EnableStageTransition re-enables a stage transition.
func (b *InMemoryBackend) EnableStageTransition(
	ctx context.Context,
	pipelineName, stageName, transitionType string,
) error {
	b.mu.Lock("EnableStageTransition")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if !b.pipelines.Has(regionKey(region, pipelineName)) {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	key := regionKey(region, stageTransitionKey{
		PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType,
	}.String())
	b.stageTransitions.Delete(key)

	return nil
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
		outKey := regionKey(region, stageTransitionKey{
			PipelineName: pipelineName, StageName: stage.Name, TransitionType: transitionTypeOutbound,
		}.String())

		var inState, outState *StageTransitionState
		if ts, found := b.stageTransitions.Get(inKey); found {
			tsCopy := *ts
			inState = &tsCopy
		}

		if ts, found := b.stageTransitions.Get(outKey); found {
			tsCopy := *ts
			outState = &tsCopy
		}

		actionExecs := b.actionExecutionsStore(region)[pipelineName]
		actionStates := make([]map[string]any, len(stage.Actions))
		for j, action := range stage.Actions {
			state := map[string]any{
				"actionName": action.Name,
			}
			// Walk backwards to find the most recent execution for this stage/action pair.
			for _, ae := range slices.Backward(actionExecs) {
				if ae.StageName == stage.Name && ae.ActionName == action.Name {
					state["latestExecution"] = map[string]any{
						"actionExecutionId": ae.ActionExecutionID,
						keyStatus:           ae.Status,
						"startTime":         float64(ae.StartTime.Unix()),
						"lastUpdateTime":    float64(ae.LastUpdateTime.Unix()),
					}

					break
				}
			}
			actionStates[j] = state
		}

		states[i] = StageState{
			StageName:               stage.Name,
			InboundTransitionState:  inState,
			OutboundTransitionState: outState,
			ActionStates:            actionStates,
		}
	}

	return states, nil
}

// RetryStageExecution retries a failed stage in a pipeline.
func (b *InMemoryBackend) RetryStageExecution(
	ctx context.Context,
	pipelineName, stageName, executionID string,
) (*PipelineExecution, error) {
	b.mu.RLock("RetryStageExecution")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return nil, ErrNotFound
	}

	_ = stageName

	return &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: executionID,
		Status:              statusInProgress,
	}, nil
}

// RollbackStage rolls back a stage to a previous successful execution.
func (b *InMemoryBackend) RollbackStage(
	ctx context.Context,
	pipelineName, stageName, targetExecutionID string,
) (*PipelineExecution, error) {
	b.mu.RLock("RollbackStage")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return nil, ErrNotFound
	}

	_ = stageName
	_ = targetExecutionID

	return &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: uuid.NewString(),
		Status:              statusInProgress,
	}, nil
}

// OverrideStageCondition overrides a stage condition.
func (b *InMemoryBackend) OverrideStageCondition(
	ctx context.Context,
	pipelineName, stageName, executionID string,
) error {
	b.mu.RLock("OverrideStageCondition")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return ErrNotFound
	}

	_ = stageName
	_ = executionID

	return nil
}
