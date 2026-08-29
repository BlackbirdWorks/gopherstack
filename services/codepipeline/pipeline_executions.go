package codepipeline

import (
	"context"
	"fmt"
	"slices"
)

// GetPipelineExecution returns the stored execution by pipeline name and execution ID.
func (b *InMemoryBackend) GetPipelineExecution(
	ctx context.Context,
	pipelineName, executionID string,
) (*PipelineExecution, error) {
	b.mu.RLock("GetPipelineExecution")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if !b.pipelines.Has(regionKey(region, pipelineName)) {
		return nil, ErrNotFound
	}

	for _, exec := range b.executionsStoreRO(region)[pipelineName] {
		if exec.PipelineExecutionID == executionID {
			cp := *exec

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: pipeline %q execution %q", ErrExecutionNotFound, pipelineName, executionID)
}

// ListPipelineExecutions returns stored executions for a pipeline, most recent first.
func (b *InMemoryBackend) ListPipelineExecutions(
	ctx context.Context,
	pipelineName string,
) ([]PipelineExecution, error) {
	b.mu.RLock("ListPipelineExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if !b.pipelines.Has(regionKey(region, pipelineName)) {
		return nil, ErrNotFound
	}

	stored := b.executionsStoreRO(region)[pipelineName]
	out := make([]PipelineExecution, len(stored))

	// Return in reverse order (most recent first).
	for i, e := range stored {
		out[len(stored)-1-i] = *e
	}

	return out, nil
}

// ListActionExecutions returns the recorded action executions for a pipeline,
// most recent first. An optional pipelineExecutionId filters to a single run.
func (b *InMemoryBackend) ListActionExecutions(
	ctx context.Context,
	pipelineName, pipelineExecutionID string,
) ([]map[string]any, error) {
	b.mu.RLock("ListActionExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if !b.pipelines.Has(regionKey(region, pipelineName)) {
		return nil, ErrNotFound
	}

	stored := b.actionExecutionsStoreRO(region)[pipelineName]
	out := make([]map[string]any, 0, len(stored))

	// Iterate in reverse so the most recent execution appears first.
	for _, ae := range slices.Backward(stored) {
		if pipelineExecutionID != "" && ae.PipelineExecutionID != pipelineExecutionID {
			continue
		}

		out = append(out, map[string]any{
			keyPipelineExecutionID: ae.PipelineExecutionID,
			"actionExecutionId":    ae.ActionExecutionID,
			"stageName":            ae.StageName,
			"actionName":           ae.ActionName,
			"startTime":            float64(ae.StartTime.Unix()),
			"lastUpdateTime":       float64(ae.LastUpdateTime.Unix()),
			keyStatus:              ae.Status,
		})
	}

	return out, nil
}

// StageSucceededInExecution reports whether stageName succeeded within
// pipelineExecutionID, backing ListPipelineExecutions'
// Filter.SucceededInStage (types.SucceededInStageFilter). A stage is
// considered succeeded when at least one action execution is recorded for
// it in this run and every one of them completed Succeeded -- the
// mechanical definition this backend's flat ActionExecution records can
// support; there is no separate per-stage status anywhere else in this
// backend to consult instead.
func (b *InMemoryBackend) StageSucceededInExecution(
	ctx context.Context,
	pipelineName, pipelineExecutionID, stageName string,
) bool {
	b.mu.RLock("StageSucceededInExecution")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	found := false

	for _, ae := range b.actionExecutionsStoreRO(region)[pipelineName] {
		if ae.PipelineExecutionID != pipelineExecutionID || ae.StageName != stageName {
			continue
		}

		found = true

		if ae.Status != statusSucceeded {
			return false
		}
	}

	return found
}

// ListDeployActionExecutionTargets returns the deploy targets for an action
// execution. Real ListDeployActionExecutionTargetsInput marks only
// ActionExecutionId required (codepipeline@v1.49.4
// api_op_ListDeployActionExecutionTargets.go); PipelineName is an optional
// narrowing filter, so an ActionExecutionId alone must resolve by scanning
// every pipeline in the region -- gopherstack-2wvq. The emulator does not
// model deployment targets, so a resolved execution still returns an empty
// (but valid) list.
func (b *InMemoryBackend) ListDeployActionExecutionTargets(
	ctx context.Context,
	pipelineName, executionID string,
) ([]map[string]any, error) {
	b.mu.RLock("ListDeployActionExecutionTargets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if pipelineName != "" {
		if !b.pipelines.Has(regionKey(region, pipelineName)) {
			return nil, ErrNotFound
		}

		if !hasActionExecution(b.actionExecutionsStoreRO(region)[pipelineName], executionID) {
			return nil, fmt.Errorf("%w: %q", ErrActionExecutionNotFound, executionID)
		}

		return []map[string]any{}, nil
	}

	for _, execs := range b.actionExecutionsStoreRO(region) {
		if hasActionExecution(execs, executionID) {
			return []map[string]any{}, nil
		}
	}

	return nil, fmt.Errorf("%w: %q", ErrActionExecutionNotFound, executionID)
}

// hasActionExecution reports whether executionID matches any ActionExecutionID in execs.
func hasActionExecution(execs []*ActionExecution, executionID string) bool {
	for _, ae := range execs {
		if ae.ActionExecutionID == executionID {
			return true
		}
	}

	return false
}
