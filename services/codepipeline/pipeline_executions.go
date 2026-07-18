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

	for _, exec := range b.executionsStore(region)[pipelineName] {
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

	stored := b.executionsStore(region)[pipelineName]
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

	stored := b.actionExecutionsStore(region)[pipelineName]
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

// ListDeployActionExecutionTargets returns the deploy targets for an action
// execution. The emulator does not model deployment targets, so it returns an
// empty (but valid) list for a known pipeline and ErrNotFound otherwise.
func (b *InMemoryBackend) ListDeployActionExecutionTargets(
	ctx context.Context,
	pipelineName, executionID string,
) ([]map[string]any, error) {
	b.mu.RLock("ListDeployActionExecutionTargets")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return nil, ErrNotFound
	}

	_ = executionID

	return []map[string]any{}, nil
}
