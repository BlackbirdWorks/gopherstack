package codepipeline

import "context"

// PutActionRevision puts an action revision for a pipeline source action.
func (b *InMemoryBackend) PutActionRevision(ctx context.Context, pipelineName, stageName, actionName string) error {
	b.mu.RLock("PutActionRevision")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return ErrNotFound
	}

	_ = stageName
	_ = actionName

	return nil
}

// PutApprovalResult submits a manual approval for a pipeline action.
func (b *InMemoryBackend) PutApprovalResult(
	ctx context.Context,
	pipelineName, stageName, actionName, status, summary string,
) error {
	b.mu.RLock("PutApprovalResult")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return ErrNotFound
	}

	_ = stageName
	_ = actionName
	_ = status
	_ = summary

	return nil
}
