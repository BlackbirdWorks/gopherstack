package codepipeline

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

// PutActionRevision records a new ActionRevision for a pipeline source action
// and starts a new pipeline execution to process it, mirroring real AWS's
// "PutActionRevision triggers a pipeline execution" behavior. NewRevision
// reports whether revisionID differs from the previously stored revision for
// this stage/action (false the first time it is called with an identical
// revisionID, matching the real semantics of "was this revision already
// processed by this pipeline").
func (b *InMemoryBackend) PutActionRevision(
	ctx context.Context,
	pipelineName, stageName, actionName, revisionID, revisionChangeID string,
) (*PipelineExecution, bool, error) {
	b.mu.Lock("PutActionRevision")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return nil, false, fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	stage := findStage(p, stageName)
	if stage == nil {
		return nil, false, fmt.Errorf(
			"%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName,
		)
	}

	if findAction(stage, actionName) == nil {
		return nil, false, fmt.Errorf("%w: action %q not found in stage %q", ErrActionNotFound, actionName, stageName)
	}

	revisions := b.actionRevisionsStore(region)
	key := actionRevisionKey(pipelineName, stageName, actionName)
	prev, existed := revisions[key]
	newRevision := !existed || prev.RevisionID != revisionID

	revisions[key] = &ActionRevisionRecord{
		RevisionID:       revisionID,
		RevisionChangeID: revisionChangeID,
		Created:          float64(time.Now().Unix()),
	}

	now := time.Now().UTC()
	exec := &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: uuid.NewString(),
		Status:              statusInProgress,
		PipelineVersion:     p.Declaration.Version,
		ExecutionMode:       p.Declaration.ExecutionMode,
		ExecutionType:       executionTypeStandard,
		Trigger:             triggerTypePutActionRevision,
		StartTime:           now,
		LastUpdateTime:      now,
	}

	execs := b.executionsStore(region)
	execs[pipelineName] = append(execs[pipelineName], exec)

	b.runPipelineActions(region, p, exec)
	exec.LastUpdateTime = time.Now().UTC()

	cp := *exec

	return &cp, newRevision, nil
}

// PutApprovalResult submits a manual approval or rejection for a pipeline's
// pending Approval-category action, resuming (Approved) or failing
// (Rejected) the paused pipeline execution that action gated -- see
// runPipelineActions in action_engine.go for how the gate is created.
// Returns the timestamp the result was recorded, for the wire response's
// approvedAt field.
func (b *InMemoryBackend) PutApprovalResult(
	ctx context.Context,
	pipelineName, stageName, actionName, token, status, summary string,
) (time.Time, error) {
	b.mu.Lock("PutApprovalResult")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelines.Get(regionKey(region, pipelineName))
	if !ok {
		return time.Time{}, fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	stage := findStage(p, stageName)
	if stage == nil {
		return time.Time{}, fmt.Errorf(
			"%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName,
		)
	}

	action := findAction(stage, actionName)
	if action == nil || action.ActionTypeID.Category != actionCategoryApproval {
		return time.Time{}, fmt.Errorf(
			"%w: %q/%q is not an Approval action in pipeline %q",
			ErrActionNotFound, stageName, actionName, pipelineName,
		)
	}

	if status != approvalStatusApproved && status != approvalStatusRejected {
		return time.Time{}, fmt.Errorf("%w: result.status must be %q or %q",
			ErrValidation, approvalStatusApproved, approvalStatusRejected)
	}

	pending := findPendingApproval(b.actionExecutionsStore(region)[pipelineName], stageName, actionName)
	if pending == nil {
		return time.Time{}, fmt.Errorf(
			"%w: no open approval request for %q/%q in pipeline %q",
			ErrApprovalAlreadyCompleted, stageName, actionName, pipelineName,
		)
	}

	if pending.Token != token {
		return time.Time{}, fmt.Errorf(
			"%w: token does not match the pending approval request for %q/%q",
			ErrInvalidApprovalToken,
			stageName,
			actionName,
		)
	}

	now := time.Now().UTC()
	pending.Summary = summary
	pending.Token = ""
	pending.LastUpdateTime = now

	exec := findExecution(b.executionsStore(region)[pipelineName], pending.PipelineExecutionID)

	if status == approvalStatusApproved {
		pending.Status = statusSucceeded

		if exec != nil {
			b.runPipelineActions(region, p, exec)
		}
	} else {
		pending.Status = statusFailed

		if exec != nil {
			exec.Status = statusFailed
		}
	}

	if exec != nil {
		exec.LastUpdateTime = now
	}

	return now, nil
}

// findPendingApproval returns the most recent InProgress action execution
// matching stageName/actionName, i.e. the one currently gating a pipeline
// execution on manual approval, or nil if no approval is currently open.
func findPendingApproval(actionExecs []*ActionExecution, stageName, actionName string) *ActionExecution {
	for _, ae := range slices.Backward(actionExecs) {
		if ae.StageName == stageName && ae.ActionName == actionName {
			if ae.Status == statusInProgress && ae.Token != "" {
				return ae
			}

			return nil
		}
	}

	return nil
}

// findExecution returns the pipeline execution with the given ID, or nil.
func findExecution(execs []*PipelineExecution, executionID string) *PipelineExecution {
	for _, e := range execs {
		if e.PipelineExecutionID == executionID {
			return e
		}
	}

	return nil
}
