package codepipeline

import (
	"context"
	"fmt"
	"time"
)

type putActionRevisionInput struct {
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	ActionName     string `json:"actionName"`
	ActionRevision struct {
		RevisionID       string `json:"revisionId"`
		RevisionChangeID string `json:"revisionChangeId"`
	} `json:"actionRevision"`
}

type putActionRevisionOutput struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
	NewRevision         bool   `json:"newRevision"`
}

func (h *Handler) handlePutActionRevision(
	ctx context.Context,
	in *putActionRevisionInput,
) (*putActionRevisionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if err := h.Backend.PutActionRevision(ctx, in.PipelineName, in.StageName, in.ActionName); err != nil {
		return nil, err
	}

	return &putActionRevisionOutput{NewRevision: true}, nil
}

type putApprovalResultInput struct {
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	ActionName     string `json:"actionName"`
	ApprovalResult struct {
		Status  string `json:"status"`
		Summary string `json:"summary"`
	} `json:"approvalResult"`
}

type putApprovalResultOutput struct {
	ApprovedAt string `json:"approvedAt"`
}

func (h *Handler) handlePutApprovalResult(
	ctx context.Context,
	in *putApprovalResultInput,
) (*putApprovalResultOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if err := h.Backend.PutApprovalResult(
		ctx, in.PipelineName, in.StageName, in.ActionName,
		in.ApprovalResult.Status, in.ApprovalResult.Summary,
	); err != nil {
		return nil, err
	}

	return &putApprovalResultOutput{
		ApprovedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}
