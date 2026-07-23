package codepipeline

import (
	"context"
	"fmt"
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

	exec, newRevision, err := h.Backend.PutActionRevision(
		ctx, in.PipelineName, in.StageName, in.ActionName,
		in.ActionRevision.RevisionID, in.ActionRevision.RevisionChangeID,
	)
	if err != nil {
		return nil, err
	}

	return &putActionRevisionOutput{
		PipelineExecutionID: exec.PipelineExecutionID,
		NewRevision:         newRevision,
	}, nil
}

// putApprovalResultInput mirrors PutApprovalResultInput. The result member is
// serialised on the wire as "result" (verified against
// awsAwsjson11_serializeOpDocumentPutApprovalResultInput in the real SDK's
// serializers.go) -- an earlier revision of this handler used the Go SDK
// field name "approvalResult" instead, which a real client would never send,
// silently no-opping every field inside it. Token is likewise required by
// real AWS (obtained from GetPipelineState's actionStates[].latestExecution.token)
// but was previously not parsed at all.
type putApprovalResultInput struct {
	PipelineName string `json:"pipelineName"`
	StageName    string `json:"stageName"`
	ActionName   string `json:"actionName"`
	Token        string `json:"token"`
	Result       struct {
		Status  string `json:"status"`
		Summary string `json:"summary"`
	} `json:"result"`
}

type putApprovalResultOutput struct {
	// ApprovedAt is epoch seconds, matching every other timestamp on the
	// awsjson1.1 wire in this service (see PARITY.md's epoch-seconds note) --
	// an earlier revision emitted an RFC3339 string here instead.
	ApprovedAt float64 `json:"approvedAt"`
}

func (h *Handler) handlePutApprovalResult(
	ctx context.Context,
	in *putApprovalResultInput,
) (*putApprovalResultOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.Token == "" {
		return nil, fmt.Errorf("%w: token is required", errInvalidRequest)
	}

	approvedAt, err := h.Backend.PutApprovalResult(
		ctx, in.PipelineName, in.StageName, in.ActionName, in.Token,
		in.Result.Status, in.Result.Summary,
	)
	if err != nil {
		return nil, err
	}

	return &putApprovalResultOutput{ApprovedAt: float64(approvedAt.Unix())}, nil
}
