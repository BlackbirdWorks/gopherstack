package emr

import (
	"context"
	"time"
)

// --- DescribeJobFlows ---

type describeJobFlowsInput struct {
	CreatedAfter  *float64 `json:"CreatedAfter"`
	CreatedBefore *float64 `json:"CreatedBefore"`
	JobFlowIDs    []string `json:"JobFlowIds"`
	JobFlowStates []string `json:"JobFlowStates"`
}

type describeJobFlowsOutput struct {
	JobFlows []JobFlow `json:"JobFlows"`
}

func (h *Handler) handleDescribeJobFlows(
	ctx context.Context, in *describeJobFlowsInput,
) (*describeJobFlowsOutput, error) {
	var createdAfter, createdBefore *time.Time

	if in.CreatedAfter != nil {
		t := epochSecondsToTime(*in.CreatedAfter)
		createdAfter = &t
	}

	if in.CreatedBefore != nil {
		t := epochSecondsToTime(*in.CreatedBefore)
		createdBefore = &t
	}

	flows := h.Backend.DescribeJobFlows(ctx, in.JobFlowIDs, in.JobFlowStates, createdAfter, createdBefore)

	return &describeJobFlowsOutput{JobFlows: flows}, nil
}
