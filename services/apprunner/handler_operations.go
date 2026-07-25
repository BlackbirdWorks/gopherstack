package apprunner

import (
	"context"
	"fmt"
)

type listOperationsInput struct {
	ServiceArn string `json:"ServiceArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type operationSummaryOutput struct {
	ID        string `json:"Id"`
	Type      string `json:"Type"`
	Status    string `json:"Status"`
	TargetArn string `json:"TargetArn"`
	StartedAt int64  `json:"StartedAt"`
	EndedAt   int64  `json:"EndedAt"`
	UpdatedAt int64  `json:"UpdatedAt"`
}

type listOperationsOutput struct {
	NextToken            string                   `json:"NextToken,omitempty"`
	OperationSummaryList []operationSummaryOutput `json:"OperationSummaryList"`
}

func (h *Handler) handleListOperations(
	_ context.Context,
	in *listOperationsInput,
) (*listOperationsOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	ops, nextToken, err := h.Backend.ListOperations(in.ServiceArn, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]operationSummaryOutput, 0, len(ops))
	for _, op := range ops {
		out = append(out, operationSummaryOutput{
			ID:        op.ID,
			Type:      op.Type,
			Status:    op.Status,
			TargetArn: op.TargetArn,
			StartedAt: op.StartedAt.Unix(),
			EndedAt:   op.EndedAt.Unix(),
			UpdatedAt: op.UpdatedAt.Unix(),
		})
	}

	return &listOperationsOutput{OperationSummaryList: out, NextToken: nextToken}, nil
}
