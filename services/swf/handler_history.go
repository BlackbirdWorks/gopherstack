package swf

import "context"

// --- GetWorkflowExecutionHistory ---

type handleGetWorkflowExecutionHistoryInput struct {
	Execution       workflowExecutionRef `json:"execution"`
	Domain          string               `json:"domain"`
	NextPageToken   string               `json:"nextPageToken,omitempty"`
	MaximumPageSize int                  `json:"maximumPageSize,omitempty"`
	ReverseOrder    bool                 `json:"reverseOrder,omitempty"`
}

type getWorkflowExecutionHistoryOutput struct {
	NextPageToken string         `json:"nextPageToken,omitempty"`
	Events        []HistoryEvent `json:"events"`
}

func (h *Handler) handleGetWorkflowExecutionHistory(
	_ context.Context,
	in *handleGetWorkflowExecutionHistoryInput,
) (*getWorkflowExecutionHistoryOutput, error) {
	if err := validatePageParams(in.NextPageToken, in.MaximumPageSize); err != nil {
		return nil, err
	}

	if err := h.Backend.RequireExecution(in.Domain, in.Execution.WorkflowID, in.Execution.RunID); err != nil {
		return nil, err
	}

	events, nextPageToken := h.Backend.GetWorkflowExecutionHistory(
		in.Domain, in.Execution.WorkflowID, in.Execution.RunID,
		in.MaximumPageSize, in.NextPageToken, in.ReverseOrder,
	)

	return &getWorkflowExecutionHistoryOutput{Events: events, NextPageToken: nextPageToken}, nil
}
