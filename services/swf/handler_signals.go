package swf

import (
	"context"
	"fmt"
)

// --- SignalWorkflowExecution ---

type handleSignalWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId,omitempty"`
	SignalName string `json:"signalName"`
	Input      string `json:"input,omitempty"`
}

type signalWorkflowExecutionOutput struct{}

func (h *Handler) handleSignalWorkflowExecution(
	_ context.Context,
	in *handleSignalWorkflowExecutionInput,
) (*signalWorkflowExecutionOutput, error) {
	if in.SignalName == "" {
		return nil, fmt.Errorf("%w: signalName is required", ErrValidation)
	}

	if err := h.Backend.SignalWorkflowExecution(
		in.Domain,
		in.WorkflowID,
		in.RunID,
		in.SignalName,
		in.Input,
	); err != nil {
		return nil, err
	}

	return &signalWorkflowExecutionOutput{}, nil
}
