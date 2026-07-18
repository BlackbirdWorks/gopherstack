package codedeploy

import (
	"context"
	"fmt"
)

type putLifecycleEventHookExecutionStatusInput struct {
	DeploymentID                  string `json:"deploymentId"`
	LifecycleEventHookExecutionID string `json:"lifecycleEventHookExecutionId"`
	Status                        string `json:"status"`
}

type putLifecycleEventHookExecutionStatusOutput struct {
	LifecycleEventHookExecutionID string `json:"lifecycleEventHookExecutionId"`
}

func (h *Handler) handlePutLifecycleEventHookExecutionStatus(
	_ context.Context,
	in *putLifecycleEventHookExecutionStatusInput,
) (*putLifecycleEventHookExecutionStatusOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &putLifecycleEventHookExecutionStatusOutput{
		LifecycleEventHookExecutionID: in.LifecycleEventHookExecutionID,
	}, nil
}
