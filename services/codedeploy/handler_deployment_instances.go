package codedeploy

import (
	"context"
	"fmt"
)

type batchGetDeploymentInstancesInput struct {
	DeploymentID string   `json:"deploymentId"`
	InstanceIDs  []string `json:"instanceIds"`
}

type batchGetDeploymentInstancesOutput struct {
	ErrorMessage     string                `json:"errorMessage,omitempty"`
	InstancesSummary []InstanceSummaryItem `json:"instancesSummary"`
}

func (h *Handler) handleBatchGetDeploymentInstances(
	_ context.Context,
	in *batchGetDeploymentInstancesInput,
) (*batchGetDeploymentInstancesOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	items, err := h.Backend.BatchGetDeploymentInstances(in.DeploymentID, in.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return &batchGetDeploymentInstancesOutput{
		InstancesSummary: items,
	}, nil
}

type batchGetDeploymentTargetsInput struct {
	DeploymentID string   `json:"deploymentId"`
	TargetIDs    []string `json:"targetIds"`
}

type batchGetDeploymentTargetsOutput struct {
	DeploymentTargets []DeploymentTargetItem `json:"deploymentTargets"`
}

func (h *Handler) handleBatchGetDeploymentTargets(
	_ context.Context,
	in *batchGetDeploymentTargetsInput,
) (*batchGetDeploymentTargetsOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	items, err := h.Backend.BatchGetDeploymentTargets(in.DeploymentID, in.TargetIDs)
	if err != nil {
		return nil, err
	}

	targets := make([]DeploymentTargetItem, 0, len(items))
	for _, item := range items {
		targets = append(targets, *item)
	}

	return &batchGetDeploymentTargetsOutput{DeploymentTargets: targets}, nil
}

type getDeploymentInstanceInput struct {
	DeploymentID string `json:"deploymentId"`
	InstanceID   string `json:"instanceId"`
}

type getDeploymentInstanceOutput struct {
	InstanceSummary InstanceSummaryItem `json:"instanceSummary"`
}

func (h *Handler) handleGetDeploymentInstance(
	_ context.Context,
	in *getDeploymentInstanceInput,
) (*getDeploymentInstanceOutput, error) {
	if in.DeploymentID == "" || in.InstanceID == "" {
		return nil, fmt.Errorf("%w: deploymentId and instanceId are required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &getDeploymentInstanceOutput{
		InstanceSummary: InstanceSummaryItem{
			DeploymentID: in.DeploymentID,
			InstanceID:   in.InstanceID,
			Status:       statusSucceeded,
		},
	}, nil
}

type getDeploymentTargetInput struct {
	DeploymentID string `json:"deploymentId"`
	TargetID     string `json:"targetId"`
}

type getDeploymentTargetOutput struct {
	DeploymentTarget DeploymentTargetItem `json:"deploymentTarget"`
}

func (h *Handler) handleGetDeploymentTarget(
	_ context.Context,
	in *getDeploymentTargetInput,
) (*getDeploymentTargetOutput, error) {
	if in.DeploymentID == "" || in.TargetID == "" {
		return nil, fmt.Errorf("%w: deploymentId and targetId are required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &getDeploymentTargetOutput{
		DeploymentTarget: DeploymentTargetItem{
			DeploymentID: in.DeploymentID,
			TargetID:     in.TargetID,
			Status:       statusSucceeded,
			TargetType:   "instanceTarget",
		},
	}, nil
}

type listDeploymentInstancesInput struct {
	DeploymentID string `json:"deploymentId"`
}

type listDeploymentInstancesOutput struct {
	InstancesList []string `json:"instancesList"`
}

func (h *Handler) handleListDeploymentInstances(
	_ context.Context,
	in *listDeploymentInstancesInput,
) (*listDeploymentInstancesOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &listDeploymentInstancesOutput{InstancesList: []string{}}, nil
}

type listDeploymentTargetsInput struct {
	DeploymentID string `json:"deploymentId"`
}

type listDeploymentTargetsOutput struct {
	TargetIDs []string `json:"targetIds"`
}

func (h *Handler) handleListDeploymentTargets(
	_ context.Context,
	in *listDeploymentTargetsInput,
) (*listDeploymentTargetsOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &listDeploymentTargetsOutput{TargetIDs: []string{}}, nil
}
