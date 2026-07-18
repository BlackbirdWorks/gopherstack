package emr

import (
	"context"
)

// --- ModifyCluster ---

type modifyClusterInput struct {
	ClusterID            string `json:"ClusterId"`
	StepConcurrencyLevel int    `json:"StepConcurrencyLevel"`
}

type modifyClusterOutput struct {
	StepConcurrencyLevel int `json:"StepConcurrencyLevel"`
}

func (h *Handler) handleModifyCluster(ctx context.Context, in *modifyClusterInput) (*modifyClusterOutput, error) {
	level, err := h.Backend.ModifyCluster(ctx, in.ClusterID, in.StepConcurrencyLevel)
	if err != nil {
		return nil, err
	}

	return &modifyClusterOutput{StepConcurrencyLevel: level}, nil
}

// --- SetTerminationProtection ---

type setTerminationProtectionInput struct {
	JobFlowIDs           []string `json:"JobFlowIds"`
	TerminationProtected bool     `json:"TerminationProtected"`
}

type setTerminationProtectionOutput struct{}

func (h *Handler) handleSetTerminationProtection(
	ctx context.Context,
	in *setTerminationProtectionInput,
) (*setTerminationProtectionOutput, error) {
	if err := h.Backend.SetTerminationProtection(ctx, in.JobFlowIDs, in.TerminationProtected); err != nil {
		return nil, err
	}

	return &setTerminationProtectionOutput{}, nil
}

// --- SetKeepJobFlowAliveWhenNoSteps ---

type setKeepJobFlowAliveWhenNoStepsInput struct {
	JobFlowIDs                  []string `json:"JobFlowIds"`
	KeepJobFlowAliveWhenNoSteps bool     `json:"KeepJobFlowAliveWhenNoSteps"`
}

type setKeepJobFlowAliveWhenNoStepsOutput struct{}

func (h *Handler) handleSetKeepJobFlowAliveWhenNoSteps(
	ctx context.Context,
	in *setKeepJobFlowAliveWhenNoStepsInput,
) (*setKeepJobFlowAliveWhenNoStepsOutput, error) {
	if err := h.Backend.SetKeepJobFlowAliveWhenNoSteps(ctx, in.JobFlowIDs, in.KeepJobFlowAliveWhenNoSteps); err != nil {
		return nil, err
	}

	return &setKeepJobFlowAliveWhenNoStepsOutput{}, nil
}

// --- SetVisibleToAllUsers ---

type setVisibleToAllUsersInput struct {
	JobFlowIDs        []string `json:"JobFlowIds"`
	VisibleToAllUsers bool     `json:"VisibleToAllUsers"`
}

type setVisibleToAllUsersOutput struct{}

func (h *Handler) handleSetVisibleToAllUsers(
	ctx context.Context,
	in *setVisibleToAllUsersInput,
) (*setVisibleToAllUsersOutput, error) {
	if err := h.Backend.SetVisibleToAllUsers(ctx, in.JobFlowIDs, in.VisibleToAllUsers); err != nil {
		return nil, err
	}

	return &setVisibleToAllUsersOutput{}, nil
}

// --- SetUnhealthyNodeReplacement ---

type setUnhealthyNodeReplacementInput struct {
	JobFlowIDs               []string `json:"JobFlowIds"`
	UnhealthyNodeReplacement bool     `json:"UnhealthyNodeReplacement"`
}

type setUnhealthyNodeReplacementOutput struct{}

func (h *Handler) handleSetUnhealthyNodeReplacement(
	ctx context.Context,
	in *setUnhealthyNodeReplacementInput,
) (*setUnhealthyNodeReplacementOutput, error) {
	if err := h.Backend.SetUnhealthyNodeReplacement(ctx, in.JobFlowIDs, in.UnhealthyNodeReplacement); err != nil {
		return nil, err
	}

	return &setUnhealthyNodeReplacementOutput{}, nil
}
