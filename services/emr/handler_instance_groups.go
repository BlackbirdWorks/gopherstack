package emr

import (
	"context"
)

// --- ListInstanceGroups ---

type listInstanceGroupsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listInstanceGroupsOutput struct {
	InstanceGroups []InstanceGroup `json:"InstanceGroups"`
}

func (h *Handler) handleListInstanceGroups(
	ctx context.Context,
	in *listInstanceGroupsInput,
) (*listInstanceGroupsOutput, error) {
	groups, err := h.Backend.ListInstanceGroups(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &listInstanceGroupsOutput{InstanceGroups: groups}, nil
}

// --- AddInstanceGroups ---

type addInstanceGroupsInput struct {
	JobFlowID      string              `json:"JobFlowId"`
	InstanceGroups []InstanceGroupSpec `json:"InstanceGroups"`
}

type addInstanceGroupsOutput struct {
	ClusterArn       string   `json:"ClusterArn"`
	JobFlowID        string   `json:"JobFlowId"`
	InstanceGroupIDs []string `json:"InstanceGroupIds"`
}

func (h *Handler) handleAddInstanceGroups(
	ctx context.Context,
	in *addInstanceGroupsInput,
) (*addInstanceGroupsOutput, error) {
	groupIDs, clusterARN, err := h.Backend.AddInstanceGroups(ctx, in.JobFlowID, in.InstanceGroups)
	if err != nil {
		return nil, err
	}

	return &addInstanceGroupsOutput{
		ClusterArn:       clusterARN,
		InstanceGroupIDs: groupIDs,
		JobFlowID:        in.JobFlowID,
	}, nil
}

// --- ModifyInstanceGroups ---

type modifyInstanceGroupsInput struct {
	ClusterID      string                      `json:"ClusterId"`
	InstanceGroups []InstanceGroupModification `json:"InstanceGroups"`
}

type modifyInstanceGroupsOutput struct{}

func (h *Handler) handleModifyInstanceGroups(
	ctx context.Context,
	in *modifyInstanceGroupsInput,
) (*modifyInstanceGroupsOutput, error) {
	if err := h.Backend.ModifyInstanceGroups(ctx, in.ClusterID, in.InstanceGroups); err != nil {
		return nil, err
	}

	return &modifyInstanceGroupsOutput{}, nil
}
