package emr

import (
	"context"
)

// --- ListInstanceFleets ---

type listInstanceFleetsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listInstanceFleetsOutput struct {
	InstanceFleets []InstanceFleet `json:"InstanceFleets"`
}

func (h *Handler) handleListInstanceFleets(
	ctx context.Context,
	in *listInstanceFleetsInput,
) (*listInstanceFleetsOutput, error) {
	fleets, err := h.Backend.ListInstanceFleets(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &listInstanceFleetsOutput{InstanceFleets: fleets}, nil
}

// --- AddInstanceFleet ---

type addInstanceFleetInput struct {
	ClusterID     string            `json:"ClusterId"`
	InstanceFleet InstanceFleetSpec `json:"InstanceFleet"`
}

type addInstanceFleetOutput struct {
	ClusterArn      string `json:"ClusterArn"`
	ClusterID       string `json:"ClusterId"`
	InstanceFleetID string `json:"InstanceFleetId"`
}

func (h *Handler) handleAddInstanceFleet(
	ctx context.Context,
	in *addInstanceFleetInput,
) (*addInstanceFleetOutput, error) {
	fleet, clusterARN, err := h.Backend.AddInstanceFleet(ctx, in.ClusterID, in.InstanceFleet)
	if err != nil {
		return nil, err
	}

	return &addInstanceFleetOutput{
		ClusterArn:      clusterARN,
		ClusterID:       in.ClusterID,
		InstanceFleetID: fleet.ID,
	}, nil
}

// --- ModifyInstanceFleet ---

type modifyInstanceFleetInput struct {
	ClusterID     string                    `json:"ClusterId"`
	InstanceFleet InstanceFleetModification `json:"InstanceFleet"`
}

type modifyInstanceFleetOutput struct{}

func (h *Handler) handleModifyInstanceFleet(
	ctx context.Context,
	in *modifyInstanceFleetInput,
) (*modifyInstanceFleetOutput, error) {
	if err := h.Backend.ModifyInstanceFleet(ctx, in.ClusterID, in.InstanceFleet); err != nil {
		return nil, err
	}

	return &modifyInstanceFleetOutput{}, nil
}
