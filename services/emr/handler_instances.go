package emr

import (
	"context"
)

// --- ListInstances ---

type listInstancesInput struct {
	ClusterID          string   `json:"ClusterId"`
	InstanceGroupID    string   `json:"InstanceGroupId"`
	InstanceFleetID    string   `json:"InstanceFleetId"`
	Marker             string   `json:"Marker"`
	InstanceGroupTypes []string `json:"InstanceGroupTypes"`
	InstanceStates     []string `json:"InstanceStates"`
}

type listInstancesOutput struct {
	Marker    string            `json:"Marker,omitempty"`
	Instances []ClusterInstance `json:"Instances"`
}

func (h *Handler) handleListInstances(ctx context.Context, in *listInstancesInput) (*listInstancesOutput, error) {
	params := ListInstancesParams{
		InstanceGroupID:    in.InstanceGroupID,
		InstanceFleetID:    in.InstanceFleetID,
		InstanceGroupTypes: in.InstanceGroupTypes,
		InstanceStates:     in.InstanceStates,
		Marker:             in.Marker,
	}

	instances, nextMarker := h.Backend.ListInstances(ctx, in.ClusterID, params)

	return &listInstancesOutput{Instances: instances, Marker: nextMarker}, nil
}
