// Package dynamodb implements the AWS DynamoDB mock service.
// handler_autoscaling.go implements the wire-JSON handler for
// UpdateTableReplicaAutoScaling. Routing (dispatchExtraOps) stays in
// handler.go; this is the leaf implementation it calls into. Backend logic
// lives in autoscaling.go.
package dynamodb

import (
	"context"
	"encoding/json"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

type updateTableReplicaAutoScalingInput struct {
	TableName string `json:"TableName"`
}

type replicaAutoScalingDescWire struct {
	RegionName    string `json:"RegionName,omitempty"`
	ReplicaStatus string `json:"ReplicaStatus,omitempty"`
}

type tableAutoScalingDescWire struct {
	TableName   string                       `json:"TableName,omitempty"`
	TableStatus string                       `json:"TableStatus,omitempty"`
	Replicas    []replicaAutoScalingDescWire `json:"Replicas,omitempty"`
}

type updateTableReplicaAutoScalingOutput struct {
	TableAutoScalingDescription tableAutoScalingDescWire `json:"TableAutoScalingDescription"`
}

func (h *DynamoDBHandler) handleUpdateTableReplicaAutoScaling(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req updateTableReplicaAutoScalingInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.UpdateTableReplicaAutoScaling(
		ctx,
		&sdkDDB.UpdateTableReplicaAutoScalingInput{
			TableName: &req.TableName,
		},
	)
	if err != nil {
		return nil, err
	}

	desc := tableAutoScalingDescWire{}
	if out.TableAutoScalingDescription != nil {
		d := out.TableAutoScalingDescription
		desc.TableName = ptrconv.String(d.TableName)
		desc.TableStatus = string(d.TableStatus)
		desc.Replicas = make([]replicaAutoScalingDescWire, 0, len(d.Replicas))

		for _, r := range d.Replicas {
			desc.Replicas = append(desc.Replicas, replicaAutoScalingDescWire{
				RegionName:    ptrconv.String(r.RegionName),
				ReplicaStatus: string(r.ReplicaStatus),
			})
		}
	}

	return &updateTableReplicaAutoScalingOutput{TableAutoScalingDescription: desc}, nil
}
